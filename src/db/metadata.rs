use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::storage::btree::{BTree, BTreeKeyCmp, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::db::{DatabaseError, RowId, Schema};
use crate::sql::analyzer::AnalyzerError;
use crate::sql::statement::{Column, Type, Value};

use super::schema::umbra_schema;
use super::Database;

/// That's all the information we've from a table during runtime.
/// We save this to the database metadata table at page zero.
/// Then we load into this structure in a form of primitive cache to do our operations
/// without hitting the database it self.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct TableMetadata {
    pub root: PageNumber,
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
    pub serials: HashMap<String, SequenceMetadata>,
    pub(in crate::db) row_id: RowId,
    pub(in crate::db) enums: EnumRegistry,
}

/// The information we know about the table indexes during runtime.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct IndexMetadata {
    pub root: PageNumber,
    pub name: String,
    pub column: Column,
    pub schema: Schema,
    /// This is an illusion because we only support `UNIQUE` indexes :)
    pub(crate) unique: bool,
}

/// The data we need to know about the table sequences during runtime.
#[derive(Debug)]
pub(crate) struct SequenceMetadata {
    pub root: PageNumber,
    pub name: String,
    pub value: AtomicU64,
    pub data_type: Type,
}

/// Basic lookup table for evaluating enums during runtime.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct EnumRegistry {
    enums: HashMap<String, Vec<String>>,
}

/// That our dispatch for relation types.
/// Some [planners](crate::vm::planner::Planner) need to deal with table Btrees and index Btrees but we only know which at runtime.
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Relation {
    Index(IndexMetadata),
    Table(TableMetadata),
    Sequence(SequenceMetadata),
}

#[macro_export]
/// — `primary on table_name`
/// — `primary on (table_name_variable)`
///
/// — `unique  on (table_name)(column_name)`
///
/// You can pass anything into `table_name` or `column_name` on `unique`
/// a string, variable etc, if you pass an non scoped variable or non string
/// it will make it a string.
///
/// On `primary` you can pass anything, but variables. To pass variables, put them inside
/// parenthesis.
macro_rules! index {
    // ── PRIMARY ───────────────────────────────────────────────────────
    (primary on ( $table:expr )) => {{
        format!("{}_pk_index", $table)
    }};

    (primary on $table:ident) => {{
        format!("{}_pk_index", stringify!($table))
    }};

    // ── UNIQUE ────────────────────────────────────────────────────────
    (unique on ( $table:expr ) ( $col:expr )) => {{
        format!("{}_{}_uq_index", $table, $col)
    }};

    (unique on $table:ident ( $col:ident )) => {{
        format!("{}_{}_uq_index", stringify!($table), stringify!($col))
    }};

    (unique on $table:ident ( $col:expr )) => {{
        format!("{}_{}_uq_index", stringify!($table), $col)
    }};

    (unique on ( $table:expr ) ( $col:ident )) => {{
        format!("{}_{}_uq_index", $table, stringify!($col))
    }};
}

macro_rules! sequence {
    // ── SEQUENCE ────────────────────────────────────────────────────────
    (sequence on ($table:expr) ($col:expr)) => {{
        format!("{}_{}_seq", $table, $col)
    }};
}

impl TableMetadata {
    pub fn next_id(&mut self) -> RowId {
        let row_id = self.row_id;
        self.row_id += 1;
        row_id
    }

    pub fn next_val(&mut self, table: &str, column: &str) -> Result<u64, DatabaseError> {
        let sequence_name = sequence!(sequence on (table) (column));
        let sequence = self
            .serials
            .get(&sequence_name)
            .expect("Failed to get next_val for this column");

        let max = sequence.data_type.max();
        let current = sequence.value.fetch_add(1, Ordering::Relaxed);

        if current >= max as u64 {
            return Err(AnalyzerError::Overflow(sequence.data_type, max).into());
        }

        Ok(current + 1)
    }

    pub fn comp(&self) -> Result<FixedSizeCmp, DatabaseError> {
        FixedSizeCmp::try_from(&self.schema.columns[0].data_type).map_err(|e| {
            DatabaseError::Corrupted(format!(
                "Table {} is using a non-int Btree key with type {:#?}",
                self.name, self.schema.columns[0].data_type
            ))
        })
    }

    pub fn get_enum(&self, identifier: &str) -> Result<Vec<&str>, DatabaseError> {
        self.enums
            .get(identifier)
            .map(|variants| variants.iter().map(String::as_str).collect())
            .ok_or(DatabaseError::Other(format!(
                "Enum '{identifier}' does not exists"
            )))
    }

    pub fn keys(&self) -> &Column {
        self.schema.keys()
    }

    pub fn key_only_schema(&self) -> Schema {
        Schema::new(vec![self.schema.columns[0].clone()])
    }
}

impl EnumRegistry {
    pub(in crate::db) fn empty() -> Self {
        Self {
            enums: HashMap::new(),
        }
    }

    pub(in crate::db::metadata) fn get(&self, identifier: &str) -> Option<&Vec<String>> {
        self.enums.get(identifier)
    }
}

impl Relation {
    pub fn root(&self) -> PageNumber {
        match self {
            Self::Sequence(seq) => seq.root,
            Self::Index(idx) => idx.root,
            Self::Table(table) => table.root,
        }
    }

    pub fn comp(&self) -> BTreeKeyCmp {
        match self {
            Self::Sequence(seq) => BTreeKeyCmp::from(&seq.data_type),
            Self::Index(idx) => BTreeKeyCmp::from(&idx.column.data_type),
            Self::Table(table) => BTreeKeyCmp::from(&table.schema.columns[0].data_type),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Self::Sequence(seq) => panic!("Sequence does not have a schema"),
            Self::Index(idx) => &idx.schema,
            Self::Table(table) => &table.schema,
        }
    }

    pub fn kind(&self) -> &str {
        match self {
            Self::Sequence(_) => "sequence",
            Self::Index(_) => "index",
            Self::Table(_) => "table",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Sequence(seq) => &seq.name,
            Self::Index(idx) => &idx.name,
            Self::Table(table) => &table.name,
        }
    }

    pub fn index(&self) -> usize {
        match self {
            Self::Sequence(_) => 2,
            Self::Index(_) => 1,
            Self::Table(_) => 0,
        }
    }
}

impl PartialEq for SequenceMetadata {
    fn eq(&self, other: &Self) -> bool {
        self.root == other.root
            && self.name == other.name
            && self.data_type == other.data_type
            && self.value.load(Ordering::SeqCst) == other.value.load(Ordering::SeqCst)
    }
}

impl Clone for SequenceMetadata {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            name: self.name.clone(),
            value: AtomicU64::new(self.value.load(Ordering::Relaxed)),
            root: self.root.clone(),
        }
    }
}
