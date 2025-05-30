use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::core::storage::btree::{BTreeKeyCmp, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::db::{DatabaseError, RowId, Schema};
use crate::sql::statement::{Column, Value};

use super::schema::umbra_schema;
use super::Database;

#[derive(Debug)]
pub(crate) struct TableMetadata {
    pub root: PageNumber,
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
    pub serials: HashMap<String, AtomicU64>,
    pub(in crate::db) row_id: RowId,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct IndexMetadata {
    pub root: PageNumber,
    pub name: String,
    pub column: Column,
    pub schema: Schema,
    pub(crate) unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Relation {
    Index(IndexMetadata),
    Table(TableMetadata),
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

impl TableMetadata {
    pub fn next_id(&mut self) -> RowId {
        let row_id = self.row_id;
        self.row_id += 1;
        row_id
    }

    pub fn next_serial_id(&mut self, column: &str) -> u64 {
        self.serials
            .get(column)
            .map(|count| count.fetch_add(1, Ordering::Relaxed) + 1)
            .expect("Unable to get next serial id for column")
    }

    pub fn create_serial_for_col(&mut self, column: String) {
        self.serials
            .entry(column)
            .or_insert_with(|| AtomicU64::new(0));
    }

    pub fn comp(&self) -> Result<FixedSizeCmp, DatabaseError> {
        FixedSizeCmp::try_from(&self.schema.columns[0].data_type).map_err(|e| {
            DatabaseError::Corrupted(format!(
                "Table {} is using a non-int Btree key with type {:#?}",
                self.name, self.schema.columns[0].data_type
            ))
        })
    }

    pub fn keys(&self) -> &Column {
        self.schema.keys()
    }

    pub fn key_only_schema(&self) -> Schema {
        Schema::new(vec![self.schema.columns[0].clone()])
    }
}

impl Relation {
    pub fn root(&self) -> PageNumber {
        match self {
            Self::Index(idx) => idx.root,
            Self::Table(table) => table.root,
        }
    }

    pub fn comp(&self) -> BTreeKeyCmp {
        match self {
            Self::Index(idx) => BTreeKeyCmp::from(&idx.column.data_type),
            Self::Table(table) => BTreeKeyCmp::from(&table.schema.columns[0].data_type),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Self::Index(idx) => &idx.schema,
            Self::Table(table) => &table.schema,
        }
    }

    pub fn kind(&self) -> &str {
        match self {
            Self::Index(_) => "index",
            Self::Table(_) => "table",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Index(idx) => &idx.name,
            Self::Table(table) => &table.name,
        }
    }

    pub fn index(&self) -> usize {
        match self {
            Self::Index(_) => 1,
            Self::Table(_) => 0,
        }
    }
}

impl Clone for TableMetadata {
    fn clone(&self) -> Self {
        let serials = self
            .serials
            .iter()
            .map(|(key, value)| (key.clone(), AtomicU64::new(value.load(Ordering::SeqCst))))
            .collect();

        TableMetadata {
            root: self.root,
            name: self.name.clone(),
            schema: self.schema.clone(),
            indexes: self.indexes.clone(),
            serials,
            row_id: self.row_id,
        }
    }
}

impl PartialEq for TableMetadata {
    fn eq(&self, other: &Self) -> bool {
        if self.root != other.root
            || self.row_id != other.row_id
            || self.schema != other.schema
            || self.indexes != other.indexes
            || self.name != other.name
            || self.serials.len() != other.serials.len()
        {
            return false;
        }

        self.serials.iter().all(|(k, v)| {
            other
                .serials
                .get(k)
                .map(|ov| v.load(Ordering::SeqCst) == ov.load(Ordering::SeqCst))
                .unwrap_or(false)
        })
    }
}

pub(crate) fn insert_metadata<File: Seek + Read + Write + FileOperations>(
    db: &mut Database<File>,
    values: Vec<Value>,
) -> Result<(), DatabaseError> {
    let mut schema = umbra_schema();
    schema.prepend_id();
    todo!("metadata insert")
}
