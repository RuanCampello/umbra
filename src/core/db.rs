use crate::core::storage::btree::FixedSizeCmp;
use crate::core::storage::page::PageNumber;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::statement::{Column, Value};
use std::collections::HashMap;
use std::marker::PhantomData;

#[derive(Debug, PartialEq)]
pub(crate) struct TableMetadata {
    root: PageNumber,
    name: String,
    schema: Schema,
    indexes: Vec<IndexMetadata>,
    row_id: RowId,
}

#[derive(Debug, PartialEq)]
struct IndexMetadata {
    root: PageNumber,
    name: String,
    column: Column,
    schema: Schema,
    unique: bool,
}

#[derive(Debug, PartialEq)]
struct Schema {
    columns: Vec<Column>,
    name_ptrs: HashMap<*const str, usize>,
    // PhantomData ensures proper drop checking
    _marker: PhantomData<Box<str>>,
}

struct Context {
    tables: HashMap<String, TableMetadata>,
    max_size: usize,
}

#[derive(Debug, PartialEq)]
pub enum SqlError {
    /// Database table isn't found or somewhat corrupted.
    InvalidTable(String),
    /// Column isn't found or not usable in the given context.
    InvalidColumn(String),
    /// Duplicated UNIQUE or PRIMARY KEY col.
    DuplicatedKey(Value),
    /// [Analyzer error](AnalyzerError).
    Analyzer(AnalyzerError),
}

type RowId = u64;

/// The identifier of [row id](https://www.sqlite.org/rowidtable.html) column.
pub(crate) const ROW_COL_ID: &str = "row_id";

pub(crate) trait Ctx {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, SqlError>;
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut name_ptrs = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            let name_ptr = col.name.as_str() as *const str;
            name_ptrs.insert(name_ptr, i);
        }

        Self {
            columns,
            name_ptrs,
            _marker: PhantomData,
        }
    }

    pub fn get_column(&self, name: &str) -> Option<&Column> {
        let name_ptr = name as *const str;
        self.name_ptrs.get(&name_ptr).map(|&i| &self.columns[i])
    }

    pub fn keys(&self) -> &Column {
        &self.columns[0]
    }
}

impl TableMetadata {
    pub fn next_id(&mut self) -> RowId {
        let row_id = self.row_id;
        self.row_id += 1;
        row_id
    }

    pub fn comp(&self) -> Result<FixedSizeCmp, SqlError> {
        FixedSizeCmp::try_from(&self.schema.columns[0].data_type)
            .map_err(|e| format!("Failed to convert data type: {e:#?}").into())
    }

    pub fn keys(&self) -> &Column {
        self.schema.keys()
    }
}

impl Ctx for Context {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, SqlError> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| format!("Table not found: {table:#?}").into())
    }
}

impl From<AnalyzerError> for SqlError {
    fn from(value: AnalyzerError) -> Self {
        SqlError::Analyzer(value)
    }
}
