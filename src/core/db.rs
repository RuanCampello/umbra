use crate::core::storage::btree::FixedSizeCmp;
use crate::core::storage::page::PageNumber;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::statement::{Column, Value};
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub(crate) struct TableMetadata<'s> {
    root: PageNumber,
    name: String,
    schema: Schema<'s>,
    pub indexes: Vec<IndexMetadata<'s>>,
    row_id: RowId,
}

#[derive(Debug, PartialEq)]
pub(crate) struct IndexMetadata<'s> {
    root: PageNumber,
    pub name: String,
    column: Column,
    schema: Schema<'s>,
    unique: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Schema<'s> {
    columns: &'s [Column],
    index: HashMap<&'s str, usize>,
}

struct Context<'s> {
    tables: HashMap<String, TableMetadata<'s>>,
    max_size: usize,
}

pub(crate) enum DatabaseError {
    Sql(SqlError),
    /// Something went wrong with the underlying storage (db or journal file).
    Corrupted(String),
}

#[derive(Debug, PartialEq)]
pub(crate) enum SqlError {
    /// Database table isn't found or somewhat corrupted.
    InvalidTable(String),
    /// Column isn't found or not usable in the given context.
    InvalidColumn(String),
    /// Duplicated UNIQUE or PRIMARY KEY col.
    DuplicatedKey(Value),
    /// [Analyzer error](AnalyzerError).
    Analyzer(AnalyzerError),
    Other(String),
}

type RowId = u64;

/// The identifier of [row id](https://www.sqlite.org/rowidtable.html) column.
pub(crate) const ROW_COL_ID: &str = "row_id";

pub(crate) trait Ctx<'s> {
    fn metadata(&'s mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError>;
}

impl<'s> Schema<'s> {
    pub fn new(columns: &'s [Column]) -> Self {
        let mut index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            index.insert(col.name.as_str(), i);
        }
        Self { columns, index }
    }

    pub fn index_of(&self, col: &str) -> Option<usize> {
        self.index.get(col).copied()
    }

    pub fn keys(&self) -> &Column {
        &self.columns[0]
    }
}

impl<'s> TableMetadata<'s> {
    pub fn next_id(&mut self) -> RowId {
        let row_id = self.row_id;
        self.row_id += 1;
        row_id
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
}

impl<'s> Ctx<'s> for Context<'s> {
    fn metadata(&'s mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| SqlError::InvalidTable(table.to_string()).into())
    }
}

impl From<AnalyzerError> for SqlError {
    fn from(value: AnalyzerError) -> Self {
        SqlError::Analyzer(value)
    }
}

impl From<SqlError> for DatabaseError {
    fn from(value: SqlError) -> Self {
        DatabaseError::Sql(value)
    }
}

impl From<AnalyzerError> for DatabaseError {
    fn from(value: AnalyzerError) -> Self {
        DatabaseError::from(SqlError::from(value))
    }
}
