use crate::core::date::DateParseError;
use crate::core::storage::btree::FixedSizeCmp;
use crate::core::storage::page::PageNumber;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::statement::{Column, Value};
use crate::vm::TypeError;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub(crate) struct TableMetadata<'s> {
    root: PageNumber,
    name: String,
    pub schema: Schema<'s>,
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
    pub columns: &'s [Column],
    index: HashMap<&'s str, usize>,
}

struct Context<'s> {
    tables: HashMap<String, TableMetadata<'s>>,
    max_size: usize,
}

pub(crate) enum DatabaseError<'exp> {
    Sql(SqlError<'exp>),
    /// Something went wrong with the underlying storage (db or journal file).
    Corrupted(String),
}

#[derive(Debug, PartialEq)]
pub(crate) enum SqlError<'exp> {
    /// Database table isn't found or somewhat corrupted.
    InvalidTable(String),
    /// Column isn't found or not usable in the given context.
    InvalidColumn(String),
    /// Duplicated UNIQUE or PRIMARY KEY col.
    DuplicatedKey(Value),
    /// [Analyzer error](AnalyzerError).
    Analyzer(AnalyzerError),
    Type(TypeError<'exp>),
    Other(String),
}

type RowId = u64;

/// The identifier of [row id](https://www.sqlite.org/rowidtable.html) column.
pub(crate) const ROW_COL_ID: &str = "row_id";
pub(crate) const DB_METADATA: &str = "limbo_db_meta";

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

    pub fn columns_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.to_string()).collect()
    }

    pub fn keys(&self) -> &Column {
        &self.columns[0]
    }

    pub fn empty() -> Self {
        Self::new(&[])
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

impl<'exp> From<AnalyzerError> for SqlError<'exp> {
    fn from(value: AnalyzerError) -> Self {
        SqlError::Analyzer(value)
    }
}

impl<'exp> From<TypeError<'exp>> for SqlError<'exp> {
    fn from(value: TypeError<'exp>) -> Self {
        SqlError::Type(value)
    }
}

impl<'exp> From<DateParseError> for SqlError<'exp> {
    fn from(value: DateParseError) -> Self {
        SqlError::Type(TypeError::InvalidDate(value))
    }
}

impl<'exp> From<SqlError<'exp>> for DatabaseError<'exp> {
    fn from(value: SqlError<'exp>) -> Self {
        DatabaseError::Sql(value)
    }
}

impl<'exp> From<AnalyzerError> for DatabaseError<'exp> {
    fn from(value: AnalyzerError) -> Self {
        DatabaseError::from(SqlError::from(value))
    }
}
