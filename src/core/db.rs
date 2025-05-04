use crate::core::date::DateParseError;
use crate::core::storage::btree::FixedSizeCmp;
use crate::core::storage::page::PageNumber;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::{Parser, ParserError};
use crate::sql::statement::{Column, Create, Statement, Type, Value};
use crate::vm::TypeError;
use std::cell::OnceCell;
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
    pub columns: Vec<Column>,
    index: OnceCell<HashMap<&'s str, usize>>,
}

struct Context<'s> {
    tables: HashMap<String, TableMetadata<'s>>,
    max_size: Option<usize>,
}

pub(crate) enum DatabaseError<'exp> {
    Parser(ParserError),
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
    pub fn new(columns: Vec<Column>) -> Self {
        let mut index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            index.insert(col.name.as_str(), i);
        }
        Self {
            columns,
            index: OnceCell::new(),
        }
    }

    pub fn prepend_id(&'s mut self) {
        debug_assert!(
            self.columns.first().map_or(true, |c| c.name.eq(ROW_COL_ID)),
            "schema already has {ROW_COL_ID}: {self:?}"
        );

        let col = Column::new(ROW_COL_ID, Type::UnsignedBigInteger);
        self.columns.insert(0, col);

        let mut new_index = HashMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            new_index.insert(col.name.as_str(), i);
        }

        self.index = OnceCell::new();
        self.index.set(new_index).unwrap();
    }

    fn ensure_index(&'s self) -> &HashMap<&'s str, usize> {
        self.index.get_or_init(|| {
            let mut map = HashMap::new();
            for (i, col) in self.columns.iter().enumerate() {
                map.insert(col.name.as_str(), i);
            }
            map
        })
    }

    pub fn index_of(&'s self, col: &str) -> Option<usize> {
        self.ensure_index().get(col).copied()
    }

    pub fn columns_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.to_string()).collect()
    }

    pub fn keys(&self) -> &Column {
        &self.columns[0]
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
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

impl<'s> Context<'s> {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            max_size: None,
        }
    }
}

impl<'s> TryFrom<&[&str]> for Context<'s> {
    type Error = DatabaseError<'s>;

    fn try_from(statements: &[&str]) -> Result<Self, Self::Error> {
        let mut ctx = Self::new();
        let mut root = 1;

        for sql in statements {
            let statement = Parser::new(sql).parse_statement()?;

            match statement {
                Statement::Create(Create::Table { name, columns }) => {
                    let mut schema = Schema::from(&columns);
                    schema.prepend_id();
                }
                _ => {}
            }
        }
        todo!()
    }
}

impl<'s> Ctx<'s> for Context<'s> {
    fn metadata(&'s mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| SqlError::InvalidTable(table.to_string()).into())
    }
}

impl<'c, Col: IntoIterator<Item = &'c Column>> From<Col> for Schema<'c> {
    fn from(columns: Col) -> Self {
        Self::new(Vec::from_iter(columns.into_iter().cloned()))
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
        TypeError::InvalidDate(value).into()
    }
}

impl<'exp> From<SqlError<'exp>> for DatabaseError<'exp> {
    fn from(value: SqlError<'exp>) -> Self {
        DatabaseError::Sql(value)
    }
}

impl<'exp> From<TypeError<'exp>> for DatabaseError<'exp> {
    fn from(value: TypeError<'exp>) -> Self {
        SqlError::from(value).into()
    }
}

impl<'exp> From<AnalyzerError> for DatabaseError<'exp> {
    fn from(value: AnalyzerError) -> Self {
        SqlError::from(value).into()
    }
}

impl<'exp> From<ParserError> for DatabaseError<'exp> {
    fn from(value: ParserError) -> Self {
        DatabaseError::Parser(value)
    }
}
