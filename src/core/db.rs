use crate::core::date::DateParseError;
use crate::core::storage::btree::FixedSizeCmp;
use crate::core::storage::page::PageNumber;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::{Parser, ParserError};
use crate::sql::statement::{Column, Constraint, Create, Statement, Type, Value};
use crate::vm::TypeError;
use std::collections::HashMap;

#[derive(Debug, PartialEq)]
pub(crate) struct TableMetadata {
    root: PageNumber,
    name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
    row_id: RowId,
}

#[derive(Debug, PartialEq)]
pub(crate) struct IndexMetadata {
    root: PageNumber,
    pub name: String,
    column: Column,
    schema: Schema,
    unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Schema {
    pub columns: Vec<Column>,
    index: HashMap<String, usize>,
}

struct Context {
    tables: HashMap<String, TableMetadata>,
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
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError<'s>>;
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            index.insert(col.name.as_str(), i);
        }
        Self {
            columns,
            index: HashMap::new(),
        }
    }

    pub fn prepend_id(&mut self) {
        debug_assert!(
            self.columns.first().map_or(true, |c| c.name.eq(ROW_COL_ID)),
            "schema already has {ROW_COL_ID}: {self:?}"
        );

        let col = Column::new(ROW_COL_ID, Type::UnsignedBigInteger);

        let mut new_index = HashMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            new_index.insert(col.name.as_str(), i);
        }

        self.columns.insert(0, col);
        self.index.values_mut().for_each(|idx| *idx += 1);
        self.index.insert(ROW_COL_ID.to_string(), 0);
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
        Self::new(Vec::new())
    }
}

impl TableMetadata {
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

impl Context {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            max_size: None,
        }
    }

    pub fn insert(&mut self, metadata: TableMetadata) {
        if self.max_size.is_some_and(|size| self.tables.len() >= size) {
            let evict = self.tables.keys().next().unwrap().clone();
            self.tables.remove(&evict);
        }

        self.tables.insert(metadata.name.to_string(), metadata);
    }
}

/// Test-only implementation: clones everything for simplicity
#[cfg(test)]
impl<'s> TryFrom<&'s [&'s str]> for Context {
    type Error = DatabaseError<'s>;

    fn try_from(statements: &[&str]) -> Result<Self, Self::Error> {
        let mut context = Self::new();
        let mut root = 1;

        for sql in statements {
            let statement = Parser::new(sql).parse_statement()?;
            match statement {
                Statement::Create(Create::Table { name, columns }) => {
                    let mut schema = Schema::from(&columns);
                    schema.prepend_id();

                    let mut metadata = TableMetadata {
                        root,
                        name: name.clone(),
                        row_id: 1,
                        schema,
                        indexes: vec![],
                    };
                    root += 1;

                    columns.iter().for_each(|col| {
                        col.constraints.iter().for_each(|constraint| {
                            let index = match constraint {
                                Constraint::Unique => format!("{}_uq_index", col.name),
                                Constraint::PrimaryKey => format!("{}_pk_index", col.name),
                            };

                            metadata.indexes.push(IndexMetadata {
                                column: col.clone(),
                                schema: Schema::new(vec![col.clone(), columns[0].clone()]),
                                name: index,
                                root,
                                unique: true,
                            });

                            root += 1;
                        })
                    });

                    context.insert(metadata);
                }
                Statement::Create(Create::Index { name, column, unique, .. }) if unique => {
                    let table = context.metadata(&name)?;
                    let index_col = &table.schema.columns[table.schema.index_of(&column).unwrap()];

                    table.indexes.push(IndexMetadata {
                        column: index_col.clone(),
                        schema: Schema::new(vec![index_col.clone(), table.schema.columns[0].clone()]),
                        name,
                        root,
                        unique,
                    });

                    root += 1;
                }

                statement => {
                    return Err(SqlError::Other(format!("Only create unique index and create table should be called by test context, but found {statement:#?}")).into())
                }
            }
        }

        Ok(context)
    }
}

impl<'s> Ctx<'s> for Context {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError<'s>> {
        self.tables
            .get_mut(table)
            .ok_or_else(|| SqlError::InvalidTable(table.to_string()).into())
    }
}
impl<'c, Col: IntoIterator<Item = &'c Column>> From<Col> for Schema {
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
