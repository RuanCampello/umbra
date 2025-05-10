#![allow(unused_imports)]

mod metadata;
mod schema;

pub(crate) use metadata::{Relation, TableMetadata};
pub(crate) use schema::Schema;

use self::metadata::IndexMetadata;
use crate::core::date::DateParseError;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::{Parser, ParserError};
use crate::sql::statement::{Column, Constraint, Create, Statement, Value};
use crate::vm::expression::{TypeError, VmError};
use std::collections::HashMap;

pub(crate) struct Context {
    tables: HashMap<String, TableMetadata>,
    max_size: Option<usize>,
}

#[derive(Debug)]
pub(crate) enum DatabaseError {
    Parser(ParserError),
    Sql(SqlError),
    Io(std::io::Error),
    /// Something went wrong with the underlying storage (db or journal file).
    Corrupted(String),
    Other(String),
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
    Type(TypeError),
    Vm(VmError),
    Other(String),
}

type RowId = u64;

/// The identifier of [row id](https://www.sqlite.org/rowidtable.html) column.
pub(crate) const ROW_COL_ID: &str = "row_id";
pub(crate) const DB_METADATA: &str = "limbo_db_meta";

pub(crate) trait Ctx<'s> {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError>;
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
impl TryFrom<&[&str]> for Context {
    type Error = DatabaseError;

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
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
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

impl From<AnalyzerError> for SqlError {
    fn from(value: AnalyzerError) -> Self {
        SqlError::Analyzer(value)
    }
}

impl From<VmError> for SqlError {
    fn from(value: VmError) -> Self {
        SqlError::Vm(value)
    }
}

impl From<TypeError> for SqlError {
    fn from(value: TypeError) -> Self {
        SqlError::Type(value)
    }
}

impl From<DateParseError> for SqlError {
    fn from(value: DateParseError) -> Self {
        TypeError::InvalidDate(value).into()
    }
}

impl From<SqlError> for DatabaseError {
    fn from(value: SqlError) -> Self {
        DatabaseError::Sql(value)
    }
}

impl From<TypeError> for DatabaseError {
    fn from(value: TypeError) -> Self {
        SqlError::from(value).into()
    }
}

impl From<AnalyzerError> for DatabaseError {
    fn from(value: AnalyzerError) -> Self {
        SqlError::from(value).into()
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ParserError> for DatabaseError {
    fn from(value: ParserError) -> Self {
        DatabaseError::Parser(value)
    }
}
