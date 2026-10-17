//! The user-facing database API: sessions ([MvccDatabase]), results ([QuerySet]) and errors

mod context;
mod metadata;
mod mvcc;
mod schema;

pub use crate::core::numeric::Numeric;
pub(crate) use context::Context;
#[cfg(test)]
pub(crate) use metadata::IndexMetadata;
pub(crate) use metadata::TableMetadata;
pub use mvcc::MvccDatabase;
#[cfg(test)]
pub(crate) use schema::SchemaBuilder;
pub(crate) use schema::{Column, ForeignKey, Schema};

use crate::core::date::{DateParseError, ExtractError};
use crate::core::uuid::UuidError;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::ParserError;
use crate::sql::statement::Value;
use crate::vm::expression::{TypeError, VmError};
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, PartialEq, Default)]
pub struct QuerySet {
    pub tuples: Vec<Vec<Value>>,
    pub schema: Schema,
}

#[derive(Debug)]
pub enum DatabaseError {
    Parser(ParserError),
    Sql(SqlError),
    Io(std::io::Error),
    /// Something went wrong with the underlying storage files
    Corrupted(String),
    /// A concurrent transaction got there first
    /// retrying the transaction may succeed
    Conflict(String),
    Other(String),
    NoMemory,
}

#[derive(Debug, PartialEq)]
pub enum SqlError {
    /// Database table isn't found or somewhat corrupted
    InvalidTable(String),
    /// Column isn't found or not usable in the given context
    InvalidColumn(String),
    /// Column doesn't exist in this given table
    InvalidQualifiedColumn {
        table: String,
        column: String,
    },
    /// Column does not appear in `group by` or isn't used in aggregation
    InvalidGroupBy(String),
    /// Duplicated UNIQUE or PRIMARY KEY col
    DuplicatedKey(Value),
    /// [Analyzer error](AnalyzerError).
    Analyzer(AnalyzerError),
    /// Invalid function arguments. Expected x but found y
    InvalidFuncArgs(usize, usize),
    Type(TypeError),
    Vm(VmError),
    Other(String),
}

/// What the analyzer and statement preparation need from a session
pub(crate) trait Ctx {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata>;
    fn next_serial(&mut self, table: &str, column: usize) -> Result<i128>;
}

/// The identifier of [row id](https://www.sqlite.org/rowidtable.html) column
pub(crate) const ROW_COL_ID: &str = "row_id";
/// A table name the catalogue reserves
pub(crate) const DB_METADATA: &str = "umbra_db_meta";
const DEFAULT_CACHE_SIZE: usize = 512;

type Result<T> = std::result::Result<T, DatabaseError>;

#[macro_export]
macro_rules! temporal {
    ($time_str:expr) => {
        $time_str.try_into().map(Value::Temporal)
    };
}

#[macro_export]
macro_rules! interval {
    ($interval_str:expr) => {
        $interval_str.parse().map(Value::Interval).expect("Couldn't create interval")
    };
}

#[allow(unused)]
impl QuerySet {
    pub(crate) fn new(schema: Schema, tuples: Vec<Vec<Value>>) -> Self {
        Self { schema, tuples }
    }

    pub(crate) fn empty() -> Self {
        Self { schema: Schema::empty(), tuples: Vec::new() }
    }

    #[inline(always)]
    fn get(&self, row: usize, column: &str) -> Option<&Value> {
        self.tuples.get(row)?.get(self.schema.index_of(column)?)
    }

    pub fn is_empty(&self) -> bool {
        self.tuples.iter().all(|tuple| tuple.is_empty())
    }

    #[inline(always)]
    pub const fn len(&self) -> usize {
        self.tuples.len()
    }

    pub fn display_enums(&mut self) {
        let variants: Vec<(_, Vec<_>)> = self
            .schema
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                let names = col.variants()?;
                Some((idx, names.iter().map(|name| Arc::from(name.as_str())).collect()))
            })
            .collect();

        if variants.is_empty() {
            return;
        }

        for tuple in &mut self.tuples {
            for (idx, names) in &variants {
                let Some(Value::Enum(variant)) = tuple.get(*idx) else {
                    continue;
                };
                if let Some(name) = names.get(*variant as usize) {
                    tuple[*idx] = Value::String(Arc::clone(name));
                }
            }
        }
    }
}

impl Display for QuerySet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let widths: Vec<usize> = self
            .schema
            .columns
            .iter()
            .enumerate()
            .map(|(i, col)| {
                let header_len = col.name().len();
                let max_data_len =
                    self.tuples.iter().map(|row| row[i].to_string().len()).max().unwrap_or(0);
                header_len.max(max_data_len)
            })
            .collect();

        let border: String =
            widths.iter().map(|&w| "-".repeat(w + 2)).collect::<Vec<_>>().join("+");
        let full_border = format!("+{}+", border);

        let header = self
            .schema
            .columns
            .iter()
            .zip(&widths)
            .map(|(col, &width)| format!(" {:<width$} ", col.name(), width = width))
            .collect::<Vec<_>>()
            .join("|");

        writeln!(f, "{}", full_border)?;
        writeln!(f, "|{}|", header)?;
        writeln!(f, "{}", full_border)?;

        if self.tuples.is_empty() {
            writeln!(
                f,
                "| (0 rows)                                                               |"
            )?;
        } else {
            for row in &self.tuples {
                let row_str = row
                    .iter()
                    .zip(&widths)
                    .enumerate()
                    .map(|(i, (value, &width))| {
                        let value_str = value.to_string();
                        match self.schema.columns[i].column_type().is_number() {
                            true => format!(" {:>width$} ", value_str, width = width),
                            _ => format!(" {:<width$} ", value_str, width = width),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                writeln!(f, "|{}|", row_str)?;
            }
        }

        writeln!(f, "{}", full_border)?;
        Ok(())
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Parser(err) => write!(f, "{err}"),
            Self::Sql(err) => write!(f, "{err}"),
            Self::Corrupted(message) => f.write_str(message),
            Self::Conflict(message) => f.write_str(message),
            Self::Other(message) => f.write_str(message),
            Self::NoMemory => f.write_str("Out of memory"),
        }
    }
}

impl Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTable(table) => write!(f, "Invalid table '{table}'"),
            Self::InvalidColumn(column) => write!(f, "Invalid column '{column}'"),
            Self::InvalidQualifiedColumn { table, column } => {
                write!(f, "Column '{column}' not found on table '{table}'")
            },
            Self::InvalidGroupBy(column) => write!(
                f,
                "Column '{column}' must appear in GROUP BY clause or be used in an aggregate function"
            ),
            Self::DuplicatedKey(key) => write!(f, "Duplicated key {key}"),
            Self::Analyzer(err) => write!(f, "{err}"),
            Self::Vm(err) => write!(f, "{err}"),
            Self::Type(err) => write!(f, "{err}"),
            Self::InvalidFuncArgs(expected, got) => {
                write!(f, "Invalid number of arguments. Expected {expected}, got {got}")
            },
            Self::Other(other) => f.write_str(other),
        }
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

impl From<ExtractError> for SqlError {
    fn from(value: ExtractError) -> Self {
        TypeError::ExtractError(value).into()
    }
}

impl From<DateParseError> for DatabaseError {
    fn from(value: DateParseError) -> Self {
        SqlError::from(value).into()
    }
}

impl From<UuidError> for SqlError {
    fn from(value: UuidError) -> Self {
        TypeError::UuidError(value).into()
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

impl PartialEq for DatabaseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(a), Self::Io(b)) => a.kind().eq(&b.kind()),
            (Self::Parser(a), Self::Parser(b)) => a.eq(b),
            (Self::Sql(a), Self::Sql(b)) => a.eq(b),
            _ => false,
        }
    }
}
