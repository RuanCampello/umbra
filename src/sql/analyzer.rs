use crate::sql::statement::Statement;
use std::fmt::Display;

#[derive(Debug, PartialEq)]
enum AnalyzerError {
    MissingCols,
    DuplicateCols(String),
    MultiplePrimaryKeys,
    AlreadyExists(AlreadyExists),
}

#[derive(Debug, PartialEq)]
enum AlreadyExists {
    Table(String),
    Index(String),
}

// TODO: we'll actually have a database error for this later
type AnalyzerResult<T> = Result<T, AnalyzerError>;

pub(in crate::sql) fn analyze(statement: &Statement) -> AnalyzerResult<()> {
    todo!()
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalyzerError::MissingCols => write!(f, "all columns must be specified"),
            AnalyzerError::DuplicateCols(col) => write!(f, "column {col} was already declared"),
            AnalyzerError::AlreadyExists(name) => f.write_str(&name.to_string()),
            AnalyzerError::MultiplePrimaryKeys => {
                write!(f, "a table can only have one primary key")
            }
        }
    }
}
impl Display for AlreadyExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlreadyExists::Table(table_name) => write!(f, "table {table_name} already exists"),
            AlreadyExists::Index(index_name) => write!(f, "index {index_name} already exists"),
        }
    }
}
