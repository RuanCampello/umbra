use crate::sql::Value;
use crate::storage::engine::table::Row;
use std::fmt::{self, Display};
use std::io;
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum EngineError {
    Io(io::Error),
    Corrupt(String),
    /// a durability failure left the engine unable to promise anything further, for the cause given
    /// reopen it
    Poisoned(Arc<str>),
    /// the engine was closed: nothing commits any more
    Closed,
    TableNotFound(String),
    TableExists(String),
    IndexExists(String),
    ColumnNotFound(String),
    /// the row is being written by another, still uncommitted, transaction
    WriteConflict,
    /// a concurrent commit invalidated this transaction's reads or writes
    SerializationFailure,
    DuplicatedKey(Value),
    /// READ COMMITTED only: the row changed after the statement's snapshot
    /// carries its newest committed image (`None` once deleted) so the statement can re-check and retry
    Stale(Option<Row>),
    /// the transaction failed earlier and was rolled back
    Aborted,
    NoSavepoint(String),
}

pub(crate) type Result<T> = std::result::Result<T, EngineError>;

impl Display for EngineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(cause) => write!(f, "storage I/O failed: {cause}"),
            Self::Corrupt(what) => write!(f, "database files are corrupt: {what}"),
            Self::Poisoned(cause) => {
                write!(f, "engine stopped after a durability failure ({cause}), reopen it")
            },
            Self::Closed => f.write_str("the database is closed"),
            Self::TableNotFound(name) => write!(f, "table {name} does not exist"),
            Self::TableExists(name) => write!(f, "table {name} already exists"),
            Self::IndexExists(name) => write!(f, "index {name} already exists"),
            Self::ColumnNotFound(name) => write!(f, "column {name} does not exist"),
            Self::WriteConflict => {
                f.write_str("could not serialize access: row is locked by a concurrent transaction")
            },
            Self::SerializationFailure => {
                f.write_str("could not serialize access due to concurrent update")
            },
            Self::DuplicatedKey(value) => write!(f, "duplicated key value: {value}"),
            Self::Stale(_) => f.write_str("row changed after the statement started"),
            Self::Aborted => f.write_str("transaction was aborted by an earlier error"),
            Self::NoSavepoint(name) => write!(f, "savepoint {name} does not exist"),
        }
    }
}

impl From<io::Error> for EngineError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
