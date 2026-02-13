use crate::db::DatabaseError;
use crate::sql::statement::Value;
use std::str::FromStr;

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum IndexKind {
    BTree,
}

/// Abstract interface for database indexes.
/// This allows the Table to hold a heterogeneous collection of indexes (BTree, Hash, etc.).
pub trait Index {
    /// Inserts a key-value pair into the index.
    /// - `key`: The value of the indexed column(s).
    /// - `id`: The Row ID (or Tuple location) that this key points to.
    fn insert(&mut self, key: &Value, id: &Value) -> Result<(), DatabaseError>;

    /// Removes a key-value pair from the index.
    fn delete(&mut self, key: &Value, id: &Value) -> Result<(), DatabaseError>;

    /// Adds multiple entries to the index in a single batch operation.
    fn insert_batch(&mut self, batch: &[(&Value, &Value)]) -> Result<(), DatabaseError> {
        unimplemented!()
    }

    /// Removes multiple entries from the index in a single batch operation.
    fn delete_batch(&mut self, batch: &[(&Value, &Value)]) -> Result<(), DatabaseError> {
        unimplemented!()
    }

    /// Searches for a specific key in the index.
    /// Returns a list of Row IDs (RIDs) that match the key.
    fn find(&mut self, key: &Value) -> Result<Vec<Value>, DatabaseError>;

    /// Finds all entries where the columns are in the given range.
    fn find_range(&mut self, _min: &Value, _max: &Value) -> Result<Vec<Value>, DatabaseError> {
        unimplemented!()
    }

    /// Returns a string identifier for the index type (e.g., "btree", "hash").
    fn kind(&self) -> IndexKind;
}

impl FromStr for IndexKind {
    type Err = DatabaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "btree" => Self::BTree,
            _ => return Err(DatabaseError::Other(format!("Invalid index kind: {s}"))),
        })
    }
}

impl From<IndexKind> for &str {
    fn from(value: IndexKind) -> Self {
        match value {
            IndexKind::BTree => "btree",
        }
    }
}
