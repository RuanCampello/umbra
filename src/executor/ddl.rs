//! DDL execution dispatch.
//!
//! Handles `CREATE TABLE` by delegating directly to the
//! MVCC [`Engine`](crate::storage::mvcc::engine::Engine).

use super::Executor;
use crate::db::{DatabaseError, SchemaNew as Schema};

impl Executor {
    /// Creates a table in the engine and returns the stored schema.
    pub fn create_table(&self, schema: Schema) -> Result<Schema, DatabaseError> {
        Ok(self.engine.create_table(schema)?)
    }
}
