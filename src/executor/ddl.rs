//! DDL execution: catalogue changes go straight to the engine

use super::Executor;
use crate::db::{DatabaseError, Schema};

impl Executor {
    #[inline(always)]
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<(), DatabaseError> {
        self.engine.create_table(name, schema)?;
        Ok(())
    }

    #[inline(always)]
    pub fn drop_table(&self, name: &str) -> Result<(), DatabaseError> {
        Ok(self.engine.drop_table(name)?)
    }
}
