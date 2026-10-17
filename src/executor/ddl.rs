//! DDL execution: catalogue changes run in the session's transaction, like writes

use super::{Executor, dml::coerce_value};
use crate::db::{self, DatabaseError, Schema};
use crate::sql::statement::{Column, Constraint};
use crate::vm::expression::resolve_only_expression;

impl Executor {
    /// a foreign key must refer to a primary key or UNIQUE column, so one row at most holds a key
    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<(), DatabaseError> {
        self.with_txn(|_, txn| {
            for key in schema.constraints.foreign_keys.iter() {
                let referenced = match key.table == name {
                    true => schema.clone(),
                    _ => txn.table(&key.table)?.schema.clone(),
                };
                let unique = referenced
                    .index_of(&key.referenced)
                    .map(|index| &referenced.columns[index])
                    .is_some_and(|column| column.is_primary_key() || column.is_unique());

                if !unique {
                    let message = format!(
                        "foreign key target {}({}) is not unique",
                        key.table, key.referenced
                    );
                    return Err(DatabaseError::Sql(db::SqlError::Other(message)));
                }
            }

            Ok(txn.create_table(name, schema).map(|_| ())?)
        })
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), DatabaseError> {
        self.with_txn(|_, txn| Ok(txn.drop_table(name)?))
    }

    pub fn create_index(
        &mut self,
        name: &str,
        table: &str,
        column: &str,
        unique: bool,
    ) -> Result<(), DatabaseError> {
        self.with_txn(|_, txn| Ok(txn.create_index(name, table, column, unique)?))
    }
}

/// the schema of a `CREATE TABLE` column list, defaults evaluated once and typed like their column
pub(super) fn table_schema(columns: &[Column]) -> Result<Schema, DatabaseError> {
    let mut schema = Schema::from_definition(columns);

    for (index, column) in columns.iter().enumerate() {
        for constraint in &column.constraints {
            if let Constraint::Default(expr) = constraint {
                let mut value = resolve_only_expression(expr)?;
                coerce_value(&schema.columns[index], &mut value)?;
                schema.set_default(index, value);
            }
        }
    }

    Ok(schema)
}
