//! Top-level statement dispatch.
//!
//! Routes a parsed [`Statement`] to the appropriate handler:
//! DDL → [`ddl`](super::ddl), DML → [`dml`](super::dml),
//! Query → [`query`](super::query).

use super::Executor;
use crate::db::DatabaseError;
use crate::sql::statement::{Create, Statement};
use crate::sql::Value;
use crate::vm::planner::Tuple;

/// Result of executing a statement.
pub(crate) enum ExecResult {
    /// DDL/DML that affected N rows.
    Affected(usize),
    /// Query that produced rows.
    Rows(Vec<Tuple>),
    /// Transaction control — no meaningful return value.
    Ok,
}

impl Executor {
    /// Dispatches a statement to the appropriate handler.
    ///
    /// Manages auto-commit: if no explicit transaction is active,
    /// a short-lived transaction is started and committed on success
    /// (or rolled back on error).
    pub fn execute(&mut self, stmt: Statement) -> Result<ExecResult, DatabaseError> {
        match stmt {
            Statement::StartTransaction => {
                self.begin_transaction()?;
                Ok(ExecResult::Ok)
            }

            Statement::Commit => {
                self.commit()?;
                Ok(ExecResult::Ok)
            }

            Statement::Rollback => {
                self.rollback()?;
                Ok(ExecResult::Ok)
            }

            Statement::Create(Create::Table { name, columns }) => {
                let schema = crate::db::SchemaBuilder::new(name).build_from_ast_columns(&columns);

                self.create_table(schema)?;
                Ok(ExecResult::Affected(0))
            }

            Statement::Select(select) => {
                let (txn_id, auto) = self.auto_txn()?;
                let result = self.execute_select(txn_id, *select);

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                Ok(ExecResult::Rows(result?))
            }

            Statement::Insert(insert) => {
                let (txn_id, auto) = self.auto_txn()?;

                let values: Vec<Tuple> = insert
                    .values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .filter_map(|expr| match expr {
                                crate::sql::statement::Expression::Value(v) => Some(v),
                                _ => None,
                            })
                            .collect()
                    })
                    .collect();

                let result = self.insert(txn_id, &insert.into, values, 1);

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                Ok(ExecResult::Affected(result?))
            }

            Statement::Delete(delete) => {
                let (txn_id, auto) = self.auto_txn()?;
                let mut scan = super::operator::Scan::new(&self.engine, txn_id, &delete.from)?;

                let mut source: Box<dyn super::operator::Operator> = Box::new(scan);

                if let Some(predicate) = delete.r#where {
                    let schema = self.resolve_schema(&delete.from)?;
                    source = Box::new(super::operator::Filter::new(source, schema, predicate));
                }

                let result = self.delete(txn_id, &delete.from, &mut *source);

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                Ok(ExecResult::Affected(result?))
            }

            Statement::Update(update) => {
                let (txn_id, auto) = self.auto_txn()?;
                let schema = self.resolve_schema(&update.table)?;

                let mut source: Box<dyn super::operator::Operator> = Box::new(
                    super::operator::Scan::new(&self.engine, txn_id, &update.table)?,
                );

                if let Some(predicate) = update.r#where {
                    source = Box::new(super::operator::Filter::new(
                        source,
                        schema.clone(),
                        predicate,
                    ));
                }

                let assignments: Vec<(usize, Value)> = update
                    .columns
                    .iter()
                    .filter_map(|assign| {
                        let idx = schema.index_of(&assign.identifier)?;
                        match &assign.value {
                            crate::sql::statement::Expression::Value(v) => Some((idx, v.clone())),
                            _ => None,
                        }
                    })
                    .collect();

                let result = self.update(txn_id, &update.table, &mut *source, &assignments);

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                Ok(ExecResult::Affected(result?))
            }

            _ => Err(DatabaseError::Other(format!(
                "statement not yet supported by the new executor: {stmt:?}"
            ))),
        }
    }
}
