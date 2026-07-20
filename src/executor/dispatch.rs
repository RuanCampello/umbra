//! Top-level statement dispatch.
//!
//! Routes a parsed [`Statement`] to the appropriate handler:
//! DDL -> [`ddl`](super::ddl), DML -> [`dml`](super::dml),
//! Query -> [`query`](super::query).

use super::Executor;
use crate::db::{DatabaseError, Schema, SchemaBuilder, SqlError};
use crate::executor::operator;
use crate::sql::statement::{self, Column, Create, Drop, Statement, Type};
use crate::sql::Value;
use crate::vm::planner::Tuple;

/// Result of executing a statement.
pub(crate) enum ExecResult {
    /// DDL/DML that affected N rows.
    Affected(usize),
    /// Query that produced rows under the projected schema.
    Rows(Schema, Vec<Tuple>),
    /// Transaction control, no meaningful return value.
    Ok,
}

impl Executor {
    /// Dispatches a statement to the appropriate handler.
    ///
    /// Manages auto-commit: if no explicit transaction is active,
    /// a short-lived transaction is started and committed on success
    /// (or rolled back on error).
    ///
    /// Once a statement fails inside an explicit transaction, the
    /// transaction is aborted: everything except `COMMIT`/`ROLLBACK`
    /// (both of which roll back) is rejected until the block ends.
    pub fn execute(&mut self, stmt: Statement) -> Result<ExecResult, DatabaseError> {
        if self.aborted {
            return match stmt {
                Statement::Commit | Statement::Rollback => {
                    self.aborted = false;
                    self.rollback()?;
                    Ok(ExecResult::Ok)
                }
                _ => Err(DatabaseError::Other(
                    "current transaction is aborted, commands ignored until end of transaction block"
                        .into(),
                )),
            };
        }

        let result = self.dispatch_statement(stmt);
        if result.is_err() && self.has_active_transaction() {
            self.aborted = true;
        }

        result
    }

    fn dispatch_statement(&mut self, stmt: Statement) -> Result<ExecResult, DatabaseError> {
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
                let schema = SchemaBuilder::new(name).from_ast_columns(&columns);

                self.create_table(schema)?;
                Ok(ExecResult::Affected(0))
            }

            Statement::Drop(Drop::Table(name)) => {
                self.drop_table(&name)?;
                Ok(ExecResult::Affected(0))
            }

            Statement::Create(Create::Index {
                name,
                table,
                column,
                unique,
            }) => {
                self.engine.create_index(&name, &table, &column, unique)?;
                Ok(ExecResult::Affected(0))
            }

            Statement::Select(select) => {
                let (txn_id, auto) = self.auto_txn()?;
                let result = self.execute_select(txn_id, select);

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                let (schema, tuples) = result?;
                Ok(ExecResult::Rows(schema, tuples))
            }

            Statement::Insert(insert) => {
                let (txn_id, auto) = self.auto_txn()?;

                let values: Vec<_> = insert
                    .values
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .filter_map(|expr| match expr {
                                statement::Expression::Value(v) => Some(v),
                                _ => None,
                            })
                            .collect()
                    })
                    .collect();

                let result =
                    self.insert(txn_id, &insert.into, values, !insert.returning.is_empty());

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                let (count, written) = result?;
                match insert.returning.is_empty() {
                    true => Ok(ExecResult::Affected(count)),
                    false => {
                        let schema = self.resolve_schema(&insert.into)?;
                        let (schema, rows) =
                            self.evaluate_returning(&schema, &insert.returning, written, 1)?;
                        Ok(ExecResult::Rows(schema, rows))
                    }
                }
            }

            Statement::Delete(delete) => {
                let (txn_id, auto) = self.auto_txn()?;
                let schema = self.resolve_schema(&delete.from)?;
                let mut source =
                    self.filtered_source(txn_id, &delete.from, delete.r#where.as_ref(), &schema)?;

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
                let mut source =
                    self.filtered_source(txn_id, &update.table, update.r#where.as_ref(), &schema)?;

                let mut assignments = Vec::with_capacity(update.columns.len());
                for assign in &update.columns {
                    let idx = schema.index_of(&assign.identifier).ok_or_else(|| {
                        DatabaseError::Sql(SqlError::InvalidColumn(assign.identifier.clone()))
                    })?;

                    assignments.push((idx, assign.value.clone()));
                }

                let result = self.update(
                    txn_id,
                    &update.table,
                    &mut *source,
                    &assignments,
                    &schema,
                    !update.returning.is_empty(),
                );

                if auto {
                    match &result {
                        Ok(_) => self.engine.commit_transaction(txn_id)?,
                        Err(_) => {
                            let _ = self.engine.rollback_transaction(txn_id);
                        }
                    }
                }

                let (count, pairs) = result?;
                match update.returning.is_empty() {
                    true => Ok(ExecResult::Affected(count)),
                    false => {
                        let width = schema.len();
                        let input = schema.update_returning_input();
                        let (schema, rows) =
                            self.evaluate_returning(&input, &update.returning, pairs, width + 1)?;
                        Ok(ExecResult::Rows(schema, rows))
                    }
                }
            }

            Statement::Explain(inner) => {
                let lines = match &*inner {
                    Statement::Select(select) => self.explain_select(select)?,
                    Statement::Insert(insert) => vec![format!("Insert into {}", insert.into)],
                    Statement::Update(update) => vec![
                        self.access_path(&update.table, update.r#where.as_ref())?,
                        format!("Update on {}", update.table),
                    ],
                    Statement::Delete(delete) => vec![
                        self.access_path(&delete.from, delete.r#where.as_ref())?,
                        format!("Delete from {}", delete.from),
                    ],
                    _ => {
                        return Err(DatabaseError::Other(String::from(
                            "EXPLAIN is meant to work only with SELECT, INSERT, UPDATE and DELETE statements",
                        )))
                    }
                };

                let schema = Schema::new(vec![Column::new("Query Plan", Type::Varchar(255))]);
                let tuples = lines
                    .into_iter()
                    .map(|line| vec![Value::String(line)])
                    .collect();

                Ok(ExecResult::Rows(schema, tuples))
            }

            _ => Err(DatabaseError::Other(format!(
                "statement not yet supported by the new executor: {stmt:?}"
            ))),
        }
    }
}
