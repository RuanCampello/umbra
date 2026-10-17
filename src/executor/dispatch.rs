//! Top-level statement dispatch
//!
//! Routes a parsed [`Statement`] to the appropriate handler:
//! DDL -> [`ddl`](super::ddl), DML -> [`dml`](super::dml),
//! Query -> [`query`](super::query)

use super::dml::Target;
use super::{Executor, ddl};
use crate::db::{Column, DatabaseError, Schema, SqlError};
use crate::executor::Tuple;
use crate::sql::Value;
use crate::sql::statement::{self, Create, Drop, Statement, Type};
use crate::vm::expression;

/// Result of executing a statement
pub(crate) enum ExecResult {
    /// DDL/DML that affected N rows
    Affected(usize),
    /// Query that produced rows under the projected schema
    Rows(Schema, Vec<Tuple>),
    /// Transaction control, no meaningful return value
    Ok,
}

impl Executor {
    /// Dispatches a statement to the appropriate handler
    ///
    /// Manages auto-commit: if no explicit transaction is active,
    /// a short-lived transaction is started and committed on success
    /// (or rolled back on error)
    ///
    /// Once a statement fails inside an explicit transaction, the
    /// transaction is aborted: everything except `COMMIT`/`ROLLBACK` and `ROLLBACK TO SAVEPOINT`
    /// is rejected until the block ends
    pub fn execute(&mut self, stmt: Statement) -> Result<ExecResult, DatabaseError> {
        if self.aborted {
            return match stmt {
                Statement::Commit | Statement::Rollback => {
                    self.aborted = false;
                    self.rollback()?;
                    Ok(ExecResult::Ok)
                }
                Statement::RollbackTo(name) => {
                    self.block()?.rollback_to(&name)?;
                    self.aborted = false;
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
            Statement::StartTransaction(level) => {
                self.begin_transaction(level)?;
                Ok(ExecResult::Ok)
            },

            Statement::SetIsolation { level, session } => {
                self.set_isolation(level, session)?;
                Ok(ExecResult::Ok)
            },

            Statement::Checkpoint => {
                self.engine.checkpoint()?;
                Ok(ExecResult::Ok)
            },

            Statement::Commit => {
                self.commit()?;
                Ok(ExecResult::Ok)
            },

            Statement::Rollback => {
                self.rollback()?;
                Ok(ExecResult::Ok)
            },

            Statement::Savepoint(name) => {
                self.block()?.savepoint(&name)?;
                Ok(ExecResult::Ok)
            },

            Statement::RollbackTo(name) => {
                self.block()?.rollback_to(&name)?;
                Ok(ExecResult::Ok)
            },

            Statement::Release(name) => {
                self.block()?.release(&name)?;
                Ok(ExecResult::Ok)
            },

            Statement::Create(Create::Table { name, columns }) => {
                self.create_table(&name, ddl::table_schema(&columns)?)?;
                Ok(ExecResult::Affected(0))
            },

            Statement::Drop(Drop::Table(name)) => {
                self.drop_table(&name)?;
                Ok(ExecResult::Affected(0))
            },

            Statement::Create(Create::Index { name, table, column, unique }) => {
                self.create_index(&name, &table, &column, unique)?;
                Ok(ExecResult::Affected(0))
            },

            Statement::Select(select) => {
                let (schema, tuples) =
                    self.with_txn(|exec, txn| exec.execute_select(txn, select))?;
                Ok(ExecResult::Rows(schema, tuples))
            },

            Statement::Insert(insert) => {
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

                let collect = !insert.returning.is_empty();
                let (count, written) =
                    self.with_txn(|exec, txn| exec.insert(txn, &insert.into, values, collect))?;

                match insert.returning.is_empty() {
                    true => Ok(ExecResult::Affected(count)),
                    _ => {
                        let schema = self.resolve_schema(&insert.into)?;
                        let (schema, rows) =
                            self.evaluate_returning(&schema, &insert.returning, written, 1)?;
                        Ok(ExecResult::Rows(schema, rows))
                    },
                }
            },

            Statement::Delete(delete) => {
                let count = self.with_txn(|exec, txn| {
                    let schema = exec.resolve_schema(&delete.from)?;
                    let target = Target {
                        table: &delete.from,
                        predicate: delete.r#where.as_ref(),
                        schema: &schema,
                    };
                    let mut source =
                        exec.filtered_source(txn, target.table, target.predicate, &schema, None)?;

                    exec.delete(txn, &target, &mut *source)
                })?;

                Ok(ExecResult::Affected(count))
            },

            Statement::Update(update) => {
                let schema = self.resolve_schema(&update.table)?;
                let mut assignments = Vec::with_capacity(update.columns.len());
                for assign in &update.columns {
                    let idx = schema.index_of(&assign.identifier).ok_or_else(|| {
                        DatabaseError::Sql(SqlError::InvalidColumn(assign.identifier.clone()))
                    })?;

                    let mut value = assign.value.clone();
                    expression::bind(&mut value, &schema);
                    assignments.push((idx, value));
                }

                let collect = !update.returning.is_empty();
                let (count, pairs) = self.with_txn(|exec, txn| {
                    let target = Target {
                        table: &update.table,
                        predicate: update.r#where.as_ref(),
                        schema: &schema,
                    };
                    let mut source =
                        exec.filtered_source(txn, target.table, target.predicate, &schema, None)?;

                    exec.update(txn, &target, &mut *source, &assignments, collect)
                })?;

                match update.returning.is_empty() {
                    true => Ok(ExecResult::Affected(count)),
                    _ => {
                        let width = schema.len();
                        let input = schema.update_returning_input();
                        let (schema, rows) =
                            self.evaluate_returning(&input, &update.returning, pairs, width + 1)?;
                        Ok(ExecResult::Rows(schema, rows))
                    },
                }
            },

            Statement::Explain(inner) => {
                let lines = match *inner {
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
                        let msg = "EXPLAIN is meant to work only with SELECT, INSERT, UPDATE and DELETE statements";
                        return Err(DatabaseError::Other(String::from(msg)));
                    },
                };

                let schema = Schema::new(vec![Column::new("Query Plan", Type::Varchar(255))]);
                let tuples =
                    lines.into_iter().map(|line| vec![Value::String(line.into())]).collect();

                Ok(ExecResult::Rows(schema, tuples))
            },

            _ => Err(DatabaseError::Other(format!(
                "statement not yet supported by the new executor: {stmt:?}"
            ))),
        }
    }
}
