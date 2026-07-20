//! SQL executor module.
//!
//! This replaces the `Planner<File>`-coupled pipeline with a clean executor
//! that talks directly to the MVCC [`Engine`](crate::storage::mvcc::engine::Engine).
//!
//! # Architecture
//!
//! ```text
//! SQL → Parser → Analyzer → Optimizer
//!                               ↓
//!                      ┌────────────────┐
//!                      │    Executor    │
//!                      └────────┬───────┘
//!                               │ dispatches
//!                     ┌─────────┼──────────┐
//!                     ▼         ▼          ▼
//!                   DDL       DML       Query
//!                    │         │          │
//!                    ▼         ▼          ▼
//!                Engine    Engine    Operator pipeline
//!              (create/  (insert/  (Scan → Filter → Project → Sort → Limit)
//!               drop)    update/         │
//!                        delete)         ▼
//!                                  VersionStorage (MVCC-aware scans)
//! ```

#![allow(unused)]

pub(crate) mod ddl;
pub(crate) mod dispatch;
pub(crate) mod dml;
pub(crate) mod operator;
pub(crate) mod query;

use crate::db::DatabaseError;
use crate::storage::mvcc::{engine::Engine, MvccError};
use std::sync::Arc;

/// SQL query executor backed by the MVCC engine.
///
/// Owns an `Arc<Engine>` and manages the transaction lifecycle.
/// Each call runs within an auto-committed transaction unless an explicit
/// transaction is active.
pub(crate) struct Executor {
    engine: Arc<Engine>,
    /// The active explicit transaction, if any (`txn_id`, `begin_seq`).
    active_txn: Option<(i64, i64)>,
    /// Set when a statement fails inside an explicit transaction, the block
    /// then rejects everything except `COMMIT`/`ROLLBACK`.
    aborted: bool,
}

impl Executor {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            engine,
            active_txn: None,
            aborted: false,
        }
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Whether an explicit transaction is in progress.
    pub fn has_active_transaction(&self) -> bool {
        self.active_txn.is_some()
    }

    pub fn begin_transaction(&mut self) -> Result<(), MvccError> {
        if self.active_txn.is_some() {
            return Err(MvccError::Other(
                "cannot start a transaction while there's one in progress".into(),
            ));
        }

        let (txn_id, seq) = self.engine.begin_transaction()?;
        self.active_txn = Some((txn_id, seq));

        Ok(())
    }

    pub fn commit(&mut self) -> Result<(), MvccError> {
        let (txn_id, _) = self.take_txn()?;
        self.engine.commit_transaction(txn_id)
    }

    pub fn rollback(&mut self) -> Result<(), MvccError> {
        let (txn_id, _) = self.take_txn()?;
        self.engine.rollback_transaction(txn_id)
    }

    /// Start a short-lived auto-commit transaction if none is active.
    /// Returns the `txn_id` and whether it should be auto-committed.
    pub fn auto_txn(&mut self) -> Result<(i64, bool), MvccError> {
        match self.active_txn {
            Some((txn_id, _)) => Ok((txn_id, false)),
            None => {
                let (txn_id, _) = self.engine.begin_transaction()?;
                Ok((txn_id, true))
            }
        }
    }

    fn take_txn(&mut self) -> Result<(i64, i64), MvccError> {
        self.active_txn
            .take()
            .ok_or(MvccError::Other("no active transaction".into()))
    }
}

impl From<MvccError> for DatabaseError {
    fn from(err: MvccError) -> Self {
        match err {
            MvccError::Io(io) => DatabaseError::Io(io),
            MvccError::NotOpen => DatabaseError::Other("MVCC engine is not open".into()),
            MvccError::TableNotFound => {
                DatabaseError::Sql(crate::db::SqlError::InvalidTable("(mvcc)".into()))
            }
            MvccError::WriteConflict => {
                DatabaseError::Other("write-write conflict detected".into())
            }
            MvccError::DuplicatedKey(value) => {
                DatabaseError::Sql(crate::db::SqlError::DuplicatedKey(value))
            }
            MvccError::Wal(e) => DatabaseError::Other(format!("WAL error: {e:?}")),
            MvccError::Other(msg) => DatabaseError::Other(msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::{statement::Type, Value};
    use operator::Operator;

    fn setup() -> Executor {
        let engine = Arc::new(Engine::in_memory());
        engine.open().unwrap();

        Executor::new(engine)
    }

    fn create_users_table(exec: &Executor) {
        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .add("name", Type::Text)
            .build();

        exec.create_table(schema).unwrap();
    }

    #[test]
    fn ddl_create_table() {
        let exec = setup();
        create_users_table(&exec);

        assert!(exec.engine().does_table_exists("users").unwrap());
    }

    #[test]
    fn insert_and_scan() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, auto) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(1), Value::String("alice".into())],
                vec![Value::Number(2), Value::String("bob".into())],
            ],
            false,
        )
        .unwrap();

        if auto {
            exec.engine().commit_transaction(txn_id).unwrap();
        }

        let (reader, _auto) = exec.auto_txn().unwrap();
        let mut scan = operator::Scan::new(exec.engine(), reader, "users").unwrap();

        let mut rows = Vec::new();
        while let Some(tuple) = scan.next().unwrap() {
            rows.push(tuple);
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Number(1));
        assert_eq!(rows[0][2], Value::String("alice".into()));
        assert_eq!(rows[1][2], Value::String("bob".into()));
    }

    #[test]
    fn filter_operator() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(1), Value::String("alice".into())],
                vec![Value::Number(2), Value::String("bob".into())],
                vec![Value::Number(3), Value::String("charlie".into())],
            ],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let (reader, _) = exec.auto_txn().unwrap();
        let scan = operator::Scan::new(exec.engine(), reader, "users").unwrap();

        use crate::sql::statement::{BinaryOperator, Column, Expression};
        let mut schema = crate::db::Schema::new(vec![
            Column::new("id", Type::Integer),
            Column::new("name", Type::Text),
        ]);
        schema.prepend_id();

        // WHERE id > 1
        let predicate = Expression::BinaryOperation {
            left: Box::new(Expression::Identifier("id".into())),
            operator: BinaryOperator::Gt,
            right: Box::new(Expression::Value(Value::Number(1))),
        };

        let mut filter = operator::Filter::new(Box::new(scan), schema, predicate);
        let mut rows = Vec::new();
        while let Some(tuple) = filter.next().unwrap() {
            rows.push(tuple);
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][2], Value::String("bob".into()));
        assert_eq!(rows[1][2], Value::String("charlie".into()));
    }

    #[test]
    fn project_operator() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![vec![Value::Number(1), Value::String("alice".into())]],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let (reader, _) = exec.auto_txn().unwrap();
        let scan = operator::Scan::new(exec.engine(), reader, "users").unwrap();

        let mut project = operator::Project::new(Box::new(scan), vec![2]);
        let row = project.next().unwrap().unwrap();

        assert_eq!(row.len(), 1);
        assert_eq!(row[0], Value::String("alice".into()));
        assert!(project.next().unwrap().is_none());
    }

    #[test]
    fn limit_operator() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(1), Value::String("a".into())],
                vec![Value::Number(2), Value::String("b".into())],
                vec![Value::Number(3), Value::String("c".into())],
            ],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let (reader, _) = exec.auto_txn().unwrap();
        let scan = operator::Scan::new(exec.engine(), reader, "users").unwrap();

        // LIMIT 2 OFFSET 1
        let mut limit = operator::Limit::new(Box::new(scan), 2, 1);

        let mut rows = Vec::new();
        while let Some(tuple) = limit.next().unwrap() {
            rows.push(tuple);
        }

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][2], Value::String("b".into()));
        assert_eq!(rows[1][2], Value::String("c".into()));
    }

    #[test]
    fn explicit_transaction_lifecycle() {
        let mut exec = setup();
        create_users_table(&exec);

        exec.begin_transaction().unwrap();
        assert!(exec.has_active_transaction());

        exec.commit().unwrap();
        assert!(!exec.has_active_transaction());

        exec.begin_transaction().unwrap();
        exec.rollback().unwrap();
        assert!(!exec.has_active_transaction());
    }

    #[test]
    fn double_begin_errors() {
        let mut exec = setup();

        exec.begin_transaction().unwrap();
        assert!(exec.begin_transaction().is_err());

        exec.rollback().unwrap();
    }

    #[test]
    fn update_with_where_targets_matched_rows_only() {
        use super::dispatch::ExecResult;
        use crate::sql::statement::{Assignment, BinaryOperator, Expression, Statement, Update};

        let mut exec = setup();
        create_users_table(&exec);

        // user ids deliberately differ from row ids to catch offset bugs
        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(10), Value::String("a".into())],
                vec![Value::Number(20), Value::String("b".into())],
            ],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let stmt = Statement::Update(Update {
            table: "users".into(),
            columns: vec![Assignment {
                identifier: "name".into(),
                value: Expression::Value(Value::String("updated".into())),
            }],
            r#where: Some(Expression::BinaryOperation {
                left: Box::new(Expression::Identifier("id".into())),
                operator: BinaryOperator::Eq,
                right: Box::new(Expression::Value(Value::Number(20))),
            }),
            returning: vec![],
        });

        let ExecResult::Affected(affected) = exec.execute(stmt).unwrap() else {
            panic!("UPDATE must report affected rows");
        };
        assert_eq!(affected, 1);

        let (reader, _) = exec.auto_txn().unwrap();
        let rows = exec.engine().scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].1,
            vec![Value::Number(10), Value::String("a".into())]
        );
        assert_eq!(
            rows[1].1,
            vec![Value::Number(20), Value::String("updated".into())]
        );
    }

    #[test]
    fn delete_with_where_targets_matched_rows_only() {
        use super::dispatch::ExecResult;
        use crate::sql::statement::{BinaryOperator, Delete, Expression, Statement};

        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(10), Value::String("a".into())],
                vec![Value::Number(20), Value::String("b".into())],
            ],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let stmt = Statement::Delete(Delete {
            from: "users".into(),
            r#where: Some(Expression::BinaryOperation {
                left: Box::new(Expression::Identifier("id".into())),
                operator: BinaryOperator::Eq,
                right: Box::new(Expression::Value(Value::Number(10))),
            }),
        });

        let ExecResult::Affected(affected) = exec.execute(stmt).unwrap() else {
            panic!("DELETE must report affected rows");
        };
        assert_eq!(affected, 1);

        let (reader, _) = exec.auto_txn().unwrap();
        let rows = exec.engine().scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
        assert_eq!(rows[0].1[0], Value::Number(20));
    }

    #[test]
    fn execution_error_aborts_explicit_transaction() {
        use super::dispatch::ExecResult;
        use crate::sql::statement::{Expression, Select, Statement, TableRef};

        let mut exec = setup();
        create_users_table(&exec);

        let select = |table: &str| {
            Statement::Select(Select {
                columns: vec![Expression::Wildcard],
                from: TableRef {
                    name: table.into(),
                    alias: None,
                },
                joins: vec![],
                r#where: None,
                order_by: vec![],
                group_by: vec![],
                limit: None,
                offset: None,
            })
        };

        exec.begin_transaction().unwrap();
        assert!(exec.execute(select("missing")).is_err());

        // block is aborted: even valid statements are rejected
        assert!(exec.execute(select("users")).is_err());

        // ROLLBACK clears the aborted state and ends the block
        assert!(matches!(
            exec.execute(Statement::Rollback).unwrap(),
            ExecResult::Ok
        ));
        assert!(!exec.has_active_transaction());

        let (reader, _) = exec.auto_txn().unwrap();
        assert!(exec.engine().scan(reader, "users").is_ok());
    }

    #[test]
    fn select_wildcard() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(1), Value::String("alice".into())],
                vec![Value::Number(2), Value::String("bob".into())],
            ],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let (reader, _) = exec.auto_txn().unwrap();
        use crate::sql::statement::{Expression, Select, TableRef};

        let select = Select {
            columns: vec![Expression::Wildcard],
            from: TableRef {
                name: "users".into(),
                alias: None,
            },
            joins: vec![],
            r#where: None,
            order_by: vec![],
            group_by: vec![],
            limit: None,
            offset: None,
        };

        let (_, rows) = exec.execute_select(reader, select).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1], Value::String("alice".into()));
    }

    #[test]
    fn select_with_projection() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![vec![Value::Number(1), Value::String("alice".into())]],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let (reader, _) = exec.auto_txn().unwrap();
        use crate::sql::statement::{Expression, Select, TableRef};

        let select = Select {
            columns: vec![Expression::Identifier("name".into())],
            from: TableRef {
                name: "users".into(),
                alias: None,
            },
            joins: vec![],
            r#where: None,
            order_by: vec![],
            group_by: vec![],
            limit: None,
            offset: None,
        };

        let (_, rows) = exec.execute_select(reader, select).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
        assert_eq!(rows[0][0], Value::String("alice".into()));
    }

    #[test]
    fn select_order_by_and_limit() {
        let mut exec = setup();
        create_users_table(&exec);

        let (txn_id, _) = exec.auto_txn().unwrap();
        exec.insert(
            txn_id,
            "users",
            vec![
                vec![Value::Number(3), Value::String("charlie".into())],
                vec![Value::Number(1), Value::String("alice".into())],
                vec![Value::Number(2), Value::String("bob".into())],
            ],
            false,
        )
        .unwrap();
        exec.engine().commit_transaction(txn_id).unwrap();

        let (reader, _) = exec.auto_txn().unwrap();
        use crate::sql::statement::{Expression, OrderBy, OrderDirection, Select, TableRef};

        let select = Select {
            columns: vec![Expression::Wildcard],
            from: TableRef {
                name: "users".into(),
                alias: None,
            },
            joins: vec![],
            r#where: None,
            order_by: vec![OrderBy {
                expr: Expression::Identifier("id".into()),
                direction: OrderDirection::Asc,
            }],
            group_by: vec![],
            limit: Some(2),
            offset: None,
        };

        let (_, rows) = exec.execute_select(reader, select).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Number(1));
        assert_eq!(rows[1][0], Value::Number(2));
    }
}
