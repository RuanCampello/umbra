//! SQL executor: runs analysed statements against the storage [Engine](crate::storage::engine::Engine)
//!
//! ```text
//! SQL → Parser → Analyzer → Optimiser → Executor ─┬─ DDL   → Engine (catalogue)
//!                                                 ├─ DML   → Transaction (insert/update/delete)
//!                                                 └─ Query → operators (Scan → Filter → Sort → …)
//! ```
//!
//! Every statement runs in the session's explicit transaction or, when there is none, in its own
//! auto-committed one

mod aggregate;
pub(crate) mod ddl;
pub(crate) mod dispatch;
pub(crate) mod dml;
pub(crate) mod operator;
pub(crate) mod query;

use crate::db::DatabaseError;
use crate::sql::Value;
use crate::storage::engine::Engine;
use crate::storage::engine::error::EngineError;
use crate::storage::engine::table::{Table, View};
use crate::storage::engine::transaction::{IsolationLevel, Transaction};
use std::sync::Arc;

/// Executes statements for one session
pub(crate) struct Executor {
    engine: Engine,
    /// The explicit transaction, if one is open
    txn: Option<Transaction>,
    /// Level of transactions this session starts by default
    level: IsolationLevel,
    /// `SET TRANSACTION` override for the next (or current, before its first statement) one
    next_level: Option<IsolationLevel>,
    /// Statements run in the explicit transaction so far
    statements: usize,
    /// Set when a statement fails inside an explicit transaction, the block
    /// then rejects everything except `COMMIT`/`ROLLBACK`
    aborted: bool,
    /// The catalogue view of the transaction running the current statement
    /// table names resolve against the newest committed state outside one
    catalogue: Option<View>,
}

/// a result row
/// the row id leads unless a projection dropped it
pub(crate) type Tuple = Vec<Value>;

impl Executor {
    pub fn new(engine: Engine) -> Self {
        Self {
            engine,
            txn: None,
            level: IsolationLevel::default(),
            next_level: None,
            statements: 0,
            aborted: false,
            catalogue: None,
        }
    }

    /// the table `name` denotes for the running statement
    pub fn table(&self, name: &str) -> Result<Arc<Table>, EngineError> {
        match self.catalogue {
            Some(view) => self.engine.table_in(name, view),
            None => self.engine.table(name),
        }
    }

    /// the table `name` denotes for the session: in its explicit transaction, if one is open
    pub fn session_table(&self, name: &str) -> Result<Arc<Table>, EngineError> {
        match &self.txn {
            Some(txn) => txn.table(name),
            None => self.engine.table(name),
        }
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// whether an explicit transaction is in progress
    pub fn has_active_transaction(&self) -> bool {
        self.txn.is_some()
    }

    pub fn begin_transaction(
        &mut self,
        level: Option<IsolationLevel>,
    ) -> Result<(), DatabaseError> {
        let msg = "cannot start a transaction while there's one in progress";
        if self.txn.is_some() {
            return Err(DatabaseError::Other(msg.into()));
        }

        let level = level.or(self.next_level.take()).unwrap_or(self.level);
        self.txn = Some(self.engine.begin(level));
        Ok(self.statements = 0)
    }

    pub fn commit(&mut self) -> Result<(), DatabaseError> {
        self.take_txn()?.commit().map_err(DatabaseError::from)
    }

    pub fn rollback(&mut self) -> Result<(), DatabaseError> {
        Ok(self.take_txn()?.rollback())
    }

    /// `SET TRANSACTION ISOLATION LEVEL`: for the current transaction while it has run nothing, otherwise for the next one
    /// `session` changes the default instead
    pub fn set_isolation(
        &mut self,
        level: IsolationLevel,
        session: bool,
    ) -> Result<(), DatabaseError> {
        if session {
            self.level = level;
            return Ok(());
        }

        match &self.txn {
            Some(_) if self.statements > 0 => Err(DatabaseError::Other(
                "SET TRANSACTION ISOLATION LEVEL must be called before any query".into(),
            )),
            Some(_) => Ok(self.txn = Some(self.engine.begin(level))),
            None => Ok(self.next_level = Some(level)),
        }
    }

    /// runs `run` in the explicit transaction, or in a fresh one committed on success and rolled back on failure
    pub fn with_txn<R>(
        &mut self,
        run: impl FnOnce(&Self, &mut Transaction) -> Result<R, DatabaseError>,
    ) -> Result<R, DatabaseError> {
        match self.txn.take() {
            Some(mut txn) => {
                if txn.level() == IsolationLevel::ReadCommitted {
                    txn.refresh_snapshot();
                }

                self.statements += 1;
                self.catalogue = Some(txn.catalogue_view());
                let result = run(self, &mut txn);
                self.catalogue = None;
                self.txn = Some(txn);
                result
            },
            _ => {
                let level = self.next_level.take().unwrap_or(self.level);
                let mut txn = self.engine.begin(level);

                self.catalogue = Some(txn.catalogue_view());
                let result = run(self, &mut txn);
                self.catalogue = None;

                match result {
                    Ok(result) => {
                        txn.commit()?;
                        Ok(result)
                    },
                    Err(error) => {
                        txn.rollback();
                        Err(error)
                    },
                }
            },
        }
    }

    /// the explicit transaction, which savepoints need
    fn block(&mut self) -> Result<&mut Transaction, DatabaseError> {
        self.txn.as_mut().ok_or_else(|| {
            DatabaseError::Other("savepoints can only be used in transaction blocks".into())
        })
    }

    fn take_txn(&mut self) -> Result<Transaction, DatabaseError> {
        self.txn
            .take()
            .ok_or_else(|| DatabaseError::Other("no active transaction".into()))
    }
}

impl From<EngineError> for DatabaseError {
    fn from(error: EngineError) -> Self {
        use crate::db::SqlError;
        use EngineError::*;

        match error {
            Io(io) => DatabaseError::Io(io),
            Corrupt(message) => DatabaseError::Corrupted(message),
            TableNotFound(table) => DatabaseError::Sql(SqlError::InvalidTable(table)),
            ColumnNotFound(column) => DatabaseError::Sql(SqlError::InvalidColumn(column)),
            DuplicatedKey(value) => DatabaseError::Sql(SqlError::DuplicatedKey(value)),
            error @ (WriteConflict | SerializationFailure) => {
                DatabaseError::Conflict(error.to_string())
            },
            error => DatabaseError::Other(error.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::dispatch::ExecResult;
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::statement::{
        Assignment, BinaryOperator, Delete, Expression, OrderBy, OrderDirection, Select, Statement,
        TableRef, Type, Update,
    };
    use operator::Operator;

    fn setup() -> Executor {
        let mut exec = Executor::new(Engine::in_memory());
        let schema = SchemaBuilder::new()
            .primary("id", Type::Integer)
            .add("name", Type::Text)
            .build();
        exec.create_table("users", schema).unwrap();
        exec
    }

    fn seed(exec: &mut Executor, rows: &[(i128, &str)]) {
        let values = rows
            .iter()
            .map(|&(id, name)| vec![Value::Number(id), Value::String(name.into())])
            .collect();
        exec.with_txn(|exec, txn| exec.insert(txn, "users", values, false)).unwrap();
    }

    fn scan(exec: &mut Executor) -> Vec<Tuple> {
        exec.with_txn(|exec, txn| {
            let table = exec.engine.table("users")?;
            let mut scan = operator::Scan::new(txn.scan(&table), None);
            let mut rows = Vec::new();
            while let Some(tuple) = scan.next()? {
                rows.push(tuple);
            }
            Ok(rows)
        })
        .unwrap()
    }

    fn select(table: &str) -> Select {
        Select {
            columns: vec![Expression::Wildcard],
            from: TableRef { name: table.into(), alias: None },
            joins: vec![],
            r#where: None,
            order_by: vec![],
            group_by: vec![],
            limit: None,
            offset: None,
        }
    }

    fn id_equals(id: i128) -> Expression {
        Expression::BinaryOperation {
            left: Box::new(Expression::Identifier("id".into())),
            operator: BinaryOperator::Eq,
            right: Box::new(Expression::Value(Value::Number(id))),
        }
    }

    #[test]
    fn scan_filter_project_and_limit_operators() {
        let mut exec = setup();
        seed(&mut exec, &[(1, "alice"), (2, "bob"), (3, "charlie")]);

        let rows = scan(&mut exec);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0][2], Value::String("alice".into()));

        let mut schema = crate::db::Schema::new(vec![
            crate::db::Column::new("id", Type::Integer),
            crate::db::Column::new("name", Type::Text),
        ]);
        schema.prepend_id();

        let names = exec
            .with_txn(|exec, txn| {
                let table = exec.engine.table("users")?;
                let source = Box::new(operator::Scan::new(txn.scan(&table), None));
                let predicate = Expression::BinaryOperation {
                    left: Box::new(Expression::Identifier("id".into())),
                    operator: BinaryOperator::Gt,
                    right: Box::new(Expression::Value(Value::Number(1))),
                };
                let filter = Box::new(operator::Filter::new(source, schema, predicate));
                let project = Box::new(operator::Project::new(filter, vec![2]));
                let mut limit = operator::Limit::new(project, 1, 1);

                let mut rows = Vec::new();
                while let Some(tuple) = limit.next()? {
                    rows.push(tuple);
                }
                Ok(rows)
            })
            .unwrap();

        assert_eq!(names, [vec![Value::String("charlie".into())]]);
    }

    #[test]
    fn explicit_transaction_lifecycle() {
        let mut exec = setup();

        exec.begin_transaction(None).unwrap();
        assert!(exec.has_active_transaction());
        assert!(exec.begin_transaction(None).is_err());

        exec.commit().unwrap();
        assert!(!exec.has_active_transaction());

        exec.begin_transaction(None).unwrap();
        exec.rollback().unwrap();
        assert!(!exec.has_active_transaction());
    }

    #[test]
    fn update_and_delete_target_matched_rows_only() {
        let mut exec = setup();
        // user ids deliberately differ from row ids to catch offset bugs
        seed(&mut exec, &[(10, "a"), (20, "b"), (30, "c")]);

        let update = Statement::Update(Update {
            table: "users".into(),
            columns: vec![Assignment {
                identifier: "name".into(),
                value: Expression::Value(Value::String("updated".into())),
            }],
            r#where: Some(id_equals(20)),
            returning: vec![],
        });
        assert!(matches!(exec.execute(update).unwrap(), ExecResult::Affected(1)));

        let delete =
            Statement::Delete(Delete { from: "users".into(), r#where: Some(id_equals(10)) });
        assert!(matches!(exec.execute(delete).unwrap(), ExecResult::Affected(1)));

        let rows = scan(&mut exec);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1..], [Value::Number(20), Value::String("updated".into())]);
        assert_eq!(rows[1][1..], [Value::Number(30), Value::String("c".into())]);
    }

    #[test]
    fn execution_error_aborts_explicit_transaction() {
        let mut exec = setup();

        exec.begin_transaction(None).unwrap();
        assert!(exec.execute(Statement::Select(select("missing"))).is_err());
        assert!(exec.execute(Statement::Select(select("users"))).is_err());

        assert!(matches!(exec.execute(Statement::Rollback).unwrap(), ExecResult::Ok));
        assert!(!exec.has_active_transaction());
        assert!(exec.execute(Statement::Select(select("users"))).is_ok());
    }

    #[test]
    fn select_projection_order_and_limit() {
        let mut exec = setup();
        seed(&mut exec, &[(3, "charlie"), (1, "alice"), (2, "bob")]);

        let mut ordered = select("users");
        ordered.order_by = vec![OrderBy {
            expr: Expression::Identifier("id".into()),
            direction: OrderDirection::Asc,
        }];
        ordered.limit = Some(2);

        let (_, rows) = exec.with_txn(|exec, txn| exec.execute_select(txn, ordered)).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Number(1));
        assert_eq!(rows[1][0], Value::Number(2));

        let mut projected = select("users");
        projected.columns = vec![Expression::Identifier("name".into())];
        let (_, rows) = exec.with_txn(|exec, txn| exec.execute_select(txn, projected)).unwrap();
        assert_eq!(rows.len(), 3);
        assert!(rows.iter().all(|row| row.len() == 1));
    }

    #[test]
    fn set_transaction_applies_before_the_first_statement_only() {
        let mut exec = setup();

        exec.begin_transaction(None).unwrap();
        exec.set_isolation(IsolationLevel::Serializable, false).unwrap();
        assert_eq!(exec.txn.as_ref().unwrap().level(), IsolationLevel::Serializable);

        exec.execute(Statement::Select(select("users"))).unwrap();
        assert!(exec.set_isolation(IsolationLevel::ReadCommitted, false).is_err());
        exec.rollback().unwrap();

        exec.set_isolation(IsolationLevel::ReadCommitted, false).unwrap();
        exec.begin_transaction(None).unwrap();
        assert_eq!(exec.txn.as_ref().unwrap().level(), IsolationLevel::ReadCommitted);
        exec.rollback().unwrap();

        exec.begin_transaction(None).unwrap();
        assert_eq!(exec.txn.as_ref().unwrap().level(), IsolationLevel::Snapshot);
        exec.rollback().unwrap();
    }
}
