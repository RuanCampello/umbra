//! MVCC-backed database session

use crate::db::{
    self, Context, Ctx, DatabaseError, QuerySet, Result, Schema, SqlError, TableMetadata,
};
use crate::executor::Executor;
use crate::executor::dispatch::ExecResult;
use crate::sql;
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::Parser;
use crate::sql::statement::{Statement, Value};
use crate::storage::engine::{Config, Engine};
use std::path::Path;

pub struct MvccDatabase {
    executor: Executor,
    /// Caches [TableMetadata] synthesised from the engine's schemas for the
    /// analyzer/prepare pipeline
    context: Context,
    /// Engine schema epoch this session's [Context] was last synced against
    /// a bump means another connection changed the catalogue and the cache is stale
    epoch: u64,
    /// Whether this session owns the engine's lifecycle
    /// Sessions spawned per connection over a shared engine leave closing to the owner
    owns_engine: bool,
}

impl MvccDatabase {
    /// opens (or creates) a durable database rooted at `dir`, recovering state from snapshots and the WAL
    pub fn init(dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::with_engine(Self::open_engine(dir, None)?, true))
    }

    /// [MvccDatabase::init] keeping at most `chunks` chunks of 1024 rows in memory
    /// colder settled chunks are evicted to disk and reloaded on demand
    pub fn init_with_budget(dir: impl AsRef<Path>, chunks: usize) -> Result<Self> {
        Ok(Self::with_engine(Self::open_engine(dir, Some(chunks))?, true))
    }

    pub fn in_memory() -> Result<Self> {
        let engine = Engine::in_memory();
        engine.start_maintenance(Config::default().maintenance_interval);

        Ok(Self::with_engine(engine, true))
    }

    /// opens and recovers a durable engine, returning the shared handle so
    /// several sessions can be spawned over it with [MvccDatabase::connect]
    pub(crate) fn open_engine(
        dir: impl AsRef<Path>,
        max_resident_chunks: Option<usize>,
    ) -> Result<Engine> {
        let config = Config { max_resident_chunks, ..Config::default() };
        let engine = Engine::open(dir, config.clone())?;
        engine.start_maintenance(config.maintenance_interval);

        Ok(engine)
    }

    /// spawns a session over an already-open shared engine
    /// the session does not own the engine's lifecycle, so dropping it only rolls back its own
    /// transaction
    pub(crate) fn connect(engine: Engine) -> Self {
        Self::with_engine(engine, false)
    }

    /// opens another session over this session's engine, for concurrent use
    /// from another thread
    /// it shares all committed state, keeps its own transaction context, and does not own the engine's lifecycle
    pub fn session(&self) -> Self {
        Self::with_engine(self.executor.engine().clone(), false)
    }

    /// the shared engine handle, for spawning further sessions over it
    #[cfg(test)]
    pub(crate) fn engine(&self) -> Engine {
        self.executor.engine().clone()
    }

    fn with_engine(engine: Engine, owns_engine: bool) -> Self {
        let epoch = engine.epoch();

        Self {
            executor: Executor::new(engine),
            context: Context::with_size(db::DEFAULT_CACHE_SIZE),
            epoch,
            owns_engine,
        }
    }

    /// drops cached metadata when another connection has changed the
    /// catalogue since this session last looked
    fn sync_schema_epoch(&mut self) {
        let epoch = self.executor.engine().epoch();
        if epoch != self.epoch {
            self.context = Context::with_size(db::DEFAULT_CACHE_SIZE);
            self.epoch = epoch;
        }
    }

    /// Executes every statement of a `.sql` file
    pub fn load(&mut self, path: impl AsRef<std::path::Path>) -> Result<()> {
        if path.as_ref().extension().and_then(|s| s.to_str()) != Some("sql") {
            return Err(DatabaseError::Other("File must have a .sql extension".to_string()));
        }

        let content = std::fs::read_to_string(path).map_err(DatabaseError::Io)?;
        let statements = Parser::new(&content).try_parse()?;

        self.sync_schema_epoch();
        for statement in statements {
            let statement = sql::process_statement(statement, self)?;
            self.execute(statement)?;
            self.sync_schema_epoch();
        }

        Ok(())
    }

    /// runs `statement`, dropping cached metadata whenever it may change the catalogue this session
    /// sees: its own DDL, and the end of a transaction that may hold some (on success or failure)
    fn execute(&mut self, statement: Statement) -> Result<ExecResult> {
        let catalogue = matches!(
            statement,
            Statement::Create(_)
                | Statement::Drop(_)
                | Statement::Commit
                | Statement::Rollback
                | Statement::RollbackTo(_)
        );
        let result = self.executor.execute(statement);
        if catalogue {
            self.context = Context::with_size(db::DEFAULT_CACHE_SIZE);
        }

        result
    }

    pub fn exec(&mut self, input: &str) -> Result<QuerySet> {
        self.sync_schema_epoch();
        let statement = sql::pipeline(input, self)?;

        if let Statement::Source(path) = statement {
            self.load(path)?;
            return Ok(QuerySet::empty());
        }

        let result = self.execute(statement)?;
        self.sync_schema_epoch();

        Ok(match result {
            ExecResult::Rows(schema, tuples) => {
                // an estimate is enough for the guard; exact sizes are for the wire protocol
                let mut total_size = 0;
                for tuple in &tuples {
                    total_size += tuple.iter().map(sql::Value::serialised_size_hint).sum::<usize>();
                    if total_size > 1 << 30 {
                        if self.executor.has_active_transaction() {
                            let _ = self.executor.rollback();
                        }
                        return Err(DatabaseError::NoMemory);
                    }
                }

                let mut set = QuerySet::new(schema, tuples);
                set.display_enums();
                set
            },
            // the wire protocol reads the affected-row count from a schema-less single value
            ExecResult::Affected(count) => {
                QuerySet::new(Schema::empty(), vec![vec![Value::Number(count as i128)]])
            },
            ExecResult::Ok => QuerySet::empty(),
        })
    }

    pub fn active_transaction(&self) -> bool {
        self.executor.has_active_transaction()
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.executor.rollback()?;
        Ok(())
    }

    /// rolls back any open transaction and, when this session owns the engine,
    /// shuts it down cleanly
    /// shared sessions leave the engine to its owner
    pub fn close(&mut self) -> Result<()> {
        if self.executor.has_active_transaction() {
            self.executor.rollback()?;
        }

        if self.owns_engine {
            self.executor.engine().close()?;
        }
        Ok(())
    }
}

impl Drop for MvccDatabase {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Ctx for MvccDatabase {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata> {
        if !self.context.contains(table) {
            let handle = self
                .executor
                .session_table(table)
                .map_err(|_| DatabaseError::Sql(SqlError::InvalidTable(table.into())))?;

            self.context.insert(TableMetadata {
                name: handle.name().to_string(),
                schema: handle.schema.clone(),
                indexes: Vec::new(),
            });
        }

        self.context.metadata(table)
    }

    fn next_serial(&mut self, table: &str, column: usize) -> Result<i128> {
        let handle = self
            .executor
            .session_table(table)
            .map_err(|_| DatabaseError::Sql(SqlError::InvalidTable(table.into())))?;
        let value = handle.next_serial(column) as i128;
        let data_type = handle.schema.columns[column].column_type();
        let max = data_type.max();

        match value > max as i128 {
            true => Err(AnalyzerError::Overflow(data_type, max).into()),
            false => Ok(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Value;

    #[test]
    fn sessions_observe_each_others_schema_changes() {
        let mut owner = MvccDatabase::in_memory().unwrap();
        let mut session = MvccDatabase::connect(owner.engine());

        owner.exec("CREATE TABLE t (id SERIAL PRIMARY KEY, v INT);").unwrap();

        // the session predates the table; the schema epoch bump makes it visible
        session.exec("INSERT INTO t (v) VALUES (10);").unwrap();
        let rows = owner.exec("SELECT v FROM t;").unwrap();
        assert_eq!(rows.tuples.len(), 1);
        assert_eq!(rows.tuples[0][0], Value::Number(10));

        owner.exec("DROP TABLE t;").unwrap();
        assert!(
            session.exec("INSERT INTO t (v) VALUES (20);").is_err(),
            "a dropped table is gone for the other session too"
        );
    }

    #[test]
    fn concurrent_sessions_never_collide_on_serials() {
        let mut owner = MvccDatabase::in_memory().unwrap();
        owner.exec("CREATE TABLE t (id SERIAL PRIMARY KEY, who INT);").unwrap();
        let engine = owner.engine();

        let handles: Vec<_> = (0..4)
            .map(|who| {
                let mut session = MvccDatabase::connect(engine.clone());
                std::thread::spawn(move || {
                    for _ in 0..50 {
                        session.exec(&format!("INSERT INTO t (who) VALUES ({who});")).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let rows = owner.exec("SELECT id FROM t ORDER BY id;").unwrap();
        assert_eq!(rows.tuples.len(), 200, "every insert committed");

        let mut ids: Vec<_> = rows.tuples.iter().map(|row| row[0].clone()).collect();
        ids.dedup();
        assert_eq!(ids.len(), 200, "no serial value handed out twice");
    }
}
