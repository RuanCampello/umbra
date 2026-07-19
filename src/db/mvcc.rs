//! MVCC-backed database session.

use crate::collections::hash::HashMap;
use crate::db::metadata::{sequence, SequenceMetadata};
use crate::db::{
    self, Context, Ctx, DatabaseError, QuerySet, Result, Schema, SchemaNew, SqlError, TableMetadata,
};
use crate::executor::dispatch::ExecResult;
use crate::executor::Executor;
use crate::sql::parser::Parser;
use crate::sql::statement::Statement;
use crate::storage::mvcc::engine::{Config, Engine};
use crate::{sql, storage};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

pub struct MvccDatabase {
    executor: Executor,
    /// Caches [TableMetadata] synthesised from the engine's schemas for the
    /// analyzer/prepare pipeline
    context: Context,
}

impl MvccDatabase {
    /// Opens (or creates) a durable database rooted at `dir`, recovering
    /// state from snapshots and the WAL.
    pub fn init(dir: impl AsRef<Path>) -> Result<Self> {
        let path = dir.as_ref().to_string_lossy().into_owned();
        let engine = Arc::new(Engine::new(Config::durable(path)));

        engine.open()?;
        engine.cleanup();

        Ok(Self::with_engine(engine))
    }

    pub fn in_memory() -> Result<Self> {
        let engine = Arc::new(Engine::in_memory());
        engine.open()?;

        Ok(Self::with_engine(engine))
    }

    fn with_engine(engine: Arc<Engine>) -> Self {
        Self {
            executor: Executor::new(engine),
            context: Context::with_size(crate::db::DEFAULT_CACHE_SIZE),
        }
    }

    /// Executes every statement of a `.sql` file
    pub fn load(&mut self, path: impl AsRef<std::path::Path>) -> Result<()> {
        if path.as_ref().extension().and_then(|s| s.to_str()) != Some("sql") {
            return Err(DatabaseError::Other(
                "File must have a .sql extension".to_string(),
            ));
        }

        let content = std::fs::read_to_string(path).map_err(DatabaseError::Io)?;
        let statements = Parser::new(&content).try_parse()?;

        for statement in statements {
            let statement = sql::process_statement(statement, self)?;
            let invalidates = matches!(statement, Statement::Create(_) | Statement::Drop(_));

            self.executor.execute(statement)?;
            if invalidates {
                self.context = Context::with_size(db::DEFAULT_CACHE_SIZE);
            }
        }

        Ok(())
    }

    pub fn exec(&mut self, input: &str) -> Result<QuerySet> {
        let statement = sql::pipeline(input, self)?;

        if let Statement::Source(path) = statement {
            self.load(path)?;
            return Ok(QuerySet::empty());
        }

        let invalidates = matches!(statement, Statement::Create(_) | Statement::Drop(_));

        let result = self.executor.execute(statement)?;
        if invalidates {
            self.context = Context::with_size(crate::db::DEFAULT_CACHE_SIZE);
        }

        Ok(match result {
            ExecResult::Rows(schema, tuples) => {
                let mut total_size = 0;
                for tuple in &tuples {
                    total_size += storage::tuple::size_of(tuple, &schema);
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
            }
            ExecResult::Affected(_) | ExecResult::Ok => QuerySet::empty(),
        })
    }

    pub fn active_transaction(&self) -> bool {
        self.executor.has_active_transaction()
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.executor.rollback()?;
        Ok(())
    }

    /// rolls back any open transaction and shuts the engine down cleanly
    pub fn close(&mut self) -> Result<()> {
        if self.executor.has_active_transaction() {
            self.executor.rollback()?;
        }

        self.executor.engine().close()?;
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
            let engine = self.executor.engine();
            let schema = engine
                .schema(table)
                .map_err(|_| DatabaseError::Sql(SqlError::InvalidTable(table.into())))?;

            let mut metadata = synthesise_metadata(&schema);

            // serial columns draw from sequences seeded with the committed max
            for (idx, col) in schema.columns.iter().enumerate() {
                if !col.column_type().is_serial() {
                    continue;
                }

                let current = engine.max_column_value(table, idx)?.unwrap_or(0).max(0) as u64;

                let name = sequence!(sequence on (table) (col.name()));
                metadata.serials.insert(
                    name.clone(),
                    SequenceMetadata {
                        root: 0,
                        name,
                        value: AtomicU64::new(current),
                        data_type: col.column_type(),
                    },
                );
            }

            self.context.insert(metadata);
        }

        self.context.metadata(table)
    }
}

fn synthesise_metadata(schema: &SchemaNew) -> TableMetadata {
    TableMetadata {
        root: 0,
        name: schema.name.clone(),
        schema: Schema::from(schema),
        indexes: Vec::new(),
        serials: HashMap::default(),
        row_id: 0,
        count: 0,
    }
}
