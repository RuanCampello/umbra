//! The MVCC storage engine of Umbra

pub(crate) mod checkpoint;
pub(crate) mod chunk;
pub(crate) mod clock;
pub(crate) mod codec;
pub(crate) mod error;
pub(crate) mod index;
pub(crate) mod io;
pub(crate) mod log;
pub(crate) mod record;
pub(crate) mod table;
pub(crate) mod transaction;

use crate::collections::hash::HashMap;
use crate::db::Schema;
use crate::index;
use crate::sql::Value;
use crate::storage::engine::checkpoint::Manifest;
use crate::storage::engine::chunk::Cold;
use crate::storage::engine::clock::Clock;
use crate::storage::engine::error::{EngineError, Result};
use crate::storage::engine::index::{Holder, Index};
use crate::storage::engine::io::{DirectoryLock, OsStorage, Storage};
use crate::storage::engine::log::{Kind, Log, SyncMode};
use crate::storage::engine::record::{Change, Ddl};
use crate::storage::engine::table::{Table, View};
use crate::storage::engine::transaction::{Commits, IsolationLevel, Transaction};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

/// Shared handle to one database
#[derive(Clone)]
pub(crate) struct Engine {
    inner: Arc<Inner>,
}

pub(in crate::storage::engine) struct Inner {
    clock: Clock,
    /// The commit critical section: timestamps, commit summaries, SERIALIZABLE snapshots
    commits: Mutex<Commits>,
    catalog: RwLock<HashMap<String, Arc<Table>>>,
    next_table_id: AtomicU32,
    /// Bumped on every catalogue change so sessions can drop cached metadata
    epoch: AtomicU64,
    /// `None` for in-memory databases
    log: Option<Log>,
    disk: Option<Disk>,
    /// Cold-chunk store, when a memory budget is configured
    cold: Option<Arc<Cold>>,
    maintenance: Mutex<Option<Maintenance>>,
}

/// Where and how a durable database lives
struct Disk {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    config: Config,
    /// `(C, start lsn)` of the current checkpoint
    /// held while one is being taken
    checkpoint: Mutex<(u64, u64)>,
    _lock: Option<DirectoryLock>,
}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    pub sync: SyncMode,
    pub segment_size: u64,
    pub checkpoint_bytes: u64,
    pub maintenance_interval: Duration,
    /// Chunks (1024 rows each) kept in memory before the coldest settled ones are evicted to disk
    /// `None` keeps everything resident
    pub max_resident_chunks: Option<usize>,
}

struct Maintenance {
    stop: Arc<(Mutex<bool>, Condvar)>,
    handle: JoinHandle<()>,
}

/// Tables rebuilt during recovery, by id
#[derive(Default)]
struct Recovered {
    tables: HashMap<u32, Arc<Table>>,
    last_ts: u64,
    max_table_id: u32,
    lsn: u64,
    cold: Option<Arc<Cold>>,
}

impl Engine {
    pub fn in_memory() -> Self {
        Self::assemble(Recovered::default(), None, None)
    }

    /// opens (creating if needed) the database in `dir`, recovering its committed state
    pub fn open(dir: impl AsRef<Path>, config: Config) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let lock = DirectoryLock::acquire(dir)?;

        Self::open_with(Arc::new(OsStorage), dir, config, Some(lock))
    }

    /// [Engine::open] over any storage
    pub fn open_with(
        storage: Arc<dyn Storage>,
        dir: &Path,
        config: Config,
        lock: Option<DirectoryLock>,
    ) -> Result<Self> {
        if !storage.exists(dir) {
            storage.create_dir(dir)?;
            if let Some(parent) = dir.parent() {
                storage.sync_dir(parent)?;
            }
        }

        let cold_dir = dir.join("cold");
        if storage.exists(&cold_dir) {
            storage.remove_dir(&cold_dir)?;
        }

        let cold = config
            .max_resident_chunks
            .map(|max| Arc::new(Cold::new(Arc::clone(&storage), cold_dir, max)));

        let mut recovered = Recovered { cold: cold.clone(), ..Recovered::default() };
        let manifest = Manifest::read(storage.as_ref(), dir)?;

        if let Some(manifest) = &manifest {
            for &id in &manifest.tables {
                let path = checkpoint::image_path(dir, manifest.ts, id);
                let table = checkpoint::read_image(
                    storage.as_ref(),
                    &path,
                    |id, name, schema| new_table(id, name, schema, cold.clone()),
                    |table| {
                        if let Some(cold) = &cold {
                            evict_coldest(cold, std::slice::from_ref(table), manifest.ts);
                        }
                    },
                )?;
                recovered.max_table_id = recovered.max_table_id.max(id);
                recovered.tables.insert(id, table);
            }
            recovered.last_ts = manifest.ts;
        }

        let from = manifest.as_ref().map_or(0, |manifest| manifest.start_lsn);
        let (sync, segment_size) = (config.sync, config.segment_size);
        let (log, replay) = Log::open(
            Arc::clone(&storage),
            dir.join("wal"),
            sync,
            segment_size,
            from,
            |kind, payload| recovered.apply(kind, payload),
        )?;

        recovered.build_indexes();
        recovered.tables.values().for_each(|table| table.rebuild_free());
        recovered.lsn = replay.end;

        if let Some(cold) = &cold {
            let tables: Vec<_> = recovered.tables.values().cloned().collect();
            evict_coldest(cold, &tables, recovered.last_ts);
        }

        let checkpoint = Mutex::new(manifest.map_or((0, 0), |m| (m.ts, m.start_lsn)));
        let dir = dir.to_path_buf();
        let disk = Disk { storage, dir, config, checkpoint, _lock: lock };

        Ok(Self::assemble(recovered, Some(log), Some(disk)))
    }

    pub fn begin(&self, level: IsolationLevel) -> Transaction {
        Transaction::new(Arc::clone(&self.inner), level)
    }

    pub fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.inner
            .catalog
            .read()
            .expect("catalogue poisoned")
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))
    }

    pub fn tables(&self) -> Vec<Arc<Table>> {
        self.inner
            .catalog
            .read()
            .expect("catalogue poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// where sorts spill: beside the data for durable databases, the system temporary directory
    /// otherwise
    /// spills create it when they first need it
    pub fn scratch_dir(&self) -> PathBuf {
        match &self.inner.disk {
            Some(disk) => disk.dir.join("tmp"),
            _ => std::env::temp_dir(),
        }
    }

    #[inline]
    pub fn epoch(&self) -> u64 {
        self.inner.epoch.load(Ordering::Acquire)
    }

    pub fn create_table(&self, name: &str, schema: Schema) -> Result<Arc<Table>> {
        let mut catalog = self.inner.catalog.write().expect("catalogue poisoned");
        if catalog.contains_key(name) {
            return Err(EngineError::TableExists(name.to_string()));
        }

        let id = self.inner.next_table_id.fetch_add(1, Ordering::Relaxed);
        let table = new_table(id, name.to_string(), schema, self.inner.cold.clone());
        let schema = table.schema.clone();
        let ddl = Ddl::CreateTable { table: id, name: name.to_string(), schema };

        self.inner.ddl(&ddl, || {
            catalog.insert(table.name().to_string(), Arc::clone(&table));
        })?;

        Ok(table)
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut catalog = self.inner.catalog.write().expect("catalogue poisoned");
        let table = catalog
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))?;

        self.inner.ddl(&Ddl::DropTable { table: table.id }, || {
            catalog.remove(name);
            table.mark_dropped();
        })
    }

    /// registers the index (so concurrent writers maintain it), then back-fills the key of every
    /// version, a unique index is refused when two rows' newest versions share a key
    pub fn create_index(&self, name: &str, table: &str, column: &str, unique: bool) -> Result<()> {
        let table = self.table(table)?;
        let column = table
            .schema
            .columns
            .iter()
            .position(|c| c.name() == column)
            .ok_or_else(|| EngineError::ColumnNotFound(column.to_string()))?;

        if table.index(name).is_some() {
            return Err(EngineError::IndexExists(name.to_string()));
        }

        let index = Arc::new(Index::new(name, column, unique));
        table.add_index(Arc::clone(&index));
        table.for_each_version(|id, row| index.insert_unchecked(row[column].clone(), id));

        if unique {
            if let Some(key) = duplicated_key(&table, &index) {
                table.remove_index(name);
                return Err(EngineError::DuplicatedKey(key));
            }
        }

        let ddl = Ddl::CreateIndex { table: table.id, column, unique, name: name.to_string() };

        self.inner.ddl(&ddl, || ())
    }

    /// writes a consistent image of every table at the newest visible timestamp `C`, makes it the
    /// recovery starting point, and drops older images and log segments
    /// never blocks writers
    pub fn checkpoint(&self) -> Result<u64> {
        let (Some(disk), Some(log)) = (&self.inner.disk, &self.inner.log) else {
            return Ok(self.inner.clock.visible());
        };

        if log.is_poisoned() {
            return Err(EngineError::Poisoned);
        }

        let mut current = disk.checkpoint.lock().expect("checkpoint lock poisoned");
        let (tables, (ts, lsn)) = {
            let catalog = self.inner.catalog.read().expect("catalogue poisoned");
            let tables: Vec<_> = catalog.values().cloned().collect();
            (tables, self.inner.clock.pin())
        };

        let written = match current.0 == ts && ts != 0 {
            true => Ok(false),
            _ => disk.write_checkpoint(&tables, ts, lsn).map(|_| true),
        };
        self.inner.clock.release(ts);

        if written? {
            *current = (ts, lsn);
            disk.discard_before(ts, lsn, log)?;
        }

        Ok(ts)
    }

    /// one maintenance round: reclaim versions no snapshot needs, trim commit summaries, and
    /// checkpoint once enough log accumulated
    pub fn maintain(&self) -> Result<usize> {
        let watermark = self.inner.clock.watermark();
        let reclaimed = self
            .tables()
            .iter()
            .map(|table| table.sweep(watermark, |id, rows| table.forget(id, rows)))
            .sum();

        self.inner.commits().trim(self.inner.clock.visible());

        if let Some(cold) = &self.inner.cold {
            cold.advance();
            evict_coldest(cold, &self.tables(), watermark);
        }

        if let (Some(disk), Some(log)) = (&self.inner.disk, &self.inner.log) {
            let since = log.end() - disk.checkpoint.lock().expect("checkpoint lock poisoned").1;
            if since >= disk.config.checkpoint_bytes {
                self.checkpoint()?;
            }
        }

        Ok(reclaimed)
    }

    /// runs [Engine::maintain] every `interval` on a background thread until [Engine::close] or
    /// until the engine is dropped
    /// the thread holds no strong reference to the engine
    pub fn start_maintenance(&self, interval: Duration) {
        let mut maintenance = self.inner.maintenance.lock().expect("maintenance poisoned");
        if maintenance.is_some() {
            return;
        }

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let weak = Arc::downgrade(&self.inner);
        let signal = Arc::clone(&stop);

        let handle = std::thread::spawn(move || {
            loop {
                {
                    let (stopped, wake) = &*signal;
                    let stopped = stopped.lock().expect("maintenance poisoned");
                    let (stopped, _) = wake
                        .wait_timeout_while(stopped, interval, |stopped| !*stopped)
                        .expect("maintenance poisoned");

                    if *stopped {
                        return;
                    }
                }

                match weak.upgrade() {
                    Some(inner) => {
                        let _ = Engine { inner }.maintain();
                    },
                    _ => return,
                }
            }
        });

        *maintenance = Some(Maintenance { stop, handle });
    }

    /// stops maintenance and writes a final checkpoint
    /// transactions still open keep working, their later commits stay durable through the log
    pub fn close(&self) -> Result<()> {
        let maintenance = self.inner.maintenance.lock().expect("maintenance poisoned").take();
        if let Some(maintenance) = maintenance {
            maintenance.stop();
            maintenance.handle.join().expect("maintenance thread panicked");
        }

        match self.inner.log.as_ref().is_some_and(Log::is_poisoned) {
            true => Ok(()),
            _ => self.checkpoint().map(|_| ()),
        }
    }

    fn assemble(recovered: Recovered, log: Option<Log>, disk: Option<Disk>) -> Self {
        let catalog = recovered
            .tables
            .into_values()
            .map(|table| (table.name().to_string(), table))
            .collect();

        Self {
            inner: Arc::new(Inner {
                clock: Clock::recovered(recovered.last_ts, recovered.lsn),
                commits: Mutex::new(Commits::new(recovered.last_ts)),
                catalog: RwLock::new(catalog),
                next_table_id: AtomicU32::new(recovered.max_table_id + 1),
                epoch: AtomicU64::new(0),
                log,
                disk,
                cold: recovered.cold,
                maintenance: Mutex::new(None),
            }),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sync: SyncMode::Full,
            segment_size: 64 << 20,
            checkpoint_bytes: 64 << 20,
            maintenance_interval: Duration::from_secs(1),
            max_resident_chunks: None,
        }
    }
}

impl Inner {
    #[inline]
    fn commits(&self) -> MutexGuard<'_, Commits> {
        self.commits.lock().expect("commit lock poisoned")
    }

    /// logs a catalogue change in commit order, waits until durable, applies it, publishes it
    fn ddl<T>(&self, ddl: &Ddl, apply: impl FnOnce() -> T) -> Result<T> {
        let mut payload = record::encode_ddl(ddl);

        let (ts, end) = {
            let mut commits = self.commits();
            if self.log.as_ref().is_some_and(Log::is_poisoned) {
                return Err(EngineError::Poisoned);
            }

            commits.last += 1;
            record::stamp_timestamp(&mut payload, commits.last);
            let end = self.log.as_ref().map(|log| log.append(Kind::Ddl, &payload));
            (commits.last, end)
        };

        if let (Some(log), Some(end)) = (&self.log, end) {
            log.wait(end)?;
        }

        let applied = apply();
        self.epoch.fetch_add(1, Ordering::Release);
        self.clock.publish(ts, end.unwrap_or(0));

        Ok(applied)
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(maintenance) = self.maintenance.get_mut().expect("maintenance poisoned") {
            maintenance.stop();
        }
    }
}

impl Disk {
    fn write_checkpoint(&self, tables: &[Arc<Table>], ts: u64, lsn: u64) -> Result<()> {
        let storage = self.storage.as_ref();
        let checkpoints = self.dir.join(checkpoint::CHECKPOINTS);
        let directory = checkpoint::directory(&self.dir, ts);

        if !storage.exists(&checkpoints) {
            storage.create_dir(&checkpoints)?;
            storage.sync_dir(&self.dir)?;
        }
        storage.create_dir(&directory)?;
        storage.sync_dir(&checkpoints)?;

        let view = View { snapshot: ts, marker: 0 };

        for table in tables {
            let mut scan = table.scan(view);
            let path = checkpoint::image_path(&self.dir, ts, table.id);
            checkpoint::write_image(storage, &path, table, ts, |rows| scan.next_batch(rows))?;
        }
        storage.sync_dir(&directory)?;

        let manifest = Manifest {
            ts,
            start_lsn: lsn,
            tables: tables.iter().map(|table| table.id).collect(),
        };
        manifest.write(storage, &self.dir)
    }

    /// removes images of other checkpoints and log segments the current one no longer needs
    fn discard_before(&self, ts: u64, lsn: u64, log: &Log) -> Result<()> {
        let checkpoints = self.dir.join(checkpoint::CHECKPOINTS);
        let current = checkpoint::directory(&self.dir, ts);

        for directory in self.storage.list(&checkpoints)? {
            if directory != current {
                self.storage.remove_dir(&directory)?;
            }
        }
        self.storage.sync_dir(&checkpoints)?;

        log.prune(lsn)
    }
}

impl Maintenance {
    fn stop(&self) {
        let (stopped, wake) = &*self.stop;
        *stopped.lock().expect("maintenance poisoned") = true;
        wake.notify_all();
    }
}

impl Recovered {
    fn apply(&mut self, kind: Kind, payload: &[u8]) -> Result<()> {
        let ts = match kind {
            Kind::Commit => record::decode_commit(payload, |ts, table, id, change| {
                let table = self.tables.get(&table).ok_or_else(|| {
                    EngineError::Corrupt(format!("commit into unknown table {table}"))
                })?;

                match change {
                    Change::Put(row) => table.recover_put(id, row, ts),
                    Change::Delete => table.recover_delete(id),
                }
                Ok(())
            })?,
            Kind::Ddl => {
                let (ts, ddl) = record::decode_ddl(payload)?;
                self.apply_ddl(ddl);
                ts
            },
        };

        if ts <= self.last_ts {
            return Err(EngineError::Corrupt(format!(
                "log timestamp {ts} does not follow {}",
                self.last_ts
            )));
        }

        Ok(self.last_ts = ts)
    }

    /// idempotent: a checkpoint image may already reflect the change
    fn apply_ddl(&mut self, ddl: Ddl) {
        match ddl {
            Ddl::CreateTable { table, name, schema } => {
                self.max_table_id = self.max_table_id.max(table);
                self.tables
                    .entry(table)
                    .or_insert_with(|| new_table(table, name, schema, self.cold.clone()));
            },
            Ddl::DropTable { table } => {
                self.tables.remove(&table);
            },
            Ddl::CreateIndex { table, column, unique, name } => {
                if let Some(table) = self.tables.get(&table) {
                    if table.index(&name).is_none() {
                        table.add_index(Arc::new(Index::new(name, column, unique)));
                    }
                }
            },
        }
    }

    fn build_indexes(&self) {
        for table in self.tables.values() {
            for index in table.indexes() {
                index.clear();
                table.for_each_version(|id, row| {
                    index.insert_unchecked(row[index.column].clone(), id)
                });
            }
        }
    }
}

/// a table with its primary-key index
fn new_table(id: u32, name: String, schema: Schema, cold: Option<Arc<Cold>>) -> Arc<Table> {
    let table = Arc::new(Table::new(id, name, schema, cold));

    if let Some(column) = table.schema.primary_column_index() {
        let name = index!(primary on (table.name()));
        table.add_index(Arc::new(Index::new(name, column, true)));
    }

    table
}

/// evicts the least recently touched settled chunks of `tables` until `cold` is within budget
fn evict_coldest(cold: &Cold, tables: &[Arc<Table>], watermark: u64) -> usize {
    if !cold.over_budget() {
        return 0;
    }

    let mut candidates: Vec<_> = tables
        .iter()
        .enumerate()
        .flat_map(|(position, table)| {
            table
                .resident_chunks()
                .into_iter()
                .map(move |(number, touched)| (touched, position, number))
        })
        .collect();
    candidates.sort_unstable();

    let mut evicted = 0;
    for (_, position, number) in candidates {
        if !cold.over_budget() {
            break;
        }
        evicted += tables[position].evict(number, watermark) as usize;
    }

    evicted
}

/// a key the newest versions of two different rows both hold
fn duplicated_key(table: &Table, index: &Index) -> Option<Value> {
    let mut duplicated = None;
    let mut current: Option<(Value, usize)> = None;

    index.visit(Bound::Unbounded, Bound::Unbounded, |key, id| {
        if duplicated.is_some() || *key == Value::Null {
            return;
        }

        let alive = table.holder(id, index.column, key, 0) != Holder::Free;
        match &mut current {
            Some((previous, count)) if previous == key => *count += alive as usize,
            _ => current = Some((key.clone(), alive as usize)),
        }

        if let Some((key, 2..)) = &current {
            duplicated = Some(key.clone());
        }
    });

    duplicated
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::statement::Type;
    use crate::storage::engine::io::memory::MemoryStorage;
    use crate::storage::engine::table::{Row, RowId};
    use std::sync::Barrier;
    use std::thread;

    fn engine() -> (Engine, Arc<Table>) {
        let engine = Engine::in_memory();
        let schema = SchemaBuilder::new()
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        let table = engine.create_table("users", schema).unwrap();

        (engine, table)
    }

    fn row(id: i128, name: &str) -> Row {
        Arc::from([Value::Number(id), Value::String(name.into())])
    }

    fn insert(engine: &Engine, table: &Arc<Table>, rows: &[(i128, &str)]) -> Vec<RowId> {
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let ids = rows
            .iter()
            .map(|&(id, name)| txn.insert(table, row(id, name)).unwrap())
            .collect();
        txn.commit().unwrap();
        ids
    }

    fn text(value: &Value) -> String {
        match value {
            Value::String(text) => text.to_string(),
            other => panic!("expected text, got {other}"),
        }
    }

    fn names(txn: &mut Transaction, table: &Arc<Table>) -> Vec<String> {
        let mut scan = txn.scan(table);
        let mut rows = Vec::new();
        while scan.next_batch(&mut rows) {}

        rows.iter().map(|(_, row)| text(&row[1])).collect()
    }

    fn name(txn: &mut Transaction, table: &Arc<Table>, id: RowId) -> Option<String> {
        txn.get(table, id).map(|row| text(&row[1]))
    }

    #[test]
    fn visibility_of_own_uncommitted_committed_and_deleted_rows() {
        let (engine, table) = engine();
        let ids = insert(&engine, &table, &[(1, "a"), (2, "b")]);

        let mut reader = engine.begin(IsolationLevel::Snapshot);
        let mut writer = engine.begin(IsolationLevel::Snapshot);
        writer.update(&table, ids[0], row(1, "a2")).unwrap();
        writer.delete(&table, ids[1]).unwrap();
        let own = writer.insert(&table, row(3, "c")).unwrap();

        assert_eq!(names(&mut writer, &table), ["a2", "c"]);
        assert_eq!(names(&mut reader, &table), ["a", "b"]);
        assert_eq!(name(&mut reader, &table, own), None);

        writer.commit().unwrap();
        assert_eq!(names(&mut reader, &table), ["a", "b"]);

        let mut late = engine.begin(IsolationLevel::Snapshot);
        assert_eq!(names(&mut late, &table), ["a2", "c"]);
    }

    #[test]
    fn a_key_deleted_and_inserted_again_keeps_its_index_bounded_but_old_snapshots_whole() {
        let (engine, table) = engine();
        let index = table.index_on(0).unwrap();
        let cycle = |name: &str| {
            let id = insert(&engine, &table, &[(7, name)])[0];
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            txn.delete(&table, id).unwrap();
            txn.commit().unwrap();
            id
        };

        let highest = (0..1_000).map(|_| cycle("x")).max().unwrap();
        assert!(index.len() <= 2, "{} entries for one key", index.len());
        assert!(highest <= 3, "churn reuses slots instead of growing to row id {highest}");

        let kept = insert(&engine, &table, &[(7, "kept")])[0];
        let mut old = engine.begin(IsolationLevel::Snapshot);
        assert_eq!(name(&mut old, &table, kept).as_deref(), Some("kept"));
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.delete(&table, kept).unwrap();
        txn.commit().unwrap();
        (0..50).for_each(|_| {
            cycle("y");
        });

        let key = Value::Number(7);
        let point = Bound::Included(&key);
        let seen: Vec<_> = old
            .index_range(&table, &index, point, point)
            .iter()
            .map(|(_, row)| text(&row[1]))
            .collect();
        assert_eq!(seen, ["kept"]);

        drop(old);
        cycle("z");

        assert!(index.len() <= 2, "{} entries once the snapshot is gone", index.len());
    }

    #[test]
    fn a_slot_is_reused_only_once_no_snapshot_can_see_its_row() {
        let (engine, table) = engine();
        let index = table.index_on(0).unwrap();
        let ids = insert(&engine, &table, &[(1, "a"), (2, "b")]);

        let mut old = engine.begin(IsolationLevel::Snapshot);
        assert_eq!(name(&mut old, &table, ids[1]).as_deref(), Some("b"));
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.delete(&table, ids[1]).unwrap();
        txn.commit().unwrap();

        engine.maintain().unwrap();
        let fresh = insert(&engine, &table, &[(3, "c")])[0];
        assert!(fresh > ids[1], "the old snapshot still sees row {}", ids[1]);
        assert_eq!(name(&mut old, &table, ids[1]).as_deref(), Some("b"));
        drop(old);

        engine.maintain().unwrap();
        assert_eq!(insert(&engine, &table, &[(4, "d")]), [ids[1]]);

        let mut reader = engine.begin(IsolationLevel::Snapshot);
        let key = |k: i128| Value::Number(k);
        let find = |reader: &mut Transaction, k: i128| {
            let k = key(k);
            reader
                .index_range(&table, &index, Bound::Included(&k), Bound::Included(&k))
                .iter()
                .map(|(_, row)| text(&row[1]))
                .collect::<Vec<_>>()
        };
        assert!(find(&mut reader, 2).is_empty(), "the deleted key finds nothing");
        assert_eq!(find(&mut reader, 4), ["d"]);
        assert_eq!(names(&mut reader, &table).len(), 3);
    }

    #[test]
    fn rollback_and_drop_leave_no_trace() {
        let (engine, table) = engine();
        let ids = insert(&engine, &table, &[(1, "a")]);

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let fresh = txn.insert(&table, row(2, "b")).unwrap();
        txn.update(&table, ids[0], row(1, "x")).unwrap();
        txn.rollback();

        {
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            txn.insert(&table, row(3, "c")).unwrap();
        }

        let mut reader = engine.begin(IsolationLevel::Snapshot);
        assert_eq!(names(&mut reader, &table), ["a"]);
        assert_eq!(name(&mut reader, &table, fresh), None);
        assert_eq!(table.index_on(0).unwrap().len(), 1);

        let mut again = engine.begin(IsolationLevel::Snapshot);
        again.insert(&table, row(2, "b")).unwrap();
        again.commit().unwrap();
    }

    #[test]
    fn multi_table_commit_is_atomic_for_concurrent_readers() {
        let (engine, users) = engine();
        let orders = engine
            .create_table("orders", SchemaBuilder::new().primary("id", Type::Integer).build())
            .unwrap();

        let stop = std::sync::atomic::AtomicBool::new(false);
        thread::scope(|scope| {
            scope.spawn(|| {
                for n in 0..500 {
                    let mut txn = engine.begin(IsolationLevel::Snapshot);
                    txn.insert(&users, row(n, "u")).unwrap();
                    txn.insert(&orders, Arc::from([Value::Number(n)])).unwrap();
                    txn.commit().unwrap();
                }
                stop.store(true, Ordering::Release);
            });

            while !stop.load(Ordering::Acquire) {
                let mut txn = engine.begin(IsolationLevel::Snapshot);
                let count = |txn: &mut Transaction, table: &Arc<Table>| {
                    let mut scan = txn.scan(table);
                    let mut rows = Vec::new();
                    while scan.next_batch(&mut rows) {}
                    rows.len()
                };
                assert_eq!(count(&mut txn, &users), count(&mut txn, &orders));
            }
        });
    }

    #[test]
    fn snapshot_reads_are_repeatable_around_commits() {
        let (engine, table) = engine();
        let ids = insert(&engine, &table, &[(1, "v0")]);

        let mut reader = engine.begin(IsolationLevel::Snapshot);
        for version in 1..20 {
            let mut writer = engine.begin(IsolationLevel::Snapshot);
            writer.update(&table, ids[0], row(1, &format!("v{version}"))).unwrap();
            writer.commit().unwrap();

            assert_eq!(name(&mut reader, &table, ids[0]).unwrap(), "v0");
        }
    }

    #[test]
    fn first_updater_wins() {
        let (engine, table) = engine();
        let ids = insert(&engine, &table, &[(1, "a")]);

        let mut first = engine.begin(IsolationLevel::Snapshot);
        let mut second = engine.begin(IsolationLevel::Snapshot);
        let mut committed = engine.begin(IsolationLevel::ReadCommitted);

        first.update(&table, ids[0], row(1, "first")).unwrap();
        assert!(matches!(
            second.update(&table, ids[0], row(1, "second")),
            Err(EngineError::WriteConflict)
        ));
        first.commit().unwrap();

        assert!(matches!(second.commit(), Err(EngineError::Aborted)));

        let mut stale = engine.begin(IsolationLevel::Snapshot);
        let mut fresh = engine.begin(IsolationLevel::Snapshot);
        fresh.update(&table, ids[0], row(1, "fresh")).unwrap();
        fresh.commit().unwrap();

        assert!(matches!(
            stale.update(&table, ids[0], row(1, "stale")),
            Err(EngineError::SerializationFailure)
        ));
        match committed.update(&table, ids[0], row(1, "rc")) {
            Err(EngineError::Stale(Some(latest))) => assert_eq!(text(&latest[1]), "fresh"),
            other => panic!("expected stale, got {:?}", other.err()),
        }
    }

    #[test]
    fn concurrent_inserts_of_one_unique_key_never_both_commit() {
        for round in 0..200 {
            let (engine, table) = engine();
            let barrier = Barrier::new(2);

            let committed: usize = thread::scope(|scope| {
                let attempts: Vec<_> = (0..2)
                    .map(|n| {
                        let (engine, table, barrier) = (&engine, &table, &barrier);
                        scope.spawn(move || {
                            let mut txn = engine.begin(IsolationLevel::Snapshot);
                            barrier.wait();
                            let inserted = txn.insert(table, row(round, &format!("t{n}")));
                            (inserted.is_ok() && txn.commit().is_ok()) as usize
                        })
                    })
                    .collect();

                attempts.into_iter().map(|a| a.join().unwrap()).sum()
            });

            assert!(committed <= 1, "round {round}: duplicate key committed");
            let mut reader = engine.begin(IsolationLevel::Snapshot);
            assert_eq!(names(&mut reader, &table).len(), committed);
        }
    }

    #[test]
    fn unique_key_can_move_within_one_transaction() {
        let (engine, table) = engine();
        let ids = insert(&engine, &table, &[(1, "a")]);

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.delete(&table, ids[0]).unwrap();
        txn.insert(&table, row(1, "b")).unwrap();
        assert!(matches!(txn.insert(&table, row(1, "c")), Err(EngineError::DuplicatedKey(_))));
        txn.rollback();

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.delete(&table, ids[0]).unwrap();
        txn.insert(&table, row(1, "b")).unwrap();
        txn.commit().unwrap();

        let mut reader = engine.begin(IsolationLevel::Snapshot);
        assert_eq!(names(&mut reader, &table), ["b"]);
    }

    #[test]
    fn index_lookups_recheck_the_visible_key() {
        let (engine, table) = engine();
        engine.create_index("by_name", "users", "name", false).unwrap();
        let ids = insert(&engine, &table, &[(1, "old"), (2, "zzz")]);
        let index = table.index("by_name").unwrap();

        let mut reader = engine.begin(IsolationLevel::Snapshot);
        let mut writer = engine.begin(IsolationLevel::Snapshot);
        writer.update(&table, ids[0], row(1, "new")).unwrap();
        writer.commit().unwrap();

        let lookup = |txn: &mut Transaction, key: &str| {
            let key = Value::String(key.into());
            txn.index_range(&table, &index, Bound::Included(&key), Bound::Included(&key))
                .len()
        };

        assert_eq!(lookup(&mut reader, "old"), 1);
        assert_eq!(lookup(&mut reader, "new"), 0);

        let mut late = engine.begin(IsolationLevel::Snapshot);
        assert_eq!(lookup(&mut late, "old"), 0);
        assert_eq!(lookup(&mut late, "new"), 1);

        let all = late.index_range(&table, &index, Bound::Unbounded, Bound::Unbounded);
        let ordered: Vec<_> = all.iter().map(|(_, row)| text(&row[1])).collect();
        assert_eq!(ordered, ["new", "zzz"]);
    }

    #[test]
    fn writes_prune_versions_no_snapshot_needs() {
        let (engine, table) = engine();
        let ids = insert(&engine, &table, &[(1, "v0")]);
        let index = table.index_on(0).unwrap();
        engine.create_index("by_name", "users", "name", false).unwrap();
        let by_name = table.index("by_name").unwrap();

        let mut pinned = engine.begin(IsolationLevel::Snapshot);
        for version in 1..=10 {
            let mut writer = engine.begin(IsolationLevel::Snapshot);
            writer.update(&table, ids[0], row(1, &format!("v{version}"))).unwrap();
            writer.commit().unwrap();
        }
        assert_eq!(name(&mut pinned, &table, ids[0]).unwrap(), "v0");
        drop(pinned);

        let mut writer = engine.begin(IsolationLevel::Snapshot);
        writer.update(&table, ids[0], row(1, "last")).unwrap();
        writer.commit().unwrap();

        let mut versions = 0;
        table.for_each_version(|_, _| versions += 1);
        assert_eq!(versions, 2);
        assert_eq!(by_name.len(), 2);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn scans_follow_row_order() {
        let (engine, table) = engine();
        let rows: Vec<_> = (0..2500).map(|n| (n, "x")).collect();
        insert(&engine, &table, &rows);

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let mut scan = txn.scan(&table);
        let mut out = Vec::new();
        while scan.next_batch(&mut out) {}

        assert_eq!(out.len(), 2500);
        assert!(out.windows(2).all(|pair| pair[0].0 < pair[1].0));
    }

    #[test]
    fn unique_index_creation_rejects_existing_duplicates() {
        let (engine, table) = engine();
        insert(&engine, &table, &[(1, "same"), (2, "same")]);

        assert!(matches!(
            engine.create_index("unique_name", "users", "name", true),
            Err(EngineError::DuplicatedKey(_))
        ));
        assert!(table.index("unique_name").is_none());
        engine.create_index("name", "users", "name", false).unwrap();
    }

    #[test]
    fn commit_into_a_dropped_table_fails() {
        let (engine, table) = engine();
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.insert(&table, row(1, "a")).unwrap();

        engine.drop_table("users").unwrap();
        assert!(matches!(txn.commit(), Err(EngineError::TableNotFound(_))));
    }

    fn number(value: &Value) -> i128 {
        match value {
            Value::Number(number) => *number,
            other => panic!("expected a number, got {other}"),
        }
    }

    fn accounts(engine: &Engine, balances: &[i128]) -> (Arc<Table>, Vec<RowId>) {
        let schema = SchemaBuilder::new()
            .primary("id", Type::Integer)
            .add("balance", Type::Integer)
            .build();
        let table = engine.create_table("accounts", schema).unwrap();

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let ids = balances
            .iter()
            .enumerate()
            .map(|(n, &balance)| {
                let row = Arc::from([Value::Number(n as i128), Value::Number(balance)]);
                txn.insert(&table, row).unwrap()
            })
            .collect();
        txn.commit().unwrap();

        (table, ids)
    }

    fn balance(txn: &mut Transaction, table: &Arc<Table>, id: RowId) -> i128 {
        number(&txn.get(table, id).unwrap()[1])
    }

    fn set(
        txn: &mut Transaction,
        table: &Arc<Table>,
        id: RowId,
        n: i128,
        value: i128,
    ) -> Result<()> {
        txn.update(table, id, Arc::from([Value::Number(n), Value::Number(value)]))
    }

    /// the executor's READ COMMITTED loop: recompute on the newest image and retry
    fn increment(
        txn: &mut Transaction,
        table: &Arc<Table>,
        id: RowId,
        mut current: i128,
    ) -> Result<()> {
        loop {
            match set(txn, table, id, 0, current + 1) {
                Err(EngineError::Stale(Some(latest))) => current = number(&latest[1]),
                other => return other,
            }
        }
    }

    #[test]
    fn read_committed_sees_commits_between_statements_and_loses_no_update() {
        let engine = Engine::in_memory();
        let (table, ids) = accounts(&engine, &[0]);

        let mut first = engine.begin(IsolationLevel::ReadCommitted);
        let mut second = engine.begin(IsolationLevel::ReadCommitted);
        let seen_first = balance(&mut first, &table, ids[0]);
        let seen_second = balance(&mut second, &table, ids[0]);

        increment(&mut first, &table, ids[0], seen_first).unwrap();
        first.commit().unwrap();

        increment(&mut second, &table, ids[0], seen_second).unwrap();
        second.commit().unwrap();

        let mut reader = engine.begin(IsolationLevel::ReadCommitted);
        assert_eq!(balance(&mut reader, &table, ids[0]), 2);
    }

    #[test]
    fn read_committed_statement_snapshot_is_stable_until_refreshed() {
        let engine = Engine::in_memory();
        let (table, ids) = accounts(&engine, &[10]);

        let mut reader = engine.begin(IsolationLevel::ReadCommitted);
        let mut writer = engine.begin(IsolationLevel::Snapshot);
        set(&mut writer, &table, ids[0], 0, 11).unwrap();
        writer.commit().unwrap();

        assert_eq!(balance(&mut reader, &table, ids[0]), 10);
        reader.refresh_snapshot();
        assert_eq!(balance(&mut reader, &table, ids[0]), 11);
    }

    /// Two doctors on call; each takes themselves off if the other is still on call.
    fn write_skew(level: IsolationLevel) -> (Result<()>, Result<()>) {
        let engine = Engine::in_memory();
        let (table, ids) = accounts(&engine, &[1, 1]);

        let mut first = engine.begin(level);
        let mut second = engine.begin(level);
        let on_call = |txn: &mut Transaction| {
            let mut scan = txn.scan(&table);
            let mut rows = Vec::new();
            while scan.next_batch(&mut rows) {}
            rows.iter().map(|(_, row)| number(&row[1])).sum::<i128>()
        };

        assert_eq!(on_call(&mut first), 2);
        assert_eq!(on_call(&mut second), 2);
        set(&mut first, &table, ids[0], 0, 0).unwrap();
        set(&mut second, &table, ids[1], 1, 0).unwrap();

        (first.commit(), second.commit())
    }

    #[test]
    fn write_skew_commits_under_snapshot_isolation() {
        let (first, second) = write_skew(IsolationLevel::Snapshot);
        assert!(first.is_ok() && second.is_ok());
    }

    #[test]
    fn write_skew_is_rejected_under_serializable() {
        let (first, second) = write_skew(IsolationLevel::Serializable);
        assert!(first.is_ok());
        assert!(matches!(second, Err(EngineError::SerializationFailure)));
    }

    #[test]
    fn serializable_rejects_phantoms_in_a_read_range() {
        let engine = Engine::in_memory();
        let (table, _) = accounts(&engine, &[5]);
        engine.create_index("by_balance", "accounts", "balance", false).unwrap();
        let index = table.index("by_balance").unwrap();
        let (low, high) = (Value::Number(0), Value::Number(100));

        let mut reader = engine.begin(IsolationLevel::Serializable);
        let mut inserter = engine.begin(IsolationLevel::Serializable);
        let seen =
            reader.index_range(&table, &index, Bound::Included(&low), Bound::Included(&high));
        assert_eq!(seen.len(), 1);

        inserter
            .insert(&table, Arc::from([Value::Number(7), Value::Number(50)]))
            .unwrap();
        inserter.commit().unwrap();

        set(&mut reader, &table, 1, 0, 6).unwrap();
        assert!(matches!(reader.commit(), Err(EngineError::SerializationFailure)));
    }

    #[test]
    fn serializable_disjoint_point_writers_both_commit() {
        let engine = Engine::in_memory();
        let (table, ids) = accounts(&engine, &[10, 20]);

        let mut first = engine.begin(IsolationLevel::Serializable);
        let mut second = engine.begin(IsolationLevel::Serializable);
        let a = balance(&mut first, &table, ids[0]);
        let b = balance(&mut second, &table, ids[1]);
        set(&mut first, &table, ids[0], 0, a + 1).unwrap();
        set(&mut second, &table, ids[1], 1, b + 1).unwrap();

        first.commit().unwrap();
        second.commit().unwrap();
    }

    #[test]
    fn serializable_read_only_transactions_never_abort() {
        let engine = Engine::in_memory();
        let (table, ids) = accounts(&engine, &[10, 20]);

        let mut reader = engine.begin(IsolationLevel::Serializable);
        let before = balance(&mut reader, &table, ids[0]) + balance(&mut reader, &table, ids[1]);

        let mut writer = engine.begin(IsolationLevel::Serializable);
        let a = balance(&mut writer, &table, ids[0]);
        set(&mut writer, &table, ids[0], 0, a - 5).unwrap();
        let b = balance(&mut writer, &table, ids[1]);
        set(&mut writer, &table, ids[1], 1, b + 5).unwrap();
        writer.commit().unwrap();

        let after = balance(&mut reader, &table, ids[0]) + balance(&mut reader, &table, ids[1]);
        assert_eq!(before, after);
        reader.commit().unwrap();
    }

    #[test]
    fn summaries_are_trimmed_once_no_serializable_snapshot_needs_them() {
        let engine = Engine::in_memory();
        let (table, ids) = accounts(&engine, &[0]);

        let pinned = engine.begin(IsolationLevel::Serializable);
        for value in 1..=5 {
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            set(&mut txn, &table, ids[0], 0, value).unwrap();
            txn.commit().unwrap();
        }
        assert_eq!(engine.inner.commits().recent_len(), 5);

        drop(pinned);
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        set(&mut txn, &table, ids[0], 0, 6).unwrap();
        txn.commit().unwrap();
        assert!(engine.inner.commits().recent_len() <= 1);
    }

    fn disk() -> MemoryStorage {
        let storage = MemoryStorage::new();
        storage.create_dir(Path::new("db")).unwrap();
        storage.sync_dir(Path::new("")).unwrap();
        storage
    }

    fn reopen(storage: &MemoryStorage) -> Engine {
        Engine::open_with(Arc::new(storage.clone()), Path::new("db"), Config::default(), None)
            .unwrap()
    }

    fn users(engine: &Engine) -> Arc<Table> {
        let schema = SchemaBuilder::new()
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        engine.create_table("users", schema).unwrap()
    }

    fn all_names(engine: &Engine, table: &str) -> Vec<String> {
        let table = engine.table(table).unwrap();
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        names(&mut txn, &table)
    }

    #[test]
    fn restart_replays_committed_transactions_only() {
        let storage = disk();
        {
            let engine = reopen(&storage);
            let table = users(&engine);
            let ids = insert(&engine, &table, &[(1, "a"), (2, "b"), (3, "c")]);

            let mut txn = engine.begin(IsolationLevel::Snapshot);
            txn.update(&table, ids[0], row(1, "a2")).unwrap();
            txn.delete(&table, ids[1]).unwrap();
            txn.commit().unwrap();

            let mut open = engine.begin(IsolationLevel::Snapshot);
            open.insert(&table, row(9, "uncommitted")).unwrap();
            std::mem::forget(open);
        }

        let engine = reopen(&storage);
        assert_eq!(all_names(&engine, "users"), ["a2", "c"]);

        let table = engine.table("users").unwrap();
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        assert!(matches!(txn.insert(&table, row(3, "dup")), Err(EngineError::DuplicatedKey(_))));
    }

    #[test]
    fn ddl_replays_with_fresh_table_ids() {
        let storage = disk();
        {
            let engine = reopen(&storage);
            let first = users(&engine);
            insert(&engine, &first, &[(1, "old")]);
            engine.drop_table("users").unwrap();

            let second = users(&engine);
            assert_ne!(first.id, second.id);
            insert(&engine, &second, &[(1, "new")]);
            engine.create_index("by_name", "users", "name", false).unwrap();
        }

        let engine = reopen(&storage);
        assert_eq!(all_names(&engine, "users"), ["new"]);

        let table = engine.table("users").unwrap();
        let index = table.index("by_name").expect("index definition replayed");
        assert_eq!(index.len(), 1);

        let fresh = users_named(&engine, "other");
        assert!(fresh.id > table.id);
    }

    fn users_named(engine: &Engine, name: &str) -> Arc<Table> {
        engine
            .create_table(name, SchemaBuilder::new().primary("id", Type::Integer).build())
            .unwrap()
    }

    #[test]
    fn reused_slots_replay_from_the_log_and_from_images() {
        let storage = disk();
        {
            let engine = reopen(&storage);
            let table = users(&engine);
            let ids = insert(&engine, &table, &[(1, "a"), (2, "b"), (3, "c")]);

            let mut txn = engine.begin(IsolationLevel::Snapshot);
            txn.delete(&table, ids[1]).unwrap();
            txn.commit().unwrap();
            engine.maintain().unwrap();
            assert_eq!(
                insert(&engine, &table, &[(4, "d")]),
                [ids[1]],
                "the vanished slot is reused"
            );

            engine.checkpoint().unwrap();
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            txn.delete(&table, ids[2]).unwrap();
            txn.commit().unwrap();
            engine.maintain().unwrap();
            assert_eq!(insert(&engine, &table, &[(5, "e")]), [ids[2]]);
        }

        let engine = reopen(&storage);
        let mut names = all_names(&engine, "users");
        names.sort();
        assert_eq!(names, ["a", "d", "e"]);

        let table = engine.table("users").unwrap();
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let all = txn.index_range(
            &table,
            &table.index_on(0).unwrap(),
            Bound::Unbounded,
            Bound::Unbounded,
        );
        assert_eq!(all.len(), 3, "one index entry per live row after recovery");
        drop(txn);

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let first = txn.get(&table, 1).map(|row| text(&row[1]));
        txn.delete(&table, 1).unwrap();
        txn.commit().unwrap();
        drop(engine);

        let engine = reopen(&storage);
        let table = engine.table("users").unwrap();
        assert_eq!(first.as_deref(), Some("a"));
        assert_eq!(insert(&engine, &table, &[(6, "f")]), [1], "restart publishes the hole as free");
    }

    #[test]
    fn a_commit_whose_sync_fails_is_never_visible_and_poisons_the_engine() {
        let storage = disk();
        let engine = reopen(&storage);
        let table = users(&engine);
        insert(&engine, &table, &[(1, "durable")]);

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.insert(&table, row(2, "lost")).unwrap();
        storage.fail_at(2);
        assert!(matches!(txn.commit(), Err(EngineError::Poisoned)));

        assert_eq!(all_names(&engine, "users"), ["durable"]);
        let mut next = engine.begin(IsolationLevel::Snapshot);
        next.insert(&table, row(3, "refused")).unwrap();
        assert!(matches!(next.commit(), Err(EngineError::Poisoned)));
        assert!(matches!(
            engine.create_table("t", SchemaBuilder::new().primary("id", Type::Integer).build()),
            Err(EngineError::Poisoned)
        ));

        drop(engine);
        storage.reboot(false);
        assert_eq!(all_names(&reopen(&storage), "users"), ["durable"]);
    }

    #[test]
    fn a_crash_keeps_exactly_the_acknowledged_commits() {
        for torn in [false, true] {
            let storage = disk();
            let engine = reopen(&storage);
            let table = users(&engine);
            insert(&engine, &table, &[(1, "a"), (2, "b")]);

            storage.crash_at(1);
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            txn.insert(&table, row(3, "c")).unwrap();
            assert!(txn.commit().is_err());
            drop(engine);

            storage.reboot(torn);
            assert_eq!(all_names(&reopen(&storage), "users"), ["a", "b"]);
        }
    }

    #[test]
    fn serials_continue_after_restart() {
        let storage = disk();
        {
            let engine = reopen(&storage);
            let schema = SchemaBuilder::new().primary("id", Type::Serial).build();
            let table = engine.create_table("s", schema).unwrap();

            let mut txn = engine.begin(IsolationLevel::Snapshot);
            for _ in 0..3 {
                let value = table.next_serial(0) as i128;
                txn.insert(&table, Arc::from([Value::Number(value)])).unwrap();
            }
            txn.commit().unwrap();
        }

        let engine = reopen(&storage);
        assert_eq!(engine.table("s").unwrap().next_serial(0), 4);
    }

    fn reopen_with(storage: &MemoryStorage, config: Config) -> Engine {
        Engine::open_with(Arc::new(storage.clone()), Path::new("db"), config, None).unwrap()
    }

    fn snapshot_names(table: &Arc<Table>, snapshot: u64) -> Vec<String> {
        let mut scan = table.scan(View { snapshot, marker: 0 });
        let mut rows = Vec::new();
        while scan.next_batch(&mut rows) {}
        rows.iter().map(|(_, row)| text(&row[1])).collect()
    }

    #[test]
    fn checkpoint_under_concurrent_writes_restores_the_final_state() {
        let storage = disk();
        let engine = reopen(&storage);
        let table = users(&engine);

        let checkpoints = thread::scope(|scope| {
            let writer = scope.spawn(|| {
                for n in 0..300 {
                    insert(&engine, &table, &[(n, "w")]);
                }
            });

            let mut taken = Vec::new();
            while !writer.is_finished() {
                let ts = engine.checkpoint().unwrap();
                let image = checkpoint::image_path(Path::new("db"), ts, table.id);
                if storage.exists(&image) {
                    taken.push(ts);
                }
            }
            taken
        });
        assert!(!checkpoints.is_empty());

        let expected = all_names(&engine, "users");
        drop(engine);
        let engine = reopen(&storage);
        assert_eq!(all_names(&engine, "users"), expected);
        assert_eq!(expected.len(), 300);
    }

    #[test]
    fn checkpoint_ignores_open_transactions_and_replays_their_commit() {
        let storage = disk();
        let engine = reopen(&storage);
        let table = users(&engine);
        insert(&engine, &table, &[(1, "before")]);

        let mut open = engine.begin(IsolationLevel::Snapshot);
        open.insert(&table, row(2, "open")).unwrap();
        let ts = engine.checkpoint().unwrap();
        assert_eq!(snapshot_names(&table, ts), ["before"]);

        open.commit().unwrap();
        insert(&engine, &table, &[(3, "after")]);
        drop(engine);

        let engine = reopen(&storage);
        assert_eq!(all_names(&engine, "users"), ["before", "open", "after"]);
    }

    #[test]
    fn a_crash_at_any_point_of_a_checkpoint_recovers_everything_committed() {
        let operations = {
            let storage = disk();
            let engine = reopen(&storage);
            let table = users(&engine);
            insert(&engine, &table, &[(1, "a"), (2, "b")]);
            engine.checkpoint().unwrap();
            insert(&engine, &table, &[(3, "c")]);

            let before = storage.operations();
            engine.checkpoint().unwrap();
            storage.operations() - before
        };
        assert!(operations > 5);

        for crash in 1..=operations {
            for torn in [false, true] {
                let storage = disk();
                let engine = reopen(&storage);
                let table = users(&engine);
                insert(&engine, &table, &[(1, "a"), (2, "b")]);
                engine.checkpoint().unwrap();
                insert(&engine, &table, &[(3, "c")]);

                storage.crash_at(crash);
                assert!(engine.checkpoint().is_err(), "crash at {crash}");
                drop(engine);

                storage.reboot(torn);
                let engine = reopen(&storage);
                assert_eq!(all_names(&engine, "users"), ["a", "b", "c"], "crash at {crash}");
            }
        }
    }

    #[test]
    fn checkpoints_discard_old_images_and_log_segments() {
        let storage = disk();
        let config = Config { segment_size: 128, ..Config::default() };

        let engine = reopen_with(&storage, config.clone());
        let table = users(&engine);
        for n in 0..50 {
            insert(&engine, &table, &[(n, "x")]);
        }
        let segments = storage.list(Path::new("db/wal")).unwrap().len();

        engine.checkpoint().unwrap();
        insert(&engine, &table, &[(50, "y")]);
        engine.checkpoint().unwrap();

        assert!(storage.list(Path::new("db/wal")).unwrap().len() < segments);
        assert_eq!(storage.list(Path::new("db/ckpt")).unwrap().len(), 1);
        drop(engine);

        let engine = reopen_with(&storage, config);
        assert_eq!(all_names(&engine, "users").len(), 51);
    }

    #[test]
    fn sweep_reclaims_what_no_snapshot_needs() {
        let (engine, table) = engine();
        engine.create_index("by_name", "users", "name", false).unwrap();
        let ids = insert(&engine, &table, &[(1, "a"), (2, "b")]);

        let mut pinned = engine.begin(IsolationLevel::Snapshot);
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.delete(&table, ids[0]).unwrap();
        txn.update(&table, ids[1], row(2, "b2")).unwrap();
        txn.commit().unwrap();

        engine.maintain().unwrap();
        assert_eq!(names(&mut pinned, &table), ["a", "b"]);
        drop(pinned);

        assert_eq!(engine.maintain().unwrap(), 3);
        let mut versions = 0;
        table.for_each_version(|_, _| versions += 1);
        assert_eq!(versions, 1);
        assert_eq!(table.index("by_name").unwrap().len(), 1);
        assert_eq!(table.index_on(0).unwrap().len(), 1);
    }

    #[test]
    fn maintenance_checkpoints_past_the_threshold_and_close_joins() {
        let storage = disk();
        let config = Config { checkpoint_bytes: 1, ..Config::default() };

        let engine = reopen_with(&storage, config);
        engine.start_maintenance(Duration::from_millis(5));
        let table = users(&engine);
        insert(&engine, &table, &[(1, "a")]);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !storage.exists(Path::new("db/MANIFEST")) {
            assert!(std::time::Instant::now() < deadline, "no checkpoint");
            thread::sleep(Duration::from_millis(5));
        }

        engine.close().unwrap();
        drop(engine);
        assert_eq!(all_names(&reopen(&storage), "users"), ["a"]);
    }

    #[test]
    fn a_directory_opens_once() {
        let dir = std::env::temp_dir().join(format!("umbra-engine-lock-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let engine = Engine::open(&dir, Config::default()).unwrap();
        assert!(Engine::open(&dir, Config::default()).is_err());
        engine.close().unwrap();
        drop(engine);

        Engine::open(&dir, Config::default()).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    type Step = fn(&Engine) -> Result<()>;

    fn put(engine: &Engine, table: &str, rows: &[(i128, &str)]) -> Result<()> {
        let table = engine.table(table)?;
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        for &(id, name) in rows {
            txn.insert(&table, row(id, name))?;
        }
        txn.commit()
    }

    fn rewrite(engine: &Engine, table: &str, f: fn(i128) -> Option<&'static str>) -> Result<()> {
        let table = engine.table(table)?;
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        let mut scan = txn.scan(&table);
        let mut rows = Vec::new();
        while scan.next_batch(&mut rows) {}

        for (id, current) in rows {
            let Value::Number(key) = current[0] else {
                unreachable!()
            };
            match f(key) {
                Some(name) => txn.update(&table, id, row(key, name))?,
                None => txn.delete(&table, id)?,
            }
        }
        txn.commit()
    }

    /// a sequential workload touching every record kind, two checkpoints and a drop
    fn crash_workload() -> Vec<Step> {
        vec![
            |e| users_named_with_text(e, "a"),
            |e| put(e, "a", &[(1, "one"), (2, "two")]),
            |e| put(e, "a", &[(3, "three")]),
            |e| e.create_index("a_name", "a", "name", false),
            |e| rewrite(e, "a", |k| (k != 2).then_some("changed")),
            |e| e.maintain().map(|_| ()),
            |e| e.checkpoint().map(|_| ()),
            |e| users_named_with_text(e, "b"),
            |e| put(e, "b", &[(10, "ten"), (11, "eleven")]),
            |e| put(e, "a", &[(4, "four"), (5, "five")]),
            |e| e.drop_table("b"),
            |e| rewrite(e, "a", |k| (k % 2 == 0).then_some("even")),
            |e| e.maintain().map(|_| ()),
            |e| e.checkpoint().map(|_| ()),
            |e| put(e, "a", &[(6, "six")]),
            |e| users_named_with_text(e, "b"),
            |e| put(e, "b", &[(12, "twelve")]),
        ]
    }

    fn users_named_with_text(engine: &Engine, name: &str) -> Result<()> {
        let schema = SchemaBuilder::new()
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        engine.create_table(name, schema).map(|_| ())
    }

    /// every table's visible rows, by table name, for state comparison
    fn contents(engine: &Engine) -> std::collections::BTreeMap<String, Vec<Vec<Value>>> {
        engine
            .tables()
            .into_iter()
            .map(|table| {
                let mut txn = engine.begin(IsolationLevel::Snapshot);
                let mut scan = txn.scan(&table);
                let mut rows = Vec::new();
                while scan.next_batch(&mut rows) {}

                let mut rows: Vec<Vec<Value>> = rows.iter().map(|(_, row)| row.to_vec()).collect();
                rows.sort();
                (table.name().to_string(), rows)
            })
            .collect()
    }

    #[test]
    fn a_crash_before_any_io_operation_recovers_a_committed_prefix() {
        for budget in [None, Some(0)] {
            crash_sweep(Config { max_resident_chunks: budget, ..Config::default() });
        }
    }

    fn crash_sweep(config: Config) {
        let workload = crash_workload();
        let open = |storage: &MemoryStorage| {
            Engine::open_with(Arc::new(storage.clone()), Path::new("db"), config.clone(), None)
        };

        let expected: Vec<_> = {
            let engine = Engine::in_memory();
            let mut states = vec![contents(&engine)];
            for step in &workload {
                step(&engine).unwrap();
                states.push(contents(&engine));
            }
            states
        };

        let operations = {
            let storage = disk();
            let engine = open(&storage).unwrap();
            workload.iter().for_each(|step| step(&engine).unwrap());
            storage.operations()
        };
        assert!(operations > 40, "the workload exercises too little I/O");

        for crash in 1..=operations {
            for torn in [false, true] {
                let storage = disk();
                storage.crash_at(crash);

                let acknowledged = match open(&storage) {
                    Ok(engine) => workload.iter().take_while(|step| step(&engine).is_ok()).count(),
                    _ => 0,
                };

                storage.reboot(torn);
                let recovered = contents(&open(&storage).unwrap());
                let allowed = &expected[acknowledged..(acknowledged + 2).min(expected.len())];

                assert!(
                    allowed.contains(&recovered),
                    "crash before operation {crash} (torn: {torn}), {acknowledged} steps acknowledged: {recovered:?}"
                );

                let again = contents(&open(&storage).unwrap());
                assert_eq!(again, recovered, "recovery is not idempotent");
            }
        }
    }

    #[test]
    fn a_damaged_manifest_is_an_error_not_an_empty_database() {
        let storage = disk();
        {
            let engine = reopen(&storage);
            let table = users(&engine);
            insert(&engine, &table, &[(1, "a")]);
            engine.checkpoint().unwrap();
        }

        let path = Path::new("db/MANIFEST");
        let mut bytes = storage.read(path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        storage.create(path).unwrap().append(&bytes).unwrap();

        let opened =
            Engine::open_with(Arc::new(storage.clone()), Path::new("db"), Config::default(), None);
        assert!(matches!(opened, Err(EngineError::Corrupt(_))));
    }

    fn budgeted(storage: &MemoryStorage, chunks: usize) -> Engine {
        let config = Config { max_resident_chunks: Some(chunks), ..Config::default() };
        Engine::open_with(Arc::new(storage.clone()), Path::new("db"), config, None).unwrap()
    }

    fn resident(engine: &Engine) -> usize {
        engine.inner.cold.as_ref().unwrap().resident()
    }

    /// a users table of `rows` rows (ids 0..rows), committed in one transactions
    fn populated(engine: &Engine, rows: i128) -> Arc<Table> {
        let table = users(engine);
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        for id in 0..rows {
            txn.insert(&table, row(id, &format!("n{id}"))).unwrap();
        }
        txn.commit().unwrap();
        table
    }

    fn lookup(engine: &Engine, table: &Arc<Table>, id: i128) -> Option<String> {
        let index = table.index_on(0).unwrap();
        let key = Value::Number(id);
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.index_range(table, &index, Bound::Included(&key), Bound::Included(&key))
            .first()
            .map(|(_, row)| text(&row[1]))
    }

    #[test]
    fn settled_chunks_are_evicted_and_read_back_transparently() {
        let storage = disk();
        let engine = budgeted(&storage, 1);
        let table = populated(&engine, 5_000);
        assert_eq!(resident(&engine), 5);

        engine.maintain().unwrap();
        assert_eq!(resident(&engine), 1);
        let len = storage.list(&Path::new("db/cold").join(table.id.to_string())).unwrap().len();
        assert_eq!(len, 4);

        assert_eq!(all_names(&engine, "users").len(), 5_000);
        assert_eq!(resident(&engine), 1, "a scan visits evicted chunks without reloading them");

        assert_eq!(lookup(&engine, &table, 7).as_deref(), Some("n7"));
        assert_eq!(resident(&engine), 2, "a point read reloads its chunk");

        // row ids start at 1, so key 2499 lives in row 2 500 (an evicted chunk)
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.update(&table, 2_500, row(2_499, "updated")).unwrap();
        txn.commit().unwrap();
        assert_eq!(lookup(&engine, &table, 2_499).as_deref(), Some("updated"));

        let mut txn = engine.begin(IsolationLevel::Snapshot);
        assert!(matches!(
            txn.insert(&table, row(4_000, "duplicate")),
            Err(EngineError::DuplicatedKey(_))
        ));
        drop(txn);

        engine.maintain().unwrap();
        assert!(resident(&engine) <= 1);
        assert_eq!(all_names(&engine, "users").len(), 5_000);
    }

    #[test]
    fn chunks_with_versions_a_snapshot_needs_stay_resident() {
        let storage = disk();
        let engine = budgeted(&storage, 0);
        let table = populated(&engine, 2_000);

        let pinned = engine.begin(IsolationLevel::Snapshot);
        let mut txn = engine.begin(IsolationLevel::Snapshot);
        txn.update(&table, 1, row(0, "new")).unwrap();
        txn.commit().unwrap();

        engine.maintain().unwrap();
        assert_eq!(resident(&engine), 1, "the chunk holding two versions of row 1 stays");
        drop(pinned);

        let mut writer = engine.begin(IsolationLevel::Snapshot);
        writer.update(&table, 1_500, row(1_499, "uncommitted")).unwrap();
        engine.maintain().unwrap();
        assert_eq!(resident(&engine), 1, "a chunk holding an uncommitted version stays");
        writer.rollback();

        engine.maintain().unwrap();
        assert_eq!(resident(&engine), 0);
    }

    #[test]
    fn recovery_loads_images_larger_than_the_budget() {
        let storage = disk();
        {
            let engine = reopen(&storage);
            populated(&engine, 6_000);
            engine.checkpoint().unwrap();
            insert(&engine, &engine.table("users").unwrap(), &[(10_000, "after")]);
        }

        let engine = budgeted(&storage, 2);
        assert!(resident(&engine) <= 2, "{} chunks resident after recovery", resident(&engine));
        assert_eq!(all_names(&engine, "users").len(), 6_001);
        assert_eq!(
            lookup(&engine, &engine.table("users").unwrap(), 5_999).as_deref(),
            Some("n5999")
        );
    }

    #[test]
    fn transfers_stay_consistent_while_maintenance_evicts() {
        let storage = disk();
        let engine = budgeted(&storage, 1);
        let table = populated(&engine, 4_096);
        let total = |engine: &Engine| -> i128 {
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            let mut scan = txn.scan(&table);
            let mut rows = Vec::new();
            while scan.next_batch(&mut rows) {}
            rows.iter().map(|(_, row)| number(&row[0])).sum()
        };
        let expected = total(&engine);

        let stop = std::sync::atomic::AtomicBool::new(false);
        thread::scope(|scope| {
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    engine.maintain().unwrap();
                }
            });

            for worker in 0..3u64 {
                let (engine, table) = (&engine, &table);
                scope.spawn(move || {
                    let mut seed = worker + 1;
                    for _ in 0..300 {
                        seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                        let (a, b) = ((seed >> 20) % 4_096 + 1, (seed >> 40) % 4_096 + 1);
                        if a == b {
                            continue;
                        }

                        let mut txn = engine.begin(IsolationLevel::Snapshot);
                        let (Some(x), Some(y)) =
                            (txn.get(table, a as i64), txn.get(table, b as i64))
                        else {
                            continue;
                        };
                        let (vx, vy) = (number(&x[0]), number(&y[0]));
                        let moved = txn
                            .update(table, a as i64, row(vx - 1, "moved"))
                            .and_then(|_| txn.update(table, b as i64, row(vy + 1, "moved")));
                        if moved.is_ok() {
                            let _ = txn.commit();
                        }
                    }
                });
            }

            for _ in 0..50 {
                assert_eq!(total(&engine), expected);
            }
            stop.store(true, Ordering::Relaxed);
        });

        assert_eq!(total(&engine), expected);
    }

    #[test]
    fn images_larger_than_the_flush_buffer_round_trip_and_detect_damage() {
        let storage = disk();
        let ts = {
            let engine = reopen(&storage);
            let table = users(&engine);
            let mut txn = engine.begin(IsolationLevel::Snapshot);
            for id in 0..20_000 {
                txn.insert(&table, row(id, &"x".repeat(64 + (id % 17) as usize))).unwrap();
            }
            txn.commit().unwrap();
            engine.checkpoint().unwrap()
        };

        let image = checkpoint::image_path(Path::new("db"), ts, 1);
        let mut bytes = storage.read(&image).unwrap();
        assert!(bytes.len() > 1 << 20, "the image must span several flushes");
        assert_eq!(all_names(&reopen(&storage), "users").len(), 20_000);

        let middle = bytes.len() / 2;
        bytes[middle] ^= 0x01;
        storage.create(&image).unwrap().append(&bytes).unwrap();

        let opened =
            Engine::open_with(Arc::new(storage.clone()), Path::new("db"), Config::default(), None);
        assert!(matches!(opened, Err(EngineError::Corrupt(_))));
    }

    #[test]
    fn index_cursors_walk_in_key_order_across_batches_and_snapshots() {
        let (engine, table) = engine();
        engine.create_index("by_name", "users", "name", false).unwrap();
        let rows: Vec<_> = (0..700).map(|n| (n, "k")).collect();
        let ids = insert(&engine, &table, &rows);

        let mut renamer = engine.begin(IsolationLevel::Snapshot);
        for (n, &id) in ids.iter().enumerate() {
            renamer.update(&table, id, row(n as i128, &format!("k{n:04}"))).unwrap();
        }
        renamer.commit().unwrap();

        let mut old = engine.begin(IsolationLevel::Snapshot);
        let mut rewrite = engine.begin(IsolationLevel::Snapshot);
        rewrite.update(&table, ids[0], row(0, "z")).unwrap();
        rewrite.commit().unwrap();

        let index = table.index("by_name").unwrap();
        let walk = |txn: &mut Transaction, descending: bool| {
            let mut cursor = txn.index_cursor(&table, &index, descending);
            let mut rows = Vec::new();
            while cursor.next_batch(&mut rows) {}
            rows.iter().map(|(_, row)| text(&row[1])).collect::<Vec<_>>()
        };

        let ascending = walk(&mut old, false);
        assert_eq!(ascending.len(), 700, "each row once, despite stale index entries");
        assert!(ascending.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(ascending[0], "k0000", "the old snapshot still sees the old key");

        let mut fresh = engine.begin(IsolationLevel::Snapshot);
        let descending = walk(&mut fresh, true);
        assert_eq!(descending.first().map(String::as_str), Some("z"));
        assert_eq!(descending.len(), 700);

        let mut cursor = fresh.index_cursor(&table, &index, false);
        let mut first = Vec::new();
        cursor.next_batch(&mut first);
        assert!(first.len() <= 256, "one batch resolves at most one page of entries");
    }
}
