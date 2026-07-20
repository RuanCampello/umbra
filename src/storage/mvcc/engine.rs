use super::FileLock;
use crate::{
    collections::hash::HashMap,
    db::SchemaNew as Schema,
    index,
    sql::Value,
    storage::{
        mvcc::{
            index::{BTreeIndex, Index},
            registry::TransactionRegistry,
            version::{
                TransationVersionStorage, TupleVersion, VersionEntry, VersionStorage, WriteKind,
            },
            wal::WalManager,
            MvccError,
        },
        wal::{WalConfig, WalEntry, WalOperation},
    },
    vm::planner::Tuple,
};
use std::{
    collections::BinaryHeap,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// MVCC storage engine.
/// This provides snapshot isolation with a multi-version concurrency control.
pub(crate) struct Engine {
    path: String,
    schemas: Arc<RwLock<HashMap<String, Arc<Schema>>>>,
    versions: Arc<RwLock<HashMap<String, Arc<VersionStorage>>>>,
    registry: Arc<TransactionRegistry>,
    wal: Arc<Option<WalManager>>,
    is_open: AtomicBool,
    /// This prevents multiple processes of trying to access the same database file.
    file: Mutex<Option<FileLock>>,
    /// This is incremented on any change of the schema.
    /// So we can invalidate the cache without lookups.
    epoch: AtomicU64,
    /// This indicates if we are loading anything from disk to
    /// shun unnecessary writes from [WAL](crate::storage::wal::Wal).
    fetching_disk: Arc<AtomicBool>,
    config: RwLock<Config>,
    clean_up_handle: Mutex<Option<CleanUpThread>>,
    /// Per-transaction local version stores.
    txn_stores: RwLock<HashMap<i64, HashMap<String, TransationVersionStorage>>>,
}

#[derive(Clone, Default)]
pub(crate) struct Config {
    path: String,
    wal: WalConfig,
    cleanup: CleanUpConfig,
    /// hot rows required before a checkpoint freezes a table into a cold
    /// segment, zero disables sealing
    seal_rows: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct CleanUpConfig {
    enabled: bool,
    /// Interval in seconds between clean up
    interval: u64,
    /// Retation period for deleted rows in seconds
    /// Rows deleted after that period will be permanenly removed.
    deleted_rows_retetion: u64,
    /// Retation period for old transactions in seconds
    transaction_retetion: u64,
}

/// This is a [JoinHandle](std::thread::JoinHandle) wrapper for stopping the
/// clean-up thread internally.
pub(in crate::storage::mvcc) struct CleanUpThread {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// Good ol' "styx"
const SNAPSHOT_MAGIC: u32 = 0x73747978;
const SNAPSHOT_VERSION: u32 = 1;

/// magic (4) + version(4) + lsn(8) + time(8) + hash(4)
const SNAPSHOT_HEADER_SIZE: usize = 28;

const IN_MEMORY_PATH: &str = "memory://";

/// Committed hot rows required before a checkpoint freezes a table's first
/// cold segment, later seals trigger at a tenth of it
const DEFAULT_SEAL_ROWS: usize = 100_000;

/// Cold segments a table may accumulate before a checkpoint merges them
const COMPACT_SEGMENTS: usize = 4;

type Result<T> = std::result::Result<T, MvccError>;

impl Engine {
    pub fn new(config: Config) -> Self {
        // an empty path means in-memory: never touch the filesystem,
        // regardless of the WAL configuration
        let wal_path = match config.path.is_empty() {
            true => None,
            _ => Some(Path::new(&config.path)),
        };
        let wal = Some(WalManager::new(wal_path, &config.wal).expect("wal manager not to fail"));

        let path = match config.path.is_empty() {
            true => IN_MEMORY_PATH.to_string(),
            _ => config.path.clone(),
        };

        Self {
            path,
            is_open: AtomicBool::new(false),
            schemas: Arc::new(RwLock::new(HashMap::default())),
            versions: Arc::new(RwLock::new(HashMap::default())),
            wal: Arc::new(wal),
            registry: Arc::new(TransactionRegistry::new()),
            epoch: AtomicU64::new(0),
            file: Mutex::new(None),
            fetching_disk: Arc::new(AtomicBool::new(false)),
            config: RwLock::new(config),
            clean_up_handle: Mutex::new(None),
            txn_stores: RwLock::new(HashMap::default()),
        }
    }

    pub fn in_memory() -> Self {
        Self::new(Config::default())
    }

    pub fn open(&self) -> Result<()> {
        if self.is_open.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        if self.path != IN_MEMORY_PATH {
            let lock = FileLock::acquire(&self.path)?;
            let mut file = self.file.lock().unwrap();
            *file = Some(lock);
        }

        self.registry.accept_transactions();

        if let Some(ref wal) = *self.wal {
            if wal.is_enabled() {
                wal.start()?;

                self.fetching_disk.store(true, Ordering::Release);
                let lsn = self.load_snapshots()?;
                self.replay(lsn)?;

                {
                    let storages = self.versions.read().unwrap();
                    storages
                        .values()
                        .for_each(|storage| storage.rebuild_indexes());
                }

                self.fetching_disk.store(false, Ordering::Release);
            }
        }

        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        if self
            .is_open
            .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        {
            let mut handle = self.clean_up_handle.lock().unwrap();
            if let Some(mut handle) = handle.take() {
                handle.stop()
            }
        }

        self.registry.deny_transactions();

        let storages = self.versions.read().unwrap();
        storages.values().for_each(|storage| storage.close());
        drop(storages);

        if let Some(ref wal) = *self.wal {
            if let Err(err) = wal.stop() {
                eprintln!("Warning: Error stopping wal manager: {err}");
            }
        }

        {
            let mut file_lock = self.file.lock().unwrap();
            *file_lock = None;
        }

        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.is_open.load(Ordering::Acquire)
    }

    pub fn create_table(&self, schema: Schema) -> Result<Schema> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        let name = schema.name.to_string();

        let version = Arc::new(VersionStorage::with_checker(
            schema.name.clone(),
            schema.clone(),
            self.registry.clone(),
        ));

        add_primary_index(&schema, &version);
        self.attach_cold_store(&name, &version)?;

        let data = Vec::from(&schema);
        let returned = schema.clone();

        {
            let mut schemas = self.schemas.write().unwrap();
            schemas.insert(name.clone(), Arc::new(schema));
        }

        {
            let mut storage = self.versions.write().unwrap();
            storage.insert(name, version);
        }

        self.record_ddl(&returned.name, WalOperation::CreateTable, &data)?;
        self.epoch.fetch_add(1, Ordering::Release);

        Ok(returned)
    }

    pub fn drop_table(&self, name: &str) -> Result<()> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        if self.schemas.write().unwrap().remove(name).is_none() {
            return Err(MvccError::TableNotFound);
        }

        if let Some(storage) = self.versions.write().unwrap().remove(name) {
            storage.close();
        }
        if let Some(dir) = self.segments_dir(name) {
            let _ = std::fs::remove_dir_all(dir);
        }

        self.record_ddl(name, WalOperation::DropTable, &[])?;
        self.epoch.fetch_add(1, Ordering::Release);

        Ok(())
    }

    /// Monotonic schema-change counter, bumped on every DDL. Sessions compare
    /// it against their last-seen value to drop stale cached metadata after a
    /// concurrent connection changes the catalogue.
    #[inline]
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn does_table_exists(&self, name: &str) -> Result<bool> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        let schemas = self.schemas.read().unwrap();
        Ok(schemas.contains_key(name))
    }

    pub fn schema(&self, table: &str) -> Result<Arc<Schema>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(table).cloned().ok_or(MvccError::TableNotFound)
    }

    fn record_ddl(&self, name: &str, operation: WalOperation, schema: &[u8]) -> Result<()> {
        if self.must_skip_wal() {
            return Ok(());
        }

        if let Some(ref wal) = *self.wal {
            wal.record_ddl(name, operation, schema)?;
        }

        Ok(())
    }

    fn segments_dir(&self, table: &str) -> Option<PathBuf> {
        match self.path.as_str() {
            IN_MEMORY_PATH => None,
            path => Some(Path::new(path).join("segments").join(table)),
        }
    }

    /// opens the table's segment store and hands it to the version storage
    fn attach_cold_store(&self, table: &str, storage: &Arc<VersionStorage>) -> Result<()> {
        if let Some(dir) = self.segments_dir(table) {
            let store = crate::storage::segment::SegmentStore::open(dir)?;
            storage.attach_cold(Arc::new(store));
        }

        Ok(())
    }

    fn must_skip_wal(&self) -> bool {
        self.fetching_disk.load(Ordering::Acquire)
    }

    pub fn cleanup(self: &Arc<Self>) {
        let config = self.config.read().unwrap();
        if !config.cleanup.enabled {
            return;
        }

        let interval = Duration::from_secs(config.cleanup.interval);
        let deleted_retetion = Duration::from_secs(config.cleanup.deleted_rows_retetion);
        let transaction_retetion = Duration::from_secs(config.cleanup.transaction_retetion);
        drop(config);

        let handle = self.periodic_cleanup(interval, deleted_retetion, transaction_retetion);
        let mut clean_up_handle = self.clean_up_handle.lock().unwrap();
        *clean_up_handle = Some(handle);
    }

    fn periodic_cleanup(
        self: &Arc<Self>,
        interval: Duration,
        deleted_retetion: Duration,
        transaction_retetion: Duration,
    ) -> CleanUpThread {
        use std::thread;

        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let engine = Arc::clone(&self);

        let thread = Some(thread::spawn(move || {
            while !stop_clone.load(Ordering::Acquire) {
                let check_interval = Duration::from_millis(100);
                let mut elapsed = Duration::ZERO;

                while elapsed < interval && !stop_clone.load(Ordering::Acquire) {
                    thread::sleep(check_interval);
                    elapsed += check_interval;
                }

                if stop_clone.load(Ordering::Acquire) {
                    break;
                }

                let _ = engine.cleanup_transactions(transaction_retetion);
                let _ = engine.cleanup_deleted_rows(transaction_retetion);
                let _ = engine.cleanup_old_versions(transaction_retetion);
                let _ = engine.checkpoint();
            }
        }));

        CleanUpThread { stop, thread }
    }

    fn cleanup_transactions(&self, max_age: Duration) -> i32 {
        if !self.is_open() {
            return 0;
        }

        self.registry.cleanup_transactions(max_age)
    }

    fn cleanup_deleted_rows(&self, max_age: Duration) -> i32 {
        if !self.is_open() {
            return 0;
        }

        let mut storage = self.versions.read().unwrap();
        let mut deleted = 0;

        storage
            .values()
            .for_each(|storage| deleted += storage.cleanup(max_age));

        deleted
    }

    fn cleanup_old_versions(&self, max_age: Duration) -> i32 {
        if !self.is_open() {
            return 0;
        }

        let mut storage = self.versions.read().unwrap();
        let mut total = 0;

        storage
            .values()
            .for_each(|storage| total += storage.cleanup_versions(max_age));

        total
    }

    fn load_snapshots(&self) -> Result<u64> {
        let wal = match self.wal.as_ref() {
            Some(wal) if wal.is_enabled() => wal,
            _ => return Ok(0),
        };

        let dir = wal.dir.join("snapshot");
        if !dir.exists() {
            return Ok(0);
        }

        let lsn = deserialise_snapshot_header(&dir.join("header.bin"));
        let mut max_header_lsn = 0u64;

        let Ok(tables) = std::fs::read_dir(&dir) else {
            return Ok(0);
        };

        for entry in tables.flatten() {
            if !entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false) {
                continue;
            }

            let table_name = entry.file_name().to_string_lossy().to_string();
            if let Some(path) = self.latest_snapshot(&entry.path()) {
                let lsn = self.load_table(&table_name, &path)?;
                if lsn > max_header_lsn {
                    max_header_lsn = lsn;
                }
            };
        }

        Ok(std::cmp::max(lsn, max_header_lsn))
    }

    fn latest_snapshot(&self, dir: &Path) -> Option<PathBuf> {
        // PERFORMANCE: test with `Vec`
        let mut snapshots = std::fs::read_dir(dir)
            .ok()?
            .filter_map(|f| f.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("snapshot-") && n.ends_with(".bin"))
                    .unwrap_or(false)
            })
            .collect::<BinaryHeap<_>>();

        snapshots.pop()
    }

    fn replay(&self, lsn: u64) -> Result<()> {
        let wal = match self.wal.as_ref() {
            Some(wal) if wal.is_enabled() => wal,
            _ => return Ok(()),
        };

        let info = wal.replay(lsn, |entry| self.apply_entry(entry))?;
        if info.skipped > 0 {
            eprintln!(
                "Recovery skipped {} from aborted or committed transactions",
                info.skipped
            )
        }

        // ids seen in the log must never be handed out again, even those of
        // uncommitted transactions whose entries were skipped
        if info.max_txn_id > 0 {
            self.registry.recover_aborted_transaction(info.max_txn_id);
        }

        Ok(())
    }

    fn apply_entry(&self, entry: WalEntry) -> Result<()> {
        use crate::storage::wal::WalOperation;

        Ok(match entry.operation {
            WalOperation::CreateTable => {
                let schema = Schema::try_from(entry.data.as_ref())?;
                let version_storage = Arc::new(VersionStorage::with_checker(
                    schema.name.clone(),
                    schema.clone(),
                    self.registry.clone(),
                ));

                add_primary_index(&schema, &version_storage);
                self.attach_cold_store(&schema.name, &version_storage)?;
                let table = schema.name.clone();

                {
                    let mut schemas = self.schemas.write().unwrap();
                    schemas.insert(table.clone(), Arc::new(schema));
                }

                {
                    let mut storages = self.versions.write().unwrap();
                    storages.insert(table, version_storage);
                }
            }

            WalOperation::DropTable => {
                let table = entry.table;

                let mut schemas = self.schemas.write().unwrap();
                schemas.remove(&table);

                let mut storages = self.versions.write().unwrap();
                if let Some(storage) = storages.remove(&table) {
                    storage.close()
                };

                if let Some(dir) = self.segments_dir(&table) {
                    let _ = std::fs::remove_dir_all(dir);
                }
            }

            WalOperation::Update | WalOperation::Insert => {
                let tuple_version = TupleVersion::try_from(entry.data.as_ref())?;
                let table = entry.table;

                let storage = self.version_storage(&table)?;
                storage.recover_version(tuple_version);
            }

            WalOperation::Delete => {
                let tuple_version = TupleVersion::try_from(entry.data.as_ref())?;
                let table = entry.table;

                let storage = self.version_storage(&table)?;
                storage.mark_deleted(tuple_version.row_id, tuple_version.txn_id);
            }

            WalOperation::CreateIndex => {
                let (name, col, unique) = decode_index(entry.data.as_ref())?;
                let storage = self.version_storage(&entry.table)?;

                // registration only: `open()` rebuilds every index after replay
                storage.add_index(name.clone(), Arc::new(BTreeIndex::new(name, col, unique)));
            }

            WalOperation::Commit if entry.txn_id > 0 => {
                self.registry.recover_commit(entry.txn_id);
            }

            _ => {}
        })
    }

    /// Persists a consistent snapshot of every table, then checkpoints the WAL
    ///
    /// Returns `false` without doing anything when the WAL is disabled or
    /// transactions are still active, a snapshot must only capture committed
    /// state, and WAL truncation would lose entries an active transaction
    /// still needs on recovery
    pub fn checkpoint(&self) -> Result<bool> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        let wal = match self.wal.as_ref() {
            Some(wal) if wal.is_enabled() => wal,
            _ => return Ok(false),
        };

        if self.registry.active_transaction_count() > 0 {
            return Ok(false);
        }

        let lsn = wal.current_lsn();
        let snapshot_dir = wal.dir.join("snapshot");
        let keep = wal.snapshot_keep();

        let tables: Vec<(Arc<Schema>, Arc<VersionStorage>)> = {
            let schemas = self.schemas.read().unwrap();
            let versions = self.versions.read().unwrap();

            versions
                .iter()
                .filter_map(|(name, storage)| {
                    schemas
                        .get(name)
                        .map(|schema| (Arc::clone(schema), Arc::clone(storage)))
                })
                .collect()
        };

        let seal_rows = self.config.read().unwrap().seal_rows;

        for (schema, storage) in tables {
            // freeze hot rows into a cold segment before snapshotting, so
            // the snapshot only carries the hot residue
            // safe here: no transactions are active (checked above), so every committed
            // row is visible to all future readers
            if seal_rows > 0 {
                if let Some(store) = storage.cold() {
                    let threshold = match store.is_empty() {
                        true => seal_rows,
                        false => std::cmp::max(seal_rows / 10, 1),
                    };

                    if storage.live_row_count() >= threshold {
                        let rows = storage.extract_for_freeze();
                        if !rows.is_empty() {
                            store.seal(&schema, rows)?;
                        }
                    }

                    store.compact(&schema, COMPACT_SEGMENTS)?;
                }
            }

            let table_dir = snapshot_dir.join(&schema.name);
            std::fs::create_dir_all(&table_dir)?;

            write_table_snapshot(&table_dir, &schema, &storage, lsn)?;
            prune_snapshots(&table_dir, keep);
        }

        // snapshot dirs of dropped tables would resurrect on restart once
        // their DropTable WAL entry is pruned by this very checkpoint
        if let Ok(entries) = std::fs::read_dir(&snapshot_dir) {
            let schemas = self.schemas.read().unwrap();
            for entry in entries.filter_map(|e| e.ok()) {
                if !entry.file_type().map(|ty| ty.is_dir()).unwrap_or(false) {
                    continue;
                }

                let name = entry.file_name().to_string_lossy().to_string();
                if !schemas.contains_key(&name) {
                    let _ = std::fs::remove_dir_all(entry.path());
                }
            }
        }

        serialise_snapshot_header(&snapshot_dir.join("header.bin"), lsn)?;
        wal.checkpoint(Vec::new())?;

        Ok(true)
    }

    fn load_table(&self, name: &str, snapshot: &Path) -> Result<u64> {
        let data = std::fs::read(snapshot)?;
        if data.len() < 24 {
            return Err(MvccError::Other(format!(
                "Snapshot for table {name} is too short"
            )));
        }

        let (content, footer) = data.split_at(data.len() - 4);
        let expected = u32::from_le_bytes(footer.try_into().unwrap());
        if super::fnv1a(content) != expected {
            return Err(MvccError::Other(format!(
                "Snapshot checksum mismatch for table {name}"
            )));
        }

        let mut cursor = 0usize;
        let mut take = |len: usize| -> Result<&[u8]> {
            match content.get(cursor..cursor + len) {
                Some(bytes) => {
                    cursor += len;
                    Ok(bytes)
                }
                None => Err(MvccError::Other(format!(
                    "Snapshot for table {name} is truncated"
                ))),
            }
        };

        let magic = u32::from_le_bytes(take(4)?.try_into().unwrap());
        if magic != SNAPSHOT_MAGIC {
            return Err(MvccError::Other(format!(
                "Wrong snapshot magic for table {name}"
            )));
        }

        let version = u32::from_le_bytes(take(4)?.try_into().unwrap());
        if version != SNAPSHOT_VERSION {
            return Err(MvccError::Other(format!(
                "Unsupported snapshot version {version} for table {name}"
            )));
        }

        let lsn = u64::from_le_bytes(take(8)?.try_into().unwrap());

        let schema_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
        let schema = Schema::try_from(take(schema_len)?)?;

        let row_count = u64::from_le_bytes(take(8)?.try_into().unwrap()) as usize;
        let mut rows = Vec::with_capacity(row_count);
        let mut max_txn_id = 0i64;

        for _ in 0..row_count {
            let row_len = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
            let row = TupleVersion::try_from(take(row_len)?)?;

            max_txn_id = max_txn_id.max(row.txn_id).max(row.deleted_at_txn_id);
            rows.push(row);
        }

        let storage = Arc::new(VersionStorage::with_checker(
            schema.name.clone(),
            schema.clone(),
            self.registry.clone(),
        ));
        add_primary_index(&schema, &storage);
        self.attach_cold_store(name, &storage)?;

        for row in rows {
            storage.recover_version(row);
        }

        if max_txn_id > 0 {
            self.registry.recover_aborted_transaction(max_txn_id);
        }

        let table = schema.name.clone();
        self.schemas
            .write()
            .unwrap()
            .insert(table.clone(), Arc::new(schema));
        self.versions.write().unwrap().insert(table, storage);

        Ok(lsn)
    }

    fn version_storage(&self, name: &str) -> Result<Arc<VersionStorage>> {
        if !self.is_open.load(Ordering::Acquire) {
            return Err(MvccError::NotOpen);
        }

        let storages = self.versions.read().unwrap();
        storages
            .get(&name.to_string())
            .cloned()
            .ok_or(MvccError::TableNotFound)
    }

    /// Get or create a `TransationVersionStorage` for the given transaction and table.
    fn with_tvs<F, R>(&self, txn_id: i64, table: &str, f: F) -> Result<R>
    where
        F: FnOnce(&mut TransationVersionStorage) -> std::result::Result<R, MvccError>,
    {
        let storage = self.version_storage(table)?;

        let mut stores = self.txn_stores.write().unwrap();
        let table_stores = stores.entry(txn_id).or_insert_with(HashMap::default);

        let tvs = table_stores
            .entry(table.to_string())
            .or_insert_with(|| TransationVersionStorage::new(txn_id, storage));

        f(tvs)
    }

    pub fn insert(&self, txn_id: i64, table: &str, row_id: i64, data: Tuple) -> Result<()> {
        self.with_tvs(txn_id, table, |tvs| {
            tvs.put(row_id, data.clone(), WriteKind::Insert)
        })?;

        if !self.must_skip_wal() {
            if let Some(ref wal) = *self.wal {
                let version = TupleVersion::new(txn_id, row_id, data);
                wal.record_dml(table, txn_id, row_id, WalOperation::Insert, version)?;
            }
        }

        Ok(())
    }

    /// Update a row in a table.
    pub fn update(&self, txn_id: i64, table: &str, row_id: i64, data: Tuple) -> Result<()> {
        self.with_tvs(txn_id, table, |tvs| {
            tvs.put(row_id, data.clone(), WriteKind::Update)
        })?;

        if !self.must_skip_wal() {
            if let Some(ref wal) = *self.wal {
                let version = TupleVersion::new(txn_id, row_id, data);
                wal.record_dml(table, txn_id, row_id, WalOperation::Update, version)?;
            }
        }

        Ok(())
    }

    /// Delete a row from a table.
    pub fn delete(&self, txn_id: i64, table: &str, row_id: i64) -> Result<()> {
        self.with_tvs(txn_id, table, |tvs| {
            tvs.put(row_id, Vec::new(), WriteKind::Delete)
        })?;

        if !self.must_skip_wal() {
            if let Some(ref wal) = *self.wal {
                let mut version = TupleVersion::new(txn_id, row_id, Vec::new());
                version.deleted_at_txn_id = txn_id;
                wal.record_dml(table, txn_id, row_id, WalOperation::Delete, version)?;
            }
        }

        Ok(())
    }

    /// Scan all visible rows for a given transaction, ordered by row id.
    ///
    /// Local uncommitted writes override the parent `VersionStorage` scan
    /// (read-your-own-writes): locally updated rows show their new data,
    /// locally deleted rows disappear, and local inserts appear.
    pub fn scan(&self, txn_id: i64, table: &str) -> Result<Vec<(i64, Tuple)>> {
        let storage = self.version_storage(table)?;
        let mut results = storage.scan_visible(txn_id);

        let stores = self.txn_stores.read().unwrap();
        let local = stores
            .get(&txn_id)
            .and_then(|tables| tables.get(table))
            .and_then(|tvs| tvs.local.as_ref())
            .filter(|local| !local.is_empty());

        if let Some(local) = local {
            results.retain(|(row_id, _)| !local.contains_key(row_id));

            for (&row_id, chain) in local.iter() {
                match chain.last() {
                    Some(version) if !version.is_deleted() => {
                        results.push((row_id, version.data.clone()))
                    }
                    _ => {}
                }
            }

            results.sort_unstable_by_key(|&(row_id, _)| row_id);
        }

        Ok(results)
    }

    /// Hands out the next unused row id for a table.
    pub fn next_row_id(&self, table: &str) -> Result<i64> {
        Ok(self.version_storage(table)?.allocate_row_id())
    }

    /// Commit a transaction: detect conflicts, update indexes, flush local writes,
    /// release claims, record WAL commit, and commit in registry.
    pub fn commit_transaction(&self, txn_id: i64) -> Result<()> {
        let mut stores = self.txn_stores.write().unwrap();
        if let Some(mut table_stores) = stores.remove(&txn_id) {
            for (_, tvs) in table_stores.iter_mut() {
                tvs.commit()?;
            }
        }
        drop(stores);

        if let Some(ref wal) = *self.wal {
            wal.record_commit(txn_id)?;
        }

        self.registry.commit_transaction(txn_id);
        Ok(())
    }

    /// Rollback a transaction: discard local writes, release claims, record WAL rollback.
    pub fn rollback_transaction(&self, txn_id: i64) -> Result<()> {
        let mut stores = self.txn_stores.write().unwrap();
        if let Some(mut table_stores) = stores.remove(&txn_id) {
            for (_, tvs) in table_stores.iter_mut() {
                tvs.rollback();
            }
        }
        drop(stores);

        if let Some(ref wal) = *self.wal {
            wal.record_rollback(txn_id)?;
        }

        self.registry.abort_transaction(txn_id);
        Ok(())
    }

    pub fn create_index(&self, name: &str, table: &str, column: &str, unique: bool) -> Result<()> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        let schema = self.schema(table)?;
        let col = schema
            .columns
            .iter()
            .position(|c| c.name() == column)
            .ok_or_else(|| MvccError::Other(format!("Column {column} does not exist")))?;

        let storage = self.version_storage(table)?;
        if storage.get_index(name).is_some() {
            return Err(MvccError::Other(format!("Index {name} already exists")));
        }

        let index = Arc::new(BTreeIndex::new(name.to_string(), col, unique));
        for row in storage.snapshot_rows() {
            if let Some(value) = row.data.get(col) {
                index
                    .add(value, row.row_id)
                    .map_err(|_| MvccError::DuplicatedKey(value.clone()))?;
            }
        }

        storage.add_index(name.to_string(), index);
        self.record_ddl(
            table,
            WalOperation::CreateIndex,
            &encode_index(name, col, unique),
        )?;
        self.epoch.fetch_add(1, Ordering::Release);

        Ok(())
    }

    pub fn max_column_value(&self, table: &str, column: usize) -> Result<Option<i128>> {
        Ok(self.version_storage(table)?.max_number_at(column))
    }

    /// Allocates the next serial value for a column from the shared per-table
    /// counter. The overflow check against the column type lives at the session
    /// layer, which owns the SQL error types.
    pub fn next_serial(&self, table: &str, column: usize) -> Result<i128> {
        Ok(self.version_storage(table)?.next_serial(column) as i128)
    }

    pub fn index_for_column(&self, table: &str, column: usize) -> Result<Option<String>> {
        let storage = self.version_storage(table)?;

        let mut fallback = None;
        for index in storage.get_indexes() {
            if index.column_index() != column {
                continue;
            }

            match index.is_unique() {
                true => return Ok(Some(index.name().to_string())),
                false => fallback = Some(index.name().to_string()),
            }
        }

        Ok(fallback)
    }

    /// Whether a transaction holds uncommitted local writes for a table
    pub fn has_local_writes(&self, txn_id: i64, table: &str) -> bool {
        self.txn_stores
            .read()
            .unwrap()
            .get(&txn_id)
            .and_then(|tables| tables.get(table))
            .is_some_and(|tvs| tvs.has_local_changes())
    }

    /// Scan an index for rows matching a specific value, filtering by MVCC visibility.
    pub fn scan_index(
        &self,
        txn_id: i64,
        table: &str,
        index_name: &str,
        value: &Value,
    ) -> Result<Vec<(i64, Tuple)>> {
        let storage = self.version_storage(table)?;

        let index = storage
            .get_index(index_name)
            .ok_or(MvccError::TableNotFound)?;

        let row_ids = index.find(value);

        let mut results = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            if let Some(version) = storage.get_visible_version(row_id, txn_id) {
                if !version.is_deleted() {
                    results.push((row_id, version.data));
                }
            }
        }

        Ok(results)
    }

    /// Scan an index for rows whose keys fall within the given bounds,
    /// filtering by MVCC visibility.
    pub fn scan_index_range(
        &self,
        txn_id: i64,
        table: &str,
        index_name: &str,
        start: std::ops::Bound<Value>,
        end: std::ops::Bound<Value>,
    ) -> Result<Vec<(i64, Tuple)>> {
        let storage = self.version_storage(table)?;

        let index = storage
            .get_index(index_name)
            .ok_or(MvccError::TableNotFound)?;

        let row_ids = index.find_range(start.as_ref(), end.as_ref());

        let mut results = Vec::with_capacity(row_ids.len());
        for row_id in row_ids {
            if let Some(version) = storage.get_visible_version(row_id, txn_id) {
                if !version.is_deleted() {
                    results.push((row_id, version.data));
                }
            }
        }

        Ok(results)
    }

    /// Get a single visible row by its row ID.
    pub fn get(&self, txn_id: i64, table: &str, row_id: i64) -> Result<Option<Tuple>> {
        let stores = self.txn_stores.read().unwrap();
        if let Some(table_stores) = stores.get(&txn_id) {
            if let Some(tvs) = table_stores.get(table) {
                if let Some(local) = tvs.get_local_version(row_id) {
                    if local.is_deleted() {
                        return Ok(None);
                    }
                    return Ok(Some(local.data.clone()));
                }
            }
        }
        drop(stores);

        let storage = self.version_storage(table)?;
        match storage.get_visible_version(row_id, txn_id) {
            Some(version) if !version.is_deleted() => Ok(Some(version.data)),
            _ => Ok(None),
        }
    }

    /// Begin a new transaction through the registry.
    pub fn begin_transaction(&self) -> Result<(i64, i64)> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        Ok(self.registry.begin())
    }
}

impl Config {
    fn with_path<P: Into<String>>(path: P) -> Self {
        Self {
            path: path.into(),
            wal: WalConfig {
                enabled: false,
                ..Default::default()
            },
            cleanup: Default::default(),
            seal_rows: 0,
        }
    }

    /// a configuration with the WAL enabled: changes survive restarts
    pub fn durable<P: Into<String>>(path: P) -> Self {
        Self {
            path: path.into(),
            wal: WalConfig::default(),
            cleanup: Default::default(),
            seal_rows: DEFAULT_SEAL_ROWS,
        }
    }

    /// overrides the freeze threshold
    pub fn seal_after(mut self, rows: usize) -> Self {
        self.seal_rows = rows;
        self
    }
}

impl Default for CleanUpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: 60,
            deleted_rows_retetion: 300,
            transaction_retetion: 3600,
        }
    }
}

impl CleanUpThread {
    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);

        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for CleanUpThread {
    fn drop(&mut self) {
        self.stop()
    }
}

/// writes one immutable snapshot file for a table: schema plus every live
/// committed row, checksummed, via atomic tmp+rename
fn write_table_snapshot(
    dir: &Path,
    schema: &Schema,
    storage: &VersionStorage,
    lsn: u64,
) -> Result<()> {
    let sequence = next_snapshot_sequence(dir);
    let path = dir.join(format!("snapshot-{sequence:08}.bin"));

    let rows = storage.snapshot_rows();
    let schema_bytes = Vec::<u8>::from(schema);

    let mut buff = Vec::with_capacity(64 + schema_bytes.len() + rows.len() * 64);
    buff.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buff.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buff.extend_from_slice(&lsn.to_le_bytes());
    buff.extend_from_slice(&(schema_bytes.len() as u32).to_le_bytes());
    buff.extend_from_slice(&schema_bytes);
    buff.extend_from_slice(&(rows.len() as u64).to_le_bytes());

    for row in rows {
        let bytes: Vec<u8> = row.try_into()?;
        buff.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        buff.extend_from_slice(&bytes);
    }

    let hash = super::fnv1a(&buff);
    buff.extend_from_slice(&hash.to_le_bytes());

    let temp = path.with_extension("tmp");
    let mut file = File::create(&temp)?;
    file.write_all(&buff)?;
    file.sync_all()?;
    std::fs::rename(&temp, &path)?;

    if let Ok(dir) = File::open(dir) {
        let _ = dir.sync_all();
    }

    Ok(())
}

/// Binary layout for `CreateIndex` WAL payloads:
/// `[name_len u16][name][column u32][unique u8]`
fn encode_index(name: &str, column: usize, unique: bool) -> Vec<u8> {
    let mut buff = Vec::with_capacity(2 + name.len() + 5);
    buff.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buff.extend_from_slice(name.as_bytes());
    buff.extend_from_slice(&(column as u32).to_le_bytes());
    buff.push(unique as u8);

    buff
}

fn decode_index(data: &[u8]) -> Result<(String, usize, bool)> {
    let too_short = || MvccError::Other("Truncated CreateIndex payload".into());

    let name_len =
        u16::from_le_bytes(data.get(0..2).ok_or_else(too_short)?.try_into().unwrap()) as usize;
    let name = String::from_utf8(data.get(2..2 + name_len).ok_or_else(too_short)?.to_vec())
        .map_err(|_| MvccError::Other("Invalid index name".into()))?;

    let cursor = 2 + name_len;
    let column = u32::from_le_bytes(
        data.get(cursor..cursor + 4)
            .ok_or_else(too_short)?
            .try_into()
            .unwrap(),
    ) as usize;
    let unique = *data.get(cursor + 4).ok_or_else(too_short)? != 0;

    Ok((name, column, unique))
}

fn next_snapshot_sequence(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };

    entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| snapshot_sequence(&entry.file_name().to_string_lossy()))
        .max()
        .map(|max| max + 1)
        .unwrap_or(0)
}

fn snapshot_sequence(name: &str) -> Option<u64> {
    name.strip_prefix("snapshot-")?
        .strip_suffix(".bin")?
        .parse()
        .ok()
}

fn prune_snapshots(dir: &Path, keep: usize) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };

    let mut snapshots: Vec<(u64, PathBuf)> = entries
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| {
            snapshot_sequence(&entry.file_name().to_string_lossy()).map(|seq| (seq, entry.path()))
        })
        .collect();

    if snapshots.len() <= keep {
        return;
    }

    snapshots.sort_unstable_by_key(|&(seq, _)| seq);
    for (_, path) in snapshots.drain(..snapshots.len() - keep) {
        let _ = std::fs::remove_file(path);
    }
}

fn serialise_snapshot_header(path: &Path, lsn: u64) -> Result<()> {
    let mut buff = Vec::with_capacity(SNAPSHOT_HEADER_SIZE);

    buff.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buff.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buff.extend_from_slice(&lsn.to_le_bytes());

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);

    buff.extend_from_slice(&timestamp.to_le_bytes());

    let hash = super::fnv1a(&buff);
    buff.extend_from_slice(&hash.to_le_bytes());

    let temp = path.with_extension("temp");
    let mut file = File::create(&temp)?;

    file.write_all(&buff)?;
    file.sync_all()?;
    std::fs::rename(&temp, path)?;

    if let Some(parent) = path.parent() {
        if let Ok(file) = File::open(parent) {
            let _ = file.sync_all();
        }
    }

    Ok(())
}

/// Returns 0 if **invalid** or not found.
fn deserialise_snapshot_header(path: &Path) -> u64 {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        _ => return 0,
    };

    if data.len() < SNAPSHOT_HEADER_SIZE {
        return 0;
    }

    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if magic != SNAPSHOT_MAGIC {
        return 0;
    }

    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != SNAPSHOT_VERSION {
        eprintln!("Snapshot version {version} not supported supported: {SNAPSHOT_VERSION}");
        return 0;
    }

    let hash = u32::from_le_bytes(data[24..28].try_into().unwrap());
    let computed_hash = super::fnv1a(&data[0..24]);
    if hash != computed_hash {
        eprintln!("Snapshot header checksum does not match");
        return 0;
    }

    u64::from_le_bytes(data[8..16].try_into().unwrap())
}

/// Creates a virtual primary key index backed by a unique `BTreeIndex`.
///
/// Automatically registered on table creation so the optimiser can use
/// `scan_index` for point lookups on the primary key.
#[inline(always)]
fn add_primary_index(schema: &Schema, version: &Arc<VersionStorage>) {
    if let Some(col_index) = schema.primary_column_index() {
        let name = index!(primary on (schema.name));
        let btree = Arc::new(BTreeIndex::new(name.clone(), col_index, true));
        version.add_index(name, btree);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::{statement::Type, Value};

    #[test]
    fn create() {
        let engine = Engine::in_memory();
        assert!(!engine.is_open());
        assert_eq!(engine.path, IN_MEMORY_PATH);
    }

    #[test]
    fn open_and_close() {
        let engine = Engine::in_memory();

        engine.open().unwrap();
        assert!(engine.is_open());

        engine.close().unwrap();
        assert!(!engine.is_open());
    }

    #[test]
    fn create_table() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();

        let created = engine.create_table(schema).unwrap();
        assert_eq!(created.name, "users");
        assert!(engine.does_table_exists("users").unwrap());

        engine.close().unwrap();
    }

    #[test]
    fn insert_and_scan() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn_id, _) = engine.registry.begin();
        engine
            .insert(
                txn_id,
                "users",
                1,
                vec![Value::Number(1), Value::String("alice".into())],
            )
            .unwrap();
        engine
            .insert(
                txn_id,
                "users",
                2,
                vec![Value::Number(2), Value::String("bob".into())],
            )
            .unwrap();
        engine.commit_transaction(txn_id).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 2);

        engine.close().unwrap();
    }

    #[test]
    fn update_row() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "items",
                1,
                vec![Value::Number(1), Value::String("old".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (txn2, _) = engine.registry.begin();
        engine
            .update(
                txn2,
                "items",
                1,
                vec![Value::Number(1), Value::String("new".into())],
            )
            .unwrap();
        engine.commit_transaction(txn2).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "items").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1[1], Value::String("new".into()));

        engine.close().unwrap();
    }

    #[test]
    fn delete_row() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "items",
                1,
                vec![Value::Number(1), Value::String("a".into())],
            )
            .unwrap();
        engine
            .insert(
                txn1,
                "items",
                2,
                vec![Value::Number(2), Value::String("b".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (txn2, _) = engine.registry.begin();
        engine.delete(txn2, "items", 1).unwrap();
        engine.commit_transaction(txn2).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "items").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
        assert_eq!(rows[0].1[1], Value::String("b".into()));

        engine.close().unwrap();
    }

    #[test]
    fn scan_primary_index() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "users",
                1,
                vec![Value::Number(1), Value::String("alice".into())],
            )
            .unwrap();
        engine
            .insert(
                txn1,
                "users",
                2,
                vec![Value::Number(2), Value::String("bob".into())],
            )
            .unwrap();
        engine
            .insert(
                txn1,
                "users",
                3,
                vec![Value::Number(3), Value::String("carol".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        // point lookup via primary index
        let (reader, _) = engine.registry.begin();
        let pk_index_name = index!(primary on users);

        let results = engine
            .scan_index(reader, "users", &pk_index_name, &Value::Number(2))
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[1], Value::String("bob".into()));

        // missing key returns empty
        let missing = engine
            .scan_index(reader, "users", &pk_index_name, &Value::Number(99))
            .unwrap();
        assert!(missing.is_empty());

        engine.close().unwrap();
    }

    #[test]
    fn scan_primary_index_range() {
        use std::ops::Bound;

        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        for (id, name) in [(1, "alice"), (2, "bob"), (3, "carol"), (4, "dave")] {
            engine
                .insert(
                    txn1,
                    "users",
                    id,
                    vec![Value::Number(id as i128), Value::String(name.into())],
                )
                .unwrap();
        }
        engine.delete(txn1, "users", 3).unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (reader, _) = engine.registry.begin();
        let pk_index_name = index!(primary on users);

        // half-open range, deleted row filtered out by visibility
        let results = engine
            .scan_index_range(
                reader,
                "users",
                &pk_index_name,
                Bound::Excluded(Value::Number(1)),
                Bound::Unbounded,
            )
            .unwrap();
        assert_eq!(
            results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![2, 4]
        );

        // fully bounded range
        let results = engine
            .scan_index_range(
                reader,
                "users",
                &pk_index_name,
                Bound::Included(Value::Number(1)),
                Bound::Excluded(Value::Number(4)),
            )
            .unwrap();
        assert_eq!(
            results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2]
        );

        engine.close().unwrap();
    }

    pub(super) fn scratch_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock must be past the epoch")
            .as_nanos();
        let dir = std::env::temp_dir()
            .join(format!("umbra-engine-{tag}-{}-{nanos}", std::process::id(),));
        std::fs::create_dir_all(&dir).expect("scratch dir must be creatable");

        dir
    }

    pub(super) fn durable_engine(dir: &Path) -> Engine {
        let engine = Engine::new(Config::durable(dir.to_string_lossy().to_string()));
        engine.open().unwrap();

        engine
    }

    #[test]
    fn restart_recovers_committed_state_only() {
        let dir = scratch_dir("recovery");

        {
            let engine = durable_engine(&dir);
            let schema = SchemaBuilder::new("users")
                .primary("id", Type::Integer)
                .nullable("name", Type::Text)
                .build();
            engine.create_table(schema).unwrap();

            let (committed, _) = engine.registry.begin();
            engine
                .insert(
                    committed,
                    "users",
                    1,
                    vec![Value::Number(1), Value::String("alice".into())],
                )
                .unwrap();
            engine
                .insert(
                    committed,
                    "users",
                    2,
                    vec![Value::Number(2), Value::String("bob".into())],
                )
                .unwrap();
            engine.commit_transaction(committed).unwrap();

            let (deleter, _) = engine.registry.begin();
            engine.delete(deleter, "users", 2).unwrap();
            engine.commit_transaction(deleter).unwrap();

            // never committed: must not survive the restart
            let (lost, _) = engine.registry.begin();
            engine
                .insert(
                    lost,
                    "users",
                    3,
                    vec![Value::Number(3), Value::String("ghost".into())],
                )
                .unwrap();

            engine.close().unwrap();
        }

        let engine = durable_engine(&dir);
        assert!(engine.does_table_exists("users").unwrap());

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1[1], Value::String("alice".into()));

        assert!(engine.get(reader, "users", 2).unwrap().is_none());
        assert!(engine.get(reader, "users", 3).unwrap().is_none());

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restart_never_reuses_transaction_or_row_ids() {
        let dir = scratch_dir("id-reuse");

        let max_txn_before;
        {
            let engine = durable_engine(&dir);
            let schema = SchemaBuilder::new("items")
                .primary("id", Type::Integer)
                .nullable("val", Type::Text)
                .build();
            engine.create_table(schema).unwrap();

            let (txn, _) = engine.registry.begin();
            engine
                .insert(
                    txn,
                    "items",
                    5,
                    vec![Value::Number(5), Value::String("five".into())],
                )
                .unwrap();
            engine.commit_transaction(txn).unwrap();

            // uncommitted transaction: its id must still be retired
            let (dangling, _) = engine.registry.begin();
            engine
                .insert(
                    dangling,
                    "items",
                    6,
                    vec![Value::Number(6), Value::String("six".into())],
                )
                .unwrap();
            max_txn_before = dangling;

            engine.close().unwrap();
        }

        let engine = durable_engine(&dir);

        let (fresh, _) = engine.registry.begin();
        assert!(
            fresh > max_txn_before,
            "fresh txn id {fresh} must exceed every logged id {max_txn_before}"
        );

        assert_eq!(engine.next_row_id("items").unwrap(), 6);

        // writes on the recovered state keep working
        engine
            .insert(
                fresh,
                "items",
                6,
                vec![Value::Number(6), Value::String("six".into())],
            )
            .unwrap();
        engine.commit_transaction(fresh).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "items").unwrap();
        assert_eq!(rows.len(), 2);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_snapshots_tables_and_prunes_wal() {
        let dir = scratch_dir("checkpoint");

        {
            let engine = durable_engine(&dir);
            let schema = SchemaBuilder::new("users")
                .primary("id", Type::Integer)
                .nullable("name", Type::Text)
                .build();
            engine.create_table(schema).unwrap();

            let (txn, _) = engine.registry.begin();
            for id in 1..=3i64 {
                engine
                    .insert(
                        txn,
                        "users",
                        id,
                        vec![Value::Number(id as i128), Value::String(format!("u{id}"))],
                    )
                    .unwrap();
            }
            engine.commit_transaction(txn).unwrap();

            let (deleter, _) = engine.registry.begin();
            engine.delete(deleter, "users", 2).unwrap();
            engine.commit_transaction(deleter).unwrap();

            assert!(engine.checkpoint().unwrap());

            assert!(dir.join("snapshot/header.bin").exists());
            assert!(dir.join("snapshot/users/snapshot-00000000.bin").exists());
            assert!(dir.join("wal/checkpoint.meta").exists());
            assert!(
                !dir.join("wal/wal-00000000.log").exists(),
                "pre-checkpoint segment must be pruned"
            );
            assert!(dir.join("wal/wal-00000001.log").exists());

            // survives only in the WAL tail, not in the snapshot
            let (txn, _) = engine.registry.begin();
            engine
                .insert(
                    txn,
                    "users",
                    4,
                    vec![Value::Number(4), Value::String("tail".into())],
                )
                .unwrap();
            engine.commit_transaction(txn).unwrap();

            engine.close().unwrap();
        }

        let engine = durable_engine(&dir);

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        let ids: Vec<i64> = rows.iter().map(|&(row_id, _)| row_id).collect();
        assert_eq!(ids, vec![1, 3, 4]);

        // indexes must be rebuilt after snapshot load + WAL tail replay
        let pk_index_name = index!(primary on users);
        let hits = engine
            .scan_index(reader, "users", &pk_index_name, &Value::Number(4))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1[1], Value::String("tail".into()));

        assert_eq!(engine.next_row_id("users").unwrap(), 5);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn unique_violations_are_rejected() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        engine.create_table(schema).unwrap();

        let (txn, _) = engine.registry.begin();
        engine
            .insert(
                txn,
                "users",
                1,
                vec![Value::Number(7), Value::String("a".into())],
            )
            .unwrap();
        engine.commit_transaction(txn).unwrap();

        // same primary key value on a fresh row id
        let (txn, _) = engine.registry.begin();
        let result = engine.insert(
            txn,
            "users",
            2,
            vec![Value::Number(7), Value::String("b".into())],
        );
        assert!(matches!(result, Err(MvccError::DuplicatedKey(_))));
        engine.rollback_transaction(txn).unwrap();

        // duplicate within a single transaction's local writes
        let (txn, _) = engine.registry.begin();
        engine
            .insert(
                txn,
                "users",
                2,
                vec![Value::Number(8), Value::String("b".into())],
            )
            .unwrap();
        let result = engine.insert(
            txn,
            "users",
            3,
            vec![Value::Number(8), Value::String("c".into())],
        );
        assert!(matches!(result, Err(MvccError::DuplicatedKey(_))));
        engine.rollback_transaction(txn).unwrap();

        // delete + reinsert of the same key inside one transaction is fine
        let (txn, _) = engine.registry.begin();
        engine.delete(txn, "users", 1).unwrap();
        engine
            .insert(
                txn,
                "users",
                4,
                vec![Value::Number(7), Value::String("again".into())],
            )
            .unwrap();
        engine.commit_transaction(txn).unwrap();

        // update moving the key onto an existing one is rejected
        let (txn, _) = engine.registry.begin();
        engine
            .insert(
                txn,
                "users",
                5,
                vec![Value::Number(9), Value::String("e".into())],
            )
            .unwrap();
        engine.commit_transaction(txn).unwrap();

        let (txn, _) = engine.registry.begin();
        let result = engine.update(
            txn,
            "users",
            5,
            vec![Value::Number(7), Value::String("clash".into())],
        );
        assert!(matches!(result, Err(MvccError::DuplicatedKey(_))));
        engine.rollback_transaction(txn).unwrap();

        engine.close().unwrap();
    }

    #[test]
    fn secondary_index_backfill_scan_and_restart() {
        let dir = scratch_dir("secondary-index");

        {
            let engine = durable_engine(&dir);
            let schema = SchemaBuilder::new("users")
                .primary("id", Type::Integer)
                .nullable("email", Type::Text)
                .build();
            engine.create_table(schema).unwrap();

            let (txn, _) = engine.registry.begin();
            for (id, email) in [(1, "a@x"), (2, "b@x")] {
                engine
                    .insert(
                        txn,
                        "users",
                        id,
                        vec![Value::Number(id as i128), Value::String(email.into())],
                    )
                    .unwrap();
            }
            engine.commit_transaction(txn).unwrap();

            // backfills from existing rows
            engine
                .create_index("users_email_idx", "users", "email", true)
                .unwrap();

            let (reader, _) = engine.registry.begin();
            let hits = engine
                .scan_index(
                    reader,
                    "users",
                    "users_email_idx",
                    &Value::String("b@x".into()),
                )
                .unwrap();
            assert_eq!(hits.len(), 1);
            assert_eq!(hits[0].0, 2);

            // uniqueness now enforced through the secondary index
            let (txn, _) = engine.registry.begin();
            let result = engine.insert(
                txn,
                "users",
                3,
                vec![Value::Number(3), Value::String("a@x".into())],
            );
            assert!(matches!(result, Err(MvccError::DuplicatedKey(_))));
            engine.rollback_transaction(txn).unwrap();

            // backfill over duplicates must fail for a unique index
            assert!(matches!(
                engine.create_index("users_email_idx", "users", "email", true),
                Err(MvccError::Other(_))
            ));

            engine.close().unwrap();
        }

        let engine = durable_engine(&dir);

        let (reader, _) = engine.registry.begin();
        let hits = engine
            .scan_index(
                reader,
                "users",
                "users_email_idx",
                &Value::String("a@x".into()),
            )
            .unwrap();
        assert_eq!(hits.len(), 1, "index must survive restart via WAL replay");
        assert_eq!(hits[0].0, 1);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_only_drop_survives_restart() {
        let dir = scratch_dir("wal-drop");

        {
            let engine = durable_engine(&dir);
            let schema = SchemaBuilder::new("gone")
                .primary("id", Type::Integer)
                .build();
            engine.create_table(schema).unwrap();

            let (txn, _) = engine.registry.begin();
            engine
                .insert(txn, "gone", 1, vec![Value::Number(1)])
                .unwrap();
            engine.commit_transaction(txn).unwrap();

            engine.drop_table("gone").unwrap();
            engine.close().unwrap();
        }

        let engine = durable_engine(&dir);
        assert!(
            !engine.does_table_exists("gone").unwrap(),
            "dropped table must not resurrect from WAL replay"
        );

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn dropped_table_stays_dropped_after_checkpoint_and_restart() {
        let dir = scratch_dir("drop-checkpoint");

        {
            let engine = durable_engine(&dir);

            for table in ["kept", "doomed"] {
                let schema = SchemaBuilder::new(table)
                    .primary("id", Type::Integer)
                    .build();
                engine.create_table(schema).unwrap();

                let (txn, _) = engine.registry.begin();
                engine
                    .insert(txn, table, 1, vec![Value::Number(1)])
                    .unwrap();
                engine.commit_transaction(txn).unwrap();
            }

            // both tables land in the snapshot, then one is dropped and a
            // second checkpoint prunes the DropTable WAL entry
            assert!(engine.checkpoint().unwrap());
            engine.drop_table("doomed").unwrap();
            assert!(engine.checkpoint().unwrap());

            assert!(matches!(
                engine.scan(0, "doomed"),
                Err(MvccError::TableNotFound)
            ));

            engine.close().unwrap();
        }

        let engine = durable_engine(&dir);
        assert!(engine.does_table_exists("kept").unwrap());
        assert!(
            !engine.does_table_exists("doomed").unwrap(),
            "dropped table must not resurrect from its stale snapshot"
        );

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoint_refuses_active_transactions() {
        let dir = scratch_dir("checkpoint-active");
        let engine = durable_engine(&dir);

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .build();
        engine.create_table(schema).unwrap();

        let (txn, _) = engine.registry.begin();
        engine
            .insert(txn, "items", 1, vec![Value::Number(1)])
            .unwrap();

        assert!(!engine.checkpoint().unwrap());

        engine.commit_transaction(txn).unwrap();
        assert!(engine.checkpoint().unwrap());

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn snapshot_pruning_keeps_newest_files() {
        let dir = scratch_dir("snapshot-prune");
        let engine = durable_engine(&dir);

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .build();
        engine.create_table(schema).unwrap();

        for round in 0..7i64 {
            let (txn, _) = engine.registry.begin();
            engine
                .insert(
                    txn,
                    "items",
                    round + 1,
                    vec![Value::Number((round + 1) as i128)],
                )
                .unwrap();
            engine.commit_transaction(txn).unwrap();

            assert!(engine.checkpoint().unwrap());
        }

        let snapshots: Vec<String> = std::fs::read_dir(dir.join("snapshot/items"))
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .collect();

        assert!(
            snapshots.len() <= 5,
            "must keep at most 5 snapshots, found {snapshots:?}"
        );
        assert!(snapshots.contains(&"snapshot-00000006.bin".to_string()));

        engine.close().unwrap();

        // latest snapshot restores the full table
        let engine = durable_engine(&dir);
        let (reader, _) = engine.registry.begin();
        assert_eq!(engine.scan(reader, "items").unwrap().len(), 7);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn wal_enabled_dml_round_trip() {
        let dir = scratch_dir("wal-dml");
        let engine = durable_engine(&dir);

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();
        engine.create_table(schema).unwrap();

        let (txn, _) = engine.registry.begin();
        engine
            .insert(
                txn,
                "items",
                1,
                vec![Value::Number(1), Value::String("a".into())],
            )
            .unwrap();
        engine.commit_transaction(txn).unwrap();

        let (txn, _) = engine.registry.begin();
        engine
            .update(
                txn,
                "items",
                1,
                vec![Value::Number(1), Value::String("b".into())],
            )
            .unwrap();
        engine.commit_transaction(txn).unwrap();

        let (rolled, _) = engine.registry.begin();
        engine
            .update(
                rolled,
                "items",
                1,
                vec![Value::Number(1), Value::String("junk".into())],
            )
            .unwrap();
        engine.rollback_transaction(rolled).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "items").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1[1], Value::String("b".into()));

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_overlays_own_uncommitted_writes() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (setup, _) = engine.registry.begin();
        engine
            .insert(
                setup,
                "items",
                1,
                vec![Value::Number(1), Value::String("one".into())],
            )
            .unwrap();
        engine
            .insert(
                setup,
                "items",
                2,
                vec![Value::Number(2), Value::String("two".into())],
            )
            .unwrap();
        engine.commit_transaction(setup).unwrap();

        let (txn, _) = engine.registry.begin();
        engine
            .update(
                txn,
                "items",
                1,
                vec![Value::Number(1), Value::String("updated".into())],
            )
            .unwrap();
        engine.delete(txn, "items", 2).unwrap();
        engine
            .insert(
                txn,
                "items",
                3,
                vec![Value::Number(3), Value::String("new".into())],
            )
            .unwrap();

        let rows = engine.scan(txn, "items").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1[1], Value::String("updated".into()));
        assert_eq!(rows[1].0, 3);
        assert_eq!(rows[1].1[1], Value::String("new".into()));

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "items").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, 1);
        assert_eq!(rows[0].1[1], Value::String("one".into()));
        assert_eq!(rows[1].0, 2);
        assert_eq!(rows[1].1[1], Value::String("two".into()));

        engine.rollback_transaction(txn).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "items").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[1], Value::String("one".into()));

        engine.close().unwrap();
    }

    #[test]
    fn row_id_allocation_continues_after_existing_rows() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn, _) = engine.registry.begin();
        engine
            .insert(
                txn,
                "items",
                7,
                vec![Value::Number(7), Value::String("seven".into())],
            )
            .unwrap();
        engine.commit_transaction(txn).unwrap();

        assert_eq!(engine.next_row_id("items").unwrap(), 8);
        assert_eq!(engine.next_row_id("items").unwrap(), 9);

        engine.close().unwrap();
    }

    #[test]
    fn get_single_row() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "users",
                1,
                vec![Value::Number(1), Value::String("alice".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (reader, _) = engine.registry.begin();
        let row = engine.get(reader, "users", 1).unwrap();
        assert!(row.is_some());
        assert_eq!(
            row.unwrap(),
            vec![Value::Number(1), Value::String("alice".into())]
        );

        let missing = engine.get(reader, "users", 999).unwrap();
        assert!(missing.is_none());

        engine.close().unwrap();
    }

    #[test]
    fn get_deleted_row_returns_none() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "items",
                1,
                vec![Value::Number(1), Value::String("x".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (txn2, _) = engine.registry.begin();
        engine.delete(txn2, "items", 1).unwrap();
        engine.commit_transaction(txn2).unwrap();

        let (reader, _) = engine.registry.begin();
        assert!(engine.get(reader, "items", 1).unwrap().is_none());

        engine.close().unwrap();
    }

    #[test]
    fn write_conflict_on_update() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "items",
                1,
                vec![Value::Number(1), Value::String("a".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (txn2, _) = engine.registry.begin();
        engine
            .update(
                txn2,
                "items",
                1,
                vec![Value::Number(1), Value::String("b".into())],
            )
            .unwrap();

        let (txn3, _) = engine.registry.begin();
        let result = engine.update(
            txn3,
            "items",
            1,
            vec![Value::Number(1), Value::String("c".into())],
        );
        assert!(matches!(result, Err(MvccError::WriteConflict)));

        engine.commit_transaction(txn2).unwrap();
        engine.close().unwrap();
    }

    #[test]
    fn write_conflict_on_delete() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "items",
                1,
                vec![Value::Number(1), Value::String("a".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (txn2, _) = engine.registry.begin();
        engine.delete(txn2, "items", 1).unwrap();
        let (txn3, _) = engine.registry.begin();
        let result = engine.delete(txn3, "items", 1);
        assert!(matches!(result, Err(MvccError::WriteConflict)));

        engine.commit_transaction(txn2).unwrap();
        engine.close().unwrap();
    }

    #[test]
    fn begin_transaction_wrapper() {
        let engine = Engine::in_memory();
        engine.open().unwrap();

        let (txn_id, seq) = engine.begin_transaction().unwrap();
        assert!(txn_id > 0);
        assert!(seq > 0);

        engine.close().unwrap();
    }

    #[test]
    fn snapshot_isolation_read() {
        use crate::storage::mvcc::registry::IsolationLevel;

        let engine = Engine::in_memory();
        engine.open().unwrap();

        let schema = SchemaBuilder::new("items")
            .primary("id", Type::Integer)
            .nullable("val", Type::Text)
            .build();

        engine.create_table(schema).unwrap();

        let (txn1, _) = engine.registry.begin();
        engine
            .insert(
                txn1,
                "items",
                1,
                vec![Value::Number(1), Value::String("original".into())],
            )
            .unwrap();
        engine.commit_transaction(txn1).unwrap();

        let (reader, _) = engine.registry.begin();
        engine
            .registry
            .set_transaction_isolation_level(reader, IsolationLevel::Snapshot);

        let (txn2, _) = engine.registry.begin();
        engine
            .update(
                txn2,
                "items",
                1,
                vec![Value::Number(1), Value::String("updated".into())],
            )
            .unwrap();
        engine.commit_transaction(txn2).unwrap();

        let row = engine.get(reader, "items", 1).unwrap().unwrap();
        assert_eq!(row[1], Value::String("original".into()));

        let (reader2, _) = engine.registry.begin();
        let row2 = engine.get(reader2, "items", 1).unwrap().unwrap();
        assert_eq!(row2[1], Value::String("updated".into()));

        engine.close().unwrap();
    }

    fn sealing_engine(dir: &Path) -> Engine {
        let config = Config::durable(dir.to_string_lossy().to_string()).seal_after(2);
        let engine = Engine::new(config);
        engine.open().unwrap();

        engine
    }

    fn insert_users(engine: &Engine, rows: &[(i64, &str)]) {
        let (txn, _) = engine.registry.begin();
        for &(id, name) in rows {
            engine
                .insert(
                    txn,
                    "users",
                    id,
                    vec![Value::Number(id as i128), Value::String(name.into())],
                )
                .unwrap();
        }
        engine.commit_transaction(txn).unwrap();
    }

    #[test]
    fn checkpoint_freezes_hot_rows_into_cold_segments() {
        let dir = scratch_dir("freeze");
        let engine = sealing_engine(&dir);

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        engine.create_table(schema).unwrap();
        insert_users(&engine, &[(1, "alice"), (2, "bob"), (3, "carol")]);

        assert!(engine.checkpoint().unwrap());

        let storage = engine.version_storage("users").unwrap();
        assert_eq!(storage.live_row_count(), 0, "hot store must be drained");
        let store = storage.cold().expect("cold store attached");
        assert!(!store.is_empty(), "a segment must exist");

        // every read path still resolves the frozen rows
        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].1[1], Value::String("alice".into()));

        let row = engine.get(reader, "users", 2).unwrap().unwrap();
        assert_eq!(row[1], Value::String("bob".into()));

        let pk = index!(primary on users);
        let hits = engine
            .scan_index(reader, "users", &pk, &Value::Number(3))
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].1[1], Value::String("carol".into()));

        // unique constraints still see frozen rows
        let (dup, _) = engine.registry.begin();
        let clash = engine.insert(
            dup,
            "users",
            engine.next_row_id("users").unwrap(),
            vec![Value::Number(2), Value::String("impostor".into())],
        );
        assert!(matches!(clash, Err(MvccError::DuplicatedKey(_))));
        engine.rollback_transaction(dup).unwrap();

        // fresh row ids never collide with frozen ones
        assert!(engine.next_row_id("users").unwrap() > 3);

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn frozen_rows_can_be_updated_and_deleted() {
        let dir = scratch_dir("thaw");
        let engine = sealing_engine(&dir);

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        engine.create_table(schema).unwrap();
        insert_users(&engine, &[(1, "alice"), (2, "bob"), (3, "carol")]);
        assert!(engine.checkpoint().unwrap());

        let (writer, _) = engine.registry.begin();
        engine
            .update(
                writer,
                "users",
                1,
                vec![Value::Number(1), Value::String("alicia".into())],
            )
            .unwrap();
        engine.delete(writer, "users", 2).unwrap();

        // uncommitted writes over frozen rows stay invisible to others
        let (concurrent, _) = engine.registry.begin();
        let rows = engine.scan(concurrent, "users").unwrap();
        assert_eq!(rows.len(), 3, "cold state holds until the writer commits");
        assert_eq!(rows[0].1[1], Value::String("alice".into()));

        engine.commit_transaction(writer).unwrap();

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[1], Value::String("alicia".into()));
        assert_eq!(rows[1].0, 3);
        assert!(engine.get(reader, "users", 2).unwrap().is_none());

        // write-write conflict on a frozen row: second writer must fail
        let (first, _) = engine.registry.begin();
        let (second, _) = engine.registry.begin();
        engine
            .update(
                first,
                "users",
                3,
                vec![Value::Number(3), Value::String("carola".into())],
            )
            .unwrap();
        let conflict = engine.update(
            second,
            "users",
            3,
            vec![Value::Number(3), Value::String("carlota".into())],
        );
        assert!(
            conflict.is_err(),
            "conflicting cold update must be rejected"
        );
        engine.rollback_transaction(second).unwrap();
        engine.commit_transaction(first).unwrap();

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn frozen_state_survives_restart() {
        let dir = scratch_dir("cold-restart");

        {
            let engine = sealing_engine(&dir);
            let schema = SchemaBuilder::new("users")
                .primary("id", Type::Integer)
                .nullable("name", Type::Text)
                .build();
            engine.create_table(schema).unwrap();
            insert_users(&engine, &[(1, "alice"), (2, "bob"), (3, "carol")]);
            assert!(engine.checkpoint().unwrap());

            // post-checkpoint writes live only in the WAL tail
            let (writer, _) = engine.registry.begin();
            engine
                .update(
                    writer,
                    "users",
                    1,
                    vec![Value::Number(1), Value::String("alicia".into())],
                )
                .unwrap();
            engine.delete(writer, "users", 2).unwrap();
            engine.commit_transaction(writer).unwrap();

            engine.close().unwrap();
        }

        let engine = sealing_engine(&dir);
        let (reader, _) = engine.registry.begin();

        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1[1], Value::String("alicia".into()));
        assert_eq!(rows[1].0, 3);
        assert!(engine.get(reader, "users", 2).unwrap().is_none());

        // indexes are rebuilt over hot + cold
        let pk = index!(primary on users);
        let hits = engine
            .scan_index(reader, "users", &pk, &Value::Number(3))
            .unwrap();
        assert_eq!(hits.len(), 1);

        // frozen ids and values still feed watermarks
        assert!(engine.next_row_id("users").unwrap() > 3);
        assert_eq!(engine.max_column_value("users", 0).unwrap(), Some(3));

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cleanup_converts_cold_delete_markers_into_tombstones() {
        let dir = scratch_dir("cold-tombstone");

        {
            let engine = sealing_engine(&dir);
            let schema = SchemaBuilder::new("users")
                .primary("id", Type::Integer)
                .nullable("name", Type::Text)
                .build();
            engine.create_table(schema).unwrap();
            insert_users(&engine, &[(1, "alice"), (2, "bob")]);
            assert!(engine.checkpoint().unwrap());

            let (writer, _) = engine.registry.begin();
            engine.delete(writer, "users", 1).unwrap();
            engine.commit_transaction(writer).unwrap();

            let storage = engine.version_storage("users").unwrap();
            assert!(storage.cleanup(Duration::ZERO) > 0, "marker must be purged");

            let store = storage.cold().unwrap();
            assert!(store.is_tombstoned(1), "purge must leave a tombstone");

            let (reader, _) = engine.registry.begin();
            let rows = engine.scan(reader, "users").unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].0, 2);

            engine.close().unwrap();
        }

        // the tombstone, not the WAL, hides the row after restart
        let engine = sealing_engine(&dir);
        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 2);
        assert!(engine.get(reader, "users", 1).unwrap().is_none());

        engine.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn checkpoints_compact_accumulated_segments() {
        let dir = scratch_dir("cold-compact");
        let engine = sealing_engine(&dir);

        let schema = SchemaBuilder::new("users")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build();
        engine.create_table(schema).unwrap();

        // every checkpoint seals one small segment, the fifth pushes the
        // store past COMPACT_SEGMENTS and the same cycle merges them
        for batch in 0..5i64 {
            let base = batch * 2 + 1;
            insert_users(&engine, &[(base, "even"), (base + 1, "odd")]);
            assert!(engine.checkpoint().unwrap());
        }

        let storage = engine.version_storage("users").unwrap();
        let store = storage.cold().unwrap();
        assert!(!store.is_empty());

        let (reader, _) = engine.registry.begin();
        let rows = engine.scan(reader, "users").unwrap();
        assert_eq!(rows.len(), 10);
        assert_eq!(rows.first().unwrap().0, 1);
        assert_eq!(rows.last().unwrap().0, 10);

        engine.close().unwrap();

        let engine = sealing_engine(&dir);
        let (reader, _) = engine.registry.begin();
        assert_eq!(engine.scan(reader, "users").unwrap().len(), 10);
        engine.close().unwrap();

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn serial_counter_is_shared_and_seeded_from_committed_max() {
        let engine = Arc::new(Engine::in_memory());
        engine.open().unwrap();

        let schema = SchemaBuilder::new("logs")
            .primary("id", Type::BigSerial)
            .nullable("msg", Type::Text)
            .build();
        engine.create_table(schema).unwrap();

        // seed a committed row so the counter starts above the existing max
        let (txn_id, _) = engine.registry.begin();
        engine
            .insert(
                txn_id,
                "logs",
                1,
                vec![Value::Number(5), Value::String("seed".into())],
            )
            .unwrap();
        engine.commit_transaction(txn_id).unwrap();

        // two threads sharing one engine stand in for two connections
        let handles: Vec<_> = (0..2)
            .map(|_| {
                let engine = Arc::clone(&engine);
                std::thread::spawn(move || {
                    (0..500)
                        .map(|_| engine.next_serial("logs", 0).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();

        let mut allocated: Vec<i128> = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect();

        assert!(allocated.iter().all(|&value| value > 5), "seeded above max");

        allocated.sort_unstable();
        let unique = allocated.len();
        allocated.dedup();
        assert_eq!(allocated.len(), unique, "no value handed out twice");

        engine.close().unwrap();
    }
}
