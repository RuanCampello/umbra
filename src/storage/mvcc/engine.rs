use super::FileLock;
use crate::{
    collections::hash::HashMap,
    db::SchemaNew as Schema,
    storage::{
        mvcc::{
            registry::TransactionRegistry,
            version::{TupleVersion, VersionEntry, VersionStorage},
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
    fetching_disk: Arc<AtomicBool>,
    config: RwLock<Config>,
    clean_up_handle: Mutex<Option<CleanUpThread>>,
}

#[derive(Clone, Default)]
pub(crate) struct Config {
    path: String,
    wal: WalConfig,
    cleanup: CleanUpConfig,
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

type Result<T> = std::result::Result<T, MvccError>;

impl Engine {
    pub fn new(config: Config) -> Self {
        let wal = Some(
            WalManager::new(Some(Path::new(&config.path)), &config.wal)
                .expect("wal manager not to fail"),
        );

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

    pub fn does_table_exists(&self, name: &str) -> Result<bool> {
        if !self.is_open() {
            return Err(MvccError::NotOpen);
        }

        let schemas = self.schemas.read().unwrap();
        Ok(schemas.contains_key(name))
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

    /// Check if we can't write to WAL (that's during reply recovery phase)
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

        let lsn = deserialise_snapshot_header(&dir);
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
        // TODO: populate indexes

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

                let table = schema.name.clone();

                let mut schemas = self.schemas.write().unwrap();
                schemas.insert(table.clone(), Arc::new(schema));

                let mut storages = self.versions.write().unwrap();
                storages.insert(table, version_storage);
            }

            WalOperation::DropTable => {
                let table = entry.table;

                let mut schemas = self.schemas.write().unwrap();
                schemas.remove(&table);

                let mut storages = self.versions.write().unwrap();
                if let Some(storage) = storages.remove(&table) {
                    storage.close()
                };
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

            _ => {}
        })
    }

    fn load_table(&self, name: &str, snapshot: &Path) -> Result<u64> {
        unimplemented!()
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

    pub fn insert(&self, txn_id: i64, table: &str, row_id: i64, data: Tuple) -> Result<()> {
        let storage = self.version_storage(table)?;
        let version = TupleVersion::new(txn_id, row_id, data);

        storage.add_version(row_id, version.clone());

        if !self.must_skip_wal() {
            if let Some(ref wal) = *self.wal {
                wal.record_dml(table, txn_id, row_id, WalOperation::Insert, version)?;
            }
        }

        Ok(())
    }

    /// Update a row in a table.
    pub fn update(&self, txn_id: i64, table: &str, row_id: i64, data: Tuple) -> Result<()> {
        let storage = self.version_storage(table)?;
        let version = TupleVersion::new(txn_id, row_id, data);

        storage.add_version(row_id, version.clone());

        if !self.must_skip_wal() {
            if let Some(ref wal) = *self.wal {
                wal.record_dml(table, txn_id, row_id, WalOperation::Update, version)?;
            }
        }

        Ok(())
    }

    /// Delete a row from a table.
    pub fn delete(&self, txn_id: i64, table: &str, row_id: i64) -> Result<()> {
        let storage = self.version_storage(table)?;

        let mut version = TupleVersion::new(txn_id, row_id, Vec::new());
        version.deleted_at_txn_id = txn_id;

        storage.add_version(row_id, version.clone());

        if !self.must_skip_wal() {
            if let Some(ref wal) = *self.wal {
                wal.record_dml(table, txn_id, row_id, WalOperation::Delete, version)?;
            }
        }

        Ok(())
    }

    /// Scan all visible rows for a given transaction.
    pub fn scan(&self, txn_id: i64, table: &str) -> Result<Vec<Tuple>> {
        let storage = self.version_storage(table)?;
        Ok(storage.scan_visible(txn_id))
    }

    /// Commit a transaction: flush WAL commit record and commit in registry.
    pub fn commit_transaction(&self, txn_id: i64) -> Result<()> {
        if let Some(ref wal) = *self.wal {
            wal.record_commit(txn_id)?;
        }
        self.registry.commit_transaction(txn_id);
        Ok(())
    }

    /// Rollback a transaction: flush WAL rollback record and abort in registry.
    pub fn rollback_transaction(&self, txn_id: i64) -> Result<()> {
        if let Some(ref wal) = *self.wal {
            wal.record_rollback(txn_id)?;
        }
        self.registry.abort_transaction(txn_id);
        Ok(())
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
        }
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
        assert_eq!(rows[0][1], Value::String("new".into()));

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
        assert_eq!(rows[0][1], Value::String("b".into()));

        engine.close().unwrap();
    }
}
