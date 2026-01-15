//! Deals with the persistence of WAL for the engine.

use crate::{
    core::{
        storage::{
            mvcc::{version::TupleVersion, MvccError},
            wal::{
                Config, Wal, WalEntry, WalError, WalOperation, SNAPSHOT_COUNT, SNAPSHOT_INTERVAL,
            },
        },
        HashMap,
    },
    db::Schema,
    sql::statement::Value,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

// This is used to coordinate the disk operations.
pub(crate) struct WalManager {
    dir: PathBuf,
    wal: Option<Wal>,
    metadata: WalManagerMetadata,
    enabled: AtomicBool,
    /// Interval between snapshots
    interval: Duration,
    /// Number of snapshots we should keep
    snapshots: usize,
    running: AtomicBool,
    schemas: RwLock<HashMap<String, Arc<Schema>>>,
}

#[derive(Default)]
pub(crate) struct WalManagerMetadata {
    /// Last used snapshot timestamp
    last_timestamp: AtomicI64,
    /// Last used snapshot lsn
    last_lsn: AtomicU64,
    /// Last WAL lsn used on recovering
    wal_lsn: AtomicU64,
}

type Result<T> = std::result::Result<T, MvccError>;

/// Special transaction ID for domain defitinion operations.
///
pub const DDL_ID: i64 = -33;

impl WalManager {
    pub fn new(path: Option<&Path>, config: Config) -> Result<Self> {
        if path.is_none() || !config.enabled {
            return Ok(Self {
                dir: PathBuf::new(),
                wal: None,
                enabled: AtomicBool::new(false),
                running: AtomicBool::new(false),
                snapshots: SNAPSHOT_COUNT,
                interval: Duration::from_secs(SNAPSHOT_INTERVAL as _),
                schemas: RwLock::new(HashMap::default()),
                metadata: WalManagerMetadata::default(),
            });
        }

        let path = path.unwrap();
        fs::create_dir_all(path)?;

        let wal_path = path.join("wal");
        let wal = Wal::with_config(&wal_path, config.sync, &config)?;
        let lsn = wal.lsn();

        let interval = match config.interval > 0 {
            true => Duration::from_secs(config.interval as _),
            _ => Duration::from_secs(SNAPSHOT_INTERVAL as _),
        };

        let snapshots = match config.snapshots > 0 {
            true => config.snapshots as usize,
            _ => SNAPSHOT_COUNT,
        };

        let manager = Self {
            dir: path.to_path_buf(),
            wal: Some(wal),
            enabled: AtomicBool::new(true),
            running: AtomicBool::new(false),
            schemas: RwLock::new(HashMap::default()),
            metadata: WalManagerMetadata::default(),
            snapshots,
            interval,
        };

        manager.metadata.last_lsn.store(lsn, Ordering::Release);
        Ok(manager)
    }

    /// Used for `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE` etc...
    /// Those are auto-committed operations that don't interleave user transactions.
    pub fn record_ddl(&self, table: &str, operation: WalOperation, content: &[u8]) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let wal = self.wal.as_ref().ok_or(WalError::NotRunning)?;
        let entry = WalEntry::new(table.to_string(), DDL_ID, 0, operation, content.to_vec());

        wal.append(entry)?;
        wal.write_commit(DDL_ID)?; // DDL operation are auto-commit

        Ok(())
    }

    pub fn record_dml(
        &self,
        table: &str,
        txn_id: i64,
        row_id: i64,
        operation: WalOperation,
        version: TupleVersion,
    ) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }

        let wal = self.wal.as_ref().ok_or(WalError::NotRunning)?;

        Ok(())
    }

    #[inline]
    fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
}

impl Value {
    #[inline(always)]
    fn serialise(&self) -> Result<Vec<u8>> {
        todo!()
    }
}
