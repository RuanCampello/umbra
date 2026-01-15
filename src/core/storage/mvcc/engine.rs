use std::{
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc, RwLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    core::{
        storage::{
            mvcc::{
                registry::TransactionRegistry, version::VersionStorage, wal::WalManager, MvccError,
            },
            wal::WalConfig,
        },
        HashMap,
    },
    db::Schema,
};

/// MVCC storage engine.
/// This provides snapshot isolation with a multi-version concurrency control.
pub(crate) struct Engine {
    path: PathBuf,
    schemas: Arc<RwLock<HashMap<String, Arc<Schema>>>>,
    versions: Arc<RwLock<HashMap<String, Arc<VersionStorage>>>>,
    registry: Arc<TransactionRegistry>,
    wal: Arc<Option<WalManager>>,
    is_open: AtomicBool,
    /// This is incremented on any change of the schema.
    /// So we can invalidate the cache without lookups.
    epoch: AtomicU64,
}

pub(crate) struct Config {
    path: String,
    wal: WalConfig,
}

/// Good ol' "styx"
const SNAPSHOT_MAGIC: u32 = 0x73747978;
const SNAPSHOT_VERSION: u32 = 1;

/// magic (4) + version(4) + lsn(8) + time(8) + hash(4)
const SNAPSHOT_HEADER_SIZE: usize = 28;

impl Engine {
    pub fn new(config: Config) -> Self {
        let wal = Some(
            WalManager::new(Some(Path::new(&config.path)), config.wal)
                .expect("wal manager not to fail"),
        );

        Self {
            path: config.path.into(),
            is_open: AtomicBool::new(false),
            schemas: Arc::new(RwLock::new(HashMap::default())),
            versions: Arc::new(RwLock::new(HashMap::default())),
            wal: Arc::new(wal),
            registry: Arc::new(TransactionRegistry::new()),
            epoch: AtomicU64::new(0),
        }
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
        }
    }
}

fn serialise_snapshot_header(path: &Path, lsn: u64) -> Result<(), MvccError> {
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
