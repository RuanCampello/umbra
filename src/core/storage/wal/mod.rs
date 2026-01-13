use std::{
    fmt::Display,
    fs::{self, File},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        Mutex,
    },
};

mod checkpoint;
mod entry;

/// Write-ahead loding
#[derive(Debug)]
pub(crate) struct Wal {
    /// Directory where WAL segments are stored.
    dir: PathBuf,
    /// Currenctly active log file.
    active_segment: Mutex<Option<File>>,
    /// ID of the current file (e.g, "wal-0001.log")
    current_segment_id: AtomicU64,
    lsn: AtomicU64,
    previous_lsn: AtomicU64,
    buffer: Mutex<Vec<u8>>,
    flush_trigger: usize,
    max_size: usize,
    last_checkpoint: AtomicU64,
    running: AtomicBool,
    file_position: AtomicU64,
    sequence: AtomicU64,
    in_flight_writes: AtomicU64,
}

#[derive(Debug)]
pub(crate) struct Config {
    enabled: bool,
    sync: Sync,
    /// Time interval between in seconds
    interval: u32,
    /// Number of snapshots to be kept
    snapshots: u32,

    flush_trigger: usize,
    buffer_size: usize,
    max_size: usize,
    batch_size: usize,
    /// Time interval in milliseconds in [default sync](self::Sync::Default)
    sync_interval: u32,
}

#[derive(Debug, Default, PartialEq, Clone, Copy)]
#[repr(u8)]
pub enum Sync {
    /// Lenient: don't forces writes, but faster
    Relaxed,
    #[default]
    /// Balanced, syncs on transactions commit.
    Default,
    /// Consistent: syncs on every write, but slower
    Strict,
}

#[derive(Debug)]
pub enum WalError {
    InvalidSize(usize),
    Name(String),
    Checksum,
    UnexpectedEnd,
    InvalidOperation(u8),
    Io(std::io::Error),
}

/// WAL entry magic marker: "mnem"
const WAL_MAGIC: u32 = 0x6D6E656D;

/// Default maximum WAL file size before rotating: 64MB, 2^26 bytes
const WAL_MAX_SIZE: usize = 1 << 26;

/// Default WAL size: 64KB
const WAL_SIZE: usize = 1 << 16;

/// Default size of the WAL before the flushing: 32KB
const WAL_FLUSH_SIZE: usize = 1 << 15;

/// As primeagen says, it's good to track the version of your binaries.
const WAL_BINARY_VERSION: u8 = 1;

const WAL_HEADER_SIZE: u16 = 1 << 5;

impl Wal {
    pub fn new(path: impl AsRef<Path>, sync: Sync) -> Self {
        todo!()
    }

    pub fn with_config(
        path: impl AsRef<Path>,
        sync: Sync,
        config: Config,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();

        fs::create_dir(&path)?;
        // TODO: recovering

        let mut file: Option<File> = None;
        let mut lsn = 0;

        todo!()
    }
}

impl From<u8> for Sync {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Relaxed,
            2 => Self::Strict,
            _ => Self::Default,
        }
    }
}

impl Display for WalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Name(name) => write!(f, "Invalid table name: {name}"),
            Self::InvalidSize(size) => write!(f, "Data size is incompatible: {size}"),
            Self::InvalidOperation(operation) => {
                write!(f, "Invalid operation type for entry: {operation}")
            }
            Self::Checksum => f.write_str("Checksum mismatch"),
            Self::UnexpectedEnd => f.write_str("Unexpected end of data on reading"),
            Self::Io(io) => f.write_str(&io.to_string()),
        }
    }
}

impl From<std::io::Error> for WalError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
