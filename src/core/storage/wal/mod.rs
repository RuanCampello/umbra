#![allow(unused)]

use std::{
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};

mod checkpoint;
mod entry;

use checkpoint::CheckpointMetadata;
use entry::WalEntry;

/// Write-ahead loding
#[derive(Debug)]
pub(crate) struct Wal {
    /// Directory where WAL segments are stored.
    dir: PathBuf,
    /// Currenctly active log file.
    active_segment: Mutex<Option<File>>,
    /// ID of the current file (e.g, "wal-0001.log")
    /// monotonically incremented
    current_segment_id: AtomicU64,
    /// Global log sequence number
    lsn: AtomicU64,
    /// LSN of the immediately previous record.
    /// Critical for transaction rollback
    previous_lsn: AtomicU64,
    /// Write buffer
    buffer: Mutex<Vec<u8>>,
    /// Number of bytes to be written before flushing
    flush_trigger: usize,
    /// Maximum size of the WAL file
    max_size: usize,
    /// LSN of the last checkpoint
    last_checkpoint: AtomicU64,
    running: AtomicBool,
    /// Current file position
    file_position: AtomicU64,
    /// Current sequence number
    sequence: AtomicU64,
    /// Number of in-flight writes
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
    WrongMagic { from: String },
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
        fs::create_dir_all(&path)?;

        let mut active_file: Option<File> = None;
        let mut initial_lsn: u64 = 0;
        let mut segment_id: u64 = 0;

        // check if checkpoint exists and try to recover from it
        let checkpoint_path = path.join("checkpoint.meta");
        if let Ok(checkpoint) = CheckpointMetadata::decode(&checkpoint_path) {
            segment_id = checkpoint.wal_segment_sequence;
            initial_lsn = checkpoint.lsn;

            let wal_filename = format!("wal-{:08}.log", segment_id);
            let wal_path = path.join(&wal_filename);

            if let Ok(file) = OpenOptions::new().read(true).append(true).open(&wal_path) {
                if let Ok(last_lsn) = find_last_lsn(&wal_path) {
                    if last_lsn > initial_lsn {
                        initial_lsn = last_lsn;
                    }
                }
                active_file = Some(file);
            }
        }

        // if no checkpoint or couldn't open WAL file, look for existing WAL files
        if active_file.is_none() {
            let mut wal_files: Vec<(u64, String)> = Vec::new();

            if let Ok(entries) = fs::read_dir(&path) {
                for entry in entries.filter_map(|e| e.ok()) {
                    let name = entry.file_name().to_string_lossy().to_string();
                    if name.starts_with("wal-") && name.ends_with(".log") {
                        if let Some(seq) = extract_sequence_from_filename(&name) {
                            wal_files.push((seq, name));
                        }
                    }
                }
            }

            // sort by sequence number and get the newest
            wal_files.sort_by_key(|(seq, _)| *seq);

            if let Some((seq, newest)) = wal_files.last() {
                segment_id = *seq;
                let wal_path = path.join(newest);

                if let Ok(file) = OpenOptions::new().read(true).append(true).open(&wal_path) {
                    if let Ok(last_lsn) = find_last_lsn(&wal_path) {
                        initial_lsn = last_lsn;
                    }
                    active_file = Some(file);
                }
            }
        }

        // create new WAL file if none exists
        if active_file.is_none() {
            let wal_filename = format!("wal-{:08}.log", segment_id);
            let wal_path = path.join(&wal_filename);

            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(&wal_path)?;

            active_file = Some(file);
        }

        // get initial file position if we have an existing WAL file
        let initial_file_position = match active_file {
            Some(ref file) => file.metadata().map(|m| m.len()).unwrap_or(0),
            None => 0,
        };

        Ok(Self {
            dir: path,
            active_segment: Mutex::new(active_file),
            current_segment_id: AtomicU64::new(segment_id),
            lsn: AtomicU64::new(initial_lsn),
            previous_lsn: AtomicU64::new(initial_lsn),
            buffer: Mutex::new(Vec::with_capacity(config.buffer_size)),
            flush_trigger: config.flush_trigger,
            max_size: config.max_size,
            last_checkpoint: AtomicU64::new(initial_lsn),
            running: AtomicBool::new(true),
            file_position: AtomicU64::new(initial_file_position),
            sequence: AtomicU64::new(segment_id),
            in_flight_writes: AtomicU64::new(0),
        })
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
            Self::WrongMagic { from } => write!(f, "Invalid magic number for: {from}"),
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

/// Extract sequence number from WAL filename (e.g., "wal-00000001.log" -> Some(1))
#[inline]
fn extract_sequence_from_filename(filename: &str) -> Option<u64> {
    if !filename.starts_with("wal-") || !filename.ends_with(".log") {
        return None;
    }

    let seq_str = &filename[4..filename.len() - 4];
    seq_str.parse::<u64>().ok()
}

/// Find the last LSN in a WAL file by scanning entries
#[inline]
fn find_last_lsn(path: &Path) -> Result<u64, WalError> {
    let mut file = File::open(path)?;
    let mut last_lsn: u64 = 0;
    // 32-byte header: magic(4) + version(1) + flags(1) + header_size(2) +
    // LSN(8) + prev_lsn(8) + entry_size(4) + reserved(4)
    let mut header_buf = [0u8; 32];

    loop {
        match file.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(_) => break,
        }

        let magic = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        if magic != WAL_MAGIC {
            break;
        }

        let header_size = u16::from_le_bytes(header_buf[6..8].try_into().unwrap()) as usize;
        let lsn = u64::from_le_bytes(header_buf[8..16].try_into().unwrap());
        let entry_size = u32::from_le_bytes(header_buf[24..28].try_into().unwrap()) as usize;

        if lsn > last_lsn {
            last_lsn = lsn;
        }

        if header_size > 32 {
            let extra = header_size - 32;
            if file.seek(SeekFrom::Current(extra as i64)).is_err() {
                break;
            }
        }

        let total_entry_size = entry_size + 4;
        if file
            .seek(SeekFrom::Current(total_entry_size as i64))
            .is_err()
        {
            break;
        }
    }

    Ok(last_lsn)
}
