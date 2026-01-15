#![allow(unused)]

use std::{
    collections::HashMap,
    fmt::Display,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Mutex,
    },
};

mod checkpoint;
mod entry;

pub(super) use entry::WalEntry;
pub(super) use entry::WalOperation;

use crate::core::{storage::wal::entry::WalFlags, HashSet};
use checkpoint::CheckpointMetadata;

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
    sync: Sync,
}

#[derive(Debug)]
pub(crate) struct Config {
    pub(super) enabled: bool,
    pub(super) sync: Sync,
    /// Time interval between snapshots in seconds
    pub(super) interval: u32,
    /// Number of snapshots to be kept
    pub(super) snapshots: u32,

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

/// Information used for two-phase recovery for WAL
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) struct TwoPhaseRecovery {
    /// Last LSN
    lsn: u64,
    /// Committed transactions found
    committed_transactions: usize,
    /// Aborted transactions found
    aborted_transactions: usize,
    applied: u64,
    skipped: u64,
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
    NotRunning,
    Closed,
    LsnOverflow,
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

/// Header size of a WAL entry.
const WAL_HEADER_SIZE: isize = 1 << 5;

pub(in crate::core::storage) const SNAPSHOT_INTERVAL: usize = 300;
pub(in crate::core::storage) const SNAPSHOT_COUNT: usize = 5;

impl Wal {
    pub fn new(path: impl AsRef<Path>, sync: Sync) -> Result<Self, WalError> {
        Self::with_config(path, sync, &Config::default())
    }

    pub fn with_config(
        path: impl AsRef<Path>,
        sync: Sync,
        config: &Config,
    ) -> Result<Self, WalError> {
        let path = path.as_ref().to_path_buf();
        fs::create_dir_all(&path)?;

        Self::recover_from_truncation(&path)?;

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
            sync,
        })
    }

    pub fn append(&self, mut entry: WalEntry) -> Result<u64, WalError> {
        if !self.running.load(Ordering::Acquire) {
            return Err(WalError::Closed);
        }

        let previous_lsn = self.previous_lsn.load(Ordering::Acquire);
        entry.previous_lsn = previous_lsn;

        let current = self.lsn.load(Ordering::Acquire);
        if current == u64::MAX {
            return Err(WalError::LsnOverflow);
        }
        entry.lsn = self.lsn.fetch_add(1, Ordering::SeqCst) + 1;
        self.previous_lsn.store(entry.lsn, Ordering::SeqCst);

        let encoded = entry.encode();

        let mut buff = self.buffer.lock().unwrap();
        buff.extend_from_slice(&encoded);

        let needs_flush = buff.len() >= self.flush_trigger as usize;

        if needs_flush || self.must_sync(entry.operation) {
            let mut buffer = std::mem::take(&mut *buff);
            self.in_flight_writes.fetch_add(1, Ordering::SeqCst);
            drop(buff);

            let result = self.write(&buffer);
            self.in_flight_writes.fetch_sub(1, Ordering::SeqCst);
            result?;

            self.file_position
                .fetch_add(buffer.len() as u64, Ordering::SeqCst);

            if self.must_sync(entry.operation) {
                self.sync_with_lock()?;
            }
        }

        let _ = encoded.len();
        Ok(entry.lsn)
    }

    pub fn write_commit(&self, txn_id: i64) -> Result<u64, WalError> {
        let entry = WalEntry::commit(txn_id);
        self.append(entry)
    }

    pub fn write_abort(&self, txn_id: i64) -> Result<u64, WalError> {
        let entry = WalEntry::rollback(txn_id);
        self.append(entry)
    }

    pub fn must_sync(&self, operation: WalOperation) -> bool {
        match self.sync {
            Sync::Strict => true,
            Sync::Default if operation.end_transaction() || operation.changes_properties() => true,
            _ => false,
        }
    }

    pub fn write(&self, buffer: &[u8]) -> Result<(), WalError> {
        if buffer.is_empty() {
            return Ok(());
        }

        let mut file = self.active_segment.lock().unwrap();
        match file.as_mut() {
            Some(file) => file.write_all(buffer)?,
            None => return Err(WalError::Closed),
        }

        Ok(())
    }

    fn sync_with_lock(&self) -> Result<(), WalError> {
        let mut file = self.active_segment.lock().unwrap();
        match file.as_mut() {
            Some(file) => file.sync_data()?,
            None => return Err(WalError::Closed),
        }

        Ok(())
    }

    fn recover_from_truncation(dir: &Path) -> Result<(), WalError> {
        if !dir.exists() {
            return Ok(());
        }

        let entries = match fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) => return Ok(()),
        };

        let mut backup = Vec::new();
        let mut temp = Vec::new();
        let mut wal = Vec::new();

        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();

            match name {
                name if name.starts_with("wal-") && name.ends_with(".log") => wal.push(name),
                name if name.starts_with("wal-") && name.ends_with(".tmp") => temp.push(path),
                _ => backup.push((name, entry.path())),
            }
        }

        for temp in temp {
            eprintln!("Removing incomplete WAL file: {temp:?}");
            let _ = fs::remove_file(&temp);
        }

        for (backup, path) in backup {
            let name = backup.trim_end_matches(".bak");
            let is_valid_wal = wal.iter().any(|wal| !wal.is_empty());

            match is_valid_wal {
                true => {
                    let restore_path = dir.join(name);
                    eprintln!("Recovering WAL from backup: {path:?} -> {restore_path:?}");
                    fs::rename(&path, &restore_path)?;
                    eprintln!("WAL backup recovery successful");
                }
                false => {
                    eprintln!("Cleaning up WAL backup: {path:?}");
                    fs::remove_file(&path)?;
                }
            }
        }

        Ok(())
    }

    pub fn flush(&self) -> Result<(), WalError> {
        if !self.running.load(Ordering::Acquire) {
            return Err(WalError::NotRunning);
        }

        let buffer = {
            let mut buffer = self.buffer.lock().unwrap();
            if buffer.is_empty() {
                return Ok(());
            }

            // increment in-flight writes before releasing the lock
            let data = std::mem::take(&mut *buffer);
            self.in_flight_writes.fetch_add(1, Ordering::SeqCst);
            data
        };

        let result = self.write(&buffer);
        self.in_flight_writes.fetch_sub(1, Ordering::SeqCst);
        result
    }

    pub fn close(&self) -> Result<(), WalError> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(WalError::NotRunning);
        }

        self.flush()?;
        self.sync_with_lock()?;

        self.running.store(false, Ordering::SeqCst);

        let mut file = self.active_segment.lock().unwrap();
        *file = None;

        Ok(())
    }

    pub fn replay_two_phase<C: FnMut(WalEntry) -> Result<(), WalEntry>>(
        &self,
        lsn: u64,
        mut callback: C,
    ) -> Result<TwoPhaseRecovery, WalError> {
        self.flush()?;

        let mut lsn = lsn;
        let path = self.dir.join("checkpoint.meta");
        if let Ok(checkpoint) = CheckpointMetadata::decode(&path) {
            if checkpoint.lsn > lsn {
                lsn = checkpoint.lsn;
            }
        }

        let mut files = Vec::new();

        if let Ok(entries) = fs::read_dir(&self.dir) {
            entries.filter_map(|entry| entry.ok()).for_each(|entry| {
                let name = entry.file_name().to_string_lossy().to_string();

                if (name.starts_with("wal-") && name.ends_with(".log")) {
                    files.push(entry.path())
                }
            })
        }

        files.sort_unstable();

        let mut committed = HashSet::default();
        let mut aborted = HashSet::default();
        let mut last_lsn = lsn;

        for path in &files {
            Self::scan(path, lsn, &mut committed, &mut aborted, &mut last_lsn)?;
        }

        let mut applied = 0;
        let mut skipped = 0;

        // TODO: phase two: redo
        todo!()
    }

    pub fn scan(
        path: &Path,
        from_lsn: u64,
        committed: &mut HashSet<i64>,
        aborted: &mut HashSet<i64>,
        last_lsn: &mut u64,
    ) -> Result<(), WalError> {
        let Ok(mut file) = File::open(path) else {
            return Ok(());
        };

        loop {
            let mut header = [0u8; WAL_HEADER_SIZE as usize];
            match file.read_exact(&mut header) {
                Err(_) => break,
                Ok(()) => {}
            };

            let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
            if magic != WAL_MAGIC {
                file.seek(SeekFrom::Current(-WAL_HEADER_SIZE as i64))?;
                if !Self::scan_magic(&mut file) {
                    break;
                }

                continue;
            }

            let flags = WalFlags::from(header[5]);
            let size = u16::from_le_bytes(header[6..8].try_into().unwrap()) as usize;
            let lsn = u64::from_le_bytes(header[8..16].try_into().unwrap());
            let previous_lsn = u64::from_le_bytes(header[16..24].try_into().unwrap());
            let entry_size = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;

            if size > 32 {
                let seek = (size - WAL_HEADER_SIZE as usize) as i64;
                if file.seek(SeekFrom::Current(seek)).is_err() {
                    break;
                }
            }

            let header_size = entry_size + 4;
            if header_size > WAL_MAX_SIZE {
                if !Self::scan_magic(&mut file) {
                    break;
                }

                continue;
            }

            if lsn < from_lsn {
                if file.seek(SeekFrom::Current(header_size as _)).is_err() {
                    break;
                }

                continue;
            }

            if flags.contains(WalFlags::COMMIT) || flags.contains(WalFlags::ABORT) {
                let mut txn_id = [0u8; 8];

                match file.read_exact(&mut txn_id) {
                    Ok(_) => {
                        let txn_id = i64::from_le_bytes(txn_id);
                        match flags.contains(WalFlags::COMMIT) {
                            true => committed.insert(txn_id),
                            _ => aborted.insert(txn_id),
                        };

                        let rest = header_size - 8;
                        if file.seek(SeekFrom::Current(rest as _)).is_err() {
                            break;
                        }
                    }
                    _ => break,
                };
            } else {
                if file.seek(SeekFrom::Current(header_size as _)).is_err() {
                    break;
                }
            }

            if lsn > *last_lsn {
                *last_lsn = lsn;
            }
        }

        Ok(())
    }

    /// Scans the file for the next magic identifier.
    #[inline(always)]
    pub fn scan_magic(file: &mut File) -> bool {
        const BUFFER_SIZE: usize = 1 << 13;
        const MAX: usize = 1 << 20;

        let mut buff = [0u8; BUFFER_SIZE];
        let mut window = 0;
        let mut total = 0;

        loop {
            let bytes = match file.read(&mut buff) {
                Ok(0) | Err(_) => return false,
                Ok(n) => n,
            };

            for (idx, &byte) in buff[..bytes].iter().enumerate() {
                window = (window >> 8) | ((byte as u32) << 24);

                total += 1;
                if total > MAX {
                    return false;
                }

                if window == WAL_MAGIC {
                    let seek = (bytes - idx - 1 + 4) as i64; // + 4 of the marker itself
                    let seek = SeekFrom::Current(-seek);

                    if file.seek(seek).is_ok() {
                        return true;
                    }

                    return false;
                }
            }
        }
    }

    pub fn lsn(&self) -> u64 {
        self.lsn.load(Ordering::Acquire)
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.close();
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

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            sync: Sync::Default,
            interval: SNAPSHOT_INTERVAL as u32,
            snapshots: SNAPSHOT_COUNT as u32,
            flush_trigger: WAL_FLUSH_SIZE,
            buffer_size: WAL_SIZE,
            max_size: WAL_MAX_SIZE,
            batch_size: 100,
            sync_interval: 10,
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
            Self::NotRunning => f.write_str("WAL is not running"),
            Self::Closed => f.write_str("WAL is closed"),
            Self::WrongMagic { from } => write!(f, "Invalid magic number for: {from}"),
            Self::Checksum => f.write_str("Checksum mismatch"),
            Self::UnexpectedEnd => f.write_str("Unexpected end of data on reading"),
            Self::LsnOverflow => {
                f.write_str("WAL LSN overflow: maximum sequence of number reached")
            }
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
