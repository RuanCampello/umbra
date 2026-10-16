//! The commit log: one checksummed frame per committed transaction or DDL, group-committed
//!
//! ```text
//! frame = crc32c u32 | len u32 | kind u8 | payload[len]
//! crc   = crc32c(lsn u64 ‖ len ‖ kind ‖ payload), lsn = the frame's offset in the log stream
//! ```
//!
//! Binding the checksum to the frame's own position rejects stale bytes left past a truncated
//! tail. Segments are `wal/<first lsn>.log`, a frame never spans two segments

use crate::core::crc32c::{crc32c, crc32c_append};
use crate::storage::engine::{
    error::{EngineError, Result},
    io::{Storage, StorageFile},
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

pub(crate) struct Log {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    sync: SyncMode,
    segment_size: u64,
    state: Mutex<LogState>,
    flushed: Condvar,
    /// Every byte below is durable (or written, with [SyncMode::Off])
    durable: AtomicU64,
    poisoned: AtomicBool,
}

struct LogState {
    buffer: Vec<u8>,
    /// Lsn after the last buffered frame
    end: u64,
    flushing: bool,
    /// `None` only while a leader is writing outside the lock
    file: Option<Box<dyn StorageFile>>,
    segment_start: u64,
}

/// What [Log::open] found while replaying
pub(crate) struct Replay {
    pub end: u64,
    pub frames: usize,
    pub truncated: bool,
}

/// How a group commit makes its frames durable
/// only `fdatasync` today, the type leaves room for `O_DSYNC`/`F_FULLFSYNC` variants
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq)]
pub enum SyncMode {
    #[default]
    Full,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum Kind {
    Commit = 1,
    Ddl = 2,
}

const HEADER: usize = 9;
const MAX_FRAME: usize = 1 << 30;

impl Log {
    /// opens the log in `dir`, replaying every valid frame at or after `from` into `apply`
    ///
    /// a damaged frame in the last segment is a torn tail: the segment is truncated there
    /// damage anywhere else is corruption and refuses to open
    pub fn open(
        storage: Arc<dyn Storage>,
        dir: PathBuf,
        sync: SyncMode,
        segment_size: u64,
        from: u64,
        mut apply: impl FnMut(Kind, &[u8]) -> Result<()>,
    ) -> Result<(Self, Replay)> {
        if !storage.exists(&dir) {
            storage.create_dir(&dir)?;
            storage.sync_dir(dir.parent().expect("log directory has a parent"))?;
        }

        let segments = segments(storage.as_ref(), &dir)?;
        let first = segments.partition_point(|&(start, _)| start <= from).saturating_sub(1);
        let segments = &segments[first.min(segments.len())..];

        let mut replay = Replay { end: from, frames: 0, truncated: false };

        let mut tail = None;
        for (position, (start, path)) in segments.iter().enumerate() {
            let last = position + 1 == segments.len();
            if (position > 0 && *start != replay.end) || (position == 0 && *start > from) {
                let msg =
                    format!("log segment {} does not follow lsn {}", path.display(), replay.end);
                return Err(EngineError::Corrupt(msg));
            }

            let bytes = storage.read(path)?;
            let mut offset = from.saturating_sub(*start) as usize;
            if offset > bytes.len() {
                let msg = format!("log segment {} ends before lsn {from}", path.display());
                return Err(EngineError::Corrupt(msg));
            }

            loop {
                match frame(&bytes, offset, start + offset as u64) {
                    Some((kind, payload)) => {
                        apply(kind, payload)?;
                        offset += HEADER + payload.len();
                        replay.frames += 1;
                    },
                    None if offset == bytes.len() => break,
                    None if last => {
                        let mut file = storage.open(path)?;
                        file.truncate(offset as u64)?;
                        file.sync()?;
                        replay.truncated = true;
                        break;
                    },
                    _ => {
                        let msg =
                            format!("log segment {} is damaged at byte {offset}", path.display());
                        return Err(EngineError::Corrupt(msg));
                    },
                }
            }

            replay.end = start + offset as u64;
            if last {
                tail = Some((*start, path.clone()));
            }
        }

        let (segment_start, file) = match tail {
            Some((start, path)) => (start, storage.open(&path)?),
            None => (replay.end, create_segment(storage.as_ref(), &dir, replay.end)?),
        };

        let state = LogState {
            buffer: Vec::new(),
            end: replay.end,
            flushing: false,
            file: Some(file),
            segment_start,
        };

        let log = Self {
            storage,
            dir,
            sync,
            segment_size,
            state: Mutex::new(state),
            flushed: Condvar::new(),
            durable: AtomicU64::new(replay.end),
            poisoned: AtomicBool::new(false),
        };

        Ok((log, replay))
    }

    /// buffers one frame and returns the lsn right after it
    /// call order is log order
    pub fn append(&self, kind: Kind, payload: &[u8]) -> u64 {
        assert!(payload.len() < MAX_FRAME, "log frame of {} bytes", payload.len());

        let mut state = self.lock();
        let lsn = state.end;
        let len = (payload.len() as u32).to_le_bytes();

        let crc = crc32c_append(
            crc32c_append(crc32c_append(crc32c(&lsn.to_le_bytes()), &len), &[kind as u8]),
            payload,
        );

        state.buffer.extend_from_slice(&crc.to_le_bytes());
        state.buffer.extend_from_slice(&len);
        state.buffer.push(kind as u8);
        state.buffer.extend_from_slice(payload);
        state.end += (HEADER + payload.len()) as u64;

        state.end
    }

    /// blocks until every frame below `end` is durable, writing and syncing on behalf of every
    /// waiter when no flush is in progress (group commit)
    pub fn wait(&self, end: u64) -> Result<()> {
        let mut state = self.lock();

        loop {
            if self.poisoned.load(Ordering::Acquire) {
                return Err(EngineError::Poisoned);
            }

            if self.durable.load(Ordering::Acquire) >= end {
                return Ok(());
            }

            if state.flushing {
                state = self.flushed.wait(state).expect("log lock poisoned");
                continue;
            }

            state.flushing = true;
            let target = state.end;
            let mut buffer = std::mem::take(&mut state.buffer);
            let mut file = state.file.take().expect("no flush in progress");
            drop(state);

            let written = file.append(&buffer).and_then(|_| match self.sync {
                SyncMode::Full => file.sync(),
            });

            state = self.lock();
            buffer.clear();
            if state.buffer.is_empty() {
                state.buffer = buffer;
            }

            let outcome = match written {
                Err(error) => Err(EngineError::from(error)),
                Ok(()) => {
                    self.durable.store(target, Ordering::Release);
                    match file.len() >= self.segment_size {
                        true => self.rotate(&mut state, target).map(|next| file = next),
                        false => Ok(()),
                    }
                },
            };

            state.file = Some(file);
            state.flushing = false;
            if outcome.is_err() {
                self.poisoned.store(true, Ordering::Release);
            }
            self.flushed.notify_all();
        }
    }

    /// lsn after the last buffered frame
    pub fn end(&self) -> u64 {
        self.lock().end
    }

    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    /// deletes every segment that ends at or before `lsn`
    pub fn prune(&self, lsn: u64) -> Result<()> {
        let current = self.lock().segment_start;
        let segments = segments(self.storage.as_ref(), &self.dir)?;

        for pair in segments.windows(2) {
            let ((_, path), (next, _)) = (&pair[0], &pair[1]);
            if *next <= lsn && *next <= current {
                self.storage.remove(path)?;
            }
        }

        self.storage.sync_dir(&self.dir)?;
        Ok(())
    }

    /// starts a new segment at `start`, buffered frames will go there
    fn rotate(&self, state: &mut LogState, start: u64) -> Result<Box<dyn StorageFile>> {
        let file = create_segment(self.storage.as_ref(), &self.dir, start)?;
        state.segment_start = start;
        Ok(file)
    }

    fn lock(&self) -> MutexGuard<'_, LogState> {
        self.state.lock().expect("log lock poisoned")
    }
}

/// `(start lsn, path)` of every segment in `dir`, ascending
fn segments(storage: &dyn Storage, dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut segments: Vec<_> = storage
        .list(dir)?
        .into_iter()
        .filter_map(|path| {
            let start = path.file_name()?.to_str()?.strip_suffix(".log")?.parse::<u64>().ok()?;
            Some((start, path))
        })
        .collect();

    segments.sort_unstable_by_key(|&(start, _)| start);
    Ok(segments)
}

fn create_segment(storage: &dyn Storage, dir: &Path, start: u64) -> Result<Box<dyn StorageFile>> {
    let mut file = storage.create(&dir.join(format!("{start:020}.log")))?;
    file.sync()?;
    storage.sync_dir(dir)?;
    Ok(file)
}

/// the frame at `offset` if complete and intact
fn frame(bytes: &[u8], offset: usize, lsn: u64) -> Option<(Kind, &[u8])> {
    let header = bytes.get(offset..offset + HEADER)?;
    let crc = u32::from_le_bytes(header[..4].try_into().unwrap());
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let kind = match header[8] {
        1 => Kind::Commit,
        2 => Kind::Ddl,
        _ => return None,
    };

    let payload = bytes.get(offset + HEADER..(offset + HEADER).checked_add(len)?)?;
    let expected = crc32c_append(crc32c_append(crc32c(&lsn.to_le_bytes()), &header[4..]), payload);

    (crc == expected).then_some((kind, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::io::memory::MemoryStorage;
    use std::thread;

    fn root() -> MemoryStorage {
        let storage = MemoryStorage::new();
        storage.create_dir(Path::new("db")).unwrap();
        storage.sync_dir(Path::new("")).unwrap();
        storage
    }

    fn open(storage: &MemoryStorage, segment_size: u64) -> (Log, Vec<Vec<u8>>) {
        let mut frames = Vec::new();
        let (log, _) = Log::open(
            Arc::new(storage.clone()),
            PathBuf::from("db/wal"),
            SyncMode::Full,
            segment_size,
            0,
            |_, payload| {
                frames.push(payload.to_vec());
                Ok(())
            },
        )
        .unwrap();
        (log, frames)
    }

    fn commit(log: &Log, payload: &[u8]) {
        let end = log.append(Kind::Commit, payload);
        log.wait(end).unwrap();
    }

    #[test]
    fn frames_replay_in_order_across_segments() {
        let storage = root();
        let (log, frames) = open(&storage, 64);
        assert!(frames.is_empty());

        for n in 0..20u8 {
            commit(&log, &[n; 10]);
        }
        drop(log);

        let (_, frames) = open(&storage, 64);
        assert_eq!(frames, (0..20u8).map(|n| vec![n; 10]).collect::<Vec<_>>());
        assert!(storage.list(Path::new("db/wal")).unwrap().len() > 1);
    }

    #[test]
    fn torn_tail_is_truncated_at_every_cut() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"first");
        commit(&log, b"second");
        drop(log);

        let path = Path::new("db/wal/00000000000000000000.log");
        let whole = storage.read(path).unwrap();
        let first_end = HEADER + 5;

        for cut in first_end..whole.len() {
            let copy = root();
            copy.create_dir(Path::new("db/wal")).unwrap();
            let mut file = copy.create(path).unwrap();
            file.append(&whole[..cut]).unwrap();

            let (log, frames) = open(&copy, u64::MAX);
            assert_eq!(frames, [b"first".to_vec()], "cut at {cut}");
            commit(&log, b"third");
            drop(log);

            let (_, frames) = open(&copy, u64::MAX);
            assert_eq!(frames, [b"first".to_vec(), b"third".to_vec()]);
        }
    }

    #[test]
    fn checksums_bind_kind_length_payload_and_position() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"payload");
        drop(log);

        let bytes = storage.read(Path::new("db/wal/00000000000000000000.log")).unwrap();
        assert!(frame(&bytes, 0, 0).is_some());
        assert!(frame(&bytes, 0, 1).is_none());

        for byte in 0..bytes.len() {
            let mut flipped = bytes.clone();
            flipped[byte] ^= 0x10;
            assert!(frame(&flipped, 0, 0).is_none(), "flip at {byte} undetected");
        }
    }

    #[test]
    fn damage_before_the_last_segment_refuses_to_open() {
        let storage = root();
        let (log, _) = open(&storage, 16);
        for n in 0..4u8 {
            commit(&log, &[n; 10]);
        }
        drop(log);

        let first = Path::new("db/wal/00000000000000000000.log");
        let mut bytes = storage.read(first).unwrap();
        bytes[HEADER] ^= 1;
        storage.create(first).unwrap().append(&bytes).unwrap();

        let opened = Log::open(
            Arc::new(storage.clone()),
            PathBuf::from("db/wal"),
            SyncMode::Full,
            16,
            0,
            |_, _| Ok(()),
        );
        assert!(matches!(opened, Err(EngineError::Corrupt(_))));
    }

    #[test]
    fn concurrent_commits_share_syncs() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        let before = storage.operations();

        thread::scope(|scope| {
            for thread in 0..8u8 {
                let log = &log;
                scope.spawn(move || {
                    for n in 0..200u8 {
                        commit(log, &[thread, n]);
                    }
                });
            }
        });

        let operations = storage.operations() - before;
        assert!(operations < 2 * 1600, "{operations} operations for 1600 commits");
        drop(log);

        let (_, frames) = open(&storage, u64::MAX);
        assert_eq!(frames.len(), 1600);
    }

    #[test]
    fn a_failed_sync_poisons_every_later_commit() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"durable");

        storage.fail_at(2);
        let end = log.append(Kind::Commit, b"lost");
        assert!(matches!(log.wait(end), Err(EngineError::Poisoned)));

        let end = log.append(Kind::Commit, b"after");
        assert!(matches!(log.wait(end), Err(EngineError::Poisoned)));
        drop(log);

        storage.reboot(false);
        let (_, frames) = open(&storage, u64::MAX);
        assert_eq!(frames, [b"durable".to_vec()]);
    }

    #[test]
    fn replay_starts_at_the_requested_lsn_and_prunes_old_segments() {
        let storage = root();
        let (log, _) = open(&storage, 16);
        for n in 0..6u8 {
            commit(&log, &[n; 10]);
        }

        let from = 3 * (HEADER as u64 + 10);
        log.prune(from).unwrap();
        drop(log);

        let mut frames = Vec::new();
        Log::open(
            Arc::new(storage.clone()),
            PathBuf::from("db/wal"),
            SyncMode::Full,
            16,
            from,
            |_, payload| {
                frames.push(payload[0]);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(frames, [3, 4, 5]);
    }
}
