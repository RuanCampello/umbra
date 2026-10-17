//! The commit log: one checksummed frame per committed transaction, group-committed
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
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, OnceLock};

pub(crate) struct Log {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    sync: SyncMode,
    segment_size: u64,
    state: Mutex<LogState>,
    flushed: Condvar,
    /// Every byte below is durable
    durable: AtomicU64,
    /// Why the log stopped accepting frames, once it did
    poison: OnceLock<Arc<str>>,
}

struct LogState {
    buffer: Vec<u8>,
    /// Lsn after the last buffered frame
    end: u64,
    flushing: bool,
    /// `None` only while a leader is writing outside the lock
    file: Option<Box<dyn StorageFile>>,
    segment_start: u64,
    /// One per failed append, in order
    /// a [Ticket]'s epoch indexes the first after its frame
    rewinds: Vec<Rewind>,
}

/// A failed append: every frame past `to` was dropped and the log went on from there
struct Rewind {
    to: u64,
    kind: io::ErrorKind,
    cause: Arc<str>,
}

/// An appended frame, redeemed with [Log::wait]
#[derive(Clone, Copy, Debug)]
pub(crate) struct Ticket {
    /// Lsn right after the frame
    pub end: u64,
    /// Rewinds before the frame was appended
    epoch: usize,
}

/// Why a frame did not become durable
#[derive(Debug)]
pub(crate) enum Unlogged {
    /// A failed write dropped the frame and the log rewound to `to`, where the next frame goes
    Discarded { error: EngineError, to: u64 },
    /// The log stopped: nothing appended before or after becomes durable
    Poisoned(EngineError),
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
    /// Ends every flush with the durable lsn the flush started from: recovery uses it to tell a
    /// torn tail from damage to bytes that were already synced
    Mark = 3,
}

const HEADER: usize = 9;
/// Bytes of a [Kind::Mark] frame
const MARK: usize = HEADER + 8;
const MAX_FRAME: usize = 1 << 30;

impl Log {
    /// opens the log in `dir`, replaying every valid frame at or after `from` into `apply`
    ///
    /// a damaged frame in the last segment is a torn tail, and the segment is truncated there,
    /// unless a later flush's mark shows the damaged bytes had been synced before it: then, as
    /// with damage anywhere else, the log is corrupt and refuses to open
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
                    Some((Kind::Mark, payload)) => offset += HEADER + payload.len(),
                    Some((kind, payload)) => {
                        apply(kind, payload)?;
                        offset += HEADER + payload.len();
                        replay.frames += 1;
                    },
                    None if offset == bytes.len() => break,
                    None if last && !synced_later(&bytes, offset, *start) => {
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
            _ => (replay.end, create_segment(storage.as_ref(), &dir, replay.end)?),
        };

        let state = LogState {
            buffer: Vec::new(),
            end: replay.end,
            flushing: false,
            file: Some(file),
            segment_start,
            rewinds: Vec::new(),
        };

        let log = Self {
            storage,
            dir,
            sync,
            segment_size,
            state: Mutex::new(state),
            flushed: Condvar::new(),
            durable: AtomicU64::new(replay.end),
            poison: OnceLock::new(),
        };

        Ok((log, replay))
    }

    /// buffers one frame
    /// call order is log order
    pub fn append(&self, kind: Kind, payload: &[u8]) -> Ticket {
        assert!(payload.len() < MAX_FRAME, "log frame of {} bytes", payload.len());

        let mut state = self.lock();
        let lsn = state.end;
        encode(&mut state.buffer, lsn, kind, payload);
        state.end += (HEADER + payload.len()) as u64;

        Ticket { end: state.end, epoch: state.rewinds.len() }
    }

    /// blocks until the ticket's frame, and every frame before it, is durable, writing and syncing
    /// on behalf of every waiter when no flush is in progress (group commit)
    ///
    /// a failed write drops the flush and every frame buffered behind it, whose checksums bind
    /// positions the log no longer reaches, and the log goes on, a failed sync stops the log, since
    /// durability is then unknown (the kernel may have dropped the pages a retry would sync)
    pub fn wait(&self, ticket: Ticket) -> std::result::Result<(), Unlogged> {
        let mut state = self.lock();

        loop {
            // the first rewind after the append decides, even once later frames pass `end`
            if let Some(rewind) = state.rewinds.get(ticket.epoch) {
                return match ticket.end <= rewind.to {
                    true => Ok(()),
                    false => {
                        let error = io::Error::new(rewind.kind, rewind.cause.to_string()).into();
                        Err(Unlogged::Discarded { error, to: rewind.to })
                    },
                };
            }

            // durable frames stay durable whatever failed after their sync
            if self.durable.load(Ordering::Acquire) >= ticket.end {
                return Ok(());
            }

            if let Some(error) = self.poisoned() {
                return Err(Unlogged::Poisoned(error));
            }

            if state.flushing {
                state = self.flushed.wait(state).expect("log lock poisoned");
                continue;
            }

            state.flushing = true;
            // every byte before `durable` was synced before this flush began
            let started = self.durable.load(Ordering::Acquire);
            let lsn = state.end;
            encode(&mut state.buffer, lsn, Kind::Mark, &started.to_le_bytes());
            state.end += MARK as u64;
            let target = state.end;
            let mut buffer = std::mem::take(&mut state.buffer);
            let mut file = state.file.take().expect("no flush in progress");
            drop(state);

            let appended = file.append(&buffer);
            let synced = match (&appended, self.sync) {
                (Ok(()), SyncMode::Full) => file.sync(),
                _ => Ok(()),
            };

            state = self.lock();
            buffer.clear();
            if state.buffer.is_empty() {
                state.buffer = buffer;
            }

            match (appended, synced) {
                (Err(error), _) => self.rewind(&mut state, file.as_mut(), started, &error),
                (_, Err(error)) => self.stop(format!("log sync failed: {error}")),
                _ => {
                    self.durable.store(target, Ordering::Release);
                    if file.len() >= self.segment_size {
                        self.rotate(&mut state, &mut file, target);
                    }
                },
            }

            state.file = Some(file);
            state.flushing = false;
            self.flushed.notify_all();
        }
    }

    /// lsn after the last buffered frame
    pub fn end(&self) -> u64 {
        self.lock().end
    }

    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poison.get().is_some()
    }

    /// the error every write gets once the log stopped
    pub fn poisoned(&self) -> Option<EngineError> {
        self.poison.get().map(|cause| EngineError::Poisoned(Arc::clone(cause)))
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

    /// drops every frame past `to`, written or buffered, so the next one goes at `to`
    fn rewind(&self, state: &mut LogState, file: &mut dyn StorageFile, to: u64, error: &io::Error) {
        match file.truncate(to - state.segment_start) {
            Ok(_) => {
                state.buffer.clear();
                state.end = to;
                let rewind = Rewind { to, kind: error.kind(), cause: error.to_string().into() };
                state.rewinds.push(rewind);
            },
            Err(truncation) => {
                self.stop(format!(
                    "log write failed ({error}), then truncating it failed: {truncation}"
                ));
            },
        }
    }

    /// moves appends to a new segment at `start`
    /// when that fails, the old segment keeps growing and the next flush retries, once whatever the attempt left is removed
    fn rotate(&self, state: &mut LogState, file: &mut Box<dyn StorageFile>, start: u64) {
        let path = segment_path(&self.dir, start);
        match create_segment(self.storage.as_ref(), &self.dir, start) {
            Ok(next) => {
                *file = next;
                state.segment_start = start;
            },
            Err(_) if !self.storage.exists(&path) => {},
            Err(error) => {
                let removed =
                    self.storage.remove(&path).and_then(|_| self.storage.sync_dir(&self.dir));

                if let Err(removal) = removed {
                    let cause = format!(
                        "creating log segment {} failed ({error}), then removing it failed: {removal}",
                        path.display()
                    );
                    self.stop(cause);
                }
            },
        }
    }

    fn stop(&self, cause: String) {
        let _ = self.poison.set(cause.into());
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

fn segment_path(dir: &Path, start: u64) -> PathBuf {
    dir.join(format!("{start:020}.log"))
}

fn create_segment(storage: &dyn Storage, dir: &Path, start: u64) -> Result<Box<dyn StorageFile>> {
    let mut file = storage.create(&segment_path(dir, start))?;
    file.sync()?;
    storage.sync_dir(dir)?;
    Ok(file)
}

/// appends the frame of `payload` at `lsn` to `buffer`
fn encode(buffer: &mut Vec<u8>, lsn: u64, kind: Kind, payload: &[u8]) {
    let len = (payload.len() as u32).to_le_bytes();
    let crc = crc32c_append(
        crc32c_append(crc32c_append(crc32c(&lsn.to_le_bytes()), &len), &[kind as u8]),
        payload,
    );

    buffer.extend_from_slice(&crc.to_le_bytes());
    buffer.extend_from_slice(&len);
    buffer.push(kind as u8);
    buffer.extend_from_slice(payload);
}

/// whether an intact mark after `offset` records a flush that started past it: the bytes at `offset`
/// were synced before that flush was written, so their damage is corruption, not a torn write
fn synced_later(bytes: &[u8], offset: usize, start: u64) -> bool {
    let damaged = start + offset as u64;
    let marked = (MARK as u32 - HEADER as u32).to_le_bytes();

    (offset + 1..bytes.len().saturating_sub(MARK - 1)).any(|position| {
        bytes[position + 4..position + 8] == marked
            && bytes[position + 8] == Kind::Mark as u8
            && frame(bytes, position, start + position as u64).is_some_and(|(_, payload)| {
                u64::from_le_bytes(payload.try_into().expect("mark payload")) > damaged
            })
    })
}

/// the frame at `offset` if complete and intact
fn frame(bytes: &[u8], offset: usize, lsn: u64) -> Option<(Kind, &[u8])> {
    let header = bytes.get(offset..offset + HEADER)?;
    let crc = u32::from_le_bytes(header[..4].try_into().unwrap());
    let len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    let kind = match header[8] {
        1 => Kind::Commit,
        3 => Kind::Mark,
        _ => return None,
    };

    let payload = bytes.get(offset + HEADER..(offset + HEADER).checked_add(len)?)?;
    let expected = crc32c_append(crc32c_append(crc32c(&lsn.to_le_bytes()), &header[4..]), payload);

    (crc == expected).then_some((kind, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::io::memory::{Loss, MemoryStorage, Op};
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
        let second_end = first_end + MARK + HEADER + 6;

        for cut in first_end..whole.len() {
            let copy = root();
            copy.create_dir(Path::new("db/wal")).unwrap();
            let mut file = copy.create(path).unwrap();
            file.append(&whole[..cut]).unwrap();

            // a whole frame is valid without its flush's mark
            let mut expected = vec![b"first".to_vec()];
            if cut >= second_end {
                expected.push(b"second".to_vec());
            }

            let (log, frames) = open(&copy, u64::MAX);
            assert_eq!(frames, expected, "cut at {cut}");
            commit(&log, b"third");
            drop(log);

            expected.push(b"third".to_vec());
            let (_, frames) = open(&copy, u64::MAX);
            assert_eq!(frames, expected);
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

        for byte in 0..HEADER + 7 {
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
        let lost = log.append(Kind::Commit, b"lost");
        match log.wait(lost) {
            Err(Unlogged::Poisoned(EngineError::Poisoned(cause))) => {
                assert!(cause.contains("sync"), "{cause}")
            },
            other => panic!("{other:?}"),
        }

        let after = log.append(Kind::Commit, b"after");
        assert!(matches!(log.wait(after), Err(Unlogged::Poisoned(_))));
        drop(log);

        storage.reboot(false);
        let (_, frames) = open(&storage, u64::MAX);
        assert_eq!(frames, [b"durable".to_vec()]);
    }

    #[test]
    fn a_failed_append_discards_its_flush_and_the_log_goes_on() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"durable");
        let durable = log.end();

        // the failing append writes half of its bytes, which the rewind truncates
        storage.fail_at(1);
        let lost = log.append(Kind::Commit, b"lost");
        assert!(matches!(log.wait(lost), Err(Unlogged::Discarded { to, .. }) if to == durable));
        assert_eq!(log.end(), durable);

        commit(&log, b"after");
        drop(log);

        for loss in [Loss::Clean, Loss::Torn] {
            storage.reboot_with(loss);
            let (_, frames) = open(&storage, u64::MAX);
            assert_eq!(frames, [b"durable".to_vec(), b"after".to_vec()], "{loss:?}");
        }
    }

    #[test]
    fn frames_buffered_behind_a_failed_flush_are_discarded_with_it() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"durable");

        storage.hold(Op::Append);
        thread::scope(|scope| {
            let leader = scope.spawn(|| {
                let first = log.append(Kind::Commit, b"first");
                log.wait(first)
            });
            while storage.waiting() == 0 {
                thread::yield_now();
            }

            // appended while the leader's write is in flight, so not part of it
            let behind = log.append(Kind::Commit, b"behind");
            storage.fail_at(1);
            storage.release();

            assert!(matches!(leader.join().unwrap(), Err(Unlogged::Discarded { .. })));
            assert!(matches!(log.wait(behind), Err(Unlogged::Discarded { .. })));

            // a later frame moves the durable position past `behind`, which stays discarded
            let later = log.append(Kind::Commit, &[7; 64]);
            log.wait(later).unwrap();
            assert!(later.end > behind.end);
            assert!(matches!(log.wait(behind), Err(Unlogged::Discarded { .. })));
        });
        drop(log);

        storage.reboot(false);
        let (_, frames) = open(&storage, u64::MAX);
        assert_eq!(frames, [b"durable".to_vec(), vec![7; 64]]);
    }

    #[test]
    fn a_failed_truncation_after_a_failed_append_poisons() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"durable");

        storage.fail_from(1);
        let lost = log.append(Kind::Commit, b"lost");
        match log.wait(lost) {
            Err(Unlogged::Poisoned(EngineError::Poisoned(cause))) => {
                assert!(cause.contains("truncat"), "{cause}")
            },
            other => panic!("{other:?}"),
        }
        drop(log);

        for loss in [Loss::Clean, Loss::Torn, Loss::Garbled] {
            storage.reboot_with(loss);
            let (_, frames) = open(&storage, u64::MAX);
            assert_eq!(frames, [b"durable".to_vec()], "{loss:?}");
        }
    }

    #[test]
    fn a_failed_rotation_keeps_the_segment_and_retries_on_the_next_flush() {
        // a flush appends (1) and syncs (2); rotating creates the segment (3), syncs it (4) and
        // syncs the directory (5)
        for failure in [3, 4, 5] {
            let storage = root();
            let (log, _) = open(&storage, 1);
            commit(&log, b"first");

            storage.fail_at(failure);
            commit(&log, b"durable before the rotation failed");
            commit(&log, b"after");
            assert!(!log.is_poisoned(), "failure at {failure}");
            drop(log);

            storage.reboot(false);
            let (_, frames) = open(&storage, 1);
            assert_eq!(frames.len(), 3, "failure at {failure}");
        }
    }

    #[test]
    fn a_partly_created_segment_that_cannot_be_removed_poisons() {
        let storage = root();
        let (log, _) = open(&storage, 1);
        commit(&log, b"first");

        // the new segment exists, its sync fails, and so does its removal
        storage.fail_from(4);
        commit(&log, b"durable");
        assert!(log.is_poisoned());
        drop(log);

        storage.reboot(false);
        let (_, frames) = open(&storage, 1);
        assert_eq!(frames, [b"first".to_vec(), b"durable".to_vec()]);
    }

    #[test]
    fn replay_starts_at_the_requested_lsn_and_prunes_old_segments() {
        let storage = root();
        let (log, _) = open(&storage, 16);
        for n in 0..6u8 {
            commit(&log, &[n; 10]);
        }

        let from = 3 * (HEADER + 10 + MARK) as u64;
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

    #[test]
    fn damage_to_a_flush_synced_before_a_later_one_refuses_to_open() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"first");
        commit(&log, b"second");
        drop(log);

        let path = Path::new("db/wal/00000000000000000000.log");
        let mut bytes = storage.read(path).unwrap();
        bytes[HEADER] ^= 1;
        storage.create(path).unwrap().append(&bytes).unwrap();

        let opened = Log::open(
            Arc::new(storage.clone()),
            PathBuf::from("db/wal"),
            SyncMode::Full,
            u64::MAX,
            0,
            |_, _| Ok(()),
        );
        assert!(matches!(opened, Err(EngineError::Corrupt(_))), "the first flush was synced");
    }

    #[test]
    fn a_reordered_write_inside_the_last_flush_is_a_torn_tail() {
        let storage = root();
        let (log, _) = open(&storage, u64::MAX);
        commit(&log, b"first");
        commit(&log, b"second");
        drop(log);

        // the last flush lost its first bytes but kept its later ones, mark included
        let path = Path::new("db/wal/00000000000000000000.log");
        let mut bytes = storage.read(path).unwrap();
        let second = HEADER + 5 + MARK;
        bytes[second..second + 4].fill(0);
        storage.create(path).unwrap().append(&bytes).unwrap();

        let (_, frames) = open(&storage, u64::MAX);
        assert_eq!(frames, [b"first".to_vec()]);
    }
}
