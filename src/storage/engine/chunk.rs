//! Chunks of 1024 row slots, and anti-caching: chunks no snapshot can see differently are written
//! to cold files and reloaded on demand
//!
//! Cold files are a cache of committed state, never its only copy: every evicted row is in the
//! commit log or the current checkpoint image, so they are not synced and `open` discards them

use crate::collections::smallvec::SmallVec;
use crate::core::{
    crc32c::crc32c,
    numeric::{Long, Numeric},
};
use crate::sql::Value;
use crate::storage::engine::{
    codec::{Cursor, put_row},
    error,
    io::Storage,
    table::Row,
};
use std::ops::{Bound, Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};

pub(crate) struct Chunk {
    slots: RwLock<Slots>,
    /// [Cold::clock] at the last access, for approximate LRU
    touched: AtomicU64,
    cold: Option<Arc<Cold>>,
    /// Zone map: per column, the least and greatest non-NULL value of every version ever written
    /// here (small materialised aggregates, Moerkotte 1998)
    /// it only widens, so it holds for every snapshot, and it stays when the chunk is evicted
    bounds: Mutex<Vec<Option<(Value, Value)>>>,
    /// What this chunk adds to [Cold::bytes]: exact when `measured`, otherwise an over-estimate
    bytes: AtomicUsize,
    measured: AtomicBool,
}

pub(crate) struct Version {
    pub(in crate::storage::engine) stamp: u64,
    /// `None` marks a deletion
    pub(in crate::storage::engine) row: Option<Row>,
}

/// The cold-file store of a durable engine, shared by its tables
pub(crate) struct Cold {
    storage: Arc<dyn Storage>,
    dir: PathBuf,
    budget: Budget,
    /// Resident chunks
    resident: AtomicUsize,
    /// Approximate bytes of the resident chunks: slots, versions, rows and what their values point to
    bytes: AtomicUsize,
    /// Coarse access clock, advanced by maintenance, chunks copy it when touched
    clock: AtomicU64,
}

/// Read access to a [resident](Slots::Resident) chunk's slots
pub(crate) struct Resident<'a>(RwLockReadGuard<'a, Slots>);
/// Write access to a [resident](Slots::Resident) chunk's slots
pub(crate) struct ResidentMut<'a>(RwLockWriteGuard<'a, Slots>);

/// How much table data a durable engine keeps in memory before evicting
/// the coldest settled chunks
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Budget {
    /// approximate bytes of resident chunks, indexes and query memory come on top
    Bytes(usize),
    /// whole chunks, whatever their size
    Chunks(usize),
}

enum Slots {
    Resident(Box<[Chain]>),
    Evicted(PathBuf),
}

pub(in crate::storage::engine) const CHUNK_BITS: u32 = 10;
pub(crate) const CHUNK_SIZE: usize = 1 << CHUNK_BITS;

/// Oldest to newest, at most one flagged version, always the newest
pub(crate) type Chain = SmallVec<Version, 1>;

pub(in crate::storage::engine) const SKELETON: usize = CHUNK_SIZE * size_of::<Chain>();
/// strong and weak counts ahead of an `Arc`'s data
const ARC_HEADER: usize = 2 * size_of::<usize>();

impl Chunk {
    pub fn new(cold: Option<Arc<Cold>>) -> Self {
        if let Some(cold) = &cold {
            cold.resident.fetch_add(1, Ordering::Relaxed);
            cold.bytes.fetch_add(SKELETON, Ordering::Relaxed);
        }

        let slots = (0..CHUNK_SIZE).map(|_| Chain::new()).collect();
        let slots = RwLock::new(Slots::Resident(slots));
        let touched = AtomicU64::new(cold.as_ref().map_or(0, |cold| cold.now()));

        Self {
            slots,
            touched,
            cold,
            bounds: Mutex::new(Vec::new()),
            bytes: AtomicUsize::new(SKELETON),
            measured: AtomicBool::new(true),
        }
    }

    /// widens the zone map to cover `row`, a new version written under the chunk latch, and
    /// counts it against the budget
    // PERFORMANCE: one comparison per column per write, under the chunk latch the writer holds
    pub fn widen(&self, row: &[Value]) {
        if let Some(cold) = &self.cold {
            let bytes = footprint(row);
            self.bytes.fetch_add(bytes, Ordering::Relaxed);
            cold.bytes.fetch_add(bytes, Ordering::Relaxed);
            self.measured.store(false, Ordering::Relaxed);
        }

        let mut bounds = self.bounds.lock().expect("zone map poisoned");
        if bounds.len() < row.len() {
            bounds.resize(row.len(), None);
        }

        for (bound, value) in bounds.iter_mut().zip(row).filter(|(_, value)| !value.is_null()) {
            match bound {
                None => *bound = Some((value.clone(), value.clone())),
                Some((least, _)) if value < least => *least = value.clone(),
                Some((_, greatest)) if value > greatest => *greatest = value.clone(),
                _ => {},
            }
        }
    }

    /// whether a row of this chunk may have its columns within every range: `false` proves none does
    pub fn may_hold(&self, ranges: &[(usize, Bound<Value>, Bound<Value>)]) -> bool {
        let bounds = self.bounds.lock().expect("zone map poisoned");
        ranges.iter().all(|(column, start, end)| {
            let Some(Some((least, greatest))) = bounds.get(*column) else {
                return false;
            };
            let above = match start {
                Bound::Included(start) => start <= greatest,
                Bound::Excluded(start) => start < greatest,
                Bound::Unbounded => true,
            };
            let below = match end {
                Bound::Included(end) => end >= least,
                Bound::Excluded(end) => end > least,
                Bound::Unbounded => true,
            };
            above && below
        })
    }

    /// a version was removed or replaced: [Chunk::measure] recounts the chunk
    #[inline]
    pub fn changed(&self) {
        self.measured.store(false, Ordering::Relaxed);
    }

    /// replaces the estimate written versions left with the exact footprint of `chains`, this chunk's slots under its latch
    pub fn measure(&self, chains: &[Chain]) {
        let Some(cold) = &self.cold else {
            return;
        };
        if self.measured.swap(true, Ordering::Relaxed) {
            return;
        }

        let exact = SKELETON + chains.iter().map(chain_footprint).sum::<usize>();
        let previous = self.bytes.swap(exact, Ordering::Relaxed);
        cold.bytes.fetch_add(exact, Ordering::Relaxed);
        cold.bytes.fetch_sub(previous, Ordering::Relaxed);
    }

    /// the slots, reloading them first if the chunk was evicted
    pub fn read(&self) -> error::Result<Resident<'_>> {
        loop {
            let slots = self.slots.read().expect("chunk latch poisoned");
            if let Slots::Resident(_) = &*slots {
                self.touch();
                return Ok(Resident(slots));
            }

            drop(slots);
            self.write()?;
        }
    }

    /// the slots for writing, reloading them first if the chunk was evicted
    /// a cold file that cannot be read leaves the chunk evicted and fails the access
    pub fn write(&self) -> error::Result<ResidentMut<'_>> {
        let mut slots = self.slots.write().expect("chunk latch poisoned");

        if let Slots::Evicted(path) = &*slots {
            let cold = self.cold.as_ref().expect("only durable tables evict");
            let loaded = cold.load(path)?;
            cold.discard(path);
            let bytes = SKELETON + loaded.iter().map(chain_footprint).sum::<usize>();
            self.bytes.store(bytes, Ordering::Relaxed);
            self.measured.store(true, Ordering::Relaxed);
            cold.resident.fetch_add(1, Ordering::Relaxed);
            cold.bytes.fetch_add(bytes, Ordering::Relaxed);
            *slots = Slots::Resident(loaded);
        }

        self.touch();
        Ok(ResidentMut(slots))
    }

    /// [Chunk::write] for a chunk known to be resident: one holding an uncommitted version is never evicted
    pub fn write_resident(&self) -> ResidentMut<'_> {
        self.write_if_resident()
            .expect("a chunk holding an uncommitted version is resident")
    }

    /// the slots when resident, without reloading
    pub fn write_if_resident(&self) -> Option<ResidentMut<'_>> {
        let slots = self.slots.write().expect("chunk latch poisoned");
        match &*slots {
            Slots::Resident(_) => Some(ResidentMut(slots)),
            _ => None,
        }
    }

    /// runs `f` over the slots
    /// an evicted chunk is decoded into a temporary buffer and stays
    /// evicted, so bulk readers (scans, checkpoints, index builds) do not refill memory
    pub fn visit<R>(&self, f: impl FnOnce(&[Chain]) -> R) -> error::Result<R> {
        let slots = self.slots.read().expect("chunk latch poisoned");
        match &*slots {
            Slots::Resident(chains) => Ok(f(chains)),
            Slots::Evicted(path) => {
                let cold = self.cold.as_ref().expect("only durable tables evict");
                Ok(f(&cold.load(path)?))
            },
        }
    }

    /// writes the chunk to `path` and drops its slots, if every row has a single version committed at or below `watermark`
    pub fn evict(&self, path: PathBuf, watermark: u64) -> bool {
        let cold = self.cold.as_ref().expect("only durable tables evict");
        let mut slots = self.slots.write().expect("chunk latch poisoned");

        let Slots::Resident(chains) = &*slots else {
            return false;
        };

        let settled = chains
            .iter()
            .all(|chain| chain.is_empty() || (chain.len() == 1 && chain[0].stamp <= watermark));

        if !settled || cold.store(&path, chains).is_err() {
            return false;
        }

        *slots = Slots::Evicted(path);
        cold.resident.fetch_sub(1, Ordering::Relaxed);
        cold.bytes.fetch_sub(self.bytes.swap(0, Ordering::Relaxed), Ordering::Relaxed);
        true
    }

    #[inline]
    pub fn is_resident(&self) -> bool {
        matches!(&*self.slots.read().expect("chunk latch poisoned"), Slots::Resident(_))
    }

    #[inline]
    pub fn touched(&self) -> u64 {
        self.touched.load(Ordering::Relaxed)
    }

    #[inline]
    fn touch(&self) {
        if let Some(cold) = &self.cold {
            self.touched.store(cold.now(), Ordering::Relaxed);
        }
    }
}

impl Cold {
    /// cold files live in `dir`
    /// memory stays within `budget` after each eviction round (as far as eligible chunks allow)
    pub fn new(storage: Arc<dyn Storage>, dir: PathBuf, budget: Budget) -> Self {
        Self {
            storage,
            dir,
            budget,
            resident: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            clock: AtomicU64::new(1),
        }
    }

    #[inline]
    pub fn now(&self) -> u64 {
        self.clock.load(Ordering::Relaxed)
    }

    pub fn advance(&self) {
        self.clock.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn resident(&self) -> usize {
        self.resident.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn over_budget(&self) -> bool {
        match self.budget {
            Budget::Bytes(max) => self.bytes() > max,
            Budget::Chunks(max) => self.resident() > max,
        }
    }

    pub fn path(&self, table: u32, chunk: usize) -> PathBuf {
        self.dir.join(table.to_string()).join(format!("{chunk}.blk"))
    }

    /// `count u16 | (slot u16 | stamp u64 | row)* | crc32c u32`
    /// lone deletions are dropped
    fn store(&self, path: &Path, chains: &[Chain]) -> std::io::Result<()> {
        let live: Vec<_> = chains
            .iter()
            .enumerate()
            .filter_map(|(slot, chain)| {
                let version = chain.first()?;
                version.row.as_ref().map(|row| (slot, version.stamp, row))
            })
            .collect();

        let mut buf = (live.len() as u16).to_le_bytes().to_vec();
        for (slot, stamp, row) in live {
            buf.extend_from_slice(&(slot as u16).to_le_bytes());
            buf.extend_from_slice(&stamp.to_le_bytes());
            put_row(&mut buf, row);
        }
        buf.extend_from_slice(&crc32c(&buf).to_le_bytes());

        self.storage
            .create_dir(path.parent().expect("cold files live in a table directory"))?;
        self.storage.create(path)?.append(&buf)
    }

    /// a cold file that cannot be read back fails the statement that needed it: its rows are still
    /// in the checkpoint image and the log, which a restart rebuilds the chunk from
    fn load(&self, path: &Path) -> error::Result<Box<[Chain]>> {
        let unreadable = |why: &dyn std::fmt::Display| {
            error::EngineError::Corrupt(format!(
                "cold chunk {} is unreadable: {why}",
                path.display()
            ))
        };

        let bytes = self.storage.read(path).map_err(|e| unreadable(&e))?;
        let split = bytes.len().checked_sub(4).ok_or_else(|| unreadable(&"truncated"))?;
        let (content, footer) = bytes.split_at(split);
        if crc32c(content) != u32::from_le_bytes(footer.try_into().unwrap()) {
            return Err(unreadable(&"checksum mismatch"));
        }

        let mut slots: Box<_> = (0..CHUNK_SIZE).map(|_| Chain::new()).collect();
        let mut cursor = Cursor::new(content, "cold chunk");
        let decoded = (|| {
            for _ in 0..cursor.u16()? {
                let slot = cursor.u16()? as usize;
                let stamp = cursor.u64()?;
                let row = Some(cursor.row()?);
                slots
                    .get_mut(slot)
                    .ok_or_else(|| cursor.corrupt("slot out of range"))?
                    .push(Version { stamp, row });
            }
            Ok::<_, error::EngineError>(())
        })();

        decoded.map_err(|e| unreadable(&e))?;
        Ok(slots)
    }

    /// removes a cold file whose chunk is resident again, a leftover is harmless (`open` clears the directory)
    fn discard(&self, path: &Path) {
        let _ = self.storage.remove(path);
    }
}

impl Deref for Resident<'_> {
    type Target = [Chain];

    #[inline]
    fn deref(&self) -> &[Chain] {
        match &*self.0 {
            Slots::Resident(chains) => chains,
            _ => unreachable!("guards only exist over resident chunks"),
        }
    }
}

impl Deref for ResidentMut<'_> {
    type Target = [Chain];

    #[inline]
    fn deref(&self) -> &[Chain] {
        match &*self.0 {
            Slots::Resident(chains) => chains,
            _ => unreachable!("guards only exist over resident chunks"),
        }
    }
}

impl DerefMut for ResidentMut<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [Chain] {
        match &mut *self.0 {
            Slots::Resident(chains) => chains,
            _ => unreachable!("guards only exist over resident chunks"),
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        let Some(cold) = &self.cold else {
            return;
        };

        match self.slots.get_mut().expect("chunk latch poisoned") {
            Slots::Evicted(path) => cold.discard(path),
            _ => {
                cold.resident.fetch_sub(1, Ordering::Relaxed);
                cold.bytes.fetch_sub(*self.bytes.get_mut(), Ordering::Relaxed);
            },
        }
    }
}

/// approximate bytes a chain holds beyond its slot
fn chain_footprint(chain: &Chain) -> usize {
    let spilled = match chain.len() > 1 {
        true => chain.capacity() * size_of::<Version>(),
        _ => 0,
    };
    spilled
        + chain
            .iter()
            .filter_map(|version| version.row.as_deref())
            .map(footprint)
            .sum::<usize>()
}

/// approximate bytes of a row: its shared slice and what its values point to
fn footprint(row: &[Value]) -> usize {
    let pointed: usize = row
        .iter()
        .map(|value| match value {
            Value::String(text) => ARC_HEADER + text.len(),
            Value::Blob(bytes) => ARC_HEADER + bytes.len(),
            Value::Numeric(Numeric::Long(long)) => size_of::<Long>() + long.digits.capacity() * 2,
            _ => 0,
        })
        .sum();
    ARC_HEADER + size_of_val(row) + pointed
}
