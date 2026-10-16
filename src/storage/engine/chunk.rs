//! Chunks of 1024 row slots, and anti-caching: chunks no snapshot can see differently are written
//! to cold files and reloaded on demand
//!
//! Cold files are a cache of committed state, never its only copy: every evicted row is in the
//! commit log or the current checkpoint image, so they are not synced and `open` discards them

use crate::collections::smallvec::SmallVec;
use crate::core::crc32c::crc32c;
use crate::storage::engine::{
    codec::{Cursor, put_row},
    error,
    io::Storage,
    table::Row,
};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
    atomic::{AtomicU64, AtomicUsize, Ordering},
};

pub(crate) struct Chunk {
    slots: RwLock<Slots>,
    /// [Cold::clock] at the last access, for approximate LRU
    touched: AtomicU64,
    cold: Option<Arc<Cold>>,
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
    max_resident: usize,
    resident: AtomicUsize,
    /// Coarse access clock, advanced by maintenance; chunks copy it when touched
    clock: AtomicU64,
}

/// Read access to a resident chunk's slots
pub(crate) struct Resident<'a>(RwLockReadGuard<'a, Slots>);
/// Write access to a resident chunk's slots
pub(crate) struct ResidentMut<'a>(RwLockWriteGuard<'a, Slots>);

enum Slots {
    Resident(Box<[Chain]>),
    Evicted(PathBuf),
}

pub(in crate::storage::engine) const CHUNK_BITS: u32 = 10;
pub(in crate::storage::engine) const CHUNK_SIZE: usize = 1 << CHUNK_BITS;

/// Oldest to newest, at most one flagged version, always the newest
pub(crate) type Chain = SmallVec<Version, 1>;

impl Chunk {
    pub fn new(cold: Option<Arc<Cold>>) -> Self {
        if let Some(cold) = &cold {
            cold.resident.fetch_add(1, Ordering::Relaxed);
        }

        let slots = (0..CHUNK_SIZE).map(|_| Chain::new()).collect();
        let slots = RwLock::new(Slots::Resident(slots));
        let touched = AtomicU64::new(cold.as_ref().map_or(0, |cold| cold.now()));

        Self { slots, touched, cold }
    }

    /// the slots, reloading them first if the chunk was evicted
    pub fn read(&self) -> Resident<'_> {
        loop {
            let slots = self.slots.read().expect("chunk latch poisoned");
            if let Slots::Resident(_) = &*slots {
                self.touch();
                return Resident(slots);
            }

            drop(slots);
            self.write();
        }
    }

    /// the slots for writing, reloading them first if the chunk was evicted
    pub fn write(&self) -> ResidentMut<'_> {
        let mut slots = self.slots.write().expect("chunk latch poisoned");

        if let Slots::Evicted(path) = &*slots {
            let cold = self.cold.as_ref().expect("only durable tables evict");
            let loaded = cold.load(path);
            cold.discard(path);
            cold.resident.fetch_add(1, Ordering::Relaxed);
            *slots = Slots::Resident(loaded);
        }

        self.touch();
        ResidentMut(slots)
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
    pub fn visit<R>(&self, f: impl FnOnce(&[Chain]) -> R) -> R {
        let slots = self.slots.read().expect("chunk latch poisoned");
        match &*slots {
            Slots::Resident(chains) => f(chains),
            Slots::Evicted(path) => {
                let cold = self.cold.as_ref().expect("only durable tables evict");
                f(&cold.load(path))
            },
        }
    }

    /// writes the chunk to `path` and drops its slots, if every row has a single version committed
    /// at or below `watermark`
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
    ///
    /// memory holds at most `max_resident` chunks after each eviction round (as far as eligible chunks allow)
    pub fn new(storage: Arc<dyn Storage>, dir: PathBuf, max_resident: usize) -> Self {
        Self {
            storage,
            dir,
            max_resident,
            resident: AtomicUsize::new(0),
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
    pub fn over_budget(&self) -> bool {
        self.resident() > self.max_resident
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

    /// a disk that cannot return what it was just given has failed: there is no row-level
    /// recovery from that, the engine stops loudly and a restart rebuilds from image and log
    fn load(&self, path: &Path) -> Box<[Chain]> {
        let unreadable = |why: &dyn std::fmt::Display| -> ! {
            panic!("cold chunk {} is unreadable: {why}", path.display())
        };

        let bytes = self.storage.read(path).unwrap_or_else(|e| unreadable(&e));
        let split = bytes.len().checked_sub(4).unwrap_or_else(|| unreadable(&"truncated"));
        let (content, footer) = bytes.split_at(split);
        if crc32c(content) != u32::from_le_bytes(footer.try_into().unwrap()) {
            unreadable(&"checksum mismatch");
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

        decoded.unwrap_or_else(|e| unreadable(&e));
        slots
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
            },
        }
    }
}
