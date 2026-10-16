//! Timestamps, ordered publication of commits and the registry of active snapshots

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

/// Commit timestamps and the snapshots readers take from them
///
/// A commit gets its timestamp under the engine's commit mutex (so timestamps, and later log
/// positions, follow one total order), stamps its versions, then [Clock::publish]es
/// `visible` only advances over a gap-free prefix of published timestamps, so a reader at
/// `S = visible` sees every commit `≤ S` completely and nothing of any later one
pub(crate) struct Clock {
    visible: AtomicU64,
    publication: Mutex<Publication>,
    snapshots: Mutex<BTreeMap<u64, u32>>,
    /// Oldest snapshot any reader may still use, refreshed whenever the registry changes
    /// stale values only err low, which keeps more versions alive, never fewer
    watermark: AtomicU64,
    next_txn: AtomicU64,
}

struct Publication {
    next: u64,
    /// Published out of order, waiting for `next`: timestamp -> log position after its record
    done: BTreeMap<u64, u64>,
    /// Log position after the record of the last visible timestamp
    lsn: u64,
}

/// Uncommitted versions carry `TXN_FLAG | txn id`, larger than every commit timestamp
pub(crate) const TXN_FLAG: u64 = 1 << 63;

impl Clock {
    /// a clock whose every timestamp up to `last` is committed and visible, `lsn` being the log
    /// position after the last one
    pub fn recovered(last: u64, lsn: u64) -> Self {
        Self {
            visible: AtomicU64::new(last),
            publication: Mutex::new(Publication { next: last + 1, done: BTreeMap::new(), lsn }),
            snapshots: Mutex::new(BTreeMap::new()),
            watermark: AtomicU64::new(last),
            next_txn: AtomicU64::new(1),
        }
    }

    #[inline]
    pub fn visible(&self) -> u64 {
        self.visible.load(Ordering::Acquire)
    }

    #[inline]
    pub fn watermark(&self) -> u64 {
        self.watermark.load(Ordering::Acquire)
    }

    /// a fresh uncommitted marker
    #[inline]
    pub fn marker(&self) -> u64 {
        TXN_FLAG | self.next_txn.fetch_add(1, Ordering::Relaxed)
    }

    /// registers and returns a snapshot of everything published so far
    pub fn snapshot(&self) -> u64 {
        let mut snapshots = self.snapshots();
        let snapshot = self.visible();

        *snapshots.entry(snapshot).or_default() += 1;
        self.refresh_watermark(&snapshots);

        snapshot
    }

    pub fn release(&self, snapshot: u64) {
        let mut snapshots = self.snapshots();
        Self::forget(&mut snapshots, snapshot);
        self.refresh_watermark(&snapshots);
    }

    /// swaps `snapshot` for a fresh one (READ COMMITTED statements)
    pub fn refresh(&self, snapshot: u64) -> u64 {
        let mut snapshots = self.snapshots();
        Self::forget(&mut snapshots, snapshot);

        let fresh = self.visible();
        *snapshots.entry(fresh).or_default() += 1;
        self.refresh_watermark(&snapshots);

        fresh
    }

    /// makes `ts` visible once every earlier timestamp is
    /// `lsn` is the log position after its record (0 without a log)
    pub fn publish(&self, ts: u64, lsn: u64) {
        let mut publication = self.publication();
        let Publication { next, done, lsn: last_lsn } = &mut *publication;
        done.insert(ts, lsn);

        let mut last = None;
        while let Some(lsn) = done.remove(next) {
            last = Some(*next);
            *last_lsn = lsn;
            *next += 1;
        }

        if let Some(last) = last {
            self.visible.store(last, Ordering::Release);
        }
    }

    /// registers a snapshot of everything published, with the log position right after it
    pub fn pin(&self) -> (u64, u64) {
        let publication = self.publication();
        (self.snapshot(), publication.lsn)
    }

    fn publication(&self) -> MutexGuard<'_, Publication> {
        self.publication.lock().expect("publication lock poisoned")
    }

    fn snapshots(&self) -> MutexGuard<'_, BTreeMap<u64, u32>> {
        self.snapshots.lock().expect("snapshot registry poisoned")
    }

    fn forget(snapshots: &mut BTreeMap<u64, u32>, snapshot: u64) {
        let count = snapshots.get_mut(&snapshot).expect("released a snapshot that was never taken");

        *count -= 1;
        if *count == 0 {
            snapshots.remove(&snapshot);
        }
    }

    fn refresh_watermark(&self, snapshots: &BTreeMap<u64, u32>) {
        let oldest = snapshots
            .first_key_value()
            .map_or_else(|| self.visible(), |(&snapshot, _)| snapshot);

        self.watermark.store(oldest, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_waits_for_every_earlier_timestamp() {
        let clock = Clock::recovered(0, 0);

        clock.publish(3, 30);
        clock.publish(2, 20);
        assert_eq!(clock.visible(), 0);
        assert_eq!(clock.pin(), (0, 0));

        clock.publish(1, 10);
        assert_eq!(clock.visible(), 3);
        assert_eq!(clock.pin(), (3, 30));
    }

    #[test]
    fn watermark_follows_the_oldest_snapshot() {
        let clock = Clock::recovered(0, 0);
        let old = clock.snapshot();

        clock.publish(1, 0);
        let young = clock.snapshot();
        assert_eq!((old, young), (0, 1));
        assert_eq!(clock.watermark(), 0);

        clock.release(old);
        assert_eq!(clock.watermark(), 1);

        let refreshed = clock.refresh(young);
        assert_eq!(refreshed, 1);
        clock.release(refreshed);
        assert_eq!(clock.watermark(), 1);
    }

    #[test]
    fn markers_are_flagged_and_unique() {
        let clock = Clock::recovered(0, 0);
        let (a, b) = (clock.marker(), clock.marker());

        assert_ne!(a, b);
        assert!(a & TXN_FLAG != 0 && b > u64::MAX / 2);
    }
}
