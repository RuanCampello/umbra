//! Timestamps, ordered publication of commits and the registry of active snapshots

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Instant;

/// Commit timestamps and the snapshots readers take from them
///
/// A commit gets its timestamp under the engine's commit mutex (so timestamps, and later log
/// positions, follow one total order), stamps its versions, then [Clock::publish]es
/// `visible` only advances over a gap-free prefix of published timestamps, so a reader at
/// `S = visible` sees every commit `≤ S` completely and nothing of any later one
pub(crate) struct Clock {
    visible: AtomicU64,
    publication: Mutex<Publication>,
    /// Active snapshots, sharded so beginning and ending transactions on different threads do not
    /// contend on one lock
    shards: Box<[Shard]>,
    /// A lower bound of every snapshot taken now or later: stale values only err low, which keeps
    /// more versions alive, never fewer
    watermark: AtomicU64,
    /// Microseconds since `epoch` of the last [Clock::settle_watermark]
    settled: AtomicU64,
    epoch: Instant,
    next_txn: AtomicU64,
}

/// A registered snapshot and the shard holding it
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Registration {
    pub ts: u64,
    shard: usize,
}

/// Snapshots registered through one shard, on a cache line of their own
#[repr(align(128))]
struct Shard {
    snapshots: Mutex<BTreeMap<u64, u32>>,
    /// The oldest of `snapshots`, `u64::MAX` when none
    oldest: AtomicU64,
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
const SHARDS: usize = 16;
/// How often, at most, an ending transaction rescans the shards for the watermark
const SETTLE_MICROS: u64 = 50;

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

impl Clock {
    /// a clock whose every timestamp up to `last` is committed and visible, `lsn` being the log
    /// position after the last one
    pub fn recovered(last: u64, lsn: u64) -> Self {
        let shards = (0..SHARDS)
            .map(|_| Shard {
                snapshots: Mutex::new(BTreeMap::new()),
                oldest: AtomicU64::new(u64::MAX),
            })
            .collect();

        Self {
            visible: AtomicU64::new(last),
            publication: Mutex::new(Publication { next: last + 1, done: BTreeMap::new(), lsn }),
            shards,
            watermark: AtomicU64::new(last),
            settled: AtomicU64::new(0),
            epoch: Instant::now(),
            next_txn: AtomicU64::new(1),
        }
    }

    #[inline]
    pub fn visible(&self) -> u64 {
        self.visible.load(Ordering::SeqCst)
    }

    /// a lower bound of every active snapshot, at most [SETTLE_MICROS] behind while transactions
    /// end, or until the next [Clock::settle_watermark]
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
    // PERFORMANCE: one uncontended shard lock, the watermark is not rescanned
    pub fn snapshot(&self) -> Registration {
        let shard = SHARD.with(|shard| *shard);
        let mut snapshots =
            self.shards[shard].snapshots.lock().expect("snapshot registry poisoned");
        let ts = self.register(shard, &mut snapshots);
        Registration { ts, shard }
    }

    pub fn release(&self, registration: Registration) {
        let shard = &self.shards[registration.shard];
        let mut snapshots = shard.snapshots.lock().expect("snapshot registry poisoned");
        Self::forget(&mut snapshots, registration.ts);
        shard.oldest.store(Self::oldest(&snapshots), Ordering::SeqCst);
        drop(snapshots);

        let now = self.epoch.elapsed().as_micros() as u64;
        let settled = self.settled.load(Ordering::Relaxed);
        let due = now.saturating_sub(settled) >= SETTLE_MICROS;
        if due
            && self
                .settled
                .compare_exchange(settled, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            self.settle_watermark();
        }
    }

    /// swaps `registration` for a fresh one in its shard (READ COMMITTED statements)
    pub fn refresh(&self, registration: Registration) -> Registration {
        let mut snapshots = self.shards[registration.shard]
            .snapshots
            .lock()
            .expect("snapshot registry poisoned");
        Self::forget(&mut snapshots, registration.ts);
        let ts = self.register(registration.shard, &mut snapshots);

        Registration { ts, ..registration }
    }

    /// recomputes the watermark from every shard
    /// `visible` is read before the shards: a snapshot registered after the read is at least it, one
    /// registered before is published in its shard by then (see [Clock::register])
    pub fn settle_watermark(&self) -> u64 {
        let visible = self.visible();
        let oldest = self
            .shards
            .iter()
            .map(|shard| shard.oldest.load(Ordering::SeqCst))
            .fold(visible, u64::min);

        self.watermark.fetch_max(oldest, Ordering::AcqRel).max(oldest)
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
            self.visible.store(last, Ordering::SeqCst);
        }
    }

    /// registers a snapshot of everything published, with the log position right after it
    pub fn pin(&self) -> (Registration, u64) {
        let publication = self.publication();
        (self.snapshot(), publication.lsn)
    }

    /// adds a snapshot of `visible` to `shard`'s `snapshots` and publishes the
    /// shard's oldest before re-reading `visible`: if a commit became visible in between, the
    /// snapshot is retaken, so a [Clock::settle_watermark] that read the newer `visible` before
    /// this registration was published cannot rise above it
    fn register(&self, shard: usize, snapshots: &mut BTreeMap<u64, u32>) -> u64 {
        let oldest = &self.shards[shard].oldest;
        loop {
            let ts = self.visible();
            *snapshots.entry(ts).or_default() += 1;
            oldest.store(Self::oldest(snapshots), Ordering::SeqCst);

            if self.visible() == ts {
                return ts;
            }
            Self::forget(snapshots, ts);
        }
    }

    fn publication(&self) -> MutexGuard<'_, Publication> {
        self.publication.lock().expect("publication lock poisoned")
    }

    fn forget(snapshots: &mut BTreeMap<u64, u32>, snapshot: u64) {
        let count = snapshots.get_mut(&snapshot).expect("released a snapshot that was never taken");

        *count -= 1;
        if *count == 0 {
            snapshots.remove(&snapshot);
        }
    }

    #[inline]
    fn oldest(snapshots: &BTreeMap<u64, u32>) -> u64 {
        snapshots.first_key_value().map_or(u64::MAX, |(&snapshot, _)| snapshot)
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
        let (registration, lsn) = clock.pin();
        assert_eq!((registration.ts, lsn), (0, 0));
        clock.release(registration);

        clock.publish(1, 10);
        assert_eq!(clock.visible(), 3);
        let (registration, lsn) = clock.pin();
        assert_eq!((registration.ts, lsn), (3, 30));
    }

    #[test]
    fn watermark_follows_the_oldest_snapshot() {
        let clock = Clock::recovered(0, 0);
        let old = clock.snapshot();

        clock.publish(1, 0);
        let young = clock.snapshot();
        assert_eq!((old.ts, young.ts), (0, 1));
        assert_eq!(clock.settle_watermark(), 0);

        clock.release(old);
        assert_eq!(clock.settle_watermark(), 1);

        clock.publish(2, 0);
        let refreshed = clock.refresh(young);
        assert_eq!(refreshed.ts, 2);
        assert_eq!(clock.settle_watermark(), 2);
        clock.release(refreshed);
        assert_eq!(clock.settle_watermark(), 2);
    }

    #[test]
    fn markers_are_flagged_and_unique() {
        let clock = Clock::recovered(0, 0);
        let (a, b) = (clock.marker(), clock.marker());

        assert_ne!(a, b);
        assert!(a & TXN_FLAG != 0 && b > u64::MAX / 2);
    }

    #[test]
    fn the_watermark_never_passes_a_snapshot_taken_concurrently() {
        let clock = Clock::recovered(0, 0);
        let stop = std::sync::atomic::AtomicBool::new(false);

        std::thread::scope(|scope| {
            scope.spawn(|| {
                for ts in 1..200_000 {
                    clock.publish(ts, 0);
                }
                stop.store(true, Ordering::Relaxed);
            });
            scope.spawn(|| {
                while !stop.load(Ordering::Relaxed) {
                    clock.settle_watermark();
                }
            });
            for _ in 0..4 {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let registration = clock.snapshot();
                        assert!(
                            clock.watermark() <= registration.ts,
                            "{} above {}",
                            clock.watermark(),
                            registration.ts
                        );
                        clock.release(registration);
                    }
                });
            }
        });
    }

    /// `cargo test --release --lib registry_throughput -- --ignored --nocapture`
    #[test]
    #[ignore]
    fn registry_throughput() {
        let clock = Clock::recovered(0, 0);
        for threads in [1, 2, 4, 8, 16] {
            let start = Instant::now();
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    scope.spawn(|| (0..1_000_000).for_each(|_| clock.release(clock.snapshot())));
                }
            });
            let rate = threads as f64 * 1e6 / start.elapsed().as_secs_f64();
            eprintln!("{threads} threads: {:.1} M snapshots/s", rate / 1e6);
        }
    }
}
