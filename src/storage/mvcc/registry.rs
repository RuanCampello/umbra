use crate::collections::chash;
use crate::collections::hash::HashMap;
use crate::storage::mvcc::version::VisibilityChecker;
use std::{
    cell::RefCell,
    sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

#[derive(Default)]
pub struct TransactionRegistry {
    next_txn_id: AtomicI64,
    transactions: HashMap<i64, TransactionState>,
    /// 0: Read Committed
    /// 1: Snapshot
    isolation_level: AtomicU8,
    transaction_isolation_levels: HashMap<i64, u8>,
    isolation_override_count: AtomicUsize,
    /// whether we should accept new transactions.
    accepting: AtomicBool,
    next_sequence: AtomicI64,
}

#[derive(Clone, Copy)]
pub struct TransactionState {
    pub status: TransactionStatus,
    pub begin: i64,
    pub commit: i64,
}

struct CommittedCache {
    entries: [i64; COMMITTED_CACHE_SIZE],
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionStatus {
    Active,
    Committing,
    Committed,
}

#[derive(Default, PartialEq, Eq)]
#[repr(u8)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    Snapshot,
}

thread_local! {
    static COMMITTED_CACHE: RefCell<CommittedCache> = const {RefCell::new(CommittedCache::new())}
}

const COMMITTED_CACHE_SIZE: usize = 1 << 16;

impl TransactionRegistry {
    pub const INVALID_ID: i64 = i64::MIN;
    pub const RECOVERY_ID: i64 = -1;

    pub fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            ..Default::default()
        }
    }

    pub fn begin(&mut self) -> (i64, i64) {
        if !self.accepting.load(Ordering::Acquire) {
            return (Self::INVALID_ID, 0);
        }

        let txn_id = self.next_txn_id.fetch_add(1, Ordering::AcqRel) + 1;
        let begin_id = self.next_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        self.transactions
            .insert(txn_id, TransactionState::new(begin_id));

        (txn_id, begin_id)
    }

    pub fn start_commit(&mut self, txn_id: i64) -> i64 {
        let commit = self.next_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        if let Some(mut entry) = self.transactions.get_mut(&txn_id) {
            entry.status = TransactionStatus::Committing;
            entry.commit = commit;
        }

        commit
    }

    pub fn complete_commit(&mut self, txn_id: i64) {
        if let Some(mut entry) = self.transactions.get_mut(&txn_id) {
            entry.status = TransactionStatus::Committed;
        }
    }

    pub fn commit_transaction(&mut self, txn_id: i64) -> i64 {
        let commit = self.next_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        if let Some(mut entry) = self.transactions.get_mut(&txn_id) {
            entry.status = TransactionStatus::Committed;
            entry.commit = commit;
        }

        commit
    }

    pub fn recover_committed_transaction(&mut self, txn_id: i64, commit: i64) {
        self.transactions.insert(
            txn_id,
            TransactionState {
                commit,
                begin: commit,
                status: TransactionStatus::Committed,
            },
        );

        self.recover_aborted_transaction(txn_id);

        loop {
            let current = self.next_sequence.load(Ordering::Acquire);

            match commit >= current {
                false => break,
                true if self
                    .next_sequence
                    .compare_exchange_weak(current, commit + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok() =>
                {
                    break
                }
                _ => {}
            };
        }
    }

    pub fn recover_aborted_transaction(&mut self, txn_id: i64) {
        loop {
            let current = self.next_txn_id.load(Ordering::Acquire);
            match txn_id >= current {
                false => break,
                true if self
                    .next_txn_id
                    .compare_exchange_weak(current, txn_id + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok() =>
                {
                    break
                }
                _ => {}
            };
        }
    }

    /// Cleans the old transactions already committed.
    ///
    /// Returns the number of transactions cleaned.
    pub fn cleanup_transactions(&self, max_age: Duration) -> i32 {
        let level = self.isolation_level();

        // we cannot clean READ COMMITTED ones
        if matches!(level, IsolationLevel::ReadCommitted) {
            return 0;
        }

        let cut = Instant::now()
            .checked_add(max_age)
            .map(|_| self.next_sequence.load(Ordering::Acquire) - (max_age.as_nanos() as i64))
            .unwrap_or_default();

        let to_be_removed = self
            .transactions
            .iter()
            .filter(|entry| {
                let id = *entry.0;
                let state = entry.1;

                if id < 0 {
                    return false;
                }

                if state.status != TransactionStatus::Committed {
                    return false;
                }

                state.commit < cut
            })
            .map(|entry| *entry.0)
            .collect::<Vec<_>>();

        let mut removed = 0;
        for id in to_be_removed {
            // self.transactions.remove(&id);
            removed += 1;
        }

        removed
    }

    #[inline]
    pub fn is_directly_visible(&self, version_txn_id: i64) -> bool {
        if version_txn_id == Self::RECOVERY_ID {
            return true;
        }

        let cached = COMMITTED_CACHE.with(|cache| cache.borrow().contains(version_txn_id));
        if cached {
            return true;
        }

        if let Some(entry) = self.transactions.get(&version_txn_id) {
            if matches!(entry.status, TransactionStatus::Committed) {
                COMMITTED_CACHE.with(|cache| cache.borrow_mut().insert(version_txn_id));
                return true;
            }
        }

        false
    }

    #[inline]
    pub fn is_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool {
        if version_txn_id == view_txn_id {
            return true;
        }

        if version_txn_id == Self::RECOVERY_ID {
            return true;
        }

        if self.isolation_level.load(Ordering::Relaxed) == 0 {
            if self.isolation_override_count.load(Ordering::Relaxed) == 0 {
                return self.is_directly_visible(version_txn_id);
            }

            if !self.transaction_isolation_levels.contains_key(&view_txn_id) {
                return self.is_directly_visible(version_txn_id);
            }

            if let Some(level) = self.transaction_isolation_levels.get(&view_txn_id) {
                if *level == 0 {
                    return self.is_directly_visible(version_txn_id);
                }
            }
        }

        self.is_snapshot_visible(version_txn_id, view_txn_id)
    }

    /// Start accepting new transactions
    pub fn accept_transactions(&self) {
        self.accepting.store(true, Ordering::Release)
    }

    #[inline(never)]
    fn is_snapshot_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool {
        if self.transaction_isolation_level(view_txn_id) == IsolationLevel::ReadCommitted {
            return self.is_directly_visible(version_txn_id);
        }

        let version_state = match self.transactions.get(&version_txn_id) {
            Some(entry) => *entry,
            None => return false,
        };

        if version_state.status != TransactionStatus::Committed {
            return false;
        }

        let view_begin = match self.transactions.get(&view_txn_id) {
            Some(entry) => entry.begin,
            None => return false,
        };

        version_state.commit <= view_begin
    }

    fn get_sequence(&self) -> i64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    fn is_committed_before(&self, txn_id: i64, cut_off: i64) -> bool {
        if txn_id < 0 {
            return true;
        }

        if let Some(entry) = self.transactions.get(&txn_id) {
            if entry.status == TransactionStatus::Committed {
                return entry.commit <= cut_off;
            }

            return false;
        }

        true
    }

    pub fn set_transaction_isolation_level(&mut self, txn_id: i64, level: IsolationLevel) {
        let is_new = !self.transaction_isolation_levels.contains_key(&txn_id);
        self.transaction_isolation_levels
            .insert(txn_id, level as u8);

        if is_new {
            self.isolation_override_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn abort_transaction(&mut self, txn_id: i64) {
        self.transactions.remove(&txn_id);
    }

    /// Returns the global [isolation level](self::IsolationLevel).
    pub fn isolation_level(&self) -> IsolationLevel {
        IsolationLevel::from(self.isolation_level.load(Ordering::Acquire))
    }

    pub fn transaction_isolation_level(&self, txn_id: i64) -> IsolationLevel {
        if let Some(level) = self.transaction_isolation_levels.get(&txn_id) {
            return IsolationLevel::from(*level);
        }

        self.isolation_level()
    }

    pub fn remove_transaction_isolation_level(&mut self, txn_id: i64) {
        if self.transaction_isolation_levels.remove(&txn_id).is_some() {
            self.isolation_level.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl VisibilityChecker for TransactionRegistry {
    fn is_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool {
        TransactionRegistry::is_visible(self, version_txn_id, view_txn_id)
    }

    fn get_sequence(&self) -> i64 {
        TransactionRegistry::get_sequence(&self)
    }

    fn get_active_transactions(&self) -> Vec<i64> {
        self.transactions
            .iter()
            .filter(|entry| entry.1.status == TransactionStatus::Active)
            .map(|entry| *entry.0)
            .collect()
    }

    fn is_committed_before(&self, txn_id: i64, cut_off: i64) -> bool {
        TransactionRegistry::is_committed_before(&self, txn_id, cut_off)
    }
}

impl CommittedCache {
    const fn new() -> Self {
        Self {
            entries: [0i64; COMMITTED_CACHE_SIZE],
        }
    }

    fn insert(&mut self, txn_id: i64) {
        let idx = (txn_id as usize) & (COMMITTED_CACHE_SIZE - 1);
        self.entries[idx] = txn_id
    }

    fn contains(&self, txn_id: i64) -> bool {
        let idx = (txn_id as usize) & (COMMITTED_CACHE_SIZE - 1);
        self.entries[idx] == txn_id
    }
}

impl TransactionState {
    const fn new(begin: i64) -> Self {
        Self {
            begin,
            status: TransactionStatus::Active,
            commit: 0,
        }
    }
}

impl From<u8> for IsolationLevel {
    #[inline]
    fn from(value: u8) -> Self {
        match value {
            0 => IsolationLevel::ReadCommitted,
            1 => IsolationLevel::Snapshot,
            _ => unsafe { std::hint::unreachable_unchecked() },
        }
    }
}
