use crate::collections::chash;
use crate::collections::hash::HashMap;
use crate::storage::mvcc::version::VisibilityChecker;
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

#[derive(Default)]
pub struct TransactionRegistry {
    next_txn_id: AtomicI64,
    transactions: Mutex<HashMap<i64, TransactionState>>,
    /// current number of active transactions for fast look-up
    active_transactions: AtomicUsize,
    snapshot_sequences: Mutex<HashMap<i64, i64>>,
    /// 0: Read Committed
    /// 1: Snapshot
    isolation_level: AtomicU8,
    /// per transaction isolation level
    transaction_isolation_levels: Mutex<HashMap<i64, u8>>,
    isolation_override_count: AtomicUsize,
    /// whether we should accept new transactions.
    accepting: AtomicBool,
    next_sequence: AtomicI64,
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionState {
    begin: i64,
    commit: i64,
}

struct CommittedCache {
    entries: [i64; COMMITTED_CACHE_SIZE],
}

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionStatus {
    Active,
    Committing,
    Aborted,
}

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
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

    /// Tries to start a new transaction and returns its id and begin sequence
    pub fn begin(&self) -> (i64, i64) {
        if !self.accepting.load(Ordering::Acquire) {
            return (Self::INVALID_ID, 0);
        }

        let txn_id = self.next_txn_id.fetch_add(1, Ordering::AcqRel) + 1;
        let begin_id = self.next_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        self.transactions
            .lock()
            .unwrap()
            .insert(txn_id, TransactionState::new(begin_id));
        self.active_transactions.fetch_add(1, Ordering::Relaxed);

        (txn_id, begin_id)
    }

    pub fn start_commit(&self, txn_id: i64) -> i64 {
        let commit = self.next_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        if let Some(mut entry) = self.transactions.lock().unwrap().get_mut(&txn_id) {
            entry.set_committing(commit);
        }

        commit
    }

    #[inline]
    pub fn complete_commit(&self, txn_id: i64) {
        {
            let mut transactions = self.transactions.lock().unwrap();
            let sequence = transactions
                .get(&txn_id)
                .map(|state| state.commit())
                .unwrap_or(0);

            if self.isolation_level.load(Ordering::Relaxed) == 1
                || self.isolation_override_count.load(Ordering::Relaxed) > 1
            {
                self.snapshot_sequences
                    .lock()
                    .unwrap()
                    .insert(txn_id, sequence);
            }

            transactions.remove(&txn_id);
        }

        self.active_transactions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn commit_transaction(&self, txn_id: i64) -> i64 {
        let commit = self.next_sequence.fetch_add(1, Ordering::AcqRel) + 1;

        {
            let mut transactions = self.transactions.lock().unwrap();

            if self.isolation_level.load(Ordering::Relaxed) == 1
                || self.isolation_override_count.load(Ordering::Relaxed) > 0
            {
                self.snapshot_sequences
                    .lock()
                    .unwrap()
                    .insert(txn_id, commit);
            }

            transactions.remove(&txn_id);
        }

        self.active_transactions.fetch_sub(1, Ordering::Relaxed);

        commit
    }

    pub fn recover_committed_transaction(&self, txn_id: i64, commit: i64) {
        self.snapshot_sequences
            .lock()
            .unwrap()
            .insert(txn_id, commit);

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

    pub fn recover_aborted_transaction(&self, txn_id: i64) {
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
        self.garbage_collection() as i32
    }

    pub fn is_active(&self, txn_id: i64) -> bool {
        self.transactions
            .lock()
            .unwrap()
            .get(&txn_id)
            .map(|state| state.status() == TransactionStatus::Active)
            .unwrap_or(false)
    }

    pub fn is_committed(&self, txn_id: i64) -> bool {
        if self.transactions.lock().unwrap().contains_key(&txn_id) {
            return false;
        }

        let next = self.next_txn_id.load(Ordering::Acquire);
        txn_id > 0 && txn_id <= next
    }

    #[inline(always)]
    pub fn is_directly_visible(&self, version_txn_id: i64) -> bool {
        if version_txn_id == Self::RECOVERY_ID {
            return true;
        }

        self.check_committed(version_txn_id)
    }

    #[inline(always)]
    pub fn is_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool {
        if version_txn_id == view_txn_id {
            return true;
        }

        if version_txn_id == Self::RECOVERY_ID {
            return true;
        }

        match self.needs_snapshot_isolation(view_txn_id) {
            true => self.is_snapshot_visible(version_txn_id, view_txn_id),
            _ => self.check_committed(version_txn_id),
        }
    }

    /// Start accepting new transactions
    pub fn accept_transactions(&self) {
        self.accepting.store(true, Ordering::Release)
    }

    pub fn is_accepting(&self) -> bool {
        self.accepting.load(Ordering::Acquire)
    }

    /// Stops accepting new transactions
    pub fn deny_transactions(&self) {
        self.accepting.store(false, Ordering::Release)
    }

    /// Runs the garbage collection.
    fn garbage_collection(&self) -> usize {
        let mut retired = 0;

        let keys = match self.isolation_override_count.load(Ordering::Relaxed) > 0 {
            true => self
                .transaction_isolation_levels
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };

        let (min, invalid) = {
            let mut transactions = self.transactions.lock().unwrap();
            let mut begin = i64::MAX;
            let mut min_id = i64::MAX;

            for (&id, state) in transactions.iter() {
                if state.is_active_or_commiting() {
                    begin = begin.min(state.begin());
                    min_id = min_id.min(id);
                }
            }

            if min_id == i64::MAX {
                min_id = self.next_txn_id.load(Ordering::Acquire);
            }

            let cutoff = min_id.saturating_sub(10000);
            if cutoff > 0 {
                let len = transactions.len();
                transactions.retain(|id, state| !state.is_aborted() || id >= &cutoff);
                retired += len - transactions.len();
            }

            let invalid = keys
                .iter()
                .filter(|&txn_id| match transactions.get(txn_id) {
                    Some(state) => state.is_aborted(),
                    _ => true,
                })
                .collect::<Vec<_>>();

            (begin, invalid)
        };

        {
            let mut sequences = self.snapshot_sequences.lock().unwrap();
            let len = sequences.len();
            sequences.retain(|_, &mut commit| commit >= min);
            retired += len - sequences.len();
        }

        if !invalid.is_empty() {
            let mut overried = self.transaction_isolation_levels.lock().unwrap();
            let mut removals = 0;

            for transaction in &invalid {
                if overried.remove(*transaction).is_some() {
                    retired += 1;
                    removals += 1;
                }
            }

            if removals > 0 {
                self.isolation_override_count
                    .fetch_sub(removals, Ordering::Relaxed);
            }
        }

        retired
    }

    #[inline(always)]
    fn needs_snapshot_isolation(&self, txn_id: i64) -> bool {
        let isolation_level = self.isolation_level.load(Ordering::Relaxed);
        if isolation_level == 1 {
            return true;
        }

        if self.isolation_override_count.load(Ordering::Relaxed) > 0 {
            if let Some(&level) = self
                .transaction_isolation_levels
                .lock()
                .unwrap()
                .get(&txn_id)
            {
                return level == 1;
            }
        }

        false
    }

    #[cold]
    #[inline(never)]
    fn is_snapshot_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool {
        let (version_state, view_sequence) = {
            let transactions = self.transactions.lock().unwrap();

            let version_sequence = match transactions.get(&view_txn_id) {
                Some(entry) if entry.is_active_or_commiting() => entry.begin(),
                _ => {
                    drop(transactions);
                    return self.check_committed(version_txn_id);
                }
            };

            let version_state = transactions.get(&version_txn_id).copied();
            (version_state, version_sequence)
        };

        match version_state {
            Some(state) => {
                if state.is_aborted() {
                    return false;
                }

                false
            }
            None => {
                let next = self.next_txn_id.load(Ordering::Acquire);

                if version_txn_id <= 0 || version_txn_id > next {
                    return false;
                }

                let commit_sequence = self
                    .snapshot_sequences
                    .lock()
                    .unwrap()
                    .get(&version_txn_id)
                    .copied();

                if let Some(sequence) = commit_sequence {
                    return sequence <= view_sequence;
                }

                true
            }
        }
    }

    #[inline(always)]
    /// Checks if a transaction is committed.
    fn check_committed(&self, txn_id: i64) -> bool {
        if COMMITTED_CACHE.with(|cache| cache.borrow().contains(txn_id)) {
            return true;
        }

        if self.transactions.lock().unwrap().contains_key(&txn_id) {
            return false;
        }

        let next = self.next_txn_id.load(Ordering::Acquire);
        if txn_id > 0 && txn_id <= next {
            COMMITTED_CACHE.with(|cache| cache.borrow_mut().insert(txn_id));
            return true;
        }

        false
    }

    fn sequence(&self) -> i64 {
        self.next_sequence.load(Ordering::Acquire)
    }

    fn commit_sequence(&self, txn_id: i64) -> Option<i64> {
        if let Some(&sequence) = self.snapshot_sequences.lock().unwrap().get(&txn_id) {
            return Some(sequence);
        }

        if let Some(state) = self.transactions.lock().unwrap().get(&txn_id) {
            if state.is_aborted() || state.is_active_or_commiting() {
                return None;
            }
        }

        let next = self.next_txn_id.load(Ordering::Acquire);
        if txn_id > 0 && txn_id <= next {
            return Some(0);
        }

        None
    }

    fn is_committed_before(&self, txn_id: i64, cut_off: i64) -> bool {
        if txn_id < 0 {
            return true;
        }

        if self.transactions.lock().unwrap().contains_key(&txn_id) {
            return false;
        }

        if let Some(&commit) = self.snapshot_sequences.lock().unwrap().get(&txn_id) {
            return commit <= cut_off;
        }

        let next = self.next_txn_id.load(Ordering::Acquire);
        txn_id > 0 && txn_id <= next
    }

    pub fn set_transaction_isolation_level(&self, txn_id: i64, level: IsolationLevel) {
        let mut transactions = self.transaction_isolation_levels.lock().unwrap();
        let is_new = !transactions.contains_key(&txn_id);

        println!("level: {}", level as u8);
        transactions.insert(txn_id, level as u8);

        if is_new {
            self.isolation_override_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn abort_transaction(&self, txn_id: i64) {
        let needs_decrement = {
            let mut transactions = self.transactions.lock().unwrap();
            match transactions.get_mut(&txn_id) {
                Some(state) => {
                    let was_active = state.is_active_or_commiting();
                    *state = TransactionState::new_aborted();
                    was_active
                }
                _ => false,
            }
        };

        if needs_decrement {
            self.active_transactions.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Returns the global [isolation level](self::IsolationLevel).
    pub fn isolation_level(&self) -> IsolationLevel {
        println!("{}", self.isolation_level.load(Ordering::Relaxed));
        IsolationLevel::from(self.isolation_level.load(Ordering::Acquire))
    }

    /// Sets the global isolation level.
    pub fn set_isolation_level(&self, level: IsolationLevel) {
        self.isolation_level.store(level as u8, Ordering::Release)
    }

    #[inline(always)]
    pub fn transaction_isolation_level(&self, txn_id: i64) -> IsolationLevel {
        if self.isolation_override_count.load(Ordering::Relaxed) > 0 {
            if let Some(&level) = self
                .transaction_isolation_levels
                .lock()
                .unwrap()
                .get(&txn_id)
            {
                return IsolationLevel::from(level);
            }
        }

        self.isolation_level()
    }

    pub fn remove_transaction_isolation_level(&self, txn_id: i64) {
        if self
            .transaction_isolation_levels
            .lock()
            .unwrap()
            .remove(&txn_id)
            .is_some()
        {
            self.isolation_override_count
                .fetch_sub(1, Ordering::Relaxed);
        }
    }
}

impl VisibilityChecker for TransactionRegistry {
    fn is_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool {
        TransactionRegistry::is_visible(self, version_txn_id, view_txn_id)
    }

    fn get_sequence(&self) -> i64 {
        TransactionRegistry::sequence(&self)
    }

    fn get_active_transactions(&self) -> Vec<i64> {
        self.transactions
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, state)| state.status() == TransactionStatus::Active)
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
    const ABORTED: i64 = -1;
    const SEQUENCE_MASK: i64 = (1 << Self::SHIFT) - 1;
    const SHIFT: u32 = 62;

    #[inline(always)]
    const fn new(begin: i64) -> Self {
        Self { begin, commit: 0 }
    }

    #[inline(always)]
    const fn new_aborted() -> Self {
        Self {
            begin: Self::ABORTED,
            commit: (TransactionStatus::Aborted as i64) << Self::SHIFT,
        }
    }

    #[inline(always)]
    const fn status(&self) -> TransactionStatus {
        if self.is_aborted() {
            return TransactionStatus::Aborted;
        }

        match (self.commit >> Self::SHIFT) as u8 {
            0 => TransactionStatus::Active,
            1 => TransactionStatus::Committing,
            _ => TransactionStatus::Aborted,
        }
    }

    #[inline(always)]
    const fn is_active_or_commiting(&self) -> bool {
        !self.is_aborted()
    }

    #[inline(always)]
    const fn is_aborted(&self) -> bool {
        self.begin == Self::ABORTED
    }

    #[inline(always)]
    const fn begin(&self) -> i64 {
        match self.is_aborted() {
            true => 0,
            _ => self.begin,
        }
    }

    #[inline(always)]
    const fn set_committing(&mut self, commit: i64) {
        self.commit = commit | (1i64 << Self::SHIFT);
    }

    #[inline(always)]
    const fn commit(&self) -> i64 {
        self.commit & Self::SEQUENCE_MASK
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

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReadCommitted => f.write_str("READ COMMITTED"),
            Self::Snapshot => f.write_str("SNAPSHOT ISOLATION"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn begin_transaction() {
        let registry = TransactionRegistry::new();

        let (transaction, sequence) = registry.begin();
        assert!(transaction > 0);
        assert!(sequence > 0);

        let (transaction_2, sequence_2) = registry.begin();
        assert!(transaction_2 > transaction);
        assert!(sequence_2 > sequence);
    }

    #[test]
    fn commit_transaction() {
        let registry = TransactionRegistry::new();

        let (transaction, _) = registry.begin();
        assert!(registry.is_active(transaction));
        assert!(!registry.is_committed(transaction));

        registry.commit_transaction(transaction);
        assert!(!registry.is_active(transaction));
        assert!(registry.is_committed(transaction));
    }

    #[test]
    fn abort_transaction() {
        let registry = TransactionRegistry::new();

        let (transaction, _) = registry.begin();
        assert!(registry.is_active(transaction));

        registry.abort_transaction(transaction);
        assert!(!registry.is_active(transaction));
        assert!(!registry.is_committed(transaction));

        let state = registry
            .transactions
            .lock()
            .unwrap()
            .get(&transaction)
            .copied();
        assert!(state.map(|state| state.is_aborted()).unwrap_or(false));
    }

    #[test]
    fn writes_visibility() {
        let registry = TransactionRegistry::new();
        let (transaction, _) = registry.begin();

        assert!(registry.is_visible(transaction, transaction));
    }

    #[test]
    fn recover_visibility() {
        let registry = TransactionRegistry::new();
        let (transaction, _) = registry.begin();

        assert!(registry.is_visible(TransactionRegistry::RECOVERY_ID, transaction));
    }

    #[test]
    fn read_committed_visibility() {
        let registry = TransactionRegistry::new();
        registry.set_isolation_level(IsolationLevel::ReadCommitted);

        let (txn_1, _) = registry.begin();
        let (txn_2, _) = registry.begin();

        assert!(!registry.is_visible(txn_1, txn_2));

        registry.commit_transaction(txn_1);
        assert!(registry.is_visible(txn_1, txn_2));
    }

    #[test]
    fn snapshot_isolation() {
        let registry = TransactionRegistry::new();
        registry.set_isolation_level(IsolationLevel::Snapshot);

        let (txn_1, _) = registry.begin();
        registry.commit_transaction(txn_1);
        let (txn_2, _) = registry.begin();

        assert!(registry.is_visible(txn_1, txn_2));

        let (txn_3, _) = registry.begin();
        registry.commit_transaction(txn_3);
        assert!(!registry.is_visible(txn_3, txn_2));
    }

    #[test]
    fn deny() {
        let registry = TransactionRegistry::new();
        assert!(registry.is_accepting());

        registry.deny_transactions();
        assert!(!registry.is_accepting());

        let (transaction, _) = registry.begin();
        assert_eq!(transaction, TransactionRegistry::INVALID_ID);
    }

    #[test]
    fn isolation_override() {
        let registry = TransactionRegistry::new();
        registry.set_isolation_level(IsolationLevel::ReadCommitted);

        let (transaction, _) = registry.begin();
        assert_eq!(
            registry.transaction_isolation_level(transaction),
            IsolationLevel::ReadCommitted
        );

        registry.set_transaction_isolation_level(transaction, IsolationLevel::Snapshot);
        assert_eq!(
            registry.transaction_isolation_level(transaction),
            IsolationLevel::Snapshot
        );

        registry.remove_transaction_isolation_level(transaction);
        assert_eq!(
            registry.transaction_isolation_level(transaction),
            IsolationLevel::ReadCommitted
        ); // falls back to the global
    }

    #[test]
    fn commit_sequence() {
        let registry = TransactionRegistry::new();
        registry.set_isolation_level(IsolationLevel::Snapshot);

        let (txn_id, _) = registry.begin();
        assert!(registry.commit_sequence(txn_id).is_none());

        let sequence = registry.commit_transaction(txn_id);
        assert_eq!(registry.commit_sequence(txn_id), Some(sequence));
    }

    #[test]
    fn recover_committed_transaction() {
        let registry = TransactionRegistry::new();
        registry.recover_committed_transaction(1000, 500);

        assert!(registry.is_committed(1000));
        assert_eq!(registry.commit_sequence(1000), Some(500));

        let (transaction, _) = registry.begin();
        assert!(transaction > 1000);
    }

    #[test]
    fn garbage_collection() {
        let registry = TransactionRegistry::new();
        registry.set_isolation_level(IsolationLevel::Snapshot);

        for _ in 0..10 {
            let (transaction, _) = registry.begin();
            registry.commit_transaction(transaction);
        }

        assert_eq!(registry.snapshot_sequences.lock().unwrap().len(), 10);
        let (active, _) = registry.begin();

        for _ in 0..5 {
            let (transaction, _) = registry.begin();
            registry.commit_transaction(transaction);
        }

        let retired = registry.garbage_collection();
        assert!(retired > 0);

        registry.commit_transaction(active);
    }
}
