use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering};

use crate::core::HashMap;

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

#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionStatus {
    Active,
    Committing,
    Committed,
}

#[derive(Default)]
#[repr(u8)]
pub enum IsolationLevel {
    #[default]
    ReadCommitted,
    Snapshot,
}

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

        loop {
            let current = self.next_txn_id.load(Ordering::Acquire);

            match txn_id > current {
                false => break,
                true if self
                    .next_txn_id
                    .compare_exchange(current, txn_id + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok() =>
                {
                    break
                }
                _ => {}
            };
        }

        loop {
            let current = self.next_sequence.load(Ordering::Acquire);

            match txn_id > current {
                false => break,
                true if self
                    .next_sequence
                    .compare_exchange(current, txn_id + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok() =>
                {
                    break
                }
                _ => {}
            };
        }
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
