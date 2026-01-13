use crate::core::storage::{mvcc::fnv1a, wal::WalError};
use std::{fs, path::Path};

#[derive(Debug, Clone)]
pub(crate) struct CheckpointMetadata {
    pub wal_segment_sequence: u64,
    pub lsn: u64,
    /// Time in nanoseconds of this checkpoint creation.
    pub timestamp: i64,
    pub is_consistent: bool,
    pub active_transactions: Vec<i64>,
    /// Helps recovery skip analysis for these IDs if found in log
    pub commited_transactions: Vec<CommitedTransaction>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitedTransaction {
    pub txn_id: i64,
    pub commit_lsn: u64,
}

/// "herm"
const CHECKPOINT_MAGIC: u32 = 0x6865726D;
const CHECKPOINT_HEADER_SIZE: u16 = 1 << 5;

impl CheckpointMetadata {
    pub fn decode(path: &Path) -> Result<Self, WalError> {
        let content = fs::read(path)?;

        if content.len() < CHECKPOINT_HEADER_SIZE as usize + 4 {
            return Err(WalError::InvalidSize(content.len()));
        }

        let from = u32::from_le_bytes(content[content.len() - 4..].try_into().unwrap());
        let expected = fnv1a(&content[..content.len() - 4]);
        if from != expected {
            return Err(WalError::Checksum);
        }

        todo!()
    }
}
