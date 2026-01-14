use crate::core::storage::{mvcc::fnv1a, wal::WalError};
use std::{fs, path::Path};

#[derive(Debug, Clone)]
pub(crate) struct CheckpointMetadata {
    pub wal_segment_sequence: u64,
    pub lsn: u64,
    /// Time in nanoseconds of this checkpoint creation
    pub timestamp: i64,
    pub is_consistent: bool,
    /// Transaction IDs active at this point
    pub active_transactions: Vec<i64>,
    /// Helps recovery skip analysis for these IDs if found in log
    pub committed_transactions: Vec<CommitedTransaction>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitedTransaction {
    pub txn_id: i64,
    pub commit_lsn: u64,
}

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum CheckpointSectionKind {
    Info = 0x0001,
    Active,
    Commited,
}

/// "herm"
const CHECKPOINT_MAGIC: u32 = 0x6865726D;
const CHECKPOINT_BINARY_VERSION: u8 = 1;
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

        let mut cursor = 0;

        let magic = u32::from_le_bytes(content[cursor..cursor + 4].try_into().unwrap());
        if magic != CHECKPOINT_MAGIC {
            return Err(WalError::WrongMagic {
                from: "checkpoint".into(),
            });
        }
        cursor += 4;

        let version = content[cursor];
        cursor += 1;
        let flags = content[cursor];
        cursor += 1;

        let header_size =
            u16::from_le_bytes(content[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;

        let lsn = u64::from_le_bytes(content[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let timestamp = i64::from_le_bytes(content[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let sec_count = u16::from_le_bytes(content[cursor..cursor + 2].try_into().unwrap());
        cursor += 2;
        cursor += 6; // reserved size

        if header_size > 32 {
            cursor = header_size;
        }

        struct Section {
            kind: u16,
            flags: u16,
            size: u32,
        }

        let mut sections = Vec::with_capacity(sec_count as usize);
        for _ in 0..sec_count {
            if cursor + 8 > content.len() - 4 {
                break;
            }

            let kind = u16::from_le_bytes(content[cursor..cursor + 2].try_into().unwrap());
            let flags = u16::from_le_bytes(content[cursor + 2..cursor + 4].try_into().unwrap());
            let size = u32::from_le_bytes(content[cursor + 4..cursor + 8].try_into().unwrap());

            sections.push(Section { flags, kind, size });
            cursor += 8;
        }

        let mut is_consistent = false;
        let mut active_transactions = Vec::new();
        let mut committed_transactions = Vec::new();
        let mut wal_segment_sequence = 0;

        for section in sections {
            let end = cursor + section.size as usize;
            if end > content.len() - 4 {
                break;
            }

            match CheckpointSectionKind::try_from(section.kind) {
                Ok(CheckpointSectionKind::Info) => {
                    is_consistent = content[cursor] != 0;
                    let mut section_c = cursor + 1;

                    // current wal name
                    if section_c + 2 <= end {
                        let len = u16::from_le_bytes(
                            content[section_c..section_c + 2].try_into().unwrap(),
                        ) as usize;
                        section_c += 2;

                        if section_c + len <= end {
                            let name = &content[section_c..section_c + len];
                            let name = String::from_utf8_lossy(name);

                            let wal_segment_sequence = name
                                .strip_prefix("wal-")
                                .and_then(|n| n.strip_suffix(".log"))
                                .and_then(|n| n.parse::<u64>().ok())
                                .expect("Format for wal segment sequence must be valid");

                            section_c += len;
                        }
                    }
                }

                Ok(CheckpointSectionKind::Active) => {
                    if cursor + 8 <= end {
                        let count =
                            u64::from_le_bytes(content[cursor..cursor + 8].try_into().unwrap())
                                as usize;
                        let mut section_c = cursor + 8;
                        active_transactions.reserve_exact(section_c);

                        for _ in 0..count {
                            if section_c + 8 > end {
                                break;
                            }

                            let txn_id = i64::from_le_bytes(
                                content[section_c..section_c + 8].try_into().unwrap(),
                            );
                            active_transactions.push(txn_id);
                            section_c += 8;
                        }
                    }
                }

                Ok(CheckpointSectionKind::Commited) => {
                    if cursor + 8 <= end {
                        let count =
                            u64::from_le_bytes(content[cursor..cursor + 8].try_into().unwrap());
                        let mut section_c = cursor + 8;
                        committed_transactions.reserve_exact(section_c);

                        for _ in 0..count {
                            if section_c + 16 > end {
                                break;
                            }

                            let txn_id =
                                i64::from_le_bytes(content[cursor..cursor + 8].try_into().unwrap());
                            let commit_lsn = u64::from_le_bytes(
                                content[cursor + 8..cursor + 16].try_into().unwrap(),
                            );

                            committed_transactions.push(CommitedTransaction { txn_id, commit_lsn })
                        }
                    }
                }

                _ => {
                    if version >= CHECKPOINT_BINARY_VERSION {
                        eprintln!(
                            "Unknown checkpoint section kind {} for {version} version",
                            section.kind
                        );
                    }
                }
            }

            cursor = end;
        }

        Ok(Self {
            wal_segment_sequence,
            lsn,
            timestamp,
            is_consistent,
            active_transactions,
            committed_transactions,
        })
    }
}

impl TryFrom<u16> for CheckpointSectionKind {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        Some(match value {
            0x0001 => Self::Info,
            0x0002 => Self::Active,
            0x0003 => Self::Commited,
            _ => return Err(()),
        })
        .ok_or(())
    }
}
