use crate::core::storage::wal::WalError;

/// Write-Ahead Logging entry format (V1, 32 bytes)
///
/// ┌───────────────────────────────────────────────────────────┐
/// │ HEADER BINARY (32 bytes)                                  │
/// ├────────────┬───────────┬──────────────────────────────────┤
/// │ Offset     │ Size      │ Field                            │
/// ├────────────┼───────────┼──────────────────────────────────┤
/// │ 0x00       │ 4 bytes   │ magic ("mnem" / 0x6D6E656D)      │
/// │ 0x04       │ 1 byte    │ version                          │
/// │ 0x05       │ 1 byte    │ flags (WalFlags)                 │
/// │ 0x06       │ 2 bytes   │ header Size (32)                 │
/// │ 0x08       │ 8 bytes   │ lSN (Log Sequence Number)        │
/// │ 0x10       │ 8 bytes   │ previous LSN                     │
/// │ 0x18       │ 4 bytes   │ content size (N)                 │
/// │ 0x1C       │ 4 bytes   │ reserved (0x00)                  │
/// ├────────────┴───────────┴──────────────────────────────────┤
/// │ CONTENT (variable n bytes)                                │
/// ├───────────────────────────────────────────────────────────┤
/// │ - txn id (8 bytes)                                        │
/// │ - table name length (2 bytes) + table name (UTF-8)        │
/// │ - row id (8 bytes)                                        │
/// │ - operation (1 byte)                                      │
/// │ - timestamp (8 bytes)                                     │
/// │ - data length (4 bytes) + data (blob)                     │
/// ├───────────────────────────────────────────────────────────┤
/// │ FOOTER                                                    │
/// ├───────────────────────────────────────────────────────────┤
/// │ - Checksum (4 bytes) -> FNV-1a of [header + content]      │
/// └───────────────────────────────────────────────────────────┘
///
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct WalEntry {
    table: String,
    /// Log Sequence Number
    pub(in crate::core::storage) lsn: u64,
    pub(super) previous_lsn: u64,
    flags: WalFlags,
    pub(in crate::core::storage) txn_id: i64,
    row_id: i64,
    pub(in crate::core::storage) operation: WalOperation,
    data: Vec<u8>,
    timestamp: i64,
}

#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub(crate) struct WalFlags(u8);

#[derive(Debug, PartialEq, Clone, Copy)]
#[repr(u8)]
pub(crate) enum WalOperation {
    Insert = 1,
    Update,
    Delete,
    Commit,
    Rollback,
    CreateTable,
    AlterTable,
    DropTable,
    CreateIndex,
    DropIndex = 10,
}

/// Marker for transactions entries after truncation.
const MARKER_ID: i64 = i64::MIN + 666;

impl WalEntry {
    #[inline]
    pub fn new(
        table: String,
        txn_id: i64,
        row_id: i64,
        operation: WalOperation,
        data: Vec<u8>,
    ) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos() as i64)
            .unwrap_or(1);

        Self {
            lsn: 0,
            previous_lsn: 0,
            flags: WalFlags::NONE,
            row_id,
            table,
            txn_id,
            operation,
            data,
            timestamp,
        }
    }

    #[inline]
    pub fn with_flags(
        table: String,
        txn_id: i64,
        row_id: i64,
        operation: WalOperation,
        data: Vec<u8>,
        flags: WalFlags,
    ) -> Self {
        let mut entry = Self::new(table, txn_id, row_id, operation, data);
        entry.flags = flags;

        entry
    }

    #[inline(always)]
    pub fn encode(&self) -> Vec<u8> {
        use super::{WAL_BINARY_VERSION, WAL_HEADER_SIZE, WAL_MAGIC};
        use crate::core::storage::mvcc::fnv1a;

        // txn_id + name length + name + row_id + operation + timestamp + data length + data
        let size = 8 + 2 + self.table.len() + 8 + 1 + 8 + 4 + self.data.len();
        let mut buff = Vec::with_capacity(WAL_HEADER_SIZE as usize + size + 4);

        // -- header
        buff.extend_from_slice(&WAL_MAGIC.to_le_bytes());
        buff.push(WAL_BINARY_VERSION);
        buff.push(self.flags.into());
        buff.extend_from_slice(&WAL_HEADER_SIZE.to_le_bytes());
        buff.extend_from_slice(&self.lsn.to_le_bytes());
        buff.extend_from_slice(&self.previous_lsn.to_le_bytes());
        buff.extend_from_slice(&(size as u32).to_le_bytes());
        buff.extend_from_slice(&[0u8; 4]); // reserved space

        // -- content
        buff.extend_from_slice(&self.txn_id.to_le_bytes());
        buff.extend_from_slice(&(self.table.len() as u16).to_le_bytes());
        buff.extend_from_slice(self.table.as_bytes());
        buff.extend_from_slice(&self.row_id.to_le_bytes());
        buff.push(self.operation as u8);
        buff.extend_from_slice(&self.timestamp.to_le_bytes());
        buff.extend_from_slice(&(self.data.len() as u32).to_le_bytes());
        buff.extend_from_slice(&self.data);

        // -- hash
        let hash = fnv1a(&buff[WAL_HEADER_SIZE as _..]);
        buff.extend_from_slice(&hash.to_le_bytes());

        buff
    }

    #[inline(always)]
    pub fn decode(
        lsn: u64,
        previous_lsn: u64,
        flags: WalFlags,
        data: &[u8],
    ) -> Result<Self, WalError> {
        use crate::core::storage::mvcc::fnv1a;

        if data.len() < 35 {
            return Err(WalError::InvalidSize(data.len()));
        };

        let offset = data.len() - 4;
        let from = u32::from_le_bytes(data[offset..].try_into().expect("Binary format mismatch"));
        let expected = fnv1a(&data[..offset]);

        if from != expected {
            return Err(WalError::Checksum);
        }

        let data = &data[..offset];
        let mut cursor = 0;

        let mut read_bytes = |n: usize| -> Result<&[u8], WalError> {
            if cursor + n > data.len() {
                return Err(WalError::UnexpectedEnd);
            }
            let slice = &data[cursor..cursor + n];
            cursor += n;
            Ok(slice)
        };

        let txn_id = i64::from_le_bytes(read_bytes(8)?.try_into().unwrap());

        let table_len = u16::from_le_bytes(read_bytes(2)?.try_into().unwrap()) as usize;
        let table_name = String::from_utf8(read_bytes(table_len)?.to_vec())
            .map_err(|e| WalError::Name(e.to_string()))?;

        let row_id = i64::from_le_bytes(read_bytes(8)?.try_into().unwrap());

        let code = read_bytes(1)?[0];
        let operation =
            WalOperation::try_from(code).map_err(|_| WalError::InvalidOperation(code))?;

        let timestamp = i64::from_le_bytes(read_bytes(8)?.try_into().unwrap());

        let content_len = u32::from_le_bytes(read_bytes(4)?.try_into().unwrap()) as usize;
        let content = data[cursor..cursor + content_len].to_vec();

        Ok(Self {
            table: table_name,
            data: content,
            lsn,
            previous_lsn,
            flags,
            txn_id,
            row_id,
            operation,
            timestamp,
        })
    }

    #[inline]
    pub fn operate(txn_id: i64, operation: WalOperation, flags: WalFlags) -> Self {
        Self::with_flags(String::new(), txn_id, 0, operation, Vec::new(), flags)
    }

    #[inline]
    pub fn commit(txn_id: i64) -> Self {
        Self::operate(txn_id, WalOperation::Commit, WalFlags::COMMIT)
    }

    #[inline]
    pub fn rollback(txn_id: i64) -> Self {
        Self::operate(txn_id, WalOperation::Rollback, WalFlags::ABORT)
    }

    #[inline]
    pub const fn is_marker(&self) -> bool {
        matches!(self.txn_id, MARKER_ID)
    }

    #[inline]
    pub const fn is_abort(&self) -> bool {
        matches!(self.operation, WalOperation::Rollback) || self.flags.contains(WalFlags::ABORT)
    }

    #[inline]
    pub const fn is_commit(&self) -> bool {
        matches!(self.operation, WalOperation::Commit) || self.flags.contains(WalFlags::COMMIT)
    }
}

impl WalOperation {
    pub const fn end_transaction(&self) -> bool {
        matches!(self, WalOperation::Commit | WalOperation::Rollback)
    }

    pub const fn changes_properties(&self) -> bool {
        matches!(
            self,
            WalOperation::Insert
                | WalOperation::Update
                | WalOperation::Delete
                | WalOperation::CreateTable
                | WalOperation::DropTable
                | WalOperation::AlterTable
                | WalOperation::CreateIndex
                | WalOperation::DropIndex
        )
    }
}

impl TryFrom<u8> for WalOperation {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Some(match value {
            1 => Self::Insert,
            2 => Self::Update,
            3 => Self::Delete,
            4 => Self::Commit,
            5 => Self::Rollback,
            6 => Self::CreateTable,
            7 => Self::DropTable,
            8 => Self::AlterTable,
            9 => Self::CreateIndex,
            10 => Self::DropIndex,
            _ => return Err(()),
        })
        .ok_or(())
    }
}

impl WalFlags {
    /// No flag is set.
    pub const NONE: WalFlags = WalFlags(0);

    /// The data is compressed.
    pub const COMPRESSED: WalFlags = WalFlags(1 << 0);

    /// Commit record marker.
    pub const COMMIT: WalFlags = WalFlags(1 << 1);
    /// Abort record marker.
    pub const ABORT: WalFlags = WalFlags(1 << 2);
    /// Checkout record marker.
    pub const CHECKPOINT: WalFlags = WalFlags(1 << 3);

    /// Snapshot start marker.
    pub const SNAPSHOT_START: WalFlags = WalFlags(1 << 4);
    /// Snapshot complete marker.
    pub const SNAPSHOT_COMPLETE: WalFlags = WalFlags(1 << 5);

    /// Rotation marker.
    pub const ROTATION: WalFlags = WalFlags(1 << 6);

    pub const fn contains(&self, other: WalFlags) -> bool {
        (self.0 & other.0) == other.0
    }

    pub fn set(&mut self, other: WalFlags) {
        self.0 |= other.0
    }

    pub fn union(self, other: WalFlags) -> WalFlags {
        WalFlags(self.0 | other.0)
    }

    pub fn clear(&mut self, other: WalFlags) {
        self.0 &= !other.0
    }
}

impl From<u8> for WalFlags {
    fn from(value: u8) -> Self {
        Self(value)
    }
}

impl From<WalFlags> for u8 {
    fn from(value: WalFlags) -> Self {
        value.0
    }
}
