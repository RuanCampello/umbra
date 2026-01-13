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
    lsn: u64,
    previous_lsn: u64,
    flags: WalFlags,
    txn_id: i64,
    row_id: i64,
    operation: WalOperation,
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

    pub fn contains(&self, other: WalFlags) -> bool {
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
