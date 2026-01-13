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
