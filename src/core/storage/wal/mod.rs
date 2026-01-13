mod entry;

/// WAL entry magic marker: "mnem"
const WAL_MAGIC: u32 = 0x6D6E656D;

/// Maximum WAL file size before rotating: 64MB, 2^26 bytes
const WAL_MAX_SIZE: u64 = 1 << 26;

/// Default WAL size: 64KB
const WAL_SIZE: u64 = 1 << 16;

/// Size of the WAL before the flushing: 32KB
const WAL_FLUSH_SIZE: u64 = 1 << 15;

/// "herm"
const CHECKPOINT_MAGIC: u32 = 0x6865726D;

/// As primeagen says, it's good to track the version of your binaries.
const WAL_BINARY_VERSION: u8 = 1;

const WAL_HEADER_SIZE: u16 = 1 << 5;
