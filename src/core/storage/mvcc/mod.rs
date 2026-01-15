#![allow(unused)]

use crate::core::storage::wal::WalError;
use std::{fmt::Display, sync::atomic::AtomicI64};

pub(self) mod arena;
pub(crate) mod engine;
pub(crate) mod registry;
pub(crate) mod version;
pub(crate) mod wal;

#[derive(Debug)]
pub enum MvccError {
    SnapshotOperation(std::io::Error),
    Wal(WalError),
}

static LAST_USED_TIMESTAMP: AtomicI64 = AtomicI64::new(0);

pub(self) fn get_timestamp() -> i64 {
    use std::sync::atomic::Ordering;
    use std::time::{SystemTime, UNIX_EPOCH};

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as i64)
        .unwrap_or(1);

    loop {
        let last = LAST_USED_TIMESTAMP.load(Ordering::Acquire);

        let next = match now > last {
            true => now,
            _ => last + 1,
        };

        if LAST_USED_TIMESTAMP
            .compare_exchange(last, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            return next;
        }
    }
}

#[inline(always)]
/// Basic hashing function, used for simplicity.
/// We could probably make it faster using `crc32` or `crc32c`, but that's a much more complex
/// algorithm.
/// That can be done in the future thou :D
///
/// For the interesed ones: [https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function].
pub(in crate::core::storage) const fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    let mut idx = 0;

    while idx < bytes.len() {
        hash ^= bytes[idx] as u32;
        hash = hash.wrapping_mul(0x01000193);

        idx += 1;
    }

    hash
}

impl Display for MvccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SnapshotOperation(cause) => write!(f, "Failed to operate on snapshot: {cause}"),
            Self::Wal(wal) => write!(f, "{wal}"),
        }
    }
}

impl From<std::io::Error> for MvccError {
    fn from(value: std::io::Error) -> Self {
        Self::SnapshotOperation(value)
    }
}

impl From<WalError> for MvccError {
    fn from(value: WalError) -> Self {
        Self::Wal(value)
    }
}
