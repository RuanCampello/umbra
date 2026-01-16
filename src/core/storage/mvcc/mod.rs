#![allow(unused)]

use crate::{
    core::storage::wal::WalError,
    os::{Fs, Open},
};
use std::{
    fmt::Display,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::AtomicI64,
};

pub(self) mod arena;
pub(crate) mod engine;
pub(crate) mod registry;
pub(crate) mod version;
pub(crate) mod wal;

#[derive(Debug)]
pub enum MvccError {
    Io(std::io::Error),
    Wal(WalError),
}

static LAST_USED_TIMESTAMP: AtomicI64 = AtomicI64::new(0);

/// Represents an exclusive lock on the database directory.
pub(self) struct FileLock {
    file: File,
    path: PathBuf,
}

impl FileLock {
    /// Acquires an exclusive lock on the database directory.
    ///
    /// Uses the OS-specific file locking from [os](crate::os).
    pub fn acquire(path: impl AsRef<Path>) -> Result<Self, MvccError> {
        let path = path.as_ref();

        fs::create_dir_all(path)?;
        let path = path.join("db.lock");

        let mut file = Fs::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .lock(true)
            .open(&path)?;

        let pid = std::process::id();
        write!(file, "{}", pid)?;
        file.sync_all()?;

        Ok(Self { file, path })
    }
}

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
            Self::Io(cause) => write!(f, "Failed to operate on IO: {cause}"),
            Self::Wal(wal) => write!(f, "{wal}"),
        }
    }
}

impl From<std::io::Error> for MvccError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<WalError> for MvccError {
    fn from(value: WalError) -> Self {
        Self::Wal(value)
    }
}
