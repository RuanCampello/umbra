#![allow(unused)]

use std::{fmt::Display, sync::atomic::AtomicI64};

pub(self) mod arena;
pub(crate) mod engine;
pub(crate) mod registry;
pub(crate) mod version;

pub enum MvccError {
    SnapshotOperation(std::io::Error),
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

impl Display for MvccError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SnapshotOperation(cause) => write!(f, "Failed to operate on snapshot: {cause}"),
        }
    }
}

impl From<std::io::Error> for MvccError {
    fn from(value: std::io::Error) -> Self {
        Self::SnapshotOperation(value)
    }
}
