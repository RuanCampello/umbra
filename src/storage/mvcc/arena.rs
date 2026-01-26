use std::sync::{Arc, RwLock};

use crate::sql::statement::Value;

/// A contiguous memory allocator for storing tuple data.
/// This way we get O(1) clone on write with `Arc` and zero-clone on read.
pub struct TupleArena {
    arena: RwLock<Arena>,
}

pub struct Arena {
    data: Vec<Arc<[Arc<Value>]>>,
    metadata: Vec<ArenaMetadata>,
}

#[derive(Clone, Copy)]
pub struct ArenaMetadata {
    row_id: i64,
    txn_id: i64,
    deleted_at_txn_id: i64,
    created_at: i64,
}

impl TupleArena {
    const DEFAULT_CAPACITY: usize = 1 << 14;

    pub fn new() -> Self {
        Self {
            arena: RwLock::new(Arena {
                data: Vec::with_capacity(Self::DEFAULT_CAPACITY),
                metadata: Vec::with_capacity(Self::DEFAULT_CAPACITY),
            }),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            arena: RwLock::new(Arena {
                data: Vec::with_capacity(capacity),
                metadata: Vec::with_capacity(capacity),
            }),
        }
    }

    #[inline]
    pub fn insert(&self, id: i64, txn_id: i64, created_at: i64, values: &[Value]) -> usize {
        let mut arena = self.arena.write().expect("lock poisoned");

        let values: Vec<Arc<Value>> = values.iter().map(|v| Arc::new(v.clone())).collect();
        let metadata = ArenaMetadata {
            txn_id,
            created_at,
            row_id: id,
            deleted_at_txn_id: 0,
        };

        let idx = arena.metadata.len();
        arena.metadata.push(metadata);

        idx
    }

    #[inline]
    pub fn mark_as_deleted(&self, id: i64, deleted_at_txn_id: i64) {
        let mut arena = self.arena.write().expect("lock poisoned");

        if (id as usize) < arena.metadata.len() {
            arena.metadata[id as usize].deleted_at_txn_id = deleted_at_txn_id
        }
    }

    pub fn len(&self) -> usize {
        self.arena.read().expect("lock poisoned").metadata.len()
    }
}

impl ArenaMetadata {
    #[inline]
    const fn is_deleted(&self) -> bool {
        self.deleted_at_txn_id != 0
    }
}
