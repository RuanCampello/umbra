use std::sync::{Arc, Mutex, RwLock};

use crate::sql::statement::Value;

/// A contiguous memory allocator for storing tuple data.
/// This way we get O(1) clone on write with `Arc` and zero-clone on read.
pub struct TupleArena {
    arena: RwLock<Arena>,
    /// Free list of cleared slot indexes to be reused.
    /// This is to prevent unbounded arena growth during `INSERT`/`DELETE` cycles.
    free_list: Mutex<Vec<usize>>,
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
}

impl TupleArena {
    const DEFAULT_CAPACITY: usize = 1 << 14;

    pub fn new() -> Self {
        Self {
            arena: RwLock::new(Arena {
                data: Vec::with_capacity(Self::DEFAULT_CAPACITY),
                metadata: Vec::with_capacity(Self::DEFAULT_CAPACITY),
            }),
            free_list: Mutex::new(Vec::new()),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            arena: RwLock::new(Arena {
                data: Vec::with_capacity(capacity),
                metadata: Vec::with_capacity(capacity),
            }),
            free_list: Mutex::new(Vec::new()),
        }
    }

    #[inline]
    pub fn insert(&self, id: i64, txn_id: i64, values: &[Value]) -> usize {
        let free_list_top = self.free_list.lock().unwrap().pop();
        let mut arena = self.arena.write().expect("lock poisoned");

        let values: Arc<[Arc<Value>]> = values.iter().map(|v| Arc::new(v.clone())).collect();
        let metadata = ArenaMetadata {
            txn_id,
            row_id: id,
            deleted_at_txn_id: 0,
        };

        match free_list_top {
            Some(index) => {
                arena.data[index] = values;
                arena.metadata[index] = metadata;

                index
            }
            _ => {
                arena.data.push(values);
                let idx = arena.metadata.len();
                arena.metadata.push(metadata);

                idx
            }
        }
    }

    #[inline]
    pub fn mark_as_deleted(&self, id: i64, deleted_at_txn_id: i64) {
        let mut arena = self.arena.write().expect("lock poisoned");

        if (id as usize) < arena.metadata.len() {
            arena.metadata[id as usize].deleted_at_txn_id = deleted_at_txn_id
        }
    }

    #[inline]
    pub fn clear(&self, indexes: &[usize]) -> usize {
        let mut cleared = Vec::with_capacity(indexes.len());

        {
            let mut arena = self.arena.write().unwrap();
            let empty = Arc::new([]);

            let metadata = ArenaMetadata {
                row_id: 0,
                txn_id: 0,
                deleted_at_txn_id: 0,
            };

            for &index in indexes {
                if index < arena.metadata.len() {
                    arena.data[index] = empty.clone();
                    arena.metadata[index] = metadata;

                    cleared.push(index);
                }
            }
        }

        let count = cleared.len();
        // update the current state of the free list to include the
        // now cleared ones
        if count > 0 {
            let mut free = self.free_list.lock().unwrap();
            free.reserve(count);
            free.extend(cleared);
        }

        count
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
