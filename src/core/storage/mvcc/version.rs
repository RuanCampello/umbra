use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicBool, Arc, RwLock},
};

use crate::{
    core::{smallvec::SmallVec, storage::mvcc::get_timestamp},
    db::Schema,
    vm::planner::Tuple,
};

pub(crate) struct VersionStorage {
    table: String,
    /// Row versions indexed by the `row_id`
    versions: RwLock<BTreeMap<i64, VersionEntry>>,
    schema: RwLock<Arc<Schema>>,
    open: AtomicBool,
    uncommited_writes: RwLock<HashMap<i64, i64>>,
    /// Maximum number of previous versions per row.
    max_version_history: usize,
}

pub(crate) struct VersionEntry {
    version: RowVersion,
    prev: Option<Arc<VersionEntry>>,
    arena_idx: Option<usize>,
    chain_depth: usize,
}

#[derive(Clone)]
pub(crate) struct RowVersion {
    pub txn_id: i64, // Transaction that created this version (min_txn_id / xmin)
    pub deleted_at_txn_id: i64, // Transaction that deleted this version (max_txn_id / xmax)
    pub data: Option<Tuple>, // TODO: we could compress this one
    pub row_id: i64, // Or RowId
    pub created_at: i64, // Timestamp
}

type VersionChain = SmallVec<RowVersion, 1>;

impl VersionStorage {
    pub fn new(table: String, schema: Schema) -> Self {
        Self::with_capacity(table, schema, 0)
    }

    pub fn with_capacity(table: String, schema: Schema, expected_rows: usize) -> Self {
        let versions = RwLock::new(BTreeMap::new());

        Self {
            table,
            versions,
            schema: RwLock::new(Arc::new(schema)),
            open: AtomicBool::new(true),
            uncommited_writes: RwLock::new(HashMap::default()),
            max_version_history: 10,
        }
    }
}

impl RowVersion {
    pub fn new(txn_id: i64, row_id: i64, data: Option<Tuple>) -> Self {
        Self {
            txn_id,
            row_id,
            data,
            deleted_at_txn_id: 0,
            created_at: get_timestamp(),
        }
    }

    pub fn with_timestamp(txn_id: i64, row_id: i64, data: Option<Tuple>, created_at: i64) -> Self {
        Self {
            txn_id,
            row_id,
            data,
            created_at,
            deleted_at_txn_id: 0,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at_txn_id != 0
    }
}

impl std::fmt::Debug for RowVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowVersion")
            .field("txn_id", &self.txn_id)
            .field("deleted_at_txn_id", &self.deleted_at_txn_id)
            .field("row_id", &self.row_id)
            .field("created_at", &self.created_at)
            .finish()
    }
}
