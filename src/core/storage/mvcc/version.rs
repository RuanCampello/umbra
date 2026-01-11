use std::{
    collections::BTreeMap,
    fmt::Debug,
    sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
};

use crate::{
    core::{
        smallvec::SmallVec,
        storage::mvcc::{arena::TupleArena, get_timestamp, registry::TransactionRegistry},
        HashMap,
    },
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
    visibility_checker: Option<Arc<TransactionRegistry>>,
    arena: TupleArena,
}

pub(crate) struct TransationVersionStorage {
    txn_id: i64,
    local: Option<HashMap<i64, VersionChain>>,
    parent: Arc<VersionStorage>,
    write_set: Option<HashMap<i64, WriteSet>>,
}

pub(crate) struct VersionEntry {
    version: TupleVersion,
    prev: Option<Arc<VersionEntry>>,
    arena_idx: Option<usize>,
    chain_depth: usize,
}

#[derive(Clone)]
pub(crate) struct TupleVersion {
    pub txn_id: i64, // Transaction that created this version (min_txn_id / xmin)
    pub deleted_at_txn_id: i64, // Transaction that deleted this version (max_txn_id / xmax)
    pub data: Option<Tuple>, // PERFORMANCE: we could compress this one
    pub row_id: i64,
    pub created_at: i64, // Timestamp
}

/// Tracks write operations.
/// This serves as conflict detection.
pub(crate) struct WriteSet {
    version: Option<TupleVersion>,
    version_sequence: i64,
}

static VERSION_CHAIN_MAP_POOL: Mutex<Vec<HashMap<i64, VersionChain>>> = Mutex::new(Vec::new());
static WRITE_SET_MAP_POOL: Mutex<Vec<HashMap<i64, WriteSet>>> = Mutex::new(Vec::new());

const MAX_POOL_SIZE: usize = 1 << 6;

type VersionChain = SmallVec<TupleVersion, 1>;

pub trait VisibilityChecker: Sync + Send {
    fn is_visible(&self, version_txn_id: i64, view_txn_id: i64) -> bool;
    fn get_sequence(&self) -> i64;
    fn get_active_transactions(&self) -> Vec<i64>;

    fn is_committed_before(&self, txn_id: i64, cut_off: i64) -> bool {
        true
    }
}

impl VersionStorage {
    pub fn new(table: String, schema: Schema) -> Self {
        Self::with_capacity(table, schema, Some(Arc::new(TransactionRegistry::new())), 0)
    }

    pub fn with_capacity(
        table: String,
        schema: Schema,
        checker: Option<Arc<TransactionRegistry>>,
        expected_rows: usize,
    ) -> Self {
        let versions = RwLock::new(BTreeMap::new());

        Self {
            table,
            versions,
            visibility_checker: checker,
            arena: TupleArena::new(),
            schema: RwLock::new(Arc::new(schema)),
            open: AtomicBool::new(true),
            uncommited_writes: RwLock::new(HashMap::default()),
            max_version_history: 10,
        }
    }
}

impl TransationVersionStorage {
    pub fn new(txn_id: i64, parent: Arc<VersionStorage>) -> Self {
        Self {
            txn_id,
            parent,
            local: None,
            write_set: None,
        }
    }
}

impl TupleVersion {
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

impl Debug for TupleVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RowVersion")
            .field("txn_id", &self.txn_id)
            .field("deleted_at_txn_id", &self.deleted_at_txn_id)
            .field("row_id", &self.row_id)
            .field("created_at", &self.created_at)
            .finish()
    }
}

impl Debug for TransationVersionStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransationVersionStorage")
            .field("txn_id", &self.txn_id)
            .field("local_count", &self.local.as_ref().map_or(0, |lv| lv.len()))
            .field(
                "write_set_count",
                &self.write_set.as_ref().map_or(0, |ws| ws.len()),
            )
            .finish()
    }
}

impl Drop for TransationVersionStorage {
    fn drop(&mut self) {
        if let Some(map) = self.local.take() {
            return_version_map(map);
        }

        if let Some(map) = self.write_set.take() {
            return_write_set_map(map);
        }
    }
}

#[inline]
fn return_version_map(mut map: HashMap<i64, VersionChain>) {
    map.clear();
    let mut pool = VERSION_CHAIN_MAP_POOL.lock().expect("lock poisoned");

    if pool.len() < MAX_POOL_SIZE {
        pool.push(map)
    }
}

#[inline]
fn return_write_set_map(mut map: HashMap<i64, WriteSet>) {
    map.clear();
    let mut pool = WRITE_SET_MAP_POOL.lock().expect("lock poisoned");

    if pool.len() < MAX_POOL_SIZE {
        pool.push(map);
    }
}
