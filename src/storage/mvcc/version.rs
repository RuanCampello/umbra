use crate::{
    collections::{hash::HashMap, smallvec::SmallVec},
    db::SchemaNew as Schema,
    sql::Value,
    storage::mvcc::{arena::TupleArena, get_timestamp, registry::TransactionRegistry},
    vm::planner::Tuple,
};
use std::{
    collections::BTreeMap,
    fmt::Debug,
    io::{Error, ErrorKind},
    num::NonZeroU64,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
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

/// Entry of the linked list version chain.
pub(crate) struct VersionEntry {
    version: TupleVersion,
    prev: Option<Arc<VersionEntry>>,
    /// Index into the arena for non-copying access.
    /// Stored as `idx + 1` for optimisation.
    arena_idx: Option<NonZeroU64>,
}

#[derive(Clone)]
pub(crate) struct TupleVersion {
    pub txn_id: i64, // Transaction that created this version (min_txn_id / xmin)
    pub deleted_at_txn_id: i64, // Transaction that deleted this version (max_txn_id / xmax)
    pub data: Tuple, // PERFORMANCE: we could compress this one
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

    pub fn with_checker(
        table: impl Into<String>,
        schema: Schema,
        checker: Arc<TransactionRegistry>,
    ) -> Self {
        Self::with_capacity(table.into(), schema, Some(checker), 0)
    }

    pub fn close(&self) {
        self.open.store(false, Ordering::Release)
    }

    pub fn recover_version(&self, version: TupleVersion) {
        let row_id = version.row_id;
        let is_deleted = version.is_deleted();
        let data = version.data;

        let versions = self.versions.read().unwrap();
        if let Some(entry) = versions.get(&row_id) {
            let entry_version = &entry.version;

            if entry_version.is_deleted() || is_deleted || entry_version.data == data {
                if row_id > 0 {
                    // TODO: increment counter
                }

                return;
            }
        }

        // TODO: add version && update indexes
    }

    /// Cleans deleted tuples that are older than the given retention time.
    pub fn cleanup(&self, retention: Duration) -> i32 {
        if !self.open.load(Ordering::Acquire) {
            return 0;
        }

        let now = self::get_timestamp();
        let cutoff = now - retention.as_nanos() as i64;

        let mut to_be_deleted = Vec::new();
        let versions = self.versions.read().unwrap();

        for (&id, chain) in versions.iter() {
            let version = &chain.version;

            if version.is_deleted() && version.created_at < cutoff {
                if self.can_be_safely_removed(version) {
                    to_be_deleted.push(id);
                }
            }
        }

        if to_be_deleted.is_empty() {
            return 0;
        }

        let mut deleted = Vec::with_capacity(to_be_deleted.len());
        let mut indices = Vec::with_capacity(to_be_deleted.len());

        {
            let mut versions = self.versions.write().unwrap();
            for id in &to_be_deleted {
                if let Some(entry) = versions.get(id) {
                    if entry.version.is_deleted() {
                        if let Some(idx) = unpack_index(entry.arena_idx) {
                            indices.push(idx);
                        }

                        versions.remove(id);
                        deleted.push(id);
                    }
                }
            }
        }

        if deleted.is_empty() {
            return 0;
        }

        // TODO: we need to remove the indexes using batches
        // after the version store removal

        self.arena.clear(&indices);
        deleted.len() as i32
    }

    pub fn cleanup_versions(&self, max_age: Duration) -> i32 {
        if !self.open.load(Ordering::Acquire) {
            return 0;
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(checker) => checker,
            _ => return 0,
        };

        let retention = Duration::from_secs(24 * 60 * 60);
        let now = self::get_timestamp();
        let cutoff = now - retention.as_nanos() as i64;

        let active = checker.get_active_transactions();
        let mut candidate = Vec::new();

        {
            let versions = self.versions.read().unwrap();
            for (&id, entry) in versions.iter() {
                if entry.prev.is_none() {
                    continue;
                }

                candidate.push(id);
            }
        }

        if candidate.is_empty() {
            return 0;
        }

        let mut cleaned = 0;
        let mut versions = self.versions.write().unwrap();

        for id in candidate {
            let Some(entry) = versions.get(&id) else {
                continue;
            };

            let mut previous = Vec::new();
            let mut current = entry.prev.as_ref();

            while let Some(prev) = current {
                previous.push(prev.clone());
                current = prev.prev.as_ref();
            }

            if previous.is_empty() {
                continue;
            }

            let mut keep_total = 0;
            for (idx, previous) in previous.iter().enumerate() {
                let mut keep = false;

                for &txn_id in &active {
                    if checker.is_visible(previous.version.txn_id, txn_id) {
                        keep = true;
                        break;
                    }
                }

                if !keep && previous.version.created_at >= cutoff {
                    keep = true;
                }

                if keep {
                    keep_total = idx + 1;
                }
            }

            if keep_total < previous.len() {
                let to_be_removed = previous.len() - keep_total;
                cleaned += to_be_removed as i32;

                let mut modified = entry.clone();

                match keep_total == 0 {
                    true => modified.prev = None,
                    _ => {
                        let kept = previous.into_iter().take(keep_total).collect::<Vec<_>>();

                        let mut new_prev = None;
                        // we build the chain from oldest -> newest
                        // so that's reversed :D
                        for entry in kept.into_iter().rev() {
                            let mut cloned = (*entry).clone();
                            cloned.prev = new_prev;
                            new_prev = Some(Arc::new(cloned));
                        }

                        modified.prev = new_prev;
                    }
                }

                versions.insert(id, modified);
            }
        }

        cleaned
    }

    /// Checks if a version can be safely removed, which means that's not
    /// visible to any active transaction.
    pub fn can_be_safely_removed(&self, version: &TupleVersion) -> bool {
        let checker = match self.visibility_checker.as_ref() {
            Some(checker) => checker,
            None => return true,
        };

        let transactions = checker.get_active_transactions();
        if transactions.is_empty() {
            return true;
        }

        for transaction in transactions {
            if checker.is_visible(version.txn_id, transaction) {
                return false;
            }
        }

        true
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
    pub fn new(txn_id: i64, row_id: i64, data: Tuple) -> Self {
        Self {
            txn_id,
            row_id,
            data,
            deleted_at_txn_id: 0,
            created_at: get_timestamp(),
        }
    }

    pub fn with_timestamp(txn_id: i64, row_id: i64, data: Tuple, created_at: i64) -> Self {
        Self {
            txn_id,
            row_id,
            data,
            created_at,
            deleted_at_txn_id: 0,
        }
    }

    pub fn get(&self, row_id: i64) -> Option<Tuple> {
        todo!()
    }

    pub fn is_deleted(&self) -> bool {
        self.deleted_at_txn_id != 0
    }
}

impl TryFrom<TupleVersion> for Vec<u8> {
    type Error = Error;

    fn try_from(value: TupleVersion) -> Result<Self, Self::Error> {
        let mut buff = Vec::with_capacity(128);

        buff.extend_from_slice(&value.txn_id.to_le_bytes()); // 8
        buff.extend_from_slice(&value.deleted_at_txn_id.to_le_bytes()); // 8
        buff.extend_from_slice(&value.row_id.to_le_bytes()); // 8
        buff.extend_from_slice(&value.created_at.to_le_bytes()); // 8

        let values = value.data;
        buff.extend_from_slice(&(values.len() as u32).to_le_bytes()); // 4
        for value in values {
            let bytes = value.serialise()?;
            buff.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            buff.extend_from_slice(&bytes);
        }

        Ok(buff)
    }
}

impl TryFrom<&[u8]> for TupleVersion {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 32 {
            return Err(Error::new(ErrorKind::InvalidInput, "Empty data"));
        }

        let mut cursor = 0;

        let txn_id = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let deleted_at_txn_id = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let row_id = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        let created_at = i64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;

        if cursor + 4 > data.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Missing tuple count"));
        }

        // read the quantity of values
        let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            // read the length prefix for this value
            if cursor + 4 > data.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Missing value length"));
            }
            let value_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;

            // read and deserialise the value
            if cursor + value_len > data.len() {
                return Err(Error::new(ErrorKind::InvalidData, "Truncated value data"));
            }
            let (value, _) = Value::deserialise(&data[cursor..cursor + value_len])?;
            values.push(value);
            cursor += value_len;
        }

        Ok(TupleVersion {
            txn_id,
            deleted_at_txn_id,
            data: values,
            row_id,
            created_at,
        })
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

impl Clone for VersionEntry {
    fn clone(&self) -> Self {
        Self {
            version: self.version.clone(),
            prev: self.prev.clone(),
            arena_idx: self.arena_idx,
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

#[inline(always)]
fn unpack_index(packed: Option<NonZeroU64>) -> Option<usize> {
    packed.map(|non_zero| (non_zero.get() - 1) as usize)
}
