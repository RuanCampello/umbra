use crate::{
    collections::{hash::HashMap, smallvec::SmallVec},
    db::SchemaNew as Schema,
    sql::Value,
    storage::mvcc::{
        arena::TupleArena, get_timestamp, index::Index, registry::TransactionRegistry, MvccError,
    },
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
    /// Registered indexes, keyed by index name.
    indexes: RwLock<HashMap<String, Arc<dyn Index>>>,
    open: AtomicBool,
    uncommited_writes: RwLock<HashMap<i64, i64>>,
    /// Maximum number of previous versions per row.
    max_version_history: usize,
    visibility_checker: Option<Arc<TransactionRegistry>>,
    arena: TupleArena,
}

pub(crate) struct TransationVersionStorage {
    txn_id: i64,
    pub(crate) local: Option<HashMap<i64, VersionChain>>,
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

/// Determines the kind of DML write operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteKind {
    Insert,
    Update,
    Delete,
}

static VERSION_CHAIN_MAP_POOL: Mutex<Vec<HashMap<i64, VersionChain>>> = Mutex::new(Vec::new());
static WRITE_SET_MAP_POOL: Mutex<Vec<HashMap<i64, WriteSet>>> = Mutex::new(Vec::new());

const MAX_POOL_SIZE: usize = 1 << 6;

type VersionChain = SmallVec<TupleVersion, 1>;
type MvccResult<T> = std::result::Result<T, MvccError>;

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
            indexes: RwLock::new(HashMap::default()),
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

    /// Recover a version from WAL replay.
    pub fn recover_version(&self, version: TupleVersion) {
        self.add_version(version.row_id, version);
    }

    /// Register an index for this table.
    pub fn add_index(&self, name: String, index: Arc<dyn Index>) {
        self.indexes.write().unwrap().insert(name, index);
    }

    /// Returns a snapshot of all registered indexes.
    pub fn get_indexes(&self) -> Vec<Arc<dyn Index>> {
        self.indexes.read().unwrap().values().cloned().collect()
    }

    /// Returns a specific index by name.
    pub fn get_index(&self, name: &str) -> Option<Arc<dyn Index>> {
        self.indexes.read().unwrap().get(name).cloned()
    }

    /// Update all indexes after an `INSERT`.
    pub fn update_indexes_on_insert(&self, row_id: i64, data: &[Value]) {
        let indexes = self.indexes.read().unwrap();
        for index in indexes.values() {
            let col = index.column_index();
            if col < data.len() {
                let _ = index.add(&data[col], row_id);
            }
        }
    }

    /// Update all indexes after a `DELETE`.
    pub fn update_indexes_on_delete(&self, row_id: i64, data: &[Value]) {
        let indexes = self.indexes.read().unwrap();
        for index in indexes.values() {
            let col = index.column_index();
            if col < data.len() {
                index.remove(&data[col], row_id);
            }
        }
    }

    /// Update all indexes after an `UPDATE`.
    pub fn update_indexes_on_update(&self, row_id: i64, old: &[Value], new: &[Value]) {
        let indexes = self.indexes.read().unwrap();
        for index in indexes.values() {
            let col = index.column_index();
            if col < old.len() {
                index.remove(&old[col], row_id);
            }
            if col < new.len() {
                let _ = index.add(&new[col], row_id);
            }
        }
    }

    /// Adds a committed version into the global store.
    ///
    /// Manages the version chain, stores data in the
    /// `TupleArena`, and trims history beyond `max_version_history`.
    pub fn add_version(&self, row_id: i64, version: TupleVersion) {
        use std::collections::btree_map::Entry;

        if !self.open.load(Ordering::Acquire) {
            return;
        }

        let is_deleted = version.is_deleted();
        let mut versions = self.versions.write().unwrap();

        match versions.entry(row_id) {
            Entry::Occupied(mut occupied) => {
                let existing = occupied.get();
                let existing_arena_idx = existing.arena_idx;

                let existing_depth = chain_depth(existing);
                let can_prune =
                    self.max_version_history > 0 && (existing_depth + 1) > self.max_version_history;

                let mut new_version = version;

                if new_version.is_deleted() && new_version.data.is_empty() {
                    new_version.data = existing.version.data.clone();
                }

                let arena_idx = match !is_deleted {
                    true => {
                        let idx = self
                            .arena
                            .insert(row_id, new_version.txn_id, &new_version.data);
                        pack_index(idx)
                    }
                    _ => {
                        if let Some(old_idx) = unpack_index(existing_arena_idx) {
                            self.arena
                                .mark_as_deleted(old_idx as i64, new_version.txn_id);
                        }
                        existing_arena_idx
                    }
                };

                let prev = match can_prune {
                    true => None,
                    _ => {
                        let existing_version = existing.version.clone();
                        let existing_prev = existing.prev.clone();
                        Some(Arc::new(VersionEntry {
                            version: existing_version,
                            prev: existing_prev,
                            arena_idx: None,
                        }))
                    }
                };

                let new_entry = VersionEntry {
                    version: new_version,
                    prev,
                    arena_idx,
                };

                occupied.insert(new_entry);
            }

            Entry::Vacant(vacant) => {
                let arena_idx = match !is_deleted {
                    true => {
                        let idx = self.arena.insert(row_id, version.txn_id, &version.data);
                        pack_index(idx)
                    }
                    _ => None,
                };

                vacant.insert(VersionEntry {
                    version,
                    prev: None,
                    arena_idx,
                });
            }
        }
    }

    /// Gets the latest visible version of a row for the given transaction.
    ///
    /// Walks the version chain, checking visibility via the `TransactionRegistry`.
    /// Returns `None` if the row doesn't exist or the visible version is deleted.
    pub fn get_visible_version(&self, row_id: i64, txn_id: i64) -> Option<TupleVersion> {
        if !self.open.load(Ordering::Acquire) {
            return None;
        }

        let checker = self.visibility_checker.as_ref()?;
        let versions = self.versions.read().unwrap();
        let entry = versions.get(&row_id)?;

        if checker.is_visible(entry.version.txn_id, txn_id) {
            if entry.version.is_deleted()
                && checker.is_visible(entry.version.deleted_at_txn_id, txn_id)
            {
                return None;
            }
            return Some(entry.version.clone());
        }

        let mut current = entry.prev.as_deref();
        while let Some(prev) = current {
            if checker.is_visible(prev.version.txn_id, txn_id) {
                if prev.version.is_deleted()
                    && checker.is_visible(prev.version.deleted_at_txn_id, txn_id)
                {
                    return None;
                }
                return Some(prev.version.clone());
            }

            current = prev.prev.as_deref();
        }

        None
    }

    /// Marks a row as deleted. Used during WAL replay for `Delete` entries.
    pub fn mark_deleted(&self, row_id: i64, txn_id: i64) {
        let mut versions = self.versions.write().unwrap();
        if let Some(entry) = versions.get_mut(&row_id) {
            entry.version.deleted_at_txn_id = txn_id;
        }
    }

    /// Scans all visible (non-deleted) rows for the given transaction.
    pub fn scan_visible(&self, txn_id: i64) -> Vec<Tuple> {
        if !self.open.load(Ordering::Acquire) {
            return Vec::new();
        }

        let checker = match self.visibility_checker.as_ref() {
            Some(c) => c,
            None => return Vec::new(),
        };

        let versions = self.versions.read().unwrap();
        let mut results = Vec::new();

        for (_, entry) in versions.iter() {
            if checker.is_visible(entry.version.txn_id, txn_id) {
                if entry.version.is_deleted()
                    && checker.is_visible(entry.version.deleted_at_txn_id, txn_id)
                {
                    continue;
                }
                results.push(entry.version.data.clone());
                continue;
            }

            let mut current = entry.prev.as_deref();
            while let Some(prev) = current {
                if checker.is_visible(prev.version.txn_id, txn_id) {
                    if prev.version.is_deleted()
                        && checker.is_visible(prev.version.deleted_at_txn_id, txn_id)
                    {
                        break;
                    }

                    results.push(prev.version.data.clone());
                    break;
                }

                current = prev.prev.as_deref();
            }
        }

        results
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

    /// Eagerly claims a tuple for writing by the given transaction.
    ///
    /// If another transaction already holds the tuple, returns `WriteConflict`.
    /// If the same transaction holds it, this is a no-op (idempotent).
    pub fn try_claim_tuple(
        &self,
        tuple_id: i64,
        txn_id: i64,
    ) -> std::result::Result<(), MvccError> {
        let mut writes = self.uncommited_writes.write().unwrap();
        match writes.get(&tuple_id) {
            Some(&holder) if holder != txn_id => Err(MvccError::WriteConflict),
            Some(_) => Ok(()), // already claimed by us
            None => {
                writes.insert(tuple_id, txn_id);
                Ok(())
            }
        }
    }

    /// Release a single tuple claim held by `txn_id`.
    pub fn release_tuple_claim(&self, tuple_id: i64, txn_id: i64) {
        let mut writes = self.uncommited_writes.write().unwrap();
        if matches!(writes.get(&tuple_id), Some(&holder) if holder == txn_id) {
            writes.remove(&tuple_id);
        }
    }

    /// Batch release of tuple claims held by `txn_id`.
    pub fn release_tuple_claims_batch(&self, tuple_ids: &[i64], txn_id: i64) {
        let mut writes = self.uncommited_writes.write().unwrap();
        for &id in tuple_ids {
            if matches!(writes.get(&id), Some(&holder) if holder == txn_id) {
                writes.remove(&id);
            }
        }
    }

    /// Returns the `txn_id` of the HEAD (latest committed) version for a tuple.
    ///
    /// Used at commit time to detect if another transaction has modified
    /// the tuple since we read it.
    pub fn get_latest_version_id(&self, tuple_id: i64) -> Option<i64> {
        let versions = self.versions.read().unwrap();
        versions.get(&tuple_id).map(|entry| entry.version.txn_id)
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

    #[inline]
    fn ensure_local(&mut self) -> &mut HashMap<i64, VersionChain> {
        self.local.get_or_insert_with(get_version_map)
    }

    #[inline]
    fn ensure_write_set(&mut self) -> &mut HashMap<i64, WriteSet> {
        self.write_set.get_or_insert_with(get_write_set_map)
    }

    /// Write a tuple into the transaction-local storage.
    /// This is the single entry point for all DML operations.
    pub fn put(&mut self, tuple_id: i64, data: Tuple, kind: WriteKind) -> MvccResult<()> {
        let mut version = TupleVersion::new(self.txn_id, tuple_id, data);
        if kind == WriteKind::Delete {
            version.deleted_at_txn_id = self.txn_id;
        }

        let has_local = self
            .local
            .as_ref()
            .is_some_and(|l| l.contains_key(&tuple_id));

        match has_local {
            true => self
                .ensure_local()
                .get_mut(&tuple_id)
                .unwrap()
                .push(version),
            _ => {
                let needs_write_set = self
                    .write_set
                    .as_ref()
                    .is_none_or(|ws| !ws.contains_key(&tuple_id));

                if needs_write_set {
                    let read_version = self.parent.get_visible_version(tuple_id, self.txn_id);
                    let tuple_exists = read_version.is_some();

                    let version_sequence = self
                        .parent
                        .visibility_checker
                        .as_ref()
                        .map(|c| c.get_sequence())
                        .unwrap_or(0);

                    self.ensure_write_set().insert(
                        tuple_id,
                        WriteSet {
                            version: read_version,
                            version_sequence,
                        },
                    );

                    if tuple_exists {
                        self.parent.try_claim_tuple(tuple_id, self.txn_id)?;
                    }
                }

                let mut chain = SmallVec::new();
                chain.push(version);
                self.ensure_local().insert(tuple_id, chain);
            }
        }

        Ok(())
    }

    /// Check local writes first (read-your-own-writes), then fall back to parent.
    pub fn get(&self, tuple_id: i64) -> Option<Tuple> {
        if let Some(local) = self.local.as_ref() {
            if let Some(versions) = local.get(&tuple_id) {
                if let Some(version) = versions.last() {
                    if version.is_deleted() {
                        return None;
                    }
                    return Some(version.data.clone());
                }
            }
        }

        self.parent
            .get_visible_version(tuple_id, self.txn_id)
            .map(|v| v.data.clone())
    }

    #[inline(always)]
    pub fn get_local_version(&self, tuple_id: i64) -> Option<&TupleVersion> {
        self.local
            .as_ref()
            .and_then(|local| local.get(&tuple_id))
            .and_then(|chain| chain.last())
    }

    #[inline(always)]
    pub fn has_local_changes(&self) -> bool {
        self.local.as_ref().is_some_and(|l| !l.is_empty())
    }

    /// Detect write-write conflicts before commit.
    ///
    /// For each entry in the write set:
    /// - UPDATE/DELETE: verify the HEAD version hasn't changed since we read it.
    /// - INSERT: verify no other transaction inserted a visible version for this tuple.
    fn detect_conflicts(&self) -> MvccResult<()> {
        let Some(write_set) = self.write_set.as_ref() else {
            return Ok(());
        };

        for (&tuple_id, entry) in write_set.iter() {
            if let Some(read_version) = &entry.version {
                match self.parent.get_latest_version_id(tuple_id) {
                    Some(latest_txn_id) if latest_txn_id != read_version.txn_id => {
                        return Err(MvccError::WriteConflict);
                    }
                    // tuple vanished (gc'd): treat as conflict.
                    None => return Err(MvccError::WriteConflict),
                    _ => {}
                }
            } else {
                if self
                    .parent
                    .get_visible_version(tuple_id, self.txn_id)
                    .is_some()
                {
                    return Err(MvccError::WriteConflict);
                }
            }
        }

        Ok(())
    }

    /// Update indexes at commit time (deferred from DML time).
    ///
    /// Walks local versions and determines the appropriate index operation:
    /// - INSERT (no old version): add to indexes
    /// - UPDATE (old version exists, new is not deleted): remove old, add new
    /// - DELETE (new version is deleted): remove from indexes
    fn update_indexes_on_commit(&self) {
        let Some(local) = self.local.as_ref() else {
            return;
        };

        for (&tuple_id, chain) in local.iter() {
            let Some(new_version) = chain.last() else {
                continue;
            };

            let old_version = self
                .write_set
                .as_ref()
                .and_then(|ws| ws.get(&tuple_id))
                .and_then(|entry| entry.version.as_ref())
                .filter(|v| !v.is_deleted());

            if new_version.is_deleted() {
                if let Some(old) = old_version {
                    self.parent.update_indexes_on_delete(tuple_id, &old.data);
                }
            } else if let Some(old) = old_version {
                self.parent
                    .update_indexes_on_update(tuple_id, &old.data, &new_version.data);
            } else {
                self.parent
                    .update_indexes_on_insert(tuple_id, &new_version.data);
            }
        }
    }

    pub fn commit(&mut self) -> MvccResult<Vec<(i64, TupleVersion)>> {
        self.detect_conflicts()?;

        self.update_indexes_on_commit();

        let local = match self.local.take() {
            Some(map) => map,
            None => {
                self.release_all_claims();
                return Ok(Vec::new());
            }
        };

        let mut batch = Vec::with_capacity(local.len());

        for (tuple_id, mut versions) in local {
            if let Some(version) = versions.pop() {
                batch.push((tuple_id, version));
            }
        }

        for (tuple_id, version) in &batch {
            self.parent.add_version(*tuple_id, version.clone());
        }

        self.release_all_claims();

        Ok(batch)
    }

    /// Discard all local writes and release tuple claims.
    pub fn rollback(&mut self) {
        if let Some(map) = self.local.take() {
            return_version_map(map);
        }

        self.release_all_claims();
    }

    /// Release all tuple claims tracked in the write set back to the parent.
    fn release_all_claims(&mut self) {
        if let Some(ws) = self.write_set.take() {
            let tuple_ids: Vec<i64> = ws.keys().copied().collect();
            if !tuple_ids.is_empty() {
                self.parent
                    .release_tuple_claims_batch(&tuple_ids, self.txn_id);
            }
            return_write_set_map(ws);
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
        match self.row_id == row_id && !self.is_deleted() {
            true => Some(self.data.clone()),
            _ => None,
        }
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
fn get_version_map() -> HashMap<i64, VersionChain> {
    VERSION_CHAIN_MAP_POOL
        .lock()
        .expect("lock poisoned")
        .pop()
        .unwrap_or_default()
}

#[inline]
fn get_write_set_map() -> HashMap<i64, WriteSet> {
    WRITE_SET_MAP_POOL
        .lock()
        .expect("lock poisoned")
        .pop()
        .unwrap_or_default()
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
fn pack_index(idx: usize) -> Option<NonZeroU64> {
    NonZeroU64::new((idx as u64).saturating_add(1))
}

#[inline(always)]
fn unpack_index(packed: Option<NonZeroU64>) -> Option<usize> {
    packed.map(|non_zero| (non_zero.get() - 1) as usize)
}

/// Count the depth of a version chain by traversing `prev` pointers.
#[inline(always)]
fn chain_depth(entry: &VersionEntry) -> usize {
    let mut depth = 1;
    let mut current = &entry.prev;
    while let Some(prev) = current {
        depth += 1;
        current = &prev.prev;
    }
    depth
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::{statement::Type, Value};

    fn make_schema() -> Schema {
        SchemaBuilder::new("test")
            .primary("id", Type::Integer)
            .nullable("name", Type::Text)
            .build()
    }

    fn make_registry() -> Arc<TransactionRegistry> {
        Arc::new(TransactionRegistry::new())
    }

    fn committed_txn(registry: &TransactionRegistry) -> i64 {
        let (txn_id, _) = registry.begin();
        registry.commit_transaction(txn_id);
        txn_id
    }

    #[test]
    fn add_and_get_visible_version() {
        let registry = make_registry();
        let storage = VersionStorage::with_checker("test", make_schema(), registry.clone());

        let (writer_txn, _) = registry.begin();
        let data: Tuple = vec![Value::Number(1), Value::String("alice".into())];
        let version = TupleVersion::new(writer_txn, 1, data.clone());
        storage.add_version(1, version);
        registry.commit_transaction(writer_txn);

        let (reader_txn, _) = registry.begin();
        let visible = storage.get_visible_version(1, reader_txn);
        assert!(visible.is_some());
        assert_eq!(visible.unwrap().data, data);
    }

    #[test]
    fn version_chain_history() {
        let registry = make_registry();
        let storage = VersionStorage::with_checker("test", make_schema(), registry.clone());

        let (txn1, _) = registry.begin();
        storage.add_version(
            1,
            TupleVersion::new(txn1, 1, vec![Value::Number(1), Value::String("v1".into())]),
        );
        registry.commit_transaction(txn1);

        let (txn2, _) = registry.begin();
        storage.add_version(
            1,
            TupleVersion::new(txn2, 1, vec![Value::Number(1), Value::String("v2".into())]),
        );
        registry.commit_transaction(txn2);

        let (reader, _) = registry.begin();
        let visible = storage.get_visible_version(1, reader).unwrap();
        assert_eq!(
            visible.data,
            vec![Value::Number(1), Value::String("v2".into())]
        );
    }

    #[test]
    fn delete_makes_row_invisible() {
        let registry = make_registry();
        let storage = VersionStorage::with_checker("test", make_schema(), registry.clone());

        // Insert
        let (txn1, _) = registry.begin();
        storage.add_version(
            1,
            TupleVersion::new(txn1, 1, vec![Value::Number(1), Value::String("x".into())]),
        );
        registry.commit_transaction(txn1);

        // Delete
        let (txn2, _) = registry.begin();
        let mut del = TupleVersion::new(txn2, 1, Vec::new());
        del.deleted_at_txn_id = txn2;
        storage.add_version(1, del);
        registry.commit_transaction(txn2);

        // Reader after delete should see nothing
        let (reader, _) = registry.begin();
        assert!(storage.get_visible_version(1, reader).is_none());
    }

    #[test]
    fn transaction_local_insert_commit() {
        let registry = make_registry();
        let storage = Arc::new(VersionStorage::with_checker(
            "test",
            make_schema(),
            registry.clone(),
        ));

        let (txn_id, _) = registry.begin();
        let mut txn_store = TransationVersionStorage::new(txn_id, storage.clone());

        txn_store
            .put(
                1,
                vec![Value::Number(1), Value::String("alice".into())],
                WriteKind::Insert,
            )
            .unwrap();
        txn_store
            .put(
                2,
                vec![Value::Number(2), Value::String("bob".into())],
                WriteKind::Insert,
            )
            .unwrap();

        assert!(storage.get_visible_version(1, txn_id).is_none());

        let batch = txn_store.commit().unwrap();
        assert_eq!(batch.len(), 2);
        registry.commit_transaction(txn_id);

        let (reader, _) = registry.begin();
        assert!(storage.get_visible_version(1, reader).is_some());
        assert!(storage.get_visible_version(2, reader).is_some());
    }

    #[test]
    fn transaction_local_rollback() {
        let registry = make_registry();
        let storage = Arc::new(VersionStorage::with_checker(
            "test",
            make_schema(),
            registry.clone(),
        ));

        let (txn_id, _) = registry.begin();
        let mut txn_store = TransationVersionStorage::new(txn_id, storage.clone());

        txn_store
            .put(
                1,
                vec![Value::Number(1), Value::String("gone".into())],
                WriteKind::Insert,
            )
            .unwrap();
        assert!(txn_store.get_local_version(1).is_some());

        txn_store.rollback();
        assert!(txn_store.get_local_version(1).is_none());
        assert!(storage.get_visible_version(1, txn_id).is_none());
    }

    #[test]
    fn scan_visible_rows() {
        let registry = make_registry();
        let storage = VersionStorage::with_checker("test", make_schema(), registry.clone());

        let (txn1, _) = registry.begin();
        for i in 1i64..=3 {
            storage.add_version(
                i,
                TupleVersion::new(
                    txn1,
                    i,
                    vec![Value::Number(i as i128), Value::String(format!("row{i}"))],
                ),
            );
        }
        registry.commit_transaction(txn1);

        let (txn2, _) = registry.begin();
        let mut del = TupleVersion::new(txn2, 2, Vec::new());
        del.deleted_at_txn_id = txn2;
        storage.add_version(2, del);
        registry.commit_transaction(txn2);

        let (reader, _) = registry.begin();
        let rows = storage.scan_visible(reader);
        assert_eq!(rows.len(), 2);
    }
}
