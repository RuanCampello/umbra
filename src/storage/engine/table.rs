//! Rows and their version chains
//!
//! A table is a growable vector of fixed-size chunks addressed by row id, each chunk guards its
//! slots with one latch held only for nanoseconds
//! A slot holds the row's chain, oldest to newest
//! Every version is stamped with its commit timestamp, or with its writer's flagged marker while
//! uncommitted, so visibility is one comparison: `stamp == own marker || stamp <= snapshot`

use crate::db::Schema;
use crate::sql::Value;
use crate::storage::engine::{
    chunk::{CHUNK_BITS, CHUNK_SIZE, Chain, Chunk, Cold, Version},
    clock::TXN_FLAG,
    error::{EngineError, Result},
    index::{Holder, Index},
    transaction::IsolationLevel,
};
use std::sync::{
    Arc, Mutex, RwLock,
    atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering},
};

pub(crate) struct Table {
    pub id: u32,
    name: String,
    /// The shape of stored rows
    pub schema: Schema,
    /// `schema` with the row id first, the shape of scanned tuples
    pub scanned: Schema,
    chunks: RwLock<Vec<Arc<Chunk>>>,
    next_row_id: AtomicI64,
    indexes: RwLock<Vec<Arc<Index>>>,
    /// Next value handed out per column, only meaningful for serial columns
    serials: Box<[AtomicI64]>,
    /// Slots whose rows vanished (deleted, settled, unindexed), taken by inserts before growing
    /// `freed` mirrors its length so tables that never delete skip the lock
    free: Mutex<Vec<RowId>>,
    freed: AtomicUsize,
    /// Stamp of the transaction that created the table, as on a version: its marker until it commits
    /// 0 for tables every snapshot sees (recovered ones)
    created: AtomicU64,
    /// Stamp of the transaction that dropped it, 0 while none did
    dropped: AtomicU64,
    /// Where evicted chunks go
    /// `None` for in-memory engines or without a memory budget
    cold: Option<Arc<Cold>>,
}

/// What a reader sees: everything committed up to `snapshot`, plus its own writes
#[derive(Clone, Copy, Debug)]
pub(crate) struct View {
    pub snapshot: u64,
    pub marker: u64,
}

/// How a transaction writes: what it sees, the newest committed stamp it accepts over the row
/// (its snapshot, or a stamp re-checked under READ COMMITTED), its level, and the GC watermark
#[derive(Clone, Copy)]
pub(crate) struct Writer {
    pub view: View,
    pub floor: u64,
    pub level: IsolationLevel,
    pub watermark: u64,
}

/// Outcome of [Table::write] that is not an error
pub(crate) enum Written {
    /// Holds the image the newest version had before the write
    Done(Option<Row>),
    /// READ COMMITTED: a commit newer than the accepted floor got there first, the caller
    /// re-checks `row` and may retry accepting `stamp`
    Stale { stamp: u64, row: Option<Row> },
}

/// A scan in row-id order, in batches of slots that start at [FIRST_BATCH] and double up to a whole
/// chunk, so a short `LIMIT` copies few rows and a long scan works chunk at a time
pub(crate) struct TableScan {
    table: Arc<Table>,
    view: View,
    chunk: usize,
    slot: usize,
    batch: usize,
}

const SLOT_MASK: i64 = CHUNK_SIZE as i64 - 1;

/// Slots in a scan's first batch
const FIRST_BATCH: usize = 64;

pub(crate) type RowId = i64;
pub(crate) type Row = Arc<[Value]>;

impl Table {
    pub fn new(id: u32, name: String, schema: Schema, cold: Option<Arc<Cold>>) -> Self {
        let serials = (0..schema.columns.len()).map(|_| AtomicI64::new(0)).collect();

        Self {
            id,
            name,
            scanned: schema.with_row_id(),
            schema,
            chunks: RwLock::new(Vec::new()),
            next_row_id: AtomicI64::new(1),
            indexes: RwLock::new(Vec::new()),
            serials,
            free: Mutex::new(Vec::new()),
            freed: AtomicUsize::new(0),
            created: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            cold,
        }
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn indexes(&self) -> Vec<Arc<Index>> {
        self.indexes.read().expect("index list poisoned").clone()
    }

    pub fn index(&self, name: &str) -> Option<Arc<Index>> {
        self.indexes
            .read()
            .expect("index list poisoned")
            .iter()
            .find(|index| index.name == name)
            .cloned()
    }

    /// the index on `column`, preferring a unique one
    pub fn index_on(&self, column: usize) -> Option<Arc<Index>> {
        let indexes = self.indexes.read().expect("index list poisoned");
        let mut on_column = indexes.iter().filter(|index| index.column == column);

        let first = on_column.next()?;
        match first.unique {
            true => Some(Arc::clone(first)),
            _ => Some(Arc::clone(on_column.find(|index| index.unique).unwrap_or(first))),
        }
    }

    pub fn add_index(&self, index: Arc<Index>) {
        self.indexes.write().expect("index list poisoned").push(index);
    }

    /// registers `index` unless one of its name exists, under one write of the index list
    pub fn try_add_index(&self, index: Arc<Index>) -> bool {
        let mut indexes = self.indexes.write().expect("index list poisoned");
        let vacant = indexes.iter().all(|existing| existing.name != index.name);
        if vacant {
            indexes.push(index);
        }
        vacant
    }

    pub fn remove_index(&self, name: &str) {
        self.indexes
            .write()
            .expect("index list poisoned")
            .retain(|index| index.name != name);
    }

    #[inline]
    pub fn created(&self) -> u64 {
        self.created.load(Ordering::Acquire)
    }

    #[inline]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Acquire)
    }

    pub fn stamp_created(&self, stamp: u64) {
        self.created.store(stamp, Ordering::Release);
    }

    /// `stamp` is a marker when a transaction drops the table, its timestamp at commit, 0 to undo
    pub fn stamp_dropped(&self, stamp: u64) {
        self.dropped.store(stamp, Ordering::Release);
    }

    /// whether a committed transaction dropped the table
    #[inline]
    pub fn is_dropped(&self) -> bool {
        let dropped = self.dropped();
        dropped != 0 && dropped & TXN_FLAG == 0
    }

    /// whether `view` sees the table: its creation and not its drop
    #[inline]
    pub fn visible_to(&self, view: View) -> bool {
        let dropped = self.dropped();
        view.sees(self.created()) && (dropped == 0 || !view.sees(dropped))
    }

    pub fn next_serial(&self, column: usize) -> i64 {
        self.serials[column].fetch_add(1, Ordering::Relaxed) + 1
    }

    /// non-zero serial high-water marks, by column
    pub fn serials(&self) -> Vec<(usize, i64)> {
        self.serials
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .enumerate()
            .filter(|&(_, value)| value != 0)
            .collect()
    }

    pub fn raise_serial(&self, column: usize, value: i64) {
        self.serials[column].fetch_max(value, Ordering::Relaxed);
    }

    #[inline]
    pub fn next_row_id(&self) -> RowId {
        self.next_row_id.load(Ordering::Relaxed)
    }

    pub fn raise_row_id(&self, next: RowId) {
        self.next_row_id.fetch_max(next, Ordering::Relaxed);
    }

    /// raises the serial and row-id counters above values already present (recovery, back-fill)
    pub fn observe(&self, id: RowId, row: &[Value]) {
        self.next_row_id.fetch_max(id + 1, Ordering::Relaxed);

        for (counter, value) in self.serials.iter().zip(row) {
            if let Value::Number(number) = value {
                counter.fetch_max(*number as i64, Ordering::Relaxed);
            }
        }
    }

    /// the row as `view` sees it
    #[cfg(test)]
    pub fn get(&self, id: RowId, view: View) -> Option<Row> {
        let chunk = self.chunk(id)?;
        let slots = chunk.read();
        visible(&slots[slot(id)], view).cloned()
    }

    /// the row as `view` sees it, provided its `column` still equals `key` (index re-check)
    pub fn get_keyed(&self, id: RowId, view: View, column: usize, key: &Value) -> Option<Row> {
        let chunk = self.chunk(id)?;
        let slots = chunk.read();
        visible(&slots[slot(id)], view).filter(|row| row[column] == *key).cloned()
    }

    /// appends to `out` the rows of `entries` (index order) that `view` sees with the entry's key
    /// still in `column`, consecutive entries in one chunk share a single latch
    pub fn resolve_keyed(
        &self,
        entries: &[(Value, RowId)],
        view: View,
        column: usize,
        out: &mut Vec<(RowId, Row)>,
    ) {
        let mut rest = entries;
        while let Some((_, first)) = rest.first() {
            let number = first >> CHUNK_BITS;
            let run =
                rest.iter().position(|(_, id)| id >> CHUNK_BITS != number).unwrap_or(rest.len());
            let (same, tail) = rest.split_at(run);
            rest = tail;

            let Some(chunk) = self.chunk(*first) else {
                continue;
            };

            let slots = chunk.read();
            for (key, id) in same {
                if let Some(row) =
                    visible(&slots[slot(*id)], view).filter(|row| row[column] == *key)
                {
                    out.push((*id, Arc::clone(row)));
                }
            }
        }
    }

    /// [Table::resolve_keyed] without copying rows out: counts the visible entries until `limit`
    /// of them, returning `(visible, entries examined)`
    pub fn count_keyed(
        &self,
        entries: &[(Value, RowId)],
        view: View,
        column: usize,
        limit: usize,
    ) -> (usize, usize) {
        let (mut visible, mut examined) = (0, 0);
        let mut rest = entries;
        while let Some((_, first)) = rest.first() {
            let number = first >> CHUNK_BITS;
            let run =
                rest.iter().position(|(_, id)| id >> CHUNK_BITS != number).unwrap_or(rest.len());
            let (same, tail) = rest.split_at(run);
            rest = tail;

            let Some(chunk) = self.chunk(*first) else {
                examined += same.len();
                continue;
            };

            let slots = chunk.read();
            for (key, id) in same {
                examined += 1;
                if visible_with(&slots[slot(*id)], view, column, key) {
                    visible += 1;
                    if visible == limit {
                        return (visible, examined);
                    }
                }
            }
        }

        (visible, examined)
    }

    /// appends a new row owned by `marker`
    pub fn insert(&self, marker: u64, row: Row) -> RowId {
        let id = self
            .take_free()
            .unwrap_or_else(|| self.next_row_id.fetch_add(1, Ordering::Relaxed));
        let chunk = self.chunk_or_grow(id);
        let mut slots = chunk.write();

        let chain = &mut slots[slot(id)];
        assert!(chain.is_empty(), "row id {id} handed out twice");
        chain.push(Version { stamp: marker, row: Some(row) });

        id
    }

    /// writes a new image of the row (`None` deletes it) under first-updater-wins
    ///
    /// a committed newest version is accepted when its stamp is at most `writer.floor`
    /// every version older than the newest one at or below the watermark is pruned on the way (rows to
    /// `pruned`, for index clean-up)
    pub fn write(
        &self,
        id: RowId,
        writer: Writer,
        row: Option<Row>,
        pruned: &mut Vec<Row>,
    ) -> Result<Written> {
        let Writer { view, floor, level, watermark } = writer;
        let chunk = self.chunk(id).ok_or_else(|| gone(level))?;
        let mut slots = chunk.write();
        let chain = &mut slots[slot(id)];

        let Some(head) = chain.last_mut() else {
            return Err(gone(level));
        };

        if head.stamp == view.marker {
            return Ok(Written::Done(std::mem::replace(&mut head.row, row)));
        }

        if head.stamp & TXN_FLAG != 0 {
            return Err(EngineError::WriteConflict);
        }

        if head.stamp > floor || head.row.is_none() {
            return match level {
                IsolationLevel::ReadCommitted => {
                    Ok(Written::Stale { stamp: head.stamp, row: head.row.clone() })
                },
                _ => Err(EngineError::SerializationFailure),
            };
        }

        let previous = head.row.clone();
        prune(chain, watermark, pruned);
        chain.push(Version { stamp: view.marker, row });

        Ok(Written::Done(previous))
    }

    /// replaces the owner's marker with its commit timestamp
    pub fn stamp(&self, id: RowId, marker: u64, ts: u64) {
        let chunk = self.chunk(id).expect("stamped row exists");
        let mut slots = chunk.write();

        let head = slots[slot(id)].last_mut().expect("stamped row has versions");
        assert_eq!(head.stamp, marker, "row {id} is not owned by the committer");
        head.stamp = ts;
    }

    /// sets the row of the owner's uncommitted version (rolling back to a savepoint)
    pub fn restore(&self, id: RowId, marker: u64, row: Option<Row>) {
        let chunk = self.chunk(id).expect("restored row exists");
        let mut slots = chunk.write();

        let head = slots[slot(id)].last_mut().expect("restored row has versions");
        assert_eq!(head.stamp, marker, "row {id} is not owned by the restorer");
        head.row = row;
    }

    /// removes the owner's uncommitted version, if still there
    pub fn unlink(&self, id: RowId, marker: u64) {
        let Some(chunk) = self.chunk(id) else {
            return;
        };

        let mut slots = chunk.write();
        slots[slot(id)].pop_if(|version| version.stamp == marker);
    }

    /// replaces the row with one committed image (recovery: no snapshot predates it)
    pub fn recover_put(&self, id: RowId, row: Row, ts: u64) {
        self.observe(id, &row);

        let chunk = self.chunk_or_grow(id);
        let mut slots = chunk.write();
        let chain = &mut slots[slot(id)];

        *chain = Chain::new();
        chain.push(Version { stamp: ts, row: Some(row) });
    }

    pub fn recover_delete(&self, id: RowId) {
        self.next_row_id.fetch_max(id + 1, Ordering::Relaxed);

        if let Some(chunk) = self.chunk(id) {
            chunk.write()[slot(id)] = Chain::new();
        }
    }

    /// who keeps `key` alive in `column` of row `id`, for a unique check by `marker`
    pub(super) fn holder(&self, id: RowId, column: usize, key: &Value, marker: u64) -> Holder {
        let Some(chunk) = self.chunk(id) else {
            return Holder::Free;
        };

        let slots = chunk.read();
        let chain = &slots[slot(id)];
        let carries = |version: &Version| version.row.as_ref().is_some_and(|r| r[column] == *key);

        let Some(head) = chain.last() else {
            return Holder::Free;
        };

        match head.stamp {
            stamp if stamp == marker => match carries(head) {
                true => Holder::Committed,
                false => Holder::Free,
            },
            stamp if stamp & TXN_FLAG != 0 => {
                let committed = chain.iter().rev().find(|v| v.stamp & TXN_FLAG == 0);
                match carries(head) || committed.is_some_and(carries) {
                    true => Holder::Uncommitted,
                    false => Holder::Free,
                }
            },
            _ => match carries(head) {
                true => Holder::Committed,
                false => Holder::Free,
            },
        }
    }

    /// whether any version of row `id` still carries `key` in `column`
    pub fn carries(&self, id: RowId, column: usize, key: &Value) -> bool {
        let Some(chunk) = self.chunk(id) else {
            return false;
        };

        let slots = chunk.read();
        slots[slot(id)]
            .iter()
            .any(|version| version.row.as_ref().is_some_and(|row| row[column] == *key))
    }

    /// drops the index entries of `rows` (versions of row `id` pruned away) that no remaining
    /// version of the row carries
    pub fn forget(&self, id: RowId, rows: &[Row]) {
        if rows.is_empty() {
            return;
        }

        for index in self.indexes() {
            for row in rows {
                let key = &row[index.column];
                index.remove_if(key, id, || !self.carries(id, index.column, key));
            }
        }
    }

    /// every `(row id, row)` held by any version, for index back-fill and rebuild
    pub fn for_each_version(&self, mut f: impl FnMut(RowId, &Row)) {
        let chunks = self.chunks.read().expect("chunk list poisoned").clone();

        for (number, chunk) in chunks.iter().enumerate() {
            chunk.visit(|slots| {
                for (offset, chain) in slots.iter().enumerate() {
                    let id = ((number << CHUNK_BITS) + offset) as RowId;
                    chain
                        .iter()
                        .filter_map(|version| version.row.as_ref())
                        .for_each(|row| f(id, row));
                }
            });
        }
    }

    /// prunes every chain against `watermark` and empties slots whose surviving version is a
    /// deletion no snapshot can see past
    /// `forget` receives the rows dropped per row id, after the chunk latch
    /// is released (index clean-up takes index locks)
    pub fn sweep(&self, watermark: u64, mut forget: impl FnMut(RowId, &[Row])) -> usize {
        let chunks = self.chunks.read().expect("chunk list poisoned").clone();
        let mut reclaimed = 0;
        let mut dropped: Vec<(RowId, Vec<Row>)> = Vec::new();

        let mut vanished = Vec::new();

        for (number, chunk) in chunks.iter().enumerate() {
            if let Some(mut slots) = chunk.write_if_resident() {
                for (offset, chain) in slots.iter_mut().enumerate() {
                    let id = ((number << CHUNK_BITS) + offset) as RowId;
                    let occupied = !chain.is_empty();
                    let mut rows = Vec::new();
                    reclaimed += settle(chain, watermark, &mut rows);
                    if !rows.is_empty() {
                        dropped.push((id, rows));
                    }
                    if occupied && chain.is_empty() {
                        vanished.push(id);
                    }
                }
            }

            for (id, rows) in dropped.drain(..) {
                forget(id, &rows);
            }
            self.release(&mut vanished);
        }

        reclaimed
    }

    /// [Table::sweep] for the one row `id`: prunes it against `watermark` and drops the index
    /// entries no remaining version carries
    /// Writers call it on rows they find dead, so a key deleted and inserted again does not
    /// pile up entries until the next maintenance round
    pub fn settle_slot(&self, id: RowId, watermark: u64) {
        let Some(chunk) = self.chunk(id) else {
            return;
        };

        let mut rows = Vec::new();
        let mut vanished = false;
        if let Some(mut slots) = chunk.write_if_resident() {
            let chain = &mut slots[slot(id)];
            let occupied = !chain.is_empty();
            settle(chain, watermark, &mut rows);
            vanished = occupied && chain.is_empty();
        }

        self.forget(id, &rows);
        if vanished {
            self.release(&mut vec![id]);
        }
    }

    /// publishes the empty slots below `next_row_id` as free, after recovery
    pub fn rebuild_free(&self) {
        let end = self.next_row_id.load(Ordering::Relaxed);
        let chunks = self.chunks.read().expect("chunk list poisoned").clone();

        let mut free = Vec::new();
        for (number, chunk) in chunks.iter().enumerate() {
            chunk.visit(|slots| {
                for (offset, chain) in slots.iter().enumerate() {
                    let id = ((number << CHUNK_BITS) + offset) as RowId;
                    if (1..end).contains(&id) && chain.is_empty() {
                        free.push(id);
                    }
                }
            });
        }

        self.release(&mut free);
    }

    /// adds `ids` (slots whose rows vanished, with their index entries gone) to the free list
    fn release(&self, ids: &mut Vec<RowId>) {
        if ids.is_empty() {
            return;
        }

        let mut free = self.free.lock().expect("free list poisoned");
        free.append(ids);
        self.freed.store(free.len(), Ordering::Relaxed);
    }

    fn take_free(&self) -> Option<RowId> {
        if self.freed.load(Ordering::Relaxed) == 0 {
            return None;
        }

        let mut free = self.free.lock().expect("free list poisoned");
        let id = free.pop();
        self.freed.store(free.len(), Ordering::Relaxed);
        id
    }

    /// evicts chunk `number` to its cold file when every row in it is settled at `watermark`
    pub fn evict(&self, number: usize, watermark: u64) -> bool {
        let Some(cold) = &self.cold else {
            return false;
        };

        let chunk = self.chunks.read().expect("chunk list poisoned").get(number).cloned();
        chunk.is_some_and(|chunk| chunk.evict(cold.path(self.id, number), watermark))
    }

    /// `(chunk number, last access)` of every resident chunk, for the eviction policy
    pub fn resident_chunks(&self) -> Vec<(usize, u64)> {
        let chunks = self.chunks.read().expect("chunk list poisoned");
        chunks
            .iter()
            .enumerate()
            .filter(|(_, chunk)| chunk.is_resident())
            .map(|(number, chunk)| (number, chunk.touched()))
            .collect()
    }

    pub fn scan(self: &Arc<Self>, view: View) -> TableScan {
        TableScan {
            table: Arc::clone(self),
            view,
            chunk: 0,
            slot: 0,
            batch: FIRST_BATCH,
        }
    }

    fn chunk(&self, id: RowId) -> Option<Arc<Chunk>> {
        let chunks = self.chunks.read().expect("chunk list poisoned");
        chunks.get((id >> CHUNK_BITS) as usize).cloned()
    }

    fn chunk_or_grow(&self, id: RowId) -> Arc<Chunk> {
        if let Some(chunk) = self.chunk(id) {
            return chunk;
        }

        let number = (id >> CHUNK_BITS) as usize;
        let mut chunks = self.chunks.write().expect("chunk list poisoned");
        while chunks.len() <= number {
            chunks.push(Arc::new(Chunk::new(self.cold.clone())));
        }

        Arc::clone(&chunks[number])
    }
}

impl View {
    #[inline(always)]
    pub const fn sees(&self, stamp: u64) -> bool {
        stamp == self.marker || stamp <= self.snapshot
    }
}

impl TableScan {
    /// appends the visible rows of the next batch of slots to `out`
    /// `false` once the table is exhausted.
    pub fn next_batch(&mut self, out: &mut Vec<(RowId, Row)>) -> bool {
        let chunk = {
            let chunks = self.table.chunks.read().expect("chunk list poisoned");
            match chunks.get(self.chunk) {
                Some(chunk) => Arc::clone(chunk),
                None => return false,
            }
        };

        let base = (self.chunk << CHUNK_BITS) as RowId;
        let start = self.slot;

        // PERFORMANCE: rows are copied out under the latch so writers wait at most one batch
        let end = chunk.visit(|slots| {
            let end = (start + self.batch).min(slots.len());
            for (offset, chain) in slots[start..end].iter().enumerate() {
                if let Some(row) = visible(chain, self.view) {
                    out.push((base + (start + offset) as RowId, Arc::clone(row)));
                }
            }
            (end < slots.len()).then_some(end)
        });

        match end {
            Some(end) => self.slot = end,
            _ => (self.chunk, self.slot) = (self.chunk + 1, 0),
        }
        self.batch = (self.batch * 2).min(CHUNK_SIZE);

        true
    }
}

/// whether `view` sees a row in `chain` that still holds `key` at `column`
#[inline]
fn visible_with(chain: &Chain, view: View, column: usize, key: &Value) -> bool {
    visible(chain, view).is_some_and(|row| row[column] == *key)
}

/// the row `view` sees in `chain`, `none` when absent or deleted
#[inline]
fn visible(chain: &Chain, view: View) -> Option<&Row> {
    chain
        .iter()
        .rev()
        .find(|version| view.sees(version.stamp))
        .and_then(|version| version.row.as_ref())
}

/// prunes `chain` against `watermark` and empties it when all that is left is a deletion every
/// snapshot sees
/// the dropped rows go to `rows`. Returns how many versions went
fn settle(chain: &mut Chain, watermark: u64, rows: &mut Vec<Row>) -> usize {
    let before = chain.len();
    prune(chain, watermark, rows);

    let vanished = chain.len() == 1 && chain[0].row.is_none() && chain[0].stamp <= watermark;
    if vanished {
        *chain = Chain::new();
    }

    before - chain.len()
}

/// drops every version older than the newest committed one at or below `watermark`: no current
/// or future reader can see past it
fn prune(chain: &mut Chain, watermark: u64, pruned: &mut Vec<Row>) {
    let Some(anchor) = chain.iter().rposition(|version| version.stamp <= watermark) else {
        return;
    };

    if anchor == 0 {
        return;
    }

    pruned.extend(chain[..anchor].iter().filter_map(|version| version.row.clone()));

    let mut position = 0;
    chain.retain(|_| {
        position += 1;
        position > anchor
    });
}

#[inline(always)]
const fn slot(id: RowId) -> usize {
    (id & SLOT_MASK) as usize
}

fn gone(level: IsolationLevel) -> EngineError {
    match level {
        IsolationLevel::ReadCommitted => EngineError::Stale(None),
        _ => EngineError::SerializationFailure,
    }
}
