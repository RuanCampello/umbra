//! Transactions: a snapshot, a marker for own writes, and the write set to stamp or undo
//!
//! SERIALIZABLE adds precision locking: the transaction records what it read as predicates, and
//! a writing commit fails when a transaction that committed after its snapshot wrote a row
//! matching one of them
//! Every writing commit leaves its write set behind as a [Summary] for that check, summaries
//! are dropped once no SERIALIZABLE snapshot can predate them

use crate::collections::hash::HashMap;
use crate::sql::Value;
pub(crate) use crate::sql::statement::IsolationLevel;
use crate::storage::engine::{
    Inner,
    error::{EngineError, Result},
    index::Index,
    log::{Kind, Log},
    record,
    table::{Row, RowId, Table, TableScan, View, Writer, Written},
};
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

/// An open transaction, owned by its session
/// Dropping it rolls back
///
/// After any error other than [EngineError::Stale] the transaction is doomed: its writes stay
/// until [Transaction::rollback] (or [Transaction::commit], which then rolls back and fails)
pub(crate) struct Transaction {
    inner: Arc<Inner>,
    level: IsolationLevel,
    view: View,
    writes: Vec<Write>,
    written: HashMap<(u32, RowId), usize>,
    /// Index entries this transaction added: (write, index, key)
    keys: Vec<(usize, Arc<Index>, Value)>,
    /// Read predicates per table id, SERIALIZABLE only
    reads: Option<HashMap<u32, Reads>>,
    /// READ COMMITTED: newest stamps the statement re-checked after [EngineError::Stale]
    rechecked: HashMap<(u32, RowId), u64>,
    state: State,
}

pub(crate) struct Write {
    pub table: Arc<Table>,
    pub id: RowId,
    /// Committed image before this transaction's first write, `None` for inserts
    pub before: Option<Row>,
    /// Image after the last write, `None` for deletions
    pub after: Option<Row>,
}

/// Rows in index order, resolved one batch of index entries at a time so a `LIMIT` stops early
/// each batch resumes strictly after the last entry seen, holding no lock in between
pub(crate) struct IndexCursor {
    table: Arc<Table>,
    index: Arc<Index>,
    view: View,
    start: Bound<Value>,
    end: Bound<Value>,
    descending: bool,
    /// Entries resolved by the next [IndexCursor::next_batch]: doubles up to `BATCH`, so a short
    /// `LIMIT` resolves few rows
    batch: usize,
    last: Option<(Value, RowId)>,
    done: bool,
}

/// Point look-ups into one index under a fixed view
pub(crate) struct IndexProbe {
    table: Arc<Table>,
    index: Arc<Index>,
    view: View,
}

/// A committed write set, kept while a SERIALIZABLE snapshot may predate it
pub(crate) struct Summary {
    pub ts: u64,
    pub writes: Vec<Write>,
}

/// The commit critical section's state
#[derive(Default)]
pub(crate) struct Commits {
    pub last: u64,
    recent: VecDeque<Arc<Summary>>,
    /// Snapshots of active SERIALIZABLE transactions
    serializable: BTreeMap<u64, u32>,
}

#[derive(Default)]
struct Reads {
    all: bool,
    rows: Vec<RowId>,
    ranges: Vec<Range>,
}

struct Range {
    column: usize,
    start: Bound<Value>,
    end: Bound<Value>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum State {
    Active,
    Doomed,
    Finished,
}

impl Transaction {
    pub(super) fn new(inner: Arc<Inner>, level: IsolationLevel) -> Self {
        let (snapshot, reads) = match level {
            IsolationLevel::Serializable => {
                let mut commits = inner.commits();
                let snapshot = inner.clock.snapshot();
                *commits.serializable.entry(snapshot).or_default() += 1;
                (snapshot, Some(HashMap::default()))
            },
            _ => (inner.clock.snapshot(), None),
        };

        let view = View { snapshot, marker: inner.clock.marker() };

        Self {
            inner,
            level,
            view,
            writes: Vec::new(),
            written: HashMap::default(),
            keys: Vec::new(),
            reads,
            rechecked: HashMap::default(),
            state: State::Active,
        }
    }

    #[inline]
    pub fn level(&self) -> IsolationLevel {
        self.level
    }

    pub fn insert(&mut self, table: &Arc<Table>, row: Row) -> Result<RowId> {
        self.check()?;

        let id = table.insert(self.view.marker, Arc::clone(&row));
        let write = self.record(table, id, None, Some(Arc::clone(&row)));
        let indexed = self.index(write, table, id, None, &row);

        self.guard(indexed).map(|_| id)
    }

    pub fn update(&mut self, table: &Arc<Table>, id: RowId, row: Row) -> Result<()> {
        self.write(table, id, Some(row))
    }

    pub fn delete(&mut self, table: &Arc<Table>, id: RowId) -> Result<()> {
        self.write(table, id, None)
    }

    #[cfg(test)]
    pub fn get(&mut self, table: &Arc<Table>, id: RowId) -> Option<Row> {
        if let Some(reads) = self.reads_of(table) {
            reads.rows.push(id);
        }

        table.get(id, self.view)
    }

    pub fn scan(&mut self, table: &Arc<Table>) -> TableScan {
        if let Some(reads) = self.reads_of(table) {
            reads.all = true;
        }

        table.scan(self.view)
    }

    /// a handle for point look-ups into `index` under this transaction's view, for index
    /// nested-loop joins
    /// its keys are not known up front, so SERIALIZABLE records a read of the whole table
    pub fn index_probe(&mut self, table: &Arc<Table>, index: &Arc<Index>) -> IndexProbe {
        if let Some(reads) = self.reads_of(table) {
            reads.all = true;
        }

        IndexProbe {
            table: Arc::clone(table),
            index: Arc::clone(index),
            view: self.view,
        }
    }

    /// a cursor over every visible row in `index` order, resolved a batch at a time
    pub fn index_cursor(
        &mut self,
        table: &Arc<Table>,
        index: &Arc<Index>,
        descending: bool,
    ) -> IndexCursor {
        if let Some(reads) = self.reads_of(table) {
            reads.all = true;
        }

        self.cursor(table, index, (Bound::Unbounded, Bound::Unbounded), descending)
    }

    /// a cursor over the visible rows whose key in `index` lies within the bounds, in key order
    pub fn index_cursor_within(
        &mut self,
        table: &Arc<Table>,
        index: &Arc<Index>,
        (start, end): (Bound<Value>, Bound<Value>),
    ) -> IndexCursor {
        if let Some(reads) = self.reads_of(table) {
            reads.ranges.push(Range {
                column: index.column,
                start: start.clone(),
                end: end.clone(),
            });
        }

        self.cursor(table, index, (start, end), false)
    }

    fn cursor(
        &self,
        table: &Arc<Table>,
        index: &Arc<Index>,
        (start, end): (Bound<Value>, Bound<Value>),
        descending: bool,
    ) -> IndexCursor {
        IndexCursor {
            table: Arc::clone(table),
            index: Arc::clone(index),
            view: self.view,
            start,
            end,
            descending,
            batch: IndexCursor::FIRST_BATCH,
            last: None,
            done: false,
        }
    }

    /// every row [Transaction::index_cursor_within] yields, at once
    #[cfg(test)]
    pub fn index_range(
        &mut self,
        table: &Arc<Table>,
        index: &Arc<Index>,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<(RowId, Row)> {
        let mut cursor = self.index_cursor_within(table, index, (start.cloned(), end.cloned()));
        let mut rows = Vec::new();
        while cursor.next_batch(&mut rows) {}
        rows
    }

    /// moves a READ COMMITTED transaction to a fresh snapshot (start of every statement)
    pub fn refresh_snapshot(&mut self) {
        debug_assert_eq!(self.level, IsolationLevel::ReadCommitted);
        self.rechecked.clear();
        self.view.snapshot = self.inner.clock.refresh(self.view.snapshot);
    }

    pub fn commit(mut self) -> Result<()> {
        match self.state {
            State::Doomed => {
                self.abort();
                return Err(EngineError::Aborted);
            },
            State::Finished => unreachable!("commit consumes the transaction"),
            State::Active => {},
        }

        if self.writes.is_empty() {
            self.finish();
            return Ok(());
        }

        if let Some(write) = self.writes.iter().find(|write| write.table.is_dropped()) {
            let name = write.table.name().to_string();
            self.abort();
            return Err(EngineError::TableNotFound(name));
        }

        // PERFORMANCE: encoded before the critical section, only the timestamp is patched in it
        let mut payload = self.inner.log.as_ref().map(|_| record::encode_commit(&self.writes));

        let (summary, end) = {
            let mut commits = self.inner.commits();

            let failure = match &self.reads {
                _ if self.inner.log.as_ref().is_some_and(Log::is_poisoned) => {
                    Some(EngineError::Poisoned)
                },
                Some(reads) if invalidated(reads, &commits.recent, self.view.snapshot) => {
                    Some(EngineError::SerializationFailure)
                },
                _ => None,
            };

            if let Some(failure) = failure {
                drop(commits);
                self.abort();
                return Err(failure);
            }

            commits.last += 1;
            let end = match (&self.inner.log, &mut payload) {
                (Some(log), Some(payload)) => {
                    record::stamp_timestamp(payload, commits.last);
                    Some(log.append(Kind::Commit, payload))
                },
                _ => None,
            };

            let writes = std::mem::take(&mut self.writes);
            let summary = Arc::new(Summary { ts: commits.last, writes });

            commits.recent.push_back(Arc::clone(&summary));
            commits.trim(self.inner.clock.visible());

            (summary, end)
        };

        if let (Some(log), Some(end)) = (&self.inner.log, end) {
            if let Err(error) = log.wait(end) {
                // the engine is poisoned: nothing is published again, the versions stay flagged
                self.finish();
                return Err(error);
            }
        }

        for write in &summary.writes {
            write.table.stamp(write.id, self.view.marker, summary.ts);
        }

        self.inner.clock.publish(summary.ts, end.unwrap_or(0));
        self.finish();

        Ok(())
    }

    pub fn rollback(mut self) {
        self.abort();
    }

    fn write(&mut self, table: &Arc<Table>, id: RowId, row: Option<Row>) -> Result<()> {
        self.check()?;

        let floor = self
            .rechecked
            .get(&(table.id, id))
            .map_or(self.view.snapshot, |&stamp| stamp.max(self.view.snapshot));

        let watermark = self.inner.clock.watermark();
        let writer = Writer { view: self.view, floor, level: self.level, watermark };

        let mut pruned = Vec::new();
        let written = table.write(id, writer, row.clone(), &mut pruned);
        table.forget(id, &pruned);

        let previous = match self.guard(written)? {
            Written::Done(previous) => previous,
            Written::Stale { stamp, row } => {
                self.rechecked.insert((table.id, id), stamp);
                return Err(EngineError::Stale(row));
            },
        };

        let write = self.record(table, id, previous.clone(), row.clone());
        match &row {
            Some(row) => {
                let indexed = self.index(write, table, id, previous.as_ref(), row);
                self.guard(indexed)
            },
            _ => Ok(()),
        }
    }

    /// notes a write, the first one to a row keeps the committed image it replaced
    fn record(
        &mut self,
        table: &Arc<Table>,
        id: RowId,
        previous: Option<Row>,
        after: Option<Row>,
    ) -> usize {
        match self.written.get(&(table.id, id)) {
            Some(&write) => {
                self.writes[write].after = after;
                write
            },
            _ => {
                let write = Write { table: Arc::clone(table), id, before: previous, after };
                self.writes.push(write);
                self.written.insert((table.id, id), self.writes.len() - 1);
                self.writes.len() - 1
            },
        }
    }

    /// adds the entries of `row`'s keys that `previous` did not already have
    fn index(
        &mut self,
        write: usize,
        table: &Table,
        id: RowId,
        previous: Option<&Row>,
        row: &Row,
    ) -> Result<()> {
        let marker = self.view.marker;

        for index in table.indexes() {
            let key = &row[index.column];
            if previous.is_some_and(|previous| previous[index.column] == *key) {
                continue;
            }

            let free =
                index.insert(key, id, |other| table.holder(other, index.column, key, marker))?;
            self.keys.push((write, index, key.clone()));

            // PERFORMANCE: settled here, a key deleted and inserted again keeps one live entry
            for dead in free {
                table.settle_slot(dead, self.inner.clock.watermark());
            }
        }

        Ok(())
    }

    #[inline]
    fn reads_of(&mut self, table: &Table) -> Option<&mut Reads> {
        self.reads.as_mut().map(|reads| reads.entry(table.id).or_default())
    }

    #[inline]
    fn check(&self) -> Result<()> {
        match self.state {
            State::Active => Ok(()),
            _ => Err(EngineError::Aborted),
        }
    }

    #[inline]
    fn guard<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.state = State::Doomed;
        }

        result
    }

    fn abort(&mut self) {
        self.writes
            .iter()
            .for_each(|write| write.table.unlink(write.id, self.view.marker));

        self.keys.drain(..).for_each(|(write, index, key)| {
            let Write { table, id, .. } = &self.writes[write];
            index.remove_if(&key, *id, || !table.carries(*id, index.column, &key));
        });

        self.finish();
    }

    fn finish(&mut self) {
        if self.reads.is_some() {
            let mut commits = self.inner.commits();
            let count = commits
                .serializable
                .get_mut(&self.view.snapshot)
                .expect("serializable snapshot registered at begin");

            *count -= 1;
            if *count == 0 {
                commits.serializable.remove(&self.view.snapshot);
            }
        }

        self.inner.clock.release(self.view.snapshot);
        self.state = State::Finished;
    }
}

impl IndexCursor {
    /// Index entries examined per batch
    const BATCH: usize = 256;
    /// Entries resolved by a cursor's first batch
    const FIRST_BATCH: usize = 16;

    /// moves past up to `n` visible rows without copying them out
    /// returns how many it passed
    pub fn skip(&mut self, n: usize) -> usize {
        let mut skipped = 0;
        while skipped < n && !self.done {
            let mut entries = Vec::with_capacity(Self::BATCH);
            self.index.page(
                (self.start.as_ref(), self.end.as_ref()),
                self.last.as_ref(),
                self.descending,
                Self::BATCH,
                |key, id| entries.push((key.clone(), id)),
            );

            let (visible, examined) =
                self.table.count_keyed(&entries, self.view, self.index.column, n - skipped);
            skipped += visible;
            self.done = entries.len() < Self::BATCH && examined == entries.len();
            if let Some(last) = examined.checked_sub(1) {
                self.last = Some(entries.swap_remove(last));
            }
        }

        skipped
    }

    /// appends the visible rows of the next batch of entries to `out`
    /// `false` once exhausted
    pub fn next_batch(&mut self, out: &mut Vec<(RowId, Row)>) -> bool {
        if self.done {
            return false;
        }

        let limit = self.batch;
        self.batch = (self.batch * 2).min(Self::BATCH);
        let mut entries = Vec::with_capacity(limit);
        self.index.page(
            (self.start.as_ref(), self.end.as_ref()),
            self.last.as_ref(),
            self.descending,
            limit,
            |key, id| entries.push((key.clone(), id)),
        );

        self.table.resolve_keyed(&entries, self.view, self.index.column, out);

        self.done = entries.len() < limit;
        let seen = !entries.is_empty();
        if let Some(last) = entries.pop() {
            self.last = Some(last);
        }
        seen
    }
}

impl IndexProbe {
    /// appends the visible rows whose indexed column equals `key` to `out`
    pub fn find(&self, key: &Value, out: &mut Vec<(RowId, Row)>) {
        let (table, view, column) = (&self.table, self.view, self.index.column);
        self.index.visit(Bound::Included(key), Bound::Included(key), |key, id| {
            if let Some(row) = table.get_keyed(id, view, column, key) {
                out.push((id, row));
            }
        });
    }
}

impl Commits {
    pub fn new(last: u64) -> Self {
        Self { last, ..Default::default() }
    }

    #[cfg(test)]
    pub fn recent_len(&self) -> usize {
        self.recent.len()
    }

    /// drops summaries no SERIALIZABLE snapshot (current or future) can predate
    pub fn trim(&mut self, visible: u64) {
        let horizon = self
            .serializable
            .first_key_value()
            .map_or(visible, |(&oldest, _)| oldest.min(visible));

        while self.recent.front().is_some_and(|summary| summary.ts <= horizon) {
            self.recent.pop_front();
        }
    }
}

impl Reads {
    fn matches(&self, write: &Write) -> bool {
        self.all
            || self.rows.contains(&write.id)
            || self.ranges.iter().any(|range| {
                let hit = |row: &Option<Row>| row.as_ref().is_some_and(|row| range.contains(row));
                hit(&write.before) || hit(&write.after)
            })
    }
}

impl Range {
    #[inline]
    fn contains(&self, row: &Row) -> bool {
        (self.start.as_ref(), self.end.as_ref()).contains(&row[self.column])
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.state != State::Finished {
            self.abort();
        }
    }
}

/// whether a commit after `snapshot` wrote something the predicates in `reads` cover
fn invalidated(
    reads: &HashMap<u32, Reads>,
    recent: &VecDeque<Arc<Summary>>,
    snapshot: u64,
) -> bool {
    recent
        .iter()
        .filter(|summary| summary.ts > snapshot)
        .flat_map(|summary| &summary.writes)
        .any(|write| reads.get(&write.table.id).is_some_and(|reads| reads.matches(write)))
}
