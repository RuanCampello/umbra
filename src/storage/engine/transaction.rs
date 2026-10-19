//! Transactions: a snapshot, a marker for own writes, and the write set to stamp or undo
//!
//! SERIALIZABLE adds precision locking: the transaction records what it read as predicates, and
//! a writing commit fails when a transaction that committed after its snapshot wrote a row
//! matching one of them
//! Every writing commit leaves its write set behind as a [Summary] for that check, summaries
//! are dropped once no SERIALIZABLE snapshot can predate them

use crate::collections::hash::HashMap;
use crate::db::Schema;
use crate::sql::Value;
pub(crate) use crate::sql::statement::IsolationLevel;
use crate::storage::engine::{
    Inner,
    clock::Registration,
    duplicated_key,
    error::{EngineError, Result},
    index::{Index, Key},
    log::{Kind, Log, Unlogged},
    record::{self, Ddl},
    table::{Row, RowId, Table, TableScan, View, Writer, Written},
};
use std::collections::{BTreeMap, VecDeque};
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// An open transaction, owned by its session
/// Dropping it rolls back
///
/// After any error other than [EngineError::Stale] the transaction is doomed: its writes stay
/// until [Transaction::rollback] (or [Transaction::commit], which then rolls back and fails)
pub(crate) struct Transaction {
    inner: Arc<Inner>,
    level: IsolationLevel,
    view: View,
    /// `view.snapshot` in the clock's registry
    registration: Registration,
    writes: Vec<Write>,
    written: HashMap<(u32, RowId), usize>,
    /// The distinct tables of `writes`, so commit checks drops per table, not per row
    tables: Vec<Arc<Table>>,
    /// Index entries this transaction added: (write, index, key)
    keys: Vec<(usize, Arc<Index>, Key)>,
    /// Catalogue changes in the order they ran, with the table each touched
    ddl: Vec<(Ddl, Arc<Table>)>,
    /// Open savepoints, oldest first
    savepoints: Vec<Savepoint>,
    /// Foreign-key checks to validate at commit
    guards: Vec<Guard>,
    /// Snapshots registered by [Transaction::constraint_view], so commits after them stay summarised
    validating: Vec<u64>,
    /// Read predicates per table id, SERIALIZABLE only
    reads: Option<HashMap<u32, Reads>>,
    /// READ COMMITTED: newest stamps the statement re-checked after [EngineError::Stale]
    rechecked: HashMap<(u32, RowId), u64>,
    state: State,
}

/// A foreign-key check that a commit after its snapshot can invalidate: with `present`, a row of
/// `table` held `key` in `column` and must keep holding it (the referenced row)
/// otherwise none did and none may come to (rows referring to a deleted key)
struct Guard {
    snapshot: u64,
    table: u32,
    column: usize,
    key: Value,
    present: bool,
}

/// How far the transaction had got when a savepoint was taken
struct Savepoint {
    name: String,
    writes: usize,
    keys: usize,
    ddl: usize,
    tables: usize,
    /// Image each earlier write had when the savepoint was taken, recorded when it is first written again
    overwritten: HashMap<usize, Option<Row>>,
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
    start: Bound<Key>,
    end: Bound<Key>,
    descending: bool,
    /// Entries resolved by the next [IndexCursor::next_batch]: doubles up to `BATCH`, so a short
    /// `LIMIT` resolves few rows
    batch: usize,
    last: Option<(Key, RowId)>,
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
    /// Snapshots whose later commits must stay summarised: active SERIALIZABLE transactions and
    /// foreign-key checks
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
        let (registration, reads) = match level {
            IsolationLevel::Serializable => {
                let mut commits = inner.commits();
                let registration = inner.clock.snapshot();
                *commits.serializable.entry(registration.ts).or_default() += 1;
                (registration, Some(HashMap::default()))
            },
            _ => (inner.clock.snapshot(), None),
        };

        let view = View { snapshot: registration.ts, marker: inner.clock.marker() };

        Self {
            inner,
            level,
            view,
            registration,
            writes: Vec::new(),
            written: HashMap::default(),
            tables: Vec::new(),
            keys: Vec::new(),
            ddl: Vec::new(),
            savepoints: Vec::new(),
            guards: Vec::new(),
            validating: Vec::new(),
            reads,
            rechecked: HashMap::default(),
            state: State::Active,
        }
    }

    #[inline]
    pub fn level(&self) -> IsolationLevel {
        self.level
    }

    /// the view table names resolve against: this transaction's snapshot with its own catalogue
    /// changes
    #[inline]
    pub fn catalogue_view(&self) -> View {
        self.view
    }

    /// the table `name` denotes for this transaction
    pub fn table(&self, name: &str) -> Result<Arc<Table>> {
        self.inner.resolve(name, self.view)
    }

    /// every table this transaction sees
    pub fn tables(&self) -> Vec<Arc<Table>> {
        self.inner.visible_tables(self.view)
    }

    pub fn create_table(&mut self, name: &str, schema: Schema) -> Result<Arc<Table>> {
        self.check()?;

        let created = self.inner.create_table(name, schema, self.view, self.level);
        let table = self.guard(created)?;
        let ddl = Ddl::CreateTable {
            table: table.id,
            name: name.into(),
            schema: table.schema.clone(),
        };
        self.ddl.push((ddl, Arc::clone(&table)));

        Ok(table)
    }

    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        self.check()?;

        let dropped = self.inner.drop_table(name, self.view, self.level);
        let table = self.guard(dropped)?;
        self.ddl.push((Ddl::DropTable { table: table.id }, table));

        Ok(())
    }

    /// a view of the newest committed state with this transaction's writes, for foreign-key checks
    /// registered so the commits after it stay summarised until this transaction ends
    pub fn constraint_view(&mut self) -> View {
        let mut commits = self.inner.commits();
        let snapshot = self.inner.clock.visible();
        *commits.serializable.entry(snapshot).or_default() += 1;
        self.validating.push(snapshot);

        View { snapshot, marker: self.view.marker }
    }

    /// whether a row of `table` holds `key` in `column` under `view`
    /// that row must still hold it when this transaction commits
    pub fn referenced(
        &mut self,
        view: View,
        table: &Arc<Table>,
        column: usize,
        key: &Value,
    ) -> Result<bool> {
        self.guard_key(view, table, column, key, true);
        Ok(!self.holding(view, table, column, key)?.is_empty())
    }

    /// the rows of `table` holding `key` in `column` under `view`
    /// no other row may come to hold it before this transaction commits
    pub fn referencing(
        &mut self,
        view: View,
        table: &Arc<Table>,
        column: usize,
        key: &Value,
    ) -> Result<Vec<(RowId, Row)>> {
        self.guard_key(view, table, column, key, false);
        self.holding(view, table, column, key)
    }

    fn guard_key(&mut self, view: View, table: &Table, column: usize, key: &Value, present: bool) {
        let guard = Guard {
            snapshot: view.snapshot,
            table: table.id,
            column,
            key: key.clone(),
            present,
        };
        self.guards.push(guard);
    }

    fn holding(
        &self,
        view: View,
        table: &Arc<Table>,
        column: usize,
        key: &Value,
    ) -> Result<Vec<(RowId, Row)>> {
        let mut rows = Vec::new();
        match table.index_on(column) {
            Some(index) => {
                let probe = IndexProbe { table: Arc::clone(table), index, view };
                probe.find(&Key::One(key.clone()), &mut rows)?
            },
            _ => {
                let mut scan = table.scan(view);
                while scan.next_batch(&mut rows)? {}
                rows.retain(|(_, row)| row[column] == *key);
            },
        }
        Ok(rows)
    }

    pub fn savepoint(&mut self, name: &str) -> Result<()> {
        self.check()?;

        Ok(self.savepoints.push(Savepoint {
            name: name.to_string(),
            writes: self.writes.len(),
            keys: self.keys.len(),
            ddl: self.ddl.len(),
            tables: self.tables.len(),
            overwritten: HashMap::default(),
        }))
    }

    /// undoes everything since the newest savepoint called `name`, which stays open, and revives a
    /// doomed transaction
    pub fn rollback_to(&mut self, name: &str) -> Result<()> {
        let position = self
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)
            .ok_or_else(|| EngineError::NoSavepoint(name.to_string()))?;
        self.savepoints.truncate(position + 1);

        let savepoint = self.savepoints.last_mut().expect("savepoint just found");
        let overwritten = std::mem::take(&mut savepoint.overwritten);
        let (writes, keys, ddl, tables) =
            (savepoint.writes, savepoint.keys, savepoint.ddl, savepoint.tables);
        let marker = self.view.marker;

        self.writes[writes..]
            .iter()
            .for_each(|write| write.table.unlink(write.id, marker));
        for (position, after) in overwritten {
            let write = &mut self.writes[position];
            write.table.restore(write.id, marker, after.clone());
            write.after = after;
        }

        for (write, index, key) in self.keys.drain(keys..) {
            let Write { table, id, .. } = &self.writes[write];
            index.remove_if(&key, *id, || !table.carries(*id, &index, &key));
        }

        self.undo_ddl(ddl);
        for write in self.writes.drain(writes..) {
            self.written.remove(&(write.table.id, write.id));
        }
        self.tables.truncate(tables);
        Ok(self.state = State::Active)
    }

    /// closes the newest savepoint called `name` and every later one, keeping their changes
    pub fn release(&mut self, name: &str) -> Result<()> {
        self.check()?;

        let position = self
            .savepoints
            .iter()
            .rposition(|savepoint| savepoint.name == name)
            .ok_or_else(|| EngineError::NoSavepoint(name.to_string()))?;

        Ok(self.savepoints.truncate(position))
    }

    /// registers the index at once, so concurrent writers maintain it, then back-fills the key of every version
    /// a unique index is refused when two rows' newest versions share a key
    pub fn create_index(
        &mut self,
        name: &str,
        table: &str,
        columns: &[&str],
        unique: bool,
    ) -> Result<()> {
        self.check()?;

        let created = self.index_table(name, table, columns, unique);
        let (table, columns) = self.guard(created)?;
        let ddl = Ddl::CreateIndex { table: table.id, columns, unique, name: name.into() };

        Ok(self.ddl.push((ddl, table)))
    }

    pub fn insert(&mut self, table: &Arc<Table>, row: Row) -> Result<RowId> {
        self.check()?;

        let id = table.insert(self.view.marker, Arc::clone(&row))?;
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

    /// a handle for point look-ups into `index` under this transaction's view, for index nested-loop joins
    /// its keys are not known up front, so SERIALIZABLE records a read of the whole table
    pub fn index_probe(&mut self, table: &Arc<Table>, index: &Arc<Index>) -> IndexProbe {
        if let Some(reads) = self.reads_of(table) {
            reads.all = true;
        }

        let (table, index) = (Arc::clone(table), Arc::clone(index));
        IndexProbe { table, index, view: self.view }
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
    /// (a bound shorter than the index's key bounds its leading columns)
    /// SERIALIZABLE records the range of the leading column the bounds lie in
    pub fn index_cursor_within(
        &mut self,
        table: &Arc<Table>,
        index: &Arc<Index>,
        (start, end): (Bound<Key>, Bound<Key>),
        descending: bool,
    ) -> IndexCursor {
        if let Some(reads) = self.reads_of(table) {
            let leading = |bound: &Bound<Key>| match bound {
                Bound::Excluded(key) if key.values().len() == 1 => {
                    Bound::Excluded(key.values()[0].clone())
                },
                Bound::Included(key) | Bound::Excluded(key) => {
                    Bound::Included(key.values()[0].clone())
                },
                Bound::Unbounded => Bound::Unbounded,
            };
            reads.ranges.push(Range {
                column: index.leading(),
                start: leading(&start),
                end: leading(&end),
            });
        }

        self.cursor(table, index, (start, end), descending)
    }

    fn cursor(
        &self,
        table: &Arc<Table>,
        index: &Arc<Index>,
        (start, end): (Bound<Key>, Bound<Key>),
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
        let key = |bound: Bound<&Value>| bound.cloned().map(Key::One);
        let mut cursor = self.index_cursor_within(table, index, (key(start), key(end)), false);
        let mut rows = Vec::new();
        while cursor.next_batch(&mut rows).expect("test chunks are readable") {}
        rows
    }

    /// moves a READ COMMITTED transaction to a fresh snapshot (start of every statement)
    pub fn refresh_snapshot(&mut self) {
        debug_assert_eq!(self.level, IsolationLevel::ReadCommitted);
        self.rechecked.clear();
        self.registration = self.inner.clock.refresh(self.registration);
        self.view.snapshot = self.registration.ts;
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

        if self.writes.is_empty() && self.ddl.is_empty() {
            self.finish();
            return Ok(());
        }

        // PERFORMANCE: encoded before the critical section, only the timestamp is patched in it
        let mut payload = self
            .inner
            .log
            .as_ref()
            .map(|_| record::encode_commit(self.ddl.iter().map(|(ddl, _)| ddl), &self.writes));

        let (summary, ticket) = {
            let mut commits = self.inner.commits();

            // under the commit lock, ordered with `DROP TABLE` (which marks inside it)
            let dropped = self.tables.iter().find(|table| table.is_dropped());
            let poisoned = self.inner.log.as_ref().and_then(Log::poisoned);
            let failure = match (poisoned, &self.reads, dropped) {
                (Some(poisoned), _, _) => Some(poisoned),
                (_, _, Some(table)) => Some(EngineError::TableNotFound(table.name().to_string())),
                (_, Some(reads), _) if invalidated(reads, &commits.recent, self.view.snapshot) => {
                    Some(EngineError::SerializationFailure)
                },
                _ if broken(&self.guards, &commits.recent) => {
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
            self.stamp_ddl(commits.last);
            let ticket = match (&self.inner.log, &mut payload) {
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

            (summary, ticket)
        };

        if let (Some(log), Some(ticket)) = (&self.inner.log, ticket) {
            match log.wait(ticket) {
                Ok(_) => {},
                // nothing is published again, the versions stay flagged
                Err(Unlogged::Poisoned(error)) => {
                    self.finish();
                    return Err(error);
                },
                // visibility advances over a gap-free prefix of timestamps: this one becomes
                // visible as an empty commit
                Err(Unlogged::Discarded { error, to }) => {
                    self.inner.commits().recent.retain(|recent| recent.ts != summary.ts);
                    self.undo(&summary.writes);
                    self.inner.clock.publish(summary.ts, to);
                    return Err(error);
                },
            }
        }

        for write in &summary.writes {
            write.table.stamp(write.id, self.view.marker, summary.ts);
        }

        self.inner.clock.publish(summary.ts, ticket.map_or(0, |ticket| ticket.end));
        if !self.ddl.is_empty() {
            self.inner.epoch.fetch_add(1, Ordering::Release);
        }
        Ok(self.finish())
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

        let watermark = match self.writes.is_empty() {
            true => self.inner.clock.settle_watermark(),
            _ => self.inner.clock.watermark(),
        };
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
                let previous = std::mem::replace(&mut self.writes[write].after, after);
                for savepoint in
                    self.savepoints.iter_mut().filter(|savepoint| write < savepoint.writes)
                {
                    savepoint.overwritten.entry(write).or_insert_with(|| previous.clone());
                }
                write
            },
            _ => {
                if self.tables.iter().all(|written| written.id != table.id) {
                    self.tables.push(Arc::clone(table));
                }
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
            let key = index.key(row);
            if previous.is_some_and(|previous| index.holds(previous, &key)) {
                continue;
            }

            let free = index.insert(&key, id, |other| table.holder(other, &index, &key, marker))?;
            self.keys.push((write, index, key));

            // PERFORMANCE: settled here, a key deleted and inserted again keeps one live entry
            if !free.is_empty() {
                let watermark = self.inner.clock.settle_watermark();
                for dead in free {
                    table.settle_slot(dead, watermark);
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn index_table(
        &self,
        name: &str,
        table: &str,
        columns: &[&str],
        unique: bool,
    ) -> Result<(Arc<Table>, Box<[usize]>)> {
        let table = self.table(table)?;
        let columns = columns
            .iter()
            .map(|&column| {
                table
                    .schema
                    .index_of(column)
                    .ok_or_else(|| EngineError::ColumnNotFound(column.to_string()))
            })
            .collect::<Result<Box<[usize]>>>()?;

        let index = Arc::new(Index::new(name, columns.clone(), unique));
        index.stamp_created(self.view.marker);
        if !table.try_add_index(Arc::clone(&index)) {
            return Err(EngineError::IndexExists(name.to_string()));
        }
        table.for_each_version(|id, row| index.insert_unchecked(index.key(row), id))?;

        match unique.then(|| duplicated_key(&table, &index)).transpose()?.flatten() {
            Some(key) => {
                table.remove_index(name);
                Err(EngineError::DuplicatedKey(key.to_value()))
            },
            _ => Ok((table, columns)),
        }
    }

    /// stamps the catalogue changes with the commit timestamp, inside the commit critical
    /// section: a commit ordered after a drop sees it (R1)
    fn stamp_ddl(&self, ts: u64) {
        for (ddl, table) in &self.ddl {
            match ddl {
                Ddl::CreateTable { .. } => table.stamp_created(ts),
                Ddl::DropTable { .. } => table.stamp_dropped(ts),
                Ddl::CreateIndex { name, .. } => {
                    table.index(name).expect("created index is registered").stamp_created(ts);
                },
            }
        }
    }

    /// reverts the catalogue changes from the `from`-th on, newest first
    fn undo_ddl(&mut self, from: usize) {
        for (ddl, table) in self.ddl.drain(from..).rev() {
            match ddl {
                Ddl::CreateTable { .. } => self.inner.forget_table(&table),
                Ddl::DropTable { .. } => table.stamp_dropped(0),
                Ddl::CreateIndex { name, .. } => table.remove_index(&name),
            }
        }
    }

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
        // a stale row changed nothing: the statement re-checks it or skips it
        if result.as_ref().is_err_and(|error| !matches!(error, EngineError::Stale(_))) {
            self.state = State::Doomed;
        }

        result
    }

    fn abort(&mut self) {
        let writes = std::mem::take(&mut self.writes);
        self.undo(&writes);
    }

    /// removes `writes` (this transaction's, in order), the index entries they added and the
    /// catalogue changes, then finishes
    fn undo(&mut self, writes: &[Write]) {
        writes.iter().for_each(|write| write.table.unlink(write.id, self.view.marker));

        self.keys.drain(..).for_each(|(write, index, key)| {
            let Write { table, id, .. } = &writes[write];
            index.remove_if(&key, *id, || !table.carries(*id, &index, &key));
        });

        self.undo_ddl(0);
        self.finish();
    }

    fn finish(&mut self) {
        let serializable = self.reads.is_some().then_some(self.view.snapshot);
        if serializable.is_some() || !self.validating.is_empty() {
            let mut commits = self.inner.commits();
            for snapshot in serializable.into_iter().chain(self.validating.drain(..)) {
                let count = commits
                    .serializable
                    .get_mut(&snapshot)
                    .expect("validating snapshot registered");

                *count -= 1;
                if *count == 0 {
                    commits.serializable.remove(&snapshot);
                }
            }
        }

        self.inner.clock.release(self.registration);
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
    pub fn skip(&mut self, n: usize) -> Result<usize> {
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
                self.table.count_keyed(&entries, self.view, &self.index, n - skipped)?;
            skipped += visible;
            self.done = entries.len() < Self::BATCH && examined == entries.len();
            if let Some(last) = examined.checked_sub(1) {
                self.last = Some(entries.swap_remove(last));
            }
        }

        Ok(skipped)
    }

    /// appends the visible rows of the next batch of entries to `out`
    /// `false` once exhausted
    pub fn next_batch(&mut self, out: &mut Vec<(RowId, Row)>) -> Result<bool> {
        if self.done {
            return Ok(false);
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

        self.table.resolve_keyed(&entries, self.view, &self.index, out)?;

        self.done = entries.len() < limit;
        let seen = !entries.is_empty();
        if let Some(last) = entries.pop() {
            self.last = Some(last);
        }
        Ok(seen)
    }
}

impl IndexProbe {
    /// appends the visible rows holding `key` (or beginning with it) in the index to `out`
    pub fn find(&self, key: &Key, out: &mut Vec<(RowId, Row)>) -> Result<()> {
        let mut ids = Vec::new();
        self.index
            .visit(Bound::Included(key), Bound::Included(key), |_, id| ids.push(id));

        Ok(for id in ids {
            if let Some(row) = self.table.get_keyed(id, self.view, &self.index, key)? {
                out.push((id, row));
            }
        })
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
/// whether a commit after a guard's snapshot removed a key it needs present, or added one it needs
/// absent
fn broken(guards: &[Guard], recent: &VecDeque<Arc<Summary>>) -> bool {
    guards.iter().any(|guard| {
        let holds =
            |row: &Option<Row>| row.as_ref().is_some_and(|row| row[guard.column] == guard.key);
        recent
            .iter()
            .filter(|summary| summary.ts > guard.snapshot)
            .flat_map(|summary| &summary.writes)
            .filter(|write| write.table.id == guard.table)
            .any(|write| match guard.present {
                true => holds(&write.before) && !holds(&write.after),
                _ => holds(&write.after),
            })
    })
}

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
