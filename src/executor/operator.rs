//! Operator trait and plan operators for the executor.
//!
//! Each operator is a pull-based iterator that produces tuples one at a time,
//! following the Volcano model. Operators compose: one operator's output
//! feeds another's input via the [`Operator`] trait.
//!
//! Operators are the **read** pipeline only — DML mutations are handled
//! by [`dml`](super::dml) which drains a source operator and calls the
//! engine directly.

use crate::db::{DatabaseError, Schema};
use crate::sql::statement::Expression;
use crate::sql::Value;
use crate::storage::mvcc::engine::Engine;
use crate::storage::mvcc::version::VersionStorage;
use crate::vm::expression::evaluate_where;
use crate::vm::planner::Tuple;
use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

/// Scans all MVCC-visible rows from a table for a given transaction.
///
/// Yields tuples with the row id prepended as column 0, mirroring the
/// `row_id`-first layout the rest of the pipeline expects, schemas for
/// downstream operators must be built with [Schema::prepend_id]
pub(crate) struct Scan {
    mode: ScanMode,
}

/// Filters tuples from a source operator using a `WHERE` clause expression.
pub(crate) struct Filter {
    source: Box<dyn Operator>,
    schema: Schema,
    predicate: Expression,
}

/// Projects specific columns from a source operator.
pub(crate) struct Project {
    source: Box<dyn Operator>,
    /// Column indices to keep from each input tuple.
    indices: Vec<usize>,
}

/// Computes one output column per expression for every source tuple
///
/// Used for projections that are not plain column references (functions,
/// arithmetic, aliases) and for hidden sort-key columns
pub(crate) struct Evaluate {
    source: Box<dyn Operator>,
    schema: Schema,
    expressions: Vec<Expression>,
}

/// Applies `LIMIT` and `OFFSET` to a source operator.
pub(crate) struct Limit {
    source: Box<dyn Operator>,
    remaining: usize,
    offset: usize,
    skipped: usize,
}

/// Sorts its input, spilling sorted runs to disk once the in-memory budget is
/// exceeded so an arbitrarily large `ORDER BY` need not fit in memory.
///
/// - `ORDER BY ... LIMIT k` keeps only the k smallest rows (plus any offset) in a
///   bounded top-N buffer, O(k) memory, no spill.
/// - a small unbounded `ORDER BY` sorts once in memory.
/// - a large unbounded `ORDER BY` runs an external merge sort: sorted runs
///   spill to temporary files and are k-way merged on demand
pub(crate) struct Sort {
    comparator: Comparator,
    output: SortOutput,
}

/// A spilled, pre-sorted run being drained during the k-way merge
struct RunCursor {
    reader: BufReader<File>,
    path: PathBuf,
}

/// The current head of one run in the merge heap, tagged with its run's index
struct HeapItem {
    tuple: Tuple,
    run: usize,
}

/// Where a [Sort] draws its ordered output from once the input is consumed
enum SortOutput {
    /// Everything fit the budget (or a `LIMIT` bounded it): sorted in place
    Memory { sorted: Vec<Tuple>, cursor: usize },
    /// Input outgrew the budget: min-heap merge over the (bounded) final runs
    Merge {
        cursors: Vec<RunCursor>,
        heap: Vec<HeapItem>,
    },
}

/// Yields pre-built tuples one at a time.
pub(crate) struct Values {
    tuples: Vec<Tuple>,
    cursor: usize,
}

/// The two shapes a [Scan] can take
enum ScanMode {
    /// Rows already materialised: read-your-own-writes or cold segments forced
    /// the eager merge up front
    Eager { tuples: Vec<Tuple>, cursor: usize },
    /// Row ids resolved a batch at a time against the version store, so
    /// `LIMIT`/`Filter` short-circuit before most rows are cloned
    Lazy {
        storage: Arc<VersionStorage>,
        txn_id: i64,
        ids: std::vec::IntoIter<i64>,
        buffer: Vec<Tuple>,
        cursor: usize,
    },
}

/// Pull-based iterator over tuples.
///
/// Every node in the execution tree implements this trait.
/// The executor calls [`Operator::next`] repeatedly until it returns `Ok(None)`.
pub(crate) trait Operator {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}

/// Rows resolved per refill of a [ScanMode::Lazy] batch
const SCAN_CHUNK: usize = 256;

/// In-memory budget a [Sort] buffers before spilling a sorted run to disk
const SORT_RUN_BUDGET: usize = 8 << 20;

/// Maximum runs merged at once, bounding open files (and heap size) during the
/// merge, more runs than this are reduced first by bounded multi-pass merging
const MERGE_FANIN: usize = 16;

type Comparator = Box<dyn Fn(&Tuple, &Tuple) -> Ordering>;

impl Scan {
    /// materialises every visible row up front
    pub fn new(engine: &Engine, txn_id: i64, table: &str) -> Result<Self, DatabaseError> {
        Self::eager(engine, txn_id, table)
    }

    /// resolves rows a batch at a time so a downstream `LIMIT`/`Filter` can
    /// short-circuit before most rows are cloned
    pub fn streaming(engine: &Engine, txn_id: i64, table: &str) -> Result<Self, DatabaseError> {
        match engine.scan_lazy(txn_id, table)? {
            Some((storage, ids)) => Ok(Self {
                mode: ScanMode::Lazy {
                    storage,
                    txn_id,
                    ids: ids.into_iter(),
                    buffer: Vec::new(),
                    cursor: 0,
                },
            }),
            None => Self::eager(engine, txn_id, table),
        }
    }

    /// Resolves the given row ids in order, a batch at a time
    /// Used to serve an `ORDER BY` from an index-ordered id list, skipping the sort
    pub fn from_ordered_ids(storage: Arc<VersionStorage>, txn_id: i64, ids: Vec<i64>) -> Self {
        Self {
            mode: ScanMode::Lazy {
                storage,
                txn_id,
                ids: ids.into_iter(),
                buffer: Vec::new(),
                cursor: 0,
            },
        }
    }

    fn eager(engine: &Engine, txn_id: i64, table: &str) -> Result<Self, DatabaseError> {
        let tuples = engine
            .scan(txn_id, table)?
            .into_iter()
            .map(|(row_id, tuple)| prepend_id(row_id, tuple))
            .collect();

        Ok(Self {
            mode: ScanMode::Eager { tuples, cursor: 0 },
        })
    }
}

impl Operator for Scan {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        match &mut self.mode {
            ScanMode::Eager { tuples, cursor } => {
                if *cursor >= tuples.len() {
                    return Ok(None);
                }

                let tuple = std::mem::take(&mut tuples[*cursor]);
                *cursor += 1;

                Ok(Some(tuple))
            }

            ScanMode::Lazy {
                storage,
                txn_id,
                ids,
                buffer,
                cursor,
            } => loop {
                if *cursor < buffer.len() {
                    let tuple = std::mem::take(&mut buffer[*cursor]);
                    *cursor += 1;
                    return Ok(Some(tuple));
                }

                let batch: Vec<_> = ids.by_ref().take(SCAN_CHUNK).collect();
                if batch.is_empty() {
                    return Ok(None);
                }

                let mut resolved = Vec::with_capacity(batch.len());
                storage.resolve_visible_chunk(&batch, *txn_id, &mut resolved);

                buffer.clear();
                buffer.extend(
                    resolved
                        .into_iter()
                        .map(|(row_id, tuple)| prepend_id(row_id, tuple)),
                );
                *cursor = 0;
            },
        }
    }
}

impl Filter {
    pub fn new(source: Box<dyn Operator>, schema: Schema, predicate: Expression) -> Self {
        Self {
            source,
            schema,
            predicate,
        }
    }
}

impl Operator for Filter {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        while let Some(tuple) = self.source.next()? {
            if evaluate_where(&self.schema, &tuple, &self.predicate)? {
                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }
}

impl Project {
    pub fn new(source: Box<dyn Operator>, indices: Vec<usize>) -> Self {
        Self { source, indices }
    }
}

impl Operator for Project {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(tuple) = self.source.next()? else {
            return Ok(None);
        };

        Ok(Some(
            self.indices.iter().map(|&idx| tuple[idx].clone()).collect(),
        ))
    }
}

impl Evaluate {
    pub fn new(source: Box<dyn Operator>, schema: Schema, expressions: Vec<Expression>) -> Self {
        Self {
            source,
            schema,
            expressions,
        }
    }
}

impl Operator for Evaluate {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        use crate::vm::expression::resolve_expression;

        let Some(tuple) = self.source.next()? else {
            return Ok(None);
        };

        let evaluated = self
            .expressions
            .iter()
            .map(|expr| resolve_expression(&tuple, &self.schema, expr))
            .collect::<Result<Tuple, _>>()?;

        Ok(Some(evaluated))
    }
}

impl Limit {
    pub fn new(source: Box<dyn Operator>, limit: usize, offset: usize) -> Self {
        Self {
            source,
            remaining: limit,
            offset,
            skipped: 0,
        }
    }
}

impl Operator for Limit {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        while self.skipped < self.offset {
            if self.source.next()?.is_none() {
                return Ok(None);
            }

            self.skipped += 1;
        }

        if self.remaining == 0 {
            return Ok(None);
        }

        let tuple = self.source.next()?;
        if tuple.is_some() {
            self.remaining -= 1;
        }

        Ok(tuple)
    }
}

impl Sort {
    /// `retain` bounds the output to the smallest `n` rows (a `LIMIT` plus its
    /// offset), `None` sorts the whole input
    pub fn new(
        source: Box<dyn Operator>,
        comparator: Comparator,
        retain: Option<usize>,
        dir: &Path,
    ) -> Result<Self, DatabaseError> {
        match retain {
            Some(cap) => Self::top_n(source, comparator, cap),
            None => Self::full(source, comparator, SORT_RUN_BUDGET, MERGE_FANIN, dir),
        }
    }

    fn top_n(
        mut source: Box<dyn Operator>,
        comparator: Comparator,
        cap: usize,
    ) -> Result<Self, DatabaseError> {
        let mut sorted = Vec::new();
        if cap > 0 {
            let trim_at = cap.saturating_mul(2);
            while let Some(tuple) = source.next()? {
                sorted.push(tuple);
                if sorted.len() >= trim_at {
                    sorted.select_nth_unstable_by(cap - 1, |a, b| comparator(a, b));
                    sorted.truncate(cap);
                }
            }
            sorted.sort_by(|a, b| comparator(a, b));
            sorted.truncate(cap);
        }

        Ok(Self {
            comparator,
            output: SortOutput::Memory { sorted, cursor: 0 },
        })
    }

    fn full(
        mut source: Box<dyn Operator>,
        comparator: Comparator,
        budget: usize,
        fanin: usize,
        dir: &Path,
    ) -> Result<Self, DatabaseError> {
        let mut run = Vec::new();
        let mut run_bytes = 0;
        let mut spills = Vec::new();

        while let Some(tuple) = source.next()? {
            run_bytes += tuple_bytes(&tuple);
            run.push(tuple);
            if run_bytes >= budget {
                run.sort_unstable_by(|a, b| comparator(a, b));
                spills.push(spill_run(&run, dir)?);
                run.clear();
                run_bytes = 0;
            }
        }

        if spills.is_empty() {
            run.sort_unstable_by(|a, b| comparator(a, b));
            return Ok(Self {
                comparator,
                output: SortOutput::Memory {
                    sorted: run,
                    cursor: 0,
                },
            });
        }

        if !run.is_empty() {
            run.sort_unstable_by(|a, b| comparator(a, b));
            spills.push(spill_run(&run, dir)?);
        }

        // reduce to at most `fanin` runs so the final merge opens a bounded
        // number of files and never lets the heap outgrow the fan-in
        let compare = &*comparator;
        while spills.len() > fanin {
            let mut merged = Vec::with_capacity(spills.len().div_ceil(fanin));
            let mut runs = spills.into_iter();
            loop {
                let group: Vec<PathBuf> = runs.by_ref().take(fanin).collect();
                if group.is_empty() {
                    break;
                }
                merged.push(merge_group(group, compare, dir)?);
            }
            spills = merged;
        }

        let mut cursors = spills
            .into_iter()
            .map(RunCursor::open)
            .collect::<Result<Vec<_>, _>>()?;
        let mut heap = Vec::with_capacity(cursors.len());
        for (run, cursor) in cursors.iter_mut().enumerate() {
            if let Some(tuple) = cursor.next_tuple()? {
                heap_push(&mut heap, HeapItem { tuple, run }, compare);
            }
        }

        Ok(Self {
            comparator,
            output: SortOutput::Merge { cursors, heap },
        })
    }
}

impl Operator for Sort {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Self { comparator, output } = self;
        match output {
            SortOutput::Memory { sorted, cursor } => {
                if *cursor >= sorted.len() {
                    return Ok(None);
                }

                let tuple = std::mem::take(&mut sorted[*cursor]);
                *cursor += 1;

                Ok(Some(tuple))
            }

            SortOutput::Merge { cursors, heap } => {
                let compare = &**comparator;
                let Some(item) = heap_pop(heap, compare) else {
                    return Ok(None);
                };

                if let Some(tuple) = cursors[item.run].next_tuple()? {
                    heap_push(
                        heap,
                        HeapItem {
                            tuple,
                            run: item.run,
                        },
                        compare,
                    );
                }

                Ok(Some(item.tuple))
            }
        }
    }
}

impl RunCursor {
    fn open(path: PathBuf) -> Result<Self, DatabaseError> {
        let reader = BufReader::new(File::open(&path).map_err(DatabaseError::Io)?);
        Ok(Self { reader, path })
    }

    fn next_tuple(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let mut len_bytes = [0u8; 4];
        match self.reader.read_exact(&mut len_bytes) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(DatabaseError::Io(err)),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        self.reader
            .read_exact(&mut payload)
            .map_err(DatabaseError::Io)?;

        let mut tuple = Vec::new();
        let mut offset = 0;
        while offset < len {
            let (value, consumed) =
                Value::deserialise(&payload[offset..]).map_err(DatabaseError::Io)?;
            tuple.push(value);
            offset += consumed;
        }

        Ok(Some(tuple))
    }
}

impl Drop for RunCursor {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

impl Values {
    pub fn new(tuples: Vec<Tuple>) -> Self {
        Self { tuples, cursor: 0 }
    }
}

impl Operator for Values {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.cursor >= self.tuples.len() {
            return Ok(None);
        }

        let tuple = std::mem::take(&mut self.tuples[self.cursor]);
        self.cursor += 1;

        Ok(Some(tuple))
    }
}

#[inline]
fn prepend_id(row_id: i64, tuple: Tuple) -> Tuple {
    let mut row = Vec::with_capacity(tuple.len() + 1);
    row.push(Value::Number(row_id as i128));
    row.extend(tuple);
    row
}

fn tuple_bytes(tuple: &[Value]) -> usize {
    tuple.iter().map(Value::serialised_size_hint).sum()
}

fn spill_path(dir: &Path) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, AtomicOrdering::Relaxed);
    dir.join(format!("umbra-sort-{}-{seq}.run", std::process::id()))
}

fn write_tuple(
    writer: &mut impl Write,
    tuple: &[Value],
    payload: &mut Vec<u8>,
) -> Result<(), DatabaseError> {
    payload.clear();
    for value in tuple {
        payload.extend_from_slice(&value.serialise().map_err(DatabaseError::Io)?);
    }
    writer
        .write_all(&(payload.len() as u32).to_le_bytes())
        .map_err(DatabaseError::Io)?;
    writer.write_all(payload).map_err(DatabaseError::Io)?;

    Ok(())
}

/// Writes a sorted run to a fresh spill file, one length-prefixed tuple at a
/// time, and returns its path for the merge step to drain.
fn spill_run(run: &[Tuple], dir: &Path) -> Result<PathBuf, DatabaseError> {
    let path = spill_path(dir);
    let mut writer = BufWriter::new(File::create(&path).map_err(DatabaseError::Io)?);
    let mut payload = Vec::new();
    for tuple in run {
        write_tuple(&mut writer, tuple, &mut payload)?;
    }
    writer.flush().map_err(DatabaseError::Io)?;

    Ok(path)
}

/// Merges a bounded group of sorted runs into one new run file, deleting the
/// inputs as their cursors drop. Keeps the fan-in (open files, heap size)
/// capped when there are more runs than [MERGE_FANIN].
fn merge_group(
    paths: Vec<PathBuf>,
    compare: &dyn Fn(&Tuple, &Tuple) -> Ordering,
    dir: &Path,
) -> Result<PathBuf, DatabaseError> {
    let mut cursors = paths
        .into_iter()
        .map(RunCursor::open)
        .collect::<Result<Vec<_>, _>>()?;
    let mut heap = Vec::with_capacity(cursors.len());
    for (run, cursor) in cursors.iter_mut().enumerate() {
        if let Some(tuple) = cursor.next_tuple()? {
            heap_push(&mut heap, HeapItem { tuple, run }, compare);
        }
    }

    let path = spill_path(dir);
    let mut writer = BufWriter::new(File::create(&path).map_err(DatabaseError::Io)?);
    let mut payload = Vec::new();
    while let Some(item) = heap_pop(&mut heap, compare) {
        write_tuple(&mut writer, &item.tuple, &mut payload)?;
        if let Some(tuple) = cursors[item.run].next_tuple()? {
            let run = item.run;
            heap_push(&mut heap, HeapItem { tuple, run }, compare);
        }
    }
    writer.flush().map_err(DatabaseError::Io)?;

    Ok(path)
}

fn heap_push(
    heap: &mut Vec<HeapItem>,
    item: HeapItem,
    compare: &dyn Fn(&Tuple, &Tuple) -> Ordering,
) {
    heap.push(item);
    let mut child = heap.len() - 1;
    while child > 0 {
        let parent = (child - 1) / 2;
        if compare(&heap[child].tuple, &heap[parent].tuple) != Ordering::Less {
            break;
        }
        heap.swap(child, parent);
        child = parent;
    }
}

fn heap_pop(
    heap: &mut Vec<HeapItem>,
    compare: &dyn Fn(&Tuple, &Tuple) -> Ordering,
) -> Option<HeapItem> {
    if heap.is_empty() {
        return None;
    }

    let last = heap.len() - 1;
    heap.swap(0, last);
    let item = heap.pop();

    let len = heap.len();
    let mut parent = 0;
    loop {
        let (left, right) = (2 * parent + 1, 2 * parent + 2);
        let mut smallest = parent;
        if left < len && compare(&heap[left].tuple, &heap[smallest].tuple) == Ordering::Less {
            smallest = left;
        }
        if right < len && compare(&heap[right].tuple, &heap[smallest].tuple) == Ordering::Less {
            smallest = right;
        }
        if smallest == parent {
            break;
        }
        heap.swap(parent, smallest);
        parent = smallest;
    }

    item
}

#[cfg(test)]
mod tests {
    use super::*;

    fn asc() -> Comparator {
        Box::new(|a: &Tuple, b: &Tuple| a[0].partial_cmp(&b[0]).unwrap())
    }

    fn dir() -> PathBuf {
        std::env::temp_dir()
    }

    fn rows(values: &[i64]) -> Vec<Tuple> {
        values
            .iter()
            .map(|&n| vec![Value::Number(n as i128)])
            .collect()
    }

    fn drain(mut sort: Sort) -> Vec<i64> {
        let mut out = Vec::new();
        while let Some(tuple) = sort.next().unwrap() {
            match &tuple[0] {
                Value::Number(n) => out.push(*n as i64),
                other => panic!("expected a number, got {other:?}"),
            }
        }
        out
    }

    #[test]
    fn external_merge_sorts_every_row() {
        let input = [9, 3, 7, 1, 8, 2, 6, 0, 5, 4];
        // a zero budget spills one tuple per run, forcing the heap merge across
        // as many runs as there are rows
        let sort = Sort::full(
            Box::new(Values::new(rows(&input))),
            asc(),
            0,
            MERGE_FANIN,
            &dir(),
        )
        .unwrap();
        assert_eq!(drain(sort), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn external_merge_reduces_runs_over_the_fan_in() {
        let input = [9, 3, 7, 1, 8, 2, 6, 0, 5, 4, 12, 11, 10, 15, 13, 14];
        // fan-in 2 with one run per row forces several bounded multi-pass merges
        let sort = Sort::full(Box::new(Values::new(rows(&input))), asc(), 0, 2, &dir()).unwrap();
        assert_eq!(drain(sort), (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn in_memory_and_external_agree_on_duplicates() {
        let input = [5, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5];
        let memory = Sort::full(
            Box::new(Values::new(rows(&input))),
            asc(),
            1 << 20,
            MERGE_FANIN,
            &dir(),
        )
        .unwrap();
        let external =
            Sort::full(Box::new(Values::new(rows(&input))), asc(), 0, 2, &dir()).unwrap();
        assert_eq!(drain(memory), drain(external));
    }

    #[test]
    fn top_n_keeps_the_smallest_rows_in_order() {
        let input = [9, 3, 7, 1, 8, 2, 6, 0, 5, 4];
        let sort = Sort::new(Box::new(Values::new(rows(&input))), asc(), Some(3), &dir()).unwrap();
        assert_eq!(drain(sort), vec![0, 1, 2]);
    }

    #[test]
    fn top_n_of_zero_yields_nothing() {
        let sort = Sort::new(
            Box::new(Values::new(rows(&[3, 1, 2]))),
            asc(),
            Some(0),
            &dir(),
        )
        .unwrap();
        assert!(drain(sort).is_empty());
    }
}
