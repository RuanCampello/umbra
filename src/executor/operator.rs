//! Operator trait and plan operators for the executor
//!
//! Each operator is a pull-based iterator that produces tuples one at a time,
//! following the Volcano model
//! Operators compose: one operator's output feeds another's input via the [Operator] trait
//!
//! Operators are the **read** pipeline only, DML mutations are handled
//! by [dml](super::dml) which drains a source operator and calls the
//! engine directly

use crate::collections::hash::{self, HashMap};
use crate::db::{DatabaseError, Schema};
use crate::executor::Tuple;
use crate::sql::Value;
use crate::sql::statement::{Expression, Type};
use crate::storage::engine::table::{Row, RowId, TableScan};
use crate::storage::engine::transaction::{IndexCursor, IndexProbe};
use crate::vm::expression::{self, bind, evaluate_where};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// Streams the rows a transaction sees, a batch at a time from a table scan or from index cursors
/// (in key order), so a downstream `LIMIT`/`Filter` stops the scan early
pub(crate) struct Scan {
    rows: Rows,
    batch: Vec<(RowId, Row)>,
    cursor: usize,
    /// user-column indices to keep (projection pushdown)
    projection: Option<Vec<usize>>,
    /// predicate over the stored row and the schema of stored rows (no row id)
    filter: Option<(Schema, Expression)>,
}

/// Filters tuples from a source operator using a `WHERE` clause expression
pub(crate) struct Filter {
    source: Box<dyn Operator>,
    schema: Schema,
    predicate: Expression,
}

/// Projects specific columns from a source operator
pub(crate) struct Project {
    source: Box<dyn Operator>,
    /// Column indices to keep from each input tuple
    indices: Vec<usize>,
    /// Strictly increasing indices: tuples are compacted in place instead of copied
    ascending: bool,
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

/// Prefixes every tuple with the values of `keys` evaluated on it, so a [Sort] can order rows by
/// computed group keys
pub(crate) struct Keyed {
    source: Box<dyn Operator>,
    schema: Schema,
    keys: Vec<Expression>,
}

/// Equi-join on `left[left_key] = right[right_key]`
pub(crate) struct HashJoin {
    spec: JoinSpec,
    pending: VecDeque<Part>,
    active: Option<Probe>,
    output: VecDeque<Tuple>,
}

/// What a [HashJoin] joins: the outer sides, the key positions and type, the row widths
pub(crate) struct JoinSpec {
    pub outer_left: bool,
    pub outer_right: bool,
    pub key_type: Type,
    pub left_key: usize,
    pub right_key: usize,
    pub left_width: usize,
    pub right_width: usize,
}

/// One partition of a [HashJoin] being joined
struct Probe {
    left: Box<dyn Operator>,
    right: Vec<Tuple>,
    table: HashMap<Vec<u8>, Vec<usize>>,
    matched: Vec<bool>,
    left_done: bool,
}

/// Equi-join probing an index of the right table for every left tuple, in left order, so a
/// downstream `LIMIT` stops after the rows it needs
pub(crate) struct IndexJoin {
    left: Box<dyn Operator>,
    probe: IndexProbe,
    left_key: usize,
    /// The right table's columns the join emits (projection pushdown)
    right_columns: Vec<usize>,
    outer_left: bool,
    filter: Option<(Schema, Expression)>,
    matches: Vec<(RowId, Row)>,
    output: VecDeque<Tuple>,
}

/// Streams the tuples of a spill file
struct Spilled {
    cursor: RunCursor,
}

/// Applies `LIMIT` and `OFFSET` to a source operator
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

/// Sorts input already ordered on its first `presorted` keys: each run of tuples with equal leading
/// keys goes through a [Sort] on all keys (spilling, and top-N within what is still `wanted`), so a
/// downstream `LIMIT` stops before later runs are read (incremental sort, PostgreSQL 13)
pub(crate) struct IncrementalSort {
    source: Box<dyn Operator>,
    keys: Vec<(usize, bool)>,
    presorted: usize,
    wanted: Option<usize>,
    scratch: PathBuf,
    /// The first tuple of the next run, read while finding the end of the current one
    pending: Option<Tuple>,
    run: Option<Sort>,
}

/// One run of an [IncrementalSort]'s input: tuples while their leading keys equal `leading`
struct Run<'a> {
    source: &'a mut dyn Operator,
    positions: &'a [(usize, bool)],
    leading: Tuple,
    first: Option<Tuple>,
    pending: &'a mut Option<Tuple>,
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

/// Where a [Scan] reads rows from
enum Rows {
    Table(TableScan),
    /// Read one after another: one cursor per range or point of an index look-up
    Index(VecDeque<IndexCursor>),
}

/// Where a [Sort] draws its ordered output from once the input is consumed
enum SortOutput {
    /// Everything fit the budget (or a `LIMIT` bounded it): sorted in place
    Memory { sorted: Vec<Tuple>, cursor: usize },
    /// Input outgrew the budget: min-heap merge over the (bounded) final runs
    Merge { cursors: Vec<RunCursor>, heap: Vec<HeapItem> },
}

/// A [HashJoin] partition: in memory, or both sides spilled
enum Part {
    Memory(Box<dyn Operator>, Vec<Tuple>),
    Spilled { left: PathBuf, right: PathBuf },
}

/// Yields pre-built tuples one at a time
pub(crate) struct Values {
    tuples: Vec<Tuple>,
    cursor: usize,
}

/// Pull-based iterator over tuples
///
/// Every node in the execution tree implements this trait
/// The executor calls [Operator::next] repeatedly until it returns `Ok(None)`
pub(crate) trait Operator {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError>;

    /// discards up to `n` tuples, returning how many there were
    fn skip(&mut self, n: usize) -> Result<usize, DatabaseError> {
        let mut skipped = 0;
        while skipped < n && self.next()?.is_some() {
            skipped += 1;
        }
        Ok(skipped)
    }
}

/// in-memory budget a [Sort] buffers before spilling a sorted run to disk
const SORT_RUN_BUDGET: usize = 8 << 20;

/// build-side bytes a [HashJoin] hashes in memory before partitioning to disk
const JOIN_BUDGET: usize = 8 << 20;

/// partitions a spilling [HashJoin] splits each side into
const JOIN_PARTITIONS: usize = 16;

/// maximum runs merged at once, bounding open files (and heap size) during the
/// merge, more runs than this are reduced first by bounded multi-pass merging
const MERGE_FANIN: usize = 16;

pub(crate) type Comparator = Box<dyn Fn(&Tuple, &Tuple) -> Ordering>;

impl Scan {
    pub fn new(scan: TableScan, projection: Option<Vec<usize>>) -> Self {
        Self::from_rows(Rows::Table(scan), projection)
    }

    /// over the rows of `cursors`, read in turn
    pub fn indexed(cursors: VecDeque<IndexCursor>, projection: Option<Vec<usize>>) -> Self {
        Self::from_rows(Rows::Index(cursors), projection)
    }

    fn from_rows(rows: Rows, projection: Option<Vec<usize>>) -> Self {
        Self { rows, batch: Vec::new(), cursor: 0, projection, filter: None }
    }

    /// keeps only rows satisfying `predicate`, whose columns must all be columns of `stored`
    pub fn filtered(mut self, stored: Schema, mut predicate: Expression) -> Self {
        bind(&mut predicate, &stored);
        self.filter = Some((stored, predicate));
        self
    }

    /// whether a row is at the cursor, fetching the next chunk once the batch is spent
    fn ready(&mut self) -> bool {
        while self.cursor == self.batch.len() {
            self.batch.clear();
            self.cursor = 0;
            if !self.rows.next_batch(&mut self.batch) {
                return false;
            }
        }

        true
    }
}

impl Operator for Scan {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        while self.ready() {
            let (row_id, row) = &self.batch[self.cursor];
            self.cursor += 1;

            match &self.filter {
                Some((schema, predicate)) if !evaluate_where(schema, row, predicate)? => continue,
                _ => return Ok(Some(project_row(*row_id, row, self.projection.as_deref()))),
            }
        }

        Ok(None)
    }

    fn skip(&mut self, n: usize) -> Result<usize, DatabaseError> {
        if self.filter.is_some() {
            let mut skipped = 0;
            while skipped < n && self.next()?.is_some() {
                skipped += 1;
            }
            return Ok(skipped);
        }

        let mut skipped = (self.batch.len() - self.cursor).min(n);
        self.cursor += skipped;

        match &mut self.rows {
            // PERFORMANCE: index cursors count visible rows without copying them out
            Rows::Index(cursors) => {
                while let Some(cursor) = cursors.front_mut().filter(|_| skipped < n) {
                    skipped += cursor.skip(n - skipped);
                    if skipped < n {
                        cursors.pop_front();
                    }
                }
            },
            Rows::Table(_) => {
                while skipped < n && self.ready() {
                    let step = (n - skipped).min(self.batch.len() - self.cursor);
                    self.cursor += step;
                    skipped += step;
                }
            },
        }

        Ok(skipped)
    }
}

impl Filter {
    pub fn new(source: Box<dyn Operator>, schema: Schema, mut predicate: Expression) -> Self {
        bind(&mut predicate, &schema);
        Self { source, schema, predicate }
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

impl Keyed {
    pub fn new(source: Box<dyn Operator>, schema: Schema, mut keys: Vec<Expression>) -> Self {
        keys.iter_mut().for_each(|key| bind(key, &schema));
        Self { source, schema, keys }
    }
}

impl Operator for Keyed {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(tuple) = self.source.next()? else {
            return Ok(None);
        };

        let mut keyed = Vec::with_capacity(self.keys.len() + tuple.len());
        for key in &self.keys {
            keyed.push(expression::resolve_expression(&tuple, &self.schema, key)?);
        }
        keyed.extend(tuple);

        Ok(Some(keyed))
    }
}

impl HashJoin {
    pub fn new(
        spec: JoinSpec,
        left: Box<dyn Operator>,
        mut right: Box<dyn Operator>,
        scratch: &Path,
    ) -> Result<Self, DatabaseError> {
        let mut buffered = Vec::new();
        let mut bytes = 0;

        while bytes <= JOIN_BUDGET {
            match right.next()? {
                Some(tuple) => {
                    bytes += tuple_bytes(&tuple);
                    buffered.push(tuple);
                },
                _ => {
                    let pending = VecDeque::from([Part::Memory(left, buffered)]);
                    return Ok(Self::with(spec, pending));
                },
            }
        }

        let rights = partition(
            &spec,
            spec.right_key,
            buffered.into_iter().map(Ok).chain(drain(right)),
            scratch,
        )?;
        let lefts = partition(&spec, spec.left_key, drain(left), scratch)?;
        let pending = lefts
            .into_iter()
            .zip(rights)
            .map(|(left, right)| Part::Spilled { left, right })
            .collect();

        Ok(Self::with(spec, pending))
    }

    fn with(spec: JoinSpec, pending: VecDeque<Part>) -> Self {
        Self { spec, pending, active: None, output: VecDeque::new() }
    }

    fn start(&self, part: Part) -> Result<Probe, DatabaseError> {
        let (left, right): (Box<dyn Operator>, Vec<_>) = match part {
            Part::Memory(left, right) => (left, right),
            Part::Spilled { left, right } => {
                let mut cursor = RunCursor::open(right)?;
                let mut rows = Vec::new();
                while let Some(tuple) = cursor.next_tuple()? {
                    rows.push(tuple);
                }
                (Box::new(Spilled { cursor: RunCursor::open(left)? }), rows)
            },
        };

        let mut table: HashMap<Vec<_>, Vec<_>> = HashMap::default();
        for (position, row) in right.iter().enumerate() {
            if let Some(key) = join_key(self.spec.key_type, &row[self.spec.right_key]) {
                table.entry(key).or_default().push(position);
            }
        }

        Ok(Probe {
            left,
            matched: vec![false; right.len()],
            right,
            table,
            left_done: false,
        })
    }
}

impl Operator for HashJoin {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        loop {
            if let Some(tuple) = self.output.pop_front() {
                return Ok(Some(tuple));
            }

            if self.active.is_none() {
                match self.pending.pop_front() {
                    Some(part) => self.active = Some(self.start(part)?),
                    None => return Ok(None),
                }
            }

            let spec = &self.spec;
            let probe = self.active.as_mut().expect("a partition is active");

            if probe.left_done {
                if spec.outer_right {
                    for (row, _) in probe.right.iter().zip(&probe.matched).filter(|(_, m)| !**m) {
                        let mut combined = vec![Value::Null; spec.left_width];
                        combined.extend(row.iter().cloned());
                        self.output.push_back(combined);
                    }
                }
                self.active = None;
                continue;
            }

            let Some(left) = probe.left.next()? else {
                probe.left_done = true;
                continue;
            };

            let key = join_key(spec.key_type, &left[spec.left_key]);
            match key.as_ref().and_then(|key| probe.table.get(key)) {
                Some(positions) => {
                    for &position in positions {
                        probe.matched[position] = true;
                        let mut combined = Vec::with_capacity(spec.left_width + spec.right_width);
                        combined.extend(left.iter().cloned());
                        combined.extend(probe.right[position].iter().cloned());
                        self.output.push_back(combined);
                    }
                },
                None if spec.outer_left => {
                    let mut combined = left;
                    combined.extend(std::iter::repeat_n(Value::Null, spec.right_width));
                    self.output.push_back(combined);
                },
                _ => {},
            }
        }
    }
}

impl IndexJoin {
    pub fn new(
        left: Box<dyn Operator>,
        probe: IndexProbe,
        left_key: usize,
        right_columns: Vec<usize>,
        outer_left: bool,
        mut filter: Option<(Schema, Expression)>,
    ) -> Self {
        if let Some((schema, predicate)) = &mut filter {
            bind(predicate, schema);
        }

        Self {
            left,
            probe,
            left_key,
            right_columns,
            outer_left,
            filter,
            matches: Vec::new(),
            output: VecDeque::new(),
        }
    }
}

impl Operator for IndexJoin {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        loop {
            if let Some(tuple) = self.output.pop_front() {
                return Ok(Some(tuple));
            }

            let Some(left) = self.left.next()? else {
                return Ok(None);
            };

            self.matches.clear();
            let key = &left[self.left_key];
            if !key.is_null() {
                self.probe.find(key, &mut self.matches);
            }

            for (_, row) in self.matches.drain(..) {
                if let Some((schema, predicate)) = &self.filter {
                    if !evaluate_where(schema, &row, predicate)? {
                        continue;
                    }
                }

                let mut combined = Vec::with_capacity(left.len() + self.right_columns.len());
                combined.extend(left.iter().cloned());
                combined.extend(self.right_columns.iter().map(|&column| row[column].clone()));
                self.output.push_back(combined);
            }

            if self.output.is_empty() && self.outer_left {
                let mut combined = left;
                combined.extend(std::iter::repeat_n(Value::Null, self.right_columns.len()));
                return Ok(Some(combined));
            }
        }
    }
}

impl Operator for Spilled {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        self.cursor.next_tuple()
    }
}

impl Project {
    pub fn new(source: Box<dyn Operator>, indices: Vec<usize>) -> Self {
        let ascending = indices.windows(2).all(|pair| pair[0] < pair[1]);
        Self { source, indices, ascending }
    }
}

impl IncrementalSort {
    pub fn new(
        source: Box<dyn Operator>,
        keys: Vec<(usize, bool)>,
        presorted: usize,
        wanted: Option<usize>,
        scratch: &Path,
    ) -> Self {
        assert!(presorted > 0 && presorted < keys.len(), "an incremental sort sorts a suffix");
        Self {
            source,
            keys,
            presorted,
            wanted,
            scratch: scratch.to_path_buf(),
            pending: None,
            run: None,
        }
    }
}

impl Operator for IncrementalSort {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        loop {
            if let Some(tuple) = self.run.as_mut().map(Sort::next).transpose()?.flatten() {
                if let Some(wanted) = &mut self.wanted {
                    *wanted -= 1;
                }
                return Ok(Some(tuple));
            }

            if self.wanted == Some(0) {
                return Ok(None);
            }

            let first = match self.pending.take() {
                Some(tuple) => tuple,
                _ => match self.source.next()? {
                    Some(tuple) => tuple,
                    _ => return Ok(None),
                },
            };

            let positions = &self.keys[..self.presorted];
            let leading = positions.iter().map(|(p, _)| first[*p].clone()).collect();
            let mut run = Run {
                source: &mut *self.source,
                positions,
                leading,
                first: Some(first),
                pending: &mut self.pending,
            };
            let order = comparator_for(self.keys.clone());
            self.run = Some(Sort::new(&mut run, order, self.wanted, &self.scratch)?);
        }
    }
}

impl Operator for Run<'_> {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if let Some(first) = self.first.take() {
            return Ok(Some(first));
        }

        let Some(tuple) = self.source.next()? else {
            return Ok(None);
        };
        match self.positions.iter().zip(&self.leading).all(|((p, _), v)| tuple[*p] == *v) {
            true => Ok(Some(tuple)),
            _ => {
                *self.pending = Some(tuple);
                Ok(None)
            },
        }
    }
}

impl Rows {
    fn next_batch(&mut self, out: &mut Vec<(RowId, Row)>) -> bool {
        match self {
            Rows::Table(scan) => scan.next_batch(out),
            Rows::Index(cursors) => loop {
                let Some(cursor) = cursors.front_mut() else {
                    return false;
                };
                if cursor.next_batch(out) {
                    return true;
                }
                cursors.pop_front();
            },
        }
    }
}

impl Operator for Project {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut tuple) = self.source.next()? else {
            return Ok(None);
        };

        if !self.ascending {
            return Ok(Some(self.indices.iter().map(|&idx| tuple[idx].clone()).collect()));
        }

        // PERFORMANCE: `indices[k] >= k`, so each kept value moves left over slots already spent
        for (slot, &index) in self.indices.iter().enumerate() {
            tuple.swap(slot, index);
        }
        tuple.truncate(self.indices.len());

        Ok(Some(tuple))
    }
    fn skip(&mut self, n: usize) -> Result<usize, DatabaseError> {
        self.source.skip(n)
    }
}

impl Evaluate {
    pub fn new(
        source: Box<dyn Operator>,
        schema: Schema,
        mut expressions: Vec<Expression>,
    ) -> Self {
        expressions.iter_mut().for_each(|expr| bind(expr, &schema));
        Self { source, schema, expressions }
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
        Self { source, remaining: limit, offset, skipped: 0 }
    }
}

impl Operator for Limit {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.skipped < self.offset {
            let wanted = self.offset - self.skipped;
            let skipped = self.source.skip(wanted)?;
            self.skipped += skipped;

            if skipped < wanted {
                return Ok(None);
            }
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
    /// consumes `source`
    /// `retain` bounds the output to the smallest `n` rows (a `LIMIT` plus its offset), `None` sorts the whole input
    pub fn new(
        source: &mut dyn Operator,
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
        source: &mut dyn Operator,
        comparator: Comparator,
        cap: usize,
    ) -> Result<Self, DatabaseError> {
        let mut sorted = Vec::new();
        if cap > 0 {
            let trim_at = cap.saturating_mul(2);
            // the largest row kept by the last trim; nothing that does not sort before it can enter
            let mut bound: Option<Tuple> = None;
            while let Some(tuple) = source.next()? {
                if bound.as_ref().is_some_and(|bound| comparator(&tuple, bound) != Ordering::Less) {
                    continue;
                }

                sorted.push(tuple);
                if sorted.len() >= trim_at {
                    sorted.select_nth_unstable_by(cap - 1, |a, b| comparator(a, b));
                    sorted.truncate(cap);
                    bound = Some(sorted[cap - 1].clone());
                }
            }
            sorted.sort_by(|a, b| comparator(a, b));
            sorted.truncate(cap);
        }

        Ok(Self { comparator, output: SortOutput::Memory { sorted, cursor: 0 } })
    }

    fn full(
        source: &mut dyn Operator,
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
            let output = SortOutput::Memory { sorted: run, cursor: 0 };
            return Ok(Self { comparator, output });
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
                let group: Vec<_> = runs.by_ref().take(fanin).collect();
                if group.is_empty() {
                    break;
                }
                merged.push(merge_group(group, compare, dir)?);
            }
            spills = merged;
        }

        let mut cursors = spills.into_iter().map(RunCursor::open).collect::<Result<Vec<_>, _>>()?;
        let mut heap = Vec::with_capacity(cursors.len());
        for (run, cursor) in cursors.iter_mut().enumerate() {
            if let Some(tuple) = cursor.next_tuple()? {
                heap_push(&mut heap, HeapItem { tuple, run }, compare);
            }
        }

        Ok(Self { comparator, output: SortOutput::Merge { cursors, heap } })
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
            },

            SortOutput::Merge { cursors, heap } => {
                let compare = &**comparator;
                let Some(item) = heap_pop(heap, compare) else {
                    return Ok(None);
                };

                if let Some(tuple) = cursors[item.run].next_tuple()? {
                    heap_push(heap, HeapItem { tuple, run: item.run }, compare);
                }

                Ok(Some(item.tuple))
            },
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
            Ok(()) => {},
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(DatabaseError::Io(err)),
        }

        let len = u32::from_le_bytes(len_bytes) as usize;
        let mut payload = vec![0u8; len];
        self.reader.read_exact(&mut payload).map_err(DatabaseError::Io)?;

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

    fn skip(&mut self, n: usize) -> Result<usize, DatabaseError> {
        let step = n.min(self.tuples.len() - self.cursor);
        self.cursor += step;
        Ok(step)
    }
}

/// the tuple a scan yields for a stored row: the projected columns, or the row id then the row
#[inline]
pub(crate) fn project_row(row_id: RowId, row: &Row, projection: Option<&[usize]>) -> Tuple {
    match projection {
        Some(columns) => columns.iter().map(|&i| row[i].clone()).collect(),
        _ => {
            let mut tuple = Vec::with_capacity(row.len() + 1);
            tuple.push(Value::Number(row_id as i128));
            tuple.extend(row.iter().cloned());
            tuple
        },
    }
}

/// the bytes a join key is hashed and compared by, encoded under the key column's type so values of different representations still meet
fn join_key(key_type: Type, value: &Value) -> Option<Vec<u8>> {
    match value.is_null() {
        true => None,
        false => Some(crate::storage::tuple::serialize(&key_type, value)),
    }
}

pub(crate) fn comparator_for(sort_keys: Vec<(usize, bool)>) -> Comparator {
    Box::new(move |a: &Tuple, b: &Tuple| {
        for &(idx, desc) in &sort_keys {
            let ord = a[idx].partial_cmp(&b[idx]).unwrap_or(Ordering::Equal);
            if ord != Ordering::Equal {
                return match desc {
                    true => ord.reverse(),
                    false => ord,
                };
            }
        }

        Ordering::Equal
    })
}

/// every tuple an operator still yields, as an iterator
fn drain(mut source: Box<dyn Operator>) -> impl Iterator<Item = Result<Tuple, DatabaseError>> {
    std::iter::from_fn(move || source.next().transpose())
}

/// writes `rows` into [JOIN_PARTITIONS] spill files by the hash of their key column
fn partition(
    spec: &JoinSpec,
    key: usize,
    rows: impl Iterator<Item = Result<Tuple, DatabaseError>>,
    dir: &Path,
) -> Result<Vec<PathBuf>, DatabaseError> {
    use std::hash::{Hash, Hasher};

    let paths: Vec<_> = (0..JOIN_PARTITIONS).map(|_| spill_path(dir)).collect();
    let mut writers = paths
        .iter()
        .map(|path| File::create(path).map(BufWriter::new).map_err(DatabaseError::Io))
        .collect::<Result<Vec<_>, _>>()?;

    let mut payload = Vec::new();
    for row in rows {
        let row = row?;
        let target = match join_key(spec.key_type, &row[key]) {
            None => 0,
            Some(bytes) => {
                let mut hasher = hash::Hasher::default();
                bytes.hash(&mut hasher);
                hasher.finish() as usize % JOIN_PARTITIONS
            },
        };
        write_tuple(&mut writers[target], &row, &mut payload)?;
    }

    for writer in &mut writers {
        writer.flush().map_err(DatabaseError::Io)?;
    }

    Ok(paths)
}

pub(crate) fn tuple_bytes(tuple: &[Value]) -> usize {
    tuple.iter().map(Value::serialised_size_hint).sum()
}

/// A fresh spill file name in `dir`, which is created on the first spill rather than per query.
fn spill_path(dir: &Path) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let _ = fs::create_dir_all(dir);
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

/// writes a sorted run to a fresh spill file, one length-prefixed tuple at a
/// time, and returns its path for the merge step to drain
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

/// merges a bounded group of sorted runs into one new run file, deleting the
/// inputs as their cursors drop
/// keeps the fan-in (open files, heap size) capped when there are more runs than [MERGE_FANIN]
fn merge_group(
    paths: Vec<PathBuf>,
    compare: &dyn Fn(&Tuple, &Tuple) -> Ordering,
    dir: &Path,
) -> Result<PathBuf, DatabaseError> {
    let mut cursors = paths.into_iter().map(RunCursor::open).collect::<Result<Vec<_>, _>>()?;
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
        values.iter().map(|&n| vec![Value::Number(n as i128)]).collect()
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
        let sort =
            Sort::full(&mut Values::new(rows(&input)), asc(), 0, MERGE_FANIN, &dir()).unwrap();
        assert_eq!(drain(sort), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn external_merge_reduces_runs_over_the_fan_in() {
        let input = [9, 3, 7, 1, 8, 2, 6, 0, 5, 4, 12, 11, 10, 15, 13, 14];
        // fan-in 2 with one run per row forces several bounded multi-pass merges
        let sort = Sort::full(&mut Values::new(rows(&input)), asc(), 0, 2, &dir()).unwrap();
        assert_eq!(drain(sort), (0..16).collect::<Vec<_>>());
    }

    #[test]
    fn in_memory_and_external_agree_on_duplicates() {
        let input = [5, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5];
        let memory =
            Sort::full(&mut Values::new(rows(&input)), asc(), 1 << 20, MERGE_FANIN, &dir())
                .unwrap();
        let external = Sort::full(&mut Values::new(rows(&input)), asc(), 0, 2, &dir()).unwrap();
        assert_eq!(drain(memory), drain(external));
    }

    #[test]
    fn top_n_keeps_the_smallest_rows_in_order() {
        let input = [9, 3, 7, 1, 8, 2, 6, 0, 5, 4];
        let sort = Sort::new(&mut Values::new(rows(&input)), asc(), Some(3), &dir()).unwrap();
        assert_eq!(drain(sort), vec![0, 1, 2]);
    }

    #[test]
    fn top_n_of_zero_yields_nothing() {
        let sort = Sort::new(&mut Values::new(rows(&[3, 1, 2])), asc(), Some(0), &dir()).unwrap();
        assert!(drain(sort).is_empty());
    }
}
