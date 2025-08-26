//! Plan trees implementation to execute the actual queries.

#![allow(dead_code)]

use super::expression::{resolve_expression, resolve_only_expression};
use crate::core::random::Rng;
use crate::core::storage::btree::{BTree, BTreeKeyCmp, BytesCmp, Cursor};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::{reassemble_content, Pager};
use crate::core::storage::tuple::{self, deserialize};
use crate::db::{DatabaseError, Relation, Schema, SqlError, TableMetadata};
use crate::sql::statement::{join, Assignment, Expression, Function, OrderDirection, Value};
use crate::vm;
use crate::vm::expression::evaluate_where;
use std::cell::RefCell;
use std::cmp::{self, Ordering};
use std::collections::{HashMap, VecDeque};
use std::fmt::Display;
use std::io::{self, BufRead, BufReader, Read, Seek, Write};
use std::ops::{Bound, Index, RangeBounds};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::{iter, ptr, slice};

#[derive(Debug, PartialEq)]
pub(crate) enum Planner<File: FileOperations> {
    /// Scans all rows from a table in sequential order.
    /// This is the most basic operation that reads every row without any filtering.
    SeqScan(SeqScan<File>),
    /// Optimised scan that finds rows where the column exactly matches a specific value.
    /// Used for queries like `SELECT * FROM table WHERE primary_key = value`.
    ExactMatch(ExactMatch<File>),
    /// Scans rows within a specific key range, either from a table or an index.
    RangeScan(RangeScan<File>),
    /// Efficiently retrieves specific ket rows using known primary keys or row id.
    /// Often used after index lookup to fetch actual row data.
    KeyScan(KeyScan<File>),
    /// Combines multiple scans using OR operations.
    LogicalScan(LogicalScan<File>),
    /// Filters rows based on `ẀHERE` clause conditions.
    /// Evaluates each row against the filter expression and keep the matching rows.
    Filter(Filter<File>),
    Sort(Sort<File>),
    Insert(Insert<File>),
    Update(Update<File>),
    Delete(Delete<File>),
    SortKeys(SortKeys<File>),
    /// Used for aggregate functions like `COUNT` or `AVG`.
    Aggregate(Aggregate<File>),
    Project(Project<File>),
    Collect(Collect<File>),
    /// Handles literal values from INSERT statements.
    Values(Values),
}

#[derive(Debug, PartialEq)]
pub(crate) struct SeqScan<File> {
    pub table: TableMetadata,
    pub pager: Rc<RefCell<Pager<File>>>,
    pub cursor: Cursor,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ExactMatch<File> {
    pub relation: Relation,
    pub key: Vec<u8>,
    pub expr: Expression,
    pub pager: Rc<RefCell<Pager<File>>>,
    pub done: bool,
    pub emit_only_key: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) struct RangeScan<File> {
    index: usize,
    relation: Relation,
    root: PageNumber,
    schema: Schema,
    pager: Rc<RefCell<Pager<File>>>,
    range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    comparator: BTreeKeyCmp,
    expr: Expression,
    cursor: Cursor,
    init: bool,
    done: bool,
    pub emit_only_key: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) struct KeyScan<File: FileOperations> {
    pub comparator: BTreeKeyCmp,
    pub table: TableMetadata,
    pub pager: Rc<RefCell<Pager<File>>>,
    pub source: Box<Planner<File>>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct LogicalScan<File: FileOperations> {
    pub(crate) scans: VecDeque<Planner<File>>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Filter<File: FileOperations> {
    pub source: Box<Planner<File>>,
    pub schema: Schema,
    pub filter: Expression,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Sort<File: FileOperations> {
    collection: Collect<File>,
    comparator: TupleComparator,
    sorted: bool,
    page_size: usize,
    input_buffers: usize,
    output_buffer: TupleBuffer,
    work_dir: PathBuf,
    input_file: Option<File>,
    output_file: Option<File>,
    input_file_path: PathBuf,
    output_file_path: PathBuf,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Insert<File: FileOperations> {
    pub pager: Rc<RefCell<Pager<File>>>,
    pub source: Box<Planner<File>>,
    pub table: TableMetadata,
    pub comparator: BTreeKeyCmp,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Update<File: FileOperations> {
    pub table: TableMetadata,
    pub assigments: Vec<Assignment>,
    pub pager: Rc<RefCell<Pager<File>>>,
    pub source: Box<Planner<File>>,
    pub comparator: BTreeKeyCmp,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Delete<File: FileOperations> {
    pub table: TableMetadata,
    pub comparator: BTreeKeyCmp,
    pub pager: Rc<RefCell<Pager<File>>>,
    pub source: Box<Planner<File>>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct SortKeys<File: FileOperations> {
    pub source: Box<Planner<File>>,
    pub schema: Schema,
    pub expressions: Vec<Expression>,
}

#[derive(Debug)]
pub(crate) struct Collect<File: FileOperations> {
    source: Box<Planner<File>>,
    schema: Schema,
    mem_buff: TupleBuffer,
    file: Option<File>,
    file_path: PathBuf,
    reader: Option<BufReader<File>>,
    work_dir: PathBuf,
    collected: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Project<File: FileOperations> {
    pub source: Box<Planner<File>>,
    pub input: Schema,
    pub output: Schema,
    pub projection: Vec<Expression>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Aggregate<File: FileOperations> {
    source: Box<Planner<File>>,
    group_by: Vec<Expression>,
    aggr_exprs: Vec<Expression>,
    output: Schema,
    output_buffer: TupleBuffer,
    filled: bool,
    page_size: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Values {
    pub values: VecDeque<Vec<Expression>>,
}

#[derive(Debug, PartialEq)]
struct TupleBuffer {
    page_size: usize,
    current_size: usize,
    largest_size: usize,
    packed: bool,
    schema: Schema,
    tuples: VecDeque<Tuple>,
}

#[derive(Debug, PartialEq)]
struct FileFifo<File: FileOperations> {
    page_size: usize,
    input_buffer: VecDeque<u32>,
    output_buffer: VecDeque<u32>,
    work_dir: PathBuf,
    read_page: usize,
    written_pages: usize,
    file: Option<File>,
    file_path: PathBuf,
    len: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) struct TupleComparator {
    schema: Schema,
    sort_schema: Schema,
    sort_indexes: Vec<usize>,
    directions: Vec<OrderDirection>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct RangeScanBuilder<File: FileOperations> {
    pub pager: Rc<RefCell<Pager<File>>>,
    pub relation: Relation,
    pub range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    pub expr: Expression,
    pub emit_only_key: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) struct SortBuilder<File: FileOperations> {
    pub page_size: usize,
    pub work_dir: PathBuf,
    pub collection: Collect<File>,
    pub comparator: TupleComparator,
    pub input_buffers: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) struct CollectBuilder<File: FileOperations> {
    pub source: Box<Planner<File>>,
    pub schema: Schema,
    pub work_dir: PathBuf,
    pub mem_buff_size: usize,
}

#[derive(Debug, PartialEq)]
pub(crate) struct AggregateBuilder<File: FileOperations> {
    pub source: Box<Planner<File>>,
    pub group_by: Vec<Expression>,
    pub aggr_exprs: Vec<Expression>,
    pub output: Schema,
    pub page_size: usize,
}

pub(crate) type Tuple = Vec<Value>;

pub(crate) const DEFAULT_SORT_BUFFER_SIZE: usize = 4;
const TUPLE_HEADER_SIZE: usize = size_of::<u32>();

pub trait PlanExecutor: Seek + Read + Write + FileOperations {}
pub trait Execute {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}
impl<File: Seek + Read + Write + FileOperations> PlanExecutor for File {}

impl<File: PlanExecutor> Execute for Planner<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        match self {
            Self::SeqScan(seq) => seq.try_next(),
            Self::ExactMatch(exact_match) => exact_match.try_next(),
            Self::RangeScan(range) => range.try_next(),
            Self::KeyScan(key) => key.try_next(),
            Self::LogicalScan(logical) => logical.try_next(),
            Self::Filter(filter) => filter.try_next(),
            Self::Sort(sort) => sort.try_next(),
            Self::Insert(insert) => insert.try_next(),
            Self::Update(update) => update.try_next(),
            Self::Delete(delete) => delete.try_next(),
            Self::SortKeys(keys) => keys.try_next(),
            Self::Project(projection) => projection.try_next(),
            Self::Aggregate(aggr) => aggr.try_next(),
            Self::Collect(collection) => collection.try_next(),
            Self::Values(values) => values.try_next(),
        }
    }
}

impl<File: FileOperations> Planner<File> {
    pub fn child(&self) -> Option<&Self> {
        Some(match self {
            Self::KeyScan(key) => &key.source,
            Self::Filter(filter) => &filter.source,
            Self::Sort(sort) => &sort.collection.source,
            Self::Insert(insert) => &insert.source,
            Self::Update(update) => &update.source,
            Self::Delete(delete) => &delete.source,
            Self::SortKeys(keys) => &keys.source,
            Self::Collect(collection) => &collection.source,
            Self::Aggregate(aggr) => &aggr.source,
            _ => return None,
        })
    }

    fn display(&self) -> String {
        let prefix = "-> ";

        let display = match self {
            Self::SeqScan(seq) => format!("{seq}"),
            Self::ExactMatch(exact_match) => format!("{exact_match}"),
            Self::RangeScan(range) => format!("{range}"),
            Self::KeyScan(key) => format!("{key}"),
            Self::LogicalScan(logical) => format!("{logical}"),
            Self::Filter(filter) => format!("{filter}"),
            Self::Sort(sort) => format!("{sort}"),
            Self::Insert(insert) => format!("{insert}"),
            Self::Update(update) => format!("{update}"),
            Self::Delete(delete) => format!("{delete}"),
            Self::SortKeys(keys) => format!("{keys}"),
            Self::Project(projection) => format!("{projection}"),
            Self::Collect(collection) => format!("{collection}"),
            Self::Values(values) => format!("{values}"),
            Self::Aggregate(aggr) => format!("{aggr}"),
        };

        format!("{prefix}{display}")
    }

    pub(crate) fn schema(&self) -> Option<Schema> {
        let schema = match self {
            Self::SeqScan(seq) => &seq.table.schema,
            Self::ExactMatch(exact_match) => exact_match.relation.schema(),
            Self::RangeScan(range) => &range.schema,
            Self::KeyScan(key) => &key.table.schema,
            Self::Sort(sort) => &sort.collection.schema,
            Self::Collect(collection) => &collection.schema,
            Self::Project(project) => &project.output,
            Self::Aggregate(aggr) => &aggr.output,
            Self::LogicalScan(logical) => return logical.scans[0].schema().to_owned(),
            Self::Filter(filter) => return filter.source.schema(),
            _ => return None,
        };

        Some(schema.to_owned())
    }
}

impl<File: PlanExecutor> Execute for SeqScan<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let mut pager = self.pager.borrow_mut();

        let Some((page, slot)) = self.cursor.try_next(&mut pager)? else {
            return Ok(None);
        };

        Ok(Some(deserialize(
            reassemble_content(&mut pager, page, slot)?.as_ref(),
            &self.table.schema,
        )))
    }
}

impl<File: PlanExecutor> SeqScan<File> {
    fn to_plan(metadata: &TableMetadata, pager: Rc<RefCell<Pager<File>>>) -> Planner<File> {
        Planner::SeqScan(SeqScan {
            table: metadata.clone(),
            pager,
            cursor: Cursor::new(metadata.root, 0),
        })
    }
}

impl<File: PlanExecutor> Execute for ExactMatch<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.done {
            return Ok(None);
        }

        self.done = true;

        let mut pager = self.pager.borrow_mut();
        let mut btree = BTree::new(&mut pager, self.relation.root(), self.relation.comp());

        let Some(entry) = btree.get(&self.key)? else {
            return Ok(None);
        };

        let mut tuple = tuple::deserialize(entry.as_ref(), self.relation.schema());
        if self.emit_only_key {
            let table_idx = self.relation.index();
            tuple.drain(table_idx + 1..);
            tuple.drain(..table_idx);
        }

        Ok(Some(tuple))
    }
}

impl<File: PlanExecutor> RangeScan<File> {
    pub(crate) fn new(
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
        relation: Relation,
        emit_only_key: bool,
        expr: Expression,
        pager: Rc<RefCell<Pager<File>>>,
    ) -> Self {
        Self {
            schema: relation.schema().clone(),
            comparator: relation.comp(),
            root: relation.root(),
            cursor: Cursor::new(relation.root(), 0),
            index: relation.index(),
            done: false,
            init: false,
            emit_only_key,
            expr,
            pager,
            range,
            relation,
        }
    }
    fn init(&mut self) -> std::io::Result<()> {
        let mut pager = self.pager.borrow_mut();

        let key = match self.range.start_bound() {
            Bound::Excluded(key) => key,
            Bound::Included(key) => key,
            Bound::Unbounded => return Ok(()),
        };

        let mut descent = Vec::new();
        let mut btree = BTree::new(&mut pager, self.root, self.comparator.clone());
        let search = btree.search(self.root, key, &mut descent)?;

        match search.index {
            Ok(slot) => {
                self.cursor = Cursor::initialized(search.page, slot, descent);

                if let Bound::Excluded(_) = self.range.start_bound() {
                    self.cursor.try_next(&mut pager)?;
                }
            }
            Err(slot) => match slot >= pager.get(search.page)?.len() {
                true => {
                    self.cursor = Cursor::initialized(search.page, slot.saturating_sub(1), descent);
                    self.cursor.try_next(&mut pager)?;
                }
                false => self.cursor = Cursor::initialized(search.page, slot, descent),
            },
        }

        Ok(())
    }
}

impl<File: PlanExecutor> Execute for RangeScan<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.done {
            return Ok(None);
        }

        if !self.init {
            self.init()?;
            self.init = true;
        }

        let mut pager = self.pager.borrow_mut();
        let Some((page, slot)) = self.cursor.try_next(&mut pager)? else {
            self.done = true;
            return Ok(None);
        };

        let entry = reassemble_content(&mut pager, page, slot)?;
        let bound = self.range.end_bound();

        if let Bound::Excluded(key) | Bound::Included(key) = bound {
            let ordering = self.comparator.cmp(entry.as_ref(), key);

            if let Ordering::Equal | Ordering::Greater = ordering {
                self.done = true;

                if matches!(bound, Bound::Excluded(_))
                    || matches!(bound, Bound::Included(_)) && ordering.eq(&Ordering::Greater)
                {
                    return Ok(None);
                }
            }
        }

        let mut tuple = tuple::deserialize(entry.as_ref(), &self.schema);
        if self.emit_only_key {
            tuple.drain(self.index + 1..);
            tuple.drain(..self.index);
        }

        Ok(Some(tuple))
    }
}

impl<File: FileOperations> From<RangeScanBuilder<File>> for RangeScan<File> {
    fn from(value: RangeScanBuilder<File>) -> Self {
        let RangeScanBuilder {
            relation,
            emit_only_key,
            range,
            pager,
            expr,
        } = value;

        Self {
            emit_only_key,
            range,
            expr,
            pager,
            init: false,
            done: false,
            relation: relation.clone(),
            index: relation.index(),
            schema: relation.schema().clone(),
            comparator: relation.comp(),
            root: relation.root(),
            cursor: Cursor::new(relation.root(), 0),
        }
    }
}

impl<File: PlanExecutor> Execute for KeyScan<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(key) = self.source.try_next()? else {
            return Ok(None);
        };

        debug_assert!(
            key.len().eq(&1),
            "KeyScan received a tuple with more than one value {key:#?}"
        );

        let mut pager = self.pager.borrow_mut();
        let mut btree = BTree::new(&mut pager, self.table.root, self.comparator.clone());

        let entry = btree
            .get(&tuple::serialize(
                &self.table.schema.columns[0].data_type,
                &key[0],
            ))?
            .ok_or_else(|| {
                DatabaseError::Corrupted(format!(
                    "KeyScan received key {key:#?} that doesn't exist on table {} at {}",
                    self.table.name, self.table.root
                ))
            })?;

        Ok(Some(tuple::deserialize(entry.as_ref(), &self.table.schema)))
    }
}

impl<File: PlanExecutor> Execute for LogicalScan<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut scan) = self.scans.front_mut() else {
            return Ok(None);
        };

        let mut tuple = scan.try_next()?;
        while tuple.is_none() {
            self.scans.pop_front();
            let Some(next) = self.scans.front_mut() else {
                return Ok(None);
            };

            scan = next;
            tuple = scan.try_next()?;
        }

        Ok(Some(tuple.unwrap()))
    }
}

impl<File: PlanExecutor> Execute for Filter<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        while let Some(tuple) = self.source.try_next()? {
            if evaluate_where(&self.schema, &tuple, &self.filter)? {
                return Ok(Some(tuple));
            }
        }

        Ok(None)
    }
}

impl<File: PlanExecutor> Execute for Sort<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if !self.sorted {
            self.collection.collect()?;
            self.sort()?;

            self.sorted = true;
        }

        if self.output_buffer.is_empty() {
            if let Some(input_file) = self.input_file.as_mut() {
                if let Err(DatabaseError::Io(err)) = self.output_buffer.read_from(input_file) {
                    match err.kind() {
                        io::ErrorKind::UnexpectedEof => self.drop_files()?,
                        _ => return Err(err.into()),
                    }
                }
            }
        }

        Ok(self.output_buffer.pop_front().map(|mut tuple| {
            tuple.drain(self.comparator.schema.len()..);
            tuple
        }))
    }
}

impl<File: PlanExecutor> Sort<File> {
    fn sort(&mut self) -> Result<(), DatabaseError> {
        if self.collection.reader.is_none() {
            self.output_buffer =
                std::mem::replace(&mut self.collection.mem_buff, TupleBuffer::empty());

            self.output_buffer
                .sort_by(|tuple, other| self.comparator.cmp(tuple, other));

            return Ok(());
        }

        let (input_path, input_file) = temp_file::<File>(&self.work_dir, "umbra.sort.input")?;
        self.input_file = Some(input_file);
        self.input_file_path = input_path;

        let (output_path, output_file) = temp_file::<File>(&self.work_dir, "umbra.sort.output")?;
        self.output_file = Some(output_file);
        self.output_file_path = output_path;

        self.page_size = cmp::max(
            TupleBuffer::page_size_for(self.collection.mem_buff.largest_size),
            self.page_size,
        );

        let mut input_buffers = Vec::from_iter(
            iter::repeat_with(|| {
                TupleBuffer::new(self.page_size, self.comparator.sort_schema.clone(), false)
            })
            .take(self.input_buffers),
        );

        self.output_buffer =
            TupleBuffer::new(self.page_size, self.comparator.sort_schema.clone(), false);

        let mut runs = FileFifo::<File>::new(self.page_size, &self.work_dir);
        let mut input_pages = 0;

        while let Some(tuple) = self.collection.try_next()? {
            if let Some(available_idx) = input_buffers.iter().position(|buff| buff.fits(&tuple)) {
                input_buffers[available_idx].push(tuple);

                continue;
            }

            let run = self.sorted_run(&mut input_buffers)?;
            input_pages += run;
            runs.push_back(run)?;

            input_buffers[0].push(tuple);
        }

        if input_buffers.iter().any(|buff| !buff.is_empty()) {
            let run = self.sorted_run(&mut input_buffers)?;
            input_pages += run;
            runs.push_back(run)?;
        }

        self.swap()?;

        let mut cursor = vec![0; input_buffers.len()];
        let mut limits = vec![0; input_buffers.len()];

        while runs.len > 1 {
            let mut output_pages = 0;
            let mut segment = 0;

            while segment < input_pages {
                cursor[0] = segment;
                limits[0] = cmp::min(segment + runs.pop_front()?.unwrap_or(0), input_pages);

                (1..self.input_buffers).try_for_each(|idx| -> io::Result<()> {
                    cursor[idx] = limits[idx - 1];
                    limits[idx] = match cursor[idx] < input_pages {
                        true => cmp::min(cursor[idx] + runs.pop_front()?.unwrap_or(0), input_pages),
                        false => input_pages,
                    };

                    Ok(())
                })?;

                (0..self.input_buffers).try_for_each(|idx| -> Result<(), DatabaseError> {
                    if cursor[idx] < limits[idx] {
                        input_buffers[idx]
                            .read_page(self.input_file.as_mut().unwrap(), cursor[idx])?;
                        cursor[idx] += 1;
                    }

                    Ok(())
                })?;

                let mut run = 0;
                while input_buffers.iter().any(|buffer| !buffer.is_empty()) {
                    let min = self.find_min_index(&input_buffers);
                    let tuple = input_buffers[min].pop_front().unwrap();

                    if input_buffers[min].is_empty() && cursor[min] < limits[min] {
                        input_buffers[min]
                            .read_page(self.input_file.as_mut().unwrap(), cursor[min])?;
                        cursor[min] += 1;
                    }

                    if !self.output_buffer.fits(&tuple) {
                        self.write_output()?;
                        run += 1;
                    }

                    self.output_buffer.push(tuple);
                }

                if !self.output_buffer.is_empty() {
                    self.write_output()?;
                    run += 1;
                }

                output_pages += run;
                runs.push_back(run)?;

                segment = limits[self.input_buffers - 1];
            }

            self.swap()?;
            input_pages = output_pages;
        }

        self.input_file.as_mut().unwrap().rewind()?;
        drop(self.output_file.take());
        File::delete(&self.output_file_path)?;

        Ok(())
    }

    fn swap(&mut self) -> io::Result<()> {
        let mut input_file = self.input_file.take().unwrap();
        let output_file = self.output_file.take().unwrap();

        input_file.truncate()?;

        self.input_file = Some(input_file);
        self.output_file = Some(output_file);

        Ok(())
    }

    fn sorted_run(&mut self, input_buffers: &mut [TupleBuffer]) -> io::Result<usize> {
        let mut run = 0;

        input_buffers
            .iter_mut()
            .for_each(|buffer| buffer.sort_by(|t1, t2| self.comparator.cmp(t1, t2)));

        while input_buffers.iter().any(|buffer| !buffer.is_empty()) {
            let min = self.find_min_index(input_buffers);
            let next = input_buffers[min].pop_front().unwrap();

            if !self.output_buffer.fits(&next) {
                self.write_output()?;

                run += 1;
            }

            self.output_buffer.push(next);
        }

        if !self.output_buffer.is_empty() {
            self.write_output()?;
            run += 1;
        }

        Ok(run)
    }

    fn find_min_index(&self, input_buffers: &[TupleBuffer]) -> usize {
        let mut min = input_buffers
            .iter()
            .position(|buffer| !buffer.is_empty())
            .unwrap();

        (min + 1..)
            .zip(&input_buffers[min + 1..])
            .filter(|(_, buffer)| !buffer.is_empty())
            .for_each(|(idx, buffer)| {
                let comp = self.comparator.cmp(&buffer[idx], &buffer[min]);

                if comp.eq(&Ordering::Less) {
                    min = idx
                }
            });

        min
    }

    fn write_output(&mut self) -> io::Result<()> {
        self.output_buffer
            .write_to(self.output_file.as_mut().unwrap())?;
        self.output_buffer.clear();

        Ok(())
    }
}

impl<File: FileOperations> From<SortBuilder<File>> for Sort<File> {
    fn from(value: SortBuilder<File>) -> Self {
        let SortBuilder {
            page_size,
            work_dir,
            comparator,
            collection,
            input_buffers,
        } = value;
        Sort {
            collection,
            page_size,
            work_dir,
            comparator,
            input_buffers,
            sorted: false,
            input_file: None,
            output_file: None,
            output_buffer: TupleBuffer::empty(),
            input_file_path: PathBuf::new(),
            output_file_path: PathBuf::new(),
        }
    }
}

impl<File: FileOperations> Sort<File> {
    fn drop_files(&mut self) -> io::Result<()> {
        if let Some(input) = self.input_file.take() {
            drop(input);
            File::delete(&self.input_file_path)?;
        }

        if let Some(output) = self.output_file.take() {
            drop(output);
            File::delete(&self.output_file_path)?;
        }

        Ok(())
    }
}

impl<File: FileOperations> Drop for Sort<File> {
    fn drop(&mut self) {
        let _ = self.drop_files();
    }
}

impl<File: PlanExecutor> Execute for Insert<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut tuple) = self.source.try_next()? else {
            return Ok(None);
        };

        let mut pager = self.pager.borrow_mut();

        BTree::new(&mut pager, self.table.root, self.comparator.clone())
            .try_insert(tuple::serialize_tuple(&self.table.schema, &tuple))?
            .map_err(|_| SqlError::DuplicatedKey(tuple.swap_remove(0)))?;

        (self.table.indexes)
            .iter()
            .try_for_each(|index| -> Result<(), DatabaseError> {
                let col = self.table.schema.index_of(&index.column.name).ok_or(
                    DatabaseError::Corrupted(format!(
                        "Index column {} not found on table {} with schema {:#?}",
                        index.column.name, self.table.name, self.table.schema
                    )),
                )?;

                let comparator = BTreeKeyCmp::from(&index.column.data_type);

                BTree::new(&mut pager, index.root, comparator)
                    .try_insert(tuple::serialize_tuple(
                        &index.schema,
                        [&tuple[col], &tuple[0]],
                    ))?
                    .map_err(|_| SqlError::DuplicatedKey(tuple.swap_remove(col)))?;

                Ok(())
            })?;

        Ok(Some(vec![]))
    }
}

impl<File: PlanExecutor> Execute for Update<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut tuple) = self.source.try_next()? else {
            return Ok(None);
        };

        let mut updated_cols = HashMap::new();

        for assignment in &self.assigments {
            let col = self.table.schema.index_of(&assignment.identifier).ok_or(
                DatabaseError::Corrupted(format!(
                    "Column {} not found in table schema {:?}",
                    assignment.identifier, self.table
                )),
            )?;

            let new_value = resolve_expression(&tuple, &self.table.schema, &assignment.value)?;

            if new_value.ne(&tuple[col]) {
                let old_value = std::mem::replace(&mut tuple[col], new_value);
                updated_cols.insert(assignment.identifier.clone(), (old_value, col));
            }
        }

        let mut pager = self.pager.borrow_mut();
        let mut btree = BTree::new(&mut pager, self.table.root, self.comparator.clone());

        let updated_entry = tuple::serialize_tuple(&self.table.schema, &tuple);

        match updated_cols.get(&self.table.schema.columns[0].name) {
            Some((old, _)) => {
                btree
                    .try_insert(updated_entry)?
                    .map_err(|_| SqlError::DuplicatedKey(tuple.swap_remove(0)))?;
                btree.remove(&tuple::serialize(
                    &self.table.schema.columns[0].data_type,
                    old,
                ))?;
            }
            None => btree.insert(updated_entry)?,
        }

        for index in &self.table.indexes {
            let mut btree = BTree::new(
                &mut pager,
                index.root,
                BTreeKeyCmp::from(&index.column.data_type),
            );

            if let Some((old, new)) = updated_cols.get(&index.column.name) {
                btree
                    .try_insert(tuple::serialize_tuple(
                        &index.schema,
                        [&tuple[*new], &tuple[0]],
                    ))?
                    .map_err(|_| SqlError::DuplicatedKey(tuple.swap_remove(*new)))?;

                btree.remove(&tuple::serialize(&index.column.data_type, old))?;
            } else if updated_cols.contains_key(&self.table.schema.columns[0].name) {
                let index_col = self.table.schema.index_of(&index.column.name).unwrap();

                btree.insert(tuple::serialize_tuple(
                    &index.schema,
                    [&tuple[index_col], &tuple[0]],
                ))?;
            }
        }

        Ok(Some(vec![]))
    }
}

impl<File: PlanExecutor> Execute for Delete<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(tuple) = self.source.try_next()? else {
            return Ok(None);
        };

        let mut pager = self.pager.borrow_mut();
        let mut btree = BTree::new(&mut pager, self.table.root, self.comparator.clone());

        btree.remove(&tuple::serialize(
            &self.table.schema.columns[0].data_type,
            &tuple[0],
        ))?;

        for index in &self.table.indexes {
            let col = self.table.schema.index_of(&index.column.name).unwrap();
            let key = tuple::serialize(&index.column.data_type, &tuple[col]);

            let mut btree = BTree::new(
                &mut pager,
                index.root,
                BTreeKeyCmp::from(&index.column.data_type),
            );

            btree.remove(&key)?;
        }

        Ok(Some(vec![]))
    }
}

impl<File: PlanExecutor> Execute for SortKeys<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut tuple) = self.source.try_next()? else {
            return Ok(None);
        };

        self.expressions
            .iter()
            .try_for_each(|expr| -> Result<(), DatabaseError> {
                debug_assert!(
                    !matches!(expr, Expression::Identifier(_)),
                    "Identifiers should not get into SortKeys"
                );

                tuple.push(vm::expression::resolve_expression(
                    &tuple,
                    &self.schema,
                    expr,
                )?);
                Ok(())
            })?;

        Ok(Some(tuple))
    }
}

impl<File: PlanExecutor> Collect<File> {
    fn collect(&mut self) -> Result<(), DatabaseError> {
        while let Some(tuple) = self.source.try_next()? {
            if !self.mem_buff.fits(&tuple) {
                if self.file.is_none() {
                    let (file_path, file) = temp_file::<File>(&self.work_dir, "umbra.query")?;
                    self.file_path = file_path;
                    self.file = Some(file);
                }
                self.mem_buff.write_to(self.file.as_mut().unwrap())?;
                self.mem_buff.clear();
            }

            self.mem_buff.push(tuple);
        }

        if let Some(mut file) = self.file.take() {
            file.rewind()?;
            self.reader = Some(BufReader::with_capacity(self.mem_buff.page_size, file));
        }

        Ok(())
    }

    fn drop_file(&mut self) -> io::Result<()> {
        drop(self.file.take());
        drop(self.reader.take());
        File::delete(&self.file_path)
    }
}

impl<File: PlanExecutor> Execute for Collect<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if !self.collected {
            self.collect()?;
            self.collected = true;
        }

        if let Some(reader) = self.reader.as_mut() {
            let has_data_left = {
                let buff = reader.fill_buf()?;
                !buff.is_empty()
            };

            if has_data_left {
                return Ok(Some(tuple::read_from(reader, &self.schema)?));
            }

            self.drop_file()?;
        }

        Ok(self.mem_buff.pop_front())
    }
}

impl<File: FileOperations> From<CollectBuilder<File>> for Collect<File> {
    fn from(value: CollectBuilder<File>) -> Self {
        let CollectBuilder {
            mem_buff_size,
            schema,
            source,
            work_dir,
        } = value;
        Self {
            source,
            mem_buff: TupleBuffer::new(mem_buff_size, schema.clone(), true),
            schema,
            work_dir,
            file_path: PathBuf::new(),
            file: None,
            reader: None,
            collected: false,
        }
    }
}

impl<File: PlanExecutor> Execute for Project<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(tuple) = self.source.try_next()? else {
            return Ok(None);
        };

        Ok(Some(
            self.projection
                .iter()
                .map(|expr| resolve_expression(&tuple, &self.input, expr))
                .collect::<Result<Tuple, _>>()?,
        ))
    }
}

impl<File: PlanExecutor> Execute for Aggregate<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.filled {
            return Ok(self.output_buffer.pop_front());
        }

        self.output_buffer.clear();

        let input_schema = self.source.schema().unwrap();
        let mut groups: HashMap<Vec<Value>, Vec<Tuple>> = HashMap::new();

        while let Some(row) = self.source.try_next()? {
            let key = self
                .group_by
                .iter()
                .map(|expr| resolve_expression(&row, &input_schema, &expr))
                .collect::<Result<Vec<_>, _>>()?;
            groups.entry(key).or_default().push(row)
        }

        if groups.is_empty() {
            self.filled = true;

            let is_count = |expr: &Expression| matches!(expr, Expression::Function { func, .. } if *func == Function::Count);
            if self.group_by.is_empty() && self.aggr_exprs.iter().all(|expr| is_count(expr)) {
                return Ok(Some(vec![Value::Number(0); self.aggr_exprs.len()]));
            }
            return Ok(None);
        }

        for (key, rows) in groups.into_iter() {
            let mut tuple = key;

            for aggr_expr in &self.aggr_exprs {
                match aggr_expr {
                    Expression::Function { func, args } if func.is_aggr() => {
                        let value = self.apply_aggr(func, args, &rows, &input_schema)?;
                        tuple.push(value)
                    }
                    _ => {
                        let expr = self.process_aggr_expr(aggr_expr, &rows, &input_schema)?;
                        let value = resolve_expression(&rows[0], &input_schema, &expr)?;
                        tuple.push(value)
                    }
                };
            }

            self.output_buffer.push(tuple);
        }

        self.filled = true;
        Ok(self.output_buffer.pop_front())
    }
}

impl<File: PlanExecutor> Aggregate<File> {
    fn apply_aggr(
        &self,
        func: &Function,
        args: &[Expression],
        rows: &[Tuple],
        schema: &Schema,
    ) -> Result<Value, DatabaseError> {
        let values = match args.first() {
            None => vec![],
            Some(Expression::Wildcard) => vec![Value::Number(1); rows.len()],
            Some(arg) => rows
                .iter()
                .map(|row| resolve_expression(row, schema, arg))
                .collect::<Result<Vec<_>, _>>()?,
        };

        match func {
            Function::Count => {
                // COUNT(*) counts all rows, COUNT(column) counts non-null values
                if matches!(args.first(), Some(Expression::Wildcard)) {
                    Ok(Value::Number(values.len() as _))
                } else {
                    let non_null_count = values.iter().filter(|v| !matches!(v, Value::Null)).count();
                    Ok(Value::Number(non_null_count as _))
                }
            }
            Function::Sum | Function::Avg => {
                let non_null_values: Vec<_> = values.iter().filter(|v| !matches!(v, Value::Null)).collect();
                
                if non_null_values.is_empty() {
                    return Ok(Value::Null); // SQL standard: aggregates on empty sets return NULL
                }
                
                let sum = non_null_values.iter().fold(0.0, |acc, v| {
                    acc + match v {
                        Value::Float(f) => *f,
                        Value::Number(n) => *n as f64,
                        _ => 0.0,
                    }
                });

                match func {
                    Function::Sum => Ok(Value::Float(sum)),
                    Function::Avg => Ok(Value::Float(sum / non_null_values.len() as f64)),
                    _ => unreachable!(),
                }
            }
            Function::Min | Function::Max => {
                let non_null_values: Vec<_> = values.iter().filter(|v| !matches!(v, Value::Null)).collect();
                
                if non_null_values.is_empty() {
                    return Ok(Value::Null);
                }

                let result = match func {
                    Function::Min => non_null_values.iter().min_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)),
                    Function::Max => non_null_values.iter().max_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal)),
                    _ => unreachable!(),
                };

                Ok(result.cloned().cloned().unwrap_or(Value::Null))
            }
            _ => unreachable!("this ain't a aggregate function"),
        }
    }

    /// Process complex expression that contains aggregate functions. This recursively finds
    /// aggregate functions, compute then and replace with its values, so we can nest aggregate
    /// functions with functions that are computed during runtime, like `TRUNC`.
    fn process_aggr_expr(
        &self,
        expr: &Expression,
        rows: &[Tuple],
        schema: &Schema,
    ) -> Result<Expression, DatabaseError> {
        match expr {
            // this is the base case: when we get a aggr function, we execute them
            // if we don't get here after traversing the expression tree, we just return the
            // expression and consider the job done :/
            Expression::Function { func, args } if func.is_aggr() => {
                let value = self.apply_aggr(func, args, rows, schema)?;
                Ok(Expression::Value(value))
            }
            Expression::Function { func, args } => {
                let args: Result<Vec<_>, _> = args
                    .iter()
                    .map(|arg| self.process_aggr_expr(arg, rows, schema))
                    .collect();

                Ok(Expression::Function {
                    func: *func,
                    args: args?,
                })
            }
            Expression::BinaryOperation {
                operator,
                left,
                right,
            } => Ok(Expression::BinaryOperation {
                operator: *operator,
                left: Box::new(self.process_aggr_expr(left, rows, schema)?),
                right: Box::new(self.process_aggr_expr(right, rows, schema)?),
            }),
            Expression::UnaryOperation { operator, expr } => Ok(Expression::UnaryOperation {
                operator: *operator,
                expr: Box::new(self.process_aggr_expr(expr, rows, schema)?),
            }),
            Expression::Nested(inner) => Ok(Expression::Nested(Box::new(
                self.process_aggr_expr(inner, rows, schema)?,
            ))),
            Expression::Alias { expr, alias } => Ok(Expression::Alias {
                alias: alias.to_string(),
                expr: Box::new(self.process_aggr_expr(expr, rows, schema)?),
            }),

            _ => Ok(expr.to_owned()),
        }
    }
}

impl<File: FileOperations> From<AggregateBuilder<File>> for Aggregate<File> {
    fn from(value: AggregateBuilder<File>) -> Self {
        let AggregateBuilder {
            aggr_exprs,
            page_size,
            group_by,
            source,
            output,
        } = value;

        Self {
            source,
            group_by,
            aggr_exprs,
            output,
            page_size,
            output_buffer: TupleBuffer::empty(),
            filled: false,
        }
    }
}

impl Execute for Values {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(mut values) = self.values.pop_front() else {
            return Ok(None);
        };

        Ok(Some(
            values
                .drain(..)
                .map(|expr| resolve_only_expression(&expr))
                .collect::<Result<Vec<Value>, SqlError>>()?,
        ))
    }
}

impl TupleBuffer {
    fn new(page_size: usize, schema: Schema, packed: bool) -> Self {
        Self {
            page_size,
            schema,
            packed,
            current_size: if packed { 0 } else { TUPLE_HEADER_SIZE },
            largest_size: 0,
            tuples: VecDeque::new(),
        }
    }

    fn empty() -> Self {
        Self {
            page_size: 0,
            current_size: 0,
            largest_size: 0,
            schema: Schema::empty(),
            packed: false,
            tuples: VecDeque::new(),
        }
    }

    fn sort_by(&mut self, cmp: impl FnMut(&Tuple, &Tuple) -> Ordering) {
        self.tuples.make_contiguous().sort_by(cmp)
    }

    fn write_to<File: Write>(&self, file: &mut File) -> io::Result<()> {
        file.write_all(&self.serialize())
    }

    fn read_page(
        &mut self,
        file: &mut (impl Seek + Read),
        page: usize,
    ) -> Result<(), DatabaseError> {
        file.seek(io::SeekFrom::Start((self.page_size * page) as u64))?;
        self.read_from(file)
    }

    fn read_from(&mut self, file: &mut impl Read) -> Result<(), DatabaseError> {
        debug_assert!(
            self.is_empty() && !self.packed,
            "read_from() only works for fixed size buffers"
        );

        let mut buff = vec![0; self.page_size];
        file.read_exact(&mut buff)?;

        let num_of_tuples = u32::from_le_bytes(
            buff[..TUPLE_HEADER_SIZE]
                .try_into()
                .map_err(|err| DatabaseError::Other(format!("Error reading buffer: {err}")))?,
        );

        let mut cursor = TUPLE_HEADER_SIZE;
        (0..num_of_tuples).for_each(|_| {
            let tuple = tuple::deserialize(&buff[cursor..], &self.schema);
            cursor += tuple::size_of(&tuple, &self.schema);
            self.push(tuple);
        });

        Ok(())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buff = Vec::with_capacity(self.page_size);

        if !self.packed {
            buff.extend_from_slice(&(self.tuples.len() as u32).to_le_bytes());
        }

        for tuple in &self.tuples {
            buff.extend_from_slice(&tuple::serialize_tuple(&self.schema, tuple));
        }

        if !self.packed {
            buff.resize(self.page_size, 0); // little padding
        }

        buff
    }

    fn push(&mut self, tuple: Tuple) {
        let size = tuple::size_of(&tuple, &self.schema);

        if size > self.largest_size {
            self.largest_size = size;
        }

        self.current_size += size;
        self.tuples.push_back(tuple);
    }

    fn pop_front(&mut self) -> Option<Tuple> {
        self.tuples
            .pop_front()
            .inspect(|tuple| self.current_size -= tuple::size_of(tuple, &self.schema))
    }

    fn page_size_for(size: usize) -> usize {
        let mut page = std::mem::size_of::<u32>() * 2 + size;
        page -= 1;
        page |= page >> 1;
        page |= page >> 2;
        page |= page >> 4;
        page |= page >> 8;
        page |= page >> 16;

        page
    }

    fn clear(&mut self) {
        self.tuples.clear();
        self.current_size = match self.packed {
            true => 0,
            false => TUPLE_HEADER_SIZE,
        };
    }

    fn fits(&self, tuple: &Tuple) -> bool {
        self.current_size + tuple::size_of(tuple, &self.schema) <= self.page_size
    }

    fn is_empty(&self) -> bool {
        self.tuples.is_empty()
    }
}

impl Index<usize> for TupleBuffer {
    type Output = Tuple;

    fn index(&self, index: usize) -> &Self::Output {
        &self.tuples[index]
    }
}

impl<File: PlanExecutor> FileFifo<File> {
    fn new(page_size: usize, work_dir: &Path) -> Self {
        debug_assert!(
            page_size % std::mem::size_of::<u32>() == 0,
            "Page size must be a multiple of 4: {page_size}"
        );

        Self {
            page_size,
            input_buffer: VecDeque::with_capacity(page_size),
            output_buffer: VecDeque::with_capacity(page_size),
            work_dir: work_dir.to_path_buf(),
            file: None,
            file_path: PathBuf::new(),
            read_page: 0,
            written_pages: 0,
            len: 0,
        }
    }

    fn push_back(&mut self, run: usize) -> io::Result<()> {
        let run = u32::try_from(run).expect("Page run is greater than u32::MAX");

        self.len += 1;

        if self.input_buffer.len() + 1 <= self.page_size / std::mem::size_of::<u32>() {
            self.input_buffer.push_back(run);

            return Ok(());
        }

        if self.file.is_none() {
            let (path, file) = temp_file(&self.work_dir, "umbra.sort.runs")?;

            self.file_path = path;
            self.file = Some(file);
        }

        let slice = unsafe {
            slice::from_raw_parts(
                ptr::from_ref(&self.input_buffer.make_contiguous()[0]).cast(),
                self.input_buffer.len() * std::mem::size_of::<u32>(),
            )
        };

        let file = self.file.as_mut().unwrap();
        file.seek(io::SeekFrom::End(0))?;
        file.write_all(slice)?;
        self.written_pages += 1;

        self.input_buffer.clear();
        self.input_buffer.push_back(run);

        Ok(())
    }

    fn pop_front(&mut self) -> io::Result<Option<usize>> {
        self.len = self.len.saturating_sub(1);

        if let Some(run) = self.output_buffer.pop_front() {
            return Ok(Some(run as usize));
        }

        if self.written_pages.eq(&0) {
            return Ok(self.input_buffer.pop_front().map(|run| run as usize));
        }

        self.output_buffer
            .resize(self.page_size / std::mem::size_of::<u32>(), 0);

        let slice = unsafe {
            slice::from_raw_parts_mut(
                ptr::from_mut(&mut self.output_buffer.make_contiguous()[0]).cast(),
                self.page_size,
            )
        };

        let file = self.file.as_mut().unwrap();
        file.seek(io::SeekFrom::Start(
            (self.page_size * self.read_page) as u64,
        ))?;
        self.read_page += 1;

        if file.read(slice)? > 0 {
            return Ok(self.output_buffer.pop_front().map(|run| run as usize));
        }

        self.output_buffer.clear();
        self.read_page = 0;
        self.written_pages = 0;
        file.truncate()?;

        Ok(self.input_buffer.pop_front().map(|run| run as usize))
    }
}

impl<File: FileOperations> FileFifo<File> {
    fn drop_file(&mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            drop(file);
            File::delete(&self.file_path)?;
        };

        Ok(())
    }
}

impl<File: FileOperations> Drop for FileFifo<File> {
    fn drop(&mut self) {
        let _ = self.drop_file();
    }
}

impl TupleComparator {
    pub(crate) fn new(
        schema: Schema,
        sort_schema: Schema,
        sort_indexes: Vec<usize>,
        directions: Vec<OrderDirection>,
    ) -> Self {
        Self {
            schema,
            sort_schema,
            sort_indexes,
            directions,
        }
    }

    fn cmp(&self, tuple: &[Value], other_tuple: &[Value]) -> Ordering {
        debug_assert!(
            tuple.len().eq(&other_tuple.len()),
            "Comp called for mismatch size tuples"
        );

        debug_assert!(
            tuple.len().eq(&self.schema.len()),
            "Comp called for mismatch tuple with schema"
        );

        self.sort_indexes
            .iter()
            .zip(self.directions.iter().copied())
            .map(|(&idx, direction)| {
                debug_assert!(
                    idx < tuple.len(),
                    "Sort index {idx} out of bounds for tuple of len {}",
                    tuple.len()
                );

                let ord = tuple[idx].partial_cmp(&other_tuple[idx]);
                (ord, direction)
            })
            .find_map(|(ord, direction)| match ord {
                Some(ordering) if ordering != Ordering::Equal => Some(match direction {
                    OrderDirection::Desc => ordering.reverse(),
                    _ => ordering,
                }),
                _ => None,
            })
            .unwrap_or(Ordering::Equal)
    }
}

impl<File: FileOperations + PartialEq> PartialEq for Collect<File> {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source && self.schema == other.schema
    }
}

fn temp_file<File: FileOperations>(
    work_dir: &Path,
    extension: &str,
) -> io::Result<(PathBuf, File)> {
    let mut rand = Rng::new();

    let filename = rand.i64(i64::MIN..=i64::MAX);
    let path = work_dir.join(format!("umbra.temp/{filename:x}.{extension}"));
    let file = File::create(&path)?;

    Ok((path, file))
}

impl<File: FileOperations> Display for Planner<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut planners = vec![self.display()];
        let mut node = self;

        while let Some(child) = node.child() {
            planners.push(child.display());
            node = child;
        }

        writeln!(f, "{}", planners.pop().unwrap())?;
        while let Some(plan) = planners.pop() {
            writeln!(f, "{plan}")?;
        }

        Ok(())
    }
}

impl<File> Display for SeqScan<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SeqScan on table {}", self.table.name)
    }
}

impl<File> Display for ExactMatch<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ExactMatch {} on {} {}",
            self.expr,
            self.relation.kind(),
            self.relation.name()
        )
    }
}

impl<File> Display for RangeScan<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RangeScan {} on {} {}",
            self.expr,
            self.relation.kind(),
            self.relation.name()
        )
    }
}

impl<File: FileOperations> Display for KeyScan<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "KeyScan {} on table {}",
            self.table.schema.columns[0].name, self.table.name
        )
    }
}

impl<File: FileOperations> Display for LogicalScan<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogicalScan")?;

        for scan in &self.scans {
            write!(f, "\n {}", scan.display())?;
        }

        Ok(())
    }
}

impl<File: FileOperations> Display for Sort<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let col_names = self
            .comparator
            .sort_indexes
            .iter()
            .map(|idx| &self.comparator.sort_schema.columns[*idx].name);

        write!(f, "Sort by {}", join(col_names, ", "))
    }
}

impl<File: FileOperations> Display for Filter<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Filter {}", self.filter)
    }
}

impl<File: FileOperations> Display for Project<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Project {}", join(&self.projection, ", "))
    }
}

impl<File: FileOperations> Display for SortKeys<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SortKeys {}", join(&self.expressions, ", "))
    }
}

impl<File: FileOperations> Display for Insert<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Insert on table {}", self.table.name)
    }
}

impl<File: FileOperations> Display for Update<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Update {} on table {}",
            join(&self.assigments, ", "),
            self.table.name
        )
    }
}

impl<File: FileOperations> Display for Delete<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Delete from table {}", self.table.name)
    }
}

impl<File: FileOperations> Display for Collect<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Collect {}",
            join(self.schema.columns.iter().map(|col| &col.name), ", ")
        )
    }
}

impl<File: FileOperations> Display for Aggregate<File> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Aggregate on {}", join(&self.group_by, ", "))
    }
}

impl Display for Values {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Values {}", join(&self.values[0], ", "))
    }
}
