//! Plan trees implementation to execute the actual queries.

use crate::core::db::{DatabaseError, IndexMetadata, Relation, Schema, SqlError, TableMetadata};
use crate::core::storage::btree::{BTree, BTreeKeyCmp, BytesCmp, Cursor, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::{self, reassemble_content, Pager};
use crate::core::storage::tuple::{self, deserialize};
use crate::sql::statement::{Expression, Value};
use crate::vm::expression::evaluate_where;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::io::{self, Read, Seek, Write};
use std::ops::{Bound, RangeBounds};
use std::path::PathBuf;
use std::rc::Rc;

use super::expression::resolve_only_expression;

#[derive(Debug, PartialEq)]
pub(crate) enum Planner<File> {
    /// Scans all rows from a table in sequential order.
    /// This is most basic operation that reads every row without any filtering.
    SeqScan(SeqScan<File>),
    /// Optimised scan that finds rows where column exactly matches a specific value.
    /// Used for queries like `SELECT * FROM table WHERE primary_key = value`.
    ExactMatch(ExactMatch<File>),
    /// Scans rows within a specific key range, either from a table or an index.
    RangeScan(RangeScan<File>),
    /// Efficiently retrieves specific ket rows using known primary keys or row id.
    /// Often used after index lookup to fetch actual row data.
    KeyScan(KeyScan<File>),
    /// Combines multiple scans using OR operations.
    LogicalScan(LogicalScan<File>),
    /// Filters rows based on `ẀHERE` clauses conditions.
    /// Evaluates each row against the filter expression and keep the matching rows.
    Filter(Filter<File>),
    /// Handles literal values from INSERT statements.
    Values(Values),
}

#[derive(Debug, PartialEq)]
struct SeqScan<File> {
    pub table: TableMetadata,
    pager: Rc<RefCell<Pager<File>>>,
    cursor: Cursor,
}

#[derive(Debug, PartialEq)]
struct ExactMatch<File> {
    relation: Relation,
    key: Vec<u8>,
    expr: Expression,
    pager: Rc<RefCell<Pager<File>>>,
    done: bool,
    emit_only_key: bool,
}

#[derive(Debug, PartialEq)]
struct RangeScan<File> {
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
struct KeyScan<File> {
    comparator: FixedSizeCmp,
    table: TableMetadata,
    pager: Rc<RefCell<Pager<File>>>,
    source: Box<Planner<File>>,
}

#[derive(Debug, PartialEq)]
struct LogicalScan<File> {
    scans: VecDeque<Planner<File>>,
}

#[derive(Debug, PartialEq)]
struct Filter<File> {
    source: Box<Planner<File>>,
    schema: Schema,
    filter: Expression,
}

#[derive(Debug, PartialEq)]
struct Sort<File> {
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
struct Values {
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
struct TupleComparator {
    schema: Schema,
    sort_schema: Schema,
    sort_indexes: Vec<usize>,
}

type Tuple = Vec<Value>;

const TUPLE_HEADER_SIZE: usize = std::mem::size_of::<u32>();

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
            Self::Values(values) => values.try_next(),
        }
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
        todo!()
    }
}

impl<File: PlanExecutor> Sort<File> {
    fn write_output(&mut self) -> io::Result<()> {
        self.output_buffer
            .write_to(self.output_file.as_mut().unwrap())?;
        self.output_buffer.clear();

        Ok(())
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
    fn write_to<File: Write>(&self, file: &mut File) -> io::Result<()> {
        file.write_all(&self.serialize())
    }

    fn serialize(&self) -> Vec<u8> {
        let mut buff = Vec::with_capacity(self.page_size);

        if !self.packed {
            buff.extend_from_slice(&(self.tuples.len() as u32).to_le_bytes());
        }

        &self
            .tuples
            .iter()
            .for_each(|tuple| buff.extend_from_slice(&tuple::serialize_tuple(&self.schema, tuple)));

        if !self.packed {
            buff.resize(self.page_size, 0); // little padding
        }

        buff
    }

    fn clear(&mut self) {
        self.tuples.clear();
        self.current_size = match self.packed {
            true => 0,
            false => TUPLE_HEADER_SIZE,
        };
    }
}

impl TupleComparator {
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
            .iter().copied()
            .map(|idx| (tuple[idx].partial_cmp(&other_tuple[idx]), &tuple[idx], &other_tuple[idx]))
            .find_map(|(cmp, v1, v2)| match cmp {
                Some(ordering) if ordering != Ordering::Equal => Some(ordering),
                None => {
                    if std::mem::discriminant(v1).ne(&std::mem::discriminant(v2)) {
                        unreachable!("This should be impossible, but type {v1} in memory is different from {v2}");
                    }

                    None                }
                _ => None,
            }).unwrap_or(Ordering::Equal)
    }
}
