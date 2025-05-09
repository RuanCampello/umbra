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
use std::io::{Read, Seek, Write};
use std::ops::{Bound, RangeBounds};
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
struct Values {
    pub values: VecDeque<Vec<Expression>>,
}

type Tuple = Vec<Value>;

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
