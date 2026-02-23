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
use crate::storage::mvcc::engine::Engine;
use crate::vm::expression::evaluate_where;
use crate::vm::planner::Tuple;

/// Scans all MVCC-visible rows from a table for a given transaction.
pub(crate) struct Scan {
    tuples: Vec<Tuple>,
    cursor: usize,
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

/// Applies `LIMIT` and `OFFSET` to a source operator.
pub(crate) struct Limit {
    source: Box<dyn Operator>,
    remaining: usize,
    offset: usize,
    skipped: usize,
}

/// Yields pre-built tuples one at a time.
pub(crate) struct Values {
    tuples: Vec<Tuple>,
    cursor: usize,
}

/// Pull-based iterator over tuples.
///
/// Every node in the execution tree implements this trait.
/// The executor calls [`Operator::next`] repeatedly until it returns `Ok(None)`.
pub(crate) trait Operator {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError>;
}

impl Scan {
    pub fn new(engine: &Engine, txn_id: i64, table: &str) -> Result<Self, DatabaseError> {
        let tuples = engine.scan(txn_id, table)?;

        Ok(Self { tuples, cursor: 0 })
    }
}

impl Operator for Scan {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.cursor >= self.tuples.len() {
            return Ok(None);
        }

        let tuple = self.tuples[self.cursor].clone();
        self.cursor += 1;

        Ok(Some(tuple))
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

        let tuple = self.tuples[self.cursor].clone();
        self.cursor += 1;

        Ok(Some(tuple))
    }
}
