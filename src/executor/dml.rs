//! DML execution dispatch.
//!
//! Handles `INSERT`, `UPDATE`, and `DELETE` by draining a source
//! [`Operator`] and calling the engine directly — mutations are not
//! wrapped in the operator pipeline.

use super::operator::Operator;
use super::Executor;
use crate::db::DatabaseError;
use crate::sql::Value;
use crate::vm::planner::Tuple;

impl Executor {
    /// Inserts all tuples from a `values` batch into `table`.
    /// Returns the number of inserted rows.
    pub fn insert(
        &self,
        txn_id: i64,
        table: &str,
        values: Vec<Tuple>,
        start_row_id: i64,
    ) -> Result<usize, DatabaseError> {
        let mut row_id = start_row_id;
        let mut count = 0;

        for tuple in values {
            self.engine.insert(txn_id, table, row_id, tuple)?;
            row_id += 1;
            count += 1;
        }

        Ok(count)
    }

    /// Deletes all rows produced by `source`.
    /// The source must yield tuples where column 0 is the `row_id`.
    pub fn delete(
        &self,
        txn_id: i64,
        table: &str,
        source: &mut dyn Operator,
    ) -> Result<usize, DatabaseError> {
        let mut count = 0;

        while let Some(tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;
            self.engine.delete(txn_id, table, row_id)?;
            count += 1;
        }

        Ok(count)
    }

    /// Updates all rows produced by `source` with the given assignments.
    /// The source must yield tuples where column 0 is the `row_id`.
    pub fn update(
        &self,
        txn_id: i64,
        table: &str,
        source: &mut dyn Operator,
        assignments: &[(usize, Value)],
    ) -> Result<usize, DatabaseError> {
        let mut count = 0;

        while let Some(mut tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;

            for (col_idx, value) in assignments {
                tuple[*col_idx] = value.clone();
            }

            // Skip the row_id column (engine manages it internally).
            let data = tuple[1..].to_vec();
            self.engine.update(txn_id, table, row_id, data)?;
            count += 1;
        }

        Ok(count)
    }
}

/// Extracts the `row_id` from column 0 of a tuple.
fn extract_row_id(tuple: &[Value]) -> Result<i64, DatabaseError> {
    match &tuple[0] {
        Value::Number(id) => Ok(*id as i64),
        _ => Err(DatabaseError::Other(
            "row_id must be a number at column 0".into(),
        )),
    }
}
