//! DML execution dispatch
//!
//! Handles `INSERT`, `UPDATE`, and `DELETE` by draining a source
//! [Operator] and calling the engine directly, mutations are not
//! wrapped in the operator pipeline

use super::operator::Operator;
use super::{Executor, Tuple};
use crate::db::{Column, DatabaseError, Schema, SqlError};
use crate::sql::Value;
use crate::sql::statement::{Expression, Type};
use crate::storage::engine::error::EngineError;
use crate::storage::engine::table::{Row, RowId, Table};
use crate::storage::engine::transaction::Transaction;
use crate::vm::expression::{evaluate_where, resolve_expression};

/// the rows a DML statement targets: its table, its `WHERE` clause and the row-id-first schema
/// the clause is evaluated against
pub(crate) struct Target<'a> {
    pub table: &'a str,
    pub predicate: Option<&'a Expression>,
    pub schema: &'a Schema,
}

impl Executor {
    /// inserts all tuples from a `values` batch into `table`; with `collect`, returns the typed
    /// tuples written, row id first
    pub fn insert(
        &self,
        txn: &mut Transaction,
        table: &str,
        values: Vec<Tuple>,
        collect: bool,
    ) -> Result<(usize, Vec<Tuple>), DatabaseError> {
        let table = self.engine.table(table)?;
        let mut count = 0;
        let mut written = Vec::new();

        for mut tuple in values {
            let typed = match collect {
                true => tuple.clone(),
                false => Vec::new(),
            };

            for (column, value) in table.schema.columns.iter().zip(tuple.iter_mut()) {
                coerce_value(column, value)?;
            }

            let row_id = txn.insert(&table, tuple.into())?;
            count += 1;

            if collect {
                let mut row = Vec::with_capacity(typed.len() + 1);
                row.push(Value::Number(row_id as i128));
                row.extend(typed);
                written.push(row);
            }
        }

        Ok((count, written))
    }

    /// deletes all rows produced by `source` (row id at column 0)
    /// under READ COMMITTED a row changed since the statement began is deleted only if its newest image still matches
    pub fn delete(
        &self,
        txn: &mut Transaction,
        target: &Target<'_>,
        source: &mut dyn Operator,
    ) -> Result<usize, DatabaseError> {
        let table = self.engine.table(target.table)?;
        let mut count = 0;

        while let Some(tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;

            loop {
                match txn.delete(&table, row_id) {
                    Ok(()) => {
                        count += 1;
                        break;
                    },
                    Err(EngineError::Stale(latest)) => {
                        if recheck(target, row_id, latest)?.is_none() {
                            break;
                        }
                    },
                    Err(error) => return Err(error.into()),
                }
            }
        }

        Ok(count)
    }

    /// updates all rows produced by `source` with the given assignments
    /// under READ COMMITTED a row changed since the statement began is re-checked against the
    /// `WHERE` clause and the assignments re-evaluated on its newest image
    pub fn update(
        &self,
        txn: &mut Transaction,
        target: &Target<'_>,
        source: &mut dyn Operator,
        assignments: &[(usize, Expression)],
        collect: bool,
    ) -> Result<(usize, Vec<Tuple>), DatabaseError> {
        let table = self.engine.table(target.table)?;
        let mut count = 0;
        let mut collected = Vec::new();

        while let Some(mut tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;

            loop {
                let new = assign(&table, &tuple, assignments, target.schema)?;
                match txn.update(&table, row_id, new[1..].into()) {
                    Ok(_) => {
                        count += 1;
                        if collect {
                            let mut pair = tuple;
                            pair.extend(new);
                            collected.push(pair);
                        }
                        break;
                    },
                    Err(EngineError::Stale(latest)) => match recheck(target, row_id, latest)? {
                        Some(latest) => tuple = latest,
                        _ => break,
                    },
                    Err(error) => return Err(error.into()),
                }
            }
        }

        Ok((count, collected))
    }
}

/// the row after `assignments`, each evaluated against the pre-update `tuple`
fn assign(
    table: &Table,
    tuple: &Tuple,
    assignments: &[(usize, Expression)],
    schema: &Schema,
) -> Result<Tuple, DatabaseError> {
    let mut new = tuple.clone();

    for (column, expression) in assignments {
        let mut value = resolve_expression(tuple, schema, expression)?;
        coerce_value(&table.schema.columns[column - 1], &mut value)?;
        new[*column] = value;
    }

    Ok(new)
}

/// the newest image of a row that changed under a READ COMMITTED statement, as a tuple, when it
/// still satisfies the statement's predicate
fn recheck(
    target: &Target<'_>,
    row_id: RowId,
    latest: Option<Row>,
) -> Result<Option<Tuple>, DatabaseError> {
    let Some(latest) = latest else {
        return Ok(None);
    };

    let mut tuple = Vec::with_capacity(latest.len() + 1);
    tuple.push(Value::Number(row_id as i128));
    tuple.extend(latest.iter().cloned());

    match target.predicate {
        Some(predicate) if !evaluate_where(target.schema, &tuple, predicate)? => Ok(None),
        _ => Ok(Some(tuple)),
    }
}

/// `value` converted to the representation of `column`'s type
pub(super) fn coerce_value(column: &Column, value: &mut Value) -> Result<(), DatabaseError> {
    use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime, Parse, interval::Interval};
    use crate::core::json::{self, Conv};
    use crate::core::numeric::Numeric;
    use crate::core::uuid::Uuid;
    use std::str::FromStr;

    let invalid = |kind: &str, literal: &str| {
        DatabaseError::Sql(SqlError::Other(format!("invalid {kind} '{literal}'")))
    };

    match (column.column_type(), &mut *value) {
        (Type::Enum(_), Value::String(literal)) => {
            let variants = column.variants().expect("enum columns carry their variants");

            match variants.iter().position(|variant| variant.as_str() == &**literal) {
                Some(idx) => *value = Value::Enum(idx as u8),
                None => return Err(invalid("enum variant", literal)),
            }
        },

        (Type::Date, Value::String(literal)) => match NaiveDate::parse_str(literal) {
            Ok(date) => *value = Value::Temporal(date.into()),
            Err(_) => return Err(invalid("date", literal)),
        },

        (Type::Time, Value::String(literal)) => match NaiveTime::parse_str(literal) {
            Ok(time) => *value = Value::Temporal(time.into()),
            Err(_) => return Err(invalid("time", literal)),
        },

        (Type::DateTime, Value::String(literal)) => match NaiveDateTime::parse_str(literal) {
            Ok(timestamp) => *value = Value::Temporal(timestamp.into()),
            Err(_) => return Err(invalid("timestamp", literal)),
        },

        (Type::Interval, Value::String(literal)) => match Interval::from_str(literal) {
            Ok(interval) => *value = Value::Interval(interval),
            Err(_) => return Err(invalid("interval", literal)),
        },

        (Type::Uuid, Value::String(literal)) => match Uuid::from_str(literal) {
            Ok(uuid) => *value = Value::Uuid(uuid),
            Err(_) => return Err(invalid("uuid", literal)),
        },

        (Type::Numeric(..), Value::Number(n)) => *value = Value::Numeric(Numeric::from(*n)),
        (Type::Numeric(..), Value::Float(f)) => match Numeric::try_from(*f) {
            Ok(numeric) => *value = Value::Numeric(numeric),
            Err(_) => {
                return Err(DatabaseError::Sql(SqlError::Other(format!("invalid numeric '{f}'"))));
            },
        },

        // REAL columns hold f32 precision
        (Type::Real, Value::Float(f)) => *f = *f as f32 as f64,

        (
            Type::Jsonb,
            Value::String(_) | Value::Boolean(_) | Value::Float(_) | Value::Number(_),
        ) => {
            let strictness = match value {
                Value::String(_) => Conv::NotStrict,
                _ => Conv::Strict,
            };

            let json = json::from_value_to_jsonb(value, strictness)
                .map_err(|e| DatabaseError::Sql(SqlError::Other(format!("invalid json: {e}"))))?;
            let element_type = json
                .element_type()
                .map_err(|e| DatabaseError::Sql(SqlError::Other(format!("invalid json: {e}"))))?;

            *value = json::from_json_to_value(json, element_type, json::OutputFlag::ElementType)
                .map_err(|e| DatabaseError::Sql(SqlError::Other(format!("invalid json: {e}"))))?;
        },

        _ => {},
    }

    Ok(())
}

#[inline(always)]
fn extract_row_id(tuple: &[Value]) -> Result<i64, DatabaseError> {
    match &tuple[0] {
        Value::Number(id) => Ok(*id as i64),
        _ => Err(DatabaseError::Other("row_id must be a number at column 0".into())),
    }
}
