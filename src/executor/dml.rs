//! DML execution dispatch.
//!
//! Handles `INSERT`, `UPDATE`, and `DELETE` by draining a source
//! [`Operator`] and calling the engine directly — mutations are not
//! wrapped in the operator pipeline.

use super::operator::Operator;
use super::Executor;
use crate::db::{DatabaseError, Schema, SchemaNew, SqlError};
use crate::sql::statement::{Expression, Type};
use crate::sql::Value;
use crate::vm::planner::Tuple;

impl Executor {
    /// Inserts all tuples from a `values` batch into `table`, allocating
    /// a fresh row id per tuple.
    pub fn insert(
        &self,
        txn_id: i64,
        table: &str,
        values: Vec<Tuple>,
        collect: bool,
    ) -> Result<(usize, Vec<Tuple>), DatabaseError> {
        let schema = self.engine.schema(table)?;
        let mut count = 0;
        let mut written = Vec::new();

        for mut tuple in values {
            let typed = match collect {
                true => tuple.clone(),
                false => Vec::new(),
            };

            for (column, value) in schema.columns.iter().zip(tuple.iter_mut()) {
                coerce_value(&schema, column.column_type(), value)?;
            }

            let row_id = self.engine.next_row_id(table)?;
            self.engine.insert(txn_id, table, row_id, tuple)?;
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

    /// Updates all rows produced by `source` with the given assignments
    /// The source must yield tuples where column 0 is the `row_id`.
    pub fn update(
        &self,
        txn_id: i64,
        table: &str,
        source: &mut dyn Operator,
        assignments: &[(usize, Expression)],
        schema: &Schema,
        collect: bool,
    ) -> Result<(usize, Vec<Tuple>), DatabaseError> {
        use crate::vm::expression::resolve_expression;

        let engine_schema = self.engine.schema(table)?;
        let mut count = 0;
        let mut collected = Vec::new();

        while let Some(mut tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;
            let old = match collect {
                true => tuple.clone(),
                false => Vec::new(),
            };

            // every assignment sees the pre-update row
            let mut new_values = Vec::with_capacity(assignments.len());
            for (col_idx, expr) in assignments {
                let mut value = resolve_expression(&tuple, schema, expr)?;
                // col_idx is into the row_id-prefixed schema
                let column = &engine_schema.columns[col_idx - 1];
                coerce_value(&engine_schema, column.column_type(), &mut value)?;
                new_values.push((*col_idx, value));
            }
            for (col_idx, value) in new_values {
                tuple[col_idx] = value;
            }

            let data = tuple[1..].to_vec();
            self.engine.update(txn_id, table, row_id, data)?;
            count += 1;

            if collect {
                let mut pair = old;
                pair.extend(tuple);
                collected.push(pair);
            }
        }

        Ok((count, collected))
    }
}

pub(super) fn coerce_value(
    schema: &SchemaNew,
    r#type: Type,
    value: &mut Value,
) -> Result<(), DatabaseError> {
    use crate::core::date::{interval::Interval, NaiveDate, NaiveDateTime, NaiveTime, Parse};
    use crate::core::json::{self, Conv};
    use crate::core::numeric::Numeric;
    use crate::core::uuid::Uuid;
    use std::str::FromStr;

    let invalid = |kind: &str, literal: &str| {
        DatabaseError::Sql(SqlError::Other(format!("invalid {kind} '{literal}'")))
    };

    match (r#type, &mut *value) {
        (Type::Enum(id), Value::String(literal)) => {
            let variants = schema
                .enum_variants(id)
                .expect("enum id registered at schema build");

            match variants
                .iter()
                .position(|variant| variant.as_str() == &**literal)
            {
                Some(idx) => *value = Value::Enum(idx as u8),
                None => return Err(invalid("enum variant", &**literal)),
            }
        }

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
                return Err(DatabaseError::Sql(SqlError::Other(format!(
                    "invalid numeric '{f}'"
                ))))
            }
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
        }

        _ => {}
    }

    Ok(())
}

#[inline(always)]
fn extract_row_id(tuple: &[Value]) -> Result<i64, DatabaseError> {
    match &tuple[0] {
        Value::Number(id) => Ok(*id as i64),
        _ => Err(DatabaseError::Other(
            "row_id must be a number at column 0".into(),
        )),
    }
}
