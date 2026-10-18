//! DML execution dispatch
//!
//! Handles `INSERT`, `UPDATE`, and `DELETE` by draining a source
//! [Operator] and calling the engine directly, mutations are not
//! wrapped in the operator pipeline

use super::operator::Operator;
use super::{Executor, Tuple};
use crate::db::{Column, DatabaseError, ForeignKey, Schema, SqlError};
use crate::sql::Value;
use crate::sql::statement::{Expression, OnDelete, Type};
use crate::storage::engine::error::EngineError;
use crate::storage::engine::table::{Row, RowId, Table, View};
use crate::storage::engine::transaction::Transaction;
use crate::vm::expression::{bind, evaluate_where, resolve_expression};
use std::sync::Arc;

/// the rows a DML statement targets: its table, its `WHERE` clause and the row-id-first schema
/// the clause is evaluated against
pub(crate) struct Target<'a> {
    pub table: &'a str,
    pub predicate: Option<&'a Expression>,
    pub schema: &'a Schema,
}

/// A foreign key resolved for one statement: rows of `referring` hold in `column` a value of
/// `referenced`'s column `key`
struct Reference {
    referring: Arc<Table>,
    column: usize,
    referenced: Arc<Table>,
    key: usize,
    on_delete: OnDelete,
}

/// The foreign keys a statement must keep: those its table holds and those referring to it, with the view their checks read
struct Integrity {
    held: Vec<Reference>,
    referring: Vec<Reference>,
    view: Option<View>,
}

impl Executor {
    /// inserts all tuples from a `values` batch into `table`
    /// with `collect`, returns the typed tuples written, row id first
    pub fn insert(
        &self,
        txn: &mut Transaction,
        table: &str,
        values: Vec<Tuple>,
        collect: bool,
    ) -> Result<(usize, Vec<Tuple>), DatabaseError> {
        let table = self.table(table)?;
        let checks = bound_checks(&table);
        let integrity = self.integrity(txn, &table, false)?;
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
            check(&table, &checks, &tuple)?;

            let row: Row = tuple.into();
            let row_id = txn.insert(&table, Arc::clone(&row))?;
            integrity.hold(txn, None, &row)?;
            count += 1;

            if collect {
                let mut row = Vec::with_capacity(typed.len() + 1);
                row.push(Value::Number(row_id as i64));
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
        let table = self.table(target.table)?;
        let integrity = self.integrity(txn, &table, true)?;
        let mut count = 0;

        while let Some(mut tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;

            loop {
                match txn.delete(&table, row_id) {
                    Ok(_) => {
                        self.release(txn, &integrity, &tuple[1..])?;
                        count += 1;
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
        let table = self.table(target.table)?;
        let checks = bound_checks(&table);
        let integrity = self.integrity(txn, &table, true)?;
        let mut count = 0;
        let mut collected = Vec::new();

        while let Some(mut tuple) = source.next()? {
            let row_id = extract_row_id(&tuple)?;

            loop {
                let new = assign(&table, &tuple, assignments, target.schema)?;
                check(&table, &checks, &new[1..])?;
                match txn.update(&table, row_id, new[1..].into()) {
                    Ok(_) => {
                        integrity.hold(txn, Some(&tuple[1..]), &new[1..])?;
                        integrity.keep(txn, &tuple[1..], &new[1..])?;
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

impl Executor {
    /// the foreign keys a write to `table` must keep, `removing` adds those referring to it
    fn integrity(
        &self,
        txn: &mut Transaction,
        table: &Arc<Table>,
        removing: bool,
    ) -> Result<Integrity, DatabaseError> {
        let resolve =
            |referring: &Arc<Table>, key: &ForeignKey| -> Result<Reference, DatabaseError> {
                let referenced = match key.table == referring.name() {
                    true => Arc::clone(referring),
                    false => self.table(&key.table)?,
                };
                let column = referenced.schema.index_of(&key.referenced).ok_or_else(|| {
                    SqlError::InvalidQualifiedColumn {
                        table: key.table.clone(),
                        column: key.referenced.clone(),
                    }
                })?;

                let referring = Arc::clone(referring);
                let (key, column, on_delete) = (column, key.column, key.on_delete);
                let reference = Reference { referring, column, referenced, key, on_delete };

                Ok(reference)
            };

        let held = table
            .schema
            .constraints
            .foreign_keys
            .iter()
            .map(|key| resolve(table, key))
            .collect::<Result<Vec<_>, _>>()?;

        let mut referring = Vec::new();
        if removing {
            for other in txn.tables() {
                for key in other
                    .schema
                    .constraints
                    .foreign_keys
                    .iter()
                    .filter(|key| key.table == table.name())
                {
                    referring.push(resolve(&other, key)?);
                }
            }
        }

        let view = (!held.is_empty() || !referring.is_empty()).then(|| txn.constraint_view());
        Ok(Integrity { held, referring, view })
    }

    /// keeps the foreign keys referring to a row just deleted: refused under RESTRICT, the referring rows deleted under CASCADE
    fn release(
        &self,
        txn: &mut Transaction,
        integrity: &Integrity,
        row: &[Value],
    ) -> Result<(), DatabaseError> {
        let Some(view) = integrity.view else {
            return Ok(());
        };

        for reference in &integrity.referring {
            let key = &row[reference.key];
            if key.is_null() {
                continue;
            }

            let rows = txn.referencing(view, &reference.referring, reference.column, key)?;
            match (reference.on_delete, rows.is_empty()) {
                (_, true) => {},
                (OnDelete::Restrict, _) => {
                    return Err(violation(reference, key, "is still referenced from"));
                },
                (OnDelete::Cascade, _) => {
                    let nested = self.integrity(txn, &reference.referring, true)?;
                    for (id, row) in rows {
                        txn.delete(&reference.referring, id)?;
                        self.release(txn, &nested, &row)?;
                    }
                },
            }
        }
        Ok(())
    }
}

impl Integrity {
    /// checks that every changed, non-NULL foreign key of a row just written refers to a row
    fn hold(
        &self,
        txn: &mut Transaction,
        before: Option<&[Value]>,
        row: &[Value],
    ) -> Result<(), DatabaseError> {
        let Some(view) = self.view else {
            return Ok(());
        };

        for reference in &self.held {
            let key = &row[reference.column];
            if key.is_null() || before.is_some_and(|before| before[reference.column] == *key) {
                continue;
            }
            if !txn.referenced(view, &reference.referenced, reference.key, key)? {
                return Err(violation(reference, key, "refers to no row of"));
            }
        }
        Ok(())
    }

    /// refuses an update taking away a key other rows still refer to (no cascade on update)
    fn keep(
        &self,
        txn: &mut Transaction,
        before: &[Value],
        after: &[Value],
    ) -> Result<(), DatabaseError> {
        let Some(view) = self.view else {
            return Ok(());
        };

        for reference in &self.referring {
            let key = &before[reference.key];
            if key.is_null() || after[reference.key] == *key {
                continue;
            }
            if !txn.referencing(view, &reference.referring, reference.column, key)?.is_empty() {
                return Err(violation(reference, key, "is still referenced from"));
            }
        }
        Ok(())
    }
}

fn violation(reference: &Reference, key: &Value, what: &str) -> DatabaseError {
    let (referring, referenced) = (&reference.referring, &reference.referenced);
    let message = format!(
        "foreign key violation: {}.{} = {key} {what} {}.{}",
        referring.name(),
        referring.schema.columns[reference.column].name(),
        referenced.name(),
        referenced.schema.columns[reference.key].name(),
    );

    DatabaseError::Sql(SqlError::Other(message))
}

/// the table's CHECK predicates bound to its row layout, once per statement
fn bound_checks(table: &Table) -> Vec<Expression> {
    let mut checks = table.schema.constraints.checks.clone();
    checks.iter_mut().for_each(|check| bind(check, &table.schema));
    checks
}

/// fails when `row` makes one of the table's CHECK predicates false, unknown passes as in SQL
fn check(table: &Table, checks: &[Expression], row: &[Value]) -> Result<(), DatabaseError> {
    for (bound, check) in checks.iter().zip(&table.schema.constraints.checks) {
        if resolve_expression(row, &table.schema, bound)? == Value::Boolean(false) {
            let message = format!("new row of {} violates CHECK ({check})", table.name());
            return Err(DatabaseError::Sql(SqlError::Other(message)));
        }
    }
    Ok(())
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
    tuple.push(Value::Number(row_id as i64));
    tuple.extend(latest.iter().cloned());

    match target.predicate {
        Some(predicate) if !evaluate_where(target.schema, &tuple, predicate)? => Ok(None),
        _ => Ok(Some(tuple)),
    }
}

/// `value` converted to the representation of `column`'s type
pub(crate) fn coerce_value(column: &Column, value: &mut Value) -> Result<(), DatabaseError> {
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
        (Type::Real, Value::Number(n)) => *value = Value::Float(*n as f32 as f64),
        (Type::DoublePrecision, Value::Number(n)) => *value = Value::Float(*n as f64),
        (Type::Real, Value::Numeric(n)) => *value = Value::Float(f64::from(n.clone()) as f32 as f64),
        (Type::DoublePrecision, Value::Numeric(n)) => *value = Value::Float(f64::from(n.clone())),

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
