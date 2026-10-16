//! Aggregate evaluation by accumulators, so a group is folded row by row and never held whole, and
//! the grouping strategies that feed them: rows arriving grouped are folded as they stream, any other
//! input goes through a hash table, whose overflow is sorted (hybrid hash aggregation, Graefe 1993)
//!
//! The arithmetic of `SUM`/`AVG` follows the first non-null value: `NUMERIC` sums as numeric,
//! integers sum as integers (`AVG` of integers is numeric), anything else as floating point
//! `MIN` keeps the first of equal minima and `MAX` the last of equal maxima, both are `0` over no
//! values
//! `COUNT(*)` counts rows, `COUNT(x)` non-null values

use super::Tuple;
use super::operator::{Operator, Sort, tuple_bytes};
use crate::collections::hash::HashMap;
use crate::core::numeric::Numeric;
use crate::db::{DatabaseError, Schema};
use crate::sql::Value;
use crate::sql::statement::{Expression, Function, Type};
use crate::storage::tuple::serialize_into;
use crate::vm::expression::{bind, resolve_expression};
use std::cmp::Ordering;
use std::path::Path;

/// The aggregate calls of a select list over rows of `schema`, planned once per query
/// each group keeps only a [State] per call
pub(crate) struct Aggregates<'s> {
    calls: Vec<(Function, Argument)>,
    schema: &'s Schema,
}

/// The rows [Aggregates::fold_hashed] cannot place once its table is full: rows of groups in the
/// table fold in place, rows of new groups come out prefixed with their key, for a [Sort]
struct Overflow<'a> {
    aggregates: &'a Aggregates<'a>,
    source: &'a mut dyn Operator,
    key: &'a [(usize, Type)],
    table: &'a HashMap<Vec<u8>, usize>,
    groups: &'a mut [Group],
    bytes: Vec<u8>,
    pending: Option<Tuple>,
}

enum Argument {
    /// `COUNT()`: nothing is counted
    Nothing,
    /// `COUNT(*)`: every row counts
    Row,
    Column(usize),
    Expression(Expression),
}

pub(crate) enum State {
    Count(i128),
    /// No non-null value seen yet
    Empty,
    Numeric(Numeric, i64),
    Integer(i128, i64),
    Float(f64, i64),
    Extreme(Option<Value>),
}

/// Bytes of first rows and keys [Aggregates::fold_hashed] keeps in memory before it sorts the rest
const AGGREGATE_BUDGET: usize = 8 << 20;

/// A group being folded: its first row and one [State] per aggregate call
pub(crate) type Group = (Tuple, Vec<State>);

/// `expr` with every aggregate call replaced by the next of `values`, in [Aggregates::plan] order
pub(crate) fn substitute(
    expr: &Expression,
    values: &mut impl Iterator<Item = Value>,
) -> Expression {
    match expr {
        Expression::Function { func, .. } if func.is_aggr() => {
            Expression::Value(values.next().expect("one value per aggregate call"))
        },
        Expression::Function { func, args } => Expression::Function {
            func: *func,
            args: args.iter().map(|arg| substitute(arg, values)).collect(),
        },
        Expression::BinaryOperation { operator, left, right } => Expression::BinaryOperation {
            operator: *operator,
            left: Box::new(substitute(left, values)),
            right: Box::new(substitute(right, values)),
        },
        Expression::UnaryOperation { operator, expr } => Expression::UnaryOperation {
            operator: *operator,
            expr: Box::new(substitute(expr, values)),
        },
        Expression::Nested(inner) => Expression::Nested(Box::new(substitute(inner, values))),
        Expression::Alias { expr, alias } => Expression::Alias {
            alias: alias.clone(),
            expr: Box::new(substitute(expr, values)),
        },
        other => other.clone(),
    }
}

/// how many aggregate calls `expr` holds
pub(crate) fn count_calls(expr: &Expression) -> usize {
    let mut calls = Vec::new();
    calls_in(expr, &mut calls);
    calls.len()
}

fn calls_in<'e>(expr: &'e Expression, calls: &mut Vec<(Function, Option<&'e Expression>)>) {
    match expr {
        Expression::Function { func, args } if func.is_aggr() => calls.push((*func, args.first())),
        Expression::Function { args, .. } => args.iter().for_each(|arg| calls_in(arg, calls)),
        Expression::BinaryOperation { left, right, .. } => {
            calls_in(left, calls);
            calls_in(right, calls);
        },
        Expression::UnaryOperation { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => calls_in(expr, calls),
        _ => {},
    }
}

/// `row`'s values at the `key` columns, each encoded under its column type behind a `NULL` tag
fn encode(key: &[(usize, Type)], row: &[Value], bytes: &mut Vec<u8>) {
    bytes.clear();
    for (position, r#type) in key {
        match &row[*position] {
            Value::Null => bytes.push(0),
            value => {
                bytes.push(1);
                serialize_into(bytes, r#type, value);
            },
        }
    }
}

fn fold(function: Function, state: &mut State, value: &Value) {
    match state {
        State::Count(n) => *n += 1,
        State::Extreme(None) => *state = State::Extreme(Some(value.clone())),
        State::Extreme(Some(current)) => {
            let order = value.partial_cmp(current).unwrap_or(Ordering::Equal);
            let replace = match function {
                Function::Min => order == Ordering::Less,
                _ => order != Ordering::Less,
            };
            if replace {
                *current = value.clone();
            }
        },
        State::Empty => {
            *state = match value {
                Value::Numeric(n) => State::Numeric(n.clone(), 1),
                Value::Number(n) => State::Integer(*n, 1),
                Value::Float(f) => State::Float(*f, 1),
                _ => State::Float(0.0, 0),
            }
        },
        State::Numeric(sum, count) => {
            *sum = match value {
                Value::Numeric(n) => &*sum + n,
                Value::Number(n) => &*sum + &Numeric::from(*n),
                Value::Float(f) => &*sum + &Numeric::try_from(*f).unwrap_or(Numeric::zero()),
                _ => return,
            };
            *count += 1;
        },
        State::Integer(sum, count) => {
            if let Value::Number(n) = value {
                *sum += n;
                *count += 1;
            }
        },
        State::Float(sum, count) => match value {
            Value::Float(f) => (*sum, *count) = (*sum + f, *count + 1),
            Value::Number(n) => (*sum, *count) = (*sum + *n as f64, *count + 1),
            _ => {},
        },
    }
}

fn finish(function: Function, state: State) -> Value {
    match (function, state) {
        (_, State::Count(n)) => Value::Number(n),
        (_, State::Extreme(value)) => value.unwrap_or(Value::Number(0)),
        (_, State::Empty) => Value::Null,
        (Function::Sum, State::Numeric(sum, _)) => Value::Numeric(sum),
        (_, State::Numeric(sum, count)) => Value::Numeric(sum / Numeric::from(count)),
        (Function::Sum, State::Integer(sum, _)) => Value::Number(sum),
        (_, State::Integer(sum, count)) => {
            Value::Numeric(Numeric::from(sum) / Numeric::from(count))
        },
        (Function::Sum, State::Float(sum, _)) => Value::Float(sum),
        (_, State::Float(sum, count)) => Value::Float(sum / count as f64),
    }
}

impl<'s> Aggregates<'s> {
    /// the aggregate calls of `expressions`, in the order [substitute] consumes them
    pub fn plan(expressions: &[Expression], schema: &'s Schema) -> Self {
        let mut found = Vec::new();
        expressions.iter().for_each(|expr| calls_in(expr, &mut found));

        let calls = found
            .into_iter()
            .map(|(function, argument)| {
                let column = match argument {
                    Some(Expression::Identifier(name)) => schema.index_of(name),
                    Some(Expression::QualifiedIdentifier { table, column }) => {
                        schema.index_of_qualified(table, column)
                    },
                    _ => None,
                };
                let argument = match (argument, column) {
                    (None, _) => Argument::Nothing,
                    (Some(Expression::Wildcard), _) => Argument::Row,
                    (_, Some(position)) => Argument::Column(position),
                    (Some(expr), _) => {
                        let mut expr = expr.clone();
                        bind(&mut expr, schema);
                        Argument::Expression(expr)
                    },
                };
                (function, argument)
            })
            .collect();

        Self { calls, schema }
    }

    /// fresh states for a new group
    pub fn start(&self) -> Vec<State> {
        self.calls
            .iter()
            .map(|(function, _)| match function {
                Function::Count => State::Count(0),
                Function::Min | Function::Max => State::Extreme(None),
                _ => State::Empty,
            })
            .collect()
    }

    pub fn add(&self, states: &mut [State], row: &[Value]) -> Result<(), DatabaseError> {
        for ((function, argument), state) in self.calls.iter().zip(states) {
            let resolved;
            let value = match argument {
                Argument::Nothing => continue,
                Argument::Row => {
                    fold(*function, state, &Value::Number(1));
                    continue;
                },
                Argument::Column(position) => &row[*position],
                Argument::Expression(expr) => {
                    resolved = resolve_expression(row, self.schema, expr)?;
                    &resolved
                },
            };

            if !value.is_null() {
                fold(*function, state, value);
            }
        }

        Ok(())
    }

    /// folds `source`, whose rows arrive with equal values at the `key` positions next to each other, into `groups`
    pub fn fold_grouped(
        &self,
        source: &mut dyn Operator,
        key: &[usize],
        offset: usize,
        wanted: Option<usize>,
        groups: &mut Vec<Group>,
    ) -> Result<(), DatabaseError> {
        let mut current: Option<Tuple> = None;

        while let Some(mut tuple) = source.next()? {
            let same = current.as_ref().is_some_and(|current| {
                key.iter().zip(current).all(|(&p, value)| tuple[p] == *value)
            });

            if same {
                let (_, states) = groups.last_mut().expect("a group is open");
                self.add(states, &tuple[offset..])?;
                continue;
            }

            if wanted.is_some_and(|wanted| groups.len() == wanted) {
                break;
            }
            current = Some(key.iter().map(|&p| tuple[p].clone()).collect());
            tuple.drain(..offset);
            let mut states = self.start();
            self.add(&mut states, &tuple)?;
            groups.push((tuple, states));
        }

        Ok(())
    }

    /// folds `source` into `groups` through a hash table on the columns `key` (position, declared
    /// type), comparing values encoded under the column type as joins do
    /// Past [AGGREGATE_BUDGET] the table stops growing and rows of new groups are sorted (spilling) and folded grouped after it
    pub fn fold_hashed(
        &self,
        source: &mut dyn Operator,
        key: &[(usize, Type)],
        scratch: &Path,
        groups: &mut Vec<Group>,
    ) -> Result<(), DatabaseError> {
        let mut table: HashMap<Vec<u8>, usize> = HashMap::default();
        let mut bytes = Vec::new();
        let mut used = 0;

        while let Some(row) = source.next()? {
            encode(key, &row, &mut bytes);

            if let Some(&group) = table.get(bytes.as_slice()) {
                self.add(&mut groups[group].1, &row)?;
                continue;
            }

            if used >= AGGREGATE_BUDGET {
                let width = key.len();
                let mut overflow = Overflow {
                    aggregates: self,
                    source,
                    key,
                    table: &table,
                    groups,
                    bytes,
                    pending: Some(row),
                };

                let order = Box::new(move |a: &Tuple, b: &Tuple| a[..width].cmp(&b[..width]));
                let mut sorted = Sort::new(&mut overflow, order, None, scratch)?;
                let positions: Vec<_> = (0..width).collect();
                return self.fold_grouped(&mut sorted, &positions, width, None, groups);
            }

            used += bytes.len() + tuple_bytes(&row) + self.calls.len() * size_of::<State>();
            let mut states = self.start();
            self.add(&mut states, &row)?;
            table.insert(bytes.clone(), groups.len());
            groups.push((row, states));
        }

        Ok(())
    }

    /// the value of every call, in plan order
    pub fn finish(&self, states: Vec<State>) -> Vec<Value> {
        self.calls
            .iter()
            .zip(states)
            .map(|((function, _), state)| finish(*function, state))
            .collect()
    }
}

impl Operator for Overflow<'_> {
    fn next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        loop {
            let row = match self.pending.take() {
                Some(row) => row,
                _ => match self.source.next()? {
                    Some(row) => row,
                    _ => return Ok(None),
                },
            };

            encode(self.key, &row, &mut self.bytes);
            match self.table.get(self.bytes.as_slice()) {
                Some(&group) => self.aggregates.add(&mut self.groups[group].1, &row)?,
                _ => {
                    let mut keyed = Vec::with_capacity(self.key.len() + row.len());
                    keyed.extend(self.key.iter().map(|(p, _)| row[*p].clone()));
                    keyed.extend(row);
                    return Ok(Some(keyed));
                },
            }
        }
    }
}
