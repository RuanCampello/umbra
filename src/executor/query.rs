//! SELECT query pipeline builder.
//!
//! Translates a parsed `Select` AST node into an operator pipeline:
//! `Scan → Filter → Sort → Project → Limit`.

use super::operator::{self, Operator};
use super::Executor;
use crate::db::{self, DatabaseError, Schema};
use crate::sql::analyzer::contains_aggregate;
use crate::sql::query::planner::resolve_type;
use crate::sql::statement::{self, Column, Expression, OrderDirection};
use crate::sql::Value;
use crate::vm::expression::resolve_expression;
use crate::vm::planner::{reduce_aggregate_expr, Tuple};

impl Executor {
    /// Builds and drains a SELECT operator pipeline, returning the projected
    /// result schema and all result rows.
    pub fn execute_select(
        &self,
        txn_id: i64,
        select: statement::Select,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        if !select.group_by.is_empty() || select.columns.iter().any(contains_aggregate) {
            return self.execute_aggregate(txn_id, select);
        }

        let (schema, mut pipeline): (Schema, Box<dyn Operator>) =
            match self.joined_source(txn_id, &select)? {
                Some((schema, rows)) => (schema, Box::new(operator::Values::new(rows))),
                None => {
                    let table = &select.from.name;
                    let schema = self.resolve_schema(table)?;

                    let indexed = match &select.r#where {
                        Some(predicate) => self.index_point_lookup(txn_id, table, predicate)?,
                        None => None,
                    };

                    let source: Box<dyn Operator> = match indexed {
                        Some(rows) => Box::new(operator::Values::new(rows)),
                        None => Box::new(operator::Scan::new(&self.engine, txn_id, table)?),
                    };

                    (schema, source)
                }
            };

        if let Some(predicate) = select.r#where {
            pipeline = Box::new(operator::Filter::new(pipeline, schema.clone(), predicate));
        }

        if !select.order_by.is_empty() {
            let sort_schema = schema.clone();
            let order_by = select.order_by;

            let comparator = build_comparator(&sort_schema, &order_by);
            pipeline = Box::new(operator::Sort::new(pipeline, comparator)?);
        }

        let indices = resolve_projection(&schema, &select.columns);
        let result_schema = Schema::new(
            indices
                .iter()
                .map(|&idx| schema.columns[idx].clone())
                .collect(),
        );
        pipeline = Box::new(operator::Project::new(pipeline, indices));

        if select.limit.is_some() || select.offset.is_some() {
            let limit = select.limit.unwrap_or(usize::MAX);
            let offset = select.offset.unwrap_or(0);
            pipeline = Box::new(operator::Limit::new(pipeline, limit, offset));
        }

        let mut results = Vec::new();
        while let Some(tuple) = pipeline.next()? {
            results.push(tuple);
        }

        Ok((result_schema, results))
    }

    /// Materialises the join of the base table with every `JOIN` clause,
    /// returning the combined schema and rows. `None` when the SELECT has no
    /// joins. The base tuples keep their `row_id` prefix, joined tables
    /// contribute their user columns only, qualified by table key.
    ///
    /// PERFORMANCE: nested-loop evaluation of the ON condition, port the
    /// legacy hash-join equi-key fast path when profiling if justifies it :D
    fn joined_source(
        &self,
        txn_id: i64,
        select: &statement::Select,
    ) -> Result<Option<(Schema, Vec<Tuple>)>, DatabaseError> {
        use crate::sql::statement::JoinType;
        use crate::vm::expression::evaluate_where;

        if select.joins.is_empty() {
            return Ok(None);
        }

        let base = &select.from.name;
        let mut schema = self.resolve_schema(base)?;
        schema.add_qualified_name(select.from.key(), 0, schema.len());

        let mut scan = operator::Scan::new(&self.engine, txn_id, base)?;
        let mut rows = Vec::new();
        while let Some(tuple) = scan.next()? {
            rows.push(tuple);
        }

        for join in &select.joins {
            let right_table = &join.table.name;
            let right_schema = self
                .engine
                .schema(right_table)
                .map_err(|_| DatabaseError::Sql(db::SqlError::InvalidTable(right_table.clone())))?;

            let right_columns: Vec<_> = right_schema
                .columns
                .iter()
                .map(|col| statement::Column::new(col.name(), col.column_type()))
                .collect();
            let right_width = right_columns.len();

            let start = schema.len();
            schema.extend_with_join(right_columns, &join.join_type);
            schema.add_qualified_name(join.table.key(), start, schema.len());

            let right_rows: Vec<_> = self
                .engine
                .scan(txn_id, right_table)?
                .into_iter()
                .map(|(_, tuple)| tuple)
                .collect();

            let left_width = start;
            let mut joined = Vec::new();
            let mut right_matched = vec![false; right_rows.len()];

            for left in &rows {
                let mut matched = false;

                for (idx, right) in right_rows.iter().enumerate() {
                    let mut combined = Vec::with_capacity(left_width + right_width);
                    combined.extend(left.iter().cloned());
                    combined.extend(right.iter().cloned());

                    if evaluate_where(&schema, &combined, &join.on)? {
                        matched = true;
                        right_matched[idx] = true;
                        joined.push(combined);
                    }
                }

                if !matched && matches!(join.join_type, JoinType::Left | JoinType::Full) {
                    let mut combined = left.clone();
                    combined.extend(std::iter::repeat(Value::Null).take(right_width));
                    joined.push(combined);
                }
            }

            if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                for (idx, right) in right_rows.iter().enumerate() {
                    if !right_matched[idx] {
                        let mut combined = vec![Value::Null; left_width];
                        combined.extend(right.iter().cloned());
                        joined.push(combined);
                    }
                }
            }

            rows = joined;
        }

        Ok(Some((schema, rows)))
    }

    /// Executes a SELECT with aggregate functions and/or GROUP BY.
    ///
    /// Materialises the filtered rows, groups them, and evaluates each select column per group,
    /// aggregates via the shared [reduce_aggregate_expr], group keys via
    /// [resolve_expression] against the group's first row
    fn execute_aggregate(
        &self,
        txn_id: i64,
        select: statement::Select,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        let (schema, mut pipeline): (Schema, Box<dyn Operator>) =
            match self.joined_source(txn_id, &select)? {
                Some((schema, rows)) => (schema, Box::new(operator::Values::new(rows))),
                None => {
                    let table = &select.from.name;
                    let schema = self.resolve_schema(table)?;
                    let scan: Box<dyn Operator> =
                        Box::new(operator::Scan::new(&self.engine, txn_id, table)?);

                    (schema, scan)
                }
            };

        if let Some(predicate) = &select.r#where {
            pipeline = Box::new(operator::Filter::new(
                pipeline,
                schema.clone(),
                predicate.clone(),
            ));
        }

        let mut rows = Vec::new();
        while let Some(tuple) = pipeline.next()? {
            rows.push(tuple);
        }

        let groups = match select.group_by.is_empty() {
            true => vec![rows],
            false => {
                let mut keyed: Vec<(_, _)> = rows
                    .into_iter()
                    .map(|row| {
                        let key = select
                            .group_by
                            .iter()
                            .map(|expr| resolve_expression(&row, &schema, expr))
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok::<_, DatabaseError>((key, row))
                    })
                    .collect::<Result<_, _>>()?;

                keyed.sort_by(|(a, _), (b, _)| {
                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                });

                let mut groups: Vec<Vec<Tuple>> = Vec::new();
                let mut current_key = None;
                for (key, row) in keyed {
                    match current_key.as_ref() == Some(&key) {
                        true => groups.last_mut().expect("run started").push(row),
                        false => {
                            current_key = Some(key);
                            groups.push(vec![row]);
                        }
                    }
                }

                groups
            }
        };

        let out_schema = Schema::new(
            select
                .columns
                .iter()
                .map(|expr| {
                    let (name, inner) = match expr {
                        Expression::Alias { alias, expr } => (alias.clone(), expr.as_ref()),
                        Expression::Function { func, .. } => (func.to_string(), expr),
                        other => (other.to_string(), expr),
                    };

                    Ok(Column::new(&name, resolve_type(&schema, inner)?))
                })
                .collect::<Result<Vec<_>, db::SqlError>>()?,
        );

        let empty_row: Tuple = Vec::new();
        let mut out_rows = Vec::with_capacity(groups.len());
        for group in groups {
            let tuple = select
                .columns
                .iter()
                .map(|expr| {
                    let expr = match expr {
                        Expression::Alias { expr, .. } => expr.as_ref(),
                        other => other,
                    };

                    match contains_aggregate(expr) {
                        true => {
                            let reduced = reduce_aggregate_expr(expr, &group, &schema)?;
                            resolve_expression(&empty_row, &schema, &reduced)
                                .map_err(DatabaseError::from)
                        }
                        false => {
                            let row = group.first().unwrap_or(&empty_row);
                            resolve_expression(row, &schema, expr).map_err(DatabaseError::from)
                        }
                    }
                })
                .collect::<Result<Tuple, DatabaseError>>()?;

            out_rows.push(tuple);
        }

        if !select.order_by.is_empty() {
            let comparator = build_comparator(&out_schema, &select.order_by);
            out_rows.sort_by(|a, b| comparator(a, b));
        }

        let offset = select.offset.unwrap_or(0);
        let limit = select.limit.unwrap_or(usize::MAX);
        let out_rows = out_rows.into_iter().skip(offset).take(limit).collect();

        Ok((out_schema, out_rows))
    }

    /// Serves `WHERE column = literal` from a covering index when the
    /// transaction has no local writes on the table (index scans bypass the
    /// read-your-own-writes overlay). Returns `row_id`-prefixed tuples.
    fn index_point_lookup(
        &self,
        txn_id: i64,
        table: &str,
        predicate: &Expression,
    ) -> Result<Option<Vec<Tuple>>, DatabaseError> {
        use crate::sql::statement::BinaryOperator;

        let Expression::BinaryOperation {
            left,
            operator: BinaryOperator::Eq,
            right,
        } = predicate
        else {
            return Ok(None);
        };

        let (column, value) = match (&**left, &**right) {
            (Expression::Identifier(column), Expression::Value(value))
            | (Expression::Value(value), Expression::Identifier(column)) => (column, value),
            _ => return Ok(None),
        };

        if self.engine.has_local_writes(txn_id, table) {
            return Ok(None);
        }

        let engine_schema = self.engine.schema(table)?;
        let Some(col) = engine_schema
            .columns
            .iter()
            .position(|c| c.name() == column)
        else {
            return Ok(None);
        };

        let Some(index) = self.engine.index_for_column(table, col)? else {
            return Ok(None);
        };

        let rows = self
            .engine
            .scan_index(txn_id, table, &index, value)?
            .into_iter()
            .map(|(row_id, tuple)| {
                let mut row = Vec::with_capacity(tuple.len() + 1);
                row.push(Value::Number(row_id as i128));
                row.extend(tuple);
                row
            })
            .collect();

        Ok(Some(rows))
    }

    /// Resolves the old `Schema` (column list) for a table from the engine's
    /// stored schema, with `row_id` prepended to match [operator::Scan] tuples.
    pub(crate) fn resolve_schema(&self, table: &str) -> Result<Schema, DatabaseError> {
        let schema_new = self.engine.schema(table)?;

        let columns: Vec<_> = schema_new
            .columns
            .iter()
            .map(|col| statement::Column::new(col.name(), col.column_type()))
            .collect();

        let mut schema = Schema::new(columns);
        schema.prepend_id();

        Ok(schema)
    }
}

fn resolve_projection(schema: &Schema, columns: &[Expression]) -> Vec<usize> {
    if columns.len() == 1 && columns[0] == Expression::Wildcard {
        return (1..schema.len()).collect();
    }

    columns
        .iter()
        .filter_map(|expr| column_position(schema, expr))
        .collect()
}

/// resolves plain, qualified, and aliased column references to a position
fn column_position(schema: &Schema, expr: &Expression) -> Option<usize> {
    match expr {
        Expression::Identifier(name) => schema.index_of(name),
        Expression::QualifiedIdentifier { table, column } => {
            schema.index_of_qualified(table, column)
        }
        Expression::Alias { expr, .. } => column_position(schema, expr),
        _ => None,
    }
}

fn build_comparator(
    schema: &Schema,
    order_by: &[statement::OrderBy],
) -> Box<dyn Fn(&Tuple, &Tuple) -> std::cmp::Ordering> {
    let sort_keys: Vec<(usize, bool)> = order_by
        .iter()
        .filter_map(|ob| {
            let idx = column_position(schema, &ob.expr)?;
            let desc = ob.direction == OrderDirection::Desc;

            Some((idx, desc))
        })
        .collect();

    Box::new(move |a: &Tuple, b: &Tuple| {
        for &(idx, desc) in &sort_keys {
            let ord = a[idx]
                .partial_cmp(&b[idx])
                .unwrap_or(std::cmp::Ordering::Equal);
            if ord != std::cmp::Ordering::Equal {
                return if desc { ord.reverse() } else { ord };
            }
        }

        std::cmp::Ordering::Equal
    })
}
