//! SELECT query pipeline builder.
//!
//! Translates a parsed `Select` AST node into an operator pipeline:
//! `Scan -> Filter -> Sort -> Project -> Limit`.

use super::operator::{self, Operator};
use super::Executor;
use crate::db::{self, DatabaseError, Schema};
use crate::executor::dml;
use crate::executor::operator::{Evaluate, Filter, Limit, Project, Scan, Sort, Values};
use crate::sql::analyzer::contains_aggregate;
use crate::sql::query::planner::resolve_type;
use crate::sql::statement::{self, BinaryOperator, Column, Expression, OrderDirection, Type};
use crate::sql::Value;
use crate::vm::expression::resolve_expression;
use crate::vm::planner::{reduce_aggregate_expr, Tuple};
use std::cmp::Ordering;
use std::ops::Bound;

/// The index-servable shape of a `WHERE` predicate on a single column: either a
/// contiguous range or a discrete set of points (`IN (..)`, `col = a OR col = b`)
enum Access<'a> {
    Range(&'a str, Bound<Value>, Bound<Value>),
    Points(&'a str, Vec<Value>),
}

impl Access<'_> {
    #[inline]
    const fn column(&self) -> &str {
        match self {
            Self::Range(column, ..) | Self::Points(column, ..) => column,
        }
    }
}

impl Executor {
    /// Builds and drains a SELECT operator pipeline, returning the projected
    /// result schema and all result rows.
    pub fn execute_select(
        &self,
        txn_id: i64,
        mut select: statement::Select,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        if !select.group_by.is_empty() || select.columns.iter().any(contains_aggregate) {
            return self.execute_aggregate(txn_id, select);
        }

        // a lazy scan only pays off when a `LIMIT` can stop the pipeline early
        // an `ORDER BY` interposes a `Sort` that drains every row, so stay eager
        let streaming = select.limit.is_some() && select.order_by.is_empty();
        let (schema, mut pipeline) = self.select_source(txn_id, &mut select, streaming)?;

        let result_schema;

        match select.columns.as_slice() {
            // lone `*` keeps the identity projection: sort on the input
            // schema, then strip the row_id prefix
            [Expression::Wildcard] => {
                if !select.order_by.is_empty() {
                    let comparator = build_comparator(&schema, &select.order_by);
                    pipeline = Box::new(Sort::new(pipeline, comparator)?);
                }

                let indices: Vec<_> = (1..schema.len()).collect();
                result_schema = Schema::new(
                    indices
                        .iter()
                        .map(|&idx| schema.columns[idx].clone())
                        .collect(),
                );
                pipeline = Box::new(Project::new(pipeline, indices));
            }
            _ => {
                let mut items: Vec<_> = select.columns.clone();
                let visible = items.len();

                // sort keys reference a projected column when they can,
                // otherwise they ride along as hidden columns
                let mut sort_keys = Vec::with_capacity(select.order_by.len());
                for order in &select.order_by {
                    let position = items[..visible]
                        .iter()
                        .position(|item| projection_matches(item, &order.expr))
                        .unwrap_or_else(|| {
                            items.push(order.expr.clone());
                            items.len() - 1
                        });

                    sort_keys.push((position, order.direction == OrderDirection::Desc));
                }

                let hidden = items.len() > visible;

                result_schema = projection_schema(&schema, &items[..visible])?;
                pipeline = Box::new(Evaluate::new(pipeline, schema.clone(), items));

                if !sort_keys.is_empty() {
                    pipeline = Box::new(Sort::new(pipeline, comparator_for(sort_keys))?);
                }

                if hidden {
                    pipeline = Box::new(Project::new(pipeline, (0..visible).collect()));
                }
            }
        }

        if select.limit.is_some() || select.offset.is_some() {
            let limit = select.limit.unwrap_or(usize::MAX);
            let offset = select.offset.unwrap_or(0);
            pipeline = Box::new(Limit::new(pipeline, limit, offset));
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

        // single-table WHERE filters only push into the scans of an all-INNER
        // join, where narrowing an input never changes the final result
        let all_inner = select
            .joins
            .iter()
            .all(|join| matches!(join.join_type, JoinType::Inner));

        let base = &select.from.name;
        let mut schema = self.resolve_schema(base)?;
        let base_filter_schema = schema.clone();
        schema.add_qualified_name(select.from.key(), 0, schema.len());

        let base_pushed = all_inner
            .then(|| pushable_predicate(select.r#where.as_ref(), select.from.key()))
            .flatten();
        let mut source = self.filtered_source(
            txn_id,
            base,
            base_pushed.as_ref(),
            &base_filter_schema,
            false,
        )?;
        let mut rows = Vec::new();
        while let Some(tuple) = source.next()? {
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
                .map(|col| {
                    let mut column = Column::new(col.name(), col.column_type());
                    if let Type::Enum(id) = col.column_type() {
                        let variants = right_schema
                            .enum_variants(id)
                            .expect("enum id registered at schema build")
                            .to_vec();
                        column.data_type = Type::Enum(schema.add_enum(variants.clone()));
                        column.type_def = Some(variants);
                    }
                    column
                })
                .collect();
            let right_width = right_columns.len();

            let start = schema.len();
            schema.extend_with_join(right_columns, &join.join_type);
            schema.index_bare_names(start, schema.len());
            schema.add_qualified_name(join.table.key(), start, schema.len());

            let right_pushed = all_inner
                .then(|| pushable_predicate(select.r#where.as_ref(), join.table.key()))
                .flatten();

            let right_rows: Vec<_> = match right_pushed {
                Some(pushed) => {
                    let right_filter_schema = self.resolve_schema(right_table)?;
                    let mut right_source = self.filtered_source(
                        txn_id,
                        right_table,
                        Some(&pushed),
                        &right_filter_schema,
                        false,
                    )?;

                    let mut right_rows = Vec::new();
                    while let Some(mut tuple) = right_source.next()? {
                        tuple.remove(0);
                        right_rows.push(tuple);
                    }
                    right_rows
                }
                None => self
                    .engine
                    .scan(txn_id, right_table)?
                    .into_iter()
                    .map(|(_, tuple)| tuple)
                    .collect(),
            };

            let left_width = start;
            let mut joined = Vec::new();
            let mut right_matched = vec![false; right_rows.len()];

            match equi_join_positions(&join.on, &schema, left_width) {
                // hash join: build on the right side, probe with the left
                // keys hash their serialised bytes under the left column's
                // type, so cross-representation values still collide
                Some((left_pos, right_pos)) => {
                    let key_type = schema.columns[left_pos].data_type;
                    let mut table: crate::collections::hash::HashMap<Vec<u8>, Vec<usize>> =
                        crate::collections::hash::HashMap::default();

                    for (idx, right) in right_rows.iter().enumerate() {
                        let key = &right[right_pos];
                        if !key.is_null() {
                            table
                                .entry(crate::storage::tuple::serialize(&key_type, key))
                                .or_default()
                                .push(idx);
                        }
                    }

                    for left in &rows {
                        let key = &left[left_pos];
                        let matches = match key.is_null() {
                            true => None,
                            false => table.get(&crate::storage::tuple::serialize(&key_type, key)),
                        };

                        match matches {
                            Some(indices) => {
                                for &idx in indices {
                                    right_matched[idx] = true;
                                    let mut combined = Vec::with_capacity(left_width + right_width);
                                    combined.extend(left.iter().cloned());
                                    combined.extend(right_rows[idx].iter().cloned());
                                    joined.push(combined);
                                }
                            }
                            None if matches!(join.join_type, JoinType::Left | JoinType::Full) => {
                                let mut combined = left.clone();
                                combined.extend(std::iter::repeat(Value::Null).take(right_width));
                                joined.push(combined);
                            }
                            None => {}
                        }
                    }
                }
                None => {
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

    /// Builds the filtered row source shared by plain and aggregate SELECTs:
    /// the joined result when the statement has joins, otherwise the
    /// (possibly index-driven) base table
    fn select_source(
        &self,
        txn_id: i64,
        select: &mut statement::Select,
        streaming: bool,
    ) -> Result<(Schema, Box<dyn Operator>), DatabaseError> {
        match self.joined_source(txn_id, select)? {
            Some((schema, rows)) => {
                let mut source: Box<dyn Operator> = Box::new(Values::new(rows));
                if let Some(mut predicate) = select.r#where.take() {
                    dealias_predicate(&mut predicate, &select.columns, &schema);
                    source = Box::new(Filter::new(source, schema.clone(), predicate));
                }

                Ok((schema, source))
            }
            None => {
                let table = &select.from.name;
                let schema = self.resolve_schema(table)?;
                if let Some(predicate) = select.r#where.as_mut() {
                    dealias_predicate(predicate, &select.columns, &schema);
                }
                let source = self.filtered_source(
                    txn_id,
                    table,
                    select.r#where.as_ref(),
                    &schema,
                    streaming,
                )?;

                Ok((schema, source))
            }
        }
    }

    /// Executes a SELECT with aggregate functions and/or GROUP BY.
    ///
    /// Materialises the filtered rows, groups them, and evaluates each select column per group,
    /// aggregates via the shared [reduce_aggregate_expr], group keys via
    /// [resolve_expression] against the group's first row
    fn execute_aggregate(
        &self,
        txn_id: i64,
        mut select: statement::Select,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        let (schema, mut pipeline) = self.select_source(txn_id, &mut select, false)?;

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
                            .map(|expr| {
                                resolve_expression(&row, &schema, dealias(expr, &select.columns))
                            })
                            .collect::<Result<Vec<_>, _>>()?;
                        Ok::<_, DatabaseError>((key, row))
                    })
                    .collect::<Result<_, _>>()?;

                keyed.sort_by(|(a, _), (b, _)| a.partial_cmp(b).unwrap_or(Ordering::Equal));

                let mut groups: Vec<Vec<_>> = Vec::new();
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

        let out_schema = projection_schema(&schema, &select.columns)?;

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

    pub(crate) fn filtered_source(
        &self,
        txn_id: i64,
        table: &str,
        predicate: Option<&Expression>,
        schema: &Schema,
        streaming: bool,
    ) -> Result<Box<dyn Operator>, DatabaseError> {
        let indexed = match predicate {
            Some(predicate) => self.indexed_rows(txn_id, table, predicate)?,
            None => None,
        };

        let mut source: Box<dyn Operator> = match indexed {
            Some(rows) => Box::new(Values::new(rows)),
            None if streaming => Box::new(Scan::streaming(&self.engine, txn_id, table)?),
            None => Box::new(Scan::new(&self.engine, txn_id, table)?),
        };

        if let Some(predicate) = predicate {
            source = Box::new(Filter::new(source, schema.clone(), predicate.clone()));
        }

        Ok(source)
    }

    /// Serves an indexable predicate from a covering index when the transaction
    /// has no local writes on the table: `AND`ed bounds (`=`, `<`, `<=`, `>`,
    /// `>=`) collapse to a range, and `IN (..)` / `col = a OR col = b` on one
    /// column collapse to a set of point look-ups
    fn indexed_rows(
        &self,
        txn_id: i64,
        table: &str,
        predicate: &Expression,
    ) -> Result<Option<Vec<Tuple>>, DatabaseError> {
        if self.engine.has_local_writes(txn_id, table) {
            return Ok(None);
        }

        let Some(access) = index_access(predicate) else {
            return Ok(None);
        };

        let engine_schema = self.engine.schema(table)?;
        let Some(col) = engine_schema
            .columns
            .iter()
            .position(|c| c.name() == access.column())
        else {
            return Ok(None);
        };

        let Some(index) = self.engine.index_for_column(table, col)? else {
            return Ok(None);
        };

        // enum values are indexed by variant index, not by their literal
        let column_type = engine_schema.columns[col].column_type();

        let matches = match access {
            Access::Range(_, mut start, mut end) => {
                for bound in [&mut start, &mut end] {
                    if let Bound::Included(value) | Bound::Excluded(value) = bound {
                        dml::coerce_value(&engine_schema, column_type, value)?;
                    }
                }

                match (&start, &end) {
                    (Bound::Included(a), Bound::Included(b)) if a == b => {
                        self.engine.scan_index(txn_id, table, &index, a)?
                    }
                    _ => self
                        .engine
                        .scan_index_range(txn_id, table, &index, start, end)?,
                }
            }
            Access::Points(_, mut values) => {
                let mut matches = Vec::new();
                for value in &mut values {
                    dml::coerce_value(&engine_schema, column_type, value)?;
                    matches.extend(self.engine.scan_index(txn_id, table, &index, value)?);
                }
                matches
            }
        };

        let mut rows: Vec<_> = matches
            .into_iter()
            .map(|(row_id, tuple)| {
                let mut row = Vec::with_capacity(tuple.len() + 1);
                row.push(Value::Number(row_id as i128));
                row.extend(tuple);
                row
            })
            .collect();

        rows.sort_unstable_by(|a, b| a[0].partial_cmp(&b[0]).unwrap_or(Ordering::Equal));
        rows.dedup_by(|a, b| a[0] == b[0]);

        Ok(Some(rows))
    }

    /// Describes the pipeline `execute_select` would build, one line per
    /// operator in execution order.
    pub(crate) fn explain_select(
        &self,
        select: &statement::Select,
    ) -> Result<Vec<String>, DatabaseError> {
        let mut lines = vec![self.access_path(&select.from.name, select.r#where.as_ref())?];

        for join in &select.joins {
            let strategy = match &join.on {
                Expression::BinaryOperation {
                    operator: BinaryOperator::Eq,
                    left,
                    right,
                } if is_column_ref(left) && is_column_ref(right) => "HashJoin",
                _ => "NestedLoopJoin",
            };

            lines.push(format!(
                "{strategy} ({:?}) with {} on ({})",
                join.join_type, join.table.name, join.on
            ));
        }

        if let Some(predicate) = &select.r#where {
            lines.push(format!("Filter ({predicate})"));
        }

        if !select.group_by.is_empty() || select.columns.iter().any(contains_aggregate) {
            lines.push(match select.group_by.is_empty() {
                true => "Aggregate".into(),
                false => format!(
                    "Aggregate group by ({})",
                    join_expressions(&select.group_by)
                ),
            });
        }

        if !select.order_by.is_empty() {
            lines.push(format!("Sort ({})", join_expressions(&select.order_by)));
        }

        lines.push(format!("Project ({})", join_expressions(&select.columns)));

        if select.limit.is_some() || select.offset.is_some() {
            let limit = select.limit.map_or("all".into(), |l| l.to_string());
            lines.push(format!(
                "Limit {limit} offset {}",
                select.offset.unwrap_or(0)
            ));
        }

        Ok(lines)
    }

    /// Names the scan `filtered_source` would choose for a table and
    /// predicate: an index point/range scan when the bounds cover an indexed
    /// column, a sequential scan otherwise
    pub(crate) fn access_path(
        &self,
        table: &str,
        predicate: Option<&Expression>,
    ) -> Result<String, DatabaseError> {
        let Some(access) = predicate.and_then(index_access) else {
            return Ok(format!("SeqScan on {table}"));
        };

        let schema = self.engine.schema(table)?;
        let indexed = schema
            .columns
            .iter()
            .position(|c| c.name() == access.column())
            .and_then(|col| self.engine.index_for_column(table, col).ok().flatten());

        Ok(match indexed {
            Some(index) => {
                let kind = match &access {
                    Access::Range(_, Bound::Included(a), Bound::Included(b)) if a == b => {
                        "IndexScan"
                    }
                    Access::Range(..) => "IndexRangeScan",
                    Access::Points(..) => "IndexScan",
                };
                format!("{kind} on {table} using {index} ({})", access.column())
            }
            None => format!("SeqScan on {table}"),
        })
    }

    /// Evaluates a `RETURNING` list over written rows
    pub(crate) fn evaluate_returning(
        &self,
        schema: &Schema,
        exprs: &[Expression],
        rows: Vec<Tuple>,
        wildcard_start: usize,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        enum Item {
            Position(usize),
            Expr(Expression),
        }

        let mut out_columns = Vec::new();
        let mut items = Vec::new();

        for expr in exprs {
            match expr {
                Expression::Wildcard => {
                    for idx in wildcard_start..schema.len() {
                        out_columns.push(schema.columns[idx].clone());
                        items.push(Item::Position(idx));
                    }
                }
                other => {
                    let single = projection_schema(schema, std::slice::from_ref(other))?;
                    out_columns.extend(single.columns);
                    items.push(Item::Expr(other.clone()));
                }
            }
        }

        let out_schema = Schema::new(out_columns);
        let mut out_rows = Vec::with_capacity(rows.len());
        for row in &rows {
            let tuple = items
                .iter()
                .map(|item| match item {
                    Item::Position(idx) => Ok(row[*idx].clone()),
                    Item::Expr(expr) => {
                        resolve_expression(row, schema, expr).map_err(DatabaseError::from)
                    }
                })
                .collect::<Result<Tuple, DatabaseError>>()?;
            out_rows.push(tuple);
        }

        Ok((out_schema, out_rows))
    }

    /// Resolves the old `Schema` (column list) for a table from the engine's
    /// stored schema, with `row_id` prepended to match [operator::Scan] tuples.
    pub(crate) fn resolve_schema(&self, table: &str) -> Result<Schema, DatabaseError> {
        let schema_new = self.engine.schema(table)?;

        let mut schema = Schema::from(schema_new.as_ref());
        schema.prepend_id();

        Ok(schema)
    }
}

fn projection_schema(schema: &Schema, columns: &[Expression]) -> Result<Schema, DatabaseError> {
    Ok(Schema::new(
        columns
            .iter()
            .map(|expr| {
                let (name, inner) = match expr {
                    Expression::Alias { alias, expr } => (alias.clone(), expr.as_ref()),
                    Expression::Function { func, .. } => (func.to_string(), expr),
                    other => (other.to_string(), expr),
                };

                match column_position(schema, inner) {
                    Some(idx) => {
                        let mut column = schema.columns[idx].clone();
                        column.name = name;
                        Ok(column)
                    }
                    // computed expressions can always evaluate to NULL
                    None => Ok(Column::nullable(&name, resolve_type(schema, inner)?)),
                }
            })
            .collect::<Result<Vec<Column>, db::SqlError>>()?,
    ))
}

/// Whether an `ORDER BY` key refers to a projected column: the same
/// expression, the aliased inner expression, or the alias by name.
fn projection_matches(item: &Expression, key: &Expression) -> bool {
    if item == key {
        return true;
    }

    match item {
        Expression::Alias { alias, expr } => {
            expr.as_ref() == key || matches!(key, Expression::Identifier(name) if name == alias)
        }
        _ => false,
    }
}

/// Substitutes a bare identifier naming a select alias with the aliased
/// expression, so `GROUP BY`/`ORDER BY` can reference projected names.
fn dealias<'e>(expr: &'e Expression, columns: &'e [Expression]) -> &'e Expression {
    let Expression::Identifier(name) = expr else {
        return expr;
    };

    columns
        .iter()
        .find_map(|column| match column {
            Expression::Alias { alias, expr } if alias == name => Some(expr.as_ref()),
            _ => None,
        })
        .unwrap_or(expr)
}

/// rewrites, in place, every identifier in a `WHERE` tree that names an
/// output alias (and no real column) into the aliased expression
fn dealias_predicate(predicate: &mut Expression, columns: &[Expression], schema: &Schema) {
    match predicate {
        Expression::Identifier(name) => {
            if schema.index_of(name).is_some() {
                return;
            }

            let aliased = columns.iter().find_map(|column| match column {
                Expression::Alias { alias, expr } if alias == name => Some(expr.as_ref()),
                _ => None,
            });

            if let Some(inner) = aliased {
                *predicate = inner.clone();
            }
        }
        Expression::UnaryOperation { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => dealias_predicate(expr, columns, schema),
        Expression::BinaryOperation { left, right, .. } => {
            dealias_predicate(left, columns, schema);
            dealias_predicate(right, columns, schema);
        }
        Expression::Function { args, .. } => args
            .iter_mut()
            .for_each(|arg| dealias_predicate(arg, columns, schema)),
        _ => {}
    }
}

fn comparator_for(sort_keys: Vec<(usize, bool)>) -> Box<dyn Fn(&Tuple, &Tuple) -> Ordering> {
    Box::new(move |a: &Tuple, b: &Tuple| {
        for &(idx, desc) in &sort_keys {
            let ord = a[idx].partial_cmp(&b[idx]).unwrap_or(Ordering::Equal);
            if ord != Ordering::Equal {
                return match desc {
                    true => ord.reverse(),
                    false => ord,
                };
            }
        }

        Ordering::Equal
    })
}

fn join_expressions<E: std::fmt::Display>(expressions: &[E]) -> String {
    expressions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn pushable_predicate(predicate: Option<&Expression>, key: &str) -> Option<Expression> {
    let mut pushed: Option<Expression> = None;
    let mut stack = vec![predicate?];

    while let Some(expr) = stack.pop() {
        if let Expression::BinaryOperation {
            operator: BinaryOperator::And,
            left,
            right,
        } = expr
        {
            stack.push(left);
            stack.push(right);
            continue;
        }

        let Some(dequalified) = dequalify(expr, key) else {
            continue;
        };

        pushed = Some(match pushed {
            Some(acc) => Expression::BinaryOperation {
                operator: BinaryOperator::And,
                left: Box::new(acc),
                right: Box::new(dequalified),
            },
            None => dequalified,
        });
    }

    pushed
}

fn dequalify(expr: &Expression, key: &str) -> Option<Expression> {
    match expr {
        Expression::QualifiedIdentifier { table, column } if table == key => {
            Some(Expression::Identifier(column.clone()))
        }
        Expression::Value(_) => Some(expr.clone()),
        Expression::BinaryOperation {
            operator,
            left,
            right,
        } => Some(Expression::BinaryOperation {
            operator: *operator,
            left: Box::new(dequalify(left, key)?),
            right: Box::new(dequalify(right, key)?),
        }),
        Expression::UnaryOperation { operator, expr } => Some(Expression::UnaryOperation {
            operator: *operator,
            expr: Box::new(dequalify(expr, key)?),
        }),
        Expression::Nested(inner) => Some(Expression::Nested(Box::new(dequalify(inner, key)?))),
        Expression::IsNull { expr, negated } => Some(Expression::IsNull {
            expr: Box::new(dequalify(expr, key)?),
            negated: *negated,
        }),
        _ => None,
    }
}

/// Chooses the index access for a predicate: a range first, then a point set.
fn index_access(predicate: &Expression) -> Option<Access<'_>> {
    if let Some((column, start, end)) = predicate_bounds(predicate) {
        return Some(Access::Range(column, start, end));
    }

    predicate_values(predicate).map(|(column, values)| Access::Points(column, values))
}

/// Recognises `col = a OR col = b OR ...` (all on one column) as a point set
/// `IN (..)` desugars to exactly this chain during parsing
fn predicate_values(predicate: &Expression) -> Option<(&str, Vec<Value>)> {
    let Expression::BinaryOperation {
        left,
        operator,
        right,
    } = predicate
    else {
        return None;
    };

    match operator {
        BinaryOperator::Or => {
            let (left_col, mut values) = predicate_values(left)?;
            let (right_col, right_values) = predicate_values(right)?;
            if left_col != right_col {
                return None;
            }

            values.extend(right_values);
            Some((left_col, values))
        }
        BinaryOperator::Eq => match (&**left, &**right) {
            (Expression::Identifier(column), Expression::Value(value))
            | (Expression::Value(value), Expression::Identifier(column)) => {
                Some((column, vec![value.clone()]))
            }
            _ => None,
        },
        _ => None,
    }
}

fn predicate_bounds(predicate: &Expression) -> Option<(&str, Bound<Value>, Bound<Value>)> {
    let Expression::BinaryOperation {
        left,
        operator,
        right,
    } = predicate
    else {
        return None;
    };

    if *operator == BinaryOperator::And {
        let (left_col, left_start, left_end) = predicate_bounds(left)?;
        let (right_col, right_start, right_end) = predicate_bounds(right)?;
        if left_col != right_col {
            return None;
        }

        let start = match left_start {
            Bound::Unbounded => right_start,
            bound => bound,
        };
        let end = match left_end {
            Bound::Unbounded => right_end,
            bound => bound,
        };

        return Some((left_col, start, end));
    }

    let (column, value, operator) = match (&**left, &**right) {
        (Expression::Identifier(column), Expression::Value(value)) => (column, value, *operator),
        // literal-first comparisons flip: `5 < col` means `col > 5`
        (Expression::Value(value), Expression::Identifier(column)) => (
            column,
            value,
            match operator {
                BinaryOperator::Lt => BinaryOperator::Gt,
                BinaryOperator::LtEq => BinaryOperator::GtEq,
                BinaryOperator::Gt => BinaryOperator::Lt,
                BinaryOperator::GtEq => BinaryOperator::LtEq,
                other => *other,
            },
        ),
        _ => return None,
    };

    match operator {
        BinaryOperator::Eq => Some((
            column,
            Bound::Included(value.clone()),
            Bound::Included(value.clone()),
        )),
        BinaryOperator::Gt => Some((column, Bound::Excluded(value.clone()), Bound::Unbounded)),
        BinaryOperator::GtEq => Some((column, Bound::Included(value.clone()), Bound::Unbounded)),
        BinaryOperator::Lt => Some((column, Bound::Unbounded, Bound::Excluded(value.clone()))),
        BinaryOperator::LtEq => Some((column, Bound::Unbounded, Bound::Included(value.clone()))),
        _ => None,
    }
}

/// resolves plain, qualified, and aliased column references to a position
fn equi_join_positions(
    on: &Expression,
    schema: &Schema,
    left_width: usize,
) -> Option<(usize, usize)> {
    let Expression::BinaryOperation {
        operator: BinaryOperator::Eq,
        left,
        right,
    } = on
    else {
        return None;
    };

    let a = column_position(schema, left)?;
    let b = column_position(schema, right)?;

    match (a < left_width, b < left_width) {
        (true, false) => Some((a, b - left_width)),
        (false, true) => Some((b, a - left_width)),
        _ => None,
    }
}

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

#[inline]
const fn is_column_ref(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Identifier(_) | Expression::QualifiedIdentifier { .. }
    )
}

fn build_comparator(
    schema: &Schema,
    order_by: &[statement::OrderBy],
) -> Box<dyn Fn(&Tuple, &Tuple) -> Ordering> {
    comparator_for(
        order_by
            .iter()
            .filter_map(|ob| {
                let idx = column_position(schema, &ob.expr)?;
                let desc = ob.direction == OrderDirection::Desc;

                Some((idx, desc))
            })
            .collect(),
    )
}
