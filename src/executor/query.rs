//! SELECT query pipeline builder
//!
//! Translates a parsed `Select` AST node into an operator pipeline:
//! `Scan -> Filter -> Sort -> Project -> Limit`

use super::aggregate::{Aggregates, Group, count_calls, substitute};
use super::operator::Operator;
use super::{Executor, Tuple};
use crate::db::{self, Column, DatabaseError, Schema, SqlError};
use crate::executor::dml;
use crate::executor::operator::{
    Comparator, Evaluate, Filter, HashJoin, IncrementalSort, IndexJoin, JoinSpec, Keyed, Limit,
    Project, Scan, Sort, Values, comparator_for,
};
use crate::sql::Value;
use crate::sql::analyzer::{self, contains_aggregate};
use crate::sql::statement::{
    self, BinaryOperator, Expression, Function, NUMERIC_ANY, OrderDirection, PathSegment, Type,
};
use crate::storage::engine::table::Table;
use crate::storage::engine::transaction::IndexCursor;
use crate::storage::engine::transaction::Transaction;
use crate::vm::expression::{VmType, resolve_expression};
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Bound;
use std::sync::Arc;

/// The index-servable shape of a `WHERE` predicate on a single column: either a
/// contiguous range or a discrete set of points (`IN (..)`, `col = a OR col = b`)
enum Access<'a> {
    Range(&'a str, Bound<Value>, Bound<Value>),
    Points(&'a str, Vec<Value>),
}

/// A column an expression reads, by name
enum Read {
    Column(String),
    /// A qualified name `t.c`: column `c`, or column `t` holding a JSON document
    Either(String, String),
}

/// How one select column of an aggregate query is produced from a finished group
enum AggregateOutput {
    /// The value of the aggregate call at this plan position
    Call(usize),
    /// A column of the group's first row
    Column(usize),
    /// Anything else: the expression with its calls (this plan range) substituted
    Expression(Expression, std::ops::Range<usize>),
}

/// A row source and the schema of the tuples it yields
type Source = (Schema, Box<dyn Operator>);

impl Access<'_> {
    #[inline]
    const fn column(&self) -> &str {
        match self {
            Self::Range(column, ..) | Self::Points(column, ..) => column,
        }
    }
}

impl Executor {
    /// builds and drains a SELECT operator pipeline, returning the projected result schema and all result rows
    pub fn execute_select(
        &self,
        txn: &mut Transaction,
        mut select: statement::Select,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        if !select.group_by.is_empty() || select.columns.iter().any(contains_aggregate) {
            return self.execute_aggregate(txn, select);
        }

        self.drive_from_filtered_side(&mut select)?;

        // the base columns this query actually reads, so the scan need not
        // materialise the rest (projection pushdown)
        let projection = self.scan_projection(&select)?;

        // an ordered index can serve the `ORDER BY` directly, skipping the sort
        // how many leading `ORDER BY` keys the source already delivers in order
        let (schema, mut pipeline, presorted) =
            match self.index_ordered_scan(txn, &select, projection.as_deref())? {
                Some((schema, source)) => (schema, source, 1),
                _ => {
                    let (schema, pipeline) =
                        self.select_source(txn, &mut select, projection.as_deref())?;
                    (schema, pipeline, 0)
                },
            };
        let sorted = presorted == select.order_by.len();

        // an `ORDER BY ... LIMIT` only needs the smallest `limit + offset` rows
        let retain = select.limit.map(|limit| limit + select.offset.unwrap_or(0));
        let scratch = self.engine.scratch_dir();

        let result_schema;

        match select.columns.as_slice() {
            // lone `*` keeps the identity projection: sort on the input schema, then strip the row_id prefix
            [Expression::Wildcard] => {
                if !sorted {
                    let keys = sort_keys(&schema, &select.order_by);
                    pipeline = order(pipeline, keys, presorted, retain, &scratch)?;
                }

                let indices: Vec<_> = (1..schema.len()).collect();
                result_schema =
                    Schema::new(indices.iter().map(|&idx| schema.columns[idx].clone()).collect());
                pipeline = Box::new(Project::new(pipeline, indices));
            },
            _ => {
                let mut items: Vec<_> = select.columns.clone();
                let visible = items.len();

                // sort keys reference a projected column when they can,
                // otherwise they ride along as hidden columns, skipped entirely
                // when an ordered index already satisfies the `ORDER BY`
                let mut sort_keys = Vec::with_capacity(select.order_by.len());
                if !sorted {
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
                }

                let hidden = items.len() > visible;

                result_schema = projection_schema(&schema, &items[..visible])?;

                // plain column references need no expression evaluation, only a projection
                let positions: Option<Vec<_>> =
                    items.iter().map(|item| column_position(&schema, item)).collect();
                pipeline = match positions {
                    Some(positions) => Box::new(Project::new(pipeline, positions)),
                    _ => Box::new(Evaluate::new(pipeline, schema.clone(), items)),
                };

                if !sort_keys.is_empty() {
                    pipeline = order(pipeline, sort_keys, presorted, retain, &scratch)?;
                }

                if hidden {
                    pipeline = Box::new(Project::new(pipeline, (0..visible).collect()));
                }
            },
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

    /// the join of the base table with every `JOIN` clause as a left-deep operator chain, with the combined schema
    /// `None` when the SELECT has no joins
    /// the base tuples keep their `row_id` prefix, joined tables contribute their user columns only, qualified by table key
    fn joined_source(
        &self,
        txn: &mut Transaction,
        select: &statement::Select,
        projection: Option<&[usize]>,
    ) -> Result<Option<Source>, DatabaseError> {
        use crate::sql::statement::JoinType;
        use crate::vm::expression::evaluate_where;

        if select.joins.is_empty() {
            return Ok(None);
        }

        // single-table WHERE filters only push into the scans of an all-INNER
        // join, where narrowing an input never changes the final result
        let all_inner = select.joins.iter().all(|join| matches!(join.join_type, JoinType::Inner));

        let base = &select.from.name;
        let mut schema = match projection {
            Some(columns) => self.resolve_schema(base)?.project(&keep_with_row_id(columns)),
            None => self.resolve_schema(base)?,
        };
        let base_filter_schema = schema.clone();
        schema.add_qualified_name(select.from.key(), 0, schema.len());

        let base_pushed = all_inner
            .then(|| pushable_predicate(select.r#where.as_ref(), select.from.key()))
            .flatten();
        let mut source =
            self.filtered_source(txn, base, base_pushed.as_ref(), &base_filter_schema, projection)?;
        let read = read_columns(select);

        for join in &select.joins {
            let right_table = &join.table.name;
            let right_schema = self
                .engine
                .table(right_table)
                .map_err(|_| DatabaseError::Sql(db::SqlError::InvalidTable(right_table.clone())))?
                .schema
                .clone();

            let right_positions: Vec<_> = match &read {
                Some(reads) => (0..right_schema.len())
                    .filter(|&p| {
                        let name = right_schema.columns[p].name();
                        reads.iter().any(|read| read.names().any(|read| read == name))
                    })
                    .collect(),
                _ => (0..right_schema.len()).collect(),
            };
            let right_columns: Vec<_> =
                right_positions.iter().map(|&p| right_schema.columns[p].clone()).collect();
            let right_width = right_columns.len();

            let start = schema.len();
            schema.extend_with_join(right_columns, &join.join_type);
            schema.add_qualified_name(join.table.key(), start, schema.len());

            let right_pushed = all_inner
                .then(|| pushable_predicate(select.r#where.as_ref(), join.table.key()))
                .flatten();

            let left_width = start;
            let outer_left = matches!(join.join_type, JoinType::Left | JoinType::Full);
            let outer_right = matches!(join.join_type, JoinType::Right | JoinType::Full);

            // an indexed right key turns the join into per-row index probes (INNER/LEFT only: RIGHT/FULL must also emit the right rows nothing probed)
            let equi = equi_join_positions(&join.on, &schema, left_width);
            let probed = equi.filter(|_| !outer_right).and_then(|(left_key, right_key)| {
                let table = self.table(right_table).ok()?;
                let index = table.index_on(right_positions[right_key])?;
                Some((left_key, table, index))
            });

            if let Some((left_key, table, index)) = probed {
                let stored = table.schema.clone();
                let filter = right_pushed
                    .filter(|pushed| reads_only(pushed, &stored))
                    .map(|pushed| (stored, pushed));
                let probe = txn.index_probe(&table, &index);
                let join =
                    IndexJoin::new(source, probe, left_key, right_positions, outer_left, filter);
                source = Box::new(join);
                continue;
            }

            // the right side contributes the projected user columns only
            let right: Box<dyn Operator> = match right_pushed {
                Some(pushed) => {
                    let keep = keep_with_row_id(&right_positions);
                    let right_filter_schema = self.resolve_schema(right_table)?.project(&keep);
                    self.filtered_source(
                        txn,
                        right_table,
                        Some(&pushed),
                        &right_filter_schema,
                        Some(&right_positions),
                    )?
                },
                None => {
                    let table = self.table(right_table)?;
                    Box::new(Scan::new(txn.scan(&table), Some(right_positions)))
                },
            };

            source = match equi {
                Some((left_key, right_key)) => {
                    let spec = JoinSpec {
                        outer_left,
                        outer_right,
                        key_type: schema.columns[left_key].column_type(),
                        left_key,
                        right_key,
                        left_width,
                        right_width,
                    };
                    let scratch = self.engine.scratch_dir();
                    Box::new(HashJoin::new(spec, source, right, &scratch)?)
                },
                _ => {
                    // PERFORMANCE: nested loop over materialised inputs, only for non-equi `ON`
                    let collect =
                        |mut operator: Box<dyn Operator>| -> Result<Vec<Tuple>, DatabaseError> {
                            let mut rows = Vec::new();
                            while let Some(tuple) = operator.next()? {
                                rows.push(tuple);
                            }
                            Ok(rows)
                        };
                    let (rows, right_rows) = (collect(source)?, collect(right)?);

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

                        if !matched && outer_left {
                            let mut combined = left.clone();
                            combined.extend(std::iter::repeat_n(Value::Null, right_width));
                            joined.push(combined);
                        }
                    }

                    if outer_right {
                        for (right, _) in
                            right_rows.iter().zip(&right_matched).filter(|(_, m)| !**m)
                        {
                            let mut combined = vec![Value::Null; left_width];
                            combined.extend(right.iter().cloned());
                            joined.push(combined);
                        }
                    }

                    Box::new(Values::new(joined))
                },
            };
        }

        Ok(Some((schema, source)))
    }

    /// builds the filtered row source shared by plain and aggregate selects:
    /// the joined result when the statement has joins, otherwise the (possibly index-driven) base table
    fn select_source(
        &self,
        txn: &mut Transaction,
        select: &mut statement::Select,
        projection: Option<&[usize]>,
    ) -> Result<Source, DatabaseError> {
        match self.joined_source(txn, select, projection)? {
            Some((schema, mut source)) => {
                if let Some(mut predicate) = select.r#where.take() {
                    dealias_predicate(&mut predicate, &select.columns, &schema);
                    source = Box::new(Filter::new(source, schema.clone(), predicate));
                }

                Ok((schema, source))
            },
            None => {
                let table = &select.from.name;
                let full = self.resolve_schema(table)?;
                if let Some(predicate) = select.r#where.as_mut() {
                    dealias_predicate(predicate, &select.columns, &full);
                }
                let schema = match projection {
                    Some(cols) => full.project(&keep_with_row_id(cols)),
                    _ => full,
                };
                let pred = select.r#where.as_ref();
                let source = self.filtered_source(txn, table, pred, &schema, projection)?;

                Ok((schema, source))
            },
        }
    }

    /// join order by rule: a single INNER join on the base table's primary key,
    /// whose joined table has an index-servable `WHERE` predicate and whose base table has none, is
    /// driven from the joined table
    /// its filtered rows then probe the base's unique key once each, instead of every
    /// base row probing the joined table and filtering what it finds
    fn drive_from_filtered_side(
        &self,
        select: &mut statement::Select,
    ) -> Result<(), DatabaseError> {
        use crate::sql::statement::JoinType;
        use Expression::*;

        let [join] = select.joins.as_slice() else {
            return Ok(());
        };
        let BinaryOperation { operator: BinaryOperator::Eq, left, right } = &join.on else {
            return Ok(());
        };
        if join.join_type != JoinType::Inner {
            return Ok(());
        }

        let (base_key, joined_key) = (select.from.key(), join.table.key());
        let base_column =
            match (left.as_ref(), right.as_ref()) {
                (
                    QualifiedIdentifier { table: a, column },
                    QualifiedIdentifier { table: b, .. },
                ) if a == base_key && b == joined_key => column,
                (
                    QualifiedIdentifier { table: a, .. },
                    QualifiedIdentifier { table: b, column },
                ) if a == joined_key && b == base_key => column,
                _ => return Ok(()),
            };

        let base = self.table(&select.from.name)?;
        let joined = self.table(&join.table.name)?;
        let on_primary_key = base
            .schema
            .primary_column_index()
            .is_some_and(|pk| base.schema.columns[pk].name() == base_column.as_str());

        let indexed = |table: &Table, key: &str| {
            pushable_predicate(select.r#where.as_ref(), key).is_some_and(|pushed| {
                index_access(&pushed).is_some_and(|access| {
                    table.schema.index_of(access.column()).and_then(|c| table.index_on(c)).is_some()
                })
            })
        };

        if on_primary_key && indexed(&joined, joined_key) && !indexed(&base, base_key) {
            std::mem::swap(&mut select.from, &mut select.joins[0].table);
        }

        Ok(())
    }

    fn scan_projection(
        &self,
        select: &statement::Select,
    ) -> Result<Option<Vec<usize>>, DatabaseError> {
        let Some(names) = read_columns(select) else {
            return Ok(None);
        };

        let engine_schema = self.table(&select.from.name)?.schema.clone();
        let mut indices = Vec::with_capacity(names.len());
        for read in &names {
            let found: Vec<usize> =
                read.names().filter_map(|name| engine_schema.index_of(name)).collect();
            match found.is_empty() {
                false => indices.extend(found),
                // with joins the name may belong to another table (or be an alias)
                true if !select.joins.is_empty() => {},
                true => return Ok(None),
            }
        }

        indices.sort_unstable();
        indices.dedup();
        Ok(Some(indices))
    }

    /// the column whose index can supply the query's `ORDER BY` order directly,
    /// when the `ORDER BY` is a single unqualified indexed column and no
    /// `WHERE`/join forces a different access path
    /// Static (no transaction), so it describes the intended plan; execution still confirms the index is
    /// usable for the snapshot
    fn index_ordered_column(
        &self,
        select: &statement::Select,
    ) -> Result<Option<usize>, DatabaseError> {
        if !select.joins.is_empty() || select.r#where.is_some() {
            return Ok(None);
        }
        let Some(order) = select.order_by.first() else {
            return Ok(None);
        };
        let Expression::Identifier(name) = &order.expr else {
            return Ok(None);
        };

        let table = self.table(&select.from.name)?;
        let Some(column) = table.schema.columns.iter().position(|c| c.name() == name.as_str())
        else {
            return Ok(None);
        };

        Ok(table.index_on(column).map(|_| column))
    }

    /// if an index can serve the `ORDER BY` in order, returns an ordered scan so the `Sort` can be skipped entirely
    fn index_ordered_scan(
        &self,
        txn: &mut Transaction,
        select: &statement::Select,
        projection: Option<&[usize]>,
    ) -> Result<Option<Source>, DatabaseError> {
        let Some(column) = self.index_ordered_column(select)? else {
            return Ok(None);
        };

        let table = self.table(&select.from.name)?;
        let index = table.index_on(column).expect("ordered column is indexed");
        let descending = select.order_by[0].direction == OrderDirection::Desc;

        let full = self.resolve_schema(&select.from.name)?;
        let schema = match projection {
            Some(cols) => full.project(&keep_with_row_id(cols)),
            None => full,
        };

        let cursor = txn.index_cursor(&table, &index, descending);
        let scan = Scan::indexed([cursor].into(), projection.map(<[usize]>::to_vec));
        Ok(Some((schema, Box::new(scan))))
    }

    /// executes a SELECT with aggregate functions and/or GROUP BY
    /// Each group keeps its first row and one accumulator state per aggregate
    /// call, non-aggregate columns are evaluated on the first row
    fn execute_aggregate(
        &self,
        txn: &mut Transaction,
        mut select: statement::Select,
    ) -> Result<(Schema, Vec<Tuple>), DatabaseError> {
        use crate::sql::statement::JoinType;

        let projection = self.scan_projection(&select)?;
        let (schema, mut pipeline) = self.select_source(txn, &mut select, projection.as_deref())?;
        let columns: Vec<_> = select
            .columns
            .iter()
            .map(|expr| match expr {
                Expression::Alias { expr, .. } => expr.as_ref().clone(),
                other => other.clone(),
            })
            .collect();

        let keys: Vec<_> = select
            .group_by
            .iter()
            .map(|expr| dealias(expr, &select.columns).clone())
            .collect();

        let base = self.table(&select.from.name)?.schema.clone();
        let primary = base.primary_column_index();
        // where the base table's columns and primary key sit in `schema`
        let (base_width, primary) = match &projection {
            Some(columns) => {
                (columns.len(), primary.and_then(|pk| columns.iter().position(|&c| c == pk)))
            },
            None => (base.columns.len() + 1, primary.map(|pk| pk + 1)),
        };

        let positions: Option<Vec<_>> = keys
            .iter()
            .map(|key| column_position(&schema, key).filter(|&p| p < base_width))
            .collect();
        // a second join may partition on a joined key and split one base row's rows
        let reordering_join = select.joins.len() > 1
            || select
                .joins
                .iter()
                .any(|join| matches!(join.join_type, JoinType::Right | JoinType::Full));
        let clustered = positions.filter(|positions| {
            !reordering_join && primary.is_some_and(|pk| positions.contains(&pk))
        });

        let aggregates = Aggregates::plan(&columns, &schema);
        let outputs = self.aggregate_outputs(&columns, &schema);
        let scratch = self.engine.scratch_dir();
        let mut groups = Vec::new();

        let plain: Option<Vec<_>> = keys
            .iter()
            .map(|key| column_position(&schema, key).map(|p| (p, schema.columns[p].column_type())))
            .collect();

        // without ORDER BY the first `limit + offset` groups in key order are the answer
        let wanted = match select.order_by.is_empty() {
            true => select.limit.map(|limit| limit + select.offset.unwrap_or(0)),
            false => None,
        };

        match (keys.is_empty(), clustered, plain) {
            (true, ..) => aggregates.fold_grouped(&mut *pipeline, &[], 0, None, &mut groups)?,
            (false, Some(positions), _) => {
                aggregates.fold_grouped(&mut *pipeline, &positions, 0, wanted, &mut groups)?
            },
            (false, None, Some(key)) => {
                aggregates.fold_hashed(&mut *pipeline, &key, &scratch, &mut groups)?;
                if select.order_by.is_empty() {
                    let order = |(a, _): &Group, (b, _): &Group| {
                        key.iter()
                            .map(|(p, _)| a[*p].cmp(&b[*p]))
                            .find(|order| order.is_ne())
                            .unwrap_or(Ordering::Equal)
                    };
                    if let Some(wanted) = wanted.filter(|&w| w > 0 && w < groups.len()) {
                        groups.select_nth_unstable_by(wanted - 1, order);
                        groups.truncate(wanted);
                    }
                    groups.sort_unstable_by(order);
                }
            },
            _ => {
                let width = keys.len();
                let mut keyed = Keyed::new(pipeline, schema.clone(), keys);
                let order = comparator_for((0..width).map(|key| (key, false)).collect());
                let mut sorted = Sort::new(&mut keyed, order, None, &scratch)?;
                let positions: Vec<usize> = (0..width).collect();
                aggregates.fold_grouped(&mut sorted, &positions, width, wanted, &mut groups)?
            },
        }

        // an aggregate without `GROUP BY` yields one row even over no input
        if groups.is_empty() && select.group_by.is_empty() {
            groups.push((Vec::new(), aggregates.start()));
        }

        let out_schema = projection_schema(&schema, &select.columns)?;
        let mut out_rows = Vec::with_capacity(groups.len());
        for (row, states) in groups {
            let values = aggregates.finish(states);

            let tuple = outputs
                .iter()
                .map(|output| match output {
                    AggregateOutput::Call(call) => Ok(values[*call].clone()),
                    AggregateOutput::Column(position) => Ok(row[*position].clone()),
                    AggregateOutput::Expression(expr, calls) => {
                        let mut values = values[calls.clone()].iter().cloned();
                        let expr = substitute(expr, &mut values);
                        let input = match contains_aggregate(&expr) {
                            true => &[][..],
                            false => &row[..],
                        };
                        resolve_expression(input, &schema, &expr)
                    },
                })
                .collect::<Result<Tuple, _>>()?;

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

    /// how each select column of an aggregate query is produced from a finished group
    fn aggregate_outputs(&self, columns: &[Expression], schema: &Schema) -> Vec<AggregateOutput> {
        let mut next_call = 0;
        columns
            .iter()
            .map(|expr| {
                let calls = count_calls(expr);
                let start = next_call;
                next_call += calls;

                match expr {
                    Expression::Function { func, .. } if func.is_aggr() => {
                        AggregateOutput::Call(start)
                    },
                    _ => match (calls, column_position(schema, expr)) {
                        (0, Some(position)) => AggregateOutput::Column(position),
                        _ => AggregateOutput::Expression(expr.clone(), start..start + calls),
                    },
                }
            })
            .collect()
    }

    pub(crate) fn filtered_source(
        &self,
        txn: &mut Transaction,
        table: &str,
        predicate: Option<&Expression>,
        schema: &Schema,
        projection: Option<&[usize]>,
    ) -> Result<Box<dyn Operator>, DatabaseError> {
        let projection = projection.map(<[usize]>::to_vec);
        let table = self.table(table)?;
        let cursors = match predicate {
            Some(predicate) => self.index_cursors(txn, &table, predicate)?,
            _ => None,
        };
        let scan = match cursors {
            Some(cursors) => Scan::indexed(cursors, projection),
            _ => Scan::new(txn.scan(&table), projection),
        };

        let Some(predicate) = predicate else {
            return Ok(Box::new(scan));
        };

        let stored = table.schema.clone();
        match reads_only(predicate, &stored) {
            true => Ok(Box::new(scan.filtered(stored, predicate.clone()))),
            false => Ok(Box::new(Filter::new(Box::new(scan), schema.clone(), predicate.clone()))),
        }
    }

    /// serves an indexable predicate from an index, lazily: `AND`ed bounds (`=`, `<`, `<=`, `>`,
    /// `>=`) become one bounded cursor, and `IN (..)` / `col = a OR col = b` on one column become one
    /// cursor per distinct point, in key order
    fn index_cursors(
        &self,
        txn: &mut Transaction,
        table: &Arc<Table>,
        predicate: &Expression,
    ) -> Result<Option<VecDeque<IndexCursor>>, DatabaseError> {
        let Some(access) = index_access(predicate) else {
            return Ok(None);
        };

        let Some(col) = table.schema.columns.iter().position(|c| c.name() == access.column())
        else {
            return Ok(None);
        };

        let Some(index) = table.index_on(col) else {
            return Ok(None);
        };

        // enum values are indexed by variant index, not by their literal
        let column = &table.schema.columns[col];

        let cursors = match access {
            Access::Range(_, mut start, mut end) => {
                for bound in [&mut start, &mut end] {
                    if let Bound::Included(value) | Bound::Excluded(value) = bound {
                        dml::coerce_value(column, value)?;
                    }
                }

                VecDeque::from([txn.index_cursor_within(table, &index, (start, end))])
            },
            Access::Points(_, mut values) => {
                for value in &mut values {
                    dml::coerce_value(column, value)?;
                }
                values.sort();
                values.dedup();

                values
                    .into_iter()
                    .map(|value| {
                        let point = (Bound::Included(value.clone()), Bound::Included(value));
                        txn.index_cursor_within(table, &index, point)
                    })
                    .collect()
            },
        };

        Ok(Some(cursors))
    }

    /// describes the pipeline `execute_select` would build, one line per
    /// operator in execution order
    pub(crate) fn explain_select(
        &self,
        mut select: statement::Select,
    ) -> Result<Vec<String>, DatabaseError> {
        let aggregate =
            !select.group_by.is_empty() || select.columns.iter().any(contains_aggregate);
        if !aggregate {
            self.drive_from_filtered_side(&mut select)?;
        }
        let select = &select;

        let mut lines = vec![self.access_path(&select.from.name, select.r#where.as_ref())?];

        for join in &select.joins {
            use crate::sql::statement::JoinType;

            let right = self.table(&join.table.name)?;
            let probes_index = |expr: &Expression| match expr {
                Expression::QualifiedIdentifier { table, column } if table == join.table.key() => {
                    right
                        .schema
                        .columns
                        .iter()
                        .position(|c| c.name() == column.as_str())
                        .is_some_and(|position| right.index_on(position).is_some())
                },
                _ => false,
            };
            let outer_right = matches!(join.join_type, JoinType::Right | JoinType::Full);

            let strategy = match &join.on {
                Expression::BinaryOperation { operator: BinaryOperator::Eq, left, right }
                    if is_column_ref(left) && is_column_ref(right) =>
                {
                    match !outer_right && (probes_index(left) || probes_index(right)) {
                        true => "IndexJoin",
                        false => "HashJoin",
                    }
                },
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
                false => format!("Aggregate group by ({})", join_expressions(&select.group_by)),
            });
        }

        if !select.order_by.is_empty() {
            lines.push(match self.index_ordered_column(select)? {
                Some(_) if select.order_by.len() == 1 => {
                    format!("Index order ({})", join_expressions(&select.order_by))
                },
                Some(_) => format!(
                    "Incremental sort ({}) over index order ({})",
                    join_expressions(&select.order_by),
                    select.order_by[0]
                ),
                None => format!("Sort ({})", join_expressions(&select.order_by)),
            });
        }

        lines.push(format!("Project ({})", join_expressions(&select.columns)));

        if select.limit.is_some() || select.offset.is_some() {
            let limit = select.limit.map_or("all".into(), |l| l.to_string());
            lines.push(format!("Limit {limit} offset {}", select.offset.unwrap_or(0)));
        }

        Ok(lines)
    }

    /// names the scan `filtered_source` would choose for a table and
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

        let table_handle = self.table(table)?;
        let indexed = table_handle
            .schema
            .columns
            .iter()
            .position(|c| c.name() == access.column())
            .and_then(|col| table_handle.index_on(col))
            .map(|index| index.name.clone());

        Ok(match indexed {
            Some(index) => {
                let kind = match &access {
                    Access::Range(_, Bound::Included(a), Bound::Included(b)) if a == b => {
                        "IndexScan"
                    },
                    Access::Range(..) => "IndexRangeScan",
                    Access::Points(..) => "IndexScan",
                };
                format!("{kind} on {table} using {index} ({})", access.column())
            },
            None => format!("SeqScan on {table}"),
        })
    }

    /// evaluates a `RETURNING` list over written rows
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

        let (mut out_columns, mut items) = (Vec::new(), Vec::new());

        for expr in exprs {
            match expr {
                Expression::Wildcard => {
                    for idx in wildcard_start..schema.len() {
                        out_columns.push(schema.columns[idx].clone());
                        items.push(Item::Position(idx));
                    }
                },
                other => {
                    let single = projection_schema(schema, std::slice::from_ref(other))?;
                    out_columns.extend(single.columns.iter().cloned());
                    items.push(Item::Expr(other.clone()));
                },
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
                    },
                })
                .collect::<Result<Tuple, DatabaseError>>()?;
            out_rows.push(tuple);
        }

        Ok((out_schema, out_rows))
    }

    /// resolves the old `Schema` (column list) for a table from the engine's
    /// stored schema, with `row_id` prepended to match [operator::Scan] tuples
    pub(crate) fn resolve_schema(&self, table: &str) -> Result<Schema, DatabaseError> {
        Ok(self.table(table)?.scanned.clone())
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
                    Some(idx) => Ok(schema.columns[idx].clone().named(name)),
                    // computed expressions can always evaluate to NULL
                    None => Ok(Column::nullable(&name, resolve_type(schema, inner)?)),
                }
            })
            .collect::<Result<Vec<Column>, db::SqlError>>()?,
    ))
}

/// whether an `ORDER BY` key refers to a projected column: the same
/// expression, the aliased inner expression, or the alias by name
fn projection_matches(item: &Expression, key: &Expression) -> bool {
    if item == key {
        return true;
    }

    match item {
        Expression::Alias { alias, expr } => {
            expr.as_ref() == key || matches!(key, Expression::Identifier(name) if name == alias)
        },
        _ => false,
    }
}

/// substitutes a bare identifier naming a select alias with the aliased
/// expression, so `GROUP BY`/`ORDER BY` can reference projected names
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
        },
        Expression::UnaryOperation { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => dealias_predicate(expr, columns, schema),
        Expression::BinaryOperation { left, right, .. } => {
            dealias_predicate(left, columns, schema);
            dealias_predicate(right, columns, schema);
        },
        Expression::Function { args, .. } => {
            args.iter_mut().for_each(|arg| dealias_predicate(arg, columns, schema))
        },
        _ => {},
    }
}

fn join_expressions<E: std::fmt::Display>(expressions: &[E]) -> String {
    expressions.iter().map(ToString::to_string).collect::<Vec<_>>().join(", ")
}

fn pushable_predicate(predicate: Option<&Expression>, key: &str) -> Option<Expression> {
    let mut pushed: Option<Expression> = None;
    let mut stack = vec![predicate?];

    while let Some(expr) = stack.pop() {
        if let Expression::BinaryOperation { operator: BinaryOperator::And, left, right } = expr {
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
        },
        Expression::Value(_) => Some(expr.clone()),
        Expression::BinaryOperation { operator, left, right } => {
            Some(Expression::BinaryOperation {
                operator: *operator,
                left: Box::new(dequalify(left, key)?),
                right: Box::new(dequalify(right, key)?),
            })
        },
        Expression::UnaryOperation { operator, expr } => Some(Expression::UnaryOperation {
            operator: *operator,
            expr: Box::new(dequalify(expr, key)?),
        }),
        Expression::Nested(inner) => Some(Expression::Nested(Box::new(dequalify(inner, key)?))),
        Expression::IsNull { expr, negated } => {
            Some(Expression::IsNull { expr: Box::new(dequalify(expr, key)?), negated: *negated })
        },
        _ => None,
    }
}

/// chooses the index access for a predicate: a range first, then a point set
fn index_access(predicate: &Expression) -> Option<Access<'_>> {
    if let Some((column, start, end)) = predicate_bounds(predicate) {
        return Some(Access::Range(column, start, end));
    }

    predicate_values(predicate).map(|(column, values)| Access::Points(column, values))
}

/// recognises `col = a OR col = b OR ...` (all on one column) as a point set
/// `IN (..)` desugars to exactly this chain during parsing
fn predicate_values(predicate: &Expression) -> Option<(&str, Vec<Value>)> {
    let Expression::BinaryOperation { left, operator, right } = predicate else {
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
        },
        BinaryOperator::Eq => match (&**left, &**right) {
            (Expression::Identifier(column), Expression::Value(value))
            | (Expression::Value(value), Expression::Identifier(column)) => {
                Some((column, vec![value.clone()]))
            },
            _ => None,
        },
        _ => None,
    }
}

fn predicate_bounds(predicate: &Expression) -> Option<(&str, Bound<Value>, Bound<Value>)> {
    use BinaryOperator::*;
    let Expression::BinaryOperation { left, operator, right } = predicate else {
        return None;
    };

    if *operator == And {
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
                Lt => Gt,
                LtEq => GtEq,
                Gt => Lt,
                GtEq => LtEq,
                other => *other,
            },
        ),
        _ => return None,
    };

    match operator {
        Eq => Some((column, Bound::Included(value.clone()), Bound::Included(value.clone()))),
        Gt => Some((column, Bound::Excluded(value.clone()), Bound::Unbounded)),
        GtEq => Some((column, Bound::Included(value.clone()), Bound::Unbounded)),
        Lt => Some((column, Bound::Unbounded, Bound::Excluded(value.clone()))),
        LtEq => Some((column, Bound::Unbounded, Bound::Included(value.clone()))),
        _ => None,
    }
}

/// resolves plain, qualified, and aliased column references to a position
fn equi_join_positions(
    on: &Expression,
    schema: &Schema,
    left_width: usize,
) -> Option<(usize, usize)> {
    let Expression::BinaryOperation { operator: BinaryOperator::Eq, left, right } = on else {
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
        },
        Expression::Alias { expr, .. } => column_position(schema, expr),
        _ => None,
    }
}

/// the columns `select` reads anywhere (select list, `WHERE`, `ON`, `ORDER BY`, `GROUP BY`),
/// `None` when an expression hides which (a wildcard)
fn read_columns(select: &statement::Select) -> Option<Vec<Read>> {
    let mut names = Vec::new();
    let expressions = select
        .columns
        .iter()
        .chain(&select.r#where)
        .chain(select.joins.iter().map(|join| &join.on))
        .chain(select.order_by.iter().map(|order| &order.expr))
        .chain(&select.group_by);

    let mut understood = true;
    for expr in expressions {
        understood &= collect_columns(expr, &mut names);
    }

    understood.then_some(names)
}

/// Whether every column `expr` reads is a column of `schema`.
fn reads_only(expr: &Expression, schema: &Schema) -> bool {
    let mut reads = Vec::new();
    collect_columns(expr, &mut reads)
        && reads
            .iter()
            .all(|read| read.names().any(|name| schema.index_of(name).is_some()))
}

fn collect_columns(expr: &Expression, out: &mut Vec<Read>) -> bool {
    match expr {
        Expression::Identifier(name) => {
            out.push(Read::Column(name.clone()));
            true
        },
        // `t.c` reads column `c`, or JSON key `c` of column `t` (evaluation tries both)
        Expression::QualifiedIdentifier { table, column } => {
            out.push(Read::Either(table.clone(), column.clone()));
            true
        },
        Expression::Path { head, segments } => {
            out.push(match segments.first() {
                Some(PathSegment::Key(first)) => Read::Either(head.clone(), first.clone()),
                _ => Read::Column(head.clone()),
            });
            true
        },
        Expression::Value(_) => true,
        Expression::Wildcard | Expression::Column(_) | Expression::JsonPath { .. } => false,
        Expression::UnaryOperation { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => collect_columns(expr, out),
        Expression::BinaryOperation { left, right, .. } => {
            collect_columns(left, out) && collect_columns(right, out)
        },
        Expression::Function { func: Function::Count, args } if args == &[Expression::Wildcard] => {
            true
        },
        Expression::Function { args, .. } => args.iter().all(|arg| collect_columns(arg, out)),
    }
}

/// maps user-column indices (into the stored row) to their positions in the
/// executor schema, which carries the `row_id` at index 0
fn keep_with_row_id(user_columns: &[usize]) -> Vec<usize> {
    user_columns.iter().map(|&i| i + 1).collect()
}

#[inline]
const fn is_column_ref(expr: &Expression) -> bool {
    matches!(expr, Expression::Identifier(_) | Expression::QualifiedIdentifier { .. })
}

/// `pipeline` sorted on `keys`, of which the first `presorted` already hold: a full [Sort], or an
/// [IncrementalSort] over the runs of equal leading keys
fn order(
    mut pipeline: Box<dyn Operator>,
    keys: Vec<(usize, bool)>,
    presorted: usize,
    retain: Option<usize>,
    scratch: &std::path::Path,
) -> Result<Box<dyn Operator>, DatabaseError> {
    Ok(match presorted {
        0 => Box::new(Sort::new(&mut *pipeline, comparator_for(keys), retain, scratch)?),
        presorted if presorted >= keys.len() => pipeline,
        _ => Box::new(IncrementalSort::new(pipeline, keys, presorted, retain, scratch)),
    })
}

fn build_comparator(schema: &Schema, order_by: &[statement::OrderBy]) -> Comparator {
    comparator_for(sort_keys(schema, order_by))
}

/// `(position, descending)` of each `ORDER BY` key that is a column of `schema`
fn sort_keys(schema: &Schema, order_by: &[statement::OrderBy]) -> Vec<(usize, bool)> {
    order_by
        .iter()
        .filter_map(|ob| {
            let idx = column_position(schema, &ob.expr)?;
            let desc = ob.direction == OrderDirection::Desc;

            Some((idx, desc))
        })
        .collect()
}

fn resolve_type(schema: &Schema, expr: &Expression) -> Result<Type, SqlError> {
    Ok(match expr {
        Expression::Identifier(col) => {
            let index = schema.index_of(col).ok_or(SqlError::InvalidColumn(col.into()))?;
            schema.columns[index].column_type()
        },
        _ => match analyzer::analyze_expression(schema, None, expr)? {
            VmType::Float => Type::DoublePrecision,
            VmType::Bool => Type::Boolean,
            VmType::Number => Type::BigInteger,
            VmType::String => Type::Text,
            VmType::Date => Type::DateTime,
            VmType::Interval => Type::Interval,
            VmType::Numeric => Type::Numeric(NUMERIC_ANY, NUMERIC_ANY),
            VmType::Enum => Type::Enum(0),
            VmType::Blob => Type::Jsonb,
        },
    })
}

impl Read {
    /// The names this read may resolve to.
    fn names(&self) -> impl Iterator<Item = &str> {
        let (first, second) = match self {
            Read::Column(name) => (name.as_str(), None),
            Read::Either(table, column) => (column.as_str(), Some(table.as_str())),
        };
        std::iter::once(first).chain(second)
    }
}
