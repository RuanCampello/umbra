use std::{
    io::{Read, Seek, Write},
    path::PathBuf,
    rc::Rc,
};

use crate::{
    core::storage::pagination::io::FileOperations,
    core::{HashMap, HashSet},
    db::{Ctx, Database, DatabaseError, Schema, SqlError},
    hash_map, hash_set,
    sql::{
        analyzer::contains_aggregate,
        query::{self, planner::resolve_type},
        statement::{
            Column, Constraint, Expression, JoinClause, JoinType, OrderBy, OrderDirection, TableRef,
        },
    },
    vm::{
        cost::{Cost, CostEstimator},
        expression::VmType,
        planner::{
            AggregateBuilder, Collect, CollectBuilder, Filter, HashJoin, IndexNestedLoopJoin,
            Limit, Planner, Project, Sort, SortBuilder, SortKeys, TupleComparator,
            DEFAULT_SORT_BUFFER_SIZE,
        },
    },
};

pub struct SelectBuilder<'s, File: Seek + Read + Write + FileOperations> {
    source: Option<Planner<File>>,
    schema: Schema,
    tables: HashMap<String, Schema>,
    columns: &'s [Expression],
    /// maps table keys to their column index ranges in the joined schema.
    ranges: HashMap<&'s str, (usize, usize)>,
    work_dir: PathBuf,
    page_size: usize,
}

enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<'s, File: Seek + Read + Write + FileOperations> SelectBuilder<'s, File> {
    pub fn new(
        source: Planner<File>,
        schema: Schema,
        columns: &'s [Expression],
        page_size: usize,
        work_dir: std::path::PathBuf,
        table_key: &'s str,
    ) -> Self {
        let tables = hash_map!(table_key.to_string() => schema.clone());
        let ranges = hash_map!(table_key => (0, schema.len()));

        Self {
            source: Some(source),
            schema,
            columns,
            tables,
            work_dir,
            page_size,
            ranges,
        }
    }

    pub fn build(self) -> Planner<File> {
        self.source.unwrap()
    }

    pub fn apply_sorting(&mut self, order_by: &[OrderBy]) -> Result<(), DatabaseError> {
        if order_by == [Expression::Identifier(self.schema.columns[0].name.to_string()).into()] {
            return Ok(());
        }

        let mut sorted_schema = self.schema.clone();
        let mut indexes = Vec::new();
        let mut extra_exprs = Vec::new();
        let mut directions = Vec::new();

        for order in order_by {
            match &order.expr {
                Expression::Identifier(ident) => {
                    match resolve_column_for_order(self.columns, &self.schema, ident)? {
                        Either::Left(expr) => {
                            let typ = resolve_type(&self.schema, expr)?;
                            indexes.push(sorted_schema.len());
                            directions.push(order.direction);
                            sorted_schema.push(Column::new(&order.expr.to_string(), typ));
                            extra_exprs.push(expr.clone());
                        }
                        Either::Right(idx) => {
                            indexes.push(idx);
                            directions.push(order.direction);
                        }
                    }
                }

                Expression::QualifiedIdentifier { table, column } => {
                    match resolve_qualified_order_idx(&self.schema, column, table) {
                        Ok(idx) => {
                            indexes.push(idx);
                            directions.push(order.direction);
                        }

                        // if it fails, this might be a JSONB field
                        // so we treat like an expression that needs to be computed
                        Err(_) => {
                            let typ = resolve_type(&self.schema, &order.expr)?;
                            indexes.push(sorted_schema.len());
                            directions.push(order.direction);

                            sorted_schema.push(Column::new(&order.expr.to_string(), typ));
                            extra_exprs.push(order.expr.clone());
                        }
                    }
                }

                _ => {
                    let ty = resolve_type(&self.schema, &order.expr)?;
                    indexes.push(sorted_schema.len());
                    directions.push(order.direction);
                    sorted_schema.push(Column::new(&order.expr.to_string(), ty));
                    extra_exprs.push(order.expr.clone());
                }
            }
        }

        if !extra_exprs.is_empty() {
            let source = self.source.take().unwrap();
            self.source = Some(Planner::SortKeys(SortKeys {
                expressions: extra_exprs,
                schema: self.schema.clone(),
                source: Box::new(source),
            }));
        }

        let source = self.source.take().unwrap();
        self.source = Some(Planner::Sort(Sort::from(SortBuilder {
            page_size: self.page_size,
            work_dir: self.work_dir.clone(),
            input_buffers: DEFAULT_SORT_BUFFER_SIZE,
            collection: Collect::from(CollectBuilder {
                source: Box::new(source),
                schema: sorted_schema.clone(),
                work_dir: self.work_dir.clone(),
                mem_buff_size: self.page_size,
            }),
            comparator: TupleComparator::new(
                sorted_schema.clone(),
                sorted_schema,
                indexes,
                directions,
            ),
        })));

        Ok(())
    }

    pub fn apply_filter(&mut self, filter: Expression) {
        let source = self.source.take().unwrap();
        self.source = Some(Planner::Filter(Filter {
            source: Box::new(source),
            schema: self.schema.clone(),
            filter,
        }))
    }

    pub fn apply_projection(&mut self, output: Schema) {
        if self.schema.ne(&output) {
            let source = self.source.take().unwrap();
            self.source = Some(Planner::Project(Project {
                source: Box::new(source),
                input: self.schema.clone(),
                projection: self.columns.to_vec(),
                output,
            }))
        }
    }

    pub fn apply_joins(
        &mut self,
        from: &TableRef,
        joins: &'s [JoinClause],
        where_clause: Option<&Expression>,
        db: &mut Database<File>,
    ) -> Result<(), DatabaseError> {
        if joins.is_empty() {
            return Ok(());
        }

        let from = from.key();
        let (start, end) = self
            .ranges
            .get(from)
            .copied()
            .expect("FROM table should always be in ranges");
        self.schema.add_qualified_name(from, start, end);

        let mut left_tables = hash_set!(from.to_string());

        for join in joins {
            let right_table = db.metadata(&join.table.name)?.clone();
            let left_len = self.schema.len();

            let index_cadidate = join.index_cadidate(&right_table);
            let left = self.source.take().unwrap();

            let filter = where_clause.and_then(|expr| extract_table_filter(expr, join.table.key()));
            let right = query::optimiser::generate_plan(&join.table.name, filter, db)?;

            let use_inlj = {
                let left_cost = left.estimate();
                let right_cost = right.estimate();

                // TODO: we should do this in the cost module
                let inlj_cost = {
                    let right_rows_total = right_table.count.max(1);
                    let lookup = (right_rows_total as f64).log2().max(1.0);
                    left_cost.value + (left_cost.rows as f64 * lookup)
                };

                let hash_cost = {
                    let io = left_cost.value + right_cost.value;
                    let cpu_cost = (left_cost.rows + right_cost.rows) as f64 * Cost::CPU_TUPLE_COST;
                    io + cpu_cost
                };

                inlj_cost <= hash_cost
            };

            match (use_inlj, index_cadidate) {
                (true, Some((index, left_key_expr))) => {
                    let right_key = join.table.key();
                    let right_end = self.schema.len() + right_table.schema.len();

                    let should_be_null = matches!(join.join_type, JoinType::Left | JoinType::Full);
                    right_table
                        .schema
                        .columns
                        .clone()
                        .into_iter()
                        .for_each(|mut col| {
                            if !col.is_nullable() && should_be_null {
                                col.constraints.push(Constraint::Nullable);
                            }
                            self.schema.push(col)
                        });

                    self.schema
                        .add_qualified_name(right_key, left_len, right_end);
                    self.ranges.insert(right_key, (left_len, right_end));

                    let right_tables = hash_set!(right_key.to_string());
                    self.source = Some(Planner::IndexNestedLoopJoin(IndexNestedLoopJoin {
                        left: Box::new(left),
                        right_table: right_table.clone(),
                        index,
                        condition: join.on.clone(),
                        join_type: join.join_type,
                        left_key_expr,
                        pager: Rc::clone(&db.pager),
                        left_tables: left_tables.clone(),
                        right_tables,
                        schema: self.schema.clone(),
                    }));
                }

                _ => {
                    let should_be_null = matches!(join.join_type, JoinType::Left | JoinType::Full);

                    right_table
                        .schema
                        .columns
                        .clone()
                        .into_iter()
                        .for_each(|mut col| {
                            if !col.is_nullable() && should_be_null {
                                col.constraints.push(Constraint::Nullable);
                            }

                            self.schema.push(col)
                        });

                    let right_key = join.table.key();
                    let right_end = self.schema.len();
                    self.schema
                        .add_qualified_name(right_key, left_len, right_end);
                    self.ranges.insert(right_key, (left_len, right_end));

                    let right_tables = hash_set!(right_key.to_string());

                    let (left_expr, right_expr, key_type) = resolve_join_keys(
                        &join.on,
                        &left.schema().unwrap(),
                        &right_table.schema,
                        &left_tables,
                        &right_tables,
                    )?;

                    self.source = Some(Planner::HashJoin(HashJoin::new(
                        left,
                        right,
                        join.join_type,
                        self.schema.clone(),
                        left_expr,
                        right_expr,
                        key_type,
                    )));
                }
            };

            left_tables.insert(join.table.key().to_string());
            self.tables
                .insert(join.table.key().into(), right_table.schema);
        }

        Ok(())
    }

    pub fn apply_aggregation(
        &mut self,
        group_by: Vec<Expression>,
        order_by: &[OrderBy],
        output: Schema,
    ) -> Result<(), DatabaseError> {
        let is_grouped = !group_by.is_empty();

        let aggr_exprs: Vec<(&Expression, String)> = self
            .columns
            .iter()
            .filter_map(|expr| match expr {
                Expression::Alias { alias, expr } if contains_aggregate(expr) => {
                    Some((expr.as_ref(), alias.to_string()))
                }
                expr if contains_aggregate(expr) => {
                    let name = match expr {
                        Expression::Function { func, .. } => func.to_string(),
                        _ => expr.to_string(),
                    };

                    Some((expr, name))
                }

                _ => None,
            })
            .collect();

        let mut aggr_schema = Schema::empty();
        for expr in &group_by {
            match expr {
                Expression::Identifier(ident) => {
                    match self.columns.iter().find_map(|col_expr| match col_expr {
                        Expression::Alias { alias, expr } if alias == ident => Some(expr.as_ref()),
                        _ => None,
                    }) {
                        Some(col_expr) => aggr_schema
                            .push(Column::new(ident, resolve_type(&self.schema, col_expr)?)),
                        None => match self.schema.index_of(ident) {
                            Some(idx) => aggr_schema.push(self.schema.columns[idx].clone()),
                            _ => return Err(SqlError::InvalidColumn(ident.clone()).into()),
                        },
                    }
                }
                other => aggr_schema.push(Column::new(
                    &other.to_string(),
                    resolve_type(&self.schema, other)?,
                )),
            }
        }

        let implicit_groups: Vec<_> =
            find_implicit_group_columns(self.columns, &group_by).collect();
        for col_expr in &implicit_groups {
            let col = col_expr.unwrap_name();
            if aggr_schema.index_of(&col).is_none() {
                let col_type = resolve_type(&self.schema, col_expr)?;
                aggr_schema.push(Column::new(&col, col_type));
            }
        }

        for (fun, name) in &aggr_exprs {
            aggr_schema.push(Column::new(name, resolve_type(&self.schema, fun)?));
        }

        if group_by.is_empty() && !order_by.is_empty() {
            let (indexes, directions) =
                extract_order_indexes_and_directions(&self.schema, order_by)?;
            let old_source = self.source.take().unwrap();
            self.source = Some(Planner::Sort(Sort::from(SortBuilder {
                page_size: self.page_size,
                work_dir: self.work_dir.clone(),
                input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                collection: Collect::from(CollectBuilder {
                    source: Box::new(old_source),
                    schema: self.schema.clone(),
                    work_dir: self.work_dir.clone(),
                    mem_buff_size: self.page_size,
                }),
                comparator: TupleComparator::new(
                    self.schema.clone(),
                    self.schema.clone(),
                    indexes,
                    directions,
                ),
            })));
        }

        let mut resolved_group_by: Vec<Expression> = group_by
            .iter()
            .map(|expr| {
                if let Expression::Identifier(ident) = expr {
                    for col_expr in self.columns {
                        if col_expr.unwrap_name().as_ref() == ident {
                            return col_expr.clone();
                        }
                    }
                }
                expr.clone()
            })
            .collect();

        for col_expr in implicit_groups {
            if !resolved_group_by
                .iter()
                .any(|g| g.unwrap_name() == col_expr.unwrap_name())
            {
                resolved_group_by.push(col_expr.clone())
            }
        }

        let source = self.source.take().unwrap();
        self.source = Some(Planner::Aggregate(
            AggregateBuilder {
                source: Box::new(source),
                aggr_exprs: aggr_exprs.iter().map(|(expr, _)| (*expr).clone()).collect(),
                page_size: self.page_size,
                group_by: resolved_group_by,
                output: aggr_schema.clone(),
            }
            .into(),
        ));

        if is_grouped && !order_by.is_empty() {
            let (indexes, directions) =
                extract_order_indexes_and_directions(&aggr_schema, order_by)?;
            let old_source = self.source.take().unwrap();
            self.source = Some(Planner::Sort(Sort::from(SortBuilder {
                page_size: self.page_size,
                work_dir: self.work_dir.clone(),
                input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                collection: Collect::from(CollectBuilder {
                    source: Box::new(old_source),
                    schema: aggr_schema.clone(),
                    work_dir: self.work_dir.clone(),
                    mem_buff_size: self.page_size,
                }),
                comparator: TupleComparator::new(
                    aggr_schema.clone(),
                    aggr_schema.clone(),
                    indexes,
                    directions,
                ),
            })));
        }

        if output.ne(&aggr_schema) {
            let projection: Vec<Expression> = self
                .columns
                .iter()
                .map(|expr| match expr {
                    Expression::Alias { .. } => Expression::Identifier(expr.unwrap_name().into()),
                    Expression::Function { func, .. } => Expression::Identifier(func.to_string()),
                    other => other.clone(),
                })
                .collect();

            let old_source = self.source.take().unwrap();
            self.source = Some(Planner::Project(Project {
                output,
                projection,
                input: aggr_schema,
                source: Box::new(old_source),
            }));
        }

        Ok(())
    }

    pub fn apply_limit(&mut self, limit: Option<usize>, offset: Option<usize>) {
        if limit.is_some() || offset.is_some() {
            let source = self.source.take().unwrap();
            self.source = Some(Planner::Limit(Limit {
                source: Box::new(source),
                limit: limit.unwrap_or(usize::MAX),
                offset: offset.unwrap_or(0),
                count: 0,
            }));
        }
    }

    pub fn build_output_schema(&self) -> Result<Schema, SqlError> {
        let cols = self
            .columns
            .iter()
            .map(|expr| match expr {
                Expression::Alias { expr, alias } => match expr.as_ref() {
                    Expression::QualifiedIdentifier { column, table } => {
                        match resolve_qualified_column(table, column, &self.tables) {
                            Ok(mut col) => {
                                col.name = alias.clone();
                                Ok(col)
                            }
                            _ => create_jsonb_field_column(
                                &self.schema,
                                table,
                                &expr.to_string(),
                                expr,
                            ),
                        }
                    }

                    Expression::Path { .. } => {
                        Ok(Column::nullable(alias, resolve_type(&self.schema, expr)?))
                    }
                    _ => Ok(Column::new(alias, resolve_type(&self.schema, expr)?)),
                },
                Expression::QualifiedIdentifier { column, table } => {
                    match resolve_qualified_column(table, column, &self.tables) {
                        Ok(col) => Ok(col),
                        Err(_) => {
                            create_jsonb_field_column(&self.schema, table, &expr.to_string(), expr)
                        }
                    }
                }
                Expression::Identifier(column) => {
                    Ok(self.schema.columns[self.schema.index_of(column).unwrap()].clone())
                }
                Expression::Path { .. } => Ok(Column::nullable(
                    &expr.to_string(),
                    resolve_type(&self.schema, expr)?,
                )),
                Expression::Function { func, .. } => Ok(Column::new(
                    &func.to_string(),
                    resolve_type(&self.schema, expr)?,
                )),
                _ => Ok(Column::new(
                    &expr.to_string(),
                    resolve_type(&self.schema, expr)?,
                )),
            })
            .collect::<Result<Vec<_>, SqlError>>()?;

        Ok(Schema::new(cols))
    }
}

fn resolve_column_for_order<'a>(
    columns: &'a [Expression],
    schema: &Schema,
    ident: &str,
) -> Result<Either<&'a Expression, usize>, SqlError> {
    for expr in columns {
        if let Expression::Alias {
            expr: aliased_expr,
            alias,
        } = expr
        {
            if alias == ident {
                return match aliased_expr.as_ref() {
                    Expression::Identifier(_) | Expression::QualifiedIdentifier { .. } => {
                        match schema.index_of(&aliased_expr.to_string()) {
                            Some(idx) => Ok(Either::Right(idx)),
                            None => Ok(Either::Left(aliased_expr.as_ref())),
                        }
                    }
                    _ => Ok(Either::Left(aliased_expr.as_ref())),
                };
            }
        }
    }

    schema
        .index_of(ident)
        .map(Either::Right)
        .ok_or(SqlError::InvalidColumn(ident.into()))
}

fn resolve_qualified_order_idx(
    schema: &Schema,
    column: &str,
    table: &str,
) -> Result<usize, SqlError> {
    let qualified = format!("{table}.{column}");
    schema
        .index_of(&qualified)
        .or(schema.last_index_of(column))
        .ok_or(SqlError::InvalidQualifiedColumn {
            table: table.into(),
            column: column.into(),
        })
}

fn resolve_qualified_column(
    table: &str,
    column: &str,
    tables: &HashMap<String, Schema>,
) -> Result<Column, SqlError> {
    let schema = tables
        .get(table)
        .ok_or(SqlError::InvalidTable(table.to_string()))?;

    let idx = schema
        .index_of(column)
        .ok_or(SqlError::InvalidQualifiedColumn {
            table: table.into(),
            column: column.into(),
        })?;

    Ok(schema.columns[idx].clone())
}

fn extract_order_indexes_and_directions(
    schema: &Schema,
    order_by: &[OrderBy],
) -> Result<(Vec<usize>, Vec<OrderDirection>), DatabaseError> {
    order_by
        .iter()
        .map(|order| {
            let idx = match &order.expr {
                Expression::Identifier(ident) => {
                    // Look up the identifier in the schema (whether it's an alias or not)
                    schema
                        .index_of(ident)
                        .ok_or_else(|| DatabaseError::Sql(SqlError::InvalidGroupBy(ident.into())))
                        .map(|idx| (idx, order.direction))
                }
                _ => schema
                    .index_of(&order.expr.to_string())
                    .ok_or_else(|| {
                        DatabaseError::Sql(SqlError::Other(format!(
                            "ORDER BY expression `{}` not found in output columns",
                            order.expr
                        )))
                    })
                    .map(|idx| (idx, order.direction)),
            }?;
            Ok(idx)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|p| p.into_iter().unzip())
}

fn resolve_join_keys(
    condition: &Expression,
    left_schema: &Schema,
    right_schema: &Schema,
    left_tables: &HashSet<String>,
    right_tables: &HashSet<String>,
) -> Result<(Expression, Expression, VmType), DatabaseError> {
    use crate::sql::analyzer::analyze_expression;
    use crate::sql::statement::BinaryOperator;

    let Expression::BinaryOperation {
        operator,
        ref left,
        ref right,
    } = condition
    else {
        return Err(DatabaseError::Other(
            "HashJoin requires a binary operation condition".into(),
        ));
    };

    if *operator != BinaryOperator::Eq {
        return Err(DatabaseError::Other(
            "HashJoin requires an equal operator".into(),
        ));
    }

    let left_expr = &**left;
    let right_expr = &**right;

    let check_side = |expr: &Expression, tables: &HashSet<String>, schema: &Schema| -> bool {
        match expr {
            Expression::QualifiedIdentifier { table, column } => {
                if !tables.is_empty() && !tables.contains(table) {
                    return false;
                }
                schema.last_index_of(column).is_some()
            }
            _ => analyze_expression(schema, None, expr).is_ok(),
        }
    };

    let left_is_left = check_side(left_expr, left_tables, left_schema);
    let right_is_right = check_side(right_expr, right_tables, right_schema);

    if left_is_left && right_is_right {
        let r#type = analyze_expression(left_schema, None, left_expr)?;
        return Ok((left_expr.clone(), right_expr.clone(), r#type));
    }

    let left_is_right = check_side(left_expr, right_tables, right_schema);
    let right_is_left = check_side(right_expr, left_tables, left_schema);

    if left_is_right && right_is_left {
        let r#type = analyze_expression(left_schema, None, right_expr)?;
        return Ok((right_expr.clone(), left_expr.clone(), r#type));
    }

    Err(DatabaseError::Other(format!(
        "Ambiguous or invalid JOIN condition for HashJoin: {}",
        condition
    )))
}

/// Finds non-aggregate columns in SELECT that are not explicitly in GROUP BY.
/// These columns are allowed by MySQL-style GROUP BY extension when they are
/// functionally dependent on the GROUP BY columns (e.g., other columns from
/// a table whose primary key is in GROUP BY).
///
/// Note: The semantic validation is performed by the analyzer, which checks
/// [functional dependency](crate::sql::analyzer::is_functionally_dependent) via primary key relationships. This function simply
/// identifies which columns need to be implicitly added to the aggregate output.
fn find_implicit_group_columns<'e>(
    columns: &'e [Expression],
    group_by: &'e [Expression],
) -> impl Iterator<Item = &'e Expression> {
    columns.iter().filter(|c| {
        if contains_aggregate(c) {
            return false;
        }

        let col = c.unwrap_name();
        let in_group_by = group_by.iter().any(|g| g.unwrap_name() == col || g.eq(c));

        !in_group_by
    })
}

fn extract_table_filter(expr: &Expression, table_key: &str) -> Option<Expression> {
    use crate::sql::statement::BinaryOperator;

    match expr {
        Expression::BinaryOperation {
            left,
            operator: BinaryOperator::And,
            right,
        } => {
            let left_filter = extract_table_filter(left, table_key);
            let right_filter = extract_table_filter(right, table_key);

            match (left_filter, right_filter) {
                (Some(l), Some(r)) => Some(Expression::BinaryOperation {
                    left: Box::new(l),
                    operator: BinaryOperator::And,
                    right: Box::new(r),
                }),
                (Some(f), None) | (None, Some(f)) => Some(f),
                (None, None) => None,
            }
        }
        Expression::Nested(inner) => extract_table_filter(inner, table_key),
        _ => match is_bound_to_table(expr, table_key) {
            true => Some(expr.clone()),
            _ => None,
        },
    }
}

fn is_bound_to_table(expr: &Expression, table_key: &str) -> bool {
    match expr {
        Expression::QualifiedIdentifier { table, .. } => table == table_key,
        Expression::BinaryOperation { left, right, .. } => {
            is_bound_to_table(left, table_key) && is_bound_to_table(right, table_key)
        }
        Expression::UnaryOperation { expr, .. } => is_bound_to_table(expr, table_key),
        Expression::Nested(expr) => is_bound_to_table(expr, table_key),
        Expression::Function { args, .. } => {
            args.iter().all(|arg| is_bound_to_table(arg, table_key))
        }
        Expression::Value(_) => true,
        _ => false,
    }
}

fn create_jsonb_field_column(
    schema: &Schema,
    table: &str,
    name: &str,
    expr: &Expression,
) -> Result<Column, SqlError> {
    use crate::sql::statement::Type;

    let is_json = schema
        .index_of(table)
        .map(|idx| matches!(schema.columns[idx].data_type, Type::Jsonb))
        .unwrap_or_default();

    let col_type = match is_json {
        true => Type::Jsonb,
        _ => resolve_type(schema, expr)?,
    };

    match is_json {
        true => Ok(Column::nullable(name, col_type)),
        _ => Ok(Column::new(name, col_type)),
    }
}
