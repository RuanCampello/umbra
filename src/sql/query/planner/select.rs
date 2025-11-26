use std::{
    collections::{HashMap, HashSet},
    io::{Read, Seek, Write},
    path::PathBuf,
    rc::Rc,
};

use crate::{
    core::storage::pagination::io::FileOperations,
    db::{Ctx, Database, DatabaseError, Schema, SqlError},
    sql::{
        analyzer::contains_aggregate,
        query::{self, planner::resolve_type},
        statement::{
            Column, Constraint, Expression, JoinClause, JoinType, OrderBy, OrderDirection, TableRef,
        },
    },
    vm::planner::{
        AggregateBuilder, Collect, CollectBuilder, Filter, HashJoin, IndexNestedLoopJoin, Limit,
        Planner, Project, Sort, SortBuilder, SortKeys, TupleComparator, DEFAULT_SORT_BUFFER_SIZE,
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
        let mut tables = HashMap::new();
        tables.insert(table_key.to_string(), schema.clone());

        let mut ranges = HashMap::new();
        ranges.insert(table_key, (0, schema.len()));

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
                    let idx = resolve_qualified_order_idx(&self.schema, column, table)?;
                    indexes.push(idx);
                    directions.push(order.direction)
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

        let mut left_tables = HashSet::from([from.to_string()]);

        for join in joins {
            let right_table = db.metadata(&join.table.name)?.clone();
            let left_len = self.schema.len();

            let index_cadidate = join.index_cadidate(&right_table);
            let left = self.source.take().unwrap();

            // heuristic: we only want to use index nested loop join if the left side is selective
            // if the left side is a sequential scan, we prefer hash join
            let use_inlj = match (&index_cadidate, &left) {
                (Some(_), Planner::KeyScan(_) | Planner::ExactMatch(_) | Planner::RangeScan(_)) => {
                    true
                }
                _ => false,
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

                    let right_tables = HashSet::from([right_key.to_string()]);
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
                        current_left: None,
                        current_right: None,
                        match_found: false,
                    }));
                }

                _ => {
                    let right = query::optimiser::generate_seq_plan(&join.table.name, None, db)?;
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

                    let right_tables = HashSet::from([right_key.to_string()]);

                    self.source = Some(Planner::HashJoin(
                        HashJoin::new(left, right, join.join_type, join.on.clone())
                            .with_table_names(left_tables.clone(), right_tables),
                    ));
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
                expr if contains_aggregate(expr) => Some((expr, expr.to_string())),
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

        for (fun, name) in &aggr_exprs {
            aggr_schema.push(Column::new(&name, resolve_type(&self.schema, fun)?));
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

        let resolved_group_by: Vec<Expression> = group_by
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
                        let mut column = resolve_qualified_column(table, column, &self.tables)?;
                        column.name = alias.clone();
                        Ok(column)
                    }
                    _ => Ok(Column::new(alias, resolve_type(&self.schema, expr)?)),
                },
                Expression::QualifiedIdentifier { column, table } => {
                    resolve_qualified_column(table, column, &self.tables)
                }
                Expression::Identifier(column) => {
                    Ok(self.schema.columns[self.schema.index_of(column).unwrap()].clone())
                }
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
    for (i, expr) in columns.iter().enumerate() {
        if let Expression::Alias {
            expr: aliased_expr,
            alias,
        } = expr
        {
            if alias == ident {
                return match aliased_expr.as_ref() {
                    Expression::Identifier(_) | Expression::QualifiedIdentifier { .. } => {
                        Ok(Either::Right(i))
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
                            order.expr.to_string()
                        )))
                    })
                    .map(|idx| (idx, order.direction)),
            }?;
            Ok(idx)
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|p| p.into_iter().unzip())
}
