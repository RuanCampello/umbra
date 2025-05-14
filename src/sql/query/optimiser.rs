use std::{
    collections::{HashMap, VecDeque},
    ops::{Bound, RangeBounds},
    rc::Rc,
};

use crate::{
    core::storage::{btree::Cursor, tuple},
    db::{Ctx, Database, DatabaseError, IndexMetadata, Relation},
    sql::{
        parser::Parser,
        statement::{BinaryOperator, Expression, Value},
    },
    vm::planner::{
        Collect, ExactMatch, Filter, KeyScan, LogicalScan, PlanExecutor, Planner, RangeScan,
        SeqScan, Sort, TupleComparator, DEFAULT_SORT_BUFFER_SIZE,
    },
};

type IndexBounds<'value> = (Bound<&'value Value>, Bound<&'value Value>);

pub(in crate::sql::query) fn generate_seq_plan<File: PlanExecutor>(
    table: &str,
    mut filter: Option<Expression>,
    db: &mut Database<File>,
) -> Result<Planner<File>, DatabaseError> {
    let source = match generate_optimised_seq_plan(table, db, &mut filter)? {
        Some(plan) => plan,
        None => generate_seq_scan_plan(table, db)?,
    };

    let Some(expr) = filter else {
        return Ok(source);
    };

    Ok(Planner::Filter(Filter {
        source: Box::new(source),
        schema: db.metadata(table)?.schema.clone(),
        filter: expr,
    }))
}

fn generate_optimised_seq_plan<File: PlanExecutor>(
    table: &str,
    db: &mut Database<File>,
    filter: &mut Option<Expression>,
) -> Result<Option<Planner<File>>, DatabaseError> {
    let Some(expr) = filter else {
        return Ok(None);
    };
    let table = db.metadata(table)?.clone();
    let paths: HashMap<&str, VecDeque<IndexBounds>> = HashMap::new();

    if paths.is_empty() {
        return Ok(None);
    }

    let indexes: HashMap<&str, &IndexMetadata> = table
        .indexes
        .iter()
        .filter(|idx| paths.contains_key(idx.column.name.as_str()))
        .map(|idx| (idx.column.name.as_str(), idx))
        .collect();

    let mut index_scans: Vec<(&str, VecDeque<Planner<File>>)> = paths
        .into_iter()
        .map(|(col, ranges)| {
            let relation = match indexes.get(col) {
                Some(&index) => Relation::Index(index.clone()),
                None => Relation::Table(table.clone()),
            };

            let col_position = table.schema.index_of(col).unwrap();
            let r#type = table.schema.columns[col_position].data_type.clone();

            let bounds = ranges.iter().map(|range| {
                let left = range
                    .start_bound()
                    .map(|value| tuple::serialize(&r#type, value));
                let right = range
                    .end_bound()
                    .map(|value| tuple::serialize(&r#type, value));

                let expr = range_to_expr(col, *range);
                let pager = Rc::clone(&db.pager);
                let relation = relation.clone();

                if is_exact_match(*range) {
                    let Bound::Included(key) = left else {
                        unreachable!();
                    };

                    Planner::ExactMatch(ExactMatch {
                        key,
                        relation,
                        expr,
                        pager,
                        emit_only_key: true,
                        done: false,
                    })
                } else {
                    Planner::RangeScan(RangeScan::new((left, right), relation, true, expr, pager))
                }
            });

            (col, bounds.collect())
        })
        .collect();

    index_scans.sort_by_key(|(col, _)| match indexes.get(col) {
        Some(index) => index.root,
        None => 0,
    });

    let scan_only_one_index = index_scans
        .first()
        .take_if(|_| index_scans.len().eq(&1))
        .map(|(col, _)| String::from(*col));
    let is_only_scan = scan_only_one_index
        .as_ref()
        .is_some_and(|col| col.eq(&table.schema.columns[0].name));

    let mut planners: VecDeque<Planner<File>> =
        index_scans.into_iter().flat_map(|(_, scan)| scan).collect();

    if is_only_scan {
        planners.iter_mut().for_each(|planner| match planner {
            Planner::RangeScan(range_scan) => range_scan.emit_only_key = false,
            Planner::ExactMatch(exact_match) => exact_match.emit_only_key = false,
            _ => unreachable!(),
        });
    }

    let mut source = match planners.len().eq(&1) {
        true => planners.pop_front().unwrap(),
        false => Planner::LogicalScan(LogicalScan { scans: planners }),
    };

    if let Some(col) = scan_only_one_index {
        skip_col_conditions(&col, expr);

        if *expr == Expression::Wildcard {
            *filter = None;
        }
    }

    if is_only_scan {
        return Ok(Some(source));
    }

    if let Planner::RangeScan(_) | Planner::LogicalScan(_) = source {
        let collection = Collect::new(
            Box::new(source),
            table.key_only_schema(),
            db.work_dir.clone(),
            db.pager.borrow().page_size,
        );
        let comparator =
            TupleComparator::new(table.key_only_schema(), table.key_only_schema(), vec![0]);
        source = Planner::Sort(Sort::new(
            db.pager.borrow().page_size,
            db.work_dir.clone(),
            collection,
            comparator,
            DEFAULT_SORT_BUFFER_SIZE,
        ));
    }

    Ok(Some(Planner::KeyScan(KeyScan {
        comparator: table.comp()?,
        pager: Rc::clone(&db.pager),
        source: Box::new(source),
        table,
    })))
}

fn generate_seq_scan_plan<File: PlanExecutor>(
    table: &str,
    db: &mut Database<File>,
) -> Result<Planner<File>, DatabaseError> {
    let metadata = db.metadata(table)?;

    Ok(Planner::SeqScan(SeqScan {
        cursor: Cursor::new(metadata.root, 0),
        table: metadata.clone(),
        pager: Rc::clone(&db.pager),
    }))
}

fn range_to_expr(col: &str, (start, end): (Bound<&Value>, Bound<&Value>)) -> Expression {
    let expr = match (start, end) {
        (Bound::Unbounded, Bound::Excluded(v)) => format!("{col} < {v}"),
        (Bound::Unbounded, Bound::Included(v)) => format!("{col} <= {v}"),
        (Bound::Excluded(v), Bound::Unbounded) => format!("{col} > {v}"),
        (Bound::Included(v), Bound::Unbounded) => format!("{col} >= {v}"),
        (Bound::Excluded(v1), Bound::Excluded(v2)) => format!("{col} > {v1} AND {col} < {v2}"),
        (Bound::Excluded(v1), Bound::Included(v2)) => format!("{col} > {v1} AND {col} <= {v2}"),
        (Bound::Included(v1), Bound::Excluded(v2)) => format!("{col} >= {v1} AND {col} < {v2}"),
        (Bound::Included(v1), Bound::Included(v2)) => match is_exact_match((start, end)) {
            true => format!("{col} = {v1}"),
            false => format!("{col} >= {v1} AND {col} <= {v2}"),
        },
        _ => unreachable!("Cannot build expression from {:?}", (start, end)),
    };

    Parser::new(&expr).parse_expr(None).unwrap()
}

fn is_exact_match(range: IndexBounds) -> bool {
    let (Bound::Included(v1), Bound::Included(v2)) = range else {
        return false;
    };

    if std::ptr::eq(v1, v2) {
        return true;
    }

    v1 == v2
}

fn skip_col_conditions(col: &str, expr: &mut Expression) {
    let Expression::BinaryOperation {
        operator,
        left,
        right,
    } = expr
    else {
        return;
    };

    match operator {
        BinaryOperator::And | BinaryOperator::Or => {
            skip_col_conditions(col, left);
            skip_col_conditions(col, right);

            if **left == Expression::Wildcard {
                *expr = std::mem::replace(right, Expression::Wildcard);
            } else if **right == Expression::Wildcard {
                *expr = std::mem::replace(left, Expression::Wildcard);
            }
        }
        _ => match (&**left, &**right) {
            (Expression::Identifier(ident), _) | (_, Expression::Identifier(ident))
                if ident.eq(&col) =>
            {
                *expr = Expression::Wildcard;
            }
            _ => {}
        },
    }
}
