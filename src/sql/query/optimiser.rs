use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    ops::{Bound, RangeBounds},
    rc::Rc,
};

use crate::{
    core::storage::{btree::Cursor, tuple},
    db::{Ctx, Database, DatabaseError, IndexMetadata, Relation},
    sql::{
        parser::Parser,
        statement::{BinaryOperator, Expression, OrderDirection, Value},
    },
    vm::planner::{
        Collect, CollectBuilder, ExactMatch, Filter, KeyScan, LogicalScan, PlanExecutor, Planner,
        RangeScan, SeqScan, Sort, SortBuilder, TupleComparator, DEFAULT_SORT_BUFFER_SIZE,
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
    let paths = find_index_paths(
        &table.schema.columns[0].name,
        &HashSet::from_iter(table.indexes.iter().map(|index| index.column.name.as_str())),
        expr,
        &mut HashSet::new(),
    );

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
        let page_size = db.pager.borrow().page_size;
        let work_dir = db.work_dir.clone();

        let (indexes, directions): (Vec<_>, Vec<_>) =
            vec![(0, OrderDirection::default())].into_iter().unzip();

        let comparator = TupleComparator::new(
            table.key_only_schema(),
            table.key_only_schema(),
            indexes,
            directions,
        );

        source = Planner::Sort(Sort::from(SortBuilder {
            page_size,
            comparator,
            work_dir: work_dir.clone(),
            input_buffers: DEFAULT_SORT_BUFFER_SIZE,
            collection: Collect::from(CollectBuilder {
                work_dir,
                source: Box::new(source),
                schema: table.key_only_schema(),
                mem_buff_size: page_size,
            }),
        }));
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

fn find_index_paths<'exp>(
    k_col: &str,
    indexes: &HashSet<&str>,
    expr: &'exp Expression,
    cancel: &mut HashSet<&'exp str>,
) -> HashMap<&'exp str, VecDeque<IndexBounds<'exp>>> {
    match expr {
        Expression::BinaryOperation {
            operator,
            left,
            right,
        } => match (&**left, &**right) {
            (Expression::Identifier(col), Expression::Value(_))
            | (Expression::Value(_), Expression::Identifier(col))
                if (indexes.contains(col.as_str()) || col == k_col)
                    && matches!(
                        operator,
                        BinaryOperator::Eq
                            | BinaryOperator::Lt
                            | BinaryOperator::LtEq
                            | BinaryOperator::Gt
                            | BinaryOperator::GtEq
                    ) =>
            {
                HashMap::from([(col.as_str(), VecDeque::from([determine_bounds(expr)]))])
            }
            (left, right) if matches!(operator, BinaryOperator::And | BinaryOperator::Or) => {
                let mut left_paths = find_index_paths(k_col, indexes, left, cancel);
                let mut right_paths = find_index_paths(k_col, indexes, right, cancel);

                match operator {
                    BinaryOperator::And => {
                        if left_paths.is_empty() && right_paths.is_empty() {
                            return left_paths;
                        }

                        'intersection: {
                            if left_paths.len() > 1 && right_paths.len() > 1 {
                                break 'intersection;
                            };

                            let (col, other) = {
                                let left_col = left_paths.iter().next();
                                let right_col = right_paths.iter().next();

                                if let Some((col, _)) = left_col {
                                    (*col, &right_paths)
                                } else {
                                    (*right_col.unwrap().0, &left_paths)
                                }
                            };

                            if cancel.contains(col) && (other.is_empty() || other.contains_key(col))
                            {
                                return HashMap::new();
                            }

                            if !other.contains_key(col) {
                                break 'intersection;
                            }

                            let (_, left_bounds) = left_paths.iter_mut().next().unwrap();
                            let (_, right_bounds) = right_paths.iter_mut().next().unwrap();

                            if left_bounds.len() > 1 && right_bounds.len() > 1 {
                                break 'intersection;
                            }

                            let factor = match left_bounds.len().eq(&1) {
                                true => left_bounds.pop_front().unwrap(),
                                false => right_bounds.pop_front().unwrap(),
                            };

                            let mut intersections = VecDeque::new();
                            for range in left_bounds.drain(..).chain(right_bounds.drain(..)) {
                                if let Some(intersection) = range_intersection(factor, range) {
                                    intersections.push_back(intersection);
                                }
                            }

                            if intersections.is_empty() {
                                cancel.insert(col);
                                return HashMap::new();
                            }

                            *left_bounds = intersections;

                            return left_paths;
                        };

                        if right_paths.is_empty()
                            || !left_paths.is_empty() && left_paths.len() < right_paths.len()
                        {
                            return left_paths;
                        }

                        if left_paths.is_empty()
                            || !right_paths.is_empty() && right_paths.len() < left_paths.len()
                        {
                            return right_paths;
                        }

                        let left_is_exact_match = left_paths
                            .iter()
                            .all(|(_, bounds)| bounds.iter().copied().all(is_exact_match));

                        let right_is_exact_math = right_paths
                            .iter()
                            .all(|(_, bounds)| bounds.iter().copied().all(is_exact_match));

                        let right_contains_key = right_paths.contains_key(k_col);

                        if right_is_exact_math && (!left_is_exact_match || right_contains_key) {
                            return right_paths;
                        }

                        left_paths
                    }
                    BinaryOperator::Or => {
                        if left_paths.is_empty() || right_paths.is_empty() {
                            return HashMap::new();
                        }

                        let mut merged: HashMap<&str, VecDeque<IndexBounds>> = HashMap::new();

                        for (col, mut left_bounds) in left_paths.into_iter() {
                            let Some(mut right_bounds) = right_paths.remove(col) else {
                                merged.insert(col, left_bounds);
                                continue;
                            };

                            let mut sorted_bounds = VecDeque::new();
                            while !left_bounds.is_empty() && !right_bounds.is_empty() {
                                sorted_bounds.push_back(
                                    match cmp_ranges(&left_bounds[0], &right_bounds[0])
                                        .ne(&Ordering::Greater)
                                    {
                                        true => left_bounds.pop_front().unwrap(),
                                        false => right_bounds.pop_front().unwrap(),
                                    },
                                );
                            }

                            sorted_bounds.append(&mut left_bounds);
                            sorted_bounds.append(&mut right_bounds);

                            let mut bounds_union = VecDeque::new();
                            for range in sorted_bounds {
                                match bounds_union.back_mut() {
                                    Some(previous) => match range_union(*previous, range) {
                                        Some(union) => *previous = union,
                                        None => bounds_union.push_back(range),
                                    },
                                    None => bounds_union.push_back(range),
                                }
                            }

                            if bounds_union[0].ne(&(Bound::Unbounded, Bound::Unbounded)) {
                                merged.insert(col, bounds_union);
                            }
                        }
                        merged.extend(right_paths);

                        merged
                    }

                    _ => unreachable!(),
                }
            }
            _ => HashMap::new(),
        },
        Expression::Nested(inner) => find_index_paths(k_col, indexes, inner, cancel),
        _ => HashMap::new(),
    }
}

fn determine_bounds(expr: &Expression) -> (Bound<&Value>, Bound<&Value>) {
    let Expression::BinaryOperation {
        left,
        operator,
        right,
    } = expr
    else {
        unreachable!("determine_bounds() called with non-binary expression: {expr}");
    };

    match (&**left, operator, &**right) {
        // SELECT * FROM t WHERE x = 13;
        // SELECT * FROM t WHERE 13 = x;
        (Expression::Identifier(_col), BinaryOperator::Eq, Expression::Value(value))
        | (Expression::Value(value), BinaryOperator::Eq, Expression::Identifier(_col)) => {
            (Bound::Included(value), Bound::Included(value))
        }

        // SELECT * FROM t WHERE x > 13;
        // SELECT * FROM t WHERE 13 < x;
        (Expression::Identifier(_col), BinaryOperator::Gt, Expression::Value(value))
        | (Expression::Value(value), BinaryOperator::Lt, Expression::Identifier(_col)) => {
            (Bound::Excluded(value), Bound::Unbounded)
        }

        // SELECT * FROM t WHERE x < 13;
        // SELECT * FROM t WHERE 13 > x;
        (Expression::Identifier(_col), BinaryOperator::Lt, Expression::Value(value))
        | (Expression::Value(value), BinaryOperator::Gt, Expression::Identifier(_col)) => {
            (Bound::Unbounded, Bound::Excluded(value))
        }

        // SELECT * FROM t WHERE x >= 13;
        // SELECT * FROM t WHERE 13 <= x;
        (Expression::Identifier(_col), BinaryOperator::GtEq, Expression::Value(value))
        | (Expression::Value(value), BinaryOperator::LtEq, Expression::Identifier(_col)) => {
            (Bound::Included(value), Bound::Unbounded)
        }

        // SELECT * FROM t WHERE x <= 13;
        // SELECT * FROM t WHERE 13 >= x;
        (Expression::Identifier(_col), BinaryOperator::LtEq, Expression::Value(value))
        | (Expression::Value(value), BinaryOperator::GtEq, Expression::Identifier(_col)) => {
            (Bound::Unbounded, Bound::Included(value))
        }

        _ => unreachable!("determine_bounds() called with unsupported operator: {expr}"),
    }
}

fn range_intersection<'value>(
    (start_one, end_one): (Bound<&'value Value>, Bound<&'value Value>),
    (start_two, end_two): (Bound<&'value Value>, Bound<&'value Value>),
) -> Option<(Bound<&'value Value>, Bound<&'value Value>)> {
    let intersection_start = std::cmp::max_by(start_one, start_two, cmp_start_bounds);
    let intersection_end = std::cmp::min_by(end_one, end_two, cmp_end_bounds);

    if cmp_start_to_end(&intersection_start, &intersection_end).eq(&Ordering::Greater) {
        return None;
    }

    Some((intersection_start, intersection_end))
}

fn range_union<'value>(
    (start_one, end_one): (Bound<&'value Value>, Bound<&'value Value>),
    (start_two, end_two): (Bound<&'value Value>, Bound<&'value Value>),
) -> Option<(Bound<&'value Value>, Bound<&'value Value>)> {
    debug_assert!(
        cmp_start_bounds(&start_one, &start_two).ne(&Ordering::Greater),
        "Ranges should be sorted at this point {:?} {:?}",
        (start_one, end_one),
        (start_two, end_two)
    );

    if cmp_start_to_end(&start_two, &end_one).eq(&Ordering::Greater) {
        return None;
    }

    let union_start = std::cmp::min_by(start_one, start_two, cmp_start_bounds);
    let union_end = std::cmp::max_by(end_one, end_two, cmp_end_bounds);

    Some((union_start, union_end))
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

fn cmp_start_bounds(bound1: &Bound<&Value>, bound2: &Bound<&Value>) -> Ordering {
    match (bound1, bound2) {
        (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
        (Bound::Unbounded, _) => Ordering::Less,
        (_, Bound::Unbounded) => Ordering::Greater,
        (
            Bound::Excluded(value1) | Bound::Included(value1),
            Bound::Excluded(value2) | Bound::Included(value2),
        ) => {
            let ordering = value1.partial_cmp(value2).unwrap_or_else(|| {
                panic!("Type errors at this point should be impossible: cmp {value1} to {value2}")
            });

            if ordering != Ordering::Equal {
                return ordering;
            }

            match (bound1, bound2) {
                (Bound::Included(_), Bound::Excluded(_)) => Ordering::Less,
                (Bound::Excluded(_), Bound::Included(_)) => Ordering::Greater,
                _ => Ordering::Equal,
            }
        }
    }
}

fn cmp_end_bounds(end_one: &Bound<&Value>, end_two: &Bound<&Value>) -> Ordering {
    let ordering = cmp_start_bounds(end_one, end_two);

    if let (Bound::Unbounded, _) | (_, Bound::Unbounded) = (end_one, end_two) {
        return ordering.reverse();
    }

    ordering
}

fn cmp_start_to_end(start: &Bound<&Value>, end: &Bound<&Value>) -> Ordering {
    if start.eq(&Bound::Unbounded) {
        return Ordering::Less;
    }

    cmp_end_bounds(start, end)
}

fn cmp_ranges(
    (start_one, end_one): &(Bound<&Value>, Bound<&Value>),
    (start_two, end_two): &(Bound<&Value>, Bound<&Value>),
) -> Ordering {
    let start_ordering = cmp_start_bounds(start_one, start_two);

    match cmp_start_bounds(start_one, start_two).ne(&Ordering::Equal) {
        true => start_ordering,
        false => cmp_end_bounds(end_one, end_two),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    struct IndexPath<'idx> {
        pk: &'idx str,
        indexes: &'idx [&'idx str],
        expr: &'idx str,
        expected: HashMap<&'idx str, VecDeque<IndexBounds<'idx>>>,
    }

    impl<'idx> IndexPath<'idx> {
        fn assert(&self) {
            let expr = Parser::new(self.expr).parse_expr(None).unwrap();
            let indexes = HashSet::from_iter(self.indexes.iter().copied());

            assert_eq!(
                find_index_paths(self.pk, &indexes, &expr, &mut HashSet::new()),
                self.expected
            );
        }
    }

    #[test]
    fn test_simple_find_index() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "id < 5",
            expected: HashMap::from([(
                "id",
                VecDeque::from([(Bound::Unbounded, Bound::Excluded(&Value::Number(5)))]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_simple_intersection() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "id > 10 AND id < 20",
            expected: HashMap::from([(
                "id",
                VecDeque::from([(
                    Bound::Excluded(&Value::Number(10)),
                    Bound::Excluded(&Value::Number(20)),
                )]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_simple_merge() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "id < 10 OR id > 20",
            expected: HashMap::from([(
                "id",
                VecDeque::from([
                    (Bound::Unbounded, Bound::Excluded(&Value::Number(10))),
                    (Bound::Excluded(&Value::Number(20)), Bound::Unbounded),
                ]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_never_evaluates_true() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "id < 5 AND id > 10",
            expected: HashMap::new(),
        }
        .assert();
    }

    #[test]
    fn test_always_evalutates_true() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "id > 10 OR id < 15",
            expected: HashMap::new(),
        }
        .assert();
    }

    #[test]
    fn test_multiple_intersection() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "(id > 5 AND id < 30) AND (id > 10 AND id < 40)",
            expected: HashMap::from([(
                "id",
                VecDeque::from([(
                    Bound::Excluded(&Value::Number(10)),
                    Bound::Excluded(&Value::Number(30)),
                )]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_multiple_merge() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "(id < 7 OR id > 30) OR (id > 10 OR id < 5)",
            expected: HashMap::from([(
                "id",
                VecDeque::from([
                    (Bound::Unbounded, Bound::Excluded(&Value::Number(7))),
                    (Bound::Excluded(&Value::Number(10)), Bound::Unbounded),
                ]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_intersection_with_range() {
        IndexPath {
            pk: "id",
            indexes: &[],
            expr: "id < 30 AND (id < 10 or id > 15)",
            expected: HashMap::from([(
                "id",
                VecDeque::from([
                    (Bound::Unbounded, Bound::Excluded(&Value::Number(10))),
                    (
                        Bound::Excluded(&Value::Number(15)),
                        Bound::Excluded(&Value::Number(30)),
                    ),
                ]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_simple_index() {
        IndexPath {
            pk: "id",
            indexes: &["email"],
            expr: "email > 'johndoe@email.com'",
            expected: HashMap::from([(
                "email",
                VecDeque::from([(
                    Bound::Excluded(&Value::String("johndoe@email.com".into())),
                    Bound::Unbounded,
                )]),
            )]),
        }
        .assert();
    }

    #[test]
    fn test_multiple_indexes() {
        IndexPath {
            pk: "id",
            indexes: &["email", "uid"],
            expr: "id = 5 OR email = 'johndoe@email.com' OR uid <= 'something'",
            expected: HashMap::from([
                (
                    "id",
                    VecDeque::from([(
                        Bound::Included(&Value::Number(5)),
                        Bound::Included(&Value::Number(5)),
                    )]),
                ),
                (
                    "email",
                    VecDeque::from([(
                        Bound::Included(&Value::String("johndoe@email.com".into())),
                        Bound::Included(&Value::String("johndoe@email.com".into())),
                    )]),
                ),
                (
                    "uid",
                    VecDeque::from([(
                        Bound::Unbounded,
                        Bound::Included(&Value::String("something".into())),
                    )]),
                ),
            ]),
        }
        .assert();
    }
}
