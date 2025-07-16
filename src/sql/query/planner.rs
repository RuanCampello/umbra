use std::{
    collections::VecDeque,
    io::{Read, Seek, Write},
    rc::Rc,
};

use crate::{
    core::storage::pagination::io::FileOperations,
    db::{Ctx, Database, DatabaseError, Schema, SqlError},
    sql::{
        analyzer,
        query::optimiser,
        statement::{Column, Delete, Expression, OrderBy, OrderDirection, Select, Type, Update},
        Statement,
    },
    vm::{
        expression::VmType,
        planner::{AggregateBuilder, Insert as InsertPlan, Planner, Sort, SortKeys, Values},
    },
};
use crate::{
    sql::statement::Insert,
    vm::planner::{
        Collect, CollectBuilder, Delete as DeletePlan, Project, SortBuilder, TupleComparator,
        Update as UpdatePlan, DEFAULT_SORT_BUFFER_SIZE,
    },
};

pub(crate) fn generate_plan<File: Seek + Read + Write + FileOperations>(
    statement: Statement,
    db: &mut Database<File>,
) -> Result<Planner<File>, DatabaseError> {
    Ok(match statement {
        Statement::Insert(Insert { into, values, .. }) => {
            let values = VecDeque::from(values);
            let source = Box::new(Planner::Values(Values { values }));
            let table = db.metadata(&into)?;

            Planner::Insert(InsertPlan {
                source,
                comparator: table.comp()?,
                table: db.metadata(&into)?.clone(),
                pager: Rc::clone(&db.pager),
            })
        }
        Statement::Select(Select {
            columns,
            from,
            r#where,
            order_by,
            group_by,
        }) => {
            let mut source = optimiser::generate_seq_plan(&from, r#where.clone(), db)?;
            let page_size = db.pager.borrow().page_size;
            let work_dir = db.work_dir.clone();
            let table = db.metadata(&from)?;
            let schema = &table.schema;

            // this is a special case for `type_of` function
            if let Some((col_name, type_of)) = single_typeof_column(&columns, &schema) {
                use crate::sql::statement::Value;

                return Ok(Planner::Project(Project {
                    output: Schema::new(vec![Column::new(&col_name, Type::Varchar(50))]),
                    input: Schema::empty(),
                    source: Box::new(Planner::Values(Values {
                        values: vec![vec![Expression::Value(Value::String(type_of.clone()))]]
                            .into(),
                    })),
                    projection: vec![Expression::Value(Value::String(type_of))],
                }));
            }

            let mut output = Schema::empty();
            for expr in &columns {
                match expr {
                    Expression::Identifier(ident) => {
                        output.push(schema.columns[schema.index_of(ident).unwrap()].clone())
                    }
                    _ => output.push(Column::new(&expr.to_string(), resolve_type(schema, expr)?)),
                }
            }

            let is_grouped = !group_by.is_empty();
            let aggr_exprs: Vec<_> = columns
                .iter()
                .filter(|e| matches!(e, Expression::Function { func, .. } if func.is_aggr()))
                .cloned()
                .collect();

            if is_grouped || !aggr_exprs.is_empty() {
                let mut aggr_schema = Schema::empty();

                for expr in &group_by {
                    match expr {
                        Expression::Identifier(ident) => aggr_schema
                            .push(schema.columns[schema.index_of(ident).unwrap()].clone()),
                        _ => aggr_schema
                            .push(Column::new(&expr.to_string(), resolve_type(schema, expr)?)),
                    }
                }

                for expr in &aggr_exprs {
                    aggr_schema.push(Column::new(&expr.to_string(), resolve_type(schema, expr)?));
                }

                if group_by.is_empty() && !order_by.is_empty() {
                    let (indexes, directions) =
                        extract_order_indexes_and_directions(schema, &order_by)?;

                    source = Planner::Sort(Sort::from(SortBuilder {
                        page_size,
                        work_dir: work_dir.clone(),
                        input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                        collection: Collect::from(CollectBuilder {
                            source: Box::new(source),
                            schema: schema.clone(),
                            work_dir: work_dir.clone(),
                            mem_buff_size: page_size,
                        }),
                        comparator: TupleComparator::new(
                            schema.clone(),
                            schema.clone(),
                            indexes,
                            directions,
                        ),
                    }));
                }

                let mut plan = Planner::Aggregate(
                    AggregateBuilder {
                        source: Box::new(source),
                        aggr_exprs,
                        page_size,
                        group_by: group_by.clone(),
                        output: aggr_schema.clone(),
                    }
                    .into(),
                );

                if is_grouped && !order_by.is_empty() {
                    let (indexes, directions) =
                        extract_order_indexes_and_directions(&aggr_schema, &order_by)?;

                    plan = Planner::Sort(Sort::from(SortBuilder {
                        page_size,
                        work_dir: work_dir.clone(),
                        input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                        collection: Collect::from(CollectBuilder {
                            source: Box::new(plan),
                            schema: aggr_schema.clone(),
                            work_dir: work_dir.clone(),
                            mem_buff_size: page_size,
                        }),
                        comparator: TupleComparator::new(
                            aggr_schema.clone(),
                            aggr_schema.clone(),
                            indexes,
                            directions,
                        ),
                    }));
                }

                if output.ne(&aggr_schema) {
                    let projection: Vec<Expression> = columns
                        .iter()
                        .map(|expr| match expr {
                            Expression::Function { func, .. } => {
                                Expression::Identifier(func.to_string())
                            }
                            other => other.clone(),
                        })
                        .collect();

                    plan = Planner::Project(Project {
                        output,
                        projection,
                        input: aggr_schema,
                        source: Box::new(plan),
                    });
                }

                return Ok(plan);
            }

            if !order_by.is_empty()
                && order_by != [Expression::Identifier(schema.columns[0].name.clone()).into()]
            {
                let mut sorted_schema = schema.clone();
                let mut indexes = Vec::new();
                let mut extra_exprs = Vec::new();
                let mut directions = Vec::new();

                for order in &order_by {
                    match order.expr {
                        Expression::Identifier(ref ident) => {
                            indexes.push(schema.index_of(ident).unwrap());
                            directions.push(order.direction);
                        }
                        _ => {
                            let ty = resolve_type(schema, &order.expr)?;
                            indexes.push(sorted_schema.len());
                            directions.push(order.direction);
                            sorted_schema.push(Column::new(&order.expr.to_string(), ty));
                            extra_exprs.push(order.expr.clone());
                        }
                    }
                }

                if !extra_exprs.is_empty() {
                    source = Planner::SortKeys(SortKeys {
                        expressions: extra_exprs,
                        schema: schema.clone(),
                        source: Box::new(source),
                    });
                }

                source = Planner::Sort(Sort::from(SortBuilder {
                    page_size,
                    work_dir: work_dir.clone(),
                    input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                    collection: Collect::from(CollectBuilder {
                        source: Box::new(source),
                        schema: sorted_schema.clone(),
                        work_dir,
                        mem_buff_size: page_size,
                    }),
                    comparator: TupleComparator::new(
                        schema.clone(),
                        sorted_schema,
                        indexes,
                        directions,
                    ),
                }));
            }

            if schema.eq(&output) {
                return Ok(source);
            }

            Planner::Project(Project {
                output,
                source: Box::new(source),
                projection: columns,
                input: schema.clone(),
            })
        }
        Statement::Update(Update {
            table,
            columns,
            r#where,
        }) => {
            let mut source = optimiser::generate_seq_plan(&table, r#where, db)?;
            let work_dir = db.work_dir.clone();
            let page_size = db.pager.borrow().page_size;
            let metadata = db.metadata(&table)?;

            if needs_collection(&source) {
                source = Planner::Collect(Collect::from(CollectBuilder {
                    source: Box::new(source),
                    schema: metadata.schema.clone(),
                    work_dir,
                    mem_buff_size: page_size,
                }));
            }

            Planner::Update(UpdatePlan {
                comparator: metadata.comp()?,
                table: metadata.clone(),
                assigments: columns,
                pager: Rc::clone(&db.pager),
                source: Box::new(source),
            })
        }

        Statement::Delete(Delete { from, r#where }) => {
            let mut source = optimiser::generate_seq_plan(&from, r#where, db)?;

            let work_dir = db.work_dir.clone();
            let page_size = db.pager.borrow().page_size;
            let metadata = db.metadata(&from)?;

            if needs_collection(&source) {
                source = Planner::Collect(
                    CollectBuilder {
                        work_dir,
                        mem_buff_size: page_size,
                        source: Box::new(source),
                        schema: metadata.schema.clone(),
                    }
                    .into(),
                );
            }

            Planner::Delete(DeletePlan {
                table: metadata.clone(),
                comparator: metadata.comp()?,
                pager: Rc::clone(&db.pager),
                source: Box::new(source),
            })
        }

        other => {
            return Err(DatabaseError::Other(format!(
                "Statement {other} not implemented or supported for this"
            )))
        }
    })
}

fn resolve_type(schema: &Schema, expr: &Expression) -> Result<Type, SqlError> {
    Ok(match expr {
        Expression::Identifier(col) => {
            let index = schema.index_of(col).unwrap();
            schema.columns[index].data_type.clone()
        }
        _ => match analyzer::analyze_expression(schema, None, expr)? {
            VmType::Float => Type::DoublePrecision,
            VmType::Bool => Type::Boolean,
            VmType::Number => Type::BigInteger,
            VmType::String => Type::Varchar(65535),
            VmType::Date => Type::DateTime,
        },
    })
}

fn extract_order_indexes_and_directions(
    schema: &Schema,
    order_by: &[OrderBy],
) -> Result<(Vec<usize>, Vec<OrderDirection>), DatabaseError> {
    order_by
        .iter()
        .map(|order| {
            let idx = match &order.expr {
                Expression::Identifier(ident) => schema
                    .index_of(ident)
                    .ok_or(SqlError::InvalidGroupBy(ident.into())),
                _ => schema
                    .index_of(&order.expr.to_string())
                    .ok_or(SqlError::Other(format!(
                        "ORDER BY expression `{}` not found in output columns",
                        order.expr.to_string()
                    ))),
            }?;
            Ok((idx, order.direction))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|p| p.into_iter().unzip())
}

fn needs_collection<File: FileOperations>(planner: &Planner<File>) -> bool {
    match planner {
        Planner::Filter(filter) => needs_collection(&filter.source),
        Planner::KeyScan(_) | Planner::ExactMatch(_) => false,
        Planner::SeqScan(_) | Planner::LogicalScan(_) | Planner::RangeScan(_) => true,
        _ => unreachable!("needs_collection() must be called only for a scan planner"),
    }
}

fn single_typeof_column<'a>(
    columns: &'a [Expression],
    schema: &'a Schema,
) -> Option<(String, String)> {
    use crate::sql::statement::Function;

    if let [Expression::Function {
        func: Function::TypeOf,
        args,
    }] = columns
    {
        if let [Expression::Identifier(ref ident)] = args.as_slice() {
            let idx = schema.index_of(ident)?;
            let type_of = schema.columns[idx].data_type.to_string();
            return Some((format!("typeof({ident})"), type_of));
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use crate::sql::statement::Function;
    use std::{cell::RefCell, collections::HashMap, ops::Bound, path::PathBuf};

    use super::*;
    use crate::{
        core::storage::{
            btree::{Cursor, FixedSizeCmp},
            pagination::pager::Pager,
            tuple::{byte_len_of_type, serialize},
            MemoryBuffer,
        },
        db::{Ctx as DbCtx, IndexMetadata, Relation, TableMetadata},
        index,
        sql::{
            self,
            parser::Parser,
            statement::{Create, Value},
        },
        vm::planner::{
            CollectBuilder, ExactMatch, Filter, KeyScan, RangeScan, RangeScanBuilder, SeqScan,
            SortBuilder,
        },
    };

    struct Ctx {
        db: Database<MemoryBuffer>,
        tables: HashMap<String, TableMetadata>,
        indexes: HashMap<String, IndexMetadata>,
    }

    impl Ctx {
        fn gen_plan(&mut self, query: &str) -> Result<Planner<MemoryBuffer>, DatabaseError> {
            let statement = sql::pipeline(query, &mut self.db)?;
            let plan = generate_plan(statement, &mut self.db)?;
            Ok(plan)
        }

        fn pager(&self) -> Rc<RefCell<Pager<MemoryBuffer>>> {
            self.db.pager.clone()
        }
    }

    fn new_db(ctx: &[&str]) -> Result<Ctx, DatabaseError> {
        let mut pager = Pager::default().block_size(4096);
        pager.init()?;

        let mut db = Database::new(Rc::new(RefCell::new(pager)), PathBuf::new());
        let mut tables = HashMap::new();
        let mut indexes = HashMap::new();
        let mut fetch_tables = Vec::new();

        for sql in ctx {
            if let Statement::Create(Create::Table { name, .. }) =
                Parser::new(sql).parse_statement()?
            {
                fetch_tables.push(name);
            }

            db.exec(sql)?;
        }

        for table_name in fetch_tables {
            let table = db.metadata(&table_name)?;
            for idx in &table.indexes {
                indexes.insert(idx.name.to_owned(), idx.to_owned());
            }

            tables.insert(table_name, table.to_owned());
        }

        Ok(Ctx {
            db,
            indexes,
            tables,
        })
    }

    type PlannerResult = Result<(), DatabaseError>;

    fn parse_expr(expr: &str) -> Expression {
        let mut expr = Parser::new(expr).parse_expr(None).unwrap();
        sql::optimiser::simplify(&mut expr).unwrap();

        expr
    }

    #[test]
    fn test_simple_sequential_plan() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users;")?,
            Planner::SeqScan(SeqScan {
                pager: db.pager(),
                cursor: Cursor::new(db.tables["users"].root, 0),
                table: db.tables["users"].clone()
            })
        );

        Ok(())
    }

    #[test]
    fn test_simple_sequential_with_filter() -> PlannerResult {
        let mut db =
            new_db(&["CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255), age INT);"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users WHERE age >= 20;")?,
            Planner::Filter(Filter {
                filter: parse_expr("age >= 20"),
                schema: db.tables["users"].schema.to_owned(),
                source: Box::new(Planner::SeqScan(SeqScan {
                    pager: db.pager(),
                    table: db.tables["users"].to_owned(),
                    cursor: Cursor::new(db.tables["users"].root, 0),
                }))
            })
        );

        Ok(())
    }

    #[test]
    fn test_sequential_scan_with_projection() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT, name VARCHAR(255));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users;")?,
            Planner::Project(Project {
                input: db.tables["users"].schema.to_owned(),
                output: Schema::new(vec![
                    Column::new("id", Type::Integer),
                    Column::new("name", Type::Varchar(255)),
                ]),
                projection: vec![
                    Expression::Identifier("id".into()),
                    Expression::Identifier("name".into()),
                ],
                source: Box::new(Planner::SeqScan(SeqScan {
                    pager: db.pager(),
                    table: db.tables["users"].to_owned(),
                    cursor: Cursor::new(db.tables["users"].root, 0)
                }))
            })
        );

        Ok(())
    }

    #[test]
    fn test_sequential_scan_with_projection_when_narrowing() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(35), email VARCHAR(135));",
        ])?;

        assert_eq!(
            db.gen_plan("SELECT email, id FROM employees;")?,
            Planner::Project(Project {
                source: Box::new(Planner::SeqScan(SeqScan {
                    cursor: Cursor::new(db.tables["employees"].root, 0),
                    table: db.tables["employees"].to_owned(),
                    pager: db.pager()
                })),
                input: db.tables["employees"].schema.to_owned(),
                output: Schema::new(vec![
                    Column::new("email", Type::Varchar(135)),
                    Column::primary_key("id", Type::Integer),
                ]),
                projection: vec![
                    Expression::Identifier("email".into()),
                    Expression::Identifier("id".into())
                ]
            })
        );

        Ok(())
    }

    #[test]
    fn test_exact_match_with_id() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE products (id INT PRIMARY KEY, name VARCHAR(100), stock_quant INT);",
        ])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM products WHERE id = 69;")?,
            Planner::ExactMatch(ExactMatch {
                done: false,
                emit_only_key: false,
                pager: db.pager(),
                expr: parse_expr("id = 69"),
                key: serialize(&Type::Integer, &Value::Number(69)),
                relation: Relation::Table(db.tables["products"].to_owned())
            })
        );

        Ok(())
    }

    #[test]
    fn test_exact_match_with_external_id() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(35), email VARCHAR(135) UNIQUE);",
        ])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM employees WHERE email = 'johndoe@email.com';")?,
            Planner::KeyScan(KeyScan {
                pager: db.pager(),
                table: db.tables["employees"].to_owned(),
                comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)),
                source: Box::new(Planner::ExactMatch(ExactMatch {
                    done: false,
                    emit_only_key: true,
                    pager: db.pager(),
                    expr: parse_expr("email = 'johndoe@email.com'"),
                    key: serialize(
                        &Type::Varchar(135),
                        &Value::String("johndoe@email.com".into())
                    ),
                    relation: Relation::Index(
                        db.indexes[&index!(unique on employees (email))].to_owned()
                    )
                }))
            })
        );

        Ok(())
    }

    #[test]
    fn test_generate_range_idx() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(120), salary INT);",
        ])?;

        let expected_range = (
            Bound::Excluded(serialize(&Type::Integer, &Value::Number(10))),
            Bound::Excluded(serialize(&Type::Integer, &Value::Number(20))),
        );

        assert_eq!(
            db.gen_plan("SELECT * FROM employees WHERE id > 10 AND id < 20;")?,
            Planner::RangeScan(RangeScan::new(
                expected_range,
                Relation::Table(db.tables["employees"].to_owned()),
                false,
                parse_expr("id > 10 AND id < 20"),
                db.pager(),
            ))
        );

        Ok(())
    }

    #[test]
    fn test_generate_range_external_idx() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(120), email VARCHAR(135) UNIQUE);",
        ])?;

        let keys = db.tables["employees"].key_only_schema();
        let page_size = db.db.pager.borrow().page_size;
        let work_dir = db.db.work_dir.clone();

        let range = (
            Bound::Unbounded,
            Bound::Included(serialize(
                &Type::Varchar(135),
                &Value::String("johndoe@email.com".into()),
            )),
        );
        let range_scan = Planner::RangeScan(RangeScan::new(
            range,
            Relation::Index(db.indexes[&index!(unique on employees (email))].clone()),
            true,
            parse_expr("email <= 'johndoe@email.com'"),
            db.pager(),
        ));

        assert_eq!(
            db.gen_plan("SELECT * FROM employees WHERE email <= 'johndoe@email.com';")?,
            Planner::KeyScan(KeyScan {
                comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)),
                pager: db.pager(),
                table: db.tables["employees"].to_owned(),
                source: Box::new(Planner::Sort(Sort::from(SortBuilder {
                    page_size,
                    work_dir: work_dir.clone(),
                    input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                    comparator: TupleComparator::new(
                        keys.clone(),
                        keys.clone(),
                        vec![0],
                        vec![Default::default()]
                    ),
                    collection: Collect::from(CollectBuilder {
                        mem_buff_size: page_size,
                        schema: keys,
                        work_dir,
                        source: Box::new(range_scan)
                    })
                }))),
            })
        );
        Ok(())
    }

    #[test]
    fn test_generate_simple_range_scan() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM employees WHERE id < 5;")?,
            Planner::RangeScan(RangeScan::from(RangeScanBuilder {
                emit_only_key: false,
                pager: db.pager(),
                expr: parse_expr("id < 5"),
                relation: Relation::Table(db.tables["employees"].to_owned()),
                range: (
                    Bound::Unbounded,
                    Bound::Excluded(serialize(&Type::Integer, &Value::Number(5)))
                )
            }))
        );

        Ok(())
    }

    #[test]
    fn test_generate_range_scan_applying_filter() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(135));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users WHERE id < 5 AND name = 'john';")?,
            Planner::Filter(Filter {
                filter: parse_expr("name = 'john'"),
                schema: db.tables["users"].schema.to_owned(),
                source: Box::new(Planner::RangeScan(
                    RangeScanBuilder {
                        emit_only_key: false,
                        pager: db.pager(),
                        expr: parse_expr("id < 5"),
                        relation: Relation::Table(db.tables["users"].to_owned()),
                        range: (
                            Bound::Unbounded,
                            Bound::Excluded(serialize(&Type::Integer, &Value::Number(5)))
                        )
                    }
                    .into()
                ))
            })
        );

        Ok(())
    }

    #[test]
    fn test_sequential_scan_if_unbounded_ranges() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(135));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users WHERE (id > 10 OR id < 15) OR id > 24;")?,
            Planner::Filter(Filter {
                filter: parse_expr("(id > 10 OR id < 15) OR id > 24"),
                schema: db.tables["users"].schema.clone(),
                source: Box::new(Planner::SeqScan(SeqScan {
                    table: db.tables["users"].to_owned(),
                    pager: db.pager(),
                    cursor: Cursor::new(db.tables["users"].root, 0)
                }))
            })
        );

        Ok(())
    }

    #[test]
    fn test_sequencial_scan_if_intersection_cancels_ranges() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(135));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users WHERE (id < 5 OR id > 10) AND id = 8;")?,
            Planner::Filter(Filter {
                schema: db.tables["users"].schema.clone(),
                filter: parse_expr("(id < 5 OR id > 10) AND id = 8"),
                source: Box::new(Planner::SeqScan(SeqScan {
                    pager: db.pager(),
                    table: db.tables["users"].to_owned(),
                    cursor: Cursor::new(db.tables["users"].root, 0),
                }))
            })
        );

        Ok(())
    }

    #[test]
    fn test_generate_simple_sort() -> PlannerResult {
        let mut db =
            new_db(&["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(135), age INT);"])?;

        let page_size = db.db.pager.borrow().page_size;
        let work_dir = db.db.work_dir.clone();
        assert_eq!(
            db.gen_plan("SELECT * FROM users ORDER BY name, age;")?,
            Planner::Sort(
                SortBuilder {
                    page_size,
                    work_dir: work_dir.clone(),
                    input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                    comparator: TupleComparator::new(
                        db.tables["users"].schema.to_owned(),
                        db.tables["users"].schema.to_owned(),
                        vec![1, 2],
                        vec![Default::default(), Default::default()]
                    ),
                    collection: CollectBuilder {
                        mem_buff_size: page_size,
                        work_dir,
                        schema: db.tables["users"].schema.to_owned(),
                        source: Box::new(Planner::SeqScan(SeqScan {
                            pager: db.pager(),
                            table: db.tables["users"].to_owned(),
                            cursor: Cursor::new(db.tables["users"].root, 0)
                        }))
                    }
                    .into()
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_generate_sort_with_expressions() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(135), age INT, followers INT);",
        ])?;

        let mut sort_schema = db.tables["users"].schema.clone();
        sort_schema.push(Column::new("age + 20", Type::BigInteger));
        sort_schema.push(Column::new("followers * 5", Type::BigInteger));

        let page_size = db.db.pager.borrow().page_size;
        let work_dir = db.db.work_dir.clone();
        assert_eq!(
            db.gen_plan("SELECT * FROM users ORDER BY name, age + 20, followers * 5;")?,
            Planner::Sort(
                SortBuilder {
                    page_size,
                    work_dir: work_dir.clone(),
                    input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                    comparator: TupleComparator::new(
                        db.tables["users"].schema.clone(),
                        sort_schema.clone(),
                        vec![1, 4, 5],
                        vec![Default::default(), Default::default(), Default::default()]
                    ),
                    collection: CollectBuilder {
                        mem_buff_size: page_size,
                        work_dir,
                        schema: sort_schema,
                        source: Box::new(Planner::SortKeys(SortKeys {
                            expressions: vec![parse_expr("age + 20"), parse_expr("followers * 5")],
                            schema: db.tables["users"].schema.clone(),
                            source: Box::new(Planner::SeqScan(SeqScan {
                                pager: db.pager(),
                                table: db.tables["users"].to_owned(),
                                cursor: Cursor::new(db.tables["users"].root, 0)
                            }))
                        }))
                    }
                    .into()
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_ignore_sorting_if_order_by() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users ORDER BY id;")?,
            Planner::SeqScan(SeqScan {
                pager: db.pager(),
                table: db.tables["users"].to_owned(),
                cursor: Cursor::new(db.tables["users"].root, 0),
            })
        );

        Ok(())
    }

    #[test]
    fn test_simple_aggr_plan() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE payments (id SERIAL PRIMARY KEY, amount REAL);"])?;
        let page_size = db.db.pager.borrow().page_size;

        assert_eq!(
            db.gen_plan("SELECT COUNT(amount) FROM payments;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::SeqScan(SeqScan {
                        pager: db.pager(),
                        table: db.tables["payments"].to_owned(),
                        cursor: Cursor::new(db.tables["payments"].root, 0)
                    })),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Count,
                        args: vec![Expression::Identifier("amount".into())]
                    }],
                    output: Schema::new(vec![Column::new("COUNT", Type::BigInteger)]),
                    page_size,
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggr_applying_filter() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, active BOOLEAN);"])?;
        let page_size = db.db.pager.borrow().page_size;

        assert_eq!(
            db.gen_plan("SELECT COUNT(*) FROM users WHERE active = TRUE;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::Filter(Filter {
                        filter: parse_expr("active = TRUE"),
                        schema: db.tables["users"].schema.to_owned(),
                        source: Box::new(Planner::SeqScan(SeqScan {
                            pager: db.pager(),
                            cursor: Cursor::new(db.tables["users"].root, 0),
                            table: db.tables["users"].to_owned(),
                        }))
                    })),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Count,
                        args: vec![Expression::Wildcard]
                    }],
                    output: Schema::new(vec![Column::new("COUNT", Type::BigInteger)]),
                    page_size,
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggr_when_sorting() -> PlannerResult {
        let mut db =
            new_db(&["CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(30), price REAL);"])?;
        let page_size = db.db.pager.borrow().page_size;
        let work_dir = db.db.work_dir.to_owned();
        let schema = db.tables["sales"].schema.to_owned();

        let sort = SortBuilder {
            page_size,
            work_dir: work_dir.clone(),
            comparator: TupleComparator::new(
                schema.clone(),
                schema.clone(),
                vec![1],
                vec![Default::default()],
            ),
            input_buffers: DEFAULT_SORT_BUFFER_SIZE,
            collection: CollectBuilder {
                work_dir,
                mem_buff_size: page_size,
                schema: schema.clone(),
                source: Box::new(Planner::SeqScan(SeqScan {
                    pager: db.pager(),
                    table: db.tables["sales"].clone(),
                    cursor: Cursor::new(db.tables["sales"].root, 0),
                })),
            }
            .into(),
        };

        assert_eq!(
            db.gen_plan("SELECT COUNT(price) FROM sales ORDER BY region;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::Sort(sort.into())),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Count,
                        args: vec![Expression::Identifier("price".into())]
                    }],
                    output: Schema::new(vec![Column::new("COUNT", Type::BigInteger)]),
                    page_size,
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_over_exact_match() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE products (id INT PRIMARY KEY, stock INT UNSIGNED);"])?;
        let page_size = db.db.pager.borrow().page_size;

        assert_eq!(
            db.gen_plan("SELECT COUNT(*) FROM products WHERE id = 24;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::ExactMatch(ExactMatch {
                        done: false,
                        emit_only_key: false,
                        pager: db.pager(),
                        expr: parse_expr("id = 24"),
                        key: serialize(&Type::UnsignedInteger, &Value::Number(24)),
                        relation: Relation::Table(db.tables["products"].clone())
                    })),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Count,
                        args: vec![Expression::Wildcard]
                    }],
                    output: Schema::new(vec![Column::new("COUNT", Type::BigInteger)]),
                    page_size,
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggr_with_range_scan() -> PlannerResult {
        let mut db =
            new_db(&["CREATE TABLE sensors (id INT PRIMARY KEY, value REAL, stamp INT);"])?;
        let page_size = db.db.pager.borrow().page_size;

        assert_eq!(
            db.gen_plan("SELECT SUM(value) FROM sensors WHERE id BETWEEN 1000 AND 2000;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::RangeScan(
                        RangeScanBuilder {
                            emit_only_key: false,
                            pager: db.pager(),
                            expr: parse_expr("id BETWEEN 1000 AND 2000"),
                            relation: Relation::Table(db.tables["sensors"].to_owned()),
                            range: (
                                Bound::Included(serialize(&Type::Integer, &Value::Number(1000))),
                                Bound::Included(serialize(&Type::Integer, &Value::Number(2000)))
                            )
                        }
                        .into()
                    )),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Sum,
                        args: vec![parse_expr("value")]
                    }],
                    output: Schema::new(vec![Column::new("SUM", Type::DoublePrecision)]),
                    page_size,
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggr_with_key_scan() -> PlannerResult {
        let mut db =
            new_db(&["CREATE TABLE users (id SERIAL PRIMARY KEY, age INT UNSIGNED, email VARCHAR(30) UNIQUE);"])?;
        let page_size = db.db.pager.borrow().page_size;

        // we need first to index over the qualified keys (email <= ...)
        let range_scan = Planner::RangeScan(RangeScan::from(RangeScanBuilder {
            emit_only_key: true,
            pager: db.pager(),
            expr: parse_expr("email <= 'johndoe@email.com'"),
            relation: Relation::Index(db.indexes[&index!(unique on users (email))].to_owned()),
            range: (
                Bound::Unbounded,
                Bound::Included(serialize(
                    &Type::Varchar(30),
                    &Value::String("johndoe@email.com".into()),
                )),
            ),
        }));

        // then we collect the results
        let collect = Collect::from(CollectBuilder {
            source: Box::new(range_scan),
            schema: Schema::new(vec![Column::primary_key("id", Type::Serial)]),
            work_dir: db.db.work_dir.clone(),
            mem_buff_size: db.db.pager.borrow().page_size,
        });

        // and sort to garantee PK order
        let sort = Sort::from(SortBuilder {
            page_size: db.db.pager.borrow().page_size,
            work_dir: db.db.work_dir.clone(),
            input_buffers: DEFAULT_SORT_BUFFER_SIZE,
            comparator: TupleComparator::new(
                Schema::new(vec![Column::primary_key("id", Type::Serial)]),
                Schema::new(vec![Column::primary_key("id", Type::Serial)]),
                vec![0],
                vec![Default::default()],
            ),
            collection: collect,
        });

        // and finally scan the main table using the keys from the index
        // and calculate the minimum over the results
        assert_eq!(
            db.gen_plan("SELECT MIN(age) FROM users WHERE email <= 'johndoe@email.com';")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::KeyScan(KeyScan {
                        pager: db.pager(),
                        table: db.tables["users"].to_owned(),
                        comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)),
                        source: Box::new(Planner::Sort(sort))
                    })),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Min,
                        args: vec![parse_expr("age")]
                    }],
                    output: Schema::new(vec![Column::new("MIN", Type::DoublePrecision)]),
                    page_size,
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_group_by_with_sum() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE sales (region VARCHAR(2), price INT);",
            "INSERT INTO sales (region, price) VALUES ('N', 10);",
            "INSERT INTO sales (region, price) VALUES ('S', 20);",
            "INSERT INTO sales (region, price) VALUES ('N', 15);",
            "INSERT INTO sales (region, price) VALUES ('S', 30);",
        ])?;

        assert_eq!(
            db.gen_plan("SELECT region, SUM(price) FROM sales GROUP BY region;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    source: Box::new(Planner::SeqScan(SeqScan {
                        pager: db.pager(),
                        table: db.tables["sales"].to_owned(),
                        cursor: Cursor::new(db.tables["sales"].root, 0)
                    })),
                    group_by: vec![Expression::Identifier("region".into())],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Sum,
                        args: vec![Expression::Identifier("price".into())]
                    }],
                    output: Schema::new(vec![
                        Column::new("region", Type::Varchar(2)),
                        Column::new("SUM", Type::DoublePrecision)
                    ]),
                    page_size: db.db.pager.borrow().page_size,
                }
                .into()
            )
        );
        Ok(())
    }
}
