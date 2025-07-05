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
        statement::{Column, Delete, Expression, Select, Type, Update},
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
        }) => {
            let mut source = optimiser::generate_seq_plan(&from, r#where, db)?;
            let page_size = db.pager.borrow().page_size;
            let work_dir = db.work_dir.clone();
            let table = db.metadata(&from)?;

            if !order_by.is_empty()
                && order_by != [Expression::Identifier(table.schema.columns[0].name.clone())]
            {
                let mut sorted_schema = table.schema.clone();
                let mut sorted_indexes = Vec::with_capacity(order_by.len());

                for expr in &order_by {
                    let index = match expr {
                        Expression::Identifier(col) => table.schema.index_of(col).unwrap(),
                        _ => {
                            let index = sorted_schema.len();
                            let r#type = resolve_type(&table.schema, expr)?;
                            let col = Column::new(&format!("{expr}"), r#type);
                            sorted_schema.push(col);

                            index
                        }
                    };

                    sorted_indexes.push(index)
                }

                let collect_source = match sorted_schema.len() > table.schema.len() {
                    true => Planner::SortKeys(SortKeys {
                        expressions: order_by
                            .into_iter()
                            .filter(|expr| !matches!(expr, Expression::Identifier(_)))
                            .collect(),
                        schema: table.schema.clone(),
                        source: Box::new(source),
                    }),
                    false => source,
                };

                source = Planner::Sort(Sort::from(SortBuilder {
                    page_size,
                    work_dir: work_dir.clone(),
                    input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                    collection: Collect::from(CollectBuilder {
                        source: Box::new(collect_source),
                        schema: sorted_schema.clone(),
                        work_dir,
                        mem_buff_size: page_size,
                    }),
                    comparator: TupleComparator::new(
                        table.schema.clone(),
                        sorted_schema,
                        sorted_indexes,
                    ),
                }));
            }

            let mut output = Schema::empty();

            for expr in &columns {
                match expr {
                    Expression::Identifier(ident) => output
                        .push(table.schema.columns[table.schema.index_of(ident).unwrap()].clone()),
                    _ => output.push(Column::new(
                        expr.to_string().as_str(),
                        resolve_type(&table.schema, expr)?,
                    )),
                }
            }

            if let Some(Expression::Function { func, args }) = &columns.first() {
                if func.is_aggr() {
                    let expr = args.first().cloned().unwrap_or(Expression::Wildcard);
                    return Ok(Planner::Aggregate(
                        AggregateBuilder {
                            expr,
                            function: *func,
                            source: Box::new(source),
                        }
                        .into(),
                    ));
                }
            }

            if table.schema == output {
                return Ok(source);
            }

            Planner::Project(Project {
                output,
                source: Box::new(source),
                projection: columns,
                input: table.schema.clone(),
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

fn needs_collection<File: FileOperations>(planner: &Planner<File>) -> bool {
    match planner {
        Planner::Filter(filter) => needs_collection(&filter.source),
        Planner::KeyScan(_) | Planner::ExactMatch(_) => false,
        Planner::SeqScan(_) | Planner::LogicalScan(_) | Planner::RangeScan(_) => true,
        _ => unreachable!("needs_collection() must be called only for a scan planner"),
    }
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
                    comparator: TupleComparator::new(keys.clone(), keys.clone(), vec![0]),
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
                        vec![1, 2]
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
                        vec![1, 4, 5]
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

        assert_eq!(
            db.gen_plan("SELECT COUNT(amount) FROM payments;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    expr: Expression::Identifier("amount".into()),
                    function: Function::Count,
                    source: Box::new(Planner::SeqScan(SeqScan {
                        pager: db.pager(),
                        table: db.tables["payments"].to_owned(),
                        cursor: Cursor::new(db.tables["payments"].root, 0)
                    }))
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggr_applying_filter() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, active BOOLEAN);"])?;

        assert_eq!(
            db.gen_plan("SELECT COUNT(*) FROM users WHERE active = TRUE;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    expr: Expression::Wildcard,
                    function: Function::Count,
                    source: Box::new(Planner::Filter(Filter {
                        filter: parse_expr("active = TRUE"),
                        schema: db.tables["users"].schema.to_owned(),
                        source: Box::new(Planner::SeqScan(SeqScan {
                            pager: db.pager(),
                            cursor: Cursor::new(db.tables["users"].root, 0),
                            table: db.tables["users"].to_owned(),
                        }))
                    })),
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

        assert_eq!(
            db.gen_plan("SELECT COUNT(price) FROM sales ORDER BY region;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    expr: Expression::Identifier("price".into()),
                    function: Function::Count,
                    source: Box::new(Planner::Sort(
                        SortBuilder {
                            page_size,
                            work_dir: work_dir.clone(),
                            comparator: TupleComparator::new(
                                schema.clone(),
                                schema.clone(),
                                vec![1]
                            ),
                            input_buffers: DEFAULT_SORT_BUFFER_SIZE,
                            collection: CollectBuilder {
                                work_dir,
                                mem_buff_size: page_size,
                                schema: schema.clone(),
                                source: Box::new(Planner::SeqScan(SeqScan {
                                    pager: db.pager(),
                                    table: db.tables["sales"].clone(),
                                    cursor: Cursor::new(db.tables["sales"].root, 0)
                                }))
                            }
                            .into()
                        }
                        .into()
                    ))
                }
                .into()
            )
        );

        Ok(())
    }

    #[test]
    fn test_aggregate_over_exact_match() -> PlannerResult {
        let mut db = new_db(&["CREATE TABLE products (id INT PRIMARY KEY, stock INT UNSIGNED);"])?;

        assert_eq!(
            db.gen_plan("SELECT COUNT(*) FROM products WHERE id = 24;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    expr: Expression::Wildcard,
                    function: Function::Count,
                    source: Box::new(Planner::ExactMatch(ExactMatch {
                        done: false,
                        emit_only_key: false,
                        pager: db.pager(),
                        expr: parse_expr("id = 24"),
                        key: serialize(&Type::UnsignedInteger, &Value::Number(24)),
                        relation: Relation::Table(db.tables["products"].clone())
                    }))
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

        assert_eq!(
            db.gen_plan("SELECT SUM(value) FROM sensors WHERE id BETWEEN 1000 AND 2000;")?,
            Planner::Aggregate(
                AggregateBuilder {
                    expr: parse_expr("value"),
                    function: Function::Sum,
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
                    ))
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
            ),
            collection: collect,
        });

        // and finally scan the main table using the keys from the index
        // and calculate the minimum over the results
        assert_eq!(
            db.gen_plan("SELECT MIN(age) FROM users WHERE email <= 'johndoe@email.com';")?,
            Planner::Aggregate(
                AggregateBuilder {
                    expr: parse_expr("age"),
                    function: Function::Min,
                    source: Box::new(Planner::KeyScan(KeyScan {
                        pager: db.pager(),
                        table: db.tables["users"].to_owned(),
                        comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)),
                        source: Box::new(Planner::Sort(sort))
                    }))
                }
                .into()
            )
        );

        Ok(())
    }
}
