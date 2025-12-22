use crate::{
    core::storage::pagination::io::FileOperations,
    db::{Ctx, Database, DatabaseError, Schema, SqlError},
    sql::{
        analyzer::{self, contains_aggregate},
        query::{optimiser, planner::select::SelectBuilder},
        statement::{Column, Delete, Expression, Insert, Select, Type, Update, NUMERIC_ANY},
        Statement,
    },
    vm::{
        expression::VmType,
        planner::{
            Collect, CollectBuilder, Delete as DeletePlan, Insert as InsertPlan, Planner, Project,
            Update as UpdatePlan, Values,
        },
    },
};
use std::{
    collections::VecDeque,
    io::{Read, Seek, Write},
    rc::Rc,
};
mod select;

pub(crate) fn generate_plan<File: Seek + Read + Write + FileOperations>(
    statement: Statement,
    db: &mut Database<File>,
) -> Result<Planner<File>, DatabaseError> {
    Ok(match statement {
        Statement::Insert(Insert {
            into,
            values,
            returning,
            ..
        }) => {
            let values = VecDeque::from(values);
            let source = Box::new(Planner::Values(Values { values }));
            let table = db.metadata(&into)?;

            let returning_schema = returning_schema(&returning, &table.schema)?;

            Planner::Insert(InsertPlan {
                source,
                returning,
                returning_schema,
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
            joins,
            limit,
            offset,
        }) => {
            let (r#where, join_where) = match !joins.is_empty() && r#where.is_some() {
                true => split_where(r#where.as_ref().unwrap(), &from.key()),
                _ => (r#where.as_ref(), None),
            };

            // this is a special case for `type_of` function
            let table_schema = db.metadata(&from.name)?.schema.clone();
            if let Some((col_name, type_of)) = single_typeof_column(&columns, &table_schema) {
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

            let source = optimiser::generate_seq_plan(&from.name, r#where.cloned(), db)?;
            let mut builder = SelectBuilder::new(
                source,
                table_schema,
                &columns,
                db.pager.borrow().page_size,
                db.work_dir.clone(),
                from.key(),
            );

            builder.apply_joins(&from, &joins, join_where, db)?;
            if let Some(filter) = join_where {
                builder.apply_filter(filter.to_owned());
            }

            let output = builder.build_output_schema()?;
            let has_aggregate =
                !group_by.is_empty() || columns.iter().any(|expr| contains_aggregate(expr));

            match has_aggregate {
                true => builder.apply_aggregation(group_by, &order_by, output)?,
                _ => {
                    if !order_by.is_empty() {
                        builder.apply_sorting(&order_by)?;
                    }
                    builder.apply_projection(output);
                }
            }

            builder.apply_limit(limit, offset);
            builder.build()
        }
        Statement::Update(Update {
            table,
            columns,
            r#where,
            returning,
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

            let returning_schema = match returning.is_empty() {
                true => None,
                _ => {
                    let input = metadata.schema.update_returning_input();
                    returning_schema(&returning, &input)?
                }
            };

            Planner::Update(UpdatePlan {
                returning,
                returning_schema,
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
            let index = schema
                .index_of(col)
                .ok_or(SqlError::InvalidColumn(col.into()))?;
            schema.columns[index].data_type.clone()
        }
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

fn split_where<'expr>(
    r#where: &'expr Expression,
    table: &str,
) -> (Option<&'expr Expression>, Option<&'expr Expression>) {
    #[rustfmt::skip]
    fn references(expr: &Expression, table: &str) -> bool {
        match  expr {
            Expression::Identifier(_) => true,
            Expression::Value(_) | Expression::Wildcard => false,
            Expression::QualifiedIdentifier { table: table_name, .. } => table_name != table,
            Expression::Path { .. } => true,
            Expression::UnaryOperation {  expr , .. } | Expression::IsNull { expr, .. } => references(expr, table),
            Expression::BinaryOperation { left, right, .. } => references(left, table) || references(right, table),
            Expression::Function {args, .. } => args.iter().any(|a| references(a, table)),
            Expression::Alias { expr, .. } => references(expr, table),
            Expression::Nested(expr) => references(expr, table),
        }
    }

    match references(r#where, table) {
        true => (None, Some(r#where)),
        _ => (Some(r#where), None),
    }
}

fn returning_schema(returning: &[Expression], schema: &Schema) -> Result<Option<Schema>, SqlError> {
    if returning.is_empty() {
        return Ok(None);
    }

    let cols = returning
        .iter()
        .map(|expr| match expr {
            Expression::Alias { expr, alias } => {
                Ok(Column::new(alias, resolve_type(schema, expr)?))
            }
            Expression::Identifier(ident) => {
                let idx = schema
                    .index_of(ident)
                    .ok_or(SqlError::InvalidColumn(ident.into()))?;

                Ok(schema.columns[idx].clone())
            }
            Expression::QualifiedIdentifier { table, column } => {
                let qualified = format!("{table}.{column}");
                let idx = schema
                    .index_of(&qualified)
                    .ok_or(SqlError::InvalidQualifiedColumn {
                        table: table.into(),
                        column: column.into(),
                    })?;

                Ok(schema.columns[idx].clone())
            }

            _ => Ok(Column::new(&expr.to_string(), resolve_type(schema, expr)?)),
        })
        .collect::<Result<Vec<_>, SqlError>>()?;

    let mut schema = Schema::new(cols);
    (0..schema.columns.len()).for_each(|idx| {
        let variants =
            schema.columns[idx]
                .type_def
                .clone()
                .or(match schema.columns[idx].data_type {
                    Type::Enum(id) => schema.get_enum(id).cloned(),
                    _ => None,
                });

        if let Some(variants) = variants {
            let id = schema.add_enum(variants);
            schema.columns[idx].data_type = Type::Enum(id);
        }
    });

    Ok(Some(schema))
}

#[cfg(test)]
mod tests {
    use crate::{
        sql::statement::{Function, JoinType},
        vm::planner::{
            AggregateBuilder, HashJoin, IndexNestedLoopJoin, Sort, SortKeys, TupleComparator,
            DEFAULT_SORT_BUFFER_SIZE,
        },
    };
    use std::{
        cell::RefCell,
        collections::{HashMap, HashSet},
        ops::Bound,
        path::PathBuf,
    };

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
                comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)).into(),
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
                comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)).into(),
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
                        sort_schema.clone(),
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
                        comparator: FixedSizeCmp(byte_len_of_type(&Type::Integer)).into(),
                        source: Box::new(Planner::Sort(sort))
                    })),
                    group_by: vec![],
                    aggr_exprs: vec![Expression::Function {
                        func: Function::Min,
                        args: vec![parse_expr("age")]
                    }],
                    output: Schema::new(vec![Column::new("MIN", Type::BigInteger)]),
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
                        Column::new("SUM", Type::Numeric(NUMERIC_ANY, NUMERIC_ANY))
                    ]),
                    page_size: db.db.pager.borrow().page_size,
                }
                .into()
            )
        );
        Ok(())
    }

    #[test]
    fn test_join() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));",
            "CREATE TABLE orders (order_id SERIAL PRIMARY KEY, user_id INT, item VARCHAR(50));",
        ])?;

        let users_table = db.tables["users"].clone();
        let orders_table = db.tables["orders"].clone();

        let mut schema = db.tables["users"].schema.clone();
        schema.extend(orders_table.schema.columns.clone());
        schema.add_qualified_name("users", 0, users_table.schema.len());
        schema.add_qualified_name("orders", users_table.schema.len(), schema.len());

        assert_eq!(
            db.gen_plan("SELECT name, order_id FROM users JOIN orders ON id = user_id;")?,
            Planner::Project(Project {
                input: schema.clone(),
                output: Schema::new(vec![
                    Column::new("name", Type::Varchar(100)),
                    Column::primary_key("order_id", Type::Serial)
                ]),
                projection: vec![
                    Expression::Identifier("name".into()),
                    Expression::Identifier("order_id".into()),
                ],
                source: Box::new(Planner::HashJoin(HashJoin::new(
                    Planner::SeqScan(SeqScan {
                        table: users_table.clone(),
                        pager: db.pager(),
                        cursor: Cursor::new(users_table.root, 0),
                    }),
                    Planner::SeqScan(SeqScan {
                        table: orders_table.clone(),
                        pager: db.pager(),
                        cursor: Cursor::new(orders_table.root, 0),
                    }),
                    JoinType::Inner,
                    schema.clone(),
                    Expression::Identifier("id".into()),
                    Expression::Identifier("user_id".into()),
                    VmType::Number,
                ))),
            })
        );

        Ok(())
    }

    #[test]
    fn test_join_with_qualified_identifiers() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));",
            "CREATE TABLE orders (order_id SERIAL PRIMARY KEY, user_id INT, item VARCHAR(50));",
        ])?;

        let users_table = db.tables["users"].clone();
        let orders_table = db.tables["orders"].clone();

        let mut joined_schema = db.tables["users"].schema.clone();
        joined_schema.extend(orders_table.schema.columns.clone());
        joined_schema.add_qualified_name("users", 0, users_table.schema.len());
        joined_schema.add_qualified_name("orders", users_table.schema.len(), joined_schema.len());

        let plan = db.gen_plan("SELECT users.name as name, orders.order_id as order_id FROM users JOIN orders ON users.id = orders.user_id;")?;

        assert_eq!(
            plan,
            Planner::Project(Project {
                input: joined_schema.clone(),
                output: Schema::new(vec![
                    Column::new("name", Type::Varchar(100)),
                    Column::primary_key("order_id", Type::Serial),
                ]),
                projection: vec![
                    Expression::Alias {
                        expr: Box::new(Expression::QualifiedIdentifier {
                            table: "users".into(),
                            column: "name".into(),
                        }),
                        alias: "name".into(),
                    },
                    Expression::Alias {
                        expr: Box::new(Expression::QualifiedIdentifier {
                            table: "orders".into(),
                            column: "order_id".into(),
                        }),
                        alias: "order_id".into(),
                    },
                ],
                source: Box::new(Planner::HashJoin(HashJoin::new(
                    Planner::SeqScan(SeqScan {
                        table: users_table.clone(),
                        pager: db.pager(),
                        cursor: Cursor::new(users_table.root, 0),
                    }),
                    Planner::SeqScan(SeqScan {
                        table: orders_table.clone(),
                        pager: db.pager(),
                        cursor: Cursor::new(orders_table.root, 0),
                    }),
                    JoinType::Inner,
                    joined_schema,
                    Expression::QualifiedIdentifier {
                        table: "users".into(),
                        column: "id".into(),
                    },
                    Expression::QualifiedIdentifier {
                        table: "orders".into(),
                        column: "user_id".into(),
                    },
                    VmType::Number,
                ))),
            })
        );

        Ok(())
    }

    #[test]
    fn test_qualified_column_non_existent() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100));",
            "CREATE TABLE orders (order_id SERIAL PRIMARY KEY, user_id INT, item VARCHAR(50));",
        ])?;

        assert_eq!(
            db.gen_plan("SELECT users.name as name, orders.id as order_id FROM users JOIN orders ON users.id = orders.user_id;").unwrap_err(),
            SqlError::InvalidQualifiedColumn { table: "orders".into(), column: "id".into() }.into()
        );

        Ok(())
    }

    #[test]
    fn test_index_nested_loop_join_plan() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));",
            "CREATE UNIQUE INDEX users_pk_index ON users(id);",
            "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);",
        ])?;

        let users_table = db.tables["users"].clone();
        let orders_table = db.tables["orders"].clone();
        let users_pk_index = db.indexes[&index!(primary on users)].clone();

        let mut joined_schema = orders_table.schema.clone();
        joined_schema.extend(users_table.schema.columns.clone());
        joined_schema.add_qualified_name("orders", 0, orders_table.schema.len());
        joined_schema.add_qualified_name("users", orders_table.schema.len(), joined_schema.len());

        // selective on orders (id=100) -> INLJ with users
        let query = r#"
            SELECT orders.amount, users.name
            FROM orders JOIN users ON orders.user_id = users.id
            WHERE orders.id = 100;"#;

        let left_plan = Planner::ExactMatch(ExactMatch {
            relation: Relation::Table(orders_table.clone()),
            key: serialize(&Type::Integer, &Value::Number(100)),
            expr: parse_expr("id = 100"),
            pager: db.pager(),
            done: false,
            emit_only_key: false,
        });

        assert_eq!(
            db.gen_plan(query)?,
            Planner::Project(Project {
                input: joined_schema.clone(),
                output: Schema::new(vec![
                    Column::new("amount", Type::Integer),
                    Column::new("name", Type::Varchar(50)),
                ]),
                projection: vec![
                    Expression::QualifiedIdentifier {
                        column: "amount".into(),
                        table: "orders".into(),
                    },
                    Expression::QualifiedIdentifier {
                        table: "users".into(),
                        column: "name".into()
                    },
                ],
                source: Box::new(Planner::IndexNestedLoopJoin(IndexNestedLoopJoin {
                    left: Box::new(left_plan),
                    right_table: users_table.clone(),
                    index: users_pk_index,
                    condition: parse_expr("orders.user_id = users.id"),
                    join_type: JoinType::Inner,
                    left_key_expr: Expression::QualifiedIdentifier {
                        table: "orders".to_string(),
                        column: "user_id".to_string(),
                    },
                    pager: db.pager(),
                    left_tables: HashSet::from(["orders".to_string()]),
                    right_tables: HashSet::from(["users".to_string()]),
                    schema: joined_schema,
                })),
            })
        );

        Ok(())
    }

    #[test]
    fn test_inlj_with_filter_on_left() -> PlannerResult {
        let mut db = new_db(&[
            "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50), tier VARCHAR(20));",
            "CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT);",
            "CREATE UNIQUE INDEX orders_cust_idx ON orders(customer_id);",
        ])?;

        let customers = db.tables["customers"].clone();
        let orders = db.tables["orders"].clone();
        let orders_idx = db.indexes["orders_cust_idx"].clone();

        let mut joined_schema = customers.schema.clone();
        joined_schema.extend(orders.schema.columns.clone());
        joined_schema.add_qualified_name("customers", 0, customers.schema.len());
        joined_schema.add_qualified_name("orders", customers.schema.len(), joined_schema.len());

        // the query has a filter on customers (tier = 'gold').
        // the heuristic should now pick inlj because the left side is filtered (selective).
        let query = r#"
            SELECT customers.name, orders.amount
            FROM customers 
            JOIN orders ON customers.id = orders.customer_id
            WHERE customers.tier = 'Gold';"#;

        let left_plan = Planner::Filter(Filter {
            filter: parse_expr("customers.tier = 'Gold'"),
            schema: customers.schema.clone(),
            source: Box::new(Planner::SeqScan(SeqScan {
                pager: db.pager(),
                table: customers.clone(),
                cursor: Cursor::new(customers.root, 0),
            })),
        });

        assert_eq!(
            db.gen_plan(query)?,
            Planner::Project(Project {
                input: joined_schema.clone(),
                output: Schema::new(vec![
                    Column::new("name", Type::Varchar(50)),
                    Column::new("amount", Type::Integer),
                ]),
                projection: vec![
                    Expression::QualifiedIdentifier {
                        table: "customers".into(),
                        column: "name".into()
                    },
                    Expression::QualifiedIdentifier {
                        column: "amount".into(),
                        table: "orders".into(),
                    },
                ],
                source: Box::new(Planner::IndexNestedLoopJoin(IndexNestedLoopJoin {
                    left: Box::new(left_plan),
                    right_table: orders.clone(),
                    index: orders_idx,
                    condition: parse_expr("customers.id = orders.customer_id"),
                    join_type: JoinType::Inner,
                    left_key_expr: Expression::QualifiedIdentifier {
                        table: "customers".to_string(),
                        column: "id".to_string(),
                    },
                    pager: db.pager(),
                    left_tables: HashSet::from(["customers".to_string()]),
                    right_tables: HashSet::from(["orders".to_string()]),
                    schema: joined_schema,
                })),
            })
        );

        Ok(())
    }
}
