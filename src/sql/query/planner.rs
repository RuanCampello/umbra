use std::{
    collections::VecDeque,
    io::{Read, Seek, Write},
    rc::Rc,
};

use crate::vm::planner::{Collect, Project, TupleComparator, Update, DEFAULT_SORT_BUFFER_SIZE};
use crate::{
    core::storage::pagination::io::FileOperations,
    db::{Ctx, Database, DatabaseError, Schema, SqlError},
    sql::{
        analyzer,
        query::optimiser,
        statement::{Column, Expression, Type},
        Statement,
    },
    vm::{
        expression::VmType,
        planner::{Insert, Planner, Sort, SortKeys, Values},
    },
};

pub(crate) fn generate_plan<File: Seek + Read + Write + FileOperations>(
    statement: Statement,
    db: &mut Database<File>,
) -> Result<Planner<File>, DatabaseError> {
    Ok(match statement {
        Statement::Insert { into, values, .. } => {
            let values = VecDeque::from(values);
            let source = Box::new(Planner::Values(Values { values }));
            let table = db.metadata(&into)?;

            Planner::Insert(Insert {
                source,
                comparator: table.comp()?,
                table: db.metadata(&into)?.clone(),
                pager: Rc::clone(&db.pager),
            })
        }
        Statement::Select {
            columns,
            from,
            r#where,
            order_by,
        } => {
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

                let collection = Collect::new(
                    Box::new(collect_source),
                    sorted_schema.clone(),
                    work_dir.clone(),
                    page_size,
                );
                let comparator = TupleComparator::new(
                    table.schema.clone(),
                    sorted_schema.clone(),
                    sorted_indexes,
                );

                source = Planner::Sort(Sort::new(
                    page_size,
                    work_dir,
                    collection,
                    comparator,
                    DEFAULT_SORT_BUFFER_SIZE,
                ));
            }

            let mut output = Schema::empty();

            for expr in &columns {
                match expr {
                    Expression::Identifier(ident) => output
                        .push(table.schema.columns[table.schema.index_of(ident).unwrap()].clone()),
                    _ => {
                        output.push(Column::new(
                            expr.to_string().as_str(),
                            resolve_type(&table.schema, expr)?,
                        ));
                    }
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
        Statement::Update {
            table,
            columns,
            r#where,
        } => {
            let mut source = optimiser::generate_seq_plan(&table, r#where, db)?;
            let work_dir = db.work_dir.clone();
            let page_size = db.pager.borrow().page_size;
            let metadata = db.metadata(&table)?;

            if needs_collection(&source) {
                source = Planner::Collect(Collect::new(
                    Box::new(source),
                    metadata.schema.clone(),
                    work_dir,
                    page_size,
                ));
            }

            Planner::Update(Update {
                comparator: metadata.comp()?,
                table: metadata.clone(),
                assigments: columns,
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
    use std::{cell::RefCell, collections::HashMap, path::PathBuf};

    use super::*;
    use crate::{
        core::storage::{btree::Cursor, pagination::pager::Pager, MemoryBuffer},
        db::{Ctx as DbCtx, IndexMetadata, TableMetadata},
        sql::{self, parser::Parser, statement::Create},
        vm::planner::{Filter, SeqScan},
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
}
