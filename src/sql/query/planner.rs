use std::{
    collections::VecDeque,
    io::{Read, Seek, Write},
    rc::Rc,
};

use super::optimiser;
use crate::db::{Schema, SqlError};
use crate::sql::analyzer;
use crate::sql::statement::Type;
use crate::vm::expression::VmType;
use crate::vm::planner::{
    Collect, CollectConfig, Delete, Project, Sort, TuplesComparator, Update,
    DEFAULT_SORT_INPUT_BUFFERS,
};
use crate::{
    core::storage::pagination::io::FileOperations,
    db::{Ctx, Database, DatabaseError},
    sql::statement::{Column, Expression, Statement},
    vm::planner::{Insert, Plan, SortConfig, SortKeysGen, Values},
};

/// Generates a query plan that's ready to execute by the VM.
pub(crate) fn generate_plan<F: Seek + Read + Write + FileOperations>(
    statement: Statement,
    db: &mut Database<F>,
) -> Result<Plan<F>, DatabaseError> {
    Ok(match statement {
        Statement::Insert {
            into,
            columns,
            values,
        } => {
            let source = Box::new(Plan::Values(Values {
                values: VecDeque::from(values),
            }));

            let table = db.metadata(&into)?.clone();

            Plan::Insert(Insert {
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
                let mut sort_schema = table.schema.clone();
                let mut sort_keys_indexes = Vec::with_capacity(order_by.len());

                // Precompute all the sort keys indexes so that the sorter
                // doesn't waste time figuring out where the columns are.
                for expr in &order_by {
                    let index = match expr {
                        Expression::Identifier(col) => table.schema.index_of(col).unwrap(),

                        _ => {
                            let index = sort_schema.len();
                            let data_type = resolve_unknown_type(&table.schema, expr)?;
                            let col = Column::new(&format!("{expr}"), data_type);
                            sort_schema.push(col);

                            index
                        }
                    };

                    sort_keys_indexes.push(index);
                }

                // If there are no expressions that need to be evaluated for
                // sorting then just skip the sort key generation completely,
                // we already have all the sort keys we need.
                let collect_source = if sort_schema.len() > table.schema.len() {
                    Plan::SortKeysGen(SortKeysGen {
                        source: Box::new(source),
                        schema: table.schema.clone(),
                        gen_exprs: order_by
                            .into_iter()
                            .filter(|expr| !matches!(expr, Expression::Identifier(_)))
                            .collect(),
                    })
                } else {
                    source
                };

                source = Plan::Sort(Sort::from(SortConfig {
                    page_size,
                    work_dir: work_dir.clone(),
                    collection: Collect::from(CollectConfig {
                        source: Box::new(collect_source),
                        work_dir,
                        schema: sort_schema.clone(),
                        mem_buf_size: page_size,
                    }),
                    comparator: TuplesComparator {
                        schema: table.schema.clone(),
                        sort_schema,
                        sort_keys_indexes,
                    },
                    input_buffers: DEFAULT_SORT_INPUT_BUFFERS,
                }));
            }

            let mut output_schema = Schema::empty();

            for expr in &columns {
                match expr {
                    Expression::Identifier(ident) => output_schema
                        .push(table.schema.columns[table.schema.index_of(ident).unwrap()].clone()),

                    _ => {
                        output_schema.push(Column {
                            name: expr.to_string(), // TODO: AS alias
                            data_type: resolve_unknown_type(&table.schema, expr)?,
                            constraints: vec![],
                        });
                    }
                }
            }

            // No need to project if the output schema is the exact same as the
            // table schema.
            if table.schema == output_schema {
                return Ok(source);
            }

            Plan::Project(Project {
                input_schema: table.schema.clone(),
                output_schema,
                projection: columns,
                source: Box::new(source),
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
                source = Plan::Collect(Collect::from(CollectConfig {
                    source: Box::new(source),
                    work_dir,
                    schema: metadata.schema.clone(),
                    mem_buf_size: page_size,
                }));
            }

            Plan::Update(Update {
                comparator: metadata.comp()?,
                table: metadata.clone(),
                assignments: columns,
                pager: Rc::clone(&db.pager),
                source: Box::new(source),
            })
        }

        Statement::Delete { from, r#where } => {
            let mut source = optimiser::generate_seq_plan(&from, r#where, db)?;
            let work_dir = db.work_dir.clone();
            let page_size = db.pager.borrow().page_size;
            let metadata = db.metadata(&from)?;

            if needs_collection(&source) {
                source = Plan::Collect(Collect::from(CollectConfig {
                    source: Box::new(source),
                    work_dir,
                    mem_buf_size: page_size,
                    schema: metadata.schema.clone(),
                }));
            }

            Plan::Delete(Delete {
                comparator: metadata.comp()?,
                table: metadata.clone(),
                pager: Rc::clone(&db.pager),
                source: Box::new(source),
            })
        }

        other => {
            return Err(DatabaseError::Other(format!(
                "statement {other} not yet implemeted or supported"
            )))
        }
    })
}

fn resolve_unknown_type(schema: &Schema, expr: &Expression) -> Result<Type, SqlError> {
    Ok(match expr {
        Expression::Identifier(col) => {
            let index = schema.index_of(col).unwrap();
            schema.columns[index].clone().data_type
        }

        _ => match analyzer::analyze_expression(schema, None, expr)? {
            VmType::Bool => Type::Boolean,
            VmType::Number => Type::BigInteger,
            VmType::String => Type::Varchar(65535),
            VmType::Date => unimplemented!("resolve_unknown_type for datetime"),
        },
    })
}

fn needs_collection<F>(plan: &Plan<F>) -> bool {
    match plan {
        Plan::Filter(filter) => needs_collection(&filter.source),
        // KeyScan has a sorter behind it which buffers all the tuples and
        // ExactMatch only returns one tuple.
        Plan::KeyScan(_) | Plan::ExactMatch(_) => false,
        // Top-level SeqScan, RangeScan and LogicalOrScan will need collection
        // to preserve their cursor state.
        Plan::SeqScan(_) | Plan::RangeScan(_) | Plan::LogicalOrScan(_) => true,
        _ => unreachable!("needs_collection() called with plan that is not a 'scan' plan"),
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
        vm::planner::SeqScan,
    };

    struct Ctx {
        db: Database<MemoryBuffer>,
        tables: HashMap<String, TableMetadata>,
        indexes: HashMap<String, IndexMetadata>,
    }

    impl Ctx {
        fn gen_plan(&mut self, query: &str) -> Result<Plan<MemoryBuffer>, DatabaseError> {
            println!("query {query}");
            let statement = sql::pipeline(query, &mut self.db)?;
            let plan = generate_plan(statement, &mut self.db)?;
            println!("plan {plan:#?}");
            Ok(plan)
        }

        fn pager(&self) -> Rc<RefCell<Pager<MemoryBuffer>>> {
            self.db.pager.clone()
        }
    }

    fn new_db(ctx: &[&str]) -> Result<Ctx, DatabaseError> {
        let mut pager = Pager::default().block_size(4096);
        pager.init()?;
        println!("pager {pager:#?}");

        let mut db = Database::new(Rc::new(RefCell::new(pager)), PathBuf::new());
        let mut tables = HashMap::new();
        let mut indexes = HashMap::new();
        let mut fetch_tables = Vec::new();

        for sql in ctx {
            if let Statement::Create(Create::Table { name, .. }) =
                Parser::new(sql).parse_statement()?
            {
                println!("table {name}");
                fetch_tables.push(name);
            }

            println!("sql: {}", sql);
            db.exec(sql)?;
        }

        for table_name in fetch_tables {
            println!("called here");
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

    fn parse_expr(expr: &str) -> Expression {
        let mut expr = Parser::new(expr).parse_expr(None).unwrap();
        sql::optimiser::simplify(&mut expr).unwrap();

        expr
    }

    #[test]
    fn test_simple_sequential_plan() -> Result<(), DatabaseError> {
        let mut db = new_db(&["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));"])?;

        assert_eq!(
            db.gen_plan("SELECT * FROM users;")?,
            Plan::SeqScan(SeqScan {
                pager: db.pager(),
                cursor: Cursor::new(db.tables["users"].root, 0),
                table: db.tables["users"].clone()
            })
        );

        Ok(())
    }
}
