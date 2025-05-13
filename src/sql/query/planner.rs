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
    let statement = match statement {
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
                && order_by.ne(&[Expression::Identifier(table.schema.columns[0].name.clone())])
            {
                let mut schema = table.schema.clone();
                let mut sorted_indexes = Vec::with_capacity(order_by.len());

                order_by
                    .iter()
                    .try_for_each(|expr| -> Result<(), DatabaseError> {
                        let index = match expr {
                            Expression::Identifier(col) => table.schema.index_of(col).unwrap(),
                            _ => {
                                let index = sorted_indexes.len();
                                let r#type = resolve_type(&schema, &expr)?;
                                let col = Column::new(&format!("{expr}"), r#type);
                                schema.push(col);

                                index
                            }
                        };

                        sorted_indexes.push(index);
                        Ok(())
                    })?;

                let collect_source = match schema.len() > table.schema.len() {
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
                    schema.clone(),
                    work_dir.clone(),
                    page_size,
                );
                let comparator = TupleComparator::new(table.schema.clone(), schema, sorted_indexes);

                source = Planner::Sort(Sort::new(
                    page_size,
                    work_dir,
                    collection,
                    comparator,
                    DEFAULT_SORT_BUFFER_SIZE,
                ));
            }

            let mut output_schema = Schema::empty();

            for expr in &columns {
                match expr {
                    Expression::Identifier(ident) => output_schema
                        .push(table.schema.columns[table.schema.index_of(ident).unwrap()].clone()),
                    _ => {
                        output_schema.push(Column::new(
                            expr.to_string().as_str(),
                            resolve_type(&table.schema, expr)?,
                        ));
                    }
                }
            }

            if table.schema.eq(&output_schema) {
                return Ok(source);
            }

            Planner::Project(Project {
                output: output_schema,
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
    };

    Ok(statement)
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
