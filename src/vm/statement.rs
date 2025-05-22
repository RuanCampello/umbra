use std::io::{Read, Seek, Write};

use crate::{
    core::storage::{
        btree::{BTree, FixedSizeCmp},
        page::{Page, PageNumber},
        pagination::io::FileOperations,
        tuple,
    },
    db::{has_btree_key, umbra_schema, Ctx, Database, DatabaseError, RowId, DB_METADATA},
    sql::statement::{Constraint, Create, Statement, Value},
};

pub(crate) fn exec<File: Seek + Read + Write + FileOperations>(
    statement: Statement,
    db: &mut Database<File>,
) -> Result<usize, DatabaseError> {
    let sql = statement.to_string();
    let mut affected_rows = 0;

    match statement {
        Statement::Create(Create::Table { name, columns }) => {
            let root = allocate_root(db)?;

            insert_into_metadata(
                db,
                vec![
                    Value::String(String::from("table")),
                    Value::String(name.clone()),
                    Value::Number(root.into()),
                    Value::String(name.clone()),
                    Value::String(sql),
                ],
            )?;

            let skip_pk_idx = match has_btree_key(&columns) {
                true => 1,
                false => 0,
            };

            let indexes = columns
                .into_iter()
                .skip(skip_pk_idx)
                .filter(|col| !col.constraints.is_empty())
                .flat_map(|col| {
                    let table = name.clone();
                    col.constraints.into_iter().map(move |constraint| {
                        let name = match constraint {
                            Constraint::PrimaryKey => format!("{table}_pk_index"),
                            Constraint::Unique => format!("{table}_uq_index"),
                        };

                        Create::Index {
                            name,
                            table: table.clone(),
                            column: col.name.clone(),
                            unique: true,
                        }
                    })
                });

            for index in indexes {
                exec(Statement::Create(index), db)?;
            }
        }
        _ => {}
    }

    Ok(affected_rows)
}

/// Allocates a page on disk to be used as root table.
fn allocate_root<File: Seek + Write + Read + FileOperations>(
    db: &mut Database<File>,
) -> std::io::Result<PageNumber> {
    let mut pager = db.pager.borrow_mut();
    let root = pager.allocate_page::<Page>()?;

    Ok(root)
}

fn insert_into_metadata<File: Write + Seek + Read + FileOperations>(
    db: &mut Database<File>,
    mut values: Vec<Value>,
) -> Result<(), DatabaseError> {
    let mut schema = umbra_schema();
    schema.prepend_id();
    values.insert(0, Value::Number(db.metadata(DB_METADATA)?.next_id().into()));

    let mut pager = db.pager.borrow_mut();
    let mut btree = BTree::new(&mut pager, 0, FixedSizeCmp::new::<RowId>());

    btree.insert(tuple::serialize_tuple(&schema, &values))?;

    Ok(())
}
