use std::{
    io::{Read, Seek, Write},
    rc::Rc,
};

use crate::{
    core::storage::{
        btree::{BTree, BytesCmp, Cursor, FixedSizeCmp},
        page::{Page, PageNumber},
        pagination::io::FileOperations,
        tuple::{self, serialize_tuple},
    },
    db::{
        has_btree_key, umbra_schema, Ctx, Database, DatabaseError, IndexMetadata, RowId, Schema,
        SqlError, DB_METADATA,
    },
    index,
    sql::statement::{Column, Constraint, Create, Statement, Value},
    vm::planner::{Execute, Planner, SeqScan},
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

            let metatada = db.metadata(&name)?;

            columns
                .iter()
                .filter(|col| col.data_type.is_serial())
                .for_each(|col| {
                    metatada.create_serial_for_col(col.name.to_string());
                });

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
                            Constraint::PrimaryKey => index!(primary on (table)),
                            Constraint::Unique => index!(unique on (table) (col.name)),
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
        Statement::Create(Create::Index {
            name,
            table,
            column,
            unique,
        }) => {
            if !unique {
                return Err(DatabaseError::Sql(SqlError::Other(
                    "Non-unique indexes are not supported yet".into(),
                )));
            }

            let root = allocate_root(db)?;
            insert_into_metadata(
                db,
                vec![
                    Value::String("index".into()),
                    Value::String(name.clone()),
                    Value::Number(root.into()),
                    Value::String(table.clone()),
                    Value::String(sql),
                ],
            )?;

            let metadata = db.metadata(&table)?;
            let col = metadata
                .schema
                .index_of(&column)
                .ok_or(SqlError::InvalidColumn(column))?;

            let index = IndexMetadata {
                root,
                name,
                unique,
                column: metadata.schema.columns[col].clone(),
                schema: Schema::new(vec![
                    metadata.schema.columns[col].clone(),
                    metadata.schema.columns[0].clone(),
                ]),
            };

            let mut scan = Planner::SeqScan(SeqScan {
                table: metadata.to_owned(),
                cursor: Cursor::new(metadata.root, 0),
                pager: Rc::clone(&db.pager),
            });

            let comp = Box::<dyn BytesCmp>::from(&index.column.data_type);
            while let Some(mut tuple) = scan.try_next()? {
                let mut pager = db.pager.borrow_mut();
                let mut btree = BTree::new(&mut pager, index.root, &comp);

                let index_key = tuple.swap_remove(col);
                let primary_key = tuple.swap_remove(0);

                let entry = serialize_tuple(&index.schema.clone(), [&index_key, &primary_key]);
                btree
                    .try_insert(entry)?
                    .map_err(|_| SqlError::DuplicatedKey(index_key))?;
            }

            db.context.invalidate(&table);
        }
        _ => todo!("exec unimplemented for {statement}"),
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
