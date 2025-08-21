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
        has_btree_key, umbra_schema, umbra_enum_schema, umbra_sequence_schema, umbra_index_schema, 
        Ctx, Database, DatabaseError, IndexMetadata, RowId, Schema,
        SqlError, DB_METADATA, MetadataTableType,
    },
    index,
    sql::{
        parser::Parser,
        statement::{Constraint, Create, Drop, Statement, Value},
    },
    vm::planner::{CollectBuilder, Execute, Filter, Planner, SeqScan},
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

            let sequences = columns
                .iter()
                .filter(|col| col.data_type.is_serial())
                .map(|col| Create::Sequence {
                    name: format!("{name}_{}_seq", col.name),
                    r#type: col.data_type.clone(),
                    table: name.clone(),
                });

            for sequence in sequences {
                exec(Statement::Create(sequence), db)?;
            }

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
                            _ => unreachable!("This ain't a index"),
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
            
            // Store the index root page in the main metadata table
            insert_into_metadata(
                db,
                vec![
                    Value::String(String::from("index")),
                    Value::String(name.clone()),
                    Value::Number(root.into()),
                    Value::String(table.clone()),
                    Value::String(format!("CREATE INDEX {} ON {} ({})", name, table, column)),
                ],
            )?;
            
            // Store index info only in the specialized index metadata table
            insert_into_specialized_metadata(
                db,
                MetadataTableType::Index,
                vec![
                    Value::String(name.clone()),
                    Value::String(table.clone()),
                    Value::String(column.clone()),
                    Value::Boolean(unique),
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

        Statement::Create(Create::Sequence {
            name,
            r#type,
            table,
        }) => {
            if !r#type.is_integer() {
                return Err(
                    SqlError::Other("Sequences can only be created for integers".into()).into(),
                );
            }

            let root = allocate_root(db)?;
            
            // Store the sequence root page in the main metadata table
            insert_into_metadata(
                db,
                vec![
                    Value::String(String::from("sequence")),
                    Value::String(name.clone()),
                    Value::Number(root.into()),
                    Value::String(table.clone()),
                    Value::String(format!("CREATE SEQUENCE {} AS {}", name, r#type)),
                ],
            )?;
            
            // Store sequence info only in the specialized sequence metadata table
            // Extract column name from sequence name (format: table_column_seq)
            let column_name = name
                .strip_prefix(&format!("{}_", table))
                .and_then(|s| s.strip_suffix("_seq"))
                .unwrap_or("");

            insert_into_specialized_metadata(
                db,
                MetadataTableType::Sequence,
                vec![
                    Value::String(name.clone()),
                    Value::String(r#type.to_string()),
                    Value::String(table),
                    Value::String(column_name.to_string()),
                    Value::Number(0), // initial value
                ],
            )?;
        }

        Statement::Create(Create::Enum { name, variants }) => {
            let root = allocate_root(db)?;
            
            // Store the enum root page in the main metadata table
            insert_into_metadata(
                db,
                vec![
                    Value::String(String::from("enum")),
                    Value::String(name.clone()),
                    Value::Number(root.into()),
                    Value::String(name.clone()),
                    Value::String(format!("CREATE TYPE {} AS ENUM ({})", name, variants.join(", "))),
                ],
            )?;
            
            // Store enum variants only in the specialized enum metadata table
            for (variant_id, variant_name) in variants.iter().enumerate() {
                insert_into_specialized_metadata(
                    db,
                    MetadataTableType::Enum,
                    vec![
                        Value::String(name.clone()),
                        Value::String(variant_name.clone()),
                        Value::Number(variant_id as i128 + 1), // variant_id starts from 1
                    ],
                )?;
            }

            // Update the enum registry in the context
            db.context.add_enum(name, variants);
        }

        Statement::Drop(Drop::Table(name)) => {
            // First, get the table metadata to free the table's btree
            let table_metadata = db.metadata(&name)?;
            let table_root = table_metadata.root;
            affected_rows = free_btree(db, table_root)?;

            // Remove from main metadata catalog (table entry)
            let comparator = db.metadata(DB_METADATA)?.comp()?;
            let mut planner = collect_from_metadata(db, &format!("table_name = '{name}' AND type = 'table'"))?;
            let schema = planner.schema().ok_or(DatabaseError::Corrupted(format!(
                "Could not obtain schema of {DB_METADATA} table"
            )))?;

            while let Some(tuple) = planner.try_next()? {
                BTree::new(&mut db.pager.borrow_mut(), 0, comparator.clone())
                    .remove(&tuple::serialize(&schema.columns[0].data_type, &tuple[0]))?;
            }

            // Clean up from specialized metadata tables
            // Remove indexes for this table
            let index_query = db.exec(&format!(
                "SELECT index_name, root FROM {} WHERE table_name = '{}';",
                MetadataTableType::Index.table_name(),
                name
            ))?;

            for tuple in &index_query.tuples {
                if let Some(Value::Number(index_root)) = tuple.get(index_query.schema.index_of("root").unwrap()) {
                    free_btree(db, *index_root as PageNumber)?;
                    // TODO: Also remove entries from specialized metadata tables
                    // This requires more complex logic to find and remove specific entries
                }
            }

            // Remove sequences for this table  
            let sequence_query = db.exec(&format!(
                "SELECT sequence_name, root FROM {} WHERE table_name = '{}';",
                MetadataTableType::Sequence.table_name(),
                name
            ))?;

            for tuple in &sequence_query.tuples {
                if let Some(Value::Number(sequence_root)) = tuple.get(sequence_query.schema.index_of("root").unwrap()) {
                    free_btree(db, *sequence_root as PageNumber)?;
                    // TODO: Also remove entries from specialized metadata tables
                }
            }

            db.context.invalidate(&name);
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

fn free_btree<File: Seek + Read + Write + FileOperations>(
    db: &mut Database<File>,
    root: PageNumber,
) -> std::io::Result<usize> {
    let mut stack = vec![root];
    let mut pager = db.pager.borrow_mut();
    let mut removed_cells = 0;

    while let Some(num) = stack.pop() {
        let page = pager.get_mut(num)?;
        stack.extend(page.iter_children().rev());

        let mut cells = page.drain(..).collect::<Vec<_>>().into_iter();
        removed_cells += cells.len();
        cells.try_for_each(|cell| pager.free_cell(cell))?;

        pager.free_page(num)?;
    }

    Ok(removed_cells)
}

pub(crate) fn insert_into_metadata<File: Write + Seek + Read + FileOperations>(
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

/// Insert data into specialized metadata tables (enum, sequence, index)
fn insert_into_specialized_metadata<File: Write + Seek + Read + FileOperations>(
    db: &mut Database<File>,
    table_type: MetadataTableType,
    mut values: Vec<Value>,
) -> Result<(), DatabaseError> {
    let schema = match table_type {
        MetadataTableType::Enum => {
            let mut schema = umbra_enum_schema();
            schema.prepend_id();
            schema
        },
        MetadataTableType::Sequence => {
            let mut schema = umbra_sequence_schema();
            schema.prepend_id();
            schema
        },
        MetadataTableType::Index => {
            let mut schema = umbra_index_schema();
            schema.prepend_id();
            schema
        },
        MetadataTableType::Main => return Err(DatabaseError::Other("Cannot insert into main metadata table using specialized function".into()))
    };

    // Get metadata for the specialized table (this will create it if it doesn't exist)
    let table_metadata = db.metadata(table_type.table_name())?;
    let root = table_metadata.root;
    
    values.insert(0, Value::Number(table_metadata.next_id().into()));

    let mut pager = db.pager.borrow_mut();
    let mut btree = BTree::new(&mut pager, root, FixedSizeCmp::new::<RowId>());

    btree.insert(tuple::serialize_tuple(&schema, &values))?;

    Ok(())
}

fn collect_from_metadata<File: Write + Seek + Read + FileOperations>(
    db: &mut Database<File>,
    filter: &str,
) -> Result<Planner<File>, DatabaseError> {
    let work_dir = db.work_dir.clone();
    let page_size = db.pager.borrow_mut().page_size;

    let table = db.metadata(DB_METADATA)?;

    Ok(Planner::Collect(
        CollectBuilder {
            work_dir,
            mem_buff_size: page_size,
            schema: table.schema.clone(),
            source: Box::new(Planner::Filter(Filter {
                filter: Parser::new(filter).parse_expr(None)?,
                schema: table.schema.clone(),
                source: Box::new(Planner::SeqScan(SeqScan {
                    table: table.to_owned(),
                    pager: Rc::clone(&db.pager),
                    cursor: Cursor::new(0, 0),
                })),
            })),
        }
        .into(),
    ))
}
