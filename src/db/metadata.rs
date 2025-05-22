use std::io::{Read, Seek, Write};

use crate::core::storage::btree::{BTreeKeyCmp, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::db::{DatabaseError, RowId, Schema};
use crate::sql::statement::{Column, Value};

use super::schema::umbra_schema;
use super::Database;

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct TableMetadata {
    pub root: PageNumber,
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
    pub(in crate::db) row_id: RowId,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct IndexMetadata {
    pub root: PageNumber,
    pub name: String,
    pub column: Column,
    pub schema: Schema,
    pub(crate) unique: bool,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) enum Relation {
    Index(IndexMetadata),
    Table(TableMetadata),
}

impl TableMetadata {
    pub fn next_id(&mut self) -> RowId {
        let row_id = self.row_id;
        self.row_id += 1;
        row_id
    }

    pub fn comp(&self) -> Result<FixedSizeCmp, DatabaseError> {
        FixedSizeCmp::try_from(&self.schema.columns[0].data_type).map_err(|e| {
            DatabaseError::Corrupted(format!(
                "Table {} is using a non-int Btree key with type {:#?}",
                self.name, self.schema.columns[0].data_type
            ))
        })
    }

    pub fn keys(&self) -> &Column {
        self.schema.keys()
    }

    pub fn key_only_schema(&self) -> Schema {
        Schema::new(vec![self.schema.columns[0].clone()])
    }
}

impl Relation {
    pub fn root(&self) -> PageNumber {
        match self {
            Self::Index(idx) => idx.root,
            Self::Table(table) => table.root,
        }
    }

    pub fn comp(&self) -> BTreeKeyCmp {
        match self {
            Self::Index(idx) => BTreeKeyCmp::from(&idx.column.data_type),
            Self::Table(table) => BTreeKeyCmp::from(&table.schema.columns[0].data_type),
        }
    }

    pub fn schema(&self) -> &Schema {
        match self {
            Self::Index(idx) => &idx.schema,
            Self::Table(table) => &table.schema,
        }
    }

    pub fn kind(&self) -> &str {
        match self {
            Self::Index(_) => "index",
            Self::Table(_) => "table",
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Self::Index(idx) => &idx.name,
            Self::Table(table) => &table.name,
        }
    }

    pub fn index(&self) -> usize {
        match self {
            Self::Index(_) => 1,
            Self::Table(_) => 0,
        }
    }
}

pub(crate) fn insert_metadata<File: Seek + Read + Write + FileOperations>(
    db: &mut Database<File>,
    values: Vec<Value>,
) -> Result<(), DatabaseError> {
    let mut schema = umbra_schema();
    schema.prepend_id();
    todo!("metadata insert")
}
