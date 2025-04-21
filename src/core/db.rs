use crate::core::storage::btree::FixedSizeCmp;
use crate::core::storage::page::PageNumber;
use crate::sql::statement::Column;
use std::collections::HashMap;

/// The metadata of a table we might need during runtime.
#[derive(Debug, PartialEq)]
struct TableMetadata<'s> {
    root: PageNumber,
    name: String,
    schema: Schema<'s>,
    indexes: Vec<IndexMetadata<'s>>,
    row_id: RowId,
}

/// Index metadata information during runtime.
#[derive(Debug, PartialEq)]
struct IndexMetadata<'s> {
    root: PageNumber,
    name: String,
    column: Column,
    schema: Schema<'s>,
    unique: bool,
}

/// The representation of a table's schema in-memory.
#[derive(Debug, PartialEq)]
struct Schema<'s> {
    columns: Vec<&'s Column>,
    index: HashMap<&'s str, usize>,
}

struct Context<'s> {
    tables: HashMap<String, TableMetadata<'s>>,
    max_size: usize,
}

type RowId = u64;
// FIXME: use a proper error type
type DatabaseError = Box<dyn std::error::Error>;

pub(crate) trait Ctx {
    fn metadata(&self) -> &str;
}

impl<'s> TableMetadata<'s> {
    pub fn next_id(&mut self) -> RowId {
        let row_id = self.row_id;
        self.row_id += 1;

        row_id
    }

    pub fn comp(&self) -> Result<FixedSizeCmp, DatabaseError> {
        FixedSizeCmp::try_from(&self.schema.columns[0].data_type)
            .map_err(|e| format!("Failed to convert data type to comparator: {e:#?}").into())
    }

    pub fn keys(&self) -> Schema<'s> {
        let col = &self.schema.columns[0];

        Schema {
            columns: vec![col],
            index: HashMap::from([(col.name.as_str(), 0)]),
        }
    }
}

impl<'s> Schema<'s> {
    fn new(columns: Vec<&'s Column>) -> Self {
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.as_str(), i))
            .collect();

        Self { columns, index }
    }
}
