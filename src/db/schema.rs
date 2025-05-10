use crate::db::ROW_COL_ID;
use crate::sql::statement::{Column, Type};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct Schema {
    pub columns: Vec<Column>,
    index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let mut index = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            index.insert(col.name.to_string(), i);
        }
        Self { columns, index }
    }

    pub fn prepend_id(&mut self) {
        debug_assert!(
            self.columns.first().map_or(true, |c| c.name.ne(ROW_COL_ID)),
            "schema already has {ROW_COL_ID}: {self:?}"
        );

        let col = Column::new(ROW_COL_ID, Type::UnsignedBigInteger);

        let mut new_index = HashMap::new();
        for (i, col) in self.columns.iter().enumerate() {
            new_index.insert(col.name.as_str(), i);
        }

        self.columns.insert(0, col);
        self.index.values_mut().for_each(|idx| *idx += 1);
        self.index.insert(ROW_COL_ID.to_string(), 0);
    }

    pub fn index_of(&self, col: &str) -> Option<usize> {
        self.index.get(col).copied()
    }

    pub fn columns_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.to_string()).collect()
    }

    pub fn keys(&self) -> &Column {
        &self.columns[0]
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }
}
