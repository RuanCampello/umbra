use crate::db::ROW_COL_ID;
use crate::sql::statement::{Column, Constraint, JoinType, Type};
use std::collections::HashMap;

/// The representation of the table schema during runtime.
#[derive(Debug, PartialEq, Clone)]
pub struct Schema {
    pub columns: Vec<Column>,
    /// Index of columns definitions based on their name
    index: HashMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        Self { columns, index }
    }

    pub fn prepend_id(&mut self) {
        debug_assert!(
            self.columns[0].name != ROW_COL_ID,
            "schema already has {ROW_COL_ID}: {self:?}"
        );

        let col = Column::new(ROW_COL_ID, Type::UnsignedBigInteger);

        self.columns.insert(0, col);
        self.index.values_mut().for_each(|idx| *idx += 1);
        self.index.insert(ROW_COL_ID.to_string(), 0);
    }

    pub fn push(&mut self, col: Column) {
        self.index.insert(col.name.to_string(), self.len());
        self.columns.push(col);
    }

    pub fn extend(&mut self, columns: impl IntoIterator<Item = Column>) {
        for col in columns {
            self.push(col)
        }
    }

    pub fn extend_with_join(
        &mut self,
        columns: impl IntoIterator<Item = Column>,
        join_type: &JoinType,
    ) {
        match join_type {
            JoinType::Inner => self.columns.extend(columns),
            JoinType::Left => {
                self.columns
                    .extend(Self::make_nullable_if_needed(columns, true));
            }
            JoinType::Full => {
                for col in &mut self.columns {
                    if !col.is_nullable() {
                        col.constraints.push(Constraint::Nullable);
                    }
                }
                self.columns
                    .extend(Self::make_nullable_if_needed(columns, true));
            }
            JoinType::Right => {
                for col in &mut self.columns {
                    if !col.is_nullable() {
                        col.constraints.push(Constraint::Nullable);
                    }
                }
                self.columns.extend(columns);
            }
        }
    }

    fn make_nullable_if_needed(
        columns: impl IntoIterator<Item = Column>,
        should_make_nullable: bool,
    ) -> impl Iterator<Item = Column> {
        columns.into_iter().map(move |mut col| {
            if should_make_nullable && !col.is_nullable() {
                col.constraints.push(Constraint::Nullable);
            }
            col
        })
    }

    pub fn index_of(&self, col: &str) -> Option<usize> {
        self.index.get(col).copied()
    }

    /// Finds the last occurence of column name in the schema.
    pub fn last_index_of(&self, col: &str) -> Option<usize> {
        self.columns.iter().rposition(|c| c.name == col)
    }

    /// Adds qualified column to index for all columns within the range.
    /// This is usefull to support qualified identifier in self-joins where the same table appears
    /// with different aliases.
    pub fn add_qualified_name(&mut self, table: &str, start: usize, end: usize) {
        for idx in start..end {
            if let Some(col) = self.columns.get(idx) {
                let qualified_name = format!("{table}.{}", col.name);
                self.index.insert(qualified_name, idx);
            }
        }
    }

    pub fn columns_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.to_string()).collect()
    }

    pub fn keys(&self) -> &Column {
        &self.columns[0]
    }

    pub fn has_btree_key(&self) -> bool {
        self.columns[0]
            .constraints
            .contains(&Constraint::PrimaryKey)
            && !matches!(self.columns[0].data_type, Type::Varchar(_) | Type::Boolean)
    }

    pub fn has_nullable(&self) -> bool {
        self.columns.iter().any(|col| col.is_nullable())
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }
}

pub(crate) fn has_btree_key(columns: &[Column]) -> bool {
    columns[0].constraints.contains(&Constraint::PrimaryKey)
        && !matches!(columns[0].data_type, Type::Varchar(_) | Type::Boolean)
}

pub(crate) fn umbra_schema() -> Schema {
    Schema::from(&[
        Column::new("type", Type::Varchar(255)),
        Column::new("name", Type::Varchar(255)),
        Column::new("root", Type::UnsignedInteger),
        Column::new("table_name", Type::Varchar(255)),
        Column::new("sql", Type::Varchar(65535)),
    ])
}
