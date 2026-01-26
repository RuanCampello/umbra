use crate::{
    collections::hash::HashMap,
    sql::{
        statement::{Constraint, JoinType, Type},
        Value,
    },
};
use core::fmt;
use std::{
    fmt::Display,
    io::{Error, ErrorKind},
    sync::{Arc, OnceLock},
};

/// The representation of the table schema during runtime.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct Schema {
    pub columns: Vec<crate::sql::statement::Column>,
    /// Index of columns definitions based on their name
    index: HashMap<String, usize>,

    enums: Vec<Vec<String>>,
}

/// The representation of the table schema.
#[derive(Debug, Default, Clone)]
#[allow(unused)]
pub struct SchemaNew {
    pub name: String,
    pub columns: Vec<Column>,
    pub created_at: u64,
    pub updated_at: u64,

    // Cached column names, computed on the first access
    column_names: OnceLock<Arc<Vec<String>>>,

    /// Cached column index map, computed on the first access
    /// column name -> index
    column_index: OnceLock<HashMap<String, usize>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    id: usize, // TODO: SUPPORT OTHER TYPES OF ID
    name: String,
    nullable: bool,
    r#type: Type,
    /// Whether this column is a part of a primary key
    primary_key: bool,
    increment: bool,
    default: Option<String>,
    default_value: Option<Value>,
}

impl SchemaNew {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        let now = crate::core::date::now() as u64;
        let column_names = OnceLock::new();
        let column_index = OnceLock::new();

        let _ = column_names.set(Arc::new(
            columns.iter().map(|col| col.name.to_string()).collect(),
        ));
        let _ = column_index.set(
            columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (col.name.clone(), idx))
                .collect(),
        );

        Self {
            columns,
            name: name.into(),
            column_index,
            column_names,
            created_at: now,
            updated_at: now,
        }
    }
}

impl Column {
    pub fn new(
        id: usize,
        name: impl Into<String>,
        r#type: Type,
        nullable: bool,
        primary_key: bool,
    ) -> Self {
        Self {
            name: name.into(),
            r#type,
            primary_key,
            id,
            nullable,
            increment: false,
            default: None,
            default_value: None,
        }
    }

    pub fn nullable(id: usize, name: impl Into<String>, r#type: Type) -> Self {
        Self::new(id, name, r#type, true, false)
    }

    pub fn primary_key(id: usize, name: impl Into<String>, r#type: Type) -> Self {
        Self::new(id, name, r#type, false, true)
    }

    pub fn with_default(
        id: usize,
        name: impl Into<String>,
        r#type: Type,
        nullable: bool,
        primary_key: bool,
        increment: bool,
        default: Option<String>,
        default_value: Option<Value>,
    ) -> Self {
        Self {
            name: name.into(),
            r#type,
            primary_key,
            id,
            nullable,
            increment,
            default,
            default_value,
        }
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers = vec!["Column", "Type", "Nullable"];

        let rows: Vec<Vec<String>> = self
            .columns
            .iter()
            .map(|col| {
                vec![
                    col.name.clone(),
                    col.data_type.to_string(),
                    match col.is_nullable() {
                        false => "not null".to_string(),
                        _ => "".to_string(),
                    },
                ]
            })
            .collect();

        let widths: Vec<usize> = headers
            .iter()
            .enumerate()
            .map(|(i, header)| {
                let max_data_len = rows.iter().map(|row| row[i].len()).max().unwrap_or(0);
                header.len().max(max_data_len)
            })
            .collect();

        let border: String = widths
            .iter()
            .map(|&w| "-".repeat(w + 2))
            .collect::<Vec<_>>()
            .join("+");
        let full_border = format!("+{}+", border);

        let header_str = headers
            .iter()
            .zip(&widths)
            .map(|(h, &w)| format!(" {:<width$} ", h, width = w))
            .collect::<Vec<_>>()
            .join("|");

        writeln!(f, "{}", full_border)?;
        writeln!(f, "|{}|", header_str)?;
        writeln!(f, "{}", full_border)?;

        if self.columns.is_empty() {
            let total_width = widths.iter().sum::<usize>() + (widths.len() * 3) - 1;
            writeln!(
                f,
                "| {:<width$} |",
                "(No columns defined)",
                width = total_width
            )?;
        } else {
            for row in rows {
                let row_str = row
                    .iter()
                    .zip(&widths)
                    .enumerate()
                    .map(|(i, (val, &width))| match i == 2 {
                        true => format!(" {:^width$} ", val, width = width),
                        _ => format!(" {:<width$} ", val, width = width),
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                writeln!(f, "|{}|", row_str)?;
            }
        }

        writeln!(f, "{}", full_border)?;
        Ok(())
    }
}

impl From<&SchemaNew> for Vec<u8> {
    fn from(value: &SchemaNew) -> Self {
        let mut buff = Vec::new();

        buff.extend_from_slice(&(value.name.len() as u16).to_le_bytes());
        buff.extend_from_slice(&value.name.as_bytes());

        buff.extend_from_slice(&(value.columns.len() as u16).to_le_bytes());

        for column in &value.columns {
            buff.extend_from_slice(&(column.name.len() as u16).to_le_bytes());
            buff.extend_from_slice(&column.name.as_bytes());

            buff.push(column.r#type.into());
            buff.push(if column.primary_key { 1 } else { 0 });
            buff.push(if column.nullable { 1 } else { 0 });
            buff.push(if column.increment { 1 } else { 0 });

            match column.default {
                Some(ref expr) => {
                    buff.extend_from_slice(&(expr.len() as u16).to_le_bytes());
                    buff.extend_from_slice(expr.as_bytes());
                }
                None => buff.extend_from_slice(&0u16.to_le_bytes()),
            }
        }

        buff
    }
}

impl TryFrom<&[u8]> for SchemaNew {
    type Error = Error;

    fn try_from(data: &[u8]) -> Result<Self, Self::Error> {
        if data.len() < 32 {
            return Err(Error::new(ErrorKind::InvalidInput, "Schema data too short"));
        }

        let mut cursor = 0;

        if cursor + 2 > data.len() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Missing schema name length",
            ));
        }
        let name_len = u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;

        if cursor + name_len > data.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Missing table name"));
        }
        let name = String::from_utf8(data[cursor..cursor + name_len].to_vec())
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid schema table name"))?;
        cursor += name_len;

        if cursor + 2 > data.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "Missing columns count"));
        }
        let column_count =
            u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;

        let mut columns = Vec::with_capacity(column_count);
        for idx in 0..column_count {
            if cursor + 2 > data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Missing column name length",
                ));
            }
            let column_name_len =
                u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
            cursor += 2;

            if cursor + column_name_len > data.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Missing column name"));
            }

            let column_name = String::from_utf8(data[cursor..cursor + column_name_len].to_vec())
                .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid column name"))?;
            cursor += column_name_len;

            if cursor >= data.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Missing column type"));
            }
            let r#type = Type::SmallInt; // TODO: parse type
            cursor += 1;

            if cursor >= data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Missing primary key field",
                ));
            }
            let primary_key = data[cursor] != 0;
            cursor += 1;

            if cursor >= data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Missing nullable field",
                ));
            }
            let nullable = data[cursor] != 0;
            cursor += 1;

            if cursor >= data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Missing increment field",
                ));
            }
            let increment = data[cursor] != 0;
            cursor += 1;

            if cursor + 2 > data.len() {
                return Err(Error::new(ErrorKind::InvalidInput, "Missing default field"));
            }
            let default_len =
                u16::from_le_bytes(data[cursor..cursor + 2].try_into().unwrap()) as usize;
            cursor += 2;

            if cursor + default_len > data.len() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Missing default expression",
                ));
            }
            let default = String::from_utf8(data[cursor..cursor + default_len].to_vec())
                .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid default expression"))?;

            columns.push(Column {
                id: idx,
                name: column_name,
                nullable,
                r#type,
                primary_key,
                increment,
                default: Some(default),
                default_value: None,
            })
        }

        Ok(SchemaNew::new(name, columns))
    }
}

impl PartialEq for SchemaNew {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
            && self.columns == other.columns
    }
}

// TODO: this will be removed after the transition to the new schema type
impl Schema {
    pub fn new(columns: Vec<crate::sql::statement::Column>) -> Self {
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, col)| (col.name.clone(), i))
            .collect();

        Self {
            columns,
            index,
            enums: vec![],
        }
    }

    pub fn prepend_id(&mut self) {
        use crate::db::ROW_COL_ID;
        use crate::sql::statement::Column;

        debug_assert!(
            self.columns[0].name != ROW_COL_ID,
            "schema already has {ROW_COL_ID}: {self:?}"
        );

        let col = Column::new(ROW_COL_ID, Type::UnsignedBigInteger);

        self.columns.insert(0, col);
        self.index.values_mut().for_each(|idx| *idx += 1);
        self.index.insert(ROW_COL_ID.to_string(), 0);
    }

    pub fn push(&mut self, col: crate::sql::statement::Column) {
        self.index.insert(col.name.to_string(), self.len());
        self.columns.push(col);
    }

    pub fn extend(&mut self, columns: impl IntoIterator<Item = crate::sql::statement::Column>) {
        for col in columns {
            self.push(col)
        }
    }

    pub fn extend_with_join(
        &mut self,
        columns: impl IntoIterator<Item = crate::sql::statement::Column>,
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
        columns: impl IntoIterator<Item = crate::sql::statement::Column>,
        should_make_nullable: bool,
    ) -> impl Iterator<Item = crate::sql::statement::Column> {
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

    pub fn keys(&self) -> &crate::sql::statement::Column {
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

    pub const fn null_bitmap_len(&self) -> usize {
        (self.len() + 7) / 8
    }

    pub fn empty() -> Self {
        Self::new(Vec::new())
    }

    pub const fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn update_returning_input(&self) -> Self {
        let len = self.len();
        let mut columns = self.columns.clone();
        columns.extend(self.columns.clone());

        let mut input_schema = Self::new(columns);
        input_schema.add_qualified_name("old", 0, len);
        input_schema.add_qualified_name("new", len, 2 * len);

        input_schema
    }

    pub fn add_enum(&mut self, variants: Vec<String>) -> u32 {
        self.enums.push(variants);
        (self.enums.len() - 1) as u32
    }

    pub fn get_enum(&self, id: u32) -> Option<&Vec<String>> {
        self.enums.get(id as usize)
    }
}

pub(crate) fn has_btree_key(columns: &[crate::sql::statement::Column]) -> bool {
    columns[0].constraints.contains(&Constraint::PrimaryKey)
        && !matches!(columns[0].data_type, Type::Varchar(_) | Type::Boolean)
}

pub(crate) fn umbra_schema() -> Schema {
    use crate::sql::statement::Column;

    Schema::from(&[
        Column::new("type", Type::Varchar(255)),
        Column::new("name", Type::Varchar(255)),
        Column::new("root", Type::UnsignedInteger),
        Column::new("table_name", Type::Varchar(255)),
        Column::new("sql", Type::Varchar(65535)),
    ])
}
