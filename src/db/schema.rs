use crate::{
    collections::hash::HashMap,
    sql::{
        statement::{self, Constraint, JoinType, Type},
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
    pub columns: Vec<statement::Column>,
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

    /// Cached primary key index, computed on the first access
    primary_key_index: OnceLock<Option<usize>>,
}

pub struct SchemaBuilder {
    table: String,
    columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    /// Positional identifier: the column's index within its table
    id: usize,
    name: String,
    nullable: bool,
    r#type: Type,
    /// Whether this column is a part of a primary key
    primary_key: bool,
    increment: bool,
    default: Option<String>,
    default_value: Option<Value>,
}

/// Bounds-checked cursor over serialised schema bytes.
struct SchemaBytes<'a> {
    data: &'a [u8],
    position: usize,
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
            primary_key_index: OnceLock::new(),
        }
    }

    #[inline]
    pub fn primary_column_index(&self) -> Option<usize> {
        *self.primary_key_index.get_or_init(|| {
            for (index, column) in self.columns.iter().enumerate() {
                // TODO: we might wanna filter out by the type
                if column.primary_key {
                    return Some(index);
                }
            }

            None
        })
    }
}

#[allow(unused)]
impl SchemaBuilder {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            columns: Vec::new(),
        }
    }

    pub fn column(
        mut self,
        name: impl Into<String>,
        r#type: Type,
        nullable: bool,
        primary: bool,
    ) -> Self {
        let idx = self.columns.len();

        self.columns
            .push(Column::new(idx, name, r#type, nullable, primary));

        self
    }

    pub fn add(self, name: impl Into<String>, r#type: Type) -> Self {
        self.column(name, r#type, false, false)
    }

    pub fn nullable(self, name: impl Into<String>, r#type: Type) -> Self {
        self.column(name, r#type, true, false)
    }

    pub fn primary(self, name: impl Into<String>, r#type: Type) -> Self {
        self.column(name, r#type, false, true)
    }

    pub fn build(self) -> SchemaNew {
        SchemaNew::new(self.table, self.columns)
    }

    /// builds the schema from parsed `CREATE TABLE` column definitions
    pub fn from_ast_columns(mut self, columns: &[crate::sql::statement::Column]) -> SchemaNew {
        for column in columns {
            let idx = self.columns.len();
            let primary = column.constraints.contains(&Constraint::PrimaryKey);

            self.columns.push(Column::new(
                idx,
                &column.name,
                column.data_type,
                column.is_nullable(),
                primary,
            ));
        }

        SchemaNew::new(self.table, self.columns)
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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn column_type(&self) -> Type {
        self.r#type
    }

    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    pub fn is_nullable(&self) -> bool {
        self.nullable
    }
}

impl<'a> SchemaBytes<'a> {
    const fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn take<'s>(&mut self, len: usize, context: &'s str) -> Result<&'a [u8], Error> {
        match self.data.get(self.position..self.position + len) {
            Some(bytes) => {
                self.position += len;
                Ok(bytes)
            }
            None => Err(Error::new(ErrorKind::InvalidInput, context)),
        }
    }

    fn u8<'s>(&mut self, context: &'s str) -> Result<u8, Error> {
        Ok(self.take(1, context)?[0])
    }

    fn flag<'s>(&mut self, context: &'s str) -> Result<bool, Error> {
        Ok(self.u8(context)? != 0)
    }

    fn u16<'s>(&mut self, context: &'s str) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(
            self.take(2, context)?.try_into().unwrap(),
        ))
    }

    fn u32<'s>(&mut self, context: &'s str) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(
            self.take(4, context)?.try_into().unwrap(),
        ))
    }

    fn u64<'s>(&mut self, context: &'s str) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(
            self.take(8, context)?.try_into().unwrap(),
        ))
    }

    fn string<'s>(&mut self, len: usize, context: &'s str) -> Result<String, Error> {
        let bytes = self.take(len, context)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| Error::new(ErrorKind::InvalidData, context))
    }

    fn column_type(&mut self) -> Result<Type, Error> {
        let discriminant = self.u8("Missing column type")?;

        Ok(match discriminant {
            0 => Type::SmallInt,
            1 => Type::UnsignedSmallInt,
            2 => Type::Integer,
            3 => Type::UnsignedInteger,
            4 => Type::BigInteger,
            5 => Type::UnsignedBigInteger,
            6 => Type::SmallSerial,
            7 => Type::Serial,
            8 => Type::BigSerial,
            9 => Type::Boolean,
            10 => Type::Varchar(self.u32("Missing varchar limit")? as usize),
            11 => Type::Text,
            12 => Type::Real,
            13 => Type::DoublePrecision,
            14 => Type::Uuid,
            15 => Type::Numeric(
                self.u32("Missing numeric precision")? as usize,
                self.u32("Missing numeric scale")? as usize,
            ),
            16 => Type::Date,
            17 => Type::Time,
            18 => Type::DateTime,
            19 => Type::Interval,
            20 => Type::Jsonb,
            21 => Type::Enum(self.u32("Missing enum id")?),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "Unknown column type discriminant",
                ))
            }
        })
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers = ["Column", "Type", "Nullable"];

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
        buff.extend_from_slice(value.name.as_bytes());
        buff.extend_from_slice(&value.created_at.to_le_bytes());
        buff.extend_from_slice(&value.updated_at.to_le_bytes());

        buff.extend_from_slice(&(value.columns.len() as u16).to_le_bytes());

        for column in &value.columns {
            buff.extend_from_slice(&(column.name.len() as u16).to_le_bytes());
            buff.extend_from_slice(column.name.as_bytes());

            serialise_type(column.r#type, &mut buff);
            buff.push(column.primary_key as u8);
            buff.push(column.nullable as u8);
            buff.push(column.increment as u8);

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
        let mut bytes = SchemaBytes::new(data);

        let name_len = bytes.u16("Missing schema name length")? as usize;
        let name = bytes.string(name_len, "Missing table name")?;
        let created_at = bytes.u64("Missing creation timestamp")?;
        let updated_at = bytes.u64("Missing update timestamp")?;

        let column_count = bytes.u16("Missing columns count")? as usize;
        let mut columns = Vec::with_capacity(column_count);

        for idx in 0..column_count {
            let column_name_len = bytes.u16("Missing column name length")? as usize;
            let column_name = bytes.string(column_name_len, "Missing column name")?;
            let r#type = bytes.column_type()?;
            let primary_key = bytes.flag("Missing primary key field")?;
            let nullable = bytes.flag("Missing nullable field")?;
            let increment = bytes.flag("Missing increment field")?;

            let default_len = bytes.u16("Missing default field")? as usize;
            let default = match default_len {
                0 => None,
                len => Some(bytes.string(len, "Missing default expression")?),
            };

            columns.push(Column {
                id: idx,
                name: column_name,
                nullable,
                r#type,
                primary_key,
                increment,
                default,
                default_value: None,
            })
        }

        let mut schema = SchemaNew::new(name, columns);
        schema.created_at = created_at;
        schema.updated_at = updated_at;

        Ok(schema)
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

    /// Resolves `table.column` without allocating a new string with `format!()`
    #[inline]
    pub fn index_of_qualified(&self, table: &str, column: &str) -> Option<usize> {
        self.index
            .iter()
            .find_map(|(key, &idx)| {
                (key.as_str().split_once('.') == Some((table, column))).then_some(idx)
            })
            .or_else(|| self.last_index_of(column))
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
        self.len().div_ceil(8)
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

/// Mirrors [`SchemaBytes::column_type`]; the discriminants must stay in sync.
fn serialise_type(r#type: Type, buff: &mut Vec<u8>) {
    match r#type {
        Type::SmallInt => buff.push(0),
        Type::UnsignedSmallInt => buff.push(1),
        Type::Integer => buff.push(2),
        Type::UnsignedInteger => buff.push(3),
        Type::BigInteger => buff.push(4),
        Type::UnsignedBigInteger => buff.push(5),
        Type::SmallSerial => buff.push(6),
        Type::Serial => buff.push(7),
        Type::BigSerial => buff.push(8),
        Type::Boolean => buff.push(9),
        Type::Varchar(limit) => {
            buff.push(10);
            let limit = u32::try_from(limit).expect("varchar limit exceeds u32");
            buff.extend_from_slice(&limit.to_le_bytes());
        }
        Type::Text => buff.push(11),
        Type::Real => buff.push(12),
        Type::DoublePrecision => buff.push(13),
        Type::Uuid => buff.push(14),
        Type::Numeric(precision, scale) => {
            buff.push(15);
            let precision = u32::try_from(precision).expect("numeric precision exceeds u32");
            let scale = u32::try_from(scale).expect("numeric scale exceeds u32");
            buff.extend_from_slice(&precision.to_le_bytes());
            buff.extend_from_slice(&scale.to_le_bytes());
        }
        Type::Date => buff.push(16),
        Type::Time => buff.push(17),
        Type::DateTime => buff.push(18),
        Type::Interval => buff.push(19),
        Type::Jsonb => buff.push(20),
        Type::Enum(id) => {
            buff.push(21);
            buff.extend_from_slice(&id.to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY_TYPE: [Type; 22] = [
        Type::SmallInt,
        Type::UnsignedSmallInt,
        Type::Integer,
        Type::UnsignedInteger,
        Type::BigInteger,
        Type::UnsignedBigInteger,
        Type::SmallSerial,
        Type::Serial,
        Type::BigSerial,
        Type::Boolean,
        Type::Varchar(255),
        Type::Text,
        Type::Real,
        Type::DoublePrecision,
        Type::Uuid,
        Type::Numeric(10, 2),
        Type::Date,
        Type::Time,
        Type::DateTime,
        Type::Interval,
        Type::Jsonb,
        Type::Enum(7),
    ];

    fn round_trip(schema: &SchemaNew) -> SchemaNew {
        let bytes = Vec::<u8>::from(schema);
        SchemaNew::try_from(bytes.as_slice()).expect("schema must round-trip")
    }

    #[test]
    fn round_trip_preserves_every_type() {
        let columns = EVERY_TYPE
            .iter()
            .enumerate()
            .map(|(idx, &r#type)| {
                Column::new(idx, format!("col_{idx}"), r#type, idx % 2 == 0, idx == 0)
            })
            .collect();

        let schema = SchemaNew::new("every_type", columns);
        assert_eq!(schema, round_trip(&schema));
    }

    #[test]
    fn round_trip_preserves_flags_defaults_and_timestamps() {
        let columns = vec![
            Column::primary_key(0, "id", Type::BigInteger),
            Column::with_default(
                1,
                "flag",
                Type::Boolean,
                true,
                false,
                false,
                Some("true".into()),
                None,
            ),
            Column::with_default(
                2,
                "counter",
                Type::Integer,
                false,
                false,
                true,
                Some("0".into()),
                None,
            ),
        ];

        let mut schema = SchemaNew::new("defaults", columns);
        schema.created_at = 1_234;
        schema.updated_at = 5_678;

        let decoded = round_trip(&schema);
        assert_eq!(schema, decoded);
        assert_eq!(decoded.created_at, 1_234);
        assert_eq!(decoded.updated_at, 5_678);
    }

    #[test]
    fn tiny_schema_round_trips() {
        let schema = SchemaBuilder::new("t").add("a", Type::Integer).build();
        assert_eq!(schema, round_trip(&schema));
    }

    #[test]
    fn truncated_schema_is_rejected() {
        let schema = SchemaBuilder::new("t").add("a", Type::Varchar(64)).build();
        let bytes = Vec::<u8>::from(&schema);

        for len in 0..bytes.len() {
            assert!(
                SchemaNew::try_from(&bytes[..len]).is_err(),
                "prefix of {len} bytes must be rejected"
            );
        }
    }

    #[test]
    fn unknown_type_discriminant_is_rejected() {
        let schema = SchemaBuilder::new("t").add("a", Type::Integer).build();
        let mut bytes = Vec::<u8>::from(&schema);

        // discriminant of the single column sits right after both name fields,
        // the timestamps and the column count
        bytes[24] = 0xFF;
        assert!(SchemaNew::try_from(bytes.as_slice()).is_err());
    }
}
