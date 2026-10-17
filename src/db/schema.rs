use crate::{
    collections::hash::HashMap,
    sql::{
        parser::Parser,
        statement::{self, Constraint, Expression, JoinType, OnDelete, Type, Value},
    },
    storage::tuple,
};
use core::fmt;
use std::{
    fmt::Display,
    io::{Error, ErrorKind},
    sync::{Arc, LazyLock},
};

/// The shape of a table's rows and of every tuple derived from them: the catalogue, the analyser and the executor all use it
/// Its parts are shared, so a clone costs two reference counts and a mutation copies only what it changes
#[derive(Debug, PartialEq, Clone, Default)]
pub struct Schema {
    pub columns: Arc<Vec<Column>>,
    qualified: Arc<HashMap<String, usize>>,
    /// What a table's rows satisfy beyond their types, empty for derived shapes
    pub constraints: Arc<Constraints>,
}

/// A table's CHECK predicates and foreign keys
#[derive(Debug, PartialEq, Default)]
pub struct Constraints {
    pub checks: Vec<Expression>,
    pub foreign_keys: Vec<ForeignKey>,
}

/// `column` holds, when not NULL, the `referenced` column of some row of `table`
#[derive(Debug, PartialEq, Clone)]
pub struct ForeignKey {
    pub column: usize,
    pub table: String,
    pub referenced: String,
    pub on_delete: OnDelete,
}

/// Builds the schema of a new table, numbering its enum columns
pub struct SchemaBuilder {
    columns: Vec<Column>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    name: String,
    r#type: Type,
    nullable: bool,
    primary_key: bool,
    /// `UNIQUE`: the engine keeps a unique index on it, as on the primary key
    unique: bool,
    /// Variant names of an enum column, carried with the column so a schema joined from several
    /// tables never mixes their per-table [Type::Enum] numbering
    variants: Option<Arc<[String]>>,
    /// The value an insert omitting the column takes, already of the column's type
    default: Option<Value>,
}

/// Bounds-checked cursor over serialised schema bytes
struct SchemaBytes<'a> {
    data: &'a [u8],
    position: usize,
}

/// Mirrors [SchemaBytes::column_type], the discriminants must stay in sync
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
        },
        Type::Text => buff.push(11),
        Type::Real => buff.push(12),
        Type::DoublePrecision => buff.push(13),
        Type::Uuid => buff.push(14),
        // u64 so the NUMERIC_ANY (usize::MAX) sentinel round-trips
        Type::Numeric(precision, scale) => {
            buff.push(15);
            buff.extend_from_slice(&(precision as u64).to_le_bytes());
            buff.extend_from_slice(&(scale as u64).to_le_bytes());
        },
        Type::Date => buff.push(16),
        Type::Time => buff.push(17),
        Type::DateTime => buff.push(18),
        Type::Interval => buff.push(19),
        Type::Jsonb => buff.push(20),
        Type::Enum(id) => {
            buff.push(21);
            buff.extend_from_slice(&id.to_le_bytes());
        },
    }
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self {
            columns: Arc::new(columns),
            qualified: Arc::default(),
            constraints: Arc::default(),
        }
    }

    /// the schema of a `CREATE TABLE` column list, enum columns are numbered in order
    pub fn from_definition(columns: &[statement::Column]) -> Self {
        let mut builder = SchemaBuilder::new();
        let mut constraints = Constraints::default();
        for (index, column) in columns.iter().enumerate() {
            let primary = column.constraints.contains(&Constraint::PrimaryKey);
            builder = match &column.type_def {
                Some(variants) => builder.enumeration(&column.name, variants, column.is_nullable()),
                _ => builder.column(&column.name, column.data_type, column.is_nullable(), primary),
            };

            for constraint in &column.constraints {
                match constraint {
                    Constraint::Check(expr) => constraints.checks.push(expr.clone()),
                    Constraint::References { table, column: referenced, on_delete } => {
                        let fk = ForeignKey {
                            column: index,
                            table: table.into(),
                            referenced: referenced.into(),
                            on_delete: *on_delete,
                        };
                        constraints.foreign_keys.push(fk);
                    },
                    _ => {},
                }
            }
        }

        let mut schema = builder.build();
        let unique = columns.iter().map(|column| column.constraints.contains(&Constraint::Unique));
        Arc::make_mut(&mut schema.columns)
            .iter_mut()
            .zip(unique)
            .for_each(|(column, unique)| {
                column.unique = unique;
            });
        schema.constraints = Arc::new(constraints);
        schema
    }

    /// a schema without columns
    /// shared, so it costs no allocation
    pub fn empty() -> Self {
        static EMPTY: LazyLock<Schema> = LazyLock::new(|| Schema::new(Vec::new()));
        EMPTY.clone()
    }

    /// the same schema with the row id as column 0, the layout of scanned tuples
    pub fn with_row_id(&self) -> Self {
        let mut schema = self.clone();
        schema.prepend_id();
        schema
    }

    /// a schema of only the columns at `keep`, for projection pushdown
    pub fn project(&self, keep: &[usize]) -> Self {
        Self::new(keep.iter().map(|&i| self.columns[i].clone()).collect())
    }

    pub fn prepend_id(&mut self) {
        use crate::db::ROW_COL_ID;

        debug_assert!(
            self.columns.first().is_none_or(|first| first.name != ROW_COL_ID),
            "schema already has {ROW_COL_ID}: {self:?}"
        );

        let col = Column::new(ROW_COL_ID, Type::UnsignedBigInteger);
        Arc::make_mut(&mut self.columns).insert(0, col);
        if !self.qualified.is_empty() {
            Arc::make_mut(&mut self.qualified).values_mut().for_each(|idx| *idx += 1);
        }
    }

    pub fn push(&mut self, col: Column) {
        Arc::make_mut(&mut self.columns).push(col);
    }

    pub fn extend(&mut self, columns: impl IntoIterator<Item = Column>) {
        for col in columns {
            self.push(col)
        }
    }

    /// appends the columns of a joined table, the outer sides of the join become nullable
    /// their bare names now resolve to them, qualified names come from [Schema::add_qualified_name]
    pub fn extend_with_join(
        &mut self,
        columns: impl IntoIterator<Item = Column>,
        join_type: &JoinType,
    ) {
        let own = Arc::make_mut(&mut self.columns);
        if matches!(join_type, JoinType::Right | JoinType::Full) {
            own.iter_mut().for_each(|col| col.nullable = true);
        }

        let pad = matches!(join_type, JoinType::Left | JoinType::Full);
        own.extend(columns.into_iter().map(|mut col| {
            col.nullable |= pad;
            col
        }));
    }

    pub fn index_of(&self, col: &str) -> Option<usize> {
        self.qualified.get(col).copied().or_else(|| self.last_index_of(col))
    }

    /// finds the last occurence of column name in the schema
    pub fn last_index_of(&self, col: &str) -> Option<usize> {
        self.columns.iter().rposition(|c| c.name == col)
    }

    /// adds qualified column to index for all columns within the range
    /// this is usefull to support qualified identifier in self-joins where the same table appears
    /// with different aliases
    pub fn add_qualified_name(&mut self, table: &str, start: usize, end: usize) {
        let qualified = Arc::make_mut(&mut self.qualified);
        for idx in start..end {
            if let Some(col) = self.columns.get(idx) {
                qualified.insert(format!("{table}.{}", col.name), idx);
            }
        }
    }

    /// resolves `table.column` without allocating a new string
    #[inline]
    pub fn index_of_qualified(&self, table: &str, column: &str) -> Option<usize> {
        self.qualified
            .iter()
            .find_map(|(key, &idx)| {
                (key.as_str().split_once('.') == Some((table, column))).then_some(idx)
            })
            .or_else(|| self.last_index_of(column))
    }

    #[inline]
    pub fn primary_column_index(&self) -> Option<usize> {
        self.columns.iter().position(Column::is_primary_key)
    }

    pub fn columns_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn has_nullable(&self) -> bool {
        self.columns.iter().any(Column::is_nullable)
    }

    pub fn null_bitmap_len(&self) -> usize {
        self.len().div_ceil(8)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn update_returning_input(&self) -> Self {
        let len = self.len();
        let mut columns = self.columns.to_vec();
        columns.extend(self.columns.iter().cloned());

        let mut input_schema = Self::new(columns);
        input_schema.add_qualified_name("old", 0, len);
        input_schema.add_qualified_name("new", len, 2 * len);

        input_schema
    }

    /// sets the default of the column at `index`
    pub fn set_default(&mut self, index: usize, value: Value) {
        Arc::make_mut(&mut self.columns)[index].default = (value != Value::Null).then_some(value);
    }

    /// the catalogue bytes of table `name` with this schema: name, columns, the variant lists of
    /// the enum columns in [Type::Enum] order, then the constraints
    pub fn encode(&self, name: &str) -> Vec<u8> {
        let mut buff = Vec::new();

        buff.extend_from_slice(&(name.len() as u16).to_le_bytes());
        buff.extend_from_slice(name.as_bytes());
        buff.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());

        let mut enums = Vec::new();
        for column in self.columns.iter() {
            buff.extend_from_slice(&(column.name.len() as u16).to_le_bytes());
            buff.extend_from_slice(column.name.as_bytes());
            serialise_type(column.r#type, &mut buff);
            buff.push(column.primary_key as u8);
            buff.push(column.nullable as u8);
            buff.push(column.unique as u8);
            buff.push(column.default.is_some() as u8);
            if let Some(default) = &column.default {
                tuple::serialize_into(&mut buff, &column.r#type, default);
            }

            if let (Type::Enum(id), Some(variants)) = (column.r#type, &column.variants) {
                enums.push((id, variants));
            }
        }

        enums.sort_by_key(|(id, _)| *id);
        buff.extend_from_slice(&(enums.len() as u16).to_le_bytes());
        for (_, variants) in enums {
            buff.extend_from_slice(&(variants.len() as u16).to_le_bytes());
            for variant in variants.iter() {
                buff.extend_from_slice(&(variant.len() as u16).to_le_bytes());
                buff.extend_from_slice(variant.as_bytes());
            }
        }

        let put_string = |buff: &mut Vec<u8>, string: &str| {
            buff.extend_from_slice(&(string.len() as u16).to_le_bytes());
            buff.extend_from_slice(string.as_bytes());
        };

        buff.extend_from_slice(&(self.constraints.checks.len() as u16).to_le_bytes());
        for check in &self.constraints.checks {
            let string = check.to_sql().expect("the analyser admits only spellable checks");
            put_string(&mut buff, &string);
        }
        buff.extend_from_slice(&(self.constraints.foreign_keys.len() as u16).to_le_bytes());
        for key in &self.constraints.foreign_keys {
            buff.extend_from_slice(&(key.column as u16).to_le_bytes());
            put_string(&mut buff, &key.table);
            put_string(&mut buff, &key.referenced);
            buff.push((key.on_delete == OnDelete::Cascade) as u8);
        }

        buff
    }

    /// the table name and schema [Schema::encode] wrote
    pub fn decode(data: &[u8]) -> Result<(String, Self), Error> {
        let mut bytes = SchemaBytes::new(data);

        let name_len = bytes.u16("Missing schema name length")? as usize;
        let name = bytes.string(name_len, "Missing table name")?;

        let column_count = bytes.u16("Missing columns count")? as usize;
        let mut columns = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            let column_name_len = bytes.u16("Missing column name length")? as usize;
            let column_name = bytes.string(column_name_len, "Missing column name")?;
            let r#type = bytes.column_type()?;
            let primary_key = bytes.flag("Missing primary key field")?;
            let nullable = bytes.flag("Missing nullable field")?;
            let unique = bytes.flag("Missing unique field")?;
            let mut column = Column {
                name: column_name,
                r#type,
                nullable,
                primary_key,
                unique,
                variants: None,
                default: None,
            };
            if bytes.flag("Missing default field")? {
                column.default = Some(bytes.value(&column)?);
            }

            columns.push(column);
        }

        let enum_count = bytes.u16("Missing enum count")? as usize;
        let mut enums = Vec::with_capacity(enum_count);
        for _ in 0..enum_count {
            let variant_count = bytes.u16("Missing enum variant count")? as usize;
            let mut variants = Vec::with_capacity(variant_count);
            for _ in 0..variant_count {
                let len = bytes.u16("Missing enum variant length")? as usize;
                variants.push(bytes.string(len, "Missing enum variant")?);
            }
            enums.push(Arc::<[String]>::from(variants));
        }

        for column in &mut columns {
            if let Type::Enum(id) = column.r#type {
                let variants = enums
                    .get(id as usize)
                    .ok_or_else(|| Error::new(ErrorKind::InvalidData, "Unknown enum id"))?;
                column.variants = Some(Arc::clone(variants));
            }
        }

        let mut constraints = Constraints::default();
        for _ in 0..bytes.u16("Missing check count")? {
            let len = bytes.u16("Missing check length")? as usize;
            let text = bytes.string(len, "Missing check")?;
            let check = Parser::new(&text).parse_expr(None).map_err(|error| {
                Error::new(ErrorKind::InvalidData, format!("check {text}: {error}"))
            })?;
            constraints.checks.push(check);
        }
        for _ in 0..bytes.u16("Missing foreign key count")? {
            let column = bytes.u16("Missing foreign key column")? as usize;
            let len = bytes.u16("Missing referenced table length")? as usize;
            let table = bytes.string(len, "Missing referenced table")?;
            let len = bytes.u16("Missing referenced column length")? as usize;
            let referenced = bytes.string(len, "Missing referenced column")?;
            let on_delete = match bytes.flag("Missing on delete action")? {
                true => OnDelete::Cascade,
                false => OnDelete::Restrict,
            };
            let fk = ForeignKey { column, table, referenced, on_delete };
            constraints.foreign_keys.push(fk);
        }

        let mut schema = Self::new(columns);
        schema.constraints = Arc::new(constraints);
        Ok((name, schema))
    }
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    pub fn column(
        mut self,
        name: impl Into<String>,
        r#type: Type,
        nullable: bool,
        primary: bool,
    ) -> Self {
        let mut column = Column::new(name, r#type);
        column.nullable = nullable;
        column.primary_key = primary;
        self.columns.push(column);

        self
    }

    #[cfg(test)]
    pub fn add(self, name: impl Into<String>, r#type: Type) -> Self {
        self.column(name, r#type, false, false)
    }

    #[cfg(test)]
    pub fn nullable(self, name: impl Into<String>, r#type: Type) -> Self {
        self.column(name, r#type, true, false)
    }

    #[cfg(test)]
    pub fn primary(self, name: impl Into<String>, r#type: Type) -> Self {
        self.column(name, r#type, false, true)
    }

    /// an enum column with these variants, numbered after the table's earlier enum columns
    pub fn enumeration(
        mut self,
        name: impl Into<String>,
        variants: &[String],
        nullable: bool,
    ) -> Self {
        let id = self.columns.iter().filter(|column| column.variants.is_some()).count() as u32;

        let mut column = Column::new(name, Type::Enum(id));
        column.nullable = nullable;
        column.variants = Some(Arc::from(variants));
        self.columns.push(column);

        self
    }

    pub fn build(self) -> Schema {
        Schema::new(self.columns)
    }
}

impl Column {
    /// a column that is neither nullable nor part of the primary key
    pub fn new(name: impl Into<String>, r#type: Type) -> Self {
        Self {
            name: name.into(),
            r#type,
            nullable: false,
            primary_key: false,
            unique: false,
            variants: None,
            default: None,
        }
    }

    pub fn nullable(name: impl Into<String>, r#type: Type) -> Self {
        let mut column = Self::new(name, r#type);
        column.nullable = true;
        column
    }

    /// this column as an enum column with these variants
    pub fn with_variants(mut self, variants: impl Into<Arc<[String]>>) -> Self {
        self.variants = Some(variants.into());
        self
    }

    /// this column under another name (an alias or an expression's text)
    pub fn named(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    pub fn column_type(&self) -> Type {
        self.r#type
    }

    #[inline]
    pub fn is_primary_key(&self) -> bool {
        self.primary_key
    }

    #[inline]
    pub fn is_nullable(&self) -> bool {
        self.nullable
    }

    #[inline]
    pub fn is_unique(&self) -> bool {
        self.unique
    }

    /// the variant names of an enum column
    #[inline]
    pub fn variants(&self) -> Option<&[String]> {
        self.variants.as_deref()
    }

    #[inline]
    pub fn default(&self) -> Option<&Value> {
        self.default.as_ref()
    }
}

impl<'a> SchemaBytes<'a> {
    const fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    fn take(&mut self, len: usize, context: &str) -> Result<&'a [u8], Error> {
        match self.data.get(self.position..self.position + len) {
            Some(bytes) => {
                self.position += len;
                Ok(bytes)
            },
            None => Err(Error::new(ErrorKind::InvalidInput, context)),
        }
    }

    fn u8(&mut self, context: &str) -> Result<u8, Error> {
        Ok(self.take(1, context)?[0])
    }

    fn flag(&mut self, context: &str) -> Result<bool, Error> {
        Ok(self.u8(context)? != 0)
    }

    /// a non-null value of `column`'s type, as [tuple::serialize_into] wrote it
    fn value(&mut self, column: &Column) -> Result<Value, Error> {
        let mut reader = std::io::Cursor::new(&self.data[self.position..]);
        let value = tuple::read_value(&mut reader, column)?;
        self.position += reader.position() as usize;
        Ok(value)
    }

    fn u16(&mut self, context: &str) -> Result<u16, Error> {
        Ok(u16::from_le_bytes(self.take(2, context)?.try_into().unwrap()))
    }

    fn u32(&mut self, context: &str) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4, context)?.try_into().unwrap()))
    }

    fn u64(&mut self, context: &str) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.take(8, context)?.try_into().unwrap()))
    }

    fn string(&mut self, len: usize, context: &str) -> Result<String, Error> {
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
                self.u64("Missing numeric precision")? as usize,
                self.u64("Missing numeric scale")? as usize,
            ),
            16 => Type::Date,
            17 => Type::Time,
            18 => Type::DateTime,
            19 => Type::Interval,
            20 => Type::Jsonb,
            21 => Type::Enum(self.u32("Missing enum id")?),
            _ => {
                return Err(Error::new(ErrorKind::InvalidData, "Unknown column type discriminant"));
            },
        })
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
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
                    col.r#type.to_string(),
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

        let border: String =
            widths.iter().map(|&w| "-".repeat(w + 2)).collect::<Vec<_>>().join("+");
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
            writeln!(f, "| {:<width$} |", "(No columns defined)", width = total_width)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    const EVERY_TYPE: [Type; 21] = [
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
    ];

    fn round_trip(name: &str, schema: &Schema) -> Schema {
        let (decoded_name, decoded) =
            Schema::decode(&schema.encode(name)).expect("schema must round-trip");
        assert_eq!(decoded_name, name);
        decoded
    }

    #[test]
    fn round_trip_preserves_every_type_and_flag() {
        let builder =
            EVERY_TYPE
                .iter()
                .enumerate()
                .fold(SchemaBuilder::new(), |builder, (idx, &r#type)| {
                    builder.column(format!("col_{idx}"), r#type, idx % 2 == 0, idx == 0)
                });
        let schema = builder.build();

        assert_eq!(schema, round_trip("every_type", &schema));
    }

    #[test]
    fn enum_columns_keep_their_variants_through_the_bytes() {
        let schema = SchemaBuilder::new()
            .primary("id", Type::Integer)
            .enumeration("mood", &["calm".into(), "angry".into()], false)
            .enumeration("size", &["s".into(), "m".into(), "l".into()], true)
            .build();

        let decoded = round_trip("moods", &schema);
        assert_eq!(schema, decoded);
        assert_eq!(decoded.columns[1].column_type(), Type::Enum(0));
        assert_eq!(decoded.columns[2].variants(), Some(&["s".into(), "m".into(), "l".into()][..]));
    }

    #[test]
    fn truncated_schema_is_rejected() {
        let bytes = SchemaBuilder::new().add("a", Type::Varchar(64)).build().encode("t");

        for len in 0..bytes.len() {
            assert!(
                Schema::decode(&bytes[..len]).is_err(),
                "prefix of {len} bytes must be rejected"
            );
        }
    }

    #[test]
    fn unknown_type_discriminant_is_rejected() {
        let mut bytes = SchemaBuilder::new().add("a", Type::Integer).build().encode("t");

        // name length + name, column count, column name length + name, then the discriminant
        bytes[2 + 1 + 2 + 2 + 1] = 0xFF;
        assert!(Schema::decode(&bytes).is_err());
    }

    #[test]
    fn joins_make_outer_sides_nullable() {
        let left = SchemaBuilder::new().primary("id", Type::Integer).build();
        let right = vec![Column::new("x", Type::Integer)];

        let mut joined = left.clone();
        joined.extend_with_join(right.clone(), &JoinType::Left);
        assert!(!joined.columns[0].is_nullable() && joined.columns[1].is_nullable());

        let mut joined = left;
        joined.extend_with_join(right, &JoinType::Right);
        assert!(joined.columns[0].is_nullable() && !joined.columns[1].is_nullable());
    }
}
