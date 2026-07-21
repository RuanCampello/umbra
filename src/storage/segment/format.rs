//! Immutable columnar segments: the cold half of the hybrid storage
//!
//! Fully-committed rows are frozen out of the hot in-memory version store
//! into segments. Each column carries a zone map (min/max/null count) so
//! scans can skip whole segments without touching their data.
//! Segments never change after being written: deletes and
//! updates of frozen rows are handled above this layer, and compaction rewrites whole files
//!
//! On-disk layout (little-endian):
//!
//! ```text
//! magic "USG1" | version u16 | row_count u32 | column_count u16
//! per column:  class u8 | nulls (1 B/row) | class data | zone map
//! dictionary:  entry_count u32 | per entry: len u32 + bytes
//! row ids:     i64 per row
//! checksum:    fnv1a u32 over everything before it
//! ```

use crate::collections::hash::HashMap;
use crate::db::SchemaNew;
use crate::sql::statement::Type;
use crate::sql::Value;
use crate::storage::mvcc::fnv1a;
use crate::vm::planner::Tuple;
use std::cmp::Ordering;
use std::io::{self, Error, ErrorKind};
use std::ops::Bound;
use std::path::Path;

/// An immutable, column-major batch of committed rows
pub(crate) struct FrozenSegment {
    row_ids: Vec<i64>,
    columns: Vec<Column>,
    dictionary: Vec<String>,
}

/// Per-column min/max statistics used to prune segments during scans
#[derive(Default)]
pub(crate) struct ZoneMap {
    min: Option<Value>,
    max: Option<Value>,
    null_count: u32,
}

struct Column {
    nulls: Vec<u8>,
    data: ColumnData,
    zone: ZoneMap,
}

/// Typed column storage, chosen by the column's declared SQL type
// PERFORMANCE: integers are stored as full `i128` slots and every column is
// decoded eagerly, width packing, lazy column loads and block compression
// are follow-ups once profiling justifies them
enum ColumnData {
    Int(Vec<i128>),
    Float(Vec<f64>),
    Bool(Vec<u8>),
    Enum(Vec<u8>),
    /// Indices into the segment's string dictionary
    Dict(Vec<u32>),
    /// Variable-length values as [Value::serialise] payloads, `offsets`
    /// has `row_count + 1` entries delimiting each row's slice of `blob`
    Bytes {
        offsets: Vec<u32>,
        blob: Vec<u8>,
    },
}

const MAGIC: [u8; 4] = *b"USG1";
const VERSION: u16 = 1;

const CLASS_INT: u8 = 1;
const CLASS_FLOAT: u8 = 2;
const CLASS_BOOL: u8 = 3;
const CLASS_ENUM: u8 = 4;
const CLASS_DICT: u8 = 5;
const CLASS_BYTES: u8 = 6;

impl FrozenSegment {
    /// Freezes `rows` (fully committed) under `schema` into a columnar segment
    ///
    /// # Panics
    ///
    /// If a value's representation does not match its column class, the
    /// engine coerces values at insert time, so a mismatch is corrupt state
    pub fn build(schema: &SchemaNew, rows: Vec<(i64, Tuple)>) -> Self {
        let row_count = rows.len();
        let mut row_ids = Vec::with_capacity(row_count);
        let mut columns: Vec<_> = schema
            .columns
            .iter()
            .map(|col| Column::empty(col.column_type(), row_count))
            .collect();

        let mut interned: HashMap<String, u32> = HashMap::default();
        let mut dictionary = Vec::new();

        for (row_id, tuple) in rows {
            debug_assert_eq!(tuple.len(), columns.len(), "row width mismatch");
            debug_assert!(
                row_ids.last().is_none_or(|last| *last < row_id),
                "segment rows must arrive in ascending row id order"
            );
            row_ids.push(row_id);

            for (column, value) in columns.iter_mut().zip(tuple) {
                column.push(value, &mut interned, &mut dictionary);
            }
        }

        Self {
            row_ids,
            columns,
            dictionary,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buff = Vec::new();

        buff.extend_from_slice(&MAGIC);
        buff.extend_from_slice(&VERSION.to_le_bytes());
        buff.extend_from_slice(&(self.row_ids.len() as u32).to_le_bytes());
        buff.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());

        for column in &self.columns {
            column.encode(&mut buff);
        }

        buff.extend_from_slice(&(self.dictionary.len() as u32).to_le_bytes());
        for entry in &self.dictionary {
            buff.extend_from_slice(&(entry.len() as u32).to_le_bytes());
            buff.extend_from_slice(entry.as_bytes());
        }

        for row_id in &self.row_ids {
            buff.extend_from_slice(&row_id.to_le_bytes());
        }

        buff.extend_from_slice(&fnv1a(&buff).to_le_bytes());
        buff
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < 4 + 2 + 4 + 2 + 4 {
            return Err(corrupt("segment too short"));
        }

        let (payload, tail) = bytes.split_at(bytes.len() - 4);
        let stored = u32::from_le_bytes(tail.try_into().unwrap());
        if fnv1a(payload) != stored {
            return Err(corrupt("segment checksum mismatch"));
        }

        let mut reader = Reader::new(payload);
        if reader.bytes(4)? != MAGIC {
            return Err(corrupt("bad segment magic"));
        }
        match reader.u16()? {
            VERSION => {}
            other => return Err(corrupt(&format!("unsupported segment version {other}"))),
        }

        let row_count = reader.u32()? as usize;
        let column_count = reader.u16()? as usize;

        let columns = (0..column_count)
            .map(|_| Column::decode(&mut reader, row_count))
            .collect::<io::Result<Vec<_>>>()?;

        let entries = reader.u32()? as usize;
        let mut dictionary = Vec::with_capacity(entries);
        for _ in 0..entries {
            let len = reader.u32()? as usize;
            let text = std::str::from_utf8(reader.bytes(len)?)
                .map_err(|_| corrupt("dictionary entry is not utf-8"))?;
            dictionary.push(text.to_string());
        }

        let mut row_ids = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            row_ids.push(reader.i64()?);
        }

        Ok(Self {
            row_ids,
            columns,
            dictionary,
        })
    }

    /// writes the segment atomically: temp file, fsync, rename
    pub fn write(&self, path: &Path) -> io::Result<()> {
        use std::io::Write;

        let tmp = path.with_extension("tmp");
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&self.encode())?;
        file.sync_all()?;
        drop(file);

        std::fs::rename(&tmp, path)?;
        if let Some(dir) = path.parent() {
            std::fs::File::open(dir)?.sync_all()?;
        }

        Ok(())
    }

    pub fn read(path: &Path) -> io::Result<Self> {
        Self::decode(&std::fs::read(path)?)
    }

    #[inline]
    pub fn row_ids(&self) -> &[i64] {
        &self.row_ids
    }

    /// index of `row_id` within the segment, if frozen here
    /// row ids are ascending by construction, so this is a binary search
    #[inline]
    pub fn position(&self, row_id: i64) -> Option<usize> {
        self.row_ids.binary_search(&row_id).ok()
    }

    #[inline]
    pub fn zone(&self, column: usize) -> &ZoneMap {
        &self.columns[column].zone
    }

    /// reconstructs the row at `index` as a tuple of owned values
    pub fn tuple(&self, index: usize) -> Tuple {
        self.columns
            .iter()
            .map(|column| column.value(index, &self.dictionary))
            .collect()
    }
}

impl ZoneMap {
    /// Whether any value in the column could fall inside `(start, end)`
    /// Columns with no comparable values keep `min`/`max` unset and are
    /// never pruned
    ///
    /// Pruning surface for cold scans, wired once predicate bounds are
    /// pushed below the executor
    #[allow(dead_code)]
    pub fn may_match(&self, start: Bound<&Value>, end: Bound<&Value>) -> bool {
        let (Some(min), Some(max)) = (&self.min, &self.max) else {
            return true;
        };

        let above_start = match start {
            Bound::Unbounded => true,
            Bound::Included(low) => !matches!(max.partial_cmp(low), Some(Ordering::Less)),
            Bound::Excluded(low) => matches!(max.partial_cmp(low), Some(Ordering::Greater)),
        };
        let below_end = match end {
            Bound::Unbounded => true,
            Bound::Included(high) => !matches!(min.partial_cmp(high), Some(Ordering::Greater)),
            Bound::Excluded(high) => matches!(min.partial_cmp(high), Some(Ordering::Less)),
        };

        above_start && below_end
    }

    #[allow(dead_code)]
    #[inline]
    pub fn null_count(&self) -> u32 {
        self.null_count
    }

    #[inline]
    pub fn max(&self) -> Option<&Value> {
        self.max.as_ref()
    }

    fn observe(&mut self, value: &Value) {
        if value.is_null() {
            self.null_count += 1;
            return;
        }

        match &self.min {
            Some(min) if !matches!(value.partial_cmp(min), Some(Ordering::Less)) => {}
            _ => self.min = Some(value.clone()),
        }
        match &self.max {
            Some(max) if !matches!(value.partial_cmp(max), Some(Ordering::Greater)) => {}
            _ => self.max = Some(value.clone()),
        }
    }

    fn encode(&self, buff: &mut Vec<u8>) {
        for bound in [&self.min, &self.max] {
            match bound {
                Some(value) => {
                    let bytes = value.serialise().expect("zone bound serialises");
                    buff.push(1);
                    buff.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                    buff.extend_from_slice(&bytes);
                }
                None => buff.push(0),
            }
        }

        buff.extend_from_slice(&self.null_count.to_le_bytes());
    }

    fn decode(reader: &mut Reader<'_>) -> io::Result<Self> {
        let mut bounds = [None, None];
        for bound in &mut bounds {
            if reader.u8()? == 1 {
                let len = reader.u32()? as usize;
                let (value, _) = Value::deserialise(reader.bytes(len)?)?;
                *bound = Some(value);
            }
        }

        let [min, max] = bounds;
        Ok(Self {
            min,
            max,
            null_count: reader.u32()?,
        })
    }
}

impl Column {
    fn empty(r#type: Type, capacity: usize) -> Self {
        let data = match r#type {
            Type::SmallInt
            | Type::UnsignedSmallInt
            | Type::Integer
            | Type::UnsignedInteger
            | Type::BigInteger
            | Type::UnsignedBigInteger
            | Type::SmallSerial
            | Type::Serial
            | Type::BigSerial => ColumnData::Int(Vec::with_capacity(capacity)),
            Type::Real | Type::DoublePrecision => ColumnData::Float(Vec::with_capacity(capacity)),
            Type::Boolean => ColumnData::Bool(Vec::with_capacity(capacity)),
            Type::Enum(_) => ColumnData::Enum(Vec::with_capacity(capacity)),
            Type::Varchar(_) | Type::Text => ColumnData::Dict(Vec::with_capacity(capacity)),
            Type::Uuid
            | Type::Numeric(..)
            | Type::Date
            | Type::Time
            | Type::DateTime
            | Type::Interval
            | Type::Jsonb => ColumnData::Bytes {
                offsets: {
                    let mut offsets = Vec::with_capacity(capacity + 1);
                    offsets.push(0);
                    offsets
                },
                blob: Vec::new(),
            },
        };

        Self {
            nulls: Vec::with_capacity(capacity),
            data,
            zone: ZoneMap::default(),
        }
    }

    fn push(
        &mut self,
        value: Value,
        interned: &mut HashMap<String, u32>,
        dictionary: &mut Vec<String>,
    ) {
        self.zone.observe(&value);
        self.nulls.push(value.is_null() as u8);

        match (&mut self.data, value) {
            (ColumnData::Int(data), Value::Number(n)) => data.push(n),
            (ColumnData::Int(data), Value::Null) => data.push(0),

            (ColumnData::Float(data), Value::Float(f)) => data.push(f),
            (ColumnData::Float(data), Value::Null) => data.push(0.0),

            (ColumnData::Bool(data), Value::Boolean(b)) => data.push(b as u8),
            (ColumnData::Bool(data), Value::Null) => data.push(0),

            (ColumnData::Enum(data), Value::Enum(idx)) => data.push(idx),
            (ColumnData::Enum(data), Value::Null) => data.push(0),

            (ColumnData::Dict(data), Value::String(s)) => {
                let index = match interned.get(&*s) {
                    Some(&index) => index,
                    None => {
                        let index = dictionary.len() as u32;
                        let owned = s.to_string();
                        interned.insert(owned.clone(), index);
                        dictionary.push(owned);
                        index
                    }
                };
                data.push(index);
            }
            (ColumnData::Dict(data), Value::Null) => data.push(0),

            (ColumnData::Bytes { offsets, blob }, value) => {
                if !value.is_null() {
                    let bytes = value.serialise().expect("value serialises");
                    blob.extend_from_slice(&bytes);
                }
                offsets.push(blob.len() as u32);
            }

            (_, value) => panic!("segment build: {value:?} does not fit its column class"),
        }
    }

    fn value(&self, index: usize, dictionary: &[String]) -> Value {
        if self.nulls[index] == 1 {
            return Value::Null;
        }

        match &self.data {
            ColumnData::Int(data) => Value::Number(data[index]),
            ColumnData::Float(data) => Value::Float(data[index]),
            ColumnData::Bool(data) => Value::Boolean(data[index] == 1),
            ColumnData::Enum(data) => Value::Enum(data[index]),
            ColumnData::Dict(data) => {
                Value::String(dictionary[data[index] as usize].as_str().into())
            }
            ColumnData::Bytes { offsets, blob } => {
                let slice = &blob[offsets[index] as usize..offsets[index + 1] as usize];
                let (value, _) = Value::deserialise(slice).expect("frozen value deserialises");
                value
            }
        }
    }

    fn encode(&self, buff: &mut Vec<u8>) {
        buff.push(self.data.class());
        buff.extend_from_slice(&self.nulls);

        match &self.data {
            ColumnData::Int(data) => data
                .iter()
                .for_each(|n| buff.extend_from_slice(&n.to_le_bytes())),
            ColumnData::Float(data) => data
                .iter()
                .for_each(|f| buff.extend_from_slice(&f.to_le_bytes())),
            ColumnData::Bool(data) | ColumnData::Enum(data) => buff.extend_from_slice(data),
            ColumnData::Dict(data) => data
                .iter()
                .for_each(|idx| buff.extend_from_slice(&idx.to_le_bytes())),
            ColumnData::Bytes { offsets, blob } => {
                offsets
                    .iter()
                    .for_each(|offset| buff.extend_from_slice(&offset.to_le_bytes()));
                buff.extend_from_slice(&(blob.len() as u32).to_le_bytes());
                buff.extend_from_slice(blob);
            }
        }

        self.zone.encode(buff);
    }

    fn decode(reader: &mut Reader<'_>, row_count: usize) -> io::Result<Self> {
        let class = reader.u8()?;
        let nulls = reader.bytes(row_count)?.to_vec();

        let data = match class {
            CLASS_INT => {
                let mut data = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    data.push(reader.i128()?);
                }
                ColumnData::Int(data)
            }
            CLASS_FLOAT => {
                let mut data = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    data.push(reader.f64()?);
                }
                ColumnData::Float(data)
            }
            CLASS_BOOL => ColumnData::Bool(reader.bytes(row_count)?.to_vec()),
            CLASS_ENUM => ColumnData::Enum(reader.bytes(row_count)?.to_vec()),
            CLASS_DICT => {
                let mut data = Vec::with_capacity(row_count);
                for _ in 0..row_count {
                    data.push(reader.u32()?);
                }
                ColumnData::Dict(data)
            }
            CLASS_BYTES => {
                let mut offsets = Vec::with_capacity(row_count + 1);
                for _ in 0..=row_count {
                    offsets.push(reader.u32()?);
                }
                let len = reader.u32()? as usize;
                ColumnData::Bytes {
                    offsets,
                    blob: reader.bytes(len)?.to_vec(),
                }
            }
            other => return Err(corrupt(&format!("unknown column class {other}"))),
        };

        Ok(Self {
            nulls,
            data,
            zone: ZoneMap::decode(reader)?,
        })
    }
}

impl ColumnData {
    const fn class(&self) -> u8 {
        match self {
            Self::Int(_) => CLASS_INT,
            Self::Float(_) => CLASS_FLOAT,
            Self::Bool(_) => CLASS_BOOL,
            Self::Enum(_) => CLASS_ENUM,
            Self::Dict(_) => CLASS_DICT,
            Self::Bytes { .. } => CLASS_BYTES,
        }
    }
}

struct Reader<'b> {
    data: &'b [u8],
    position: usize,
}

impl<'b> Reader<'b> {
    const fn new(data: &'b [u8]) -> Self {
        Self { data, position: 0 }
    }

    #[inline]
    fn bytes(&mut self, len: usize) -> io::Result<&'b [u8]> {
        match self.data.len() - self.position >= len {
            true => {
                let slice = &self.data[self.position..self.position + len];
                self.position += len;
                Ok(slice)
            }
            false => Err(corrupt("truncated segment")),
        }
    }

    #[inline]
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    #[inline]
    fn u16(&mut self) -> io::Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    #[inline]
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    #[inline]
    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    #[inline]
    fn i128(&mut self) -> io::Result<i128> {
        Ok(i128::from_le_bytes(self.bytes(16)?.try_into().unwrap()))
    }

    #[inline]
    fn f64(&mut self) -> io::Result<f64> {
        Ok(f64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }
}

fn corrupt(message: &str) -> Error {
    Error::new(ErrorKind::InvalidData, message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::statement::{Column as AstColumn, Constraint};

    fn schema() -> SchemaNew {
        SchemaBuilder::new("frozen").from_ast_columns(&[
            AstColumn {
                name: "id".into(),
                data_type: Type::Integer,
                constraints: vec![Constraint::PrimaryKey],
                type_def: None,
            },
            AstColumn {
                name: "name".into(),
                data_type: Type::Varchar(64),
                constraints: vec![Constraint::Nullable],
                type_def: None,
            },
            AstColumn {
                name: "score".into(),
                data_type: Type::DoublePrecision,
                constraints: vec![Constraint::Nullable],
                type_def: None,
            },
            AstColumn {
                name: "active".into(),
                data_type: Type::Boolean,
                constraints: vec![Constraint::Nullable],
                type_def: None,
            },
            AstColumn {
                name: "balance".into(),
                data_type: Type::Numeric(10, 2),
                constraints: vec![Constraint::Nullable],
                type_def: None,
            },
        ])
    }

    fn rows() -> Vec<(i64, Tuple)> {
        use crate::core::numeric::Numeric;

        vec![
            (
                1,
                vec![
                    1.into(),
                    "alice".into(),
                    9.5f32.into(),
                    Value::Boolean(true),
                    Value::Numeric(Numeric::from(100i64)),
                ],
            ),
            (
                2,
                vec![
                    2.into(),
                    "bob".into(),
                    Value::Null,
                    Value::Boolean(false),
                    Value::Null,
                ],
            ),
            (
                7,
                vec![
                    7.into(),
                    "alice".into(),
                    (-3.25f32).into(),
                    Value::Null,
                    Value::Numeric(Numeric::from(-5i64)),
                ],
            ),
        ]
    }

    #[test]
    fn round_trips_through_bytes() {
        let segment = FrozenSegment::build(&schema(), rows());
        let decoded = FrozenSegment::decode(&segment.encode()).unwrap();

        assert_eq!(decoded.row_ids(), &[1, 2, 7]);
        for (index, (_, tuple)) in rows().into_iter().enumerate() {
            assert_eq!(decoded.tuple(index), tuple);
        }
    }

    #[test]
    fn shares_dictionary_entries() {
        let segment = FrozenSegment::build(&schema(), rows());
        assert_eq!(segment.dictionary, vec!["alice".to_string(), "bob".into()]);
    }

    #[test]
    fn zone_maps_prune_ranges() {
        let segment = FrozenSegment::build(&schema(), rows());

        let id = segment.zone(0);
        assert!(id.may_match(Bound::Included(&Value::Number(2)), Bound::Unbounded));
        assert!(id.may_match(Bound::Unbounded, Bound::Excluded(&Value::Number(2))));
        assert!(!id.may_match(Bound::Included(&Value::Number(8)), Bound::Unbounded));
        assert!(!id.may_match(Bound::Unbounded, Bound::Excluded(&Value::Number(1))));
        assert!(!id.may_match(
            Bound::Excluded(&Value::Number(7)),
            Bound::Included(&Value::Number(10)),
        ));

        let score = segment.zone(2);
        assert_eq!(score.null_count(), 1);
        assert!(score.may_match(
            Bound::Included(&Value::Float(0.0)),
            Bound::Included(&Value::Float(5.0)),
        ));
        assert!(!score.may_match(Bound::Excluded(&Value::Float(9.5)), Bound::Unbounded));
    }

    #[test]
    fn rejects_corruption() {
        let segment = FrozenSegment::build(&schema(), rows());
        let mut bytes = segment.encode();

        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        assert!(FrozenSegment::decode(&bytes).is_err());

        assert!(FrozenSegment::decode(&[0u8; 8]).is_err());
    }

    #[test]
    fn writes_and_reads_file() {
        let dir = std::env::temp_dir().join(format!("umbra_segment_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("000001.seg");

        let segment = FrozenSegment::build(&schema(), rows());
        segment.write(&path).unwrap();

        let loaded = FrozenSegment::read(&path).unwrap();
        assert_eq!(loaded.row_ids(), segment.row_ids());
        assert_eq!(loaded.tuple(2), segment.tuple(2));

        std::fs::remove_dir_all(&dir).unwrap();
    }
}
