use std::{
    io::{self, Read},
    mem,
    str::FromStr,
};

use crate::{
    core::date::{NaiveDateTime, Serialize},
    db::{RowId, Schema},
    sql::statement::Temporal,
};
use crate::{
    core::{
        date::{NaiveDate, NaiveTime, Parse},
        uuid::Uuid,
    },
    sql::statement::{Type, Value},
};

trait ValueSerialize {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type);
}

/// Returns the byte length of a given SQL [`Type`].
pub(crate) const fn byte_len_of_type(data_type: &Type) -> usize {
    match data_type {
        Type::Uuid => 16,
        Type::BigInteger
        | Type::BigSerial
        | Type::UnsignedBigInteger
        | Type::DateTime
        | Type::DoublePrecision => 8,
        Type::Integer | Type::Serial | Type::UnsignedInteger | Type::Date | Type::Real => 4,
        Type::Time => 3,
        Type::SmallInt | Type::SmallSerial | Type::UnsignedSmallInt => 2,
        Type::Boolean => 1,
        _ => panic!("This must be used only for types with length defined at compile time"),
    }
}

/// Returns the length of bytes needed to storage a given `Varchar` [`Type`].
pub(crate) const fn utf_8_length_bytes(max_size: usize) -> usize {
    match max_size {
        0..64 => 1,
        64..16384 => 2,
        _ => 4,
    }
}

/// Returns the size in bytes of the varlena header, given its first byte.
/// If the first byte less than 127: 1-byte header (short string, length is the first byte)
/// If the first byte greater or equal to 128: 4-byte header (long string, length is in the next 3 bytes)
///
/// *Note*: the first byte being equal to 0x7f is reserved and cannot be used.

/// Returns the size in bytes of the varlena header, given its first byte.
/// If the first byte less than 127: 1-byte header (short string, length is the first byte)
/// If the first byte greater or equal to 128: 4-byte header (long string, length is in the next 3 bytes)
///
/// *Note*: the first byte being equal to 0x7f is reserved and cannot be used.
const fn varlena_header_len(byte: u8) -> usize {
    match byte < 0x7f {
        true => 1,
        false => 4,
    }
}

pub(crate) fn deserialize(buff: &[u8], schema: &Schema) -> Vec<Value> {
    read_from(&mut io::Cursor::new(buff), schema).unwrap()
}



pub(crate) fn serialize(r#type: &Type, value: &Value) -> Vec<u8> {
    let mut buff = Vec::new();
    serialize_into(&mut buff, r#type, value);

    buff
}

fn serialize_into(buff: &mut Vec<u8>, r#type: &Type, value: &Value) {
    match value {
        Value::String(string) => match r#type {
            Type::Varchar(max) => {
                if string.as_bytes().len() > u32::MAX as usize {
                    todo!("Strings too long are not supported {}", u32::MAX);
                }

                let b_len = string.as_bytes().len().to_le_bytes();
                let len_prefix = utf_8_length_bytes(*max);

                buff.extend_from_slice(&b_len[..len_prefix]);
                buff.extend_from_slice(string.as_bytes());
            }

            _ => string.serialize(buff, r#type),
        },
        Value::Number(n) => n.serialize(buff, r#type),
        Value::Float(f) => f.serialize(buff, r#type),
        Value::Boolean(b) => b.serialize(buff, r#type),
        Value::Temporal(t) => t.serialize(buff, r#type),
        Value::Uuid(u) => u.serialize(buff, r#type),
        Value::Null => panic!("NULL values cannot be serialised"),
    }
}

pub(crate) fn deserialize_row_id<'value>(buff: &[u8]) -> RowId {
    RowId::from_be_bytes(buff[..mem::size_of::<RowId>()].try_into().unwrap())
}

pub(crate) fn serialize_tuple<'value>(
    schema: &Schema,
    values: impl IntoIterator<Item = &'value Value> + Copy,
) -> Vec<u8> {
    use crate::sql::statement::Column;

    let mut buff = Vec::new();

    debug_assert_eq!(
        schema.len(),
        values.into_iter().count(),
        "Length of schema and values length mismatch"
    );

    let filter: fn(&(&Column, &Value)) -> bool = match schema.has_nullable() {
        true => {
            let bitmap = null_bitmap(schema.len(), values);
            buff.extend_from_slice(&bitmap);

            // Serialize all non-NULL values (both nullable and non-nullable columns)
            |(_, val)| !val.is_null()
        }
        false => |_| true, // no filter needed
    };
    
    schema
        .columns
        .iter()
        .zip(values)
        .filter(filter)
        .for_each(|(col, val)| serialize_into(&mut buff, &col.data_type, val));

    buff
}

fn null_bitmap<'value, V>(schema_length: usize, values: V) -> Vec<u8>
where
    V: IntoIterator<Item = &'value Value>,
{
    let mut bitmap = vec![0u8; (schema_length + 7) / 8];

    values
        .into_iter()
        .enumerate()
        .filter(|(_, value)| value.is_null())
        .for_each(|(idx, _)| bitmap[idx / 8] |= 1 << (idx % 8));

    bitmap
}

// FIXME: use of sorting with different type from schema
// FIXME: use of sorting with different type from schema
#[rustfmt::skip]
pub(crate) fn size_of(tuple: &[Value], schema: &Schema) -> usize {
    let bitmap_size = match schema.has_nullable() {
        true => (schema.columns.len() + 7) / 8,
        _ => 0,
    };

    bitmap_size + schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            // For nullable schemas, skip NULL values (they don't get serialized)
            if schema.has_nullable() && tuple[idx].is_null() {
                return None;
            }
            
            let size = match &tuple[idx] {
                Value::String(string) => match col.data_type{
                    Type::Varchar(max) => utf_8_length_bytes(max) + string.as_bytes().len(),
                    Type::Text => {
                        if tuple[idx].is_null() {
                        return None;
                    }

                    let Value::String(string) = &tuple[idx] else {
                        panic!("Expected data type TEXT but found {}", tuple[idx]);
                    };

                    let len = string.as_bytes().len();
                    if len < 0x7f {
                        1 + len
                    } else {
                        4 + len
                    }
                }
                    _ => byte_len_of_type(&col.data_type),
                },
                _ => byte_len_of_type(&col.data_type)
            };
            
            Some(size)
        })
        .sum::<usize>()
}

pub(crate) fn read_from(reader: &mut impl Read, schema: &Schema) -> io::Result<Vec<Value>> {
    let bitmap = match schema.has_nullable() {
        true => {
            let bitmap_size = (schema.len() + 7) / 8;
            let mut bitmap = vec![0u8; bitmap_size];
            reader.read_exact(&mut bitmap)?;
            Some(bitmap)
        }

        false => None,
    };

    let values = schema.columns.iter().enumerate().map(|(i, col)| {
        if bitmap
            .as_ref()
            .map_or(false, |b| (b[i / 8] & (1 << (i % 8))) != 0)
        {
            // Column is NULL according to bitmap, don't read from stream
            return Ok(Value::Null);
        }

        // Column is not NULL, read its value from stream
        read_value(reader, col)
    });

    values.collect()
}

fn read_value(reader: &mut impl Read, col: &crate::sql::statement::Column) -> io::Result<Value> {
    match &col.data_type {
        Type::Varchar(size) => {
            // Standard VARCHAR deserialization
            let mut len = [0; mem::size_of::<usize>()];
            let prefix = utf_8_length_bytes(*size);

            reader.read_exact(&mut len[..prefix])?;
            let len = usize::from_le_bytes(len);

            let mut string = vec![0; len];
            reader.read_exact(&mut string)?;

            Ok(Value::String(
                String::from_utf8(string).expect("Couldn't parse string from utf8"),
            ))
        }

        Type::Text => {
            let mut header = [0; 4];
            reader.read_exact(&mut header[..1])?;
            let header_len = varlena_header_len(header[0]);

            let length = match header_len == 1 {
                false => {
                    reader.read_exact(&mut header[1..4])?;
                    let be = [0, header[1], header[2], header[3]];
                    u32::from_be_bytes(be) as usize
                }
                _ => header[0] as usize,
            };

            let mut buf = vec![0; length];
            reader.read_exact(&mut buf)?;
            Ok(Value::String(
                String::from_utf8(buf).expect("Couldn't parse TEXT from utf8"),
            ))
        }

        Type::Boolean => {
            let mut byte = [0; byte_len_of_type(&Type::Boolean)];
            reader.read_exact(&mut byte)?;
            Ok(Value::Boolean(byte[0] != 0))
        }

        Type::SmallInt => {
            let mut buf = [0; byte_len_of_type(&Type::SmallInt)];
            reader.read_exact(&mut buf)?;
            let n = i16::from_be_bytes(buf) as i128;
            Ok(Value::Number(n))
        }

        Type::UnsignedSmallInt | Type::SmallSerial => {
            let mut buf = [0; byte_len_of_type(&Type::UnsignedSmallInt)];
            reader.read_exact(&mut buf)?;
            let n = u16::from_be_bytes(buf) as i128;
            Ok(Value::Number(n))
        }

        Type::Integer => {
            let mut buf = [0; byte_len_of_type(&Type::Integer)];
            reader.read_exact(&mut buf)?;
            let n = i32::from_be_bytes(buf) as i128;
            Ok(Value::Number(n))
        }

        Type::UnsignedInteger | Type::Serial => {
            let mut buf = [0; byte_len_of_type(&Type::UnsignedInteger)];
            reader.read_exact(&mut buf)?;
            let n = u32::from_be_bytes(buf) as i128;
            Ok(Value::Number(n))
        }

        Type::BigInteger => {
            let mut buf = [0; byte_len_of_type(&Type::BigInteger)];
            reader.read_exact(&mut buf)?;
            let n = i64::from_be_bytes(buf) as i128;
            Ok(Value::Number(n))
        }

        Type::UnsignedBigInteger | Type::BigSerial => {
            let mut buf = [0; byte_len_of_type(&Type::UnsignedBigInteger)];
            reader.read_exact(&mut buf)?;
            let n = u64::from_be_bytes(buf) as i128;
            Ok(Value::Number(n))
        }

        Type::Uuid => {
            let mut buf = [0; byte_len_of_type(&Type::Uuid)];
            reader.read_exact(&mut buf)?;
            Ok(Value::Uuid(Uuid::from_bytes(buf)))
        }

        Type::Real => {
            let mut bytes = [0; byte_len_of_type(&Type::Real)];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Float(f32::from_be_bytes(bytes).into()))
        }

        Type::DoublePrecision => {
            let mut bytes = [0; byte_len_of_type(&Type::DoublePrecision)];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Float(f64::from_be_bytes(bytes)))
        }

        Type::Date => {
            let mut bytes = [0; byte_len_of_type(&Type::Date)];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Temporal(NaiveDate::try_from(bytes)?.into()))
        }

        Type::Time => {
            let mut bytes = [0; byte_len_of_type(&Type::Time)];
            reader.read_exact(&mut bytes)?;
            Ok(Value::Temporal(NaiveTime::from(bytes).into()))
        }

        Type::DateTime => {
            let mut date_bytes = [0; byte_len_of_type(&Type::Date)];
            reader.read_exact(&mut date_bytes)?;
            let mut time_bytes = [0; byte_len_of_type(&Type::Time)];
            reader.read_exact(&mut time_bytes)?;

            let bytes = (date_bytes, time_bytes);

            Ok(Value::Temporal(NaiveDateTime::try_from(bytes)?.into()))
        }
    }
}

impl ValueSerialize for String {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match to {
            Type::Text => {
                let bytes = self.as_bytes();
                let len = bytes.len();

                match len < 127 {
                    true => buff.push(len as u8),
                    _ => {
                        // 4 byte header: 0x80 + 3 bytes BE length
                        buff.push(0x80);
                        buff.extend_from_slice(&(len as u32).to_be_bytes()[1..]);
                    }
                }

                // then we add the string content itself
                buff.extend_from_slice(bytes);
            }
            Type::Date => NaiveDate::parse_str(self).unwrap().serialize(buff),
            Type::Time => NaiveTime::parse_str(self).unwrap().serialize(buff),
            Type::DateTime => NaiveDateTime::parse_str(self).unwrap().serialize(buff),
            Type::Uuid => buff.extend_from_slice(Uuid::from_str(self).unwrap().as_ref()),
            _ => unreachable!("Unsupported type {to} for String value"),
        }
    }
}

impl ValueSerialize for i128 {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match to {
            typ if typ.is_integer() => {
                let b_len = byte_len_of_type(to);
                let be_bytes = self.to_be_bytes();
                buff.extend_from_slice(&be_bytes[be_bytes.len() - b_len..]);
            }
            typ if typ.is_float() => match typ {
                Type::Real => buff.extend_from_slice(&(*self as f32).to_be_bytes()),
                Type::DoublePrecision => buff.extend_from_slice(&(*self as f64).to_be_bytes()),
                _ => unreachable!("What kind of float is this?"),
            },
            _ => unreachable!("Unsupported type {to} for i128 value"),
        }
    }
}

impl ValueSerialize for f64 {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match to {
            typ if typ.is_float() => match typ {
                Type::Real => buff.extend_from_slice(&(*self as f32).to_be_bytes()),
                Type::DoublePrecision => buff.extend_from_slice(&self.to_be_bytes()),
                _ => unreachable!("What kind of float is this?"),
            },
            typ if typ.is_integer() => {
                let value = *self as i128;
                value.serialize(buff, typ);
            }
            _ => unreachable!("Unsupported type {to} for f64 value"),
        }
    }
}

impl ValueSerialize for Temporal {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match (to, self) {
            (Type::Date, Temporal::Date(date)) => date.serialize(buff),
            (Type::Time, Temporal::Time(time)) => time.serialize(buff),
            (Type::DateTime, Temporal::DateTime(datetime)) => datetime.serialize(buff),
            (Type::DoublePrecision, temporal) => match temporal {
                Temporal::Date(date) => date.serialize(buff),
                Temporal::DateTime(datetime) => datetime.serialize(buff),
                Temporal::Time(time) => time.serialize(buff),
            },
            _ => unreachable!("Unsupported type {to} for Temporal {self}"),
        }
    }
}

impl ValueSerialize for Uuid {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match to {
            Type::Uuid => buff.extend_from_slice(self.as_ref()),
            _ => unreachable!("Unsupported type {to} for UUID"),
        }
    }
}

impl ValueSerialize for bool {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match to {
            Type::Boolean => buff.push(u8::from(*self)),
            _ => unreachable!("This ain't a boolean"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::sql::statement::Column;

    use super::*;

    fn round_trip(r#type: Type, value: &Value) -> Value {
        let schema = Schema::new(vec![Column::new("col", r#type)]);
        let values = [value];
        
        // Use the proper tuple serialization (with bitmap)
        let buff = serialize_tuple(&schema, values);

        let mut cursor = Cursor::new(buff);
        let mut rows = read_from(&mut cursor, &schema).expect("Failed to read from schema");
        rows.pop().expect("No value inside rows")
    }

    #[test]
    fn test_int_round_trip() {
        let original = Value::Number(69);
        let got = round_trip(Type::Integer, &Value::Number(69));

        assert_eq!(got, original)
    }

    #[test]
    #[should_panic]
    fn test_type_mismatch() {
        round_trip(Type::Varchar(20), &Value::Number(20));
    }

    #[test]
    fn test_bitmap_null_handling() {
        use crate::sql::statement::{Column, Constraint};
        
        // Test with nullable column (should use bitmap format)
        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: Type::Integer,
                constraints: vec![],
            },
            Column {
                name: "name".to_string(),
                data_type: Type::Varchar(50),
                constraints: vec![Constraint::Nullable],
            },
        ]);
        
        let values = [&Value::Number(1), &Value::Null];
        let serialized = serialize_tuple(&schema, values);
        
        println!("Serialized bytes: {:?}", serialized);
        
        let mut cursor = Cursor::new(serialized);
        let deserialized = read_from(&mut cursor, &schema).unwrap();
        
        println!("Deserialized: {:?}", deserialized);
        
        assert_eq!(deserialized.len(), 2);
        assert_eq!(deserialized[0], Value::Number(1));
        assert!(matches!(deserialized[1], Value::Null));
    }

    #[test]
    fn test_backward_compatibility() {
        use crate::sql::statement::Column;
        
        // Test with non-nullable columns (should use original format)
        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: Type::Integer,
                constraints: vec![],
            },
            Column {
                name: "name".to_string(),
                data_type: Type::Varchar(50),
                constraints: vec![],
            },
        ]);
        
        let values = [&Value::Number(1), &Value::String("hello".to_string())];
        let serialized = serialize_tuple(&schema, values);
        
        let mut cursor = Cursor::new(serialized);
        let deserialized = read_from(&mut cursor, &schema).unwrap();
        
        assert_eq!(deserialized[0], Value::Number(1));
        assert_eq!(deserialized[1], Value::String("hello".to_string()));
    }

    #[test]
    fn test_text_varlena_roundtrip() {
        let cases: Vec<String> = vec![
            "".into(),  // empty
            "a".into(), // 1 byte
            "x".repeat(126),
            "y".repeat(127),
            "日本語".into(), // multibyte
            "z".repeat(1024),
        ];

        for s in cases {
            let schema = Schema::new(vec![Column::new("t", Type::Text)]);
            let string_val = Value::String(s.clone());
            let values = [&string_val];
            
            let buf = serialize_tuple(&schema, values);

            let mut cursor = Cursor::new(&buf);
            let row = read_from(&mut cursor, &schema).unwrap();

            assert_eq!(
                row[0],
                Value::String(s.clone()),
                "Failed roundtrip for text: {s:?}",
            );
        }
    }

    #[test]
    fn test_bitmap_comparator_logic() {
        use crate::core::storage::btree::{BTreeKeyCmp, BitmapFixedSizeCmp, BytesCmp};
        use crate::sql::statement::{Column, Constraint};
        
        // Create a schema with nullable columns (should use bitmap format)
        let schema = Schema::new(vec![
            Column {
                name: "id".to_string(),
                data_type: Type::Integer,
                constraints: vec![],
            },
            Column {
                name: "name".to_string(),
                data_type: Type::Varchar(50),
                constraints: vec![Constraint::Nullable],
            },
        ]);
        
        // Create two tuples with different primary keys
        let tuple1 = [&Value::Number(1), &Value::String("Alice".to_string())];
        let tuple2 = [&Value::Number(2), &Value::String("Bob".to_string())];
        let tuple3 = [&Value::Number(3), &Value::Null]; // NULL case
        
        // Serialize all tuples (should use bitmap format)
        let serialized1 = serialize_tuple(&schema, tuple1);
        let serialized2 = serialize_tuple(&schema, tuple2);
        let serialized3 = serialize_tuple(&schema, tuple3);
        
        println!("Tuple1 serialized: {:?}", serialized1);
        println!("Tuple2 serialized: {:?}", serialized2);
        println!("Tuple3 serialized: {:?}", serialized3);
        
        // Create bitmap comparator
        let bitmap_size = (schema.columns.len() + 7) / 8; // Should be 1 byte for 2 columns
        let pk_size = 4; // Integer is 4 bytes
        let comparator = BTreeKeyCmp::BitmapMemCmp(BitmapFixedSizeCmp {
            bitmap_size,
            pk_size,
        });
        
        // Test comparisons
        let cmp_1_2 = comparator.cmp(&serialized1, &serialized2);
        let cmp_2_3 = comparator.cmp(&serialized2, &serialized3);
        let cmp_1_3 = comparator.cmp(&serialized1, &serialized3);
        
        println!("Comparison 1 vs 2: {:?}", cmp_1_2);
        println!("Comparison 2 vs 3: {:?}", cmp_2_3);
        println!("Comparison 1 vs 3: {:?}", cmp_1_3);
        
        // Should be Less since 1 < 2 < 3
        assert_eq!(cmp_1_2, std::cmp::Ordering::Less);
        assert_eq!(cmp_2_3, std::cmp::Ordering::Less);
        assert_eq!(cmp_1_3, std::cmp::Ordering::Less);
    }
}
