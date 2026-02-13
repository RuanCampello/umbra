use crate::{
    core::{
        date::{interval::Interval, NaiveDate, NaiveDateTime, NaiveTime, Parse},
        json::{self, Conv, Jsonb},
        numeric::Numeric,
        uuid::Uuid,
        Serialize,
    },
    db::{RowId, Schema},
    sql::statement::{Column, Temporal, Type, Value},
};
use std::{
    io::{self, Read},
    mem,
    str::FromStr,
};

// TODO: we are going to move this logic to the mvcc

trait ValueSerialize {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type);
}

/// Returns the byte length of a given SQL [`Type`].
#[inline(always)]
pub(crate) const fn byte_len_of_type(data_type: &Type) -> usize {
    match data_type {
        Type::Uuid | Type::Interval => 16,
        Type::BigInteger
        | Type::BigSerial
        | Type::UnsignedBigInteger
        | Type::DateTime
        | Type::DoublePrecision => 8,
        Type::Integer | Type::Serial | Type::UnsignedInteger | Type::Date | Type::Real => 4,
        Type::Time => 3,
        Type::SmallInt | Type::SmallSerial | Type::UnsignedSmallInt => 2,
        Type::Boolean | Type::Enum(_) => 1,
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
const fn varlena_header_len(byte: u8) -> usize {
    match byte < 0x7f {
        true => 1,
        false => 4,
    }
}

pub(crate) fn deserialize(buff: &[u8], schema: &Schema) -> Vec<Value> {
    let cursor = &mut io::Cursor::new(buff);
    read_from(cursor, schema).unwrap()
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
                if string.len() > u32::MAX as usize {
                    todo!("Strings too long are not supported {}", u32::MAX);
                }

                let b_len = string.len().to_le_bytes();
                let len_prefix = utf_8_length_bytes(*max);

                buff.extend_from_slice(&b_len[..len_prefix]);
                buff.extend_from_slice(string.as_bytes());
            }

            Type::Jsonb => {
                let json = json::from_value_to_jsonb(value, Conv::NotStrict)
                    .expect("JSON serialisation should not fail");
                serialize_into(buff, r#type, &Value::Blob(json.data()))
            }

            _ => string.serialize(buff, r#type),
        },
        Value::Boolean(_) | Value::Float(_) | Value::Number(_) if matches!(r#type, Type::Jsonb) => {
            let json = json::from_value_to_jsonb(value, Conv::Strict).unwrap();
            serialize_into(buff, r#type, &Value::Blob(json.data()))
        }
        Value::Number(n) => n.serialize(buff, r#type),
        Value::Float(f) => f.serialize(buff, r#type),
        Value::Boolean(b) => b.serialize(buff, r#type),
        Value::Temporal(t) => t.serialize(buff, r#type),
        Value::Uuid(u) => u.serialize(buff, r#type),
        Value::Interval(i) => ValueSerialize::serialize(i, buff, r#type),
        Value::Numeric(n) => n.serialize(buff),
        Value::Enum(idx) => buff.push(*idx),
        Value::Blob(blob) => match r#type {
            Type::Jsonb => {
                let len = blob.len();
                match len < 127 {
                    true => buff.push(len as u8),
                    false => {
                        buff.push(0x80);
                        buff.extend_from_slice(&(len as u32).to_be_bytes()[1..]);
                    }
                }

                buff.extend_from_slice(blob);
            }
            _ => unreachable!("Unsupported type {} for Blob value", r#type),
        },
        Value::Null => panic!("NULL values cannot be serialised"),
    }
}

pub(crate) fn deserialize_row_id(buff: &[u8]) -> RowId {
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
        "Lenght of schema and values length mismatch"
    );

    let filter: fn(&(&Column, &Value)) -> bool = match schema.has_nullable() {
        true => {
            let bitmap = null_bitmap(schema.len(), values);
            buff.extend_from_slice(&bitmap);

            |(_, value)| !value.is_null()
        }
        false => |_| true, // no filter needed
    };
    schema
        .columns
        .iter()
        .zip(values)
        .filter(filter)
        .for_each(|(col, val)| match (col.data_type, val) {
            (Type::Enum(id), Value::String(s)) => {
                let variants = schema
                    .get_enum(id)
                    .or(col.type_def.as_ref())
                    .unwrap_or_else(|| panic!("Enum variants not found in schema for id: {id}"));
                let idx = variants.iter().position(|v| v == s).unwrap_or_else(|| {
                    panic!(
                        "Invalid enum variant: '{s}'. Expected one of: {:?}",
                        variants.join(", ")
                    )
                }) as u8;

                buff.push(idx)
            }

            _ => serialize_into(&mut buff, &col.data_type, val),
        });

    buff
}

// FIXME: use of sorting with different type from schema
pub(crate) fn size_of(tuple: &[Value], schema: &Schema) -> usize {
    let bitmap_size = match schema.has_nullable() {
        true => schema.columns.len().div_ceil(8),
        _ => 0,
    };

    bitmap_size
        + schema
            .columns
            .iter()
            .enumerate()
            .filter_map(|(idx, col)| {
                if tuple[idx].is_null() {
                    return None;
                }
                // special handling for Jsonb to ensure we use the correct serialisation logic
                if matches!(col.data_type, Type::Jsonb) {
                    let json = json::from_value_to_jsonb(&tuple[idx], Conv::NotStrict).unwrap();
                    let len = json.len();
                    let header_len = if len < 127 { 1 } else { 4 };
                    return Some(header_len + len);
                }

                Some(match &tuple[idx] {
                    Value::String(str) => match col.data_type {
                        Type::Varchar(max) => utf_8_length_bytes(max) + str.len(),
                        Type::Text => {
                            let len = str.len();
                            let header_len = if len < 127 { 1 } else { 4 };
                            header_len + len
                        }
                        _ => byte_len_of_type(&col.data_type),
                    },
                    Value::Numeric(num) => num.size(),
                    Value::Blob(blob) => {
                        let len = blob.len();
                        let header_len = if len < 127 { 1 } else { 4 };
                        header_len + len
                    }
                    Value::Float(_) | Value::Number(_)
                        if matches!(col.data_type, Type::Numeric(_, _)) =>
                    {
                        let num = match &tuple[idx] {
                            Value::Float(f) => Numeric::try_from(*f).unwrap(),
                            Value::Number(n) => Numeric::from(*n),
                            _ => unsafe { std::hint::unreachable_unchecked() },
                        };
                        num.size()
                    }
                    _ => match &col.data_type {
                        Type::Text => {
                            let len = tuple[idx].to_string().len();
                            match len < 127 {
                                true => 1 + len,
                                _ => 4 + len,
                            }
                        }
                        typ => byte_len_of_type(typ),
                    },
                })
            })
            .sum::<usize>()
}

pub(crate) fn read_from(reader: &mut impl Read, schema: &Schema) -> io::Result<Vec<Value>> {
    let bitmap = match schema.has_nullable() {
        true => {
            let bitmap_size = schema.null_bitmap_len();
            let mut bitmap = vec![0u8; bitmap_size];
            reader.read_exact(&mut bitmap)?;
            Some(bitmap)
        }

        false => None,
    };

    let values = schema.columns.iter().enumerate().map(|(i, col)| {
        if bitmap
            .as_ref()
            .is_some_and(|b| (b[i / 8] & (1 << (i % 8))) != 0)
        {
            return Ok(Value::Null);
        }

        read_value(reader, col)
    });

    values.collect()
}

fn read_varlena(reader: &mut impl Read) -> io::Result<(usize, Vec<u8>)> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header[0..1])?;
    let header_len = varlena_header_len(header[0]);

    let length = match header_len == 1 {
        false => {
            reader.read_exact(&mut header[1..4])?;
            let be = [0, header[1], header[2], header[3]];
            u32::from_be_bytes(be) as usize
        }
        true => header[0] as usize,
    };

    let mut buf = vec![0; length];
    reader.read_exact(&mut buf)?;
    Ok((length, buf))
}

fn read_value(reader: &mut impl Read, col: &Column) -> io::Result<Value> {
    match col.data_type {
        Type::Varchar(size) => {
            let mut len = [0; mem::size_of::<usize>()];
            let prefix = utf_8_length_bytes(size);

            reader.read_exact(&mut len[..prefix])?;
            let len = usize::from_le_bytes(len);

            let mut string = vec![0; len];
            reader.read_exact(&mut string)?;

            Ok(Value::String(
                String::from_utf8(string).expect("Couldn't parse string from utf8"),
            ))
        }

        Type::Text => {
            let (_, buf) = read_varlena(reader)?;
            Ok(Value::String(
                String::from_utf8(buf).expect("Couldn't parse TEXT from utf8"),
            ))
        }

        Type::Jsonb => {
            use std::io::{Error, ErrorKind};
            let (_, buf) = read_varlena(reader)?;

            let json = Jsonb::from_data(&buf);
            let element_type = json
                .element_type()
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

            json::from_json_to_value(json, element_type, json::OutputFlag::ElementType)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))
        }

        Type::Numeric(_, _) => {
            let mut header = [0; 8];
            reader.read_exact(&mut header)?;

            let tag = (u64::from_le_bytes(header) >> 62) & 0b11;
            match tag {
                0b00 | 0b10 => Ok(Value::Numeric(Numeric::from(header))),
                0b01 => {
                    let mut meta = [0; 6];
                    reader.read_exact(&mut meta)?;

                    let len = u16::from_le_bytes(meta[0..2].try_into().unwrap()) as usize;
                    let weight = i16::from_le_bytes(meta[2..4].try_into().unwrap());
                    let sign_dscale = u16::from_le_bytes(meta[4..6].try_into().unwrap());

                    let mut digits_bytes = vec![0; len * 2];
                    reader.read_exact(&mut digits_bytes)?;

                    let mut digits = Vec::with_capacity(len);
                    for chunk in digits_bytes.chunks_exact(2) {
                        digits.push(i16::from_le_bytes(chunk.try_into().unwrap()));
                    }

                    Ok(Value::Numeric(Numeric::Long {
                        weight,
                        digits,
                        sign_dscale,
                    }))
                }
                _ => unreachable!("Invalid numeric tag: {tag}"),
            }
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

        Type::Interval => {
            let mut bytes = [0; byte_len_of_type(&Type::Interval)];
            reader.read_exact(&mut bytes)?;

            let months = i32::from_le_bytes(bytes[0..4].try_into().unwrap());
            let days = i32::from_le_bytes(bytes[4..8].try_into().unwrap());
            let microseconds = i64::from_le_bytes(bytes[8..16].try_into().unwrap());

            Ok(Value::Interval(Interval::new(months, days, microseconds)))
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

        Type::Enum(_) => {
            let mut byte = [0; 1];
            reader.read_exact(&mut byte)?;
            Ok(Value::Enum(byte[0]))
        }
    }
}

fn null_bitmap<'value, V>(schema_length: usize, values: V) -> Vec<u8>
where
    V: IntoIterator<Item = &'value Value>,
{
    let size = schema_length.div_ceil(8);
    let mut bitmap = vec![0u8; size];

    values
        .into_iter()
        .enumerate()
        .filter(|(_, value)| value.is_null())
        .for_each(|(idx, _)| bitmap[idx / 8] |= 1 << (idx % 8));

    bitmap
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
            Type::Interval => {
                ValueSerialize::serialize(&Interval::from_str(self).unwrap(), buff, &Type::Interval)
            }
            Type::Uuid => buff.extend_from_slice(Uuid::from_str(self).unwrap().as_ref()),
            Type::Enum(_) => unimplemented!(),
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
            Type::Numeric(_, _) => Numeric::from(*self).serialize(buff),
            Type::Text | Type::Jsonb => {
                // Serialize number as varlena for JSONB field extraction
                let s = self.to_string();
                let bytes = s.as_bytes();
                if bytes.len() < 127 {
                    buff.push(bytes.len() as u8);
                } else {
                    buff.push(0x80);
                    buff.extend_from_slice(&(bytes.len() as u32).to_be_bytes()[1..]);
                }
                buff.extend_from_slice(bytes);
            }
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
            Type::Numeric(_, _) => Numeric::try_from(*self)
                .expect("Invalid float for numeric conversion")
                .serialize(buff),
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

impl ValueSerialize for Interval {
    fn serialize(&self, buff: &mut Vec<u8>, to: &Type) {
        match to {
            Type::Interval => {
                buff.extend_from_slice(&self.months.to_le_bytes());
                buff.extend_from_slice(&self.days.to_le_bytes());
                buff.extend_from_slice(&self.microseconds.to_le_bytes());
            }
            _ => unreachable!("Unsupported type {to} for Interval"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::sql::statement::Column;

    use super::*;

    fn round_trip(r#type: Type, value: &Value) -> Value {
        let mut buff = Vec::new();
        serialize_into(&mut buff, &r#type, value);

        let mut cursor = Cursor::new(buff);
        let schema = Schema::new(vec![Column::new("col", r#type)]);

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
            let mut buf = Vec::new();
            serialize_into(&mut buf, &Type::Text, &Value::String(s.to_string()));

            if s.len() < 0x7f {
                assert_eq!(buf[0], s.len() as u8);
            } else {
                assert_eq!(buf[0], 0x80);
                let len = s.len() as u32;
                let be = len.to_be_bytes();
                assert_eq!(&buf[1..4], &be[1..]);
            }

            let mut cursor = Cursor::new(&buf);
            let schema = Schema::new(vec![Column::new("t", Type::Text)]);
            let row = read_from(&mut cursor, &schema).unwrap();

            assert_eq!(
                row[0],
                Value::String(s.clone()),
                "Failed roundtrip for text: {s:?}",
            );
        }
    }
}
