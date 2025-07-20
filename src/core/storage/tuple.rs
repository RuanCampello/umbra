use std::{
    io::{self, Read},
    mem,
    str::FromStr,
};

use crate::{
    core::date::{NaiveDateTime, Serialize},
    db::{RowId, Schema},
};
use crate::{
    core::{
        date::{NaiveDate, NaiveTime, Parse},
        uuid::Uuid,
    },
    sql::statement::{Type, Value},
};

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
pub(in crate::core::storage) const fn utf_8_length_bytes(max_size: usize) -> usize {
    match max_size {
        0..64 => 1,
        64..16384 => 2,
        _ => 4,
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
    match (r#type, value) {
        (Type::Varchar(max), Value::String(string)) => {
            if string.as_bytes().len() > u32::MAX as usize {
                todo!("Strings too long are not supported {}", u32::MAX);
            }

            let b_len = string.as_bytes().len().to_le_bytes();
            let len_prefix = utf_8_length_bytes(*max);

            buff.extend_from_slice(&b_len[..len_prefix]);
            buff.extend_from_slice(string.as_bytes());
        }
        (Type::Boolean, Value::Boolean(bool)) => buff.push(u8::from(*bool)),
        (int, Value::Number(num)) if int.is_integer() => {
            let b_len = byte_len_of_type(int);
            let be_bytes = num.to_be_bytes();
            buff.extend_from_slice(&be_bytes[be_bytes.len() - b_len..]);
        }
        (float, Value::Float(num)) if float.is_float() => match float {
            Type::Real => buff.extend_from_slice(&(*num as f32).to_be_bytes()),
            Type::DoublePrecision => buff.extend_from_slice(&num.to_be_bytes()),
            _ => unreachable!("what kind of float is this?"),
        },
        (Type::Date, Value::String(date)) => NaiveDate::parse_str(date).unwrap().serialize(buff),
        (Type::Time, Value::String(time)) => NaiveTime::parse_str(time).unwrap().serialize(buff),
        (Type::DateTime, Value::String(datetime)) => {
            NaiveDateTime::parse_str(datetime).unwrap().serialize(buff)
        }
        (Type::Uuid, Value::Uuid(bytes)) => buff.extend_from_slice(bytes.as_ref()),
        (Type::Uuid, Value::String(uuid)) => {
            let uuid = Uuid::from_str(uuid).unwrap();
            buff.extend_from_slice(uuid.as_ref())
        }

        _ => unimplemented!("Tried to call serialize from {value} into {type}"),
    }
}

pub(crate) fn deserialize_row_id<'value>(buff: &[u8]) -> RowId {
    RowId::from_be_bytes(buff[..mem::size_of::<RowId>()].try_into().unwrap())
}

pub(crate) fn serialize_tuple<'value>(
    schema: &Schema,
    values: impl IntoIterator<Item = &'value Value> + Copy,
) -> Vec<u8> {
    let mut buff = Vec::new();

    debug_assert_eq!(
        schema.len(),
        values.into_iter().count(),
        "Lenght of schema and values length mismatch"
    );

    schema
        .columns
        .iter()
        .zip(values)
        .for_each(|(col, val)| serialize_into(&mut buff, &col.data_type, val));

    buff
}

// FIXME: use of sorting with different type from schema
pub(crate) fn size_of(tuple: &[Value], schema: &Schema) -> usize {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| match col.data_type {
            Type::Boolean => 1,
            Type::Varchar(max) => {
                let Value::String(string) = &tuple[idx] else {
                    panic!(
                        "Expected data type {} but found {}",
                        Type::Varchar(max),
                        tuple[idx]
                    );
                };

                utf_8_length_bytes(max) + string.as_bytes().len()
            }
            other => byte_len_of_type(&other),
        })
        .sum()
}

pub(crate) fn read_from(reader: &mut impl Read, schema: &Schema) -> io::Result<Vec<Value>> {
    let values = schema.columns.iter().map(|col| match &col.data_type {
        Type::Varchar(size) => {
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
    });

    values.collect()
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
}
