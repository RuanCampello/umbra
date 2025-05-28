use std::{
    io::{self, Read},
    mem,
};

use crate::{
    core::date::{NaiveDate, NaiveTime, Parse},
    sql::statement::{Type, Value},
};
use crate::{
    core::date::{NaiveDateTime, Serialize},
    db::{RowId, Schema},
};

/// Returns the byte length of a given SQL [`Type`].
pub(crate) fn byte_len_of_type(data_type: &Type) -> usize {
    match data_type {
        Type::BigInteger | Type::UnsignedBigInteger | Type::DateTime => 8,
        Type::Integer | Type::UnsignedInteger | Type::Date => 4,
        Type::Time => 3, // TODO: this can be 4, but let it happen
        Type::Boolean => 1,
        _ => unreachable!("This must only be used for integers."),
    }
}

/// Returns the length of bytes needed to storage a given `Varchar` [`Type`].
pub(in crate::core::storage) fn utf_8_length_bytes(max_size: usize) -> usize {
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
        (
            int @ (Type::Integer
            | Type::BigInteger
            | Type::UnsignedInteger
            | Type::UnsignedBigInteger),
            Value::Number(num),
        ) => {
            let b_len = byte_len_of_type(int);
            let endian_b = num.to_be_bytes();
            buff.extend_from_slice(&endian_b[endian_b.len() - b_len..]);
        }
        // TODO: checkout why this values are not coming already with the date parsed
        (Type::Date, Value::String(date)) => NaiveDate::parse_str(date).unwrap().serialize(buff),
        (Type::Time, Value::String(time)) => NaiveTime::parse_str(time).unwrap().serialize(buff),
        (Type::DateTime, Value::String(datetime)) => {
            NaiveDateTime::parse_str(datetime).unwrap().serialize(buff)
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

pub(crate) fn size_of(tuple: &[Value], schema: &Schema) -> usize {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| match col.data_type.clone() {
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
            let mut byte = [0];
            reader.read_exact(&mut byte)?;
            Ok(Value::Boolean(byte[0] != 0))
        }

        int @ (Type::Integer
        | Type::UnsignedInteger
        | Type::BigInteger
        | Type::UnsignedBigInteger) => {
            let len = byte_len_of_type(int);
            let mut big_endian = [0; mem::size_of::<i128>()];

            let start = mem::size_of::<i128>() - len;
            reader.read_exact(&mut big_endian[start..])?;

            if big_endian[start] & 0x80 != 0 && matches!(int, Type::BigInteger | Type::Integer) {
                big_endian[..start].fill(u8::MAX);
            }

            Ok(Value::Number(i128::from_be_bytes(big_endian)))
        }

        date @ (Type::Date | Type::Time | Type::DateTime) => match date {
            Type::Date => {
                let mut bytes = [0; 4];
                reader.read_exact(&mut bytes)?;
                Ok(Value::Temporal(NaiveDate::try_from(bytes)?.into()))
            }
            Type::Time => {
                let mut bytes = [0; 3];
                reader.read_exact(&mut bytes)?;
                Ok(Value::Temporal(NaiveTime::from(bytes).into()))
            }
            Type::DateTime => {
                let mut date_bytes = [0; 4];
                reader.read_exact(&mut date_bytes)?;
                let mut time_bytes = [0; 3];
                reader.read_exact(&mut time_bytes)?;

                let bytes = (date_bytes, time_bytes);

                Ok(Value::Temporal(NaiveDateTime::try_from(bytes)?.into()))
            }

            _ => todo!(),
        },
    });

    values.collect()
}
