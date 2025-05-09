use std::{
    io::{self, Read},
    mem,
};

use crate::{
    core::db::Schema,
    sql::statement::{Type, Value},
};

/// Returns the byte length of a given SQL [`Type`].
pub(in crate::core::storage) fn byte_len_of_type(data_type: &Type) -> usize {
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
        _ => unimplemented!("Tried to call serialize from {value} into {type}"),
    }
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
        .zip(values.into_iter())
        .for_each(|(col, val)| serialize_into(&mut buff, &col.data_type, val));

    todo!()
}

fn read_from(reader: &mut impl Read, schema: &Schema) -> io::Result<Vec<Value>> {
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

        date @ (Type::Date | Type::Time | Type::DateTime) => {
            todo!()
        }
    });

    values.collect()
}
