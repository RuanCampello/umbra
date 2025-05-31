use std::{array::TryFromSliceError, fmt::Display, io::empty, num::TryFromIntError};

use crate::{
    core::storage::tuple,
    db::{DatabaseError, QuerySet},
    sql::statement::{Column, Type, Value},
};

#[derive(Debug, PartialEq)]
pub enum Response {
    QuerySet(QuerySet),
    Empty(usize),
    Err(String),
}

#[derive(Debug, PartialEq)]
pub enum EncodingError {
    IntConversion(TryFromIntError),
    SliceConversion(String),
    InvalidPrefix(u8),
    InvalidType(u8),
}

const BOOLEAN_CATEGORY: u8 = 0x00;
const INTEGER_CATEGORY: u8 = 0x10;
// const FLOAT_CATEGORY: u8    = 0x30;
const STRING_CATEGORY: u8 = 0x40;
const TEMPORAL_CATEGORY: u8 = 0x50;

pub fn serialize(content: &Response) -> Result<Vec<u8>, EncodingError> {
    let mut packed: Vec<u8> = Vec::from(0u32.to_le_bytes());

    match content {
        Response::QuerySet(query_set) => {
            packed.push(b'+');
            packed.extend_from_slice(&(u16::try_from(query_set.schema.len())?).to_le_bytes());

            for column in &query_set.schema.columns {
                packed.extend_from_slice(&(u16::try_from(column.name.len())?).to_le_bytes());
                packed.extend_from_slice(column.name.as_bytes());

                packed.push(u8::from(&column.data_type));
                if let Type::Varchar(length) = column.data_type {
                    packed.extend_from_slice(&(length as u32).to_le_bytes());
                }
            }

            packed.extend_from_slice(&(u32::try_from(query_set.tuples.len())?).to_le_bytes());
            query_set.tuples.iter().for_each(|tuple| {
                packed.extend_from_slice(&tuple::serialize_tuple(&query_set.schema, tuple))
            });
        }
        Response::Empty(affected_rows) => {
            packed.push(b'!');
            packed.extend_from_slice(&(u32::try_from(*affected_rows)?).to_le_bytes());
        }
        Response::Err(err) => {
            packed.push(b'-');
            packed.extend_from_slice(err.as_bytes());
        }
    }

    let content_len = u32::try_from(packed[4..].len())?;
    packed[..4].copy_from_slice(&content_len.to_le_bytes());

    Ok(packed)
}

pub fn deserialize(content: &[u8]) -> Result<Response, EncodingError> {
    Ok(match content[0] {
        b'+' => {
            let mut query_set = QuerySet::empty();
            let mut cursor = 1;

            let schema_len = u16::from_le_bytes(content[cursor..cursor + 2].try_into()?);
            cursor += 2;

            for _ in 0..schema_len {
                let name_len = u16::from_le_bytes(content[cursor..cursor + 2].try_into()?) as usize;
                cursor += 2;

                let name =
                    String::from_utf8(Vec::from(&content[cursor..cursor + name_len])).unwrap();
                cursor += name_len;

                let data_type = match content[cursor] {
                    STRING_CATEGORY | 0x0 => {
                        let mut buff = [0; 4];
                        buff.copy_from_slice(&content[cursor + 1..cursor + 5]);
                        let max_chars = u32::from_le_bytes(buff) as usize;
                        cursor += 4;
                        Type::Varchar(max_chars)
                    }

                    content => {
                        Type::try_from(&content).map_err(|_| EncodingError::InvalidType(content))?
                    }
                };
                cursor += 1;

                query_set.schema.push(Column::new(&name, data_type));
            }

            let num_tuples = u32::from_le_bytes(content[cursor..cursor + 4].try_into()?);
            cursor += 4;

            (0..num_tuples).for_each(|_| {
                let tuple = tuple::deserialize(&content[cursor..], &query_set.schema);
                cursor += tuple::size_of(&tuple, &query_set.schema);
                query_set.tuples.push(tuple);
            });

            Response::QuerySet(query_set)
        }
        b'!' => {
            let affected_rows = u32::from_le_bytes(content[1..].try_into()?) as usize;
            Response::Empty(affected_rows)
        }
        b'-' => Response::Err(String::from_utf8(Vec::from(&content[1..])).unwrap()),
        prefix => Err(EncodingError::InvalidPrefix(prefix))?,
    })
}

impl Display for EncodingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IntConversion(int_conversion) => write!(f, "{int_conversion}"),
            Self::SliceConversion(slice_conversion) => write!(f, "{slice_conversion}"),
            Self::InvalidPrefix(prefix) => write!(f, "Invalid ASCII prefix: {prefix}"),
            Self::InvalidType(typ) => write!(f, "Invalid data type: {typ}"),
        }
    }
}

// we could just do a const fn here but :/
impl From<&Type> for u8 {
    fn from(r#type: &Type) -> Self {
        match r#type {
            Type::Boolean => BOOLEAN_CATEGORY | 0x0,

            Type::SmallInt => INTEGER_CATEGORY | 0x0,
            Type::UnsignedSmallInt => INTEGER_CATEGORY | 0x1,
            Type::Integer => INTEGER_CATEGORY | 0x2,
            Type::UnsignedInteger => INTEGER_CATEGORY | 0x3,
            Type::BigInteger => INTEGER_CATEGORY | 0x4,
            Type::UnsignedBigInteger => INTEGER_CATEGORY | 0x5,
            Type::SmallSerial => INTEGER_CATEGORY | 0x06,
            Type::Serial => INTEGER_CATEGORY | 0x07,
            Type::BigSerial => INTEGER_CATEGORY | 0x08,

            Type::Varchar(_) => STRING_CATEGORY | 0x0,

            Type::Date => TEMPORAL_CATEGORY | 0x0,
            Type::Time => TEMPORAL_CATEGORY | 0x1,
            Type::DateTime => TEMPORAL_CATEGORY | 0x2,
        }
    }
}

impl TryFrom<&u8> for Type {
    type Error = ();

    fn try_from(byte: &u8) -> Result<Self, Self::Error> {
        let cat = byte & 0xF0;
        let sub = byte & 0x0F;

        match (cat, sub) {
            (BOOLEAN_CATEGORY, 0x0) => Ok(Type::Boolean),

            (INTEGER_CATEGORY, 0x0) => Ok(Type::SmallInt),
            (INTEGER_CATEGORY, 0x1) => Ok(Type::UnsignedSmallInt),
            (INTEGER_CATEGORY, 0x2) => Ok(Type::Integer),
            (INTEGER_CATEGORY, 0x3) => Ok(Type::UnsignedInteger),
            (INTEGER_CATEGORY, 0x4) => Ok(Type::BigInteger),
            (INTEGER_CATEGORY, 0x5) => Ok(Type::UnsignedBigInteger),
            (INTEGER_CATEGORY, 0x6) => Ok(Type::SmallSerial),
            (INTEGER_CATEGORY, 0x7) => Ok(Type::Serial),
            (INTEGER_CATEGORY, 0x8) => Ok(Type::BigSerial),

            (TEMPORAL_CATEGORY, 0x0) => Ok(Type::Date),
            (TEMPORAL_CATEGORY, 0x1) => Ok(Type::Time),
            (TEMPORAL_CATEGORY, 0x2) => Ok(Type::DateTime),

            _ => Err(()),
        }
    }
}

impl From<Result<QuerySet, DatabaseError>> for Response {
    fn from(value: Result<QuerySet, DatabaseError>) -> Self {
        match value {
            Ok(empty) if empty.schema.columns.is_empty() => {
                let affected_rows = match empty.tuples.first().map(|r#type| r#type.as_slice()) {
                    Some([Value::Number(number)]) => *number as usize,
                    _ => empty.tuples.len(),
                };

                Response::Empty(affected_rows)
            }

            Ok(query_set) => Response::QuerySet(query_set),
            Err(err) => Response::Err(err.to_string()),
        }
    }
}

impl From<TryFromIntError> for EncodingError {
    fn from(value: TryFromIntError) -> Self {
        Self::IntConversion(value)
    }
}

impl From<TryFromSliceError> for EncodingError {
    fn from(value: TryFromSliceError) -> Self {
        Self::SliceConversion(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{QuerySet, Schema},
        sql::statement::{Column, Type, Value},
    };

    use super::*;

    #[test]
    fn serialize_and_deserialize() -> Result<(), EncodingError> {
        let content = QuerySet {
            schema: Schema::new(vec![
                Column::new("item_id", Type::BigSerial),
                Column::new("name", Type::Varchar(255)),
                Column::new("darkness_level", Type::UnsignedSmallInt),
                Column::new("souls_count", Type::UnsignedBigInteger),
            ]),
            tuples: vec![
                vec![
                    Value::Number(1),
                    Value::String("Sword of Darkness".to_string()),
                    Value::Number(42),
                    Value::Number(1000),
                ],
                vec![
                    Value::Number(2),
                    Value::String("Shield of Light".to_string()),
                    Value::Number(5),
                    Value::Number(500),
                ],
                vec![
                    Value::Number(3),
                    Value::String("Amulet of Shadows".to_string()),
                    Value::Number(87),
                    Value::Number(2500),
                ],
            ],
        };

        let response = Response::QuerySet(content);
        let packet = serialize(&response)?;

        assert_eq!(deserialize(&packet[4..])?, response);

        Ok(())
    }

    #[test]
    fn serialize_empty() -> Result<(), EncodingError> {
        let empty = QuerySet::empty();
        let response = Response::from(Ok(empty));
        Ok(())
    }
}
