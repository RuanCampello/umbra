use crate::{
    core::json::{
        jsonb::{ElementType, JsonHeader, parse_error},
        path::json_path,
    },
    sql::statement::Value,
};
use error::Error as JsonError;
pub use jsonb::Jsonb;
use std::str::FromStr;

pub mod error;
mod jsonb;
mod path;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Conv {
    Strict,
    /// text that does not parse as JSON is stored as a JSON string
    NotStrict,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum OutputFlag {
    ElementType,
    Binary,
}

pub type Result<T> = std::result::Result<T, JsonError>;

pub fn get_path(value: &Value, path: &str, flag: OutputFlag) -> Result<Value> {
    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    let json = from_value_to_jsonb(value, Conv::Strict)?;
    let path = json_path(path)?;
    let json = match json.lookup(&path) {
        Ok(Some(element)) => Jsonb::from_data(element),
        _ => return Ok(Value::Null),
    };
    let element_type = match json.element_type() {
        Ok(t) => t,
        Err(_) => return Ok(Value::Null),
    };

    let flag = match element_type {
        ElementType::ARRAY | ElementType::OBJECT => OutputFlag::Binary,
        _ => flag,
    };

    from_json_to_value(json, element_type, flag)
}

#[inline(always)]
pub fn from_value_to_jsonb(value: &Value, strict: Conv) -> Result<Jsonb> {
    match value {
        Value::String(string) => match strict {
            Conv::Strict => Jsonb::from_str(string),
            Conv::NotStrict => Jsonb::from_str(string).or(Jsonb::from_str(&escape_string(string))),
        },
        Value::Null => Ok(Jsonb::from_data(JsonHeader::null().into_bytes().as_bytes())),
        Value::Number(int) => {
            Jsonb::from_str(&int.to_string()).map_err(|_| parse_error("Malformed JSON", None))
        },
        Value::Float(float) => match float.is_infinite() {
            true => {
                let json = match float.is_sign_negative() {
                    true => "-9.0e+999",
                    _ => "9.0e+999",
                };

                Jsonb::from_str(json).map_err(|_| parse_error("Malformed JSON", None))
            },
            _ => Jsonb::from_str(float.to_string().as_str())
                .map_err(|_| parse_error("Malformed JSON", None)),
        },
        Value::Boolean(b) => Jsonb::from_str(match *b {
            true => "true",
            _ => "false",
        }),
        Value::Blob(blob) => {
            let index = blob
                .iter()
                .position(|&b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
                .unwrap_or(blob.len());
            let slice = &blob[index..];

            let json = match slice {
                [b'"', ..] | [b'-', ..] | [b'0'..=b'2', ..] => slice_to_json(slice)?,
                _ => match JsonHeader::from_slice(0, slice) {
                    Ok((header, offset)) => {
                        let size = header.payload_size();
                        let expected = size + offset;

                        match expected != slice.len() {
                            true => slice_to_json(slice)?,
                            _ => {
                                let json = Jsonb::from_data(slice);
                                let is_valid = match expected <= 7 {
                                    true => json.is_valid(),
                                    _ => json.element_type().is_ok(),
                                };

                                match is_valid {
                                    true => json,
                                    _ => slice_to_json(slice)?,
                                }
                            },
                        }
                    },
                    _ => slice_to_json(slice)?,
                },
            };

            json.element_type()?;
            Ok(json)
        },
        other => Err(parse_error(&format!("{other} is not JSON"), None)),
    }
}

pub(crate) fn from_json_to_value(
    json: Jsonb,
    element_type: ElementType,
    flag: OutputFlag,
) -> Result<Value> {
    use ElementType::*;
    if matches!(flag, OutputFlag::Binary) {
        return Ok(Value::Blob(json.data().into()));
    }

    let mut string = json.to_string();

    Ok(match element_type {
        ARRAY | OBJECT => Value::String(string.into()),
        TEXT | TEXT5 | TEXTJ | TEXTRAW => {
            string.remove(string.len() - 1);
            string.remove(0);

            Value::String(string.into())
        },
        INT | INT5 => match i64::from_str(&string) {
            Ok(int) => Value::Number(int as i64),
            _ => f64::from_str(&string).map(Value::Float).unwrap_or(Value::Null),
        },
        NULL => Value::Null,
        TRUE => Value::Boolean(true),
        FALSE => Value::Boolean(false),
        FLOAT | FLOAT5 => f64::from_str(&string).map(Value::Float).unwrap_or(Value::Null),
        _ => unreachable!(),
    })
}

fn slice_to_json(slice: &[u8]) -> Result<Jsonb> {
    let zero = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
    let str =
        std::str::from_utf8(&slice[..zero]).map_err(|_| parse_error("Malformed JSON", None))?;

    Jsonb::from_str(str)
}

fn escape_string(str: &str) -> String {
    let mut str = str.replace('\\', "\\\\").replace('"', "\\\"");
    str.insert(0, '"');
    str.push('"');
    str
}
