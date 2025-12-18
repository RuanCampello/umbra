use std::str::FromStr;

use error::Error as JsonError;
use jsonb::Jsonb;

use crate::{
    core::json::jsonb::{parse_error, JsonHeader},
    sql::statement::Value,
};

mod cache;
pub mod error;
mod jsonb;
mod ops;
mod path;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Conv {
    Strict,
    NotStrict,
    ToString,
}

pub type Result<T> = std::result::Result<T, JsonError>;

pub fn from_value_to_jsonb(value: &Value, strict: Conv) -> Result<Jsonb> {
    match value {
        Value::String(string) => {
            let result = match matches!(strict, Conv::Strict) {
                true => Jsonb::from_str_with_mode(&string, strict),
                _ => {
                    let mut str = string.replace('\\', "\\\\").replace('"', "\\\"");
                    str.insert(0, '"');
                    str.push('"');

                    Jsonb::from_str(&str)
                }
            };

            result.map_err(|_| parse_error("Malformed JSON", None))
        }
        Value::Null => Ok(Jsonb::from_data(JsonHeader::null().into_bytes().as_bytes())),
        Value::Number(int) => {
            Jsonb::from_str(&int.to_string()).map_err(|_| parse_error("Malformed JSON", None))
        }
        Value::Float(float) => match float.is_infinite() {
            true => {
                let json = match float.is_sign_negative() {
                    true => "-9.0e+999",
                    _ => "9.0e+999",
                };

                Jsonb::from_str(json).map_err(|_| parse_error("Malformed JSON", None))
            }
            _ => Jsonb::from_str(float.to_string().as_str())
                .map_err(|_| parse_error("Malformed JSON", None)),
        },
        _ => todo!(),
    }
}
