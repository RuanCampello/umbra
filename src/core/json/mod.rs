use error::Error as JsonError;

use crate::sql::statement::Value;

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

pub fn from_value_to_jsonb(value: &Value, strict: Conv) -> crate::Result<jsonb::Jsonb> {
    todo!()
}
