//! SQL mathematical logics in Rust.
//! Sometimes, you need to implement `sqrt` from scratch, to make it work.

use super::expression::{TypeError, VmType};
use crate::{db::SqlError, sql::statement::Value};

#[inline(always)]
pub(super) const fn abs(value: &Value) -> Result<Value, SqlError<2>> {
    match value {
        Value::Float(f) => Ok(Value::Float(f.abs())),
        Value::Number(n) => Ok(Value::Number(n.abs())),
        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: [VmType::Number, VmType::Float],
        })),
    }
}
