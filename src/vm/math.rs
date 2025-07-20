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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::SqlError, sql::statement::Value};

    #[test]
    fn test_abs() -> Result<(), SqlError<2>> {
        assert_eq!(Value::Number(24), abs(&Value::Number(-24))?);
        assert_eq!(Value::Number(69), abs(&Value::Number(69))?);
        assert_eq!(
            Value::Float(f64::INFINITY),
            abs(&Value::Float(f64::NEG_INFINITY))?
        );
        assert_ne!(Value::Float(f64::NAN), abs(&Value::Float(f64::NAN))?);

        Ok(())
    }
}
