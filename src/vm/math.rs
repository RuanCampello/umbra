//! SQL mathematical logics in Rust.
//! Sometimes, you need to implement `sqrt` from scratch, to make it work.

use super::expression::{TypeError, VmType};
use crate::{db::SqlError, sql::statement::Value};

#[repr(i8)]
#[derive(Debug, PartialEq)]
enum Sign {
    Negative = -1,
    Neutral,
    Positive,
}

trait Signed: Sized {
    fn sign(&self) -> Sign;
}

impl Signed for i128 {
    #[inline(always)]
    fn sign(&self) -> Sign {
        match self.gt(&0) {
            true => Sign::Positive,
            _ => match self.eq(&0) {
                true => Sign::Neutral,
                false => Sign::Negative,
            },
        }
    }
}

impl Signed for f64 {
    #[inline(always)]
    fn sign(&self) -> Sign {
        match self {
            x if x.is_nan() => Sign::Neutral,
            x if *x == 0.0 && x.is_sign_negative() => Sign::Negative,
            x if *x > 0.0 => Sign::Positive,
            x if *x < 0.0 => Sign::Negative,
            _ => Sign::Neutral,
        }
    }
}

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

#[inline(always)]
pub(super) fn power(base: &Value, expoent: &Value) -> Result<Value, SqlError<2>> {
    match (base, expoent) {
        (Value::Float(b), Value::Float(e)) => Ok(Value::Float(b.powf(*e))),
        (Value::Number(b), Value::Number(e)) => Ok(Value::Number(b.pow(*e as u32))),
        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: [VmType::Number, VmType::Float],
        })),
    }
}

#[inline(always)]
pub(super) fn trunc(value: &Value, decimals: Option<&Value>) -> Result<Value, SqlError<1>> {
    use crate::sql::statement::Expression;

    match (value, decimals) {
        (Value::Float(value), None) => Ok(value.trunc().into()),
        (Value::Float(value), Some(decimals)) => match decimals {
            Value::Number(decimals) => {
                let factor = 10f64.powi(*decimals as i32);
                Ok(((value * factor).trunc() / factor).into())
            }
            _ => {
                return Err(SqlError::Type(TypeError::ExpectedType {
                    expected: VmType::Number,
                    found: Expression::Value(decimals.clone()),
                }))
            }
        },
        _ => Err(TypeError::ExpectedType {
            expected: VmType::Float,
            found: Expression::Value(value.clone()),
        }
        .into()),
    }
}

#[inline(always)]
pub(super) fn sign(value: &Value) -> Result<Value, SqlError<2>> {
    match value {
        Value::Number(n) => Ok(Value::Number(n.sign() as i128)),
        Value::Float(f) => Ok(Value::Number(f.sign() as i128)),
        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: [VmType::Float, VmType::Number],
        })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::SqlError,
        sql::statement::{Expression, Value},
    };

    #[test]
    fn test_abs() -> Result<(), SqlError<2>> {
        assert_eq!(Value::Number(24), abs(&Value::Number(-24))?);
        assert_eq!(Value::Number(69), abs(&Value::Number(69))?);
        assert_eq!(Value::Float(f64::INFINITY), abs(&f64::NEG_INFINITY.into())?);
        assert_ne!(Value::Float(f64::NAN), abs(&f64::NAN.into())?);

        Ok(())
    }

    #[test]
    fn test_power() -> Result<(), SqlError<2>> {
        assert_eq!(Value::Number(729), power(&9.into(), &3.into())?);
        assert_eq!(Value::Number(1), power(&i128::MAX.into(), &0.into())?);
        assert!(matches!(power(&f64::NAN.into(), &2.0.into())?, Value::Float(x) if x.is_nan()));
        assert_eq!(Value::Float(1.0), power(&0.0.into(), &0.0.into())?);
        assert_eq!(
            Value::Float(f64::INFINITY),
            power(&0.0.into(), &(-1.0).into())?
        );
        assert!(matches!(power(&(-2.0).into(), &0.5.into())?, Value::Float(x) if x.is_nan()));
        assert_eq!(
            Value::Float(f64::INFINITY),
            power(&2.0.into(), &1024.0.into())?
        );

        Ok(())
    }

    #[test]
    fn test_trunc() -> Result<(), SqlError<1>> {
        use std::f64::consts::PI;

        assert_eq!(Value::Float(3f64), trunc(&PI.into(), None)?);
        assert_eq!(Value::Float(3.14), trunc(&PI.into(), Some(&2.into()))?);

        assert_eq!(
            Err(TypeError::ExpectedType {
                expected: VmType::Number,
                found: Expression::Value(3.5.into())
            }
            .into()),
            trunc(&PI.into(), Some(&3.5.into()))
        );
        assert_eq!(
            Err(TypeError::ExpectedType {
                expected: VmType::Number,
                found: Expression::Value(3.into())
            }
            .into()),
            trunc(&3.into(), None)
        );

        assert!(matches!(trunc(&f64::NAN.into(), None)?, Value::Float(x) if x.is_nan()));
        assert_eq!(
            Value::Float(f64::INFINITY),
            trunc(&f64::INFINITY.into(), None)?
        );
        assert_eq!(
            Value::Float(f64::NEG_INFINITY),
            trunc(&f64::NEG_INFINITY.into(), None)?
        );
        assert_eq!(Value::Float(0.0), trunc(&0.0.into(), None)?);
        assert_eq!(
            Value::Float(123456789.0),
            trunc(&123456789.12345.into(), None)?
        );

        Ok(())
    }

    #[test]
    fn test_signum() {
        assert_eq!(Sign::Positive, i128::MAX.sign());
        assert_eq!(Sign::Neutral, 0.sign());
        assert_eq!(Sign::Negative, i128::MIN.sign());

        assert_eq!(Sign::Positive, f64::MAX.sign());
        assert_eq!(Sign::Neutral, 0.00000f64.sign());
        assert_eq!(Sign::Positive, 0.00001f64.sign());
        assert_eq!(Sign::Negative, f64::MIN.sign());

        assert_eq!(Sign::Neutral, f64::NAN.sign());
        assert_eq!(Sign::Positive, f64::INFINITY.sign());
        assert_eq!(Sign::Negative, f64::NEG_INFINITY.sign());
    }
}
