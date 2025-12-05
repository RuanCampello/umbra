//! SQL mathematical runtime logics in Rust.
//! Sometimes, you need to implement `sqrt` from scratch, to make it work.

#![allow(dead_code)]

use super::expression::{TypeError, VmError, VmType};
use crate::{
    core::numeric::{Numeric, NumericError},
    db::SqlError,
    sql::statement::Value,
};
use std::{ops::Div, str::FromStr, sync::OnceLock};

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

trait SquaredRoot: Sized + PartialEq + PartialOrd + std::ops::Add<Self> {
    const EPSILON: f64 = f64::EPSILON;
    fn sqrt(&self) -> Result<Self, SqlError>;
}

static NUMERIC_TWO: OnceLock<Numeric> = OnceLock::new();
static NUMERIC_HALF: OnceLock<Numeric> = OnceLock::new();

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

impl SquaredRoot for u128 {
    #[inline(always)]
    fn sqrt(&self) -> Result<Self, SqlError> {
        if self.le(&2u128) {
            return Ok(*self);
        }

        let mut num = *self;
        let mut res = 0u128;
        let mut bit = 1u128 << 126;

        while bit.gt(self) {
            bit >>= 2;
        }

        while bit.ne(&0u128) {
            match num.ge(&(res + bit)) {
                true => {
                    num -= res + bit;
                    res = (res >> 1) + bit;
                }
                false => res >>= 1,
            }

            bit >>= 2;
        }

        Ok(res)
    }
}

impl SquaredRoot for f64 {
    #[inline(always)]
    fn sqrt(&self) -> Result<Self, SqlError> {
        if self.sign().eq(&Sign::Negative) {
            return Err(SqlError::Vm(VmError::NegativeNumSqrt));
        }

        if self.eq(&0f64) || self.eq(&1f64) {
            return Ok(*self);
        }

        if self.is_sign_positive() && self.is_infinite() {
            return Ok(f64::INFINITY);
        }

        if self.is_nan() {
            return Ok(f64::NAN);
        }

        let mut guess: f64 = self / 2f64;
        let mut prev = 0f64;
        while (guess - prev).abs() > Self::EPSILON {
            prev = guess;
            guess = (guess + self / guess) / 2f64;
        }

        Ok(guess)
    }
}

impl SquaredRoot for Numeric {
    #[inline(always)]
    fn sqrt(&self) -> Result<Self, SqlError> {
        if self.is_negative() {
            return Err(TypeError::NumericError(NumericError::InvalidFormat).into());
        }

        if self.is_zero() {
            return Ok(Self::zero());
        }

        if self.is_nan() {
            return Ok(Self::NaN);
        }

        let two = NUMERIC_TWO.get_or_init(|| Numeric::from(2u64));
        let half = NUMERIC_HALF.get_or_init(|| Numeric::from_str("0.5").unwrap());
        let mut x = self / two;

        for _ in 0..20 {
            let next = half * &(&x + (self / &x));
            if x == next {
                break;
            }

            x = next;
        }

        Ok(x)
    }
}

#[inline(always)]
pub(super) fn abs(value: &Value) -> Result<Value, SqlError> {
    match value {
        Value::Float(f) => Ok(Value::Float(f.abs())),
        Value::Number(n) => Ok(Value::Number(n.abs())),
        Value::Numeric(n) => Ok(Value::Numeric(n.abs())),
        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: vec![VmType::Number, VmType::Numeric, VmType::Float],
        })),
    }
}

#[inline(always)]
pub(super) fn power(base: &Value, expoent: &Value) -> Result<Value, SqlError> {
    match (base, expoent) {
        (Value::Float(b), Value::Float(e)) => Ok(Value::Float(b.powf(*e))),
        (Value::Float(b), Value::Number(e)) => Ok(Value::Float(b.powi(*e as _))),
        (Value::Number(b), Value::Number(e)) => Ok(Value::Number(b.pow(*e as _))),
        (Value::Numeric(b), Value::Numeric(e)) => {
            Ok(Value::Numeric(b.pow(e).expect("Negative numeric sqrt")))
        }

        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: vec![VmType::Number, VmType::Numeric, VmType::Float],
        })),
    }
}

#[inline(always)]
pub(super) fn trunc(value: &Value, decimals: Option<Value>) -> Result<Value, SqlError> {
    use crate::sql::statement::Expression;

    match (value, decimals) {
        (Value::Float(value), None) => Ok(value.trunc().into()),
        (Value::Float(value), Some(decimals)) => match decimals {
            Value::Number(decimals) => {
                let factor = 10f64.powi(decimals as i32);
                Ok(value.mul_add(factor, 0f64).trunc().div(factor).into())
            }
            _ => {
                return Err(SqlError::Type(TypeError::ExpectedType {
                    expected: VmType::Number,
                    found: Expression::Value(decimals.clone()),
                }));
            }
        },
        (Value::Numeric(value), decimals) => {
            let decimals = match decimals {
                Some(Value::Number(d)) => d as i32,
                None => 0,
                Some(v) => {
                    return Err(SqlError::Type(TypeError::ExpectedType {
                        expected: VmType::Number,
                        found: Expression::Value(v),
                    }));
                }
            };

            Ok(Value::Numeric(value.trunc(decimals)))
        }
        _ => Err(TypeError::ExpectedType {
            expected: VmType::Float,
            found: Expression::Value(value.clone()),
        }
        .into()),
    }
}

#[inline(always)]
pub(super) fn sign(value: &Value) -> Result<Value, SqlError> {
    match value {
        Value::Number(n) => Ok(Value::Number(n.sign() as i128)),
        Value::Float(f) => Ok(Value::Number(f.sign() as i128)),
        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: vec![VmType::Float, VmType::Number],
        })),
    }
}

#[inline(always)]
pub(super) fn sqrt(value: &Value) -> Result<Value, SqlError> {
    match value {
        Value::Number(n) => {
            let n = u128::try_from(*n).or(Err(SqlError::Vm(VmError::NegativeNumSqrt)))?;
            let sqrt = n.sqrt()?;
            Ok(Value::Number(sqrt as i128))
        }
        Value::Float(f) => Ok(Value::Float(f.sqrt()?)),
        Value::Numeric(n) => Ok(Value::Numeric(n.sqrt().expect("Negative num sqrt"))),
        _ => Err(SqlError::Type(TypeError::ExpectedOneOfTypes {
            expected: vec![VmType::Float, VmType::Number],
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
    fn test_abs() -> Result<(), SqlError> {
        assert_eq!(Value::Number(24), abs(&Value::Number(-24))?);
        assert_eq!(Value::Number(69), abs(&Value::Number(69))?);
        assert_eq!(Value::Float(f64::INFINITY), abs(&f64::NEG_INFINITY.into())?);
        assert_ne!(Value::Float(f64::NAN), abs(&f64::NAN.into())?);

        Ok(())
    }

    #[test]
    fn test_power() -> Result<(), SqlError> {
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
    fn test_trunc() -> Result<(), SqlError> {
        use std::f64::consts::PI;

        assert_eq!(Value::Float(3f64), trunc(&PI.into(), None)?);
        assert_eq!(Value::Float(3.14), trunc(&PI.into(), Some(2.into()))?);

        assert_eq!(
            Err(TypeError::ExpectedType {
                expected: VmType::Number,
                found: Expression::Value(3.5.into())
            }
            .into()),
            trunc(&PI.into(), Some(3.5.into()))
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

    #[test]
    fn test_integer_sqrt() {
        assert_eq!(Value::Number(4), sqrt(&Value::Number(16)).unwrap());
        assert_eq!(Value::Number(10), sqrt(&Value::Number(100)).unwrap());

        assert_eq!(Value::Number(5), sqrt(&Value::Number(27)).unwrap());
        assert_eq!(Value::Number(3), sqrt(&Value::Number(10)).unwrap());

        assert_eq!(Value::Number(5), sqrt(&27i128.into()).unwrap());
        assert_eq!(Value::Number(0), sqrt(&Value::Number(0)).unwrap());

        assert_eq!(
            SqlError::Vm(VmError::NegativeNumSqrt),
            sqrt(&Value::Number(-1)).unwrap_err()
        );
    }

    #[test]
    fn test_float_sqrt() {
        assert_eq!(
            Value::Float(5.196152422706632),
            sqrt(&27f64.into()).unwrap()
        );
        assert_eq!(Value::Float(4.0), sqrt(&Value::Float(16.0)).unwrap());
        assert_eq!(Value::Float(0f64), sqrt(&0f64.into()).unwrap());

        assert_eq!(
            Value::Float(f64::INFINITY),
            sqrt(&f64::INFINITY.into()).unwrap()
        );
        assert_eq!(
            SqlError::Vm(VmError::NegativeNumSqrt),
            sqrt(&f64::NEG_INFINITY.into()).unwrap_err()
        );

        assert_eq!(
            SqlError::Type(TypeError::ExpectedOneOfTypes {
                expected: vec![VmType::Number, VmType::Float],
            }),
            sqrt(&Value::Boolean(true)).unwrap_err()
        );
    }
}
