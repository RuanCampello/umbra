//! SQL Value types and binary serialisation
//!
//! This module defines the core runtime value types used throughout the database,
//! including [Value] for storing column data and [Temporal] for date/time values
//!
//! # Binary Serialisation Format
//!
//! Values are serialised in a self-describing format that doesn't require schema
//! information to decode. The format is designed for:
//! - **Compactness**: Integers use variable-width encoding (smallest that fits)
//! - **Expandability**: Tags 11-255 are reserved for future types
//! - **Performance**: Inline hints and direct byte operations
//!
//! Format: `[tag: 1 byte] [sub-tag if needed: 1 byte] [payload: N bytes]`
//!
//! ┌─────┬──────────┬────────┬───────────────────┐
//! │ Tag │ Type     │ SubTag │ Payload           │
//! ├─────┼──────────┼────────┼───────────────────┤
//! │ 0   │ Null     │ —      │ 0 bytes           │
//! │ 1   │ Boolean  │ —      │ 1 byte            │
//! │ 2   │ Number   │ size   │ 2/4/8/16 bytes LE │
//! │ 3   │ Float    │ —      │ 8 bytes (f64 LE)  │
//! │ 4   │ String   │ —      │ u32 len + bytes   │
//! │ 5   │ Temporal │ 0/1/2  │ 4/3/7 bytes       │
//! │ 6   │ Uuid     │ —      │ 16 bytes          │
//! │ 7   │ Interval │ —      │ 16 bytes          │
//! │ 8   │ Numeric  │ —      │ variable          │
//! │ 9   │ Enum     │ —      │ 1 byte            │
//! │ 10  │ Blob     │ —      │ u32 len + bytes   │
//! │ 11+ │ Reserved │ —      │ future use        │
//! └─────┴──────────┴────────┴───────────────────┘

use super::statement::UnaryOperator;
use crate::core::Serialize;
use crate::core::date::interval::Interval;
use crate::core::date::{DateParseError, NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::core::numeric::Numeric;
use crate::core::uuid::Uuid;
use crate::db::SqlError;
use crate::vm::expression::{TypeError, VmError};
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::io::{self, Error, ErrorKind};
use std::ops::Neg;
use std::sync::Arc;

/// Type tags for binary serialisation format
/// Tags 11-255 are reserved for future types
mod tags {
    pub const NULL: u8 = 0;
    pub const BOOLEAN: u8 = 1;
    pub const NUMBER: u8 = 2;
    pub const FLOAT: u8 = 3;
    pub const STRING: u8 = 4;
    pub const TEMPORAL: u8 = 5;
    pub const UUID: u8 = 6;
    pub const INTERVAL: u8 = 7;
    pub const NUMERIC: u8 = 8;
    pub const ENUM: u8 = 9;
    pub const BLOB: u8 = 10;
    // Tags 11-255 reserved for future types
}

/// Temporal sub-tags for Date/Time/DateTime discrimination
mod temporal_tags {
    pub const DATE: u8 = 0;
    pub const TIME: u8 = 1;
    pub const DATETIME: u8 = 2;
}

/// Number size hints for variable-width integer encoding
mod number_sizes {
    pub const I16: u8 = 2;
    pub const I32: u8 = 4;
    pub const I64: u8 = 8;
    pub const I128: u8 = 16;
}

/// A runtime value stored in a table row or returned in a query
///
/// This enum represents a *concrete value* associated with a column,
/// typically used in query results, expression evaluation, and row serialisation
///
/// For example:
/// - A row with a `VARCHAR` column might store `Value::String("hello".into())`.
/// - A `DATE` column would store `Value::Temporal(Temporal::Date(...))`.
///
/// `Value` is the counterpart to `Type`:
/// - `Type` defines what *kind* of value is allowed
/// - `Value` holds the *actual* data
///
/// This separation allows the system to validate values against schema definitions at runtime
///
/// ## Equality semantics
///
/// `Value` implements [`PartialEq`] for deep equality:
/// - Two values are equal only if they are the same variant and hold exactly equal contents
///   For example, `Value::Number(5) == Value::Number(5)` is true,
///   but `Value::Number(5) == Value::Float(5.0)` is also true, because they can be coerced
///   losslessly from integer to float.
///
/// This enable coercing comparisons (e.g., treating `Number` and `Float` as "same kind")
/// during query planning.
///
/// ```rust
/// use umbra::sql::Value;
///
/// assert_eq!(Value::Number(5), Value::Float(5.0));
/// assert_ne!(Value::Number(5), Value::Float(5.1));
/// assert!(Value::Number(5).eq(&Value::Float(5.0))); // both numeric kinds and lossless coercible
/// ```
#[derive(Debug, Clone)]
pub enum Value {
    String(Arc<str>),
    Number(i64),
    Float(f64),
    Boolean(bool),
    Temporal(Temporal),
    Uuid(Uuid),
    Interval(Interval),
    Numeric(Numeric),
    Enum(u8),
    Blob(Arc<[u8]>),
    Null,
}

/// Date/Time related types
///
/// This enum wraps actual values of date/time types, such as a specific calendar date or a time of day
/// It distinguishes between `DATE`, `TIME`, and `TIMESTAMP` at the value level
///
/// Values of this type are stored in the `Value::Temporal` variant
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum Temporal {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

pub trait Coerce: Sized {
    fn coerce_to(self, other: Self) -> (Self, Self);
}

/// Literally "NULL" in hex
const NULL_HASH: u32 = 0x4E554C4C;

// PERFORMANCE: every row holds one per column and every index entry one per key, a payload wider than 16 bytes belongs behind a pointer
const _: () = assert!(size_of::<Value>() == 24);

impl Value {
    #[inline]
    pub(crate) const fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
    /// The rank of a value's kind in the order across kinds, blobs and `NULL` come last.
    const fn class(&self) -> u8 {
        match self {
            Value::Boolean(_) => 0,
            Value::Number(_) | Value::Float(_) | Value::Numeric(_) => 1,
            Value::Temporal(_) => 2,
            Value::Interval(_) => 3,
            Value::Uuid(_) => 4,
            Value::Enum(_) => 5,
            Value::String(_) => 6,
            Value::Blob(_) => 7,
            Value::Null => 8,
        }
    }

    pub const fn is_zero(&self) -> bool {
        match self {
            Value::Number(n) if *n == 0 => true,
            Value::Float(f) if *f == 0.0 => true,
            Value::Numeric(n) if n.is_zero() => true,
            _ => false,
        }
    }

    #[inline]
    pub const fn is_compatible(&self, other: &Self) -> bool {
        use self::Value::*;

        matches!(
            (self, other),
            (Float(_) | Number(_) | Numeric(_), Float(_) | Number(_) | Numeric(_))
                | (Temporal(_), Interval(_))
                | (Interval(_), Temporal(_))
        )
    }

    /// Returns the type tag for binary serialisation.
    #[inline]
    const fn tag(&self) -> u8 {
        match self {
            Value::Null => tags::NULL,
            Value::Boolean(_) => tags::BOOLEAN,
            Value::Number(_) => tags::NUMBER,
            Value::Float(_) => tags::FLOAT,
            Value::String(_) => tags::STRING,
            Value::Temporal(_) => tags::TEMPORAL,
            Value::Uuid(_) => tags::UUID,
            Value::Interval(_) => tags::INTERVAL,
            Value::Numeric(_) => tags::NUMERIC,
            Value::Enum(_) => tags::ENUM,
            Value::Blob(_) => tags::BLOB,
        }
    }

    /// serialise a value to a self-describing binary format
    #[inline]
    pub fn serialise(&self) -> io::Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(self.serialised_size_hint());
        self.serialise_into(&mut buf);
        Ok(buf)
    }

    /// appends the self-delimiting encoding of the value to `buf`
    #[inline]
    pub fn serialise_into(&self, buf: &mut Vec<u8>) {
        buf.push(self.tag());

        match self {
            Value::Null => {},
            Value::Boolean(b) => {
                buf.push(match *b {
                    true => 1,
                    _ => 0,
                });
            },

            Value::Number(n) => {
                const I16_MIN: i64 = i16::MIN as _;
                const I16_MAX: i64 = i16::MAX as _;
                const I32_MAX: i64 = i32::MAX as _;
                const I32_MIN: i64 = i32::MIN as _;

                match *n {
                    I16_MIN..=I16_MAX => {
                        buf.push(number_sizes::I16);
                        buf.extend_from_slice(&(*n as i16).to_le_bytes());
                    },

                    I32_MIN..=I32_MAX => {
                        buf.push(number_sizes::I32);
                        buf.extend_from_slice(&(*n as i32).to_le_bytes());
                    },

                    _ => {
                        buf.push(number_sizes::I64);
                        buf.extend_from_slice(&n.to_le_bytes());
                    },
                }
            },
            Value::Float(f) => buf.extend_from_slice(&f.to_le_bytes()),
            Value::String(s) => {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            },
            Value::Temporal(t) => match t {
                Temporal::Date(d) => {
                    buf.push(temporal_tags::DATE);
                    d.serialize(buf);
                },
                Temporal::Time(t) => {
                    buf.push(temporal_tags::TIME);
                    t.serialize(buf);
                },
                Temporal::DateTime(dt) => {
                    buf.push(temporal_tags::DATETIME);
                    dt.serialize(buf);
                },
            },

            Value::Uuid(u) => buf.extend_from_slice(u.as_ref()),
            Value::Interval(i) => i.serialize(buf),
            Value::Numeric(n) => n.serialize(buf),
            Value::Enum(e) => buf.push(*e),
            Value::Blob(b) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            },
        }
    }

    /// deserialise a value from binary format
    #[inline]
    pub fn deserialise(data: &[u8]) -> io::Result<(Self, usize)> {
        if data.is_empty() {
            return Err(Error::new(ErrorKind::InvalidData, "Empty data"));
        }

        let tag = data[0];
        let content = &data[1..];

        Ok(match tag {
            tags::NULL => (Value::Null, 1),

            tags::BOOLEAN => {
                if content.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidData, "Missing boolean value"));
                }

                (Value::Boolean(content[0] != 0), 2)
            },

            tags::NUMBER => {
                if content.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidData, "Missing number size"));
                }
                let size = content[0];
                let payload = &content[1..];

                let (value, consumed) = match size {
                    number_sizes::I16 => {
                        if payload.len() < 2 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i16"));
                        }
                        let n = i16::from_le_bytes([payload[0], payload[1]]);
                        (Value::Number(n as i64), 4)
                    },
                    number_sizes::I32 => {
                        if payload.len() < 4 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i32"));
                        }
                        let n = i32::from_le_bytes(payload[..4].try_into().unwrap());
                        (Value::Number(n as i64), 6)
                    },
                    number_sizes::I64 => {
                        if payload.len() < 8 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i64"));
                        }
                        let n = i64::from_le_bytes(payload[..8].try_into().unwrap());
                        (Value::Number(n), 10)
                    },
                    // written before integers were 64-bit, readable while they fit
                    number_sizes::I128 => {
                        if payload.len() < 16 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i128"));
                        }
                        let n = i128::from_le_bytes(payload[..16].try_into().unwrap());
                        let n = i64::try_from(n).map_err(|_| {
                            Error::new(ErrorKind::InvalidData, "Integer out of range")
                        })?;
                        (Value::Number(n), 18)
                    },
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid number size")),
                };

                (value, consumed)
            },

            tags::FLOAT => {
                if content.len() < 8 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated float"));
                }
                let f = f64::from_le_bytes(content[..8].try_into().unwrap());

                (Value::Float(f), 9)
            },

            tags::STRING => {
                if content.len() < 4 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated string length"));
                }
                let len = u32::from_le_bytes(content[..4].try_into().unwrap()) as usize;
                if content.len() < 4 + len {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated string data"));
                }
                let s = String::from_utf8(content[4..4 + len].to_vec())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

                (Value::String(s.into()), 5 + len)
            },

            tags::TEMPORAL => {
                if content.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidData, "Missing temporal sub-tag"));
                }
                let sub_tag = content[0];
                let payload = &content[1..];

                match sub_tag {
                    temporal_tags::DATE => {
                        if payload.len() < 4 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated date"));
                        }
                        let bytes: [u8; 4] = payload[..4].try_into().unwrap();
                        let date = NaiveDate::try_from(bytes)?;

                        (Value::Temporal(Temporal::Date(date)), 6)
                    },
                    temporal_tags::TIME => {
                        if payload.len() < 3 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated time"));
                        }
                        let bytes: [u8; 3] = payload[..3].try_into().unwrap();
                        let time = NaiveTime::from(bytes);

                        (Value::Temporal(Temporal::Time(time)), 5)
                    },
                    temporal_tags::DATETIME => {
                        if payload.len() < 7 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated datetime"));
                        }
                        let date_bytes: [u8; 4] = payload[..4].try_into().unwrap();
                        let time_bytes: [u8; 3] = payload[4..7].try_into().unwrap();
                        let dt = NaiveDateTime::try_from((date_bytes, time_bytes))?;

                        (Value::Temporal(Temporal::DateTime(dt)), 9)
                    },
                    _ => {
                        return Err(Error::new(ErrorKind::InvalidData, "Invalid temporal sub-tag"));
                    },
                }
            },

            tags::UUID => {
                if content.len() < 16 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated UUID"));
                }
                let bytes: [u8; _] = content[..16].try_into().unwrap();
                (Value::Uuid(Uuid::from_bytes(bytes)), 17)
            },

            tags::INTERVAL => {
                if content.len() < 16 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated interval"));
                }
                let months = i32::from_le_bytes(content[..4].try_into().unwrap());
                let days = i32::from_le_bytes(content[4..8].try_into().unwrap());
                let microseconds = i64::from_le_bytes(content[8..16].try_into().unwrap());

                let interval = Interval::new(months, days, microseconds);
                (Value::Interval(interval), 17)
            },

            tags::NUMERIC => {
                // numeric uses its own variable-length format
                // first 8 bytes contain the header/short form
                if content.len() < 8 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated numeric"));
                }
                let packed = u64::from_le_bytes(content[..8].try_into().unwrap());
                let tag_bits = packed >> 62;

                if tag_bits == 0b00 || tag_bits == 0b10 {
                    let n = Numeric::from_serialized_bytes(&content[..8])
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                    (Value::Numeric(n), 9)
                } else {
                    if content.len() < 14 {
                        return Err(Error::new(ErrorKind::InvalidData, "Truncated long numeric"));
                    }
                    let ndigits = u16::from_le_bytes(content[8..10].try_into().unwrap()) as usize;
                    let total_len = 8 + 6 + ndigits * 2;
                    if content.len() < total_len {
                        return Err(Error::new(ErrorKind::InvalidData, "Truncated numeric digits"));
                    }
                    let n = Numeric::from_serialized_bytes(&content[..total_len])
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                    (Value::Numeric(n), 1 + total_len)
                }
            },

            tags::ENUM => {
                if content.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidData, "Missing enum value"));
                }
                (Value::Enum(content[0]), 2)
            },

            tags::BLOB => {
                if content.len() < 4 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated blob length"));
                }
                let len = u32::from_le_bytes(content[..4].try_into().unwrap()) as usize;
                if content.len() < 4 + len {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated blob data"));
                }
                (Value::Blob(Arc::from(&content[4..4 + len])), 5 + len)
            },

            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unknown type tag: {}", tag),
                ));
            },
        })
    }

    #[inline]
    pub(crate) fn serialised_size_hint(&self) -> usize {
        match self {
            Value::Null => 1,
            Value::Boolean(_) => 2,
            Value::Number(_) => 19, // max: 1 + 1 + 16 + 1 for safety
            Value::Float(_) => 9,
            Value::String(s) => 5 + s.len(),
            Value::Temporal(t) => match t {
                Temporal::Date(_) => 6,
                Temporal::Time(_) => 5,
                Temporal::DateTime(_) => 9,
            },
            Value::Uuid(_) => 17,
            Value::Interval(_) => 17,
            Value::Numeric(_) => 32, // variable, estimate
            Value::Enum(_) => 2,
            Value::Blob(b) => 5 + b.len(),
        }
    }
}

macro_rules! impl_value_op {
    ($trait:ty, $method:ident, $op:tt, $integer:ident) => {
        impl $trait for &Value {
            type Output = Option<Value>;
            fn $method(self, rhs: Self) -> Self::Output {
                match (self, rhs) {
                    // float arithmetic
                    (Value::Float(a), Value::Float(b)) => Some(Value::Float(*a $op *b)),
                    (Value::Number(a), Value::Float(b)) => Some(Value::Float(*a as f64 $op *b)),
                    (Value::Float(a), Value::Number(b)) => Some(Value::Float(*a $op *b as f64)),
                    // exact, and division truncates towards zero
                    (Value::Number(a), Value::Number(b)) => a.$integer(*b).map(Value::Number),
                    // arbitrary precision numeric
                    (Value::Numeric(a), Value::Numeric(b)) => Some(Value::Numeric(a $op b)),
                    (Value::Numeric(a), Value::Number(b)) => Some(Value::Numeric(a $op &Numeric::from(*b))),
                    (Value::Number(a), Value::Numeric(b)) => Some(Value::Numeric(&Numeric::from(*a) $op b)),
                    // approximate wins
                    (Value::Numeric(a), Value::Float(b)) => Some(Value::Float(f64::from(a.clone()) $op *b)),
                    (Value::Float(a), Value::Numeric(b)) => Some(Value::Float(*a $op f64::from(b.clone()))),

                    _ => None,
                }
            }
        }
    };
}

impl_value_op!(std::ops::Add, add, +, checked_add);
impl_value_op!(std::ops::Sub, sub, -, checked_sub);
impl_value_op!(std::ops::Mul, mul, *, checked_mul);
impl_value_op!(std::ops::Div, div, /, checked_div);
impl_value_op!(std::ops::Rem, rem, %, checked_rem);

impl Coerce for Value {
    fn coerce_to(self, other: Self) -> (Self, Self) {
        match (&self, &other) {
            // Numeric coercion: Integer <-> Float
            (Value::Float(f), Value::Number(n)) => (Value::Float(*f), Value::Float(*n as f64)),
            (Value::Number(n), Value::Float(f)) => (Value::Float(*n as f64), Value::Float(*f)),

            // String -> Temporal coercion
            (Value::String(string), Value::Temporal(_)) => match Temporal::try_from(&**string) {
                Ok(parsed) => (Value::Temporal(parsed), other),
                _ => (self, other),
            },
            (Value::Temporal(from), Value::String(string)) => {
                use std::mem::discriminant;

                match Temporal::try_from(&**string) {
                    Ok(parsed) => match discriminant(from) == discriminant(&parsed) {
                        true => (self, Value::Temporal(parsed)),

                        _ => {
                            let (left, right) = from.coerce_to(parsed);
                            (Value::Temporal(left), Value::Temporal(right))
                        },
                    },
                    _ => (self, other),
                }
            },

            // String <-> UUID coercion
            (Value::Uuid(_), Value::String(s)) => match std::str::FromStr::from_str(s) {
                Ok(parsed) => (self, Value::Uuid(parsed)),
                _ => (self, other),
            },
            (Value::String(s), Value::Uuid(_)) => match std::str::FromStr::from_str(s) {
                Ok(parsed) => (Value::Uuid(parsed), other),
                _ => (self, other),
            },

            // No coercion needed or not possible
            _ => (self, other),
        }
    }
}

impl Coerce for Temporal {
    fn coerce_to(self, other: Self) -> (Self, Self) {
        match (self, other) {
            (Self::Time(_), Self::DateTime(timestamp)) => (self, Self::Time(timestamp.into())),
            (Self::DateTime(timestamp), Self::Time(_)) => (Self::Time(timestamp.into()), other),
            (Self::Date(_), Self::DateTime(timestamp)) => (self, Self::Date(timestamp.into())),
            (Self::DateTime(timestamp), Self::Date(_)) => (Self::Date(timestamp.into()), other),
            _ => (self, other),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use self::Value::{Numeric as Arbitrary, *};

        match (self, other) {
            (Number(a), Number(b)) => a == b,
            (Float(a), Float(b)) => a == b || (a.is_nan() && b.is_nan()),
            // we do this because we can coerce them later to do a comparison between floats and
            // integers
            (Number(a), Float(b)) => (*a as f64) == *b,
            (Float(a), Number(b)) => *a == (*b as f64),
            (Arbitrary(a), Arbitrary(b)) => a == b,
            (Arbitrary(a), Number(n)) | (Number(n), Arbitrary(a)) => {
                a == &crate::core::numeric::Numeric::from(*n)
            },
            (Arbitrary(a), Float(f)) | (Float(f), Arbitrary(a)) => f64::from(a.clone()) == *f,
            (String(a), String(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,
            (Temporal(a), Temporal(b)) => a == b,
            (Interval(a), Interval(b)) => a == b,
            (Uuid(a), Uuid(b)) => a == b,
            (Enum(a), Enum(b)) => a == b,
            (Blob(a), Blob(b)) => a == b,
            // For grouping and hashing, NULL values should be equal.
            // SQL semantics (NULL = NULL returns NULL) is handled separetly.
            (Null, Null) => true,
            (Null, _) | (_, Null) => false,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => float_cmp(*a, *b),
            (Value::Number(a), Value::Number(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Temporal(a), Value::Temporal(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Interval(a), Value::Interval(b)) => a.cmp(b),
            (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
            // numbers of different representations order by value, as PartialEq compares them
            (Value::Number(a), Value::Float(b)) => float_cmp(*a as f64, *b),
            (Value::Float(a), Value::Number(b)) => float_cmp(*a, *b as f64),
            (Value::Numeric(a), Value::Number(b)) => a.cmp(&Numeric::from(*b)),
            (Value::Number(a), Value::Numeric(b)) => Numeric::from(*a).cmp(b),
            (Value::Numeric(a), Value::Float(b)) => float_cmp(f64::from(a.clone()), *b),
            (Value::Float(a), Value::Numeric(b)) => float_cmp(*a, f64::from(b.clone())),
            (Value::Enum(a), Value::Enum(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (Value::Blob(_), _) => std::cmp::Ordering::Greater,
            (_, Value::Blob(_)) => std::cmp::Ordering::Less,
            // For sorting, NULL values are considered equal to each other
            // and sort after all non-NULL values.
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            // values of different kinds order by kind, as SQLite orders storage classes
            _ => self.class().cmp(&other.class()),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        #[inline]
        const fn encode_float(f: &f64) -> u64 {
            // normalize -0.0 and 0.0 to the same bits, and treat all nans the same.
            let mut bits = f.to_bits();
            if bits == (-0.0f64).to_bits() {
                bits = 0.0f64.to_bits();
            }

            if f.is_nan() {
                // all nans are hashed as the same.
                bits = 0x7ff8000000000000u64;
            } else if bits >> 63 != 0 {
                // for negatives, flip all bits for total ordering.
                bits = !bits;
            }

            bits
        }

        // numbers equal across representations hash alike: integral values up to 2^53 (exact in
        // every representation) by their integer, the rest by their nearest double
        const EXACT: f64 = (1u64 << 53) as f64;
        let mut approximate = |f: f64| match f.fract() == 0.0 && f.abs() <= EXACT {
            true => (f as i64).hash(state),
            _ => encode_float(&f).hash(state),
        };

        match self {
            Self::Float(f) => approximate(*f),
            Self::Number(n) => approximate(*n as f64),
            Self::Numeric(n) => approximate(f64::from(n.clone())),
            Self::Null => NULL_HASH.hash(state),
            Self::String(s) => s.hash(state),
            Self::Boolean(b) => b.hash(state),
            Self::Temporal(t) => t.hash(state),
            Self::Uuid(u) => u.hash(state),
            Self::Interval(i) => i.hash(state),
            Self::Enum(e) => e.hash(state),
            Self::Blob(b) => b.hash(state),
        }
    }
}

impl Neg for Value {
    type Output = Result<Self, SqlError>;
    fn neg(self) -> Self::Output {
        match self {
            Value::Number(num) => num
                .checked_neg()
                .map(Value::Number)
                .ok_or(SqlError::Vm(VmError::IntegerOutOfRange)),
            Value::Float(float) => Ok(Value::Float(-float)),
            Value::Numeric(num) => Ok(Value::Numeric(-num)),
            Value::Null => Ok(Value::Null),
            v => {
                Err(TypeError::CannotApplyUnary { operator: UnaryOperator::Minus, value: v }.into())
            },
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(string) => write!(f, "'{}'", string.replace('\'', "''")),
            Value::Number(number) => write!(f, "{number}"),
            Value::Float(float) => write!(f, "{float}"),
            Value::Boolean(bool) => f.write_str(match *bool {
                true => "TRUE",
                _ => "FALSE",
            }),
            Value::Temporal(temporal) => write!(f, "{temporal}"),
            Value::Uuid(uuid) => write!(f, "{uuid}"),
            Value::Interval(interval) => write!(f, "{interval}"),
            Value::Numeric(numeric) => write!(f, "{numeric}"),
            Value::Enum(index) => write!(f, "{index}"),
            Value::Blob(blob) => {
                use crate::core::json::Jsonb;
                write!(f, "{}", Jsonb::new(blob.len(), Some(blob)))
            },
            Value::Null => f.write_str("NULL"),
        }
    }
}

impl Display for Temporal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DateTime(datetime) => Display::fmt(datetime, f),
            Self::Date(date) => Display::fmt(date, f),
            Self::Time(time) => Display::fmt(time, f),
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Number(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::Float(value as f64)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<Temporal> for Value {
    fn from(value: Temporal) -> Self {
        Self::Temporal(value)
    }
}

impl From<Interval> for Value {
    fn from(value: Interval) -> Self {
        Self::Interval(value)
    }
}

impl From<NaiveDate> for Temporal {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<NaiveDateTime> for Temporal {
    fn from(value: NaiveDateTime) -> Self {
        Self::DateTime(value)
    }
}

impl From<NaiveTime> for Temporal {
    fn from(value: NaiveTime) -> Self {
        Self::Time(value)
    }
}

impl TryFrom<&str> for Temporal {
    type Error = DateParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Ok(dt) = NaiveDateTime::parse_str(value) {
            return Ok(Temporal::DateTime(dt));
        }

        if let Ok(date) = NaiveDate::parse_str(value) {
            return Ok(Temporal::Date(date));
        }

        if let Ok(time) = NaiveTime::parse_str(value) {
            return Ok(Temporal::Time(time));
        }

        Err(DateParseError::InvalidDateTime)
    }
}

/// IEEE order with `-0.0 = 0.0`, and NaN equal to itself and above every number
#[inline]
fn float_cmp(a: f64, b: f64) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => a.partial_cmp(&b).expect("neither is NaN"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_serialise_null() {
        let value = Value::Null;
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes, vec![0]);

        let (deserialized, consumed) = Value::deserialise(&bytes).unwrap();
        assert_eq!(consumed, 1);
        assert!(matches!(deserialized, Value::Null));
    }

    #[test]
    fn test_value_serialise_boolean() {
        let value = Value::Boolean(true);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes, vec![1, 1]);

        let (deserialized, _) = Value::deserialise(&bytes).unwrap();
        assert_eq!(deserialized, Value::Boolean(true));

        let value = Value::Boolean(false);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes, vec![1, 0]);

        let (deserialized, _) = Value::deserialise(&bytes).unwrap();
        assert_eq!(deserialized, Value::Boolean(false));
    }

    #[test]
    fn test_value_serialise_number_sizes() {
        // Small number fits in i16
        let value = Value::Number(42);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[0], 2); // NUMBER tag
        assert_eq!(bytes[1], 2); // i16 size
        assert_eq!(bytes.len(), 4); // tag + size + 2 bytes

        let (deserialized, consumed) = Value::deserialise(&bytes).unwrap();
        assert_eq!(consumed, 4);
        assert_eq!(deserialized, Value::Number(42));

        // Larger number needs i32
        let value = Value::Number(100_000);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[1], 4); // i32 size

        // Even larger needs i64
        let value = Value::Number(5_000_000_000);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[1], 8); // i64 size

        let value = Value::Number(i64::MAX);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[1], 8);
    }

    #[test]
    fn test_value_serialise_string() {
        let value = Value::String("hello".into());
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[0], 4); // STRING tag

        let (deserialized, consumed) = Value::deserialise(&bytes).unwrap();
        assert_eq!(consumed, 10); // tag + 4 len + 5 chars
        assert_eq!(deserialized, Value::String("hello".into()));
    }

    #[test]
    fn test_value_serialise_float() {
        let value = Value::Float(3.14159);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[0], 3); // FLOAT tag
        assert_eq!(bytes.len(), 9); // tag + 8 bytes

        let (deserialized, consumed) = Value::deserialise(&bytes).unwrap();
        assert_eq!(consumed, 9);
        assert_eq!(deserialized, Value::Float(3.14159));
    }

    #[test]
    fn test_value_serialise_roundtrip() {
        let values = vec![
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Number(-42),
            Value::Number(0),
            Value::Number(i32::MAX as i64),
            Value::Float(-0.0),
            Value::Float(f64::MAX),
            Value::String("".into()),
            Value::String("hello world 🌍".into()),
            Value::Enum(255),
            Value::Blob(Arc::from([1, 2, 3, 4, 5])),
        ];

        for value in values {
            let bytes = value.serialise().unwrap();
            let (deserialized, _) = Value::deserialise(&bytes).unwrap();
            assert_eq!(value, deserialized, "Roundtrip failed for {:?}", value);
        }
    }
}
