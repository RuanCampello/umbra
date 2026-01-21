//! SQL Value types and binary serialisation.
//!
//! This module defines the core runtime value types used throughout the database,
//! including [`Value`] for storing column data and [`Temporal`] for date/time values.
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
use crate::core::date::interval::Interval;
use crate::core::date::{DateParseError, NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::core::numeric::Numeric;
use crate::core::uuid::Uuid;
use crate::core::Serialize;
use crate::vm::expression::TypeError;
use std::cmp::Ordering;
use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::io::{self, Error, ErrorKind};
use std::ops::Neg;

/// Type tags for binary serialisation format.
/// Tags 11-255 are reserved for future types.
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

/// Temporal sub-tags for Date/Time/DateTime discrimination.
mod temporal_tags {
    pub const DATE: u8 = 0;
    pub const TIME: u8 = 1;
    pub const DATETIME: u8 = 2;
}

/// Number size hints for variable-width integer encoding.
mod number_sizes {
    pub const I16: u8 = 2;
    pub const I32: u8 = 4;
    pub const I64: u8 = 8;
    pub const I128: u8 = 16;
}

/// A runtime value stored in a table row or returned in a query.
///
/// This enum represents a *concrete value* associated with a column,
/// typically used in query results, expression evaluation, and row serialisation.
///
/// For example:
/// - A row with a `VARCHAR` column might store `Value::String("hello".to_string())`.
/// - A `DATE` column would store `Value::Temporal(Temporal::Date(...))`.
///
/// `Value` is the counterpart to `Type`:  
/// - `Type` defines what *kind* of value is allowed.  
/// - `Value` holds the *actual* data.
///
/// This separation allows the system to validate values against schema definitions at runtime.
///
/// ## Equality semantics
///
/// `Value` implements [`PartialEq`] for deep equality:
/// - Two values are equal only if they are the same variant and hold exactly equal contents.
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
    String(String),
    Number(i128),
    Float(f64),
    Boolean(bool),
    Temporal(Temporal),
    Uuid(Uuid),
    Interval(Interval),
    Numeric(Numeric),
    Enum(u8),
    Blob(Vec<u8>),
    Null,
}

/// Date/Time related types.
///
/// This enum wraps actual values of date/time types, such as a specific calendar date or a time of day.
/// It distinguishes between `DATE`, `TIME`, and `TIMESTAMP` at the value level.
///
/// Values of this type are stored in the `Value::Temporal` variant.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum Temporal {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

pub trait Coerce: Sized {
    fn coerce_to(self, other: Self) -> (Self, Self);
}

/// Literally "NULL" in hex.
const NULL_HASH: u32 = 0x4E554C4C;

impl Value {
    #[inline]
    pub(crate) const fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    #[inline]
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
            (
                Float(_) | Number(_) | Numeric(_),
                Float(_) | Number(_) | Numeric(_)
            ) | (Temporal(_), Interval(_))
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

    /// Serialise a Value to a self-describing binary format.
    ///
    /// The format is designed for fast serialisation/deserialisation without
    /// requiring schema information. Integers use variable-width encoding
    /// to minimize space while maintaining performance.
    ///
    /// # Performance
    ///
    /// This method is optimised for:
    /// - Minimal allocations (pre-sized Vec)
    /// - Direct byte operations (no intermediate formats)
    /// - Branch prediction (common cases first)
    #[inline]
    pub fn serialise(&self) -> io::Result<Vec<u8>> {
        // pre-allocate based on expected size
        let mut buf = Vec::with_capacity(self.serialised_size_hint());

        buf.push(self.tag());

        match self {
            Value::Null => {}

            Value::Boolean(b) => {
                buf.push(if *b { 1 } else { 0 });
            }

            Value::Number(n) => {
                const I16_MIN: i128 = i16::MIN as _;
                const I16_MAX: i128 = i16::MAX as _;
                const I32_MAX: i128 = i32::MAX as _;
                const I32_MIN: i128 = i32::MIN as _;
                const I64_MIN: i128 = i64::MIN as _;
                const I64_MAX: i128 = i64::MAX as _;

                // transform this in a match with ranges cases
                // variable-width integer encoding: use smallest representation
                match *n {
                    I16_MIN..=I16_MAX => {
                        buf.push(number_sizes::I16);
                        buf.extend_from_slice(&(*n as i16).to_le_bytes());
                    }

                    I32_MIN..=I32_MAX => {
                        buf.push(number_sizes::I32);
                        buf.extend_from_slice(&(*n as i32).to_le_bytes());
                    }

                    I64_MIN..=I64_MAX => {
                        buf.push(number_sizes::I64);
                        buf.extend_from_slice(&(*n as i64).to_le_bytes());
                    }

                    _ => {
                        buf.push(number_sizes::I128);
                        buf.extend_from_slice(&n.to_le_bytes());
                    }
                }
            }
            Value::Float(f) => buf.extend_from_slice(&f.to_le_bytes()),
            Value::String(s) => {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Temporal(t) => match t {
                Temporal::Date(d) => {
                    buf.push(temporal_tags::DATE);
                    d.serialize(&mut buf);
                }
                Temporal::Time(t) => {
                    buf.push(temporal_tags::TIME);
                    t.serialize(&mut buf);
                }
                Temporal::DateTime(dt) => {
                    buf.push(temporal_tags::DATETIME);
                    dt.serialize(&mut buf);
                }
            },

            Value::Uuid(u) => buf.extend_from_slice(u.as_ref()),
            Value::Interval(i) => i.serialize(&mut buf),
            Value::Numeric(n) => n.serialize(&mut buf),
            Value::Enum(e) => buf.push(*e),
            Value::Blob(b) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
        }

        Ok(buf)
    }

    /// Deserialise a Value from binary format.
    ///
    /// Returns the deserialised Value and the number of bytes consumed.
    /// This allows efficient parsing of consecutive values.
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
            }

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
                        (Value::Number(n as i128), 4)
                    }
                    number_sizes::I32 => {
                        if payload.len() < 4 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i32"));
                        }
                        let n = i32::from_le_bytes(payload[..4].try_into().unwrap());
                        (Value::Number(n as i128), 6)
                    }
                    number_sizes::I64 => {
                        if payload.len() < 8 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i64"));
                        }
                        let n = i64::from_le_bytes(payload[..8].try_into().unwrap());
                        (Value::Number(n as i128), 10)
                    }
                    number_sizes::I128 => {
                        if payload.len() < 16 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated i128"));
                        }
                        let n = i128::from_le_bytes(payload[..16].try_into().unwrap());
                        (Value::Number(n), 18)
                    }
                    _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid number size")),
                };

                (value, consumed)
            }

            tags::FLOAT => {
                if content.len() < 8 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated float"));
                }
                let f = f64::from_le_bytes(content[..8].try_into().unwrap());

                (Value::Float(f), 9)
            }

            tags::STRING => {
                if content.len() < 4 {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Truncated string length",
                    ));
                }
                let len = u32::from_le_bytes(content[..4].try_into().unwrap()) as usize;
                if content.len() < 4 + len {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated string data"));
                }
                let s = String::from_utf8(content[4..4 + len].to_vec())
                    .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

                (Value::String(s), 5 + len)
            }

            tags::TEMPORAL => {
                if content.is_empty() {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "Missing temporal sub-tag",
                    ));
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
                    }
                    temporal_tags::TIME => {
                        if payload.len() < 3 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated time"));
                        }
                        let bytes: [u8; 3] = payload[..3].try_into().unwrap();
                        let time = NaiveTime::from(bytes);

                        (Value::Temporal(Temporal::Time(time)), 5)
                    }
                    temporal_tags::DATETIME => {
                        if payload.len() < 7 {
                            return Err(Error::new(ErrorKind::InvalidData, "Truncated datetime"));
                        }
                        let date_bytes: [u8; 4] = payload[..4].try_into().unwrap();
                        let time_bytes: [u8; 3] = payload[4..7].try_into().unwrap();
                        let dt = NaiveDateTime::try_from((date_bytes, time_bytes))?;

                        (Value::Temporal(Temporal::DateTime(dt)), 9)
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Invalid temporal sub-tag",
                        ))
                    }
                }
            }

            tags::UUID => {
                if content.len() < 16 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated UUID"));
                }
                let bytes: [u8; 16] = content[..16].try_into().unwrap();
                (Value::Uuid(Uuid::from_bytes(bytes)), 17)
            }

            tags::INTERVAL => {
                if content.len() < 16 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated interval"));
                }
                let months = i32::from_le_bytes(content[..4].try_into().unwrap());
                let days = i32::from_le_bytes(content[4..8].try_into().unwrap());
                let microseconds = i64::from_le_bytes(content[8..16].try_into().unwrap());

                let interval = Interval::new(months, days, microseconds);
                (Value::Interval(interval), 17)
            }

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
                        return Err(Error::new(
                            ErrorKind::InvalidData,
                            "Truncated numeric digits",
                        ));
                    }
                    let n = Numeric::from_serialized_bytes(&content[..total_len])
                        .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
                    (Value::Numeric(n), 1 + total_len)
                }
            }

            tags::ENUM => {
                if content.is_empty() {
                    return Err(Error::new(ErrorKind::InvalidData, "Missing enum value"));
                }
                (Value::Enum(content[0]), 2)
            }

            tags::BLOB => {
                if content.len() < 4 {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated blob length"));
                }
                let len = u32::from_le_bytes(content[..4].try_into().unwrap()) as usize;
                if content.len() < 4 + len {
                    return Err(Error::new(ErrorKind::InvalidData, "Truncated blob data"));
                }
                (Value::Blob(content[4..4 + len].to_vec()), 5 + len)
            }

            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("Unknown type tag: {}", tag),
                ))
            }
        })
    }

    #[inline]
    const fn serialised_size_hint(&self) -> usize {
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
    ($trait:ty, $method:ident, $op:tt) => {
        impl $trait for &Value {
            type Output = Option<Value>;
            fn $method(self, rhs: Self) -> Self::Output {
                match (self, rhs) {
                    // float arithmetic
                    (Value::Float(a), Value::Float(b)) => Some(Value::Float(*a $op *b)),
                    (Value::Number(a), Value::Float(b)) => Some(Value::Float(*a as f64 $op *b)),
                    (Value::Float(a), Value::Number(b)) => Some(Value::Float(*a $op *b as f64)),
                    (Value::Number(a), Value::Number(b)) => {
                        let result = *a as f64 $op *b as f64;
                        match result.fract() == 0.0 {
                            true => Some(Value::Number(result as i128)),
                            _ => Some(Value::Float(result))
                        }
                    }

                    // arbitrary precision numeric
                    (Value::Numeric(a), Value::Numeric(b)) => Some(Value::Numeric(a $op b)),
                    (Value::Numeric(a), Value::Number(b)) => {
                        Some(Value::Numeric(a $op &Numeric::from(*b)))
                    }
                    (Value::Number(a), Value::Numeric(b)) => {
                        Some(Value::Numeric(&Numeric::from(*a) $op b))
                    }
                    (Value::Numeric(a), Value::Float(b)) => {
                        Numeric::try_from(*b).ok().map(|nb| Value::Numeric(a $op &nb))
                    }
                    (Value::Float(a), Value::Numeric(b)) => {
                        Numeric::try_from(*a).ok().map(|na| Value::Numeric(&na $op b))
                    }

                    _ => None,
                }
            }
        }
    };
}

impl_value_op!(std::ops::Add, add, +);
impl_value_op!(std::ops::Sub, sub, -);
impl_value_op!(std::ops::Mul, mul, *);
impl_value_op!(std::ops::Div, div, /);

impl Coerce for Value {
    fn coerce_to(self, other: Self) -> (Self, Self) {
        match (&self, &other) {
            // Numeric coercion: Integer <-> Float
            (Value::Float(f), Value::Number(n)) => (Value::Float(*f), Value::Float(*n as f64)),
            (Value::Number(n), Value::Float(f)) => (Value::Float(*n as f64), Value::Float(*f)),

            // String -> Temporal coercion
            (Value::String(string), Value::Temporal(_)) => {
                match Temporal::try_from(string.as_str()) {
                    Ok(parsed) => (Value::Temporal(parsed), other),
                    _ => (self, other),
                }
            }
            (Value::Temporal(from), Value::String(string)) => {
                use std::mem::discriminant;

                match Temporal::try_from(string.as_str()) {
                    Ok(parsed) => match discriminant(from) == discriminant(&parsed) {
                        true => (self, Value::Temporal(parsed)),

                        _ => {
                            let (left, right) = from.coerce_to(parsed);
                            (Value::Temporal(left), Value::Temporal(right))
                        }
                    },
                    _ => (self, other),
                }
            }

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
            (Float(a), Float(b)) => a == b,
            // we do this because we can coerce them later to do a comparison between floats and
            // integers
            (Number(a), Float(b)) => (*a as f64) == *b,
            (Float(a), Number(b)) => *a == (*b as f64),
            (Arbitrary(a), Arbitrary(b)) => a == b,
            (Arbitrary(a), Number(n)) | (Number(n), Arbitrary(a)) => {
                a == &crate::core::numeric::Numeric::from(*n)
            }
            (Arbitrary(a), Float(f)) | (Float(f), Arbitrary(a)) => {
                crate::core::numeric::Numeric::try_from(*f)
                    .map(|f| &f == a)
                    .unwrap_or(false)
            }
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
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Number(a), Value::Number(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Temporal(a), Value::Temporal(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Interval(a), Value::Interval(b)) => a.cmp(b),
            (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
            (Value::Enum(a), Value::Enum(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (Value::Blob(_), _) => std::cmp::Ordering::Greater,
            (_, Value::Blob(_)) => std::cmp::Ordering::Less,
            // For sorting, NULL values are considered equal to each other
            // and sort after all non-NULL values.
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            _ => panic!("these values are not comparable"),
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

        match self {
            Self::Float(f) => encode_float(f).hash(state),
            Self::Null => NULL_HASH.hash(state),
            Self::String(s) => s.hash(state),
            Self::Number(n) => n.hash(state),
            Self::Boolean(b) => b.hash(state),
            Self::Temporal(t) => t.hash(state),
            Self::Uuid(u) => u.hash(state),
            Self::Interval(i) => i.hash(state),
            Self::Numeric(n) => n.hash(state),
            Self::Enum(e) => e.hash(state),
            Self::Blob(b) => b.hash(state),
        }
    }
}

impl Neg for Value {
    type Output = Result<Self, TypeError>;
    fn neg(self) -> Self::Output {
        match self {
            Value::Number(num) => Ok(Value::Number(-num)),
            Value::Float(float) => Ok(Value::Float(-float)),
            Value::Numeric(num) => Ok(Value::Numeric(-num)),
            Value::Null => Ok(Value::Null),
            v => Err(TypeError::CannotApplyUnary {
                operator: UnaryOperator::Minus,
                value: v,
            }),
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(string) => write!(f, "\"{string}\""),
            Value::Number(number) => write!(f, "{number}"),
            Value::Float(float) => write!(f, "{float}"),
            Value::Boolean(bool) => f.write_str(if *bool { "TRUE" } else { "FALSE" }),
            Value::Temporal(temporal) => write!(f, "{temporal}"),
            Value::Uuid(uuid) => write!(f, "{uuid}"),
            Value::Interval(interval) => write!(f, "{interval}"),
            Value::Numeric(numeric) => write!(f, "{numeric}"),
            Value::Enum(index) => write!(f, "{index}"),
            Value::Blob(blob) => {
                use crate::core::json::Jsonb;
                write!(f, "{}", Jsonb::new(blob.len(), Some(blob)))
            }
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

impl From<i128> for Value {
    fn from(value: i128) -> Self {
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
        Self::String(value.to_string())
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

        // Maximum size i128
        let value = Value::Number(i64::MAX as i128 + 1);
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[1], 16); // i128 size
    }

    #[test]
    fn test_value_serialise_string() {
        let value = Value::String("hello".to_string());
        let bytes = value.serialise().unwrap();
        assert_eq!(bytes[0], 4); // STRING tag

        let (deserialized, consumed) = Value::deserialise(&bytes).unwrap();
        assert_eq!(consumed, 10); // tag + 4 len + 5 chars
        assert_eq!(deserialized, Value::String("hello".to_string()));
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
            Value::Number(i32::MAX as i128),
            Value::Float(-0.0),
            Value::Float(f64::MAX),
            Value::String("".to_string()),
            Value::String("hello world 🌍".to_string()),
            Value::Enum(255),
            Value::Blob(vec![1, 2, 3, 4, 5]),
        ];

        for value in values {
            let bytes = value.serialise().unwrap();
            let (deserialized, _) = Value::deserialise(&bytes).unwrap();
            assert_eq!(value, deserialized, "Roundtrip failed for {:?}", value);
        }
    }
}
