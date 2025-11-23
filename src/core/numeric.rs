//! High-performance arbitrary-precision numeric type for the database storage.
//!
//! This module implements a numeric type optimised for storage and operations,
//! following PostgreSQL's internal architecture with base-10000 representation.

use crate::core::Serialize;
use std::{cmp::Ordering, fmt::Display, ops::Add, str::FromStr};

/// Arbitrary-precision numeric type.
/// This can represent any decimal with arbitrary precision.
#[derive(Debug, Clone)]
pub enum Numeric {
    /// Small numeric value packed into a single 64-bit word.
    /// No heap allocation, optimal for cache locality.
    Short(u64),

    /// variable-length numeric. Uses base-10000 representation.
    Long {
        weight: i16,
        sign_dscale: u16,
        digits: Vec<i16>,
    },

    /// Not a number. Represents an invalid/undefined numeric values.
    NaN,
}

#[derive(Debug, PartialEq)]
pub enum NumericError {
    InvalidFormat,
    Overflow,
}

/// Base for internal digit representation. Each digit represents a value from 0 to 9999.
const N_BASE: i32 = 10000;

/// Maximum number of digits that can be stored inline in the variable-length format.
const TAG_SHORT: u64 = 0b00;

// Bit positions and masks for packed format.

const TAG_SHIFT: u64 = 62;
const SIGN_SHIFT: u64 = 61;
const SCALE_SHIFT: u64 = 54;
const WEIGHT_SHIFT: u64 = 46;
const SIGN_MASK: u64 = 1 << SIGN_SHIFT;
const SCALE_MASK: u64 = 0x7F << SCALE_SHIFT;
const PAYLOAD_MASK: u64 = (1 << 46) - 1;

// Sign values for variable-length format.

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const SIGN_DSCALE_MASK: u16 = 0xC000;
const DSCALE_MASK: u16 = 0x3FFF;

impl Numeric {
    /// Returns a numeric representing zero.
    pub const fn zero() -> Self {
        Self::Short(TAG_SHORT << TAG_SHIFT)
    }

    pub const fn is_zero(&self) -> bool {
        match self {
            Self::Short(packed) => (*packed & PAYLOAD_MASK) == 0,
            Self::Long { digits, .. } => digits.is_empty(),
            Self::NaN => false,
        }
    }

    pub const fn is_nan(&self) -> bool {
        matches!(self, Self::NaN)
    }

    pub const fn is_negative(&self) -> bool {
        match self {
            Self::Short(packed) => {
                let sign = (*packed & SIGN_MASK) >> SIGN_SHIFT;
                sign != 0 && !self.is_zero()
            }
            Self::Long { sign_dscale, .. } => (*sign_dscale & SIGN_DSCALE_MASK) == NUMERIC_NEG,
            Self::NaN => false,
        }
    }

    pub const fn scale(&self) -> u16 {
        match self {
            Self::Short(packed) => ((*packed & SCALE_MASK) >> SCALE_SHIFT) as u16,
            Self::Long { sign_dscale, .. } => *sign_dscale & DSCALE_MASK,
            Self::NaN => 0,
        }
    }

    fn from_long_i64(value: i64) -> Self {
        if value == 0 {
            return Self::zero();
        }

        let abs = value.unsigned_abs();

        let mut digits = Vec::new();
        let mut remaining = abs;

        while remaining > 0 {
            digits.push((remaining % N_BASE as u64) as i16);
            remaining /= N_BASE as u64;
        }

        digits.reverse();

        let weight = (digits.len() as i16) - 1;
        let sign_dscale = match value.is_negative() {
            true => NUMERIC_NEG,
            _ => NUMERIC_POS,
        };

        Self::Long {
            weight,
            sign_dscale,
            digits,
        }
    }

    /// Compares absolute values, flipping if both are negative.
    #[inline]
    fn cmp_mag(&self, other: &Self, both_negative: bool) -> Ordering {
        let result = match (self, other) {
            (Self::Short(a), Self::Short(b)) => {
                let (a_payload, b_payload) = (a & PAYLOAD_MASK, b & PAYLOAD_MASK);
                let a_scale = ((a & SCALE_MASK) >> SCALE_SHIFT) as i32;
                let b_scale = ((b & SCALE_MASK) >> SCALE_SHIFT) as i32;

                // normalise if needed to compare
                if a_scale == b_scale {
                    a_payload.cmp(&b_payload)
                } else if a_scale < b_scale {
                    let a_scaled = a_payload * 10_u64.pow((b_scale - a_scale) as u32);
                    a_scaled.cmp(&b_payload)
                } else {
                    let b_scaled = b_payload * 10_u64.pow((a_scale - b_scale) as u32);
                    a_payload.cmp(&b_scaled)
                }
            }

            (Self::Short(_), Self::Long { .. }) | (Self::Long { .. }, Self::Short(_)) => {
                let a_val = Option::<f64>::from(self).unwrap();
                let b_val = Option::<f64>::from(other).unwrap();

                a_val
                    .abs()
                    .partial_cmp(&b_val.abs())
                    .unwrap_or(Ordering::Equal)
            }

            (
                Self::Long {
                    weight: w1,
                    digits: d1,
                    ..
                },
                Self::Long {
                    weight: w2,
                    digits: d2,
                    ..
                },
            ) => match w1.cmp(w2) {
                Ordering::Equal => {
                    for i in 0..d1.len().min(d2.len()) {
                        match d1[i].cmp(&d2[i]) {
                            Ordering::Equal => continue,
                            other => {
                                return if both_negative {
                                    other.reverse()
                                } else {
                                    other
                                }
                            }
                        }
                    }
                    d1.len().cmp(&d2.len())
                }
                other => other,
            },
            _ => Ordering::Equal,
        };

        match both_negative {
            true => result.reverse(),
            _ => result,
        }
    }

    #[inline]
    fn from_scaled_i128(value: i128, scale: u16) -> Result<Self, NumericError> {
        if value == 0 {
            return Ok(Self::zero());
        }

        let is_negative = value.is_negative();
        let mut abs = value.unsigned_abs();

        // try the short format to see if it fits (that's what she said)
        if scale <= 127 && abs <= PAYLOAD_MASK as u128 {
            let sign = if is_negative { 1u64 } else { 0u64 };
            let packed = (TAG_SHORT << TAG_SHIFT)
                | (sign << SIGN_SHIFT)
                | ((scale as u64) << SCALE_SHIFT)
                | (0 << WEIGHT_SHIFT)
                | (abs as u64);
            return Ok(Self::Short(packed));
        }

        // alignment padding
        //
        // ex: if scale is 3 (e.g 0.123), we have 3 decimals. We need 4 to fill a chunk.
        // we must multiply by 10 to get 0.1230 (represented as integer 1230)
        let align_pad = scale % 4;
        if align_pad > 0 {
            let multiplier = 10u128.pow((4 - align_pad) as u32);
            abs = abs.checked_mul(multiplier).ok_or(NumericError::Overflow)?;
        }

        let mut digits = Vec::new();
        let mut remaining = abs;

        while remaining > 0 {
            digits.push((remaining % N_BASE as u128) as i16);
            remaining /= N_BASE as u128;
        }

        if digits.is_empty() {
            return Ok(Self::zero());
        }

        digits.reverse();

        // calculate the weight
        //
        // the weight is the index of the base-10000 digit where 10^0 starts
        // to calculate how many groups of 4 decimals we have:
        //     Scale 0-4 -> 1 group, Scale 5-8 -> 2 groups
        //     so we ceil(scale / 4)
        //

        let fract_groups = (scale + 3) / 4;
        let weight = (digits.len() as i16) - (fract_groups as i16) - 1;

        let sign_part = match is_negative {
            true => NUMERIC_NEG,
            _ => NUMERIC_POS,
        };

        let sign_dscale = sign_part | (scale & DSCALE_MASK);

        Ok(Self::Long {
            weight,
            sign_dscale,
            digits,
        })
    }

    #[inline]
    /// Performs addition of absolute values: |A| + |B|
    fn add_abs(w1: i16, d1: &[i16], w2: i16, d2: &[i16]) -> Self {
        use std::cmp::{max, min};

        let w1_end = w1 - (d1.len() as i16) + 1;
        let w2_end = w2 - (d2.len() as i16) + 1;

        let weight_max = max(w1, w2);
        let weight_min = min(w1_end, w2_end);

        let mut digits = Vec::new();
        let mut carry = 0i32;

        for weight in weight_min..weight_max {
            let v1 = match weight <= w1 && weight >= w1_end {
                true => d1[(w1 - weight) as usize] as i32,
                _ => 0,
            };

            let v2 = match weight <= w2 && weight >= w2_end {
                true => d2[(w2 - weight) as usize] as i32,
                _ => 0,
            };

            let sum = v1 + v2 + carry;
            match sum > N_BASE {
                true => {
                    carry = 1;
                    digits.push((sum - N_BASE) as i16);
                }
                _ => {
                    carry = 0;
                    digits.push(sum as i16)
                }
            }
        }

        let weight = match carry > 0 {
            true => {
                digits.push(carry as i16);
                weight_max + 1
            }
            _ => weight_max,
        };

        while digits.last() == Some(&0) {
            digits.pop();
        }

        if digits.is_empty() {
            return Numeric::from_scaled_i128(0, 0).unwrap();
        }

        digits.reverse();

        Numeric::Long {
            weight,
            sign_dscale: 0,
            digits,
        }
    }

    /// standardise short and long format access
    #[inline]
    fn as_long_view(&self) -> (i16, Vec<i16>, bool, u16) {
        match self {
            Self::NaN => panic!("NaN must be handled elsewhere"),
            Self::Short(_) => {
                let (v, s) = self.unpack_short();

                if let Numeric::Long {
                    weight,
                    sign_dscale,
                    digits,
                } = Numeric::from_scaled_i128(v, s).unwrap()
                {
                    let is_neg = (sign_dscale & NUMERIC_NEG) != 0;
                    (weight, digits, is_neg, s)
                } else {
                    unreachable!()
                }
            }
            Self::Long {
                weight,
                sign_dscale,
                digits,
            } => {
                let is_neg = (sign_dscale & NUMERIC_NEG) != 0;
                let scale = sign_dscale & DSCALE_MASK;

                (*weight, digits.clone(), is_neg, scale)
            }
        }
    }

    #[inline]
    fn unpack_short(&self) -> (i128, u16) {
        match self {
            Self::Short(packed) => {
                let abs = packed & PAYLOAD_MASK;

                let scale = ((packed & SCALE_MASK) >> SCALE_SHIFT) as u16;

                let is_negative = ((packed >> SIGN_SHIFT) & 1) == 1;

                let val = match is_negative {
                    true => -(abs as i128),
                    _ => abs as i128,
                };

                (val, scale)
            }
            _ => panic!("unpack_short called on non-Short variant"),
        }
    }
}

impl Serialize for Numeric {
    fn serialize(&self, buff: &mut Vec<u8>) {
        match self {
            // in short format, just serialise the 8 bytes as is
            Self::Short(packed) => buff.extend_from_slice(&packed.to_le_bytes()),

            // variable-length format:
            // 8 bytes: tag marker (0x01 shifted to TAG_SHIFT position)
            // 2 bytes: number of digits (length)
            // 2 bytes: weight
            // 2 bytes: sign_dscale
            // N * 2 bytes: digits
            Self::Long {
                weight,
                sign_dscale,
                digits,
            } => {
                let tag = 0b01u64 << TAG_SHIFT;
                buff.extend_from_slice(&tag.to_le_bytes());

                let len = digits.len() as u16;
                buff.extend_from_slice(&len.to_le_bytes());
                buff.extend_from_slice(&weight.to_le_bytes());
                buff.extend_from_slice(&sign_dscale.to_le_bytes());

                digits
                    .iter()
                    .for_each(|d| buff.extend_from_slice(&d.to_le_bytes()));
            }

            // nan is represented as a short format with a special bit pattern
            Self::NaN => buff.extend_from_slice(&(0b10u64 << TAG_SHIFT).to_le_bytes()),
        }
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NaN, Self::NaN) => true,
            (Self::NaN, _) | (_, Self::NaN) => false,
            _ => self.cmp(other).eq(&Ordering::Equal),
        }
    }
}

impl Ord for Numeric {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::NaN, Self::NaN) => Ordering::Equal,
            (Self::NaN, _) => Ordering::Greater,
            (_, Self::NaN) => Ordering::Less,

            (a, b) if a.is_zero() && b.is_zero() => Ordering::Equal,
            (a, _) if a.is_zero() && !a.is_negative() => match other.is_negative() {
                true => Ordering::Greater,
                _ => Ordering::Less,
            },

            (_, b) if b.is_zero() && !b.is_negative() => match self.is_negative() {
                true => Ordering::Less,
                _ => Ordering::Greater,
            },

            (a, b) if a.is_negative() != b.is_negative() => match a.is_negative() {
                true => Ordering::Less,
                _ => Ordering::Greater,
            },

            // both have the same sign, so we need to compare its value
            _ => self.cmp_mag(other, self.is_negative()),
        }
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Numeric {}

impl Default for Numeric {
    fn default() -> Self {
        Self::zero()
    }
}

impl Add for Numeric {
    type Output = Self;

    #[inline]
    /// Hardly based on [postgres](https://github.com/postgres/postgres/blob/7d9043aee803bf9bf3307ce5f45f3464ea288cb1/src/backend/utils/adt/numeric.c#L8062) implementation.
    fn add(self, rhs: Self) -> Self::Output {
        if self.is_nan() || rhs.is_nan() {
            return Self::NaN;
        }

        if self.is_zero() {
            return rhs;
        }
        if rhs.is_zero() {
            return self;
        }

        if let (Numeric::Short(_), Numeric::Short(_)) = (&self, &rhs) {
            let (val_a, scale_a) = self.unpack_short();
            let (val_b, scale_b) = rhs.unpack_short();

            let res_scale = std::cmp::max(scale_a, scale_b);

            let a = match scale_a < res_scale {
                true => val_a.checked_mul(10i128.pow((res_scale - scale_a) as u32)),
                _ => Some(val_a),
            };

            let b = match scale_b < res_scale {
                true => val_b.checked_mul(10i128.pow((res_scale - scale_b) as u32)),
                _ => Some(val_b),
            };

            if let (Some(a), Some(b)) = (a, b) {
                if let Some(sum) = a.checked_add(b) {
                    if let Ok(res) = Numeric::from_scaled_i128(sum, res_scale) {
                        return res;
                    }
                }
            }
        }

        // slow path: values are long or some of them is a long variant
        let (w_a, digits_a, neg_a, scale_a) = self.as_long_view();
        let (w_b, digits_b, neg_b, scale_b) = self.as_long_view();

        match neg_a == neg_b {
            // case: (+A) + (+B) or (-A) + (-B)
            // operation has the same magnitudes
            // results the same sign as A
            true => Numeric::add_abs(w_a, &digits_a, w_b, &digits_b),

            // case: (+A) + (-B) or (-A) + (+B)
            // operation is a sub magnitudes: |A| - |B|
            // results sign need to involve the comparison of magnitudes
            _ => unimplemented!(),
        }
    }
}

impl From<[u8; 8]> for Numeric {
    fn from(value: [u8; 8]) -> Self {
        let packed = u64::from_le_bytes(value);
        Self::Short(packed)
    }
}

impl From<&[u8; 8]> for Numeric {
    fn from(value: &[u8; 8]) -> Self {
        Self::from(*value)
    }
}

impl From<u64> for Numeric {
    fn from(value: u64) -> Self {
        Self::Short(value)
    }
}

impl TryFrom<&[u8]> for Numeric {
    type Error = NumericError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 8 {
            return Err(NumericError::InvalidFormat);
        }

        let packed = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let tag = (packed >> TAG_SHIFT) & 0b11;

        match tag {
            0b00 => Ok(Self::from(packed)),
            0b10 => Ok(Self::NaN),
            0b01 => {
                if bytes.len() < 14 {
                    return Err(NumericError::InvalidFormat);
                }

                let len = u16::from_le_bytes(bytes[8..10].try_into().unwrap()) as usize;
                let weight = i16::from_le_bytes(bytes[10..12].try_into().unwrap());
                let sign_dscale = u16::from_le_bytes(bytes[12..14].try_into().unwrap());

                let expected_len = 14 + len * 2;
                if bytes.len() < expected_len {
                    return Err(NumericError::InvalidFormat);
                }

                let mut digits = Vec::with_capacity(len);
                (0..len).for_each(|idx| {
                    let offset = 14 + idx * 2;
                    let digit = i16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
                    digits.push(digit);
                });

                Ok(Self::Long {
                    weight,
                    sign_dscale,
                    digits,
                })
            }
            _ => Err(NumericError::InvalidFormat),
        }
    }
}

impl From<i64> for Numeric {
    #[inline]
    fn from(value: i64) -> Self {
        if value == 0 {
            return Self::zero();
        }

        let sign = if value.is_negative() { 1u64 } else { 0u64 };
        let abs = value.unsigned_abs();

        if abs <= PAYLOAD_MASK {
            let packed = (TAG_SHORT << TAG_SHIFT)
                | (sign << SIGN_SHIFT)
                | (0 << SCALE_SHIFT)
                | (0 << WEIGHT_SHIFT)
                | abs;

            return Self::Short(packed);
        }

        Self::from_long_i64(value)
    }
}

impl From<&Numeric> for Option<f64> {
    fn from(value: &Numeric) -> Self {
        match value {
            Numeric::Short(packed) => {
                let payload = (packed & PAYLOAD_MASK) as f64;
                let sign = (packed & SIGN_MASK) >> SIGN_SHIFT;
                let scale = ((packed & SCALE_MASK) >> SCALE_SHIFT) as u32;

                let divisor = 10_f64.powi(scale as i32);
                let value = payload / divisor;

                Some(if sign != 0 { -value } else { value })
            }

            Numeric::Long {
                weight,
                sign_dscale,
                digits,
            } => {
                if digits.is_empty() {
                    return Some(0.0);
                }

                let mut value = 0.0f64;

                digits.iter().enumerate().for_each(|(idx, &digit)| {
                    let power = weight - idx as i16;
                    let mul = (N_BASE as f64).powi(power as i32);

                    value += digit as f64 * mul;
                });

                let scale = (sign_dscale & DSCALE_MASK) as u32;
                let divisor = 10_f64.powi(scale as i32);
                value /= divisor;

                let is_negative = (*sign_dscale & SIGN_DSCALE_MASK) == NUMERIC_NEG;
                Some(if is_negative { -value } else { value })
            }

            Numeric::NaN => Some(f64::NAN),
        }
    }
}

impl From<Numeric> for Option<f64> {
    fn from(value: Numeric) -> Self {
        Self::from(&value)
    }
}

impl From<&Numeric> for Option<i64> {
    fn from(value: &Numeric) -> Self {
        match value {
            Numeric::NaN => None,

            Numeric::Short(packed) => {
                let payload = packed & PAYLOAD_MASK;
                let sign = (packed & SIGN_MASK) >> SIGN_SHIFT;
                let scale = ((packed & SCALE_MASK) >> SCALE_SHIFT) as u32;

                // divide by 10^scale to remove fractional part
                let mut value = payload;
                (0..scale).for_each(|_| value /= 10);

                if value > i64::MAX as u64 {
                    return None;
                }

                let value = value as i64;
                Some(if sign != 0 { -value } else { value })
            }

            Numeric::Long {
                weight,
                sign_dscale,
                digits,
            } => {
                if digits.is_empty() {
                    return Some(0);
                }

                let mut value: i128 = 0;
                for (idx, &digit) in digits.iter().enumerate() {
                    let power = weight - idx as i16;
                    if power < 0 {
                        break;
                    }

                    let multiplier = (N_BASE as i128).pow(power as u32);
                    value += digit as i128 * multiplier;
                }

                if value > i64::MAX as i128 {
                    return None;
                }

                let is_negative = (*sign_dscale & SIGN_DSCALE_MASK) == NUMERIC_NEG;
                let value = value as i64;
                Some(if is_negative { -value } else { value })
            }
        }
    }
}

impl From<Numeric> for Option<i64> {
    fn from(value: Numeric) -> Self {
        Self::from(&value)
    }
}

impl FromStr for Numeric {
    type Err = NumericError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err(NumericError::InvalidFormat);
        }

        if s.eq_ignore_ascii_case("nan") {
            return Ok(Self::NaN);
        }

        let is_negative = s.starts_with('-');
        let s = s.trim_start_matches(&['+', '-'][..]);
        if s.is_empty() {
            return Err(NumericError::InvalidFormat);
        }

        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() > 2 {
            return Err(NumericError::InvalidFormat);
        }

        let int = parts[0];
        let fract = if parts.len() == 2 { parts[1] } else { "" };
        let scale = fract.len();

        let combined = format!("{int}{fract}");

        let value = combined
            .parse::<i128>()
            .map_err(|_| NumericError::InvalidFormat)?;

        let value = if is_negative { -value } else { value };
        Self::from_scaled_i128(value, scale as u16)
    }
}

impl Display for Numeric {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NaN => write!(f, "NaN"),
            Self::Short(packed) => {
                let payload = packed & PAYLOAD_MASK;
                let sign = (packed & SIGN_MASK) >> SIGN_SHIFT;
                let scale = ((packed & SCALE_MASK) >> SCALE_SHIFT) as u32;

                if payload == 0 {
                    return write!(f, "0");
                }

                let sign_str = if sign != 0 { "-" } else { "" };

                match scale == 0 {
                    true => write!(f, "{sign_str}{payload}"),
                    _ => {
                        let divisor = 10_u64.pow(scale);
                        let int = payload / divisor;
                        let fract = payload % divisor;

                        write!(f, "{sign_str}{int}.{fract:0width$}", width = scale as usize)
                    }
                }
            }
            Self::Long {
                sign_dscale,
                digits,
                ..
            } => {
                if digits.is_empty() {
                    return write!(f, "0");
                }

                let is_negative = (*sign_dscale & SIGN_DSCALE_MASK) == NUMERIC_NEG;
                let scale = (sign_dscale & DSCALE_MASK) as i16;

                if is_negative {
                    write!(f, "-")?;
                }

                let mut decimal_str = String::new();
                for (i, &digit) in digits.iter().enumerate() {
                    match i == 0 {
                        true => decimal_str.push_str(&format!("{}", digit)),
                        _ => decimal_str.push_str(&format!("{:04}", digit)),
                    }
                }

                if scale > 0 {
                    let point_pos = decimal_str.len().saturating_sub(scale as usize);
                    if point_pos == 0 {
                        decimal_str.insert_str(0, "0.");
                    } else if point_pos < decimal_str.len() {
                        decimal_str.insert(point_pos, '.');
                    }
                }

                write!(f, "{decimal_str}")
            }
        }
    }
}

impl Display for NumericError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFormat => write!(f, "Invalid numeric format"),
            Self::Overflow => write!(f, "Numeric overflow"),
        }
    }
}

impl From<Numeric> for f64 {
    fn from(value: Numeric) -> Self {
        Option::<f64>::from(&value).unwrap_or(f64::NAN)
    }
}

impl From<Numeric> for i64 {
    fn from(value: Numeric) -> Self {
        Option::<i64>::from(&value).expect("Invalid numeric conversion for i64")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero() {
        let zero = Numeric::zero();
        assert!(!zero.is_negative());
        assert!(zero.is_zero());
        assert_eq!(i64::from(zero), 0);
    }

    #[test]
    fn from_str() {
        let n = Numeric::from_str("123.45").unwrap();
        let val = f64::from(n);
        assert!((val - 123.45).abs() < 0.001);

        let n = Numeric::from_str("-456.78").unwrap();
        let val = f64::from(n);
        assert!((val + 456.78).abs() < 0.001);
    }

    #[test]
    fn base_10000_representation() {
        // 10000 should be represented as a single digit in base-10000
        let num = Numeric::from(10000u64);
        if let Numeric::Long { digits, weight, .. } = num {
            assert_eq!(digits.len(), 1);
            assert_eq!(digits[0], 1);
            assert_eq!(weight, 0);
        }
    }

    #[test]
    fn comparison() {
        let a = Numeric::from(10u64);
        let b = Numeric::from(20u64);
        assert!(a < b);

        let c = Numeric::from(-5i64);
        assert!(c < a);
    }

    #[test]
    fn large_number() {
        // about 9.22 quintillions, that's huge
        let large = Numeric::from(i64::MAX);
        assert!(Option::<i64>::from(large).is_some());
    }

    #[test]
    fn xxx() {
        let n = Numeric::from_scaled_i128(12345, 131).unwrap();
        //assert_eq!(n.digits, vec![12, 345]);
        println!("numeric: {n:#?} {n}");
    }
}
