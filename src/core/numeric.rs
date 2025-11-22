//! High-performance arbitrary-precision numeric type for the database storage.
//!
//! This module implements a numeric type optimised for storage and operations,
//! following PostgreSQL's internal architecture with base-10000 representation.

use std::cmp::Ordering;

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

            _ => todo!(),
        }
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Numeric {}

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
