//! High-performance arbitrary-precision numeric type for the database storage.
//!
//! This module implements a numeric type optimised for storage and operations,
//! following PostgreSQL's internal architecture with base-10000 representation.

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
