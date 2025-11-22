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
        weight: u16,
        sigh_dscale: u16,
        digits: Vec<u16>,
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
        todo!()
    }
}

impl From<i64> for Numeric {
    fn from(value: i64) -> Self {
        todo!()
    }
}
