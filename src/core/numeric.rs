//! High-performance arbitrary-precision numeric type for the database storage.
//!
//! This module implements a numeric type optimised for storage and operations,
//! following PostgreSQL's internal architecture with base-10000 representation.

use crate::core::Serialize;
use std::borrow::Cow;
use std::cmp::{max, min};
use std::ops::{Mul, Neg, Sub};
use std::{cmp::Ordering, convert::TryFrom, fmt::Display, ops::Add, str::FromStr};

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

    #[inline]
    #[rustfmt::skip]
    pub fn precision(&self) -> usize {
        if self.is_zero() { return 1; };
        if self.is_nan() { return 0; };

        let (weight, digits, _, scale) = self.as_long_view();
        let int_groups = (weight + 1).max(0) as isize;
        let mut int_digits = int_groups * 4;

        if let Some(first) = digits.first() {
            if *first < 10 { int_digits -= 3; }
            else if *first < 100 { int_digits -= 2; }
            else if *first < 1000 { int_digits -= 1; }
        }

        (int_digits + scale as isize) as usize
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

    /// Compares absolute values: |A| vs |B|
    #[inline]
    pub fn cmp_abs(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Self::Short(_), Self::Short(_)) => {
                let (val_a, scale_a) = self.unpack_short();
                let (val_b, scale_b) = other.unpack_short();

                let abs_a = val_a.unsigned_abs();
                let abs_b = val_b.unsigned_abs();

                if scale_a == scale_b {
                    abs_a.cmp(&abs_b)
                } else {
                    if scale_a < scale_b {
                        let diff = (scale_b - scale_a) as u32;
                        match 10u128.checked_pow(diff) {
                            Some(pow) => match abs_a.checked_mul(pow) {
                                Some(scaled_a) => scaled_a.cmp(&abs_b),
                                None => Ordering::Greater,
                            },
                            None => Ordering::Greater,
                        }
                    } else {
                        let diff = (scale_a - scale_b) as u32;
                        match 10u128.checked_pow(diff) {
                            Some(pow) => match abs_b.checked_mul(pow) {
                                Some(scaled_b) => abs_a.cmp(&scaled_b),
                                None => Ordering::Less,
                            },
                            None => Ordering::Less,
                        }
                    }
                }
            }

            _ => {
                let (w1, d1, _, _) = self.as_long_view();
                let (w2, d2, _, _) = other.as_long_view();

                match w1.cmp(&w2) {
                    Ordering::Equal => {
                        let len = max(d1.len(), d2.len());

                        for i in 0..len {
                            let v1 = *d1.get(i).unwrap_or(&0);
                            let v2 = *d2.get(i).unwrap_or(&0);

                            match v1.cmp(&v2) {
                                Ordering::Equal => continue,
                                ord => return ord,
                            }
                        }

                        Ordering::Equal
                    }
                    ord => ord,
                }
            }
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
        let w1_end = w1 - (d1.len() as i16) + 1;
        let w2_end = w2 - (d2.len() as i16) + 1;

        let weight_max = max(w1, w2);
        let weight_min = min(w1_end, w2_end);

        let mut digits = Vec::new();
        let mut carry = 0i32;

        for weight in weight_min..=weight_max {
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

    #[inline]
    fn sub_abs(w_large: i16, d_large: &[i16], w_small: i16, d_small: &[i16]) -> Numeric {
        let w_large_end = w_large - (d_large.len() as i16) + 1;
        let w_small_end = w_small - (d_small.len() as i16) + 1;

        let weight_max = w_large;
        let weight_min = min(w_large_end, w_small_end);

        let mut digits = Vec::new();
        let mut borrow: i32 = 0;

        for weigth in weight_min..=weight_max {
            let val_large = match weigth <= w_large && weigth >= w_large_end {
                true => d_large[(w_large - weigth) as usize] as i32,
                _ => 0,
            };

            let val_small = match weigth <= w_small && weigth >= w_small_end {
                true => d_small[(w_small - weigth) as usize] as i32,
                _ => 0,
            };
            let mut diff = val_large - val_small - borrow;

            match diff < 0 {
                true => {
                    diff += 10000;
                    borrow = 1;
                }
                _ => borrow = 0,
            };

            digits.push(diff as i16);
        }

        if borrow > 0 {
            panic!("sub_abs underflow. inputs must be sorted |A| >= |B|.");
        }

        digits.reverse();
        let mut weight = weight_max;
        let mut offset = 0;

        while offset < digits.len() && digits[offset] == 0 {
            offset += 1;
        }

        if offset > 0 {
            digits.drain(0..offset);
            weight -= offset as i16;
        }

        if digits.is_empty() {
            return Numeric::Long {
                weight: 0,
                sign_dscale: 0,
                digits: vec![0],
            };
        }

        Numeric::Long {
            weight,
            sign_dscale: 0,
            digits,
        }
    }

    /// standardise short and long format access
    #[inline]
    fn as_long_view(&self) -> (i16, Cow<'_, [i16]>, bool, u16) {
        match self {
            Self::NaN => panic!("NaN must be handled elsewhere"),

            Self::Short(_) => {
                let (v, s) = self.unpack_short();

                if v == 0 {
                    return (0, Cow::Owned(vec![]), false, s);
                }

                let mut abs = v.unsigned_abs();
                let align_pad = s % 4;
                if align_pad > 0 {
                    let multiplier = 10u128.pow((4 - align_pad) as u32);
                    abs *= multiplier;
                }

                let mut digits = Vec::new();
                let mut remaining = abs;

                while remaining > 0 {
                    digits.push((remaining % N_BASE as u128) as i16);
                    remaining /= N_BASE as u128;
                }
                digits.reverse();

                let fract_groups = (s + 3) / 4;
                let weight = (digits.len() as i16) - (fract_groups as i16) - 1;

                (weight, Cow::Owned(digits), v.is_negative(), s)
            }

            Self::Long {
                weight,
                sign_dscale,
                digits,
            } => {
                let is_neg = (sign_dscale & NUMERIC_NEG) != 0;
                let scale = sign_dscale & DSCALE_MASK;

                (*weight, Cow::Borrowed(digits), is_neg, scale)
            }
        }
    }

    #[inline]
    const fn set_sign(&mut self, neg: bool) {
        match self {
            Self::Long { sign_dscale, .. } => match neg {
                true => *sign_dscale |= NUMERIC_NEG,
                _ => *sign_dscale &= !NUMERIC_NEG,
            },
            Self::Short(packed) => match neg {
                true => *packed |= 1 << SIGN_SHIFT,
                _ => *packed &= !(1 << SIGN_SHIFT),
            },
            Self::NaN => {}
        }
    }

    #[inline]
    const fn set_scale(&mut self, scale: u16) {
        match self {
            Self::Long { sign_dscale, .. } => {
                *sign_dscale = (*sign_dscale & NUMERIC_NEG) | (scale & DSCALE_MASK)
            }

            Self::Short(packed) => {
                *packed &= !(SCALE_MASK);
                *packed |= (scale as u64) << SCALE_SHIFT;
            }

            _ => {}
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
            _ => match (self.is_negative(), other.is_negative()) {
                (false, false) => self.cmp_abs(other),
                (true, true) => other.cmp_abs(self),
                (false, true) => Ordering::Greater,
                (true, false) => Ordering::Less,
            },
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

impl Neg for Numeric {
    type Output = Self;

    #[inline]
    fn neg(mut self) -> Self::Output {
        self.set_sign(!self.is_negative());
        self
    }
}

impl<'a, 'b> Add<&'b Numeric> for &'a Numeric {
    type Output = Numeric;

    #[inline]
    /// Hardly based on [postgres](https://github.com/postgres/postgres/blob/7d9043aee803bf9bf3307ce5f45f3464ea288cb1/src/backend/utils/adt/numeric.c#L8062) implementation.
    fn add(self, rhs: &'b Numeric) -> Self::Output {
        if self.is_nan() || rhs.is_nan() {
            return Numeric::NaN;
        }

        if self.is_zero() {
            return rhs.clone();
        }
        if rhs.is_zero() {
            return self.clone();
        }

        if let (Numeric::Short(_), Numeric::Short(_)) = (&self, &rhs) {
            let (val_a, scale_a) = self.unpack_short();
            let (val_b, scale_b) = rhs.unpack_short();

            let res_scale = max(scale_a, scale_b);

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
        let (w_b, digits_b, neg_b, scale_b) = rhs.as_long_view();
        let scale = max(scale_a, scale_b);

        match neg_a == neg_b {
            // case: (+A) + (+B) or (-A) + (-B)
            // operation has the same magnitudes
            // results the same sign as A
            true => {
                let mut result = Numeric::add_abs(w_a, &digits_a, w_b, &digits_b);
                result.set_sign(neg_a);
                result.set_scale(max(scale_a, scale_b));

                result
            }

            // case: (+A) + (-B) or (-A) + (+B)
            // operation is a sub magnitudes: |A| - |B|
            // results sign need to involve the comparison of magnitudes
            _ => match Numeric::cmp_abs(&self, &rhs) {
                Ordering::Equal => Numeric::from_scaled_i128(0, scale).unwrap(),
                Ordering::Greater => {
                    let mut result = Numeric::sub_abs(w_a, &digits_a, w_b, &digits_b);
                    result.set_sign(neg_a);
                    result.set_scale(scale);

                    result
                }
                Ordering::Less => {
                    let mut result = Numeric::sub_abs(w_b, &digits_b, w_a, &digits_a);
                    result.set_sign(neg_b);
                    result.set_scale(scale);

                    result
                }
            },
        }
    }
}

impl Add for Numeric {
    type Output = Self;
    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        &self + &rhs
    }
}

impl<'n> Add<&'n Numeric> for Numeric {
    type Output = Numeric;
    #[inline]
    fn add(self, rhs: &'n Numeric) -> Self::Output {
        &self + rhs
    }
}

impl<'n> Add<Numeric> for &'n Numeric {
    type Output = Numeric;
    #[inline]
    fn add(self, rhs: Numeric) -> Self::Output {
        self + &rhs
    }
}

impl<'a, 'b> Sub<&'b Numeric> for &'a Numeric {
    type Output = Numeric;

    #[inline]
    /// Based on postgres `sub_var` implementation.
    /// Conceptually A - B is A + (-B).
    fn sub(self, rhs: &'b Numeric) -> Self::Output {
        if self.is_nan() || rhs.is_nan() {
            return Numeric::NaN;
        }

        match (self.is_zero(), rhs.is_zero()) {
            (true, _) => {
                let mut res = rhs.clone();
                res.set_sign(!rhs.is_negative());
                return res;
            }
            (_, true) => return self.clone(),
            _ => {}
        };

        if let (Numeric::Short(_), Numeric::Short(_)) = (self, rhs) {
            let (val_a, scale_a) = self.unpack_short();
            let (val_b, scale_b) = rhs.unpack_short();

            let scale = max(scale_a, scale_b);

            let a = match scale_a < scale {
                true => val_a.checked_mul(10i128.pow((scale - scale_a) as u32)),
                _ => Some(val_a),
            };

            let b = match scale_b < scale {
                true => val_b.checked_mul(10i128.pow((scale - scale_b) as u32)),
                _ => Some(val_b),
            };

            if let (Some(a), Some(b)) = (a, b) {
                if let Some(diff) = a.checked_sub(b) {
                    if let Ok(res) = Numeric::from_scaled_i128(diff, scale) {
                        return res;
                    }
                }
            }
        }

        let (w_a, digits_a, neg_a, scale_a) = self.as_long_view();
        let (w_b, digits_b, neg_b, scale_b) = rhs.as_long_view();
        let scale = max(scale_a, scale_b);

        // in subtraction:
        // different signs (A - (-B)) -> add magnitudes
        // same signs (A - B) -> subtract magnitudes
        match neg_a != neg_b {
            true => {
                let mut res = Numeric::add_abs(w_a, &digits_b, w_b, &digits_b);
                res.set_sign(neg_a);
                res.set_scale(scale);
                res
            }
            _ => {
                // case: (+A) - (+B) -> |A| - |B|
                // case: (-A) - (-B) -> -|A| + |B| -> |B| - |A|
                match Numeric::cmp_abs(self, rhs) {
                    Ordering::Equal => Numeric::from_scaled_i128(0, scale).unwrap(),
                    Ordering::Greater => {
                        // |A| > |B|
                        // if (+A) - (+B) -> positive (neg_a)
                        // if (-A) - (-B) -> negative (neg_a)
                        let mut result = Numeric::sub_abs(w_a, &digits_a, w_b, &digits_b);
                        result.set_sign(neg_a);
                        result.set_scale(scale);
                        result
                    }

                    Ordering::Less => {
                        // |A| < |B|
                        // if (+A) - (+B) -> negative (!neg_a)
                        // if (-A) - (-B) -> positive (!neg_a)
                        let mut result = Numeric::sub_abs(w_b, &digits_b, w_a, &digits_a);
                        result.set_sign(!neg_a);
                        result.set_scale(scale);
                        result
                    }
                }
            }
        }
    }
}

impl Sub<Numeric> for Numeric {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Numeric) -> Self::Output {
        &self - &rhs
    }
}

impl<'n> Sub<&'n Numeric> for Numeric {
    type Output = Numeric;

    #[inline]
    fn sub(self, rhs: &'n Numeric) -> Self::Output {
        &self + rhs
    }
}

impl<'n> Sub<Numeric> for &'n Numeric {
    type Output = Numeric;

    #[inline]
    fn sub(self, rhs: Numeric) -> Self::Output {
        self + &rhs
    }
}

impl<'a, 'b> Mul<&'b Numeric> for &'a Numeric {
    type Output = Numeric;

    #[inline]
    fn mul(self, rhs: &'b Numeric) -> Self::Output {
        let (w1, dig1, neg1, d1) = self.as_long_view();
        let (w2, dig2, neg2, d2) = rhs.as_long_view();

        if dig1.is_empty() || dig2.is_empty() {
            return Numeric::Long {
                weight: 0,
                sign_dscale: d1 + d2,
                digits: vec![],
            };
        }

        let mut acc = vec![0i32; dig1.len() + dig2.len()];

        for (idx, &lhs) in dig1.iter().enumerate() {
            if lhs == 0 {
                continue;
            }

            for (sec, &rhs) in dig2.iter().enumerate() {
                acc[idx + sec + 1] += (lhs as i32) * (rhs as i32);
            }
        }

        for i in (1..acc.len()).rev() {
            let val = acc[i];
            let carry = val / N_BASE;
            acc[i - 1] += carry;
            acc[i] = val % N_BASE;
        }

        let mut start = 0;
        while start < acc.len() && acc[start] == 0 {
            start += 1;
        }

        let mut end = acc.len();
        while end > start && acc[end - 1] == 0 {
            end -= 1;
        }

        let dscale = d1 + d2;
        let mut sign_dscale = dscale & DSCALE_MASK;
        if neg1 ^ neg2 {
            sign_dscale |= SIGN_MASK as u16;
        }

        if start >= end {
            return Numeric::Long {
                weight: 0,
                sign_dscale,
                digits: vec![],
            };
        };

        let digits = acc[start..end]
            .iter()
            .map(|&x| x as i16)
            .collect::<Vec<i16>>();

        let weight = (w1 + w2 + 1) - (start as i16);
        Numeric::Long {
            weight,
            sign_dscale,
            digits,
        }
    }
}

impl Mul<Numeric> for Numeric {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Numeric) -> Self::Output {
        &self * &rhs
    }
}

impl<'n> Mul<&'n Numeric> for Numeric {
    type Output = Numeric;

    #[inline]
    fn mul(self, rhs: &'n Numeric) -> Self::Output {
        &self * rhs
    }
}

impl<'n> Mul<Numeric> for &'n Numeric {
    type Output = Numeric;

    #[inline]
    fn mul(self, rhs: Numeric) -> Self::Output {
        self * &rhs
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

impl From<i128> for Numeric {
    #[inline]
    fn from(value: i128) -> Self {
        Self::from_scaled_i128(value, 0).expect("i128 must always fit into numeric")
    }
}

impl TryFrom<f64> for Numeric {
    type Error = NumericError;

    #[inline]
    fn try_from(value: f64) -> Result<Self, Self::Error> {
        if value.is_nan() {
            return Ok(Self::NaN);
        }
        if value.is_infinite() {
            return Err(NumericError::Overflow);
        }

        Self::from_str(&value.to_string())
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
                weight,
            } => {
                if digits.is_empty() {
                    return write!(f, "0");
                }

                let is_negative = (*sign_dscale & SIGN_DSCALE_MASK) == NUMERIC_NEG;
                let scale = (sign_dscale & DSCALE_MASK) as usize;

                if is_negative {
                    write!(f, "-")?;
                }

                let mut decimal_str = String::new();

                match *weight < 0 {
                    true => {
                        // case 1: pure fraction (0.00...xyz)
                        // weight < 0 means the number is smaller than 0.0001
                        let zeros = (-weight - 1) as usize;
                        decimal_str.push_str("0.");
                        // The 'gap' between 0. and our digits
                        decimal_str.push_str(&"0000".repeat(zeros));

                        digits
                            .iter()
                            .for_each(|d| decimal_str.push_str(&format!("{:04}", d)));
                    }

                    _ => {
                        // case 2: number with integer part (123.456)
                        for (i, &digit) in digits.iter().enumerate() {
                            match i == 0 {
                                true => decimal_str.push_str(&format!("{}", digit)),
                                _ => decimal_str.push_str(&format!("{:04}", digit)),
                            }
                            // insert the decimal point naturally
                            // weight is the index of the 10^0 group.
                            if scale > 0 && i as i16 == *weight {
                                decimal_str.push('.');
                            }
                        }
                    }
                }

                if scale > 0 {
                    // 1. ensure dot exists (edge case: 1.00 where digits=[1])
                    if !decimal_str.contains('.') {
                        decimal_str.push('.');
                    }

                    // 2. pad right if needed (e.g. 1.00 -> "1." -> "1.00")
                    let dot_pos = decimal_str.find('.').unwrap();
                    let current_decimals = decimal_str.len() - dot_pos - 1;
                    if current_decimals < scale {
                        decimal_str.push_str(&"0".repeat(scale - current_decimals));
                    }

                    // 3. truncate excess
                    let max_len = dot_pos + 1 + scale;
                    if decimal_str.len() > max_len {
                        decimal_str.truncate(max_len);
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

    fn num(val: i128, scale: u16) -> Numeric {
        Numeric::from_scaled_i128(val, scale).expect("Invalid test numeric data")
    }

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
    fn add_basic_numerics() {
        let (a, b) = (num(1, 0), num(2, 0));
        let result = a + b;

        match result {
            Numeric::Short(_) => assert_eq!(result.unpack_short(), (3, 0)),
            _ => panic!(),
        }
    }

    #[test]
    fn add_decimals_same_scale() {
        let (a, b) = (num(11, 1), num(22, 1));
        let result = a + b;

        assert_eq!(result.unpack_short(), (33, 1));
    }

    #[test]
    fn aligment_boundary_crossing() {
        let (a, b) = (num(12345, 3), num(1, 4));
        let result = a + b;
        let (val, scale) = result.unpack_short();

        assert_eq!((val, scale), (123451, 4));
    }

    #[test]
    fn add_diff_sign() {
        let (a, b) = (num(10, 0), num(-3, 0));
        let result = a + b;

        assert_eq!(result.unpack_short(), (7, 0));
    }

    #[test]
    fn sub_across_chunks() {
        let (a, b) = (num(10000, 4), num(-1, 4));
        let res = a + b;

        assert_eq!(res.unpack_short(), (9999, 4));
    }

    #[test]
    fn result_zero() {
        let (a, b) = (num(5, 0), num(-5, 0));
        let result = a + b;

        assert_eq!(result.unpack_short(), (0, 0));
    }

    #[test]
    fn add_long_format() {
        let (a, b) = (num(1, 131), num(2, 131));
        let result = a + b;

        // scale 131 means we need 33 groups of 4 digits (132 digits capacity).
        // 131 % 4 = 3, so we pad the single digit by 10^1.
        // 3 becomes 30 internally.
        match result {
            Numeric::Long {
                weight,
                sign_dscale,
                ref digits,
            } => {
                assert_eq!(digits, &vec![30]);
                assert_eq!(weight, -33);
                assert_eq!(sign_dscale & DSCALE_MASK, 131);
            }
            _ => panic!(),
        };

        let s = result.to_string();
        let zeros = s.matches('0').count();

        assert!(s.starts_with('0'));
        assert!(s.ends_with('3'));
        assert_eq!(zeros, 131);
    }

    #[test]
    fn display_fractions() {
        let n = Numeric::from_str("0.00001234").unwrap();
        assert_eq!(n.to_string(), "0.00001234");

        let n = Numeric::from_str("0.0001").unwrap();
        assert_eq!(n.to_string(), "0.0001");

        let n = Numeric::from_str("0.00000001").unwrap();
        assert_eq!(n.to_string(), "0.00000001")
    }

    #[test]
    fn arithmetic_add_integers() {
        let a = Numeric::from(100u64);
        let b = Numeric::from(200u64);
        assert_eq!(a + b, Numeric::from(300u64));

        let big_short = Numeric::from(1_000_000_000_000u64);
        let sum = &big_short + &big_short;
        assert_eq!(i64::from(sum), 2_000_000_000_000);
    }

    #[test]
    fn arithmetic_mixed_signs() {
        let a = Numeric::from(10i64);
        let b = Numeric::from(-3i64);
        assert_eq!(a + b, Numeric::from(7i64));

        let a = Numeric::from(3i64);
        let b = Numeric::from(-10i64);
        assert_eq!(a + b, Numeric::from(-7i64));

        let a = Numeric::from(-10i64);
        let b = Numeric::from(3i64);
        assert_eq!(a + b, Numeric::from(-7i64));
    }

    #[test]
    fn nan_propagation() {
        let n = Numeric::from(10u64);
        let nan = Numeric::NaN;

        assert!((&n + &nan).is_nan());
        assert!((nan + n).is_nan());
    }

    #[test]
    fn comparison_order() {
        let nums = vec![
            Numeric::from(-10i64),
            Numeric::from(-5i64),
            Numeric::zero(),
            Numeric::from(5u64),
            Numeric::from(10u64),
            Numeric::NaN,
        ];

        for i in 0..nums.len() - 1 {
            assert!(nums[i] < nums[i + 1], "order failed at index {}", i);
        }
    }

    #[test]
    fn arithmetic_sub_basic() {
        let a = Numeric::from(10i64);
        let b = Numeric::from(5i64);
        assert_eq!(a - b, Numeric::from(5i64));

        let a = Numeric::from(10i64);
        let b = Numeric::from(20i64);
        assert_eq!(a - b, Numeric::from(-10i64));

        let a = Numeric::from(10i64);
        let b = Numeric::from(-5i64);
        assert_eq!(a - b, Numeric::from(15i64));

        let a = Numeric::from(-10i64);
        let b = Numeric::from(-5i64);
        assert_eq!(a - b, Numeric::from(-5i64));
    }

    #[test]
    fn arithmetic_sub_zero_identity() {
        let a = Numeric::from(42u64);
        let zero = Numeric::zero();

        assert_eq!(&a - &zero, a);
        assert_eq!(&zero - &a, Numeric::from(-42i64));
        assert_eq!(&a - &a, zero);
    }

    #[test]
    fn arithmetic_sub_decimals() {
        let a = Numeric::from_str("0.3").unwrap();
        let b = Numeric::from_str("0.1").unwrap();
        assert_eq!((&a - &b).to_string(), "0.2");

        let a = Numeric::from_str("1.0000").unwrap();
        let b = Numeric::from_str("0.0001").unwrap();
        let res = a - b;
        assert_eq!(res.to_string(), "0.9999");

        let a = Numeric::from_str("0.5").unwrap();
        let b = Numeric::from_str("0.6").unwrap();
        assert_eq!((a - b).to_string(), "-0.1");
    }

    #[test]
    fn arithmetic_sub_tiny_numbers() {
        let a = num(3, 131);
        let b = num(1, 131);
        let res = &a - &b;

        let s = res.to_string();
        assert!(s.ends_with("2"));
        assert_eq!(s.matches('0').count(), 131);

        let a = num(1, 131);
        let b = num(2, 131);
        let res = a - b;

        let s = res.to_string();
        assert_eq!(s.matches('0').count(), 131);
        assert!(s.starts_with("-0."));
        assert!(s.ends_with("1"));
    }

    #[test]
    fn arithmetic_mul_basic() {
        let a = Numeric::from(10u64);
        let b = Numeric::from(20u64);
        assert_eq!((a * b).to_string(), "200");

        let a = Numeric::from_str("0.5").unwrap();
        let b = Numeric::from_str("0.5").unwrap();
        assert_eq!((a * b).to_string(), "0.25");

        let a = Numeric::from_str("1.2").unwrap();
        let b = Numeric::from_str("1.2").unwrap();
        assert_eq!((a * b).to_string(), "1.44");
    }

    #[test]
    fn arithmetic_mul_mixed_signs() {
        let a = Numeric::from(10i64);
        let b = Numeric::from(-5i64);
        assert_eq!((&a * &b).to_string(), "-50");

        let a = Numeric::from(-10i64);
        let b = Numeric::from(-5i64);
        assert_eq!((a * b).to_string(), "50");
    }

    #[test]
    fn invalid_input_handling() {
        assert_eq!(Numeric::from_str("abc"), Err(NumericError::InvalidFormat));
        assert_eq!(
            Numeric::from_str("12.34.56"),
            Err(NumericError::InvalidFormat)
        );
        assert_eq!(Numeric::from_str(""), Err(NumericError::InvalidFormat));
        assert_eq!(Numeric::try_from(&[][..]), Err(NumericError::InvalidFormat));

        let bytes = vec![0u8; 4];
        assert_eq!(
            Numeric::try_from(bytes.as_slice()),
            Err(NumericError::InvalidFormat)
        );
    }
}
