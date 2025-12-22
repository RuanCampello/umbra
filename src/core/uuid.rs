//! UUID generation internal module.

#![allow(dead_code)]

use super::random::{random_seed, Rng};
use std::{fmt::Display, str::FromStr};

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialOrd, Ord, Hash)]
/// A 128 bits Universally Unique Identifier.
/// To learn more about UUIDs on databases,
/// read [this](https://planetscale.com/blog/the-problem-with-using-a-uuid-primary-key-in-mysql) article about MySql
/// and [this](https://supabase.com/blog/choosing-a-postgres-primary-key) on Postgres' side.
pub struct Uuid([u8; 16]);

#[derive(Debug, PartialEq)]
pub enum UuidError {
    InvalidLength,
    InvalidUUID,
}

/// Hexadecimal table
const HEX_TABLE: &[u8; 256] = &{
    let mut buffer = [0; 256];
    let mut idx: u8 = 0;

    loop {
        buffer[idx as usize] = match idx {
            b'0'..=b'9' => idx - b'0',
            b'a'..=b'f' => idx - b'a' + 10,
            b'A'..=b'F' => idx - b'A' + 10,
            _ => 0xff,
        };

        if idx == 255 {
            break buffer;
        }

        idx += 1;
    }
};

/// Bit-wise shift left by four table
const SHL_TABLE: &[u8; 256] = &{
    let mut buffer = [0; 256];
    let mut idx: u8 = 0;

    loop {
        buffer[idx as usize] = idx.wrapping_shl(4);

        if idx == 255 {
            break buffer;
        }

        idx += 1;
    }
};

impl Uuid {
    const VERSION_MASK: u128 = 0xFFFFFFFFFFFF0FFF3FFFFFFFFFFFFFFF;
    const VARIANT_MASK: u128 = 0x40008000000000000000;

    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Creates a new random version 4 `UUID`.
    pub fn new_v4() -> Self {
        let mut rng = Rng::with_seed(random_seed().unwrap_or_default());
        Self::from_u128(rng.u128(u128::MIN..u128::MAX) & Self::VERSION_MASK | Self::VARIANT_MASK)
    }

    pub fn try_parse(input: &str) -> Result<Uuid, UuidError> {
        let bytes = parse_hyphenated(input.as_bytes())?;
        Ok(Self::from_bytes(bytes))
    }

    /// Creates a `UUID` from a 128 bit value.
    const fn from_u128(value: u128) -> Self {
        Self::from_bytes(value.to_be_bytes())
    }

    #[inline(always)]
    const fn get_version(&self) -> usize {
        (self.0[6] >> 4) as usize
    }

    #[inline(always)]
    const fn format_hyphenated(&self) -> [u8; 36] {
        const LOWER: &[u8; 16] = b"0123456789abcdef";
        const GROUPS: [(usize, usize); 5] = [(0, 8), (9, 13), (14, 18), (19, 23), (24, 36)];

        let mut dst = [0u8; 36];
        let bytes = self.0;
        let mut src_idx = 0;
        let mut group_idx = 0;

        while group_idx < 5 {
            let (start, end) = GROUPS[group_idx];
            let mut dst_idx = start;

            while dst_idx < end {
                let byte = bytes[src_idx];
                src_idx += 1;

                dst[dst_idx] = LOWER[(byte >> 4) as usize];
                dst[dst_idx + 1] = LOWER[(byte & 0x0F) as usize];
                dst_idx += 2;
            }

            if group_idx < 4 {
                dst[end] = b'-';
            }
            group_idx += 1;
        }

        dst
    }
}

const fn try_parse(input: &[u8]) -> Result<[u8; 16], UuidError> {
    match (input.len(), input) {
        (45, [b'u', b'r', b'n', b':', b'u', b'u', b'i', b'd', b':', string @ ..]) => {
            parse_hyphenated(string)
        }
        _ => Err(UuidError::InvalidUUID),
    }
}

const fn parse_hyphenated(string: &[u8]) -> Result<[u8; 16], UuidError> {
    if string.len() != 36 {
        return Err(UuidError::InvalidLength);
    }

    match [string[8], string[13], string[18], string[23]] {
        [b'-', b'-', b'-', b'-'] => {}
        _ => return Err(UuidError::InvalidUUID),
    }

    let positions: [u8; 8] = [0, 4, 9, 14, 19, 24, 28, 32];
    let mut buffer: [u8; 16] = [0; 16];
    let mut cursor = 0;

    while cursor < 8 {
        let idx = positions[cursor];

        let h1 = HEX_TABLE[string[idx as usize] as usize];
        let h2 = HEX_TABLE[string[(idx + 1) as usize] as usize];
        let h3 = HEX_TABLE[string[(idx + 2) as usize] as usize];
        let h4 = HEX_TABLE[string[(idx + 3) as usize] as usize];

        if h1 | h2 | h3 | h4 == 0xff {
            return Err(UuidError::InvalidUUID);
        }

        buffer[cursor * 2] = SHL_TABLE[h1 as usize] | h2;
        buffer[cursor * 2 + 1] = SHL_TABLE[h3 as usize] | h4;
        cursor += 1;
    }

    Ok(buffer)
}

#[cfg(all(feature = "simd", target_arch = "x86_64"))]
impl PartialEq for Uuid {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        unsafe {
            use std::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};

            let a = _mm_loadu_si128(self.0.as_ptr() as *const __m128i);
            let b = _mm_loadu_si128(other.0.as_ptr() as *const __m128i);

            let cmp = _mm_cmpeq_epi8(a, b);

            _mm_movemask_epi8(cmp) == 0xFFFF
        }
    }
}

#[cfg(not(all(feature = "simd", target_arch = "x86_64")))]
impl PartialEq for Uuid {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for Uuid {}

impl Display for Uuid {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let formated = self.format_hyphenated();
        let string = unsafe { std::str::from_utf8_unchecked(&formated) };

        f.write_str(string)
    }
}

impl Display for UuidError {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidUUID => f.write_str("Unable to parse this UUID"),
            Self::InvalidLength => f.write_str("This UUID does not have the correct length"),
        }
    }
}

impl FromStr for Uuid {
    type Err = UuidError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_parse(s)
    }
}

impl AsRef<[u8]> for Uuid {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Uuid> for u128 {
    fn from(value: Uuid) -> Self {
        u128::from_be_bytes(value.0)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr};

    use crate::core::random::Rng;

    use super::Uuid;

    #[test]
    fn test_new() {
        let uuid = Uuid::new_v4();

        assert_eq!(uuid.get_version(), 4);
    }

    #[test]
    fn test_v4_basic_randomness() {
        let mut rng = Rng::new();
        let range = rng.i32(0..1024);
        let mut uuids = HashSet::with_capacity(range as usize);

        for _ in 0..range {
            let uuid = Uuid::new_v4();
            assert!(!uuids.contains(&uuid), "Duplicated UUID found {uuid}");

            uuids.insert(uuid);
        }

        assert_eq!(uuids.len(), range as usize);
    }

    #[test]
    fn test_uuid_parsing() {
        assert!(Uuid::from_str("00000000-0000-0000-0000-000000000000").is_ok());
        assert!(Uuid::from_str("123e4567-e89b-12d3-a456-426614174000").is_ok());
        assert!(Uuid::from_str("550e8400-e29b-41d4-a716-446655440000").is_ok());
        assert!(Uuid::from_str("f47ac10b-58cc-4372-a567-0e02b2c3d479").is_ok());

        assert_eq!(
            Uuid::from_str("F47AC10B-58CC-4372-A567-0E02B2C3D479"),
            Uuid::from_str("f47ac10b-58cc-4372-a567-0e02b2c3d479")
        );
    }

    #[test]
    fn test_invalid_uuid_parsing() {
        assert!(Uuid::from_str("123e4567-e89b-12d3-a456-42661417400").is_err());
        assert!(Uuid::from_str("123e4567-e89b-12d3-a456-4266141740000").is_err());
        assert!(Uuid::from_str("123e4567-e89b-12d3-a456-42661417400g").is_err());
        assert!(Uuid::from_str("123e4567e89b-12d3-a456-426614174000").is_err());
        assert!(Uuid::from_str("").is_err());
    }
}
