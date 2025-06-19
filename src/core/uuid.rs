//! UUID generation internal module.

#![allow(dead_code)]
use super::random::{random_seed, Rng};
use std::fmt::Display;

#[repr(C, packed)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
/// A Universally Unique Identifier.
/// To learn more about UUIDs on databases,
/// read [this](https://planetscale.com/blog/the-problem-with-using-a-uuid-primary-key-in-mysql) article about MySql
/// and [this](https://supabase.com/blog/choosing-a-postgres-primary-key) on Postgres' side.
pub(crate) struct Uuid([u8; 16]);

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

impl Display for Uuid {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let formated = self.format_hyphenated();
        let string = unsafe { std::str::from_utf8_unchecked(&formated) };

        f.write_str(string)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
}
