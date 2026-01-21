//! # Bloom filter optimisation.
//!
//! This Bloom filter is constructed during the hash join build phase to
//! probabilistically summarise the set of join keys. The resulting filter
//! may be propagated to the probe side and applied as a runtime filter,
//! allowing tuples that are guaranteed not to match to be discarded early.
//!
//! # Example
//!
//! Consider the following query:
//!
//! ```sql
//! SELECT *
//! FROM sessions AS s
//! JOIN users AS u ON s.user_id = u.id;
//! ```
//!
//! Without a Bloom filter, execution proceeds as a conventional hash join:
//!
//! 1. Build a hash table from `users.id`.
//! 2. Scan `sessions` and probe the hash table for every row,
//!    even when `user_id` values cannot possibly match.
//!
//! With a Bloom filter enabled:
//!
//! 1. During the build phase, a Bloom filter is populated with the
//!    `users.id` values.
//! 2. The Bloom filter is pushed down to the `sessions` scan.
//! 3. Each `sessions.user_id` is first tested against the filter.
//!    Rows that are guaranteed not to match are discarded immediately,
//!    and only potential matches reach the hash table probe.
//!
//! Read more in: <https://en.wikipedia.org/wiki/Bloom_filter>

use crate::sql::Value;

#[derive(Debug)]
pub(crate) struct BloomFilter {
    bits: Vec<usize>,
    num_bits: usize,
    num_hashes: usize,
    elements: usize,
}

impl BloomFilter {
    const MAX_BITS: usize = 1 << 23;
    const MIN_BITS: usize = 1 << 6;
    const NUMBER_OF_HASHES: usize = 7;

    pub fn new(expected: usize, false_positive: f64) -> Self {
        use std::f64::consts::LN_2;
        use std::ops::Neg;

        let rate = false_positive.clamp(0.0001, 0.5);
        // see: <https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions>

        // m = -n ln(p) / (ln 2)^2
        let bits = ((expected as f64).neg() * rate.ln() / (LN_2 * LN_2)).ceil() as usize;
        let bits = bits.clamp(Self::MIN_BITS, Self::MAX_BITS);
        let bits = bits.div_ceil(64) * 64;

        // k = (m / n) ln 2
        let hashes = (bits as f64 / expected.max(1) as f64 * LN_2).ceil() as usize;
        let hashes = hashes.clamp(1, 15);

        Self {
            bits: vec![0usize; bits / 64],
            num_bits: bits,
            num_hashes: hashes,
            elements: 0,
        }
    }

    pub fn insert(&mut self, value: &Value) {
        self.add(self.hash(value));
        self.elements += 1;
    }

    /// Check if the item has been inserted into this bloom filter.
    /// This function can return false positives, but not false negatives.
    pub fn contains(&self, value: &Value) -> bool {
        self.get(self.hash(value))
    }

    #[inline(always)]
    fn hash(&self, value: &Value) -> u64 {
        use crate::core::BuildHasher;
        use std::hash::{BuildHasher as Build, Hash, Hasher};

        let hasher = &mut BuildHasher::default().build_hasher();
        std::mem::discriminant(value).hash(hasher);

        match value {
            Value::String(string) => string.hash(hasher),
            Value::Number(num) => num.hash(hasher),
            Value::Boolean(bool) => bool.hash(hasher),
            Value::Blob(blob) => blob.hash(hasher),
            Value::Temporal(temporal) => temporal.hash(hasher),
            Value::Uuid(uuid) => uuid.hash(hasher),
            Value::Interval(interval) => interval.hash(hasher),
            Value::Numeric(numeric) => numeric.hash(hasher),
            Value::Float(float) => float.to_bits().hash(hasher),
            Value::Enum(_) | Value::Null => {}
        }

        hasher.finish()
    }

    #[inline(always)]
    fn add(&mut self, hash: u64) {
        use std::ops::Add;

        let h = hash as usize;
        let h_2 = (hash >> 32) as usize;

        let num_bits = self.num_bits;
        let num_hashes = self.num_hashes;
        let bits = self.bits.as_mut_ptr();

        let mut index = 0;

        while index < num_hashes {
            let bit = h
                .wrapping_add(index.wrapping_mul(h_2))
                .wrapping_add(index * index)
                % num_bits;

            let word = bit / 64;
            let offset = bit % 64;

            unsafe {
                *bits.add(word) |= 1usize << offset;
            };

            index += 1;
        }
    }

    #[inline(always)]
    fn get(&self, hash: u64) -> bool {
        use std::ops::Add;

        let h = hash as usize;
        let h_2 = (hash >> 32) as usize;

        let num_bits = self.num_bits;
        let num_hashes = self.num_hashes;
        let bits = self.bits.as_ptr();

        let mut index = 0;

        while index < num_hashes {
            let bit = h
                .wrapping_add(index.wrapping_mul(h_2))
                .wrapping_add(index * index)
                % num_bits;

            let word = bit / 64;
            let offset = bit % 64;

            let word = unsafe { *bits.add(word) };
            if (word & (1usize << offset)) == 0 {
                return false;
            }

            index += 1;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut bloom = BloomFilter::new(1_000, 0.01);
        bloom.insert(&1.into());
        assert!(bloom.contains(&1.into()));
    }
}
