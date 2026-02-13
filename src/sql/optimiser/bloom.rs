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

    pub fn with_capacity(capacity: usize) -> Self {
        Self::new(capacity, 0.01)
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

    pub const fn len(&self) -> usize {
        self.elements
    }

    #[inline(always)]
    pub fn union(&mut self, other: &Self) {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            panic!("Invalid Bloom filters for union: number of bits: ({}, {}) and number of hashes: ({}, {})",
                self.num_hashes, other.num_hashes, self.num_hashes, other.num_hashes);
        }

        self.bits
            .iter_mut()
            .zip(other.bits.iter())
            .for_each(|(w, w2)| *w |= *w2);
        self.elements += other.elements;
    }

    #[inline(always)]
    pub fn load_factor(&self) -> f64 {
        let setted: usize = self.bits.iter().map(|b| b.count_ones() as usize).sum();
        setted as f64 / self.num_bits as f64
    }

    #[inline(always)]
    fn hash(&self, value: &Value) -> u64 {
        use crate::collections::hash::BuildHasher;
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

impl Default for BloomFilter {
    fn default() -> Self {
        Self::with_capacity(1 << 10)
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

    #[test]
    fn union() {
        let mut bloom = BloomFilter::with_capacity(124);
        let mut bloom_2 = BloomFilter::with_capacity(124);

        bloom.insert(&1.into());
        bloom.insert(&2.into());
        bloom_2.insert(&3.into());

        bloom.union(&bloom_2);

        assert!(bloom.contains(&1.into()));
        assert!(bloom.contains(&2.into()));
        assert!(bloom.contains(&3.into()));
    }

    #[test]
    fn false_positive() {
        let mut bloom = BloomFilter::default();
        let mut false_positives = 0;

        for idx in 0..1024 {
            bloom.insert(&idx.into());
        }

        /// none of this values is contained in the bloom filter, of course.
        for idx in 10_000..15_000 {
            if bloom.contains(&idx.into()) {
                false_positives += 1;
            }
        }

        let rate = false_positives as f64 / 5_000.0;
        assert!(
            rate < 0.05,
            "False positive rate was to high: {false_positives:.2} expected ~0.01"
        );
    }

    #[test]
    fn load_factor() {
        let mut bloom = BloomFilter::default();
        assert!(bloom.load_factor() < 0.01);

        for idx in 0..1024 {
            bloom.insert(&idx.into());
        }

        let load_factor = bloom.load_factor();
        assert!(load_factor > 0.3 && load_factor < 0.7);
    }
}
