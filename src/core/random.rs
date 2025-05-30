// ! Random numbers implementation for identifiers without dependencies.
// ! This is a simplified version of https://github.com/smol-rs/fastrand implementation.

#![allow(dead_code)]

use core::ops::Bound;
use std::cell::Cell;
use std::hash::Hasher;

/// Random number generator.
#[derive(Debug, PartialEq, Eq)]
pub struct Rng(u64);

/// Grants the [`RNG`] safety even in panic.
pub struct Restore<'r> {
    rng: &'r Cell<Rng>,
    current: Rng,
}

const DEFAULT_SEED: u64 = 0xa3f4c92d57e8b6c3;

std::thread_local! {
    /// The local thread used to storage [`Rng`].
    static RNG_THREAD: Cell<Rng> = Cell::new(Rng(random_seed().unwrap_or(DEFAULT_SEED)))
}

/// Used for generating the implementations of [`Rng`] for integers.
macro_rules! rng_int {
    ($t:tt, $unsigned_t:tt, $gen:tt, $mod:tt, $doc:tt) => {
        #[doc = $doc]
        ///
        /// Panic at the streets of London if the range is empty.
        #[allow(unused)]
        #[inline]
        pub fn $t(&mut self, range: impl std::ops::RangeBounds<$t>) -> $t {
            let panic_empty_range = || {
                panic!(
                    "empty range: {:?}..{:?}",
                    range.start_bound(),
                    range.end_bound()
                )
            };

            let low = match range.start_bound() {
                Bound::Unbounded => $t::MIN,
                Bound::Included(&x) => x,
                Bound::Excluded(&x) => x.checked_add(1).unwrap_or_else(panic_empty_range),
            };

            let high = match range.end_bound() {
                Bound::Unbounded => $t::MAX,
                Bound::Included(&x) => x,
                Bound::Excluded(&x) => x.checked_sub(1).unwrap_or_else(panic_empty_range),
            };

            if low > high {
                panic_empty_range();
            }

            if low == $t::MIN && high == $t::MAX {
                self.$gen() as $t
            } else {
                let len = high.wrapping_sub(low).wrapping_add(1);
                low.wrapping_add(self.$mod(len as $unsigned_t as _) as $t)
            }
        }
    };
}

impl Rng {
    pub fn new() -> Self {
        try_random_thread(Rng::branch).unwrap_or(Rng::with_seed(0xac7e48b36d19f25c))
    }

    /// Creates a new instance of [`Rng`] with a given seed.
    pub fn with_seed(seed: u64) -> Self {
        Rng(seed)
    }

    #[must_use = "This is needed to create an instance of `Rng`"]
    pub fn branch(&mut self) -> Self {
        Rng::with_seed(self.gen_rand_u64())
    }

    rng_int!(
        i16,
        u16,
        gen_rand_u32,
        gen_mod_u32,
        "Generates a random `i16` for this given range."
    );

    rng_int!(
        i32,
        u32,
        gen_rand_u32,
        gen_mod_u32,
        "Generates a random `i32` for this given range."
    );

    rng_int!(
        i64,
        u64,
        gen_rand_u64,
        gen_mod_u64,
        "Generates a random `i64` for this given range."
    );

    rng_int!(
        u16,
        u16,
        gen_rand_u32,
        gen_mod_u32,
        "Generates a random `i16` for this given range."
    );

    rng_int!(
        u32,
        u32,
        gen_rand_u32,
        gen_mod_u32,
        "Generates a random `i32` for this given range."
    );

    rng_int!(
        u64,
        u64,
        gen_rand_u64,
        gen_mod_u64,
        "Generates a random `i64` for this given range."
    );

    fn gen_rand_u64(&mut self) -> u64 {
        // Constants taken from: https://github.com/wangyi-fudan/wyhash/blob/master/wyhash.h#L151.
        const WY_CONST_0: u64 = 0x2d35_8dcc_aa6c_78a5;
        const WY_CONST_1: u64 = 0x8bb8_4b93_962e_acc9;

        let checksum = self.0.wrapping_add(WY_CONST_0);
        self.0 = checksum;

        let tail = u128::from(checksum) * u128::from(checksum ^ WY_CONST_1);
        (tail as u64) ^ (tail >> 64) as u64
    }

    /// Could life ever be sane again?
    fn gen_rand_u32(&mut self) -> u32 {
        self.gen_rand_u64() as u32
    }

    fn gen_mod_u32(&mut self, number: u32) -> u32 {
        // Read more here: https://lemire.me/blog/2016/06/30/fast-random-shuffling
        let mut rand = self.gen_rand_u32();
        let mut high = high_u32_bits(rand, number);
        let mut lower = rand.wrapping_mul(number);

        if lower < number {
            let tail = number.wrapping_neg() % number;
            while lower < tail {
                rand = self.gen_rand_u32();
                high = high_u32_bits(rand, number);
                lower = rand.wrapping_mul(number);
            }
        }

        high
    }

    fn gen_mod_u64(&mut self, number: u64) -> u64 {
        // Read more here: https://lemire.me/blog/2016/06/30/fast-random-shuffling/
        let mut rand = self.gen_rand_u64();
        let mut high = high_u64_bits(rand, number);
        let mut lower = rand.wrapping_mul(number);

        if lower < number {
            let t = number.wrapping_neg() % number;
            while lower < t {
                rand = self.gen_rand_u64();
                high = high_u64_bits(rand, number);
                lower = rand.wrapping_mul(number);
            }
        }

        high
    }
}

/// Tries to apply a given function to the local [`Rng`] thread.
fn try_random_thread<RNG>(
    f: impl FnOnce(&mut Rng) -> RNG,
) -> Result<RNG, std::thread::AccessError> {
    RNG_THREAD.try_with(|rng| {
        let current = rng.replace(Rng(0));
        let mut res = Restore { rng, current };

        f(&mut res.current)
    })
}

fn random_thread<RNG>(f: impl FnOnce(&mut Rng) -> RNG) -> RNG {
    RNG_THREAD.with(|rng| {
        let current = rng.replace(Rng(0));
        let mut res = Restore { rng, current };

        f(&mut res.current)
    })
}

/// Computes the high 32 bits of the 64-bit product of two 32-bit unsigned integers.
///
/// This function multiplies `a` and `b`, both of which are [`u32`], promoting them to [`u64`]
/// to avoid overflow. The product is then right-shifted by 32 bits to extract the higher
/// 32 bits, which are returned as a [`u32`].
#[inline]
fn high_u32_bits(a: u32, b: u32) -> u32 {
    (((a as u64) * (b as u64)) >> 32) as u32
}

/// The same as [`high_u32_bits`], but for [`u64`].
fn high_u64_bits(a: u64, b: u64) -> u64 {
    (((a as u128) * (b as u128)) >> 64) as u64
}

/// Generates a random seed using [`std::collections::hash_map::HashMap`].
/// For more information about random numbers' generation in rust, see the discussion [here](https://blog.orhun.dev/zero-deps-random-in-rust/).
fn random_seed() -> Option<u64> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::thread;
    use std::time::Instant;

    let mut hasher = DefaultHasher::new();
    Instant::now().hash(&mut hasher);
    thread::current().id().hash(&mut hasher);

    Some(hasher.finish())
}

#[cfg(test)]
mod tests {
    use crate::core::random::*;
    use std::collections::HashSet;

    #[test]
    fn test_seed_generation_randomness() {
        let mut rand = Rng::new();
        let range = rand.u16(0..128);
        let mut seeds = HashSet::new();

        for _ in 0..range {
            let seed = random_seed().expect("Unable to generate seed");

            assert!(!seeds.contains(&seed), "Duplicated seed {seed} found");
            assert_eq!(size_of_val(&seed), size_of::<u64>()); // grants it's a u64
            seeds.insert(seed);
        }

        assert_eq!(seeds.len(), range as usize);
    }

    #[test]
    fn test_seed_determinism() {
        let seeds: [u64; 4] = [1u64, 12345u64, 67890u64, 98765u64];

        for seed in seeds {
            let mut rng_one = Rng::with_seed(seed);
            let mut rng_two = Rng::with_seed(seed);

            let range = rng_one.i32(4..32);
            let range_two = rng_two.i32(4..32);

            let numbers_of_one: Vec<i32> = (0..range).map(|_| rng_one.i32(0..range)).collect();
            let numbers_of_two: Vec<i32> =
                (0..range_two).map(|_| rng_two.i32(0..range_two)).collect();

            assert_eq!(numbers_of_one, numbers_of_two);
        }
    }
}
