//! Hashing algorithm.
//!
//! This implementation is derived from `rustc-hash`
//! (https://github.com/rust-lang/rustc-hash),
//! licensed under the MIT License.
//! Copyright © the Rust Project Developers.
//!
//! The original documentation explains the design trade-offs
//! and references used.

/// A speedy non-cryptographic hash.
/// As we deal with trusted input (at least in the vast majority)
/// we do not need to trade-off performance for DOS protection.
pub struct Hasher {
    hash: usize,
}

#[derive(Copy, Clone, Default)]
pub struct BuildHasher;

/// Reference: [https://github.com/rust-lang/rustc-hash/blob/1a998d5b89b04ba730d4cd249f811e8b48aa7d8c/src/lib.rs#L63C1-L73C37]
const K: usize = 0xf1357aea2e62a9c5;

const SEEDS: [u64; 2] = [0x243f6a8885a308d3, 0x13198a2e03707344];

const PREVENT_TRIVIAL_ZERO_COLLAPSE: u64 = 0xa4093822299f31d0;

pub type HashMap<K, V> = std::collections::HashMap<K, V, BuildHasher>;
pub type HashSet<V> = std::collections::HashSet<V, BuildHasher>;

#[macro_export]
macro_rules! hash_set {
    ($($elem:expr),+ $(,)?) => {{
        let mut set = HashSet::default();
        $(set.insert($elem);)+
        set
    }};
}

#[macro_export]
macro_rules! hash_map {
    ($($key:expr => $value:expr),+ $(,)?) => {{
        let mut map = HashMap::default();
        $(map.insert($key, $value);)+
        map
    }};
}

impl Hasher {
    #[inline]
    pub const fn new() -> Self {
        Self { hash: 0 }
    }

    #[inline]
    pub const fn add(&mut self, addition: usize) {
        self.hash = self.hash.wrapping_add(addition).wrapping_mul(K);
    }
}

#[inline]
fn hash(bytes: &[u8]) -> u64 {
    let [mut s0, mut s1] = SEEDS;
    let len = bytes.len();

    if len <= 16 {
        if len >= 8 {
            s0 ^= u64::from_le_bytes(bytes[0..8].try_into().unwrap());
            s1 ^= u64::from_le_bytes(bytes[len - 8..].try_into().unwrap());
        } else if len >= 4 {
            s0 ^= u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as u64;
            s1 ^= u32::from_le_bytes(bytes[len - 4..].try_into().unwrap()) as u64;
        } else if len > 0 {
            let lo = bytes[0];
            let mid = bytes[len / 2];
            let hi = bytes[len - 1];
            s0 ^= lo as u64;
            s1 ^= ((hi as u64) << 8) | mid as u64;
        }
    } else {
        let mut off = 0;
        while off < len - 16 {
            let x = u64::from_le_bytes(bytes[off..off + 8].try_into().unwrap());
            let y = u64::from_le_bytes(bytes[off + 8..off + 16].try_into().unwrap());

            let t = multiply(s0 ^ x, PREVENT_TRIVIAL_ZERO_COLLAPSE ^ y);
            s0 = s1;
            s1 = t;
            off += 16;
        }

        let suffix = &bytes[len - 16..];
        s0 ^= u64::from_le_bytes(suffix[0..8].try_into().unwrap());
        s1 ^= u64::from_le_bytes(suffix[8..16].try_into().unwrap());
    }

    multiply(s0, s1) ^ (len as u64)
}

#[inline]
fn multiply(x: u64, y: u64) -> u64 {
    let full = (x as u128).wrapping_mul(y as u128);
    let lo = full as u64;
    let hi = (full >> 64) as u64;

    lo ^ hi
}

impl std::hash::Hasher for Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.write_u64(hash(bytes));
    }

    #[inline]
    fn finish(&self) -> u64 {
        const ROTATE: u32 = 26;

        self.hash.rotate_left(ROTATE) as u64
    }

    fn write_u8(&mut self, addition: u8) {
        self.add(addition as usize);
    }

    fn write_u16(&mut self, addition: u16) {
        self.add(addition as usize);
    }

    fn write_u32(&mut self, addition: u32) {
        self.add(addition as usize);
    }

    fn write_u64(&mut self, addition: u64) {
        self.add(addition as usize);
    }

    fn write_u128(&mut self, addition: u128) {
        self.add(addition as usize);
    }

    fn write_usize(&mut self, addition: usize) {
        self.add(addition);
    }

    fn write_isize(&mut self, addition: isize) {
        self.add(addition as usize);
    }
}

impl Default for Hasher {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl std::hash::BuildHasher for BuildHasher {
    type Hasher = Hasher;

    fn build_hasher(&self) -> Self::Hasher {
        Hasher::default()
    }
}
