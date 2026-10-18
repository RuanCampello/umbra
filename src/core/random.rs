//! wyrand (https://github.com/wangyi-fudan/wyhash), seeded from the clock and the thread identity

use std::hash::{BuildHasher, Hash, Hasher};

pub struct Rng(u64);

impl Rng {
    pub fn seeded() -> Self {
        let mut hasher = std::collections::hash_map::RandomState::new().build_hasher();
        std::time::Instant::now().hash(&mut hasher);
        std::thread::current().id().hash(&mut hasher);

        Self(hasher.finish())
    }

    pub fn u64(&mut self) -> u64 {
        const WY_CONST_0: u64 = 0x2d35_8dcc_aa6c_78a5;
        const WY_CONST_1: u64 = 0x8bb8_4b93_962e_acc9;

        self.0 = self.0.wrapping_add(WY_CONST_0);
        let product = u128::from(self.0) * u128::from(self.0 ^ WY_CONST_1);
        (product as u64) ^ (product >> 64) as u64
    }

    pub fn u128(&mut self) -> u128 {
        (u128::from(self.u64()) << 64) | u128::from(self.u64())
    }
}
