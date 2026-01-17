mod collections;
pub(crate) mod date;

use collections::hash;
pub(crate) mod json;
pub(crate) mod log;
pub(crate) mod numeric;
pub(crate) mod random;
pub(crate) use collections::smallvec;
pub(crate) mod storage;
pub(crate) mod uuid;

pub(crate) use hash::{BuildHasher, HashMap, HashSet};

/// This serialises the date types into a compact binary format.
pub trait Serialize {
    fn serialize(&self, buff: &mut Vec<u8>);
}
