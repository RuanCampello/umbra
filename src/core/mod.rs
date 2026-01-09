pub(crate) mod date;
pub(crate) mod json;
pub(crate) mod log;
pub(crate) mod numeric;
pub(crate) mod random;
pub(crate) mod smallvec;
pub(crate) mod storage;
pub(crate) mod uuid;

/// This serialises the date types into a compact binary format.
pub trait Serialize {
    fn serialize(&self, buff: &mut Vec<u8>);
}
