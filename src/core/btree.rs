// ! Disk management and B-Tree data structure implementation.

use crate::sql::sql::Type;
use std::cmp::Ordering;

/// Key comparator to [`BTree`].
/// Compares two keys to determine their [`Ordering`].
pub(crate) trait BytesCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering;
}

/// Compares the `self.0` using a `memcmp`.
/// If the integer keys at the beginning of buffer array are stored as big endians,
/// that's all needed to determine its [`Ordering`].
pub(crate) struct FixedSizeCmp(pub usize);

impl FixedSizeCmp {
    /// This only make sense with simple data types.
    pub fn new<T>() -> Self {
        Self(size_of::<T>())
    }
}

impl TryFrom<&Type> for FixedSizeCmp {
    type Error = ();

    fn try_from(data_type: &Type) -> Result<Self, Self::Error> {
        match data_type {
            Type::Varchar(_) | Type::Boolean => Err(()),
            fixed => Ok(Self(byte_len_of_int_type(fixed))),
        }
    }
}

/// Compares UTF-8 strings.
#[derive(Debug, PartialEq)]
pub(crate) struct StringCmp(pub usize);

/// Computes the length of a string reading its first `self.0` as a big endian.
impl BytesCmp for StringCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        debug_assert!(
            self.0 <= 4,
            "Strings longer than {} are not supported.",
            u32::MAX,
        );

        let mut buffer = [0; size_of::<usize>()];
        buffer[..self.0].copy_from_slice(&a[..self.0]);
        let len_a = usize::from_le_bytes(buffer);
        buffer.fill(0);
        buffer[..self.0].copy_from_slice(&b[..self.0]);
        let len_b = usize::from_le_bytes(buffer);

        // TODO: check those unwraps
        std::str::from_utf8(&a[self.0..self.0 + len_a])
            .unwrap()
            .cmp(std::str::from_utf8(&b[self.0..self.0 + len_b]).unwrap())
    }
}

/// Returns the byte length of a given SQL [`Type`] for integers.
fn byte_len_of_int_type(data_type: &Type) -> usize {
    match data_type {
        Type::Integer | Type::UnsignedInteger => 4,
        Type::BigInteger | Type::UnsignedBigInteger => 8,
        _ => unreachable!("This must only be used for integers."),
    }
}
