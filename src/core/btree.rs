// ! Disk management and B-Tree data structure implementation.

use crate::core::{byte_len_of_int_type, utf_8_length_bytes};
use crate::sql::statements::Type;
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

impl BytesCmp for FixedSizeCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        a[..self.0].cmp(&b[..self.0])
    }
}

impl From<&Type> for Box<dyn BytesCmp> {
    fn from(value: &Type) -> Self {
        match value {
            Type::Varchar(max) => Box::new(StringCmp(utf_8_length_bytes(*max))),
            not_var_type => Box::new(FixedSizeCmp(byte_len_of_int_type(not_var_type))),
        }
    }
}

impl BytesCmp for &Box<dyn BytesCmp> {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        self.as_ref().cmp(a, b)
    }
}

/// No allocations comparing to [`Box`].
pub(crate) enum BTreeKeyCmp {
    MemCmp(FixedSizeCmp),
    StrCmp(StringCmp),
}

impl From<&Type> for BTreeKeyCmp {
    fn from(value: &Type) -> Self {
        match value {
            Type::Varchar(max) => Self::StrCmp(StringCmp(utf_8_length_bytes(*max))),
            not_var_type => Self::MemCmp(FixedSizeCmp(byte_len_of_int_type(not_var_type))),
        }
    }
}

impl BytesCmp for BTreeKeyCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        match self {
            Self::MemCmp(mem) => mem.cmp(a, b),
            Self::StrCmp(str) => str.cmp(a, b),
        }
    }
}
