// ! Disk management and B-Tree data structure implementation.

use crate::core::page::SlotId;
use crate::core::{byte_len_of_int_type, utf_8_length_bytes, PageNumber};
use crate::sql::statements::Type;
use std::cmp::Ordering;

/// B-Tree data structure hardly inspired on SQLite B*-Tree.
/// [This](https://www.youtube.com/watch?v=aZjYr87r1b8) video is a great introduction to B-trees and its idiosyncrasies and singularities.
/// Checkout [here](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html) to see a visualizer of this data structure.
pub(in crate::core) struct BTree<Cmp> {
    root: PageNumber,
    min_keys: usize,
    balanced_siblings: usize,

    /// The bytes comparator used to get [`Ordering`].
    comparator: Cmp,
}

struct Sibling {
    page: PageNumber,

    /// The index of the [Cell](crate::core::page::Cell) that points to this node.
    index: SlotId,
}

/// Result of a search in [`Btree].
struct Search {
    page: PageNumber,

    /// Stores the searched element index on [`Ok`] or
    /// the index where it should be on [`Err`]
    index: Result<u16, u16>,
}

/// Compares the `self.0` using a `memcmp`.
/// If the integer keys at the beginning of buffer array are stored as big endians,
/// that's all needed to determine its [`Ordering`].
pub(crate) struct FixedSizeCmp(pub usize);

/// Compares UTF-8 strings.
#[derive(Debug, PartialEq)]
pub(crate) struct StringCmp(pub usize);

/// No allocations comparing to [`Box`].
pub(crate) enum BTreeKeyCmp {
    MemCmp(FixedSizeCmp),
    StrCmp(StringCmp),
}

/// Represents the result of reading content from the [`BTree`].
enum Content<'a> {
    /// Content was found within a single page and can be accessed directly as a slice.
    PageRef(&'a [u8]),
    /// The content spans multiple pages and has been reassembled into a contiguous buffer.
    Reassembled(Box<[u8]>),
}

/// Specifies the search direction for finding a key within a leaf node of a [`BTree`].
///
/// Max: Search for the maximum key within the leaf node.
///
/// Min: Search for the minimum key within the leaf node.
enum LeafKeySearch {
    /// Search for the maximum key.
    Max,
    /// Search for the minimum key.
    Min,
}

const DEFAULT_BALANCED_SIBLINGS: usize = 1;
const DEFAULT_MIN_KEYS: usize = 4;

/// Key comparator to [`BTree`].
/// Compares two keys to determine their [`Ordering`].
pub(crate) trait BytesCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering;
}

impl<Cmp: BytesCmp> BTree<Cmp> {
    pub fn new(root: PageNumber, comparator: Cmp) -> Self {
        Self {
            root,
            comparator,
            balanced_siblings: DEFAULT_BALANCED_SIBLINGS,
            min_keys: DEFAULT_MIN_KEYS,
        }
    }
}

impl Sibling {
    pub fn new(page: PageNumber, index: u16) -> Self {
        Self { page, index }
    }
}

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

impl<'a> AsRef<[u8]> for Content<'a> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::PageRef(reference) => reference,
            Self::Reassembled(boxed) => boxed,
        }
    }
}
