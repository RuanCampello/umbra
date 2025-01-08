//! Disk management and B-Tree data structure implementation.

use super::page::{Cell, Page, PageConversion, SlotId};
use super::pagination::io::FileOperations;
use super::pagination::pager::Pager;
use super::{byte_len_of_int_type, utf_8_length_bytes, PageNumber};
use crate::core::page::overflow::OverflowPage;
use crate::sql::statements::Type;
use std::cmp::{min, Ordering};
use std::io::{Read, Seek, Write};

/// B-Tree data structure hardly inspired on SQLite B*-Tree.
/// [This](https://www.youtube.com/watch?v=aZjYr87r1b8) video is a great introduction to B-trees and its idiosyncrasies and singularities.
/// Checkout [here](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html) to see a visualizer of this data structure.
pub(in crate::core) struct BTree<'p, File, Cmp> {
    root: PageNumber,
    min_keys: usize,
    balanced_siblings: usize,
    // TODO: this should be ideally a owned value not a mutable reference.
    pager: &'p mut Pager<File>,
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
#[derive(Default)]
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
pub(in crate::core) enum Content<'a> {
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

impl<'p, File: Read + Write + Seek + FileOperations, Cmp: BytesCmp> BTree<'p, File, Cmp> {
    /// Returns a value of a given key.
    pub fn get(&mut self, entry: &[u8]) -> std::io::Result<Option<Content>> {
        todo!()
    }

    pub fn insert(&mut self, entry: Vec<u8>) -> std::io::Result<()> {
        let mut parents = Vec::new();
        let search = self.search(self.root, &entry, &mut parents)?;

        let mut cell = self.allocate_cell(entry)?;
        let node = self.pager.get_mut(search.page)?;

        match search.index {
            Ok(idx) => {
                cell.header.left_child = node.cell(idx).header.left_child;
                let curr_cell = node.replace(cell, idx);

                todo!()
            }
            Err(idx) => node.insert(idx, cell),
        }

        todo!()
    }

    fn search(
        &mut self,
        page: PageNumber,
        entry: &[u8],
        parents: &mut Vec<PageNumber>,
    ) -> std::io::Result<Search> {
        let index = self.binary_search(page, entry)?;
        let node = self.pager.get(page)?;

        // recursion breaking condition
        if index.is_ok() || node.is_leaf() {
            return Ok(Search { page, index });
        }

        parents.push(page);
        let next = node.children(index.unwrap_err());

        self.search(next, entry, parents)
    }

    fn binary_search(
        &mut self,
        page_number: PageNumber,
        entry: &[u8],
    ) -> std::io::Result<Result<u16, u16>> {
        let mut size = self.pager.get(page_number)?.len();

        let mut left = 0;
        let mut right = size;

        while left < right {
            let mid = (left + right) / 2;

            let cell = self.pager.get(page_number)?.cell(mid as _);
            let overflow: Box<[u8]>;

            let content = match cell.header.is_overflow {
                false => &cell.content,
                true => match self.pager.reassemble_content(page_number, mid as _)? {
                    Content::Reassembled(buffer) => {
                        overflow = buffer.clone();
                        &overflow
                    }
                    _ => panic!("Couldn't complete reassemble content"),
                },
            };

            match self.comparator.cmp(content, entry) {
                Ordering::Equal => return Ok(Ok(mid)),
                Ordering::Greater => right = mid,
                Ordering::Less => left = mid + 1,
            }

            size = right - left
        }

        Ok(Err(left))
    }

    /// Allocates a [`Cell`] that can store all the given content.
    fn allocate_cell(&mut self, content: Vec<u8>) -> std::io::Result<Box<Cell>> {
        let max_content = Page::ideal_max_content_size(self.pager.page_size, self.min_keys);

        // in this case, we don't need to use an OverflowPage
        if content.len() <= max_content {
            return Ok(Cell::new(content));
        };

        let first_cell_size = max_content - size_of::<PageNumber>();
        let mut overflow_page_num = self.pager.allocate_page::<OverflowPage>()?;

        let cell = Cell::new_overflow(Vec::from(&content[..first_cell_size]), overflow_page_num);

        let mut bytes = first_cell_size;

        loop {
            let overflowed_bytes = min(
                OverflowPage::usable_space(self.pager.page_size) as usize,
                content[bytes..].len(),
            );

            let overflow_page = self.pager.get_mut_as::<OverflowPage>(overflow_page_num)?;

            overflow_page.buffer.mutable_content()[..overflowed_bytes]
                .copy_from_slice(&content[bytes..bytes + overflowed_bytes]);
            overflow_page.buffer.mutable_header().num_bytes = overflowed_bytes as u16;

            bytes += overflowed_bytes;
            if bytes >= content.len() {
                break;
            };

            let next_page = self.pager.allocate_page::<OverflowPage>()?;
            self.pager
                .get_mut_as::<OverflowPage>(overflow_page_num)?
                .buffer
                .mutable_header()
                .next = next_page;

            overflow_page_num = next_page
        }

        Ok(cell)
    }

    fn allocate_page<Page: PageConversion>(&mut self, page: Page) {
        todo!()
    }
}

impl<'p, File, Cmp: BytesCmp> BTree<'p, File, Cmp> {
    pub fn new(pager: &'p mut Pager<File>, root: PageNumber, comparator: Cmp) -> Self {
        Self {
            pager,
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

#[cfg(test)]
mod tests {
    use crate::core::btree::*;

    type MemoryBuffer = std::io::Cursor<Vec<u8>>;
    trait Keys: IntoIterator<Item = u64> {}
    impl<Type> Keys for Type where Type: IntoIterator<Item = u64> {}

    #[derive(Debug, PartialEq)]
    /// We can make an entire tree in-memory and then compare it to an actual [`BTree`].
    struct Node {
        children: Vec<Node>,
        keys: Vec<u64>,
    }

    impl Node {
        fn new<K: Keys>(keys: K) -> Self {
            Self {
                keys: keys.into_iter().collect(),
                children: Vec::new(),
            }
        }
    }

    impl<'p> Default for BTree<'p, MemoryBuffer, FixedSizeCmp> {
        fn default() -> Self {
            static mut PAGER: Option<Pager<MemoryBuffer>> = None;

            unsafe {
                if PAGER.is_none() {
                    PAGER = Some(Pager::new(MemoryBuffer::new(Vec::new())));
                }

                let pager = PAGER.as_mut().unwrap();
                let comparator = FixedSizeCmp::default();

                Self {
                    root: 0,
                    min_keys: 1,
                    balanced_siblings: 0,
                    pager,
                    comparator,
                }
            }
        }
    }

    impl<'p> BTree<'p, MemoryBuffer, FixedSizeCmp> {
        fn with_keys<K: Keys>(keys: K) -> Self {
            let mut default = Self::default();

            todo!()
        }

        fn try_insert_keys<K: Keys>(&mut self, keys: K) -> std::io::Result<()> {
            // keys.into_iter().for_each(|key| self.insert());
            todo!()
        }
    }

    #[test]
    fn test_fill_root() {
        let pager = &mut Pager::default();
    }
}
