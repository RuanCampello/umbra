//! Disk management and B-Tree data structure implementation.

use super::page::{Cell, Page, SlotId};
use super::pagination::io::FileOperations;
use super::pagination::pager::Pager;
use super::{byte_len_of_int_type, utf_8_length_bytes, PageNumber};
use crate::core::page::overflow::OverflowPage;
use crate::sql::statements::Type;
use std::cmp::{min, Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::io::{Read, Result as IOResult, Seek, Write};

/// B-Tree data structure hardly inspired on SQLite B*-Tree.
/// [This](https://www.youtube.com/watch?v=aZjYr87r1b8) video is a great introduction to B-trees and its idiosyncrasies and singularities.
/// Checkout [here](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html) to see a visualizer of this data structure.
pub(in crate::core) struct BTree<'p, File, Cmp> {
    root: PageNumber,
    min_keys: usize,
    balanced_siblings: usize,
    // TODO: this should be ideally a owned value not a mutable reference.
    pager: &'p mut Pager<File>,
    /// The byte comparator used to get [`Ordering`].
    comparator: Cmp,
}

#[derive(Debug)]
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
/// If the integer keys at the beginning of buffer array are stored as big endian,
/// that's all needed to determine its [`Ordering`].
#[derive(Debug, Default)]
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

/// [`BTree`] common keys iterator
trait Keys: IntoIterator<Item = u64> {}
impl<Type> Keys for Type where Type: IntoIterator<Item = u64> {}

impl<'p, File: Read + Write + Seek + FileOperations, Cmp: BytesCmp> BTree<'p, File, Cmp> {
    /// Returns a value of a given key.
    pub fn get(&mut self, entry: &[u8]) -> IOResult<Option<Content>> {
        todo!()
    }

    /// Inserts a new entry into the tree or replace it if already exists.
    pub fn insert(&mut self, entry: Vec<u8>) -> IOResult<()> {
        let mut parents = Vec::new();
        let search = self.search(self.root, &entry, &mut parents)?;

        let mut cell = self.allocate_cell(entry)?;
        let node = self.pager.get_mut(search.page)?;

        match search.index {
            // if the key was found, we just need to replace it
            Ok(idx) => {
                cell.header.left_child = node.cell(idx).header.left_child;
                let curr_cell = node.replace(cell, idx);

                self.pager.free_cell(curr_cell)?;
            }
            // wasn't found, insert it
            Err(idx) => node.insert(idx, cell),
        }

        self.balance(search.page, &mut parents)
    }

    fn search(
        &mut self,
        page: PageNumber,
        entry: &[u8],
        parents: &mut Vec<PageNumber>,
    ) -> IOResult<Search> {
        let index = self.binary_search(page, entry)?;
        let node = self.pager.get(page)?;

        // recursion-breaking condition
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
    ) -> IOResult<Result<u16, u16>> {
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

    /// BTree balancing algorithm.
    /// You can check out this mostly same `balance`
    /// implementation for this [Sqlite](https://www.sqlite.org/src/file?name=src/btree.c&ci=590f963b6599e4e2)
    /// version.
    fn balance(
        &mut self,
        mut page_number: PageNumber,
        parents: &mut Vec<PageNumber>,
    ) -> IOResult<()> {
        let node = self.pager.get(page_number)?;
        let is_root = parents.is_empty();
        let is_underflow = !is_root || node.len() == 0 || node.is_underflow();

        // the node is balanced, so no need of doing anything
        if !node.is_underflow() && is_underflow {
            return Ok(());
        }

        // underflow in root
        if is_root && is_underflow {
            // if the node has no children, there's nothing we can do
            if node.is_leaf() {
                return Ok(());
            }

            let child_page_number = node.header().right_child;
            let space_needed = self.pager.get(child_page_number)?.bytes_occupied();

            if self.pager.get(page_number)?.header().free_space < space_needed {
                return Ok(());
            }

            let child = self.pager.get_mut(child_page_number)?;
            let grand_child = child.header().right_child;
            let cells: Vec<Box<Cell>> = child.drain(..).collect();

            self.pager.free_page(child_page_number)?;

            let root = self.pager.get_mut(page_number)?;
            root.push_all(cells);
            root.mutable_header().right_child = grand_child;

            return Ok(());
        }

        // overflow in root
        if is_root && node.is_overflow() {
            let new_page_number = self.pager.allocate_page::<Page>()?;

            let root = self.pager.get_mut(page_number)?;
            let grand_child =
                std::mem::replace(&mut root.mutable_header().right_child, new_page_number);
            let cells: Vec<Box<Cell>> = root.drain(..).collect();

            // the created page
            let child = self.pager.get_mut(new_page_number)?;
            child.push_all(cells);
            child.mutable_header().right_child = grand_child;
            parents.push(page_number);

            page_number = new_page_number
        }

        let parent = parents.remove(parents.len() - 1);
        let mut siblings = self.siblings(page_number, parent)?;

        debug_assert_eq!(
            HashSet::<PageNumber>::from_iter(siblings.iter().map(|sibling| sibling.page)).len(),
            siblings.len(),
            "Sibling array contains duplicates {siblings:#?}"
        );

        let mut cells = VecDeque::new();
        let div_idx = siblings[0].index;

        siblings
            .iter()
            .enumerate()
            .try_for_each(|(idx, sibling)| -> IOResult<()> {
                cells.extend(self.pager.get_mut(sibling.page)?.drain(..));

                if idx > siblings.len() - 1 {
                    let mut div = self.pager.get_mut(parent)?.remove(div_idx);
                    div.header.left_child = self.pager.get(sibling.page)?.header().right_child;
                    cells.push_back(div);
                }
                Ok(())
            })?;

        let usable_space = Page::usable_space(self.pager.page_size);
        let mut total_size_per_age = vec![0];
        let mut number_of_cells_per_page = vec![0];

        // compute the distribution (left-based, yeah lil comrade)
        cells.iter().for_each(|cell| {
            let idx = number_of_cells_per_page.len() - 1;
            match total_size_per_age[idx] + cell.storage_size() <= usable_space {
                true => {
                    number_of_cells_per_page[idx] += 1;
                    total_size_per_age[idx] += cell.storage_size()
                }
                false => {
                    number_of_cells_per_page.push(0);
                    total_size_per_age.push(0)
                }
            }
        });

        if number_of_cells_per_page.len() >= 2 {
            let mut div_cell = cells.len() - number_of_cells_per_page.last().unwrap() - 1;

            (1..=(total_size_per_age.len() - 1)).rev().for_each(|idx| {
                // checkout underflow
                while total_size_per_age[idx] < usable_space / 2 {
                    number_of_cells_per_page[idx] += 1;
                    total_size_per_age[idx] += &cells[div_cell].storage_size();

                    number_of_cells_per_page[idx - 1] -= 1;
                    total_size_per_age[idx - 1] -= &cells[div_cell - 1].storage_size();

                    div_cell -= 1
                }
            });

            // adjustment to maintain the left-based assumption
            if total_size_per_age[0] < usable_space / 2 {
                number_of_cells_per_page[0] += 1;
                total_size_per_age[1] -= 1;
            }
        }

        let right_child = self
            .pager
            .get(siblings.last().unwrap().page)?
            .header()
            .right_child;

        // alloc the missing pages
        while siblings.len() < number_of_cells_per_page.len() {
            let page = self.pager.allocate_page::<Page>()?;
            let parent_idx = siblings.last().unwrap().index + 1;

            siblings.push(Sibling::new(page, parent_idx));
        }

        // free unneeded pages
        while number_of_cells_per_page.len() < siblings.len() {
            self.pager.free_page(siblings.pop().unwrap().page)?;
        }

        // put this baby in ascending order, you know, sequential IO wins from this
        BinaryHeap::from_iter(siblings.iter().map(|sibling| Reverse(sibling.page)))
            .iter()
            .enumerate()
            .for_each(|(idx, Reverse(page))| siblings[idx].page = *page);

        // correct the pointers
        let last_sibling = &siblings[siblings.len() - 1];
        self.pager
            .get_mut(last_sibling.page)?
            .mutable_header()
            .right_child = right_child;

        let parent_node = self.pager.get_mut(parent)?;
        match div_idx == parent_node.len() {
            true => parent_node.mutable_header().right_child = last_sibling.page,
            false => parent_node.mutable_cell(div_idx).header.left_child = last_sibling.page,
        }

        // redistribution
        number_of_cells_per_page
            .iter()
            .enumerate()
            .try_for_each(|(idx, num)| -> IOResult<()> {
                let page = self.pager.get_mut(siblings[idx].page)?;
                (0..*num).for_each(|_| page.push(cells.pop_front().unwrap()));

                if idx < siblings.len() - 1 {
                    let mut div = cells.pop_front().unwrap();

                    page.mutable_header().right_child = div.header.left_child;
                    div.header.left_child = siblings[idx].page;
                    self.pager.get_mut(parent)?.insert(siblings[idx].index, div);
                }

                Ok(())
            })?;

        self.balance(parent, parents)?;
        Ok(())
    }

    // TODO: explain this madness
    fn siblings(&mut self, page_number: PageNumber, parent: PageNumber) -> IOResult<Vec<Sibling>> {
        let mut siblings_per_size = self.balanced_siblings as u16;
        let parent = self.pager.get(parent)?;

        let idx = parent
            .iter_children()
            .position(|p| p == page_number)
            .unwrap() as u16;

        if idx == 0 || idx == parent.len() {
            siblings_per_size *= 2;
        }

        let left = idx.saturating_sub(siblings_per_size)..idx;
        let right = (idx + 1)..min(idx + siblings_per_size + 1, parent.len() + 1);
        let sibling = |idx| Sibling::new(parent.children(idx), idx);

        let siblings: Vec<Sibling> = left
            .map(sibling)
            .chain(std::iter::once(Sibling::new(page_number, idx)))
            .chain(right.map(sibling))
            .collect();

        Ok(siblings)
    }

    /// Allocates a [`Cell`] that can store all the given content. Which can [overflow](OverflowPage).
    fn allocate_cell(&mut self, content: Vec<u8>) -> IOResult<Box<Cell>> {
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
    /// This only makes sense with simple data types.
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
    use crate::method_builder;

    type MemoryBuffer = std::io::Cursor<Vec<u8>>;

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

    impl<'p> TryFrom<BTree<'p, MemoryBuffer, FixedSizeCmp>> for Node {
        type Error = std::io::Error;

        fn try_from(mut btree: BTree<'p, MemoryBuffer, FixedSizeCmp>) -> Result<Self, Self::Error> {
            btree.into_node(btree.root)
        }
    }

    impl<'p> BTree<'p, MemoryBuffer, FixedSizeCmp> {
        fn with_keys<K: Keys>(keys: K) -> Self {
            let mut default = Self::default();
            // println!("tree before inserting {default:#?}");
            default
                .try_insert_keys(keys)
                .expect("Failed to create Btree with {keys:?}");

            // println!("tree after inserting {default:#?}");
            default
        }

        fn try_insert_keys<K: Keys>(&mut self, keys: K) -> IOResult<()> {
            for key in keys {
                self.insert(Vec::from(key.to_be_bytes()))?;
            }

            Ok(())
        }

        fn into_node(&mut self, root: PageNumber) -> IOResult<Node> {
            let page = self.pager.get(root)?;

            let mut node = Node {
                keys: (0..page.len())
                    .map(|idx| {
                        // deserialization
                        u64::from_be_bytes(
                            page.cell(idx).content[..size_of::<u64>()]
                                .try_into()
                                .unwrap(),
                        )
                    })
                    .collect(),
                children: vec![],
            };

            let children = page.iter_children().collect::<Vec<PageNumber>>();
            for page in children {
                node.children.push(self.into_node(page)?);
            }

            Ok(node)
        }

        fn on<K: Keys>(
            &mut self,
            keys: K,
            pager: &'p mut Pager<MemoryBuffer>,
        ) -> IOResult<BTree<'p, MemoryBuffer, FixedSizeCmp>> {
            let root = pager.allocate_page::<Page>()?;

            let mut btree = BTree {
                pager,
                root,
                balanced_siblings: self.balanced_siblings,
                comparator: FixedSizeCmp::new::<u64>(),
                min_keys: self.min_keys,
            };

            btree.try_insert_keys(keys)?;
            Ok(btree)
        }

        method_builder!(pager, &'p mut Pager<MemoryBuffer>);
    }

    #[test]
    fn test_fill_root() -> IOResult<()> {
        let pager = &mut Pager::default();
        let btree = BTree::default().on(1..=3, pager)?;

        assert_eq!(Node::try_from(btree)?, Node::new([1, 2, 3]));

        Ok(())
    }
}
