//! Disk management and B-Tree data structure implementation.

#![allow(dead_code, unused_assignments)]

use super::page::{Cell, OverflowPage, Page, PageNumber, SlotId};
use super::pagination::io::FileOperations;
use super::pagination::pager::{reassemble_content, Pager};
use crate::core::storage::tuple::{byte_len_of_type, utf_8_length_bytes};
use crate::sql::statement::Type;
use std::cmp::{min, Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet, VecDeque};
use std::io::{Read, Result as IOResult, Seek, Write};
use std::mem;

/// B-Tree data structure hardly inspired on SQLite B*-Tree.
/// [This](https://www.youtube.com/watch?v=aZjYr87r1b8) video is a great introduction to B-trees and its idiosyncrasies and singularities.
/// Checkout [here](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html) to see a visualiser of this data structure.
pub(crate) struct BTree<'p, File, Cmp> {
    root: PageNumber,
    min_keys: usize,
    balanced_siblings: usize,
    // TODO: this should be ideally a owned value not a mutable reference.
    pager: &'p mut Pager<File>,
    /// The byte comparator used to get [`Ordering`].
    comparator: Cmp,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Cursor {
    page: PageNumber,
    slot: SlotId,
    descent: Vec<PageNumber>,
    init: bool,
    done: bool,
}

#[derive(Debug)]
struct Sibling {
    page: PageNumber,

    /// The index of the [Cell](crate::core::page::Cell) that points to this node.
    index: SlotId,
}

/// Result of a search in [`Btree].
#[derive(Debug)]
pub struct Search {
    pub page: PageNumber,

    /// Stores the searched element index on [`Ok`] or
    /// the index where it should be on [`Err`]
    pub index: Result<u16, u16>,
}

/// The result of a [remove](BTree::remove) operation in the BTree.
struct Removal {
    cell: Box<Cell>,
    leaf_node: PageNumber,
    internal_node: Option<PageNumber>,
}

/// Compares the `self.0` using a `memcmp`.
/// If the integer keys at the beginning of a buffer array are stored as big endian,
/// that's all needed to determine its [`Ordering`].
#[derive(Debug, PartialEq, Default, Clone)]
pub(crate) struct FixedSizeCmp(pub usize);

/// Compares UTF-8 strings.
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct StringCmp(pub usize);

/// No allocations comparing to [`Box`].
#[derive(Debug, PartialEq, Clone)]
pub(crate) enum BTreeKeyCmp {
    MemCmp(FixedSizeCmp),
    StrCmp(StringCmp),
}

/// Represents the result of reading content from the [`BTree`].
#[derive(Debug)]
pub(crate) enum Content<'a> {
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

pub(crate) type MemoryBuffer = std::io::Cursor<Vec<u8>>;

/// [`BTree`] common keys iterator
trait Keys: IntoIterator<Item = u64> {}
impl<Type> Keys for Type where Type: IntoIterator<Item = u64> {}

impl<'p, File: Read + Write + Seek + FileOperations, Cmp: BytesCmp> BTree<'p, File, Cmp> {
    /// Returns a value of a given key.
    pub fn get(&mut self, entry: &[u8]) -> IOResult<Option<Content>> {
        let search = self.search(self.root, entry, &mut Vec::new())?;

        match search.index {
            Err(_) => Ok(None),
            Ok(index) => Ok(Some(reassemble_content(self.pager, search.page, index)?)),
        }
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
    pub fn try_insert(&mut self, entry: Vec<u8>) -> IOResult<Result<(), Search>> {
        let mut parents = Vec::new();
        let search = self.search(self.root, &entry, &mut parents)?;

        let Err(index) = search.index else {
            return Ok(Err(search));
        };

        let cell = self.allocate_cell(entry)?;
        self.pager.get_mut(search.page)?.insert(index, cell);

        self.balance(search.page, &mut parents)?;

        Ok(Ok(()))
    }

    pub fn remove(&mut self, entry: &[u8]) -> IOResult<Option<Box<Cell>>> {
        let mut parents = Vec::new();
        let Some(Removal {
            cell,
            leaf_node,
            internal_node,
        }) = self.remove_entry(entry, &mut parents)?
        else {
            return Ok(None);
        };

        self.balance(leaf_node, &mut parents)?;

        if let Some(node) = internal_node {
            if let Some(index) = parents.iter().position(|n| n.eq(&node)) {
                parents.drain(index..);
                self.balance(node, &mut parents)?;
            }
        }

        Ok(Some(cell))
    }

    fn remove_entry(
        &mut self,
        entry: &[u8],
        parents: &mut Vec<PageNumber>,
    ) -> IOResult<Option<Removal>> {
        let search = self.search(self.root, entry, parents)?;
        let node = self.pager.get(search.page)?;

        // not found, can't remove the entry
        let Ok(index) = search.index else {
            return Ok(None);
        };

        // this is the simplest case, we just need to remove the key
        if node.is_leaf() {
            let cell = self.pager.get_mut(search.page)?.remove(index);

            return Ok(Some(Removal {
                cell,
                leaf_node: search.page,
                internal_node: None,
            }));
        }

        let left_c = node.children(index);
        let right_c = node.children(index.saturating_add(1));

        parents.push(search.page);

        let (leaf, key) = match self.pager.get(left_c)?.len() >= self.pager.get(right_c)?.len() {
            true => self.search_leaf_key(left_c, parents, LeafKeySearch::Max)?,
            false => self.search_leaf_key(right_c, parents, LeafKeySearch::Min)?,
        };

        let mut placeholder = self.pager.get_mut(leaf)?.remove(key);
        let node = self.pager.get_mut(search.page)?;
        placeholder.header.left_child = node.children(index);
        let cell = node.replace(placeholder, index);

        Ok(Some(Removal {
            cell,
            leaf_node: leaf,
            internal_node: Some(search.page),
        }))
    }

    pub fn search(
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
                true => match reassemble_content(self.pager, page_number, mid as _)? {
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
        let is_underflow = node.len() == 0 || !is_root && node.is_underflow();

        // the node is balanced, so no need of doing anything
        if !node.is_overflow() && !is_underflow {
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
            let grandchild = child.header().right_child;
            let cells = child.drain(..).collect::<Vec<_>>();

            self.pager.free_page(child_page_number)?;

            let root = self.pager.get_mut(page_number)?;
            root.push_all(cells);
            root.mutable_header().right_child = grandchild;

            return Ok(());
        }

        // overflow in root
        if is_root && node.is_overflow() {
            let new_page = self.pager.allocate_page::<Page>()?;

            let root = self.pager.get_mut(page_number)?;
            let grandchild = mem::replace(&mut root.mutable_header().right_child, new_page);
            let cells = root.drain(..).collect::<Vec<_>>();

            let new_child = self.pager.get_mut(new_page)?;
            cells.into_iter().for_each(|cell| new_child.push(cell));
            new_child.mutable_header().right_child = grandchild;

            parents.push(page_number);
            page_number = new_page;
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
                if idx < siblings.len() - 1 {
                    let mut div = self.pager.get_mut(parent)?.remove(div_idx);
                    div.header.left_child = self.pager.get(sibling.page)?.header().right_child;
                    cells.push_back(div);
                }
                Ok(())
            })?;

        let usable_space = Page::usable_space(self.pager.page_size);

        let mut total_size_in_each_page = vec![0];
        let mut number_of_cells_per_page = vec![0];

        // compute the distribution (left-based, yeah lil comrade)
        cells.iter().for_each(|cell| {
            let i = number_of_cells_per_page.len() - 1;
            if total_size_in_each_page[i] + cell.storage_size() <= usable_space {
                number_of_cells_per_page[i] += 1;
                total_size_in_each_page[i] += cell.storage_size();
            } else {
                number_of_cells_per_page.push(0);
                total_size_in_each_page.push(0);
            }
        });

        if number_of_cells_per_page.len() >= 2 {
            let mut div_cell = cells.len() - number_of_cells_per_page.last().unwrap() - 1;
            (1..total_size_in_each_page.len()).rev().for_each(|i| {
                // checkout underflow
                while total_size_in_each_page[i] < usable_space / 2 {
                    number_of_cells_per_page[i] += 1;
                    total_size_in_each_page[i] += &cells[div_cell].storage_size();

                    number_of_cells_per_page[i - 1] -= 1;
                    total_size_in_each_page[i - 1] -= &cells[div_cell - 1].storage_size();
                    div_cell -= 1;
                }
            });

            // adjustment to maintain the left-based assumption
            if total_size_in_each_page[0] < usable_space / 2 {
                number_of_cells_per_page[0] += 1;
                number_of_cells_per_page[1] -= 1;
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

    pub(crate) fn max(&mut self) -> IOResult<Option<Content>> {
        let (page, slot) = self.search_leaf_key(self.root, &mut Vec::new(), LeafKeySearch::Max)?;

        if self.pager.get(page)?.len() == 0 {
            return Ok(None);
        };

        reassemble_content(self.pager, page, slot).map(Some)
    }

    fn search_leaf_key(
        &mut self,
        page_number: PageNumber,
        parents: &mut Vec<PageNumber>,
        leaf_key_search: LeafKeySearch,
    ) -> IOResult<(PageNumber, u16)> {
        let node = self.pager.get(page_number)?;

        let (key, child) = match leaf_key_search {
            LeafKeySearch::Min => (0, 0),
            LeafKeySearch::Max => (node.len().saturating_sub(1), node.len()),
        };

        if node.is_leaf() {
            return Ok((page_number, key));
        }

        parents.push(page_number);
        let children = node.children(child);

        self.search_leaf_key(children, parents, leaf_key_search)
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

impl Cursor {
    pub fn new(page: PageNumber, slot: SlotId) -> Self {
        Self {
            page,
            slot,
            descent: vec![],
            init: false,
            done: false,
        }
    }

    pub fn initialized(page: PageNumber, slot: SlotId, descent: Vec<PageNumber>) -> Self {
        Self {
            page,
            slot,
            descent,
            init: true,
            done: false,
        }
    }

    pub fn done() -> Self {
        Self {
            page: 0,
            slot: 0,
            descent: vec![],
            init: true,
            done: true,
        }
    }

    pub fn next<File: Seek + Read + Write + FileOperations>(
        &mut self,
        pager: &mut Pager<File>,
    ) -> Option<IOResult<(PageNumber, SlotId)>> {
        self.try_next(pager).transpose()
    }

    pub fn try_next<File: Seek + Read + Write + FileOperations>(
        &mut self,
        pager: &mut Pager<File>,
    ) -> IOResult<Option<(PageNumber, SlotId)>> {
        // what did you expect?
        if self.done {
            return Ok(None);
        }

        if !self.init {
            self.move_leftmost(pager)?;
            self.init = true;
        }

        let node = pager.get(self.page)?;
        if node.len() == 0 && node.is_leaf() {
            self.done = true;
            return Ok(None);
        }

        let position = Ok(Some((self.page, self.slot)));
        if node.is_leaf() && self.slot + 1 < node.len() {
            self.slot += 1;
            return position;
        }

        if !node.is_leaf() && self.slot < node.len() {
            self.descent.push(self.page);
            self.page = node.children(self.slot + 1);
            self.move_leftmost(pager)?;

            return position;
        }

        let mut found_branch = false;
        while !self.descent.is_empty() && !found_branch {
            let parent_page = self.descent.pop().unwrap();
            let parent_node = pager.get(parent_page)?;

            let idx = parent_node
                .iter_children()
                .position(|p| p == self.page)
                .unwrap() as u16;
            self.page = parent_page;

            if idx < parent_node.len() {
                self.slot = idx;
                found_branch = true;
            }
        }

        if self.descent.is_empty() && !found_branch {
            self.done = true;
        }

        position
    }

    fn move_leftmost<File: Seek + Read + Write + FileOperations>(
        &mut self,
        pager: &mut Pager<File>,
    ) -> IOResult<()> {
        let mut node = pager.get(self.page)?;

        while !node.is_leaf() {
            self.descent.push(self.page);
            self.page = node.children(0);
            node = pager.get(self.page)?;
        }

        self.slot = 0;

        Ok(())
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
            fixed => Ok(Self(byte_len_of_type(fixed))),
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
            not_var_type => Box::new(FixedSizeCmp(byte_len_of_type(not_var_type))),
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
            not_var_type => Self::MemCmp(FixedSizeCmp(byte_len_of_type(not_var_type))),
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
    use crate::core::storage::btree::*;
    use crate::core::storage::page::*;
    use crate::method_builder;
    use std::alloc::Layout;
    use std::fmt::Debug;

    type Key = u64;

    #[derive(Debug, PartialEq)]
    /// We can make an entire tree in-memory and then compare it to an actual [`BTree`].
    struct Node {
        keys: Vec<u64>,
        children: Vec<Self>,
    }

    impl Node {
        fn new<K: Keys>(keys: K) -> Self {
            Self {
                keys: keys.into_iter().collect(),
                children: Vec::new(),
            }
        }
    }

    impl Pager<MemoryBuffer> {
        fn for_test() -> Self {
            Pager::from_order(4)
        }

        fn from_order(order: usize) -> Self {
            let size = Pager::optimal_page_size(order);
            let mut pager = Pager::default().page_size(size);
            pager
                .init()
                .expect("Failed to init pager for btree testing");

            pager
        }

        fn optimal_page_size(order: usize) -> usize {
            let max_key_size = size_of::<u64>();

            let min_keys = order - 1;

            let align_up = |size, align| {
                Layout::from_size_align(size, align)
                    .unwrap()
                    .pad_to_align()
                    .size()
            };

            let cell_storage_size =
                CELL_HEADER_SIZE + SLOT_SIZE + align_up(max_key_size, CELL_ALIGNMENT) as u16;
            let total_size = PAGE_HEADER_SIZE + (cell_storage_size * min_keys as u16);

            align_up(total_size as usize, CELL_ALIGNMENT)
        }
    }

    #[allow(static_mut_refs)]
    impl<'p> Default for BTree<'p, MemoryBuffer, FixedSizeCmp> {
        fn default() -> Self {
            static mut PAGER: Option<Pager<MemoryBuffer>> = None;

            unsafe {
                if PAGER.is_none() {
                    PAGER = Some(Pager::new(MemoryBuffer::new(Vec::new())));
                }

                let pager = PAGER.as_mut().unwrap();
                let comparator = FixedSizeCmp::new::<u64>();

                Self {
                    root: 0,
                    min_keys: 1,
                    balanced_siblings: DEFAULT_BALANCED_SIBLINGS,
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
        fn try_insert_key<K: Keys>(&mut self, keys: K) -> IOResult<()> {
            keys.into_iter().try_for_each(|key| -> IOResult<()> {
                self.insert(Vec::from(serialize(key)))?;

                Ok(())
            })
        }

        fn try_remove_keys<K: Keys>(&mut self, keys: K) -> IOResult<Vec<Option<Box<Cell>>>> {
            keys.into_iter()
                .map(|key| self.remove(&serialize(key)))
                .collect()
        }

        fn into_node(&mut self, root: PageNumber) -> IOResult<Node> {
            let page = self.pager.get(root)?;

            let mut node = Node {
                keys: (0..page.len())
                    .map(|idx| deserialize(&page.cell(idx).content))
                    .collect(),
                children: vec![],
            };

            let children = page.iter_children().collect::<Vec<PageNumber>>();
            for page in children {
                node.children.push(self.into_node(page)?);
            }

            Ok(node)
        }

        fn with_keys<K: Keys>(
            &mut self,
            pager: &'p mut Pager<MemoryBuffer>,
            keys: K,
        ) -> IOResult<BTree<'p, MemoryBuffer, FixedSizeCmp>> {
            let root = pager.allocate_page::<Page>()?;

            let mut btree = BTree {
                pager,
                root,
                balanced_siblings: self.balanced_siblings,
                comparator: FixedSizeCmp::new::<u64>(),
                min_keys: self.min_keys,
            };

            btree.try_insert_key(keys)?;
            Ok(btree)
        }

        method_builder!(pager, &'p mut Pager<MemoryBuffer>);
    }

    fn traversal_matches(
        btree: &mut BTree<'_, MemoryBuffer, FixedSizeCmp>,
        keys: impl Iterator<Item = Key>,
    ) -> IOResult<()> {
        let mut cursor = Cursor::new(btree.root, 0);

        for expected_key in keys {
            let (page, slot) = cursor.next(btree.pager).unwrap_or_else(|| {
                panic!("Cursor should not be done but it is. Expected key: {expected_key}")
            })?;

            let key = deserialize(&btree.pager.get(page)?.cell(slot).content);

            assert_eq!(
                key, expected_key,
                "Cursor at {page} should have returned key {expected_key} but got {key}"
            );
        }

        assert!(cursor.next(btree.pager).is_none());
        Ok(())
    }

    fn serialize(key: Key) -> [u8; size_of::<Key>()] {
        key.to_be_bytes()
    }

    fn deserialize(content: &[u8]) -> Key {
        u64::from_be_bytes(content[..size_of::<u64>()].try_into().unwrap())
    }

    #[test]
    fn test_insertion() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let btree = BTree::default().with_keys(pager, 1..=15)?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![4, 8, 12],
                children: vec![
                    Node::new([1, 2, 3]),
                    Node::new([5, 6, 7]),
                    Node::new([9, 10, 11]),
                    Node::new([13, 14, 15]),
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_fill_root() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let btree = BTree::default().with_keys(pager, 1..=3)?;

        assert_eq!(Node::try_from(btree)?, Node::new([1, 2, 3]));

        Ok(())
    }

    #[test]
    fn test_split_root() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let btree = BTree::default().with_keys(pager, 1..=4)?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![3],
                children: vec![Node::new([1, 2]), Node::new([4])]
            }
        );

        Ok(())
    }

    #[test]
    fn test_split_leaf_node() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let btree = BTree::default().with_keys(pager, 1..=8)?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![3, 6],
                children: vec![Node::new([1, 2]), Node::new([4, 5]), Node::new([7, 8])]
            }
        );

        Ok(())
    }

    #[test]
    /// Verifies root split behaviour when inserting enough keys to overflow it.
    fn test_propagate_root() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let btree = BTree::default().with_keys(pager, 1..=16)?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![11],
                children: vec![
                    Node {
                        keys: vec![4, 8],
                        children: vec![
                            Node::new([1, 2, 3]),
                            Node::new([5, 6, 7]),
                            Node::new([9, 10])
                        ]
                    },
                    Node {
                        keys: vec![14],
                        children: vec![Node::new([12, 13]), Node::new([15, 16])]
                    }
                ]
            }
        );

        Ok(())
    }

    #[test]
    /// Checks that an internal node correctly splits and redistributes keys during insertion.
    fn test_internal_node_split() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let btree = BTree::default().with_keys(pager, 1..=27)?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![15],
                children: vec![
                    Node {
                        keys: vec![4, 8, 11],
                        children: vec![
                            Node::new([1, 2, 3]),
                            Node::new([5, 6, 7]),
                            Node::new([9, 10]),
                            Node::new([12, 13, 14])
                        ]
                    },
                    Node {
                        keys: vec![19, 22, 25],
                        children: vec![
                            Node::new([16, 17, 18]),
                            Node::new([20, 21]),
                            Node::new([23, 24]),
                            Node::new([26, 27]),
                        ]
                    },
                ]
            },
        );

        Ok(())
    }

    #[test]
    /// Confirms that deleting a key from a leaf node rebalances the tree without merging.
    fn test_delete_from_leaf() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let mut btree = BTree::default().with_keys(pager, 1..=15)?;

        btree.remove(&serialize(13))?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![4, 8, 12],
                children: vec![
                    Node::new([1, 2, 3]),
                    Node::new([5, 6, 7]),
                    Node::new([9, 10, 11]),
                    Node::new([14, 15]),
                ]
            }
        );

        Ok(())
    }

    #[test]
    /// Validates merging of nodes and key redistribution after multiple deletions.
    fn test_merge_nodes() -> IOResult<()> {
        let pager = &mut Pager::for_test();
        let mut btree = BTree::default().with_keys(pager, 1..=35)?;

        btree
            .try_remove_keys(1..=3)
            .and_then(|_| btree.remove(&serialize(35)))?;

        assert_eq!(
            Node::try_from(btree)?,
            Node {
                keys: vec![19],
                children: vec![
                    Node {
                        keys: vec![7, 11, 15],
                        children: vec![
                            Node::new([4, 5, 6]),
                            Node::new([8, 9, 10]),
                            Node::new([12, 13, 14]),
                            Node::new([16, 17, 18]),
                        ]
                    },
                    Node {
                        keys: vec![23, 27, 31],
                        children: vec![
                            Node::new([20, 21, 22]),
                            Node::new([24, 25, 26]),
                            Node::new([28, 29, 30]),
                            Node::new([32, 33, 34]),
                        ]
                    },
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_basic_cursor() -> IOResult<()> {
        let pager = &mut Pager::from_order(3);

        let keys = 1..=30;
        let mut btree = BTree::default().with_keys(pager, keys.clone())?;
        traversal_matches(&mut btree, keys)
    }

    #[test]
    #[cfg(not(miri))]
    fn test_deep_cursor() -> IOResult<()> {
        let pager = &mut Pager::from_order(6);

        let keys = 1..=400;
        let mut btree = BTree::default().with_keys(pager, keys.clone())?;

        traversal_matches(&mut btree, keys)
    }

    #[test]
    fn test_cursor_on_empty_root() -> IOResult<()> {
        let pager = &mut Pager::default();

        let btree = BTree::default().pager(pager);
        let mut cursor = Cursor::new(btree.root, 0);

        assert!(cursor.next(btree.pager).is_none());
        Ok(())
    }
}
