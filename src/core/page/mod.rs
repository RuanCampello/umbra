// ! Implementation of disk "page".
// ! As this manually alloc memory, it contains the only database's `unsafe` module.

mod buffer;
mod overflow;
mod zero;

use crate::core::{page::buffer::BufferWithHeader, PageNumber};
use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Debug, Formatter};
use std::ops::Bound;
use std::ptr::NonNull;
use std::{self, alloc, iter, ptr};

/// Fixed-size slotted page for storing [`super::btree::BTree`] nodes, used in indexes and tables.
///
/// The page uses a "slot array" to manage offsets pointing to stored cells.
/// [`Cell`] grow from the end of the page, while the slot array grows from the
/// start, leaving free space in the middle. Rearranging or deleting cells
/// only modifies the slot array, making these operations efficient.
///
/// ```text
///
///   HEADER   SLOT ARRAY   FREE SPACE    USED CELLS
///  +------+----+----+----+----------+--------+--------+
///  |      | O1 | O2 | O3 | ->    <- | CELL 3 | CELL 2 |
///  +------+----+----+----+----------+--------+--------+
///
/// ```
///
struct Page {
    /// In-memory buffer containing data read from disk, including a header.
    buffer: BufferWithHeader<PageHeader>,
    /// Map storing overflow cells, keyed by their slot IDs.
    overflow: HashMap<SlotId, Box<Cell>>,
}

/// Slotted page header.
///
/// ```text
///                           HEADER                                     CONTENT
/// +-------------------------------------------------------------+-------------------+
/// | +------------+------------+------------------+-------------+ |                  |
/// | | free_space | slot_count | last_used_offset | right_child | |                  |
/// | +------------+------------+------------------+-------------+ |                  |
/// +-------------------------------------------------------------+-------------------+
///                                     PAGE
/// ```
#[derive(Debug)]
struct PageHeader {
    /// The Page's free space available.
    free_space: u16,
    /// Number of entries in the slot array.
    slot_count: u16,
    /// Offset of the last inserted cell, starting from the page content.
    /// For empty pages, this equals the content size, allowing consistent offset calculations.
    last_used_offset: u16,
    /// Manual padding to avoid uninitialized bytes, ensuring reliable memory comparison.
    padding: u16,
    /// Last child of this page.
    pub right_child: PageNumber,
}

///The Cell struct holds a [`crate::core::btree::BTree`] entry (key/value) and a pointer to a node with smaller keys.
/// It works with the BTree to rearrange entries during overflow or underflow situations.
/// This uses a DST (Dynamically Sized Type), which is complex but improves efficiency when working with references and ownership.
#[derive(Debug, PartialEq)]
struct Cell {
    header: CellHeader,

    /// When `header.is_overflow` is true, those last 4 bytes are going to point to an overflow page.
    content: [u8],
}

#[derive(Debug, PartialEq)]
struct CellHeader {
    pub is_overflow: bool,
    /// Padding bytes to ensure correct alignment and avoid undefined behavior.
    padding: u8,
    /// Size of the cell's content.
    size: u16,
    /// [`PageNumber`] of the Btree node containing keys smaller than this cell's key.
    left_child: PageNumber,
}

/// The slot array will never be greater than [`MAX_PAGE_SIZE`], therefore can be indexed with 2 bytes.
type SlotId = u16;

const CELL_ALIGNMENT: usize = align_of::<CellHeader>();
const CELL_HEADER_SIZE: u16 = size_of::<CellHeader>() as u16;
const SLOT_SIZE: u16 = size_of::<u16>() as u16;
const PAGE_ALIGNMENT: usize = 4096;
const PAGE_HEADER_SIZE: u16 = size_of::<PageHeader>() as u16;
const MIN_PAGE_SIZE: usize = 512;
const MAX_PAGE_SIZE: usize = 64 << 10;

impl Page {
    /// Allocates a new page with a given size.
    pub fn alloc(size: usize) -> Self {
        Self::from(BufferWithHeader::<PageHeader>::for_page(size))
    }

    /// Calculates the available space within a page for storing [`Cell`] instances.
    ///
    /// This value is determined by subtracting the size of the page header from the
    /// total page size. Since [`MAX_PAGE_SIZE`] is 64 KiB, the usable space should
    /// typically be less than [`u16::MAX`], as a zero-sized page header would be
    /// impractical.
    ///
    /// Check [`BufferWithHeader::usable_space`].
    pub fn usable_space(page_size: usize) -> u16 {
        BufferWithHeader::<PageHeader>::usable_space(page_size)
    }

    /// Adds a given [`Cell`] to the page, which can possibly overflow it.
    pub fn push(&mut self, cell: Box<Cell>) {
        let idx = self.len() as u16;

        self.insert(idx, cell)
    }

    pub fn insert(&mut self, idx: SlotId, cell: Box<Cell>) {
        let content_length = cell.content.len();
        let max = Self::max_content_size(Self::usable_space(self.buffer.size));

        assert!(
            content_length <= max as usize,
            "Unable to storage a content with {content_length} size where the max allowed is {max}"
        );

        if self.is_overflow() {
            self.overflow.insert(idx, cell);
        } else if let Err(cell) = self.try_insert(idx, cell) {
            self.overflow.insert(idx, cell);
        }
    }

    /// Attempts to insert the given [`Cell`]in this page.
    ///
    /// If there's enough contiguous free space, inserts the cell directly.
    /// Otherwise, defragments the page to create enough space and then inserts.
    pub fn try_insert(&mut self, id: SlotId, cell: Box<Cell>) -> Result<SlotId, Box<Cell>> {
        let cell_size = cell.storage_size();

        // what would you expect?
        if self.buffer.header().free_space < cell_size {
            return Err(cell);
        }

        let space_available = {
            let right = self.buffer.header().last_used_offset;
            let left = self.buffer.header().slot_count * SLOT_SIZE;
            right - left
        };

        if space_available < cell_size {
            self.defragment()
        }

        let offset = self.buffer.header().last_used_offset - cell.total_size();

        unsafe {
            let cell_header_size = cell.header.size as usize;

            let header: NonNull<CellHeader> = self.buffer.content.byte_add(offset as usize).cast();
            header.write(cell.header);
            let mut content = NonNull::slice_from_raw_parts(header.add(1).cast(), cell_header_size);

            content.as_mut().copy_from_slice(&cell.content)
        }

        let mutable_header = self.buffer.mutable_header();

        mutable_header.last_used_offset = offset;
        mutable_header.free_space -= cell_size;
        mutable_header.slot_count += 1;

        let idx = id as usize;

        if id < self.buffer.header().slot_count {
            let right = self.buffer.header().slot_count as usize - 1;

            self.mutable_slot_array().copy_within(idx..right, idx + 1)
        }

        self.mutable_slot_array()[idx] = offset;
        Ok(id)
    }

    /// Defragments the page by shifting cells to the right to create a single contiguous free space block.
    pub fn defragment(&mut self) {
        let mut offsets = BinaryHeap::from_iter(
            self.slot_array()
                .iter()
                .enumerate()
                .map(|(idx, offset)| (*offset, idx)),
        );

        let mut dest = self.buffer.size - PAGE_HEADER_SIZE as usize;

        // moves the cell to its new position in the buffer, updating the destination index.
        while let Some((offset, idx)) = offsets.pop() {
            unsafe {
                let cell = self.cell_at_offset(offset);
                let size = cell.as_ref().total_size() as usize;
                dest -= size;

                cell.cast::<u8>()
                    .copy_to(self.buffer.content.byte_add(dest).cast::<u8>(), size)
            }

            self.mutable_slot_array()[idx] = dest as u16;
        }

        self.buffer.mutable_header().last_used_offset = dest as u16;
    }

    /// Removes a cell pointer of a given [`SlotId`].
    pub fn remove(&mut self, id: SlotId) -> Box<Cell> {
        debug_assert!(
            !self.is_overflow(),
            "Remove is not meant to handle overflow indexes"
        );

        let len = self.buffer.header().slot_count;
        assert!(id < len, "Index {id} out of range for {len} length");

        let cell = self.cell(id).to_owned();

        let idx = id as usize;
        self.mutable_slot_array()
            .copy_within(idx + 1..len as usize, idx); // remove the idx
        self.buffer.mutable_header().free_space += cell.total_size(); // adds back the free space
        self.buffer.mutable_header().free_space += SLOT_SIZE;
        self.buffer.mutable_header().slot_count -= 1;

        cell
    }

    /// Tries to replace a [`Cell`] at a given [`SlotId`] with a new one.
    pub fn replace(&mut self, new: Box<Cell>, id: SlotId) -> Box<Cell> {
        debug_assert!(
            !self.is_overflow(),
            "Replace it not meant to handle overflow indexes"
        );

        match self.try_replace(new, id) {
            Ok(old) => old,
            Err(new) => {
                let current = self.remove(id);
                self.overflow.insert(id, new);
                current
            }
        }
    }

    /// Removes the [`Cell`]s in the given range and returns its owned version.
    pub fn drain<'a>(
        &'a mut self,
        range: impl std::ops::RangeBounds<usize>,
    ) -> impl Iterator<Item = Box<Cell>> + 'a {
        let left = match range.start_bound() {
            Bound::Unbounded => 0,
            Bound::Included(idx) => *idx,
            Bound::Excluded(idx) => idx + 1,
        };

        let right = match range.end_bound() {
            Bound::Unbounded => self.len(),
            Bound::Excluded(i) => *i,
            Bound::Included(i) => i + 1,
        };

        let mut drain_idx = left;
        let mut slot_idx = left;

        iter::from_fn(move || {
            if drain_idx < right {
                let cell = self.overflow.remove(&(drain_idx as u16)).unwrap_or({
                    let cell = self.cloned_cell(slot_idx as SlotId);
                    slot_idx += 1;
                    cell
                });
                drain_idx += 1;

                Some(cell)
            } else {
                self.buffer.mutable_header().free_space += (left..right)
                    .map(|slot| self.cell(slot as u16).storage_size())
                    .sum::<u16>();
                self.mutable_slot_array().copy_within(slot_idx.., left);
                self.buffer.mutable_header().slot_count -= (slot_idx - left) as u16;

                None
            }
        })
    }

    fn try_replace(&mut self, new: Box<Cell>, id: SlotId) -> Result<Box<Cell>, Box<Cell>> {
        let current = self.cell(id);

        // this can't fit (that's what she said)
        if self.buffer.header().free_space + current.total_size() < new.total_size() {
            return Err(new);
        }

        // if the new cell is smaller than the current
        if new.header.size <= current.header.size {
            let free_bytes = current.header.size - new.header.size;
            let cell = self.cloned_cell(id);
            let current = self.mutable_cell(id);

            current.content[..new.content.len()].copy_from_slice(&new.content);
            current.header = new.header;
            self.buffer.mutable_header().free_space += free_bytes;

            return Ok(cell);
        }

        // worst place to be. the new can fit, but we have to remove the old one before.
        // this can potentially cause page defragmentation.
        let current = self.remove(id);
        self.try_insert(id, new).expect("It should fit, he said");

        Ok(current)
    }

    /// Returns a [`Cell`] reference of a given [`SlotId`].
    pub fn cell(&self, id: SlotId) -> &Cell {
        let cell = self.cell_pointer(id);

        unsafe { cell.as_ref() }
    }

    /// Returns a mutable reference of a [`Cell`] at a given [`SlotId`].
    fn mutable_cell(&mut self, id: SlotId) -> &mut Cell {
        let mut cell = self.cell_pointer(id);

        unsafe { cell.as_mut() }
    }

    /// Returns a pointer to a [`Cell`] of a given [`SlotId`].
    fn cell_pointer(&self, id: SlotId) -> NonNull<Cell> {
        let length = self.buffer.header().slot_count;
        debug_assert!(
            id < length,
            "SlotId {id} out of bounds for slot array with length {length}"
        );

        unsafe { self.cell_at_offset(self.slot_array()[id as usize]) }
    }

    /// Returns a [`Cell`] pointer of a given offset.
    unsafe fn cell_at_offset(&self, offset: u16) -> NonNull<Cell> {
        let header: NonNull<CellHeader> = self.buffer.content.byte_add(offset as usize).cast();

        let cell =
            ptr::slice_from_raw_parts(header.cast::<u8>().as_ptr(), header.as_ref().size as usize)
                as *mut Cell;
        NonNull::new_unchecked(cell)
    }

    /// Returns an owned [`Cell`] at given [`SlotId`] by coping it.
    fn cloned_cell(&self, id: SlotId) -> Box<Cell> {
        self.cell(id).to_owned()
    }

    /// Returns a pointer to the slot array.
    fn slot_array_pointer(&self) -> NonNull<[u16]> {
        NonNull::slice_from_raw_parts(
            self.buffer.content.cast(),
            self.buffer.header().slot_count as usize,
        )
    }

    /// Returns the reference of the slot array slice.
    fn slot_array(&self) -> &[u16] {
        unsafe { self.slot_array_pointer().as_ref() }
    }

    /// Returns a mutable reference of the slot array slice.
    fn mutable_slot_array(&mut self) -> &mut [u16] {
        unsafe { self.slot_array_pointer().as_mut() }
    }

    /// The maximum content size that can be stored in a given usable space.
    fn max_content_size(usable_size: u16) -> u16 {
        (usable_size - CELL_HEADER_SIZE - SLOT_SIZE) & !(CELL_ALIGNMENT as u16 - 1)
    }

    fn is_overflow(&self) -> bool {
        !self.overflow.is_empty()
    }

    fn len(&self) -> usize {
        self.buffer.header().slot_count as usize + self.overflow.len()
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("Page");

        struct DebugCell<'a> {
            size: u16,
            content: &'a [u8],
        }

        impl<'a> Debug for DebugCell<'a> {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("Cell")
                    .field("size", &self.size)
                    .field("content", &self.content)
                    .finish()
            }
        }

        debug_struct
            .field("slot array", &self.slot_array())
            .field("header", &self.buffer.header())
            .field("overflow", &self.overflow)
            .field("size", &self.buffer.size)
            .field("cells", &{
                struct ClosureDebug<'a, F: Fn(&mut Formatter<'_>) -> std::fmt::Result>(&'a F);
                impl<'a, F: Fn(&mut Formatter<'_>) -> std::fmt::Result> Debug for ClosureDebug<'a, F> {
                    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                        (self.0)(f)
                    }
                }

                ClosureDebug(&|f: &mut Formatter<'_>| {
                    let mut list = f.debug_list();
                    for slot in 0..self.buffer.header().slot_count {
                        let cell = self.cell(slot);
                         list.entry(&DebugCell {
                            size: cell.header.size,
                            content: &cell.content,
                        });
                    }
                    list.finish()
                })
            });

        debug_struct.finish()
    }
}

impl<Header> From<BufferWithHeader<Header>> for Page {
    fn from(buffer: BufferWithHeader<Header>) -> Self {
        let mut buffer = buffer.cast();
        *buffer.mutable_header() = PageHeader::new(buffer.size);

        Self {
            buffer,
            overflow: HashMap::new(),
        }
    }
}

impl Cell {
    pub fn new(mut content: Vec<u8>) -> Box<Self> {
        let size = Self::calculate_aligned_size(&content);
        content.resize(size as usize, 0); // padding

        let mut buffer = unsafe {
            let layout_size = (size + CELL_HEADER_SIZE) as usize;

            let layout = alloc::Layout::from_size_align(layout_size, CELL_ALIGNMENT)
                .expect("Failed to create layout for Cell buffer");

            let ptr = alloc::alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Memory allocation failed for BufferWithHeader");
            }

            let non_null = BufferWithHeader::<CellHeader>::from_raw_parts(ptr, layout_size);
            BufferWithHeader::<CellHeader>::from_non_null(non_null)
        };

        buffer.mutable_header().size = size;
        buffer.mutable_content().copy_from_slice(&content);

        // TODO: this is a much complicated case. checkout it here later. very dangerous.
        // maybe implement a smart pointer, because it can seg fault, and no one want that in rust, right?

        unsafe {
            Box::from_raw(ptr::slice_from_raw_parts(
                buffer.into_non_null().cast::<u8>().as_ptr(),
                content.len(),
            ) as *mut Cell)
        }
    }

    /// Calculates the aligned size of the data based on CELL_ALIGNMENT.
    pub fn calculate_aligned_size(data: &[u8]) -> u16 {
        alloc::Layout::from_size_align(data.len(), CELL_ALIGNMENT)
            .unwrap()
            .pad_to_align()
            .size() as u16
    }

    /// The cell's total size, including the header.
    pub fn total_size(&self) -> u16 {
        CELL_HEADER_SIZE + self.header.size
    }

    /// Returns the total size of the cell in storage, including its header and
    /// the space required to store its offset in the slot array.
    pub fn storage_size(&self) -> u16 {
        SLOT_SIZE + self.total_size()
    }
}

impl Clone for Box<Cell> {
    fn clone(&self) -> Self {
        self.as_ref().to_owned()
    }
}

impl ToOwned for Cell {
    type Owned = Box<Cell>;

    fn to_owned(&self) -> Self::Owned {
        let header_size = size_of::<CellHeader>();
        let content_size = self.content.len();
        let total_size = header_size + content_size;

        let layout = alloc::Layout::from_size_align(total_size, align_of::<CellHeader>())
            .expect("Unable to create layout for Cell");
        let ptr = unsafe { alloc::alloc(layout) };
        if ptr.is_null() {
            panic!("Memory allocation failed for Cell");
        }

        unsafe {
            ptr::copy_nonoverlapping(&self.header as *const CellHeader, ptr as *mut CellHeader, 1);
            let content_ptr = ptr.add(header_size);

            ptr::copy_nonoverlapping(self.content.as_ptr(), content_ptr, content_size);

            Box::from_raw(
                ptr::slice_from_raw_parts_mut(ptr as *mut CellHeader, content_size) as *mut Cell,
            )
        }
    }
}

impl PageHeader {
    pub fn new(size: usize) -> Self {
        Self {
            free_space: Page::usable_space(size),
            slot_count: 0,
            last_used_offset: (size - PAGE_HEADER_SIZE as usize) as u16,
            padding: 0,
            right_child: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::core::page::{overflow::*, *};
    use std::slice;

    impl Page {
        fn push_all_cells<'a>(&'a mut self, cells: &'a [Box<Cell>]) {
            cells.iter().for_each(|cell| self.push(cell.clone()))
        }

        fn create_page_with_cells(sizes: &[usize]) -> (Self, Vec<Box<Cell>>) {
            let cells = create_cells_with_size(sizes);
            let size = page_size_to_fit(&cells);

            println!("SIZE BUILT {size}");
            let mut page = Page::alloc(size);
            page.push_all_cells(&cells);

            (page, cells)
        }
    }

    /// Helper function to create cells with variables sizes.
    fn create_cells_with_size(sizes: &[usize]) -> Vec<Box<Cell>> {
        sizes
            .iter()
            .enumerate()
            .map(|(idx, size)| Cell::new(vec![idx as u8 + 1; *size]))
            .collect()
    }

    fn fixed_size_cells(size: usize, amount: usize) -> Vec<Box<Cell>> {
        create_cells_with_size(&vec![size; amount])
    }

    fn page_size_to_fit(cells: &[Box<Cell>]) -> usize {
        let size = PAGE_HEADER_SIZE + cells.iter().map(|cell| cell.storage_size()).sum::<u16>();

        alloc::Layout::from_size_align(size as usize, CELL_ALIGNMENT)
            .expect("Failed to create layout")
            .pad_to_align()
            .size()
    }

    /// Calls [`assert_eq_cells`] to perform general assertions on the page and cells.
    /// Calculates the expected offset for each cell based on its size and the preceding cells.
    /// Asserts that the calculated expected offset matches the actual offset stored in the page's slot array for each cell.
    fn assert_consecutive_cell_offsets(page: &Page, cells: &[Box<Cell>]) {
        assert_eq_cells(page, cells);

        let mut expected_offset = page.buffer.size - PAGE_HEADER_SIZE as usize;
        for (i, cell) in cells.iter().enumerate() {
            expected_offset -= cell.total_size() as usize;

            assert_eq!(
                page.slot_array()[i],
                expected_offset as u16,
                "Offset mismatch for cell at index {i}: {cell:?}"
            );
        }
    }

    /// Asserts that the given page contains all the provided cells in the correct order.
    fn assert_eq_cells(page: &Page, cells: &[Box<Cell>]) {
        assert_eq!(
            page.buffer.header().slot_count,
            cells.len() as u16,
            "Number of slots in page header does not match the number of inserted cells"
        );

        // calculate expected free space
        let used_space =
            PAGE_HEADER_SIZE + cells.iter().map(|cell| cell.storage_size()).sum::<u16>();
        let expected_free_space = page.buffer.size - used_space as usize;

        assert_eq!(
            page.buffer.header().free_space,
            expected_free_space as u16,
            "Page with size {} should contain {expected_free_space} bytes of free space after inserting paylods of sizes {:?}",
            page.buffer.size,
            cells.iter().map(|cell| cell.header.size).collect::<Vec<_>>(),
        );

        for (idx, cell) in cells.iter().enumerate() {
            assert_eq!(
                page.cell(idx as u16),
                cell.as_ref(),
                "Data mismatch for cell at index {idx}"
            );
        }
    }

    #[test]
    fn test_buffer_with_header_construction() {
        const HEADER_SIZE: usize = size_of::<OverflowPageHeader>();
        const CONTENT_SIZE: usize = 69;
        const TOTAL_SIZE: usize = HEADER_SIZE + CONTENT_SIZE;

        let mut buffer = BufferWithHeader::<OverflowPageHeader>::new(TOTAL_SIZE);
        let header = OverflowPageHeader::new(10, CONTENT_SIZE as u16);
        let content_byte = 0b11000; // 24 in decimal

        *buffer.mutable_header() = header.clone();
        buffer.mutable_content().fill(content_byte);

        let mut expected_buffer = [0; TOTAL_SIZE];
        expected_buffer[HEADER_SIZE..].fill(content_byte);

        unsafe {
            expected_buffer[..HEADER_SIZE].copy_from_slice(slice::from_raw_parts(
                ptr::from_ref(&header).cast(),
                HEADER_SIZE,
            ));
        };

        assert_eq!(buffer.as_slice(), &expected_buffer);
        assert_eq!(buffer.size, TOTAL_SIZE);
        assert_eq!(buffer.header(), &header);
        assert_eq!(buffer.content(), &[content_byte; CONTENT_SIZE]);
    }

    #[test]
    fn test_push_different_bytes_sizes_cell() {
        let cells = fixed_size_cells(69, 10);
        let size = page_size_to_fit(&cells);

        let mut page = Page::alloc(size);
        page.push_all_cells(&cells);

        assert_consecutive_cell_offsets(&page, &cells)
    }

    #[test]
    fn test_defragmentation() {
        let (mut page, mut cells) = Page::create_page_with_cells(&[59, 99, 69, 420, 24]);

        // remove cells at indices 1 and 2 from both the page and the cell vector.
        // this creates fragmentation within the page's cell structure.
        for idx in [1, 2] {
            page.remove(idx);
            cells.remove(idx as usize);
        }

        // perform defragmentation on the page to consolidate fragmented cells,
        // ensuring all remaining cells are stored contiguously in memory.
        page.defragment();

        assert_consecutive_cell_offsets(&page, &cells)
    }

    #[test]
    fn test_delete() {
        let (mut page, mut cells) = Page::create_page_with_cells(&[59, 99, 69, 420, 24]);

        #[allow(clippy::filter_map_bool_then)]
        let offsets: Vec<u16> = (0..cells.len())
            .filter_map(|idx| (idx != 1).then(|| page.slot_array()[idx]))
            .collect(); // 0 to len excluding 1 idx

        page.remove(1);
        cells.remove(1);

        assert_eq!(page.slot_array(), offsets);
        assert_eq_cells(&page, &cells);
    }

    #[test]
    fn test_replace() {
        let (mut page, mut cells) = Page::create_page_with_cells(&[64, 32, 122, 420]);

        let cell = Cell::new(vec![4; 32]);
        cells[1] = cell.clone();

        // TODO: test replace with deletion
        page.replace(cell, 1);
        assert_consecutive_cell_offsets(&page, &cells);
    }
}
