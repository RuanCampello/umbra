// ! Implementation of disk "page".
// ! As this manually alloc memory, it contains the only database's `unsafe` module.

mod buffer;
mod overflow;
mod zero;

use crate::core::{page::buffer::BufferWithHeader, PageNumber};
use std::collections::HashMap;
use std::fmt::Debug;
use std::ptr::NonNull;
use std::{self, alloc, ptr};

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
#[derive(Debug)]
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
        let idx = self.buffer.header().slot_count + self.overflow.len() as u16;

        self.insert(idx, cell)
    }

    pub fn insert(&mut self, idx: SlotId, cell: Box<Cell>) {
        let content_length = cell.content.len();
        let max = Self::max_content_size(Self::usable_space(self.buffer.size));

        assert!(
            content_length <= max as usize,
            "Unable to storage a content with {content_length} size where the max allowed is {max}"
        );

        if !self.overflow.is_empty() {
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
            // TODO: we can fit it, but we'll need to defrag it first
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

    /// Returns a [`Cell`] reference of a given [`SlotId`].
    pub fn cell(&self, id: SlotId) -> &Cell {
        let cell = self.cell_pointer(id);

        unsafe { cell.as_ref() }
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

mod tests {
    use crate::core::page::{overflow::*, *};
    use std::slice;

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
        // println!("Page: {page:#?}, cells: {cells:#?}");

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
        let cells = fixed_size_cells(50, 10);
        let cells_clone = fixed_size_cells(50, 10);

        let size = page_size_to_fit(&cells);

        let mut page = Page::alloc(size);
        for cell in cells_clone {
            page.push(cell);
        }
        println!("page {:#?}", page.buffer.header());
        println!("slot count {}", page.buffer.header().slot_count);

        assert_consecutive_cell_offsets(&page, &cells)
    }
}
