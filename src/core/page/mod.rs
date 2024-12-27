// ! Implementation of disk "page".
// ! As this manually alloc memory, it contains the only database's `unsafe` module.

mod buffer;

use crate::core::{page::buffer::BufferWithHeader, PageNumber};
use std::fmt::Debug;
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
struct Page {
    buffer: BufferWithHeader<PageHeader>,
}

/// Slotted page header.
///
/// ```text
///                           HEADER                                          CONTENT
/// +-------------------------------------------------------------+-----------------------+
/// | +------------+------------+------------------+-------------+ |                       |
/// | | free_space | slot_count | last_used_offset | right_child | |                       |
/// | +------------+------------+------------------+-------------+ |                       |
/// +-------------------------------------------------------------+-----------------------+
///                                          PAGE
/// ```
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

impl PageHeader {
    pub fn new(size: usize) -> Self {
        Self {
            free_space: 0,
            slot_count: 0,
            last_used_offset: (size - PAGE_HEADER_SIZE as usize) as u16,
            padding: 0,
            right_child: 0,
        }
    }
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

const CELL_ALIGNMENT: usize = align_of::<CellHeader>();
const CELL_HEADER_SIZE: u16 = size_of::<CellHeader>() as u16;
const PAGE_ALIGNMENT: usize = 4096;
const PAGE_HEADER_SIZE: u16 = size_of::<PageHeader>() as u16;
const MIN_PAGE_SIZE: usize = 512;
const MAX_PAGE_SIZE: usize = 64 << 10;

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
}
