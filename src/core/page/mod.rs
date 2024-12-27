// ! Implementation of disk "page".
// ! As this manually alloc memory, it contains the only database's `unsafe` module.

mod buffer;

use crate::core::{page::buffer::BufferWithHeader, PageNumber};
use std::fmt::Debug;
use std::{self, alloc, ptr};

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
