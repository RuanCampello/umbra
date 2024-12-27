// ! Implementation of disk "page".
// ! As this manually alloc memory, it contains the only database's `unsafe` module.

use crate::core::PageNumber;
use std::any::type_name;
use std::ptr::NonNull;
use std::{self, alloc, mem};

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

/// In-memory buffer with a header.
///
/// Represents a buffer split into a header and content.
/// Header size is determined by the generic type `Header`.
/// Provides methods for accessing header and content directly.
#[derive(Debug)]
struct BufferWithHeader<Header> {
    /// Total size of the buffer in bytes.
    size: usize,
    /// Pointer to the content.
    content: NonNull<[u8]>,
    /// Pointer to the header.
    header: NonNull<Header>,
}

const CELL_ALIGNMENT: usize = align_of::<CellHeader>();
const PAGE_ALIGNMENT: usize = 4096;
const MIN_PAGE_SIZE: usize = 512;
const MAX_PAGE_SIZE: usize = 64 << 10;

impl<Header> BufferWithHeader<Header> {
    /// Creates a buffer with the given `size` with all bytes set as 0.
    pub fn new(size: usize) -> Self {
        let allocation = Self::alloc(size);
        unsafe { Self::from_non_null(allocation) }
    }

    /// Works as [`Self::new`] but checks range bounds.
    pub fn for_page(size: usize) -> Self {
        println!("MIN {MIN_PAGE_SIZE} MAX {MAX_PAGE_SIZE} SIZE {size}");

        assert!(
            (MIN_PAGE_SIZE..=MAX_PAGE_SIZE).contains(&size),
            "Page size {size} is not between {MIN_PAGE_SIZE} and {MAX_PAGE_SIZE}"
        );

        Self::new(size)
    }

    pub fn alloc(size: usize) -> NonNull<[u8]> {
        let header_size = size_of::<Header>();

        assert!(
            size > header_size,
            "Insufficient allocation size: {} requires at least {} bytes, but got {}",
            type_name::<Self>(),
            header_size,
            size,
        );

        let layout = alloc::Layout::from_size_align(size, PAGE_ALIGNMENT)
            .expect("Unable to create layout for buffer");

        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            panic!("Memory allocation failed for BufferWithHeader");
        }

        NonNull::slice_from_raw_parts(NonNull::new(ptr).expect("Non-null allocation failed"), size)
    }

    /// Creates a buffer from a [`NonNull`] pointer.
    pub unsafe fn from_non_null(pointer: NonNull<[u8]>) -> Self {
        let header_size = size_of::<Header>();
        let name = type_name::<Self>();
        let size = pointer.len();

        assert!(
            size > header_size,
            "Invalid pointer {name} of size {size} when the size of {name} is {header_size}",
        );

        assert_eq!(
            (pointer.as_ptr() as *const u8 as usize) % CELL_ALIGNMENT,
            0,
            "Pointer alignment does not match Cell alignment"
        );

        let header = pointer.cast::<Header>();
        let content = NonNull::slice_from_raw_parts(
            header.byte_add(header_size).cast::<u8>(),
            Self::usable_space(size) as usize,
        );

        Self {
            header,
            content,
            size,
        }
    }

    /// Consumes [`self`] and returns a buffer of a given [`BufferWithHeader`] type.
    pub fn cast<Type>(self) -> BufferWithHeader<Type> {
        let Self {
            header,
            content,
            size,
        } = self;

        let size_of_type = size_of::<Type>();
        let name_of_type = type_name::<Type>();
        let buffer_name = type_name::<BufferWithHeader<Type>>();

        assert!(
            size > size_of_type,
            "Unable to cast {} of {size} bytes to a new header {buffer_name} with {size_of_type} bytes size", type_name::<Self>(), 
        );

        mem::forget(self);

        let header = header.cast();
        let content = unsafe {
            NonNull::slice_from_raw_parts(
                header.byte_add(size_of_type).cast::<u8>(),
                BufferWithHeader::<Type>::usable_space(size) as usize,
            )
        };

        BufferWithHeader {
            header,
            content,
            size,
        }
    }

    /// Returns a [`NonNull`] pointer of the buffer.
    pub fn as_non_null(&self) -> NonNull<[u8]> {
        NonNull::slice_from_raw_parts(self.header.cast::<u8>(), self.size)
    }

    /// The number of bytes that can be used for `content`.
    fn usable_space(size: usize) -> u16 {
        (size - size_of::<Header>()) as u16
    }
}

impl<Header> Drop for BufferWithHeader<Header> {
    fn drop(&mut self) {
        let layout = alloc::Layout::from_size_align(self.size, PAGE_ALIGNMENT)
            .expect("Unable to create layout for buffer");

        unsafe { alloc::dealloc(self.as_non_null().as_ptr() as *mut u8, layout) }
    }
}

mod tests {
    use crate::core::page::*;

    #[test]
    fn test_buffer_allocation_with_range() {
        let buffer = BufferWithHeader::<CellHeader>::new(MIN_PAGE_SIZE);
        assert_eq!(buffer.size, MIN_PAGE_SIZE);
        
        println!("buffer {buffer:#?}");

        let buffer = BufferWithHeader::<CellHeader>::new(MAX_PAGE_SIZE);
        assert_eq!(buffer.size, MAX_PAGE_SIZE);
    }

    #[test]
    #[should_panic]
    fn test_allocation_below_min() {
        BufferWithHeader::<CellHeader>::for_page(MIN_PAGE_SIZE - 1);
    }

    #[test]
    #[should_panic]
    fn test_allocation_above_max() {
        BufferWithHeader::<CellHeader>::for_page(MAX_PAGE_SIZE + 1);
    }
}
