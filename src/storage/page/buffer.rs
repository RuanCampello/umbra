use crate::storage::page::{CELL_ALIGNMENT, MAX_PAGE_SIZE, MIN_PAGE_SIZE, PAGE_ALIGNMENT};
use std::any::type_name;
use std::fmt::{Debug, Formatter};
use std::ptr::NonNull;
use std::{alloc, mem};

/// In-memory buffer with a header.
///
/// Represents a buffer split into a header and content.
/// Header size is determined by the generic type `Header`.
/// Provides methods for accessing header and content directly.
pub(crate) struct BufferWithHeader<Header> {
    /// Total size of the buffer in bytes.
    pub size: usize,
    /// Pointer to the content.
    pub content: NonNull<[u8]>,
    /// Pointer to the header.
    pub header: NonNull<Header>,
}

impl<Header> BufferWithHeader<Header> {
    /// Creates a buffer with the given `size` with all bytes set as 0.
    pub fn new(size: usize) -> Self {
        let allocation = Self::alloc(size);
        unsafe { Self::from_non_null(allocation) }
    }

    /// Works as [`Self::new`] but checks range bounds.
    pub fn for_page(size: usize) -> Self {
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

        Self::from_raw_parts(ptr, size)
    }

    pub fn from_raw_parts(ptr: *mut u8, size: usize) -> NonNull<[u8]> {
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
    #[allow(unused_variables)]
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

    pub fn mutable_header(&mut self) -> &mut Header {
        unsafe { self.header.as_mut() }
    }

    pub fn header(&self) -> &Header {
        unsafe { self.header.as_ref() }
    }

    pub fn mutable_content(&mut self) -> &mut [u8] {
        unsafe { self.content.as_mut() }
    }

    pub fn content(&self) -> &[u8] {
        unsafe { self.content.as_ref() }
    }

    /// Consumes `self` and return the memory buffer pointer.
    pub fn into_non_null(self) -> NonNull<[u8]> {
        mem::ManuallyDrop::new(self).as_non_null()
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { self.as_non_null().as_ref() }
    }

    pub fn as_mutable_slice(&mut self) -> &mut [u8] {
        unsafe { self.as_non_null().as_mut() }
    }

    /// The number of bytes that can be used for `content`.
    pub(crate) fn usable_space(size: usize) -> u16 {
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

impl<Header: Debug> Debug for BufferWithHeader<Header> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferWithHeader")
            .field("content", &self.content)
            .field("header", &self.header)
            .field("size", &self.size)
            .finish()
    }
}

impl<Header> PartialEq for BufferWithHeader<Header> {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl<Header> AsRef<[u8]> for BufferWithHeader<Header> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<Header> AsMut<[u8]> for BufferWithHeader<Header> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mutable_slice()
    }
}

impl<Header> Clone for BufferWithHeader<Header> {
    fn clone(&self) -> Self {
        let mut cloned = Self::new(self.size);
        cloned.as_mutable_slice().copy_from_slice(self.as_slice());
        cloned
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::page::*;

    #[test]
    fn test_buffer_allocation_size() {
        let buffer = BufferWithHeader::<CellHeader>::new(MIN_PAGE_SIZE);
        assert_eq!(buffer.size, MIN_PAGE_SIZE);

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
