use crate::core::{
    page::{buffer::BufferWithHeader, Page, PageHeader},
    PageNumber,
};
use std::collections::HashMap;
use std::mem::ManuallyDrop;
use std::ptr;

/// The first page of the DB file (offset 0) is a special case.
///
/// It contains an additional [`DatabaseHeader`] along with the regular [`Page`] data.
/// This results in less usable space for storing [Cells](crate::core::page::Cell) instances compared
/// to subsequent pages.
///
/// Note: While B-Tree balancing algorithms typically operate on pages of
/// uniform size, this special case for the first page does not introduce
/// any significant issues, especially with reasonable page sizes (e.g., 4096 bytes or larger).
/// Using an entire page solely for the [`DatabaseHeader`] would be inefficient in such cases.
#[derive(Debug, PartialEq, Clone)]
pub(in crate::core) struct PageZero {
    /// Page buffer, including the [`DatabaseHeader`].
    pub(in crate::core) buffer: ManuallyDrop<BufferWithHeader<DatabaseHeader>>,
    /// Inner B-Tree slotted page.
    page: ManuallyDrop<Page>,
}

/// Header of the database file.
///
/// This struct is located at the beginning of the database file and contains
/// essential metadata used by the [Pager](crate::core::pager::Pager) for
/// managing pages and free space within the database.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(in crate::core) struct DatabaseHeader {
    pub identifier: u32,
    pub page_size: u16,
    pub total_pages: u16,
    pub free_pages: u16,
    pub first_free_page: PageNumber,
    pub last_free_page: PageNumber,
}

/// This means literally "dusk", because "umbra" wouldn't fit in an [`core::u32`].
pub(in crate::core) const DATABASE_IDENTIFIER: u32 = 0x6475736b;

impl PageZero {
    pub fn alloc(size: usize) -> Self {
        Self::from(BufferWithHeader::<DatabaseHeader>::for_page(size))
    }

    /// Returns a reference of a [`DatabaseHeader`].
    pub fn header(&self) -> &DatabaseHeader {
        self.buffer.header()
    }

    /// Returns a reference of the slot [`Page`].
    pub fn btree_page(&self) -> &Page {
        &self.page
    }

    /// Returns a mutable reference of the slot [`Page`].
    pub fn mutable_btree_page(&mut self) -> &mut Page {
        &mut self.page
    }

    /// Erase all the metadata and returns the underlying buffer.
    pub fn buffer(mut self) -> BufferWithHeader<DatabaseHeader> {
        let Page {
            buffer, overflow, ..
        } = unsafe { ManuallyDrop::take(&mut self.page) };

        std::mem::forget(buffer);
        std::mem::drop(overflow);

        let buffer = unsafe { ManuallyDrop::take(&mut self.buffer) };
        std::mem::forget(self); // very poetic, init?

        buffer
    }
}

impl DatabaseHeader {
    pub fn new(size: usize) -> Self {
        Self {
            identifier: DATABASE_IDENTIFIER,
            page_size: size as u16,
            total_pages: 1,
            free_pages: 0,
            first_free_page: 0,
            last_free_page: 0,
        }
    }
}

impl Drop for PageZero {
    fn drop(&mut self) {
        unsafe { drop(ptr::from_mut(self).read().buffer()) }
    }
}

impl AsRef<[u8]> for PageZero {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl AsMut<[u8]> for PageZero {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}

impl<Header> From<BufferWithHeader<Header>> for PageZero {
    fn from(buffer: BufferWithHeader<Header>) -> Self {
        let mut buffer: ManuallyDrop<BufferWithHeader<DatabaseHeader>> =
            ManuallyDrop::new(buffer.cast());
        *buffer.mutable_header() = DatabaseHeader::new(buffer.size);

        let page_buffer = unsafe {
            let page_buffer = BufferWithHeader::from_non_null(buffer.content);

            page_buffer
                .header
                .write(PageHeader::new(buffer.size - size_of::<DatabaseHeader>()));

            page_buffer
        };

        let page = ManuallyDrop::new(Page {
            buffer: page_buffer,
            overflow: HashMap::new(),
        });

        Self { buffer, page }
    }
}
