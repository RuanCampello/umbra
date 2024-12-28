use crate::core::{
    page::{buffer::BufferWithHeader, Page, PageHeader},
    PageNumber,
};
use std::collections::HashMap;
use std::mem::ManuallyDrop;

/// The first page of the DB file (offset 0) is a special case.
///
/// It contains an additional [`DatabaseHeader`] along with the regular [`Page`] data.
/// This results in less usable space for storing [`crate::core::page::Cell`] instances compared
/// to subsequent pages.
///
/// Note: While B-Tree balancing algorithms typically operate on pages of
/// uniform size, this special case for the first page does not introduce
/// any significant issues, especially with reasonable page sizes (e.g., 4096 bytes or larger).
/// Using an entire page solely for the [`DatabaseHeader`] would be inefficient in such cases.
pub(crate) struct PageZero {
    /// Page buffer, including the [`DatabaseHeader`].
    buffer: ManuallyDrop<BufferWithHeader<DatabaseHeader>>,
    /// Inner B-Tree slotted page.
    page: ManuallyDrop<Page>,
}

/// Header of the database file.
///
/// This struct is located at the beginning of the database file and contains
/// essential metadata used by the [`crate::core::pager::Pager`] for
/// managing pages and free space within the database.
#[derive(Debug)]
struct DatabaseHeader {
    identifier: u32,
    page_size: u16,
    total_pages: u16,
    free_pages: u16,
    first_free_page: PageNumber,
    last_free_page: PageNumber,
}

/// This means literally "dusk", because "umbra" wouldn't fit in an [`core::u32`].
const DATABASE_IDENTIFIER: u32 = 0x6475736b;

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
