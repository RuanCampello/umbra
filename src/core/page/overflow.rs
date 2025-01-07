use crate::core::{page::buffer::BufferWithHeader, PageNumber};

/// Cell overflow page.
///
/// Stores the overflow data for cells that exceed the maximum size allowed
/// within a regular page. Overflow pages are linked together using the
/// `next` field in the [`OverflowPageHeader`].
///
/// ```text
/// PAGE       SLOT          FREE
/// HEADER     ARRAY         SPACE         CELLS
/// +----------------------------------------------------------------+
/// | PAGE   | +---+                      +--------+---------+-----+ |
/// | HEADER | | 1 | ->                <- | HEADER | PAYLOAD | OVF | |
/// |        | +---+                      +--------+---------+--|--+ |
/// +------------|----------------------------------------------|----+
///              |                        ^                     |
///              |                        |                     |
///              +------------------------+                     |
///                                                             |
///      +------------------------------------------------------+
///      |
///      V
/// +--------------------+-------------------------------------------+
/// | +----------------+ | +---------------------------------------+ |
/// | | next | n_bytes | | |    OVERFLOW PAYLOAD (FULL PAGE)       | |
/// | +--|---+---------+ | +---------------------------------------+ |
/// +----|---------------+-------------------------------------------+
///      |
///      V
/// +--------------------+-------------------------------------------+
/// | +----------------+ | +------------------------+                |
/// | | next | n_bytes | | | REMAINING OVF PAYLOAD  |                |
/// | +------+---------+ | +------------------------+                |
/// +--------------------+-------------------------------------------+
/// OVERFLOW PAGE                    OVERFLOW PAGE CONTENT
/// HEADER                    (stores as many bytes as needed)
/// ```
///
/// When a [CellHeader::is_overflow](super::CellHeader) flag is set in a cell's header,
/// the last 4 bytes of the cell's payload contain a pointer to the first
/// overflow page for that cell.
#[derive(Debug, PartialEq, Clone)]
pub(in crate::core::page) struct OverflowPage {
    /// In-memory page buffer.
    buffer: BufferWithHeader<OverflowPageHeader>,
}

/// Header of an overflow page.
#[derive(Debug, Default, PartialEq, Clone)]
pub(in crate::core::page) struct OverflowPageHeader {
    /// Next overflow page in the linked list.
    pub next: PageNumber,
    /// Number of bytes stored in this page.
    pub num_bytes: u16,
    /// Padding for alignment.
    pub padding: u16,
}

/// Free pages can be represented using the [`OverflowPage`] struct,
/// as they only require a linked list for efficient management.
type FreePage = OverflowPage;

impl OverflowPage {
    pub fn alloc(size: usize) -> Self {
        Self::from(BufferWithHeader::<OverflowPageHeader>::for_page(size))
    }
}

impl OverflowPageHeader {
    pub fn new(next: PageNumber, num_bytes: u16) -> Self {
        Self {
            next,
            num_bytes,
            padding: 0,
        }
    }
}

impl<Header> From<BufferWithHeader<Header>> for OverflowPage {
    fn from(buffer: BufferWithHeader<Header>) -> Self {
        let mut buffer: BufferWithHeader<OverflowPageHeader> = buffer.cast();
        *buffer.mutable_header() = OverflowPageHeader::default();

        Self { buffer }
    }
}

impl AsRef<[u8]> for OverflowPage {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}

impl  AsMut<[u8]> for OverflowPage {
    fn as_mut(&mut self) -> &mut [u8] {
        self.buffer.as_mut()
    }
}