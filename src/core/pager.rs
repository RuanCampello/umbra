use crate::core::PageNumber;
use std::collections::HashSet;
use std::path::PathBuf;

/// Inspired by [SQLite 2.8.1 pager].
/// It manages IO over a "block" in disk to storage the database.
pub(crate) struct Pager<File> {
    file: File,

    /// Block size to read/write a buffer.
    block_size: usize,
    page_size: usize,

    /// The modified files.
    dirty_pages: HashSet<PageNumber>,

    journal: Journal<File>,
}

/// The journal file implements "commit" and "rollback" using a chunk-based format.
/// Each chunk starts with a magic number and a page count, followed by page metadata.
/// Pages store their number, content, and a simple checksum (not a true checksum).
/// An in-memory buffer minimizes sys-calls and avoids file seeks for efficiency.
struct Journal<File> {
    buffer: Vec<u8>,
    page_size: usize,
    /// The maximum of pages to be loaded in memory.
    max_pages: usize,
    /// The current number of pages held in the `buffer`.
    buffered_pages: u32,
    path: PathBuf,
    file: Option<File>,
}

const JOURNAL_NUMBER: u64 = 0x9DD505F920A163D6;
const JOURNAL_SIZE: usize = size_of::<u64>();
const JOURNAL_PAGE_SIZE: usize = size_of::<u16>();
const JOURNAL_CHECKSUM_SIZE: usize = size_of::<u16>();
const JOURNAL_HEADER_SIZE: usize = size_of::<u16>();

impl<File> Journal<File> {
    pub fn new(page_size: usize, max_pages: usize, path: PathBuf) -> Self {
        let mut buffer = Vec::with_capacity(journal_chunk_size(page_size, max_pages));

        buffer.extend_from_slice(&JOURNAL_NUMBER.to_le_bytes());
        buffer.extend_from_slice(&[0; JOURNAL_PAGE_SIZE]);

        Self {
            buffer,
            max_pages,
            path,
            page_size,
            buffered_pages: 0,
            file: None,
        }
    }
}

fn journal_chunk_size(page_size: usize, pages_number: usize) -> usize {
    JOURNAL_SIZE + JOURNAL_PAGE_SIZE + (pages_number) * journal_page_size(page_size)
}

fn journal_page_size(size: usize) -> usize {
    JOURNAL_PAGE_SIZE + size + JOURNAL_CHECKSUM_SIZE
}
