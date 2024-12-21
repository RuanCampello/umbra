use crate::core::io::FileOperations;
use crate::core::{io::BlockIo, PageNumber};
use std::collections::HashSet;
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

/// Inspired by [SQLite 2.8.1 pager].
/// It manages IO over a "block" in disk to storage the database.
pub(crate) struct Pager<File> {
    file: BlockIo<File>,
    /// Block size to read/write a buffer.
    block_size: usize,
    page_size: usize,
    /// The modified files.
    dirty_pages: HashSet<PageNumber>,
    /// Written pages.
    journal_pages: HashSet<PageNumber>,
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

type FrameId = usize;

const JOURNAL_NUMBER: u64 = 0x9DD505F920A163D6;
const JOURNAL_SIZE: usize = size_of::<u64>();
const JOURNAL_PAGE_SIZE: usize = size_of::<u16>();
const JOURNAL_CHECKSUM_SIZE: usize = size_of::<u16>();
const JOURNAL_HEADER_SIZE: usize = size_of::<u16>();

impl<File: Seek + Read> Pager<File> {
    /// Reads directly from the disk.
    fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read(page_number, buffer)
    }
}

impl<File: Seek + FileOperations> Pager<File> {
    /// Adds a given page to the queue of "pages to be written" if it has not being already written.
    fn push_to_queue(
        &mut self,
        page_number: PageNumber,
        content: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        self.dirty_pages.insert(page_number);

        if !self.journal_pages.contains(&page_number) {
            self.journal.push(page_number, content)?;
            self.journal_pages.insert(page_number);
        }

        Ok(())
    }
}

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

    pub fn clear_buff(&mut self) {
        self.buffer.drain(JOURNAL_SIZE..);
        self.buffer.extend_from_slice(&[0; JOURNAL_PAGE_SIZE]);
        self.buffered_pages = 0;
    }
}

impl<File: Write> Journal<File> {
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.as_mut().unwrap().flush()
    }
}

impl<File: FileOperations> Journal<File> {
    /// Add a page to the journal buffer.
    pub fn push(&mut self, page_number: PageNumber, page: impl AsRef<[u8]>) -> io::Result<()> {
        if self.buffered_pages as usize >= self.max_pages {
            // TODO: when there are not space, just write to empty the queue.
        }

        self.buffer.extend_from_slice(&page_number.to_le_bytes()); // write the page_number.
        self.buffer.extend_from_slice(page.as_ref()); // write its actual content.

        // TODO: instead of this, maybe generate a random number would be a great place to be.
        let checksum = (JOURNAL_NUMBER as u16).wrapping_add(page_number);

        self.buffer.extend_from_slice(&checksum.to_le_bytes());

        let pages_range = (JOURNAL_NUMBER as usize)..JOURNAL_SIZE + JOURNAL_PAGE_SIZE;

        self.buffered_pages += 1;
        self.buffer[pages_range].copy_from_slice(&self.buffered_pages.to_le_bytes());
        Ok(())
    }
}

fn journal_chunk_size(page_size: usize, pages_number: usize) -> usize {
    JOURNAL_SIZE + JOURNAL_PAGE_SIZE + (pages_number) * journal_page_size(page_size)
}

fn journal_page_size(size: usize) -> usize {
    JOURNAL_PAGE_SIZE + size + JOURNAL_CHECKSUM_SIZE
}
