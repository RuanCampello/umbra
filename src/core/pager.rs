use crate::core::io::FileOperations;
use crate::core::random::Rng;
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
    /// The random journal's identifier.
    journal_number: u64,
    path: PathBuf,
    file: Option<File>,
}

type FrameId = usize;

const JOURNAL_SIZE: usize = size_of::<u64>();
const JOURNAL_PAGE_SIZE: usize = size_of::<u32>();
const JOURNAL_CHECKSUM_SIZE: usize = size_of::<u32>();
const JOURNAL_HEADER_SIZE: usize = JOURNAL_SIZE + JOURNAL_PAGE_SIZE;

const DEFAULT_PAGE_SIZE: usize = 4096;
const DEFAULT_BUFFERED_PAGES: usize = 10;

macro_rules! method_builder {
    ($field:ident, $ty:ty) => {
        pub fn $field(mut self, value: $ty) -> Self {
            self.$field = value;
            self
        }
    };
}

impl Pager<io::Cursor<Vec<u8>>> {
    pub fn default() -> Self {
        let io: io::Cursor<Vec<u8>> = io::Cursor::new(Vec::new());
        let block_size = usize::default();

        Self {
            file: BlockIo::new(io, block_size, DEFAULT_PAGE_SIZE),
            journal: Journal::new(
                DEFAULT_PAGE_SIZE,
                DEFAULT_BUFFERED_PAGES,
                PathBuf::default(),
            ),
            dirty_pages: HashSet::new(),
            journal_pages: HashSet::new(),
            page_size: DEFAULT_PAGE_SIZE,
            block_size,
        }
    }

    pub fn journal_path_file(mut self, path: PathBuf) -> Self {
        let journal = Journal::new(self.page_size, self.journal.max_pages, path);
        self.journal = journal;
        self
    }

    method_builder!(block_size, usize);
    method_builder!(page_size, usize);
    method_builder!(dirty_pages, HashSet<PageNumber>);
    method_builder!(journal_pages, HashSet<PageNumber>);
}

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
        let mut rand = Rng::new();
        let journal_number = rand.u64(0..u64::MAX);

        buffer.extend_from_slice(&journal_number.to_le_bytes());
        buffer.extend_from_slice(&[0; JOURNAL_PAGE_SIZE]);

        Self {
            buffer,
            max_pages,
            path,
            page_size,
            journal_number,
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

        let checksum = (self.journal_number as u32).wrapping_add(page_number);

        self.buffer.extend_from_slice(&checksum.to_le_bytes());

        let pages_range = JOURNAL_SIZE..JOURNAL_SIZE + JOURNAL_PAGE_SIZE;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;

    #[test]
    fn test_journal_construction() {
        let page_size: usize = 1024;
        let max: usize = 4;
        let path = PathBuf::from("test_journal");

        let journal = Journal::<Cursor<&[u8]>>::new(page_size, max, path.clone());

        assert_eq!(journal.path, path);
        assert_eq!(journal.page_size, page_size);
        assert_eq!(journal.max_pages, max);
        assert_eq!(journal.buffered_pages, 0);
        assert!(journal
            .buffer
            .starts_with(&journal.journal_number.to_le_bytes()));
    }

    #[test]
    fn test_journal_push() {
        let page_size: usize = 1024;
        let max: usize = 8;
        let path = PathBuf::from("test_journal");

        let mut journal = Journal::<Cursor<Vec<u8>>>::new(page_size, max, path.clone());

        let page_number: PageNumber = 1u32;
        let page_content = vec![1u8; page_size];

        journal
            .push(page_number, &page_content)
            .expect("Was unable to push to journal");
        assert_eq!(journal.buffered_pages, 1);

        let mut output_buffer = Vec::new();
        output_buffer.extend_from_slice(&journal.journal_number.to_le_bytes()); // file header identifier
        output_buffer.extend_from_slice(&1u32.to_le_bytes()); // buffered pages (one)
        output_buffer.extend_from_slice(&page_number.to_le_bytes());
        output_buffer.extend_from_slice(&page_content);
        let checksum = (journal.journal_number as u32).wrapping_add(page_number);
        output_buffer.extend_from_slice(&checksum.to_le_bytes());

        assert_eq!(&journal.buffer[..output_buffer.len()], &output_buffer);

        let second_page_number: PageNumber = 2u32;
        let second_page_content = vec![2u8; page_size];

        journal
            .push(second_page_number, &second_page_content)
            .expect("Was unable to push to journal's second page");
        assert_eq!(journal.buffered_pages, 2);
        assert_eq!(
            journal.buffer.len(),
            JOURNAL_SIZE + JOURNAL_PAGE_SIZE + (2 * journal_page_size(page_size))
        );

        let second_page_start = JOURNAL_SIZE + JOURNAL_PAGE_SIZE + journal_page_size(page_size);
        let mut output_second_page = Vec::new();
        output_second_page.extend_from_slice(&second_page_number.to_le_bytes());
        output_second_page.extend_from_slice(&second_page_content);
        let second_checksum = (journal.journal_number as u32).wrapping_add(second_page_number);
        output_second_page.extend_from_slice(&second_checksum.to_le_bytes());

        assert_eq!(
            &journal.buffer[second_page_start..second_page_start + journal_page_size(page_size)],
            &output_second_page
        );
    }
}
