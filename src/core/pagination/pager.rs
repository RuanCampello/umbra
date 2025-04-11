use super::cache::{Cache, FrameId};
use super::io::{BlockIo, FileOperations};
use crate::core::btree::Content;
use crate::core::page::overflow::OverflowPage;
use crate::core::page::zero::{PageZero, DATABASE_IDENTIFIER};
use crate::core::page::{Cell, MemoryPage, Page, PageConversion, SlotId};
use crate::core::random::Rng;
use crate::core::PageNumber;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::fmt::{Debug, Formatter};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

/// Inspired by [SQLite 2.8.1 pager].
/// It manages IO over a "block" in disk to storage the database.
pub(in crate::core) struct Pager<File> {
    file: BlockIo<File>,
    cache: Cache,
    /// Block size to read/write a buffer.
    block_size: usize,
    pub page_size: usize,
    /// The modified files.
    dirty_pages: HashSet<PageNumber>,
    /// Written pages.
    journal_pages: HashSet<PageNumber>,
    journal: Journal<File>,
}

/// The journal file implements "commit" and "rollback" using a chunk-based format.
/// Each chunk starts with a magic number and a page count, followed by page metadata.
/// Pages store their number, content, and a simple checksum (not a true checksum).
/// An in-memory buffer minimizes sys-calls and avoids a file seeks for efficiency.
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

const JOURNAL_SIZE: usize = size_of::<u64>();
const JOURNAL_PAGE_SIZE: usize = size_of::<u32>();
const JOURNAL_CHECKSUM_SIZE: usize = size_of::<u32>();
const JOURNAL_HEADER_SIZE: usize = JOURNAL_SIZE + JOURNAL_PAGE_SIZE;

pub(in crate::core) const DEFAULT_PAGE_SIZE: usize = 4096;
const DEFAULT_BUFFERED_PAGES: usize = 10;

#[macro_export]
macro_rules! method_builder {
    ($field:ident, $ty:ty) => {
        pub fn $field(mut self, value: $ty) -> Self {
            self.$field = value;
            self
        }
    };
}

impl Pager<io::Cursor<Vec<u8>>> {
    /// Creates a default `Pager` with [`Cursor<Vec<u8>>`] as the file.
    pub fn default() -> Self {
        let io = io::Cursor::new(Vec::new());
        let block_size = usize::default();
        let cache = Cache::default();

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
            cache,
        }
    }
}

impl<File> Pager<File> {
    pub fn new(file: File) -> Self {
        let block_size = usize::default();
        let cache = Cache::default();

        Self {
            file: BlockIo::new(file, block_size, DEFAULT_PAGE_SIZE),
            journal: Journal::new(
                DEFAULT_PAGE_SIZE,
                DEFAULT_BUFFERED_PAGES,
                PathBuf::default(),
            ),
            dirty_pages: HashSet::new(),
            journal_pages: HashSet::new(),
            page_size: DEFAULT_PAGE_SIZE,
            block_size,
            cache,
        }
    }

    pub fn journal_path_file(mut self, path: PathBuf) -> Self {
        let journal = Journal::new(self.page_size, self.journal.max_pages, path);
        self.journal = journal;
        self
    }

    pub fn page_size(mut self, page_size: usize) -> Self {
        self.update_pages_size(page_size);
        self
    }

    pub fn block_size(mut self, block_size: usize) -> Self {
        self.update_blocks_size(block_size);
        self
    }

    /// Update all instances that use `page_size` to the new `page_size`.
    fn update_pages_size(&mut self, page_size: usize) {
        self.page_size = page_size;
        self.cache.page_size = page_size;
        self.journal.page_size = page_size;
        self.file.page_size = page_size;
    }

    /// Performs the same as [`Self::update_pages_size`] but for `block_size`.
    fn update_blocks_size(&mut self, block_size: usize) {
        self.block_size = block_size;
        self.file.block_size = block_size;
    }

    method_builder!(dirty_pages, HashSet<PageNumber>);
    method_builder!(journal_pages, HashSet<PageNumber>);
}

impl<File: Seek + Write + Read + FileOperations> Pager<File> {
    /// Initialise a database file.
    pub fn init(&mut self) -> io::Result<()> {
        let mut page_zero = PageZero::alloc(self.page_size);
        page_zero.as_mut().fill(0);
        self.file.read(0, page_zero.as_mut())?;

        let header = page_zero.buffer.header();
        let identifier = header.identifier;
        let page_size = header.page_size as usize;

        if identifier == DATABASE_IDENTIFIER {
            &self.update_pages_size(page_size);

            return Ok(());
        }

        if identifier.swap_bytes() == DATABASE_IDENTIFIER {
            panic!("The database file identifier was created using a different endian")
        }

        let page_zero = PageZero::alloc(self.page_size);
        self.write(0, page_zero.as_ref())?;

        Ok(())
    }

    /// Returns a reference to a [Btree](crate::core::btree) [page](crate::core::page::Page).
    pub fn get(&mut self, page_number: PageNumber) -> io::Result<&Page> {
        self.get_as::<Page>(page_number)
    }

    pub fn get_as<'p, Page>(&'p mut self, page_number: PageNumber) -> io::Result<&Page>
    where
        Page: PageConversion + AsMut<[u8]>,
        &'p Page: TryFrom<&'p MemoryPage>,
        <&'p Page as TryFrom<&'p MemoryPage>>::Error: Debug,
    {
        let idx = self.lookup::<Page>(page_number)?;
        let memory_page = &self.cache[idx];

        println!("idx found in the lookup {idx}");

        Ok(memory_page.try_into().expect("Error converting page type"))
    }

    pub fn get_mut(&mut self, page_number: PageNumber) -> io::Result<&mut Page> {
        self.get_mut_as::<Page>(page_number)
    }

    /// The same as [get_as](Self::get_as) but for a mutable reference. Of course, it marks the page as `dirty`.
    pub fn get_mut_as<'p, Page>(&'p mut self, page_number: PageNumber) -> io::Result<&mut Page>
    where
        Page: PageConversion + AsMut<[u8]>,
        &'p mut Page: TryFrom<&'p mut MemoryPage>,
        <&'p mut Page as TryFrom<&'p mut MemoryPage>>::Error: std::fmt::Debug,
    {
        let idx = self.lookup::<Page>(page_number)?;
        self.push_to_written_queue(page_number, idx)?;

        let memory_page = &mut self.cache[idx];

        Ok(memory_page.try_into().expect("Error converting page type"))
    }

    /// Frees the pages used by this given [`Cell`].
    pub fn free_cell(&mut self, cell: Box<Cell>) -> io::Result<()> {
        if !cell.header.is_overflow {
            return Ok(());
        }

        let mut overflow_page = cell.overflow_page();
        while overflow_page != 0 {
            let page = self.get_as::<OverflowPage>(overflow_page)?;
            let next = page.buffer.header().next;

            self.free_page(overflow_page)?;
            overflow_page = next
        }

        Ok(())
    }

    /// Adds a given page to the `free` list.
    pub fn free_page(&mut self, page_number: PageNumber) -> io::Result<()> {
        let idx = self.lookup::<OverflowPage>(page_number)?;
        self.push_to_written_queue(page_number, idx)?;

        self.cache[idx].reinit_as::<OverflowPage>();

        let mut header = self.get_as::<PageZero>(0).map(PageZero::header).copied()?;

        match header.first_free_page == 0 {
            true => header.free_pages = page_number as _,
            false => {
                let last_free = self.get_mut_as::<OverflowPage>(page_number)?;
                last_free.buffer.mutable_header().next = page_number;
            }
        }

        header.free_pages += 1;
        header.last_free_page = page_number;

        *self.get_mut_as::<PageZero>(0)?.buffer.mutable_header() = header;

        Ok(())
    }

    /// Same as [allocate_page_disk](Self::allocate_page_disk) but maps into a given page type and create a [`Cache`] entry for it.
    pub fn allocate_page<Page: PageConversion>(&mut self) -> io::Result<PageNumber> {
        let page_number = self.allocate_page_disk()?;
        self.map_page::<Page>(page_number)?;

        Ok(page_number)
    }

    /// Allocates a new page on disk.
    fn allocate_page_disk(&mut self) -> io::Result<PageNumber> {
        let mut header = self.get_as::<PageZero>(0).map(PageZero::header).copied()?;

        let free_page = match header.first_free_page == 0 {
            true => {
                let page = header.total_pages;
                header.total_pages += 1;

                page.into()
            }
            false => {
                let page = header.first_free_page;
                let free_page = self.get_as::<OverflowPage>(page)?;
                header.first_free_page = free_page.buffer.header().next;
                header.free_pages -= 1;

                page
            }
        };

        if header.first_free_page == 0 {
            header.last_free_page = 0;
        };

        *self.get_mut_as::<PageZero>(0)?.buffer.mutable_header() = header;

        Ok(free_page)
    }

    /// Returns the cache id for a given [`PageNumber`].
    fn lookup<Page: PageConversion + AsMut<[u8]>>(
        &mut self,
        page_number: PageNumber,
    ) -> io::Result<usize> {
        if let Some(idx) = self.cache.get(page_number) {
            return Ok(idx);
        }

        match page_number {
            0 => self.load::<PageZero>(page_number)?,
            _ => self.load::<Page>(page_number)?,
        }

        Ok(self.cache.get(page_number).unwrap())
    }

    /// Like [`Cache::load`], loads the page from disk and move it to the cache.
    fn load<Page: PageConversion + AsMut<[u8]>>(
        &mut self,
        page_number: PageNumber,
    ) -> io::Result<()> {
        let idx = self.map_page::<Page>(page_number)?;
        self.file.read(page_number, self.cache[idx].as_mut())?;

        Ok(())
    }

    /// Maps the given [`PageNumber`] into an entry on cache.
    fn map_page<Page: PageConversion>(&mut self, page_number: PageNumber) -> io::Result<usize> {
        println!("Mapping page {}", page_number);
        if self.cache.must_evict_dirty_page() {
            self.write_dirty_pages()?;
        }

        let idx = self.cache.map(page_number);
        self.cache[idx].reinit_as::<Page>();

        Ok(idx)
    }
}

impl<File: Seek + Write + FileOperations> Pager<File> {
    pub fn write_dirty_pages(&mut self) -> io::Result<()> {
        if self.dirty_pages.is_empty() {
            return Ok(());
        }

        self.journal.persist()?;

        let page_numbers = BinaryHeap::from_iter(self.dirty_pages.iter().copied().map(Reverse));
        
        for Reverse(page_number) in page_numbers {
            let idx = self.cache.get(page_number).unwrap();
            let page = &self.cache[idx];
            
            self.file.write(page_number, page.as_ref())?;
            self.cache.mark_clean(page_number);
            self.dirty_pages.remove(&page_number);
        }
        
        Ok(())
    }
}

impl<File: Seek + Write> Pager<File> {
    pub fn write(&mut self, page_number: PageNumber, buffer: &[u8]) -> io::Result<usize> {
        self.file.write(page_number, buffer)
    }
}

impl<File: Seek + Read> Pager<File> {
    /// Reads directly from the disk.
    fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read(page_number, buffer)
    }
}

impl<File: Seek + FileOperations> Pager<File> {
    /// Adds a given page to the queue of "pages to be written" if it has not been already written.
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

impl<File: Write + FileOperations> Pager<File> {
    /// Append the given page to `already written` pages in the journal.
    fn push_to_written_queue(&mut self, page_number: PageNumber, id: FrameId) -> io::Result<()> {
        // TODO: mark as `dirty` in cache
        self.dirty_pages.insert(page_number);

        if !self.journal_pages.contains(&page_number) {
            self.journal_pages.insert(page_number);
            self.journal.push(page_number, &self.cache[id])?;
        }

        Ok(())
    }
}

impl<File> Debug for Pager<File> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pager")
            .field("page_size", &self.page_size)
            .field("cache_size", &self.cache.max_size)
            .field("journal_buffer_size", &self.journal.max_pages)
            .finish()
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

impl<File: Write + FileOperations> Journal<File> {
    /// Saves the data written to the journal to disk.
    pub fn persist(&mut self) -> io::Result<()> {
        self.write()?;
        self.flush()?;
        self.sync()?;

        Ok(())
    }

    /// Passes the in-memory buffer to the journal file then clears it.
    fn write(&mut self) -> io::Result<()> {
        if self.buffer.len() <= JOURNAL_HEADER_SIZE {
            return Ok(());
        }

        if self.file.is_none() {
            self.file = Some(FileOperations::create(&self.path)?);
        }

        self.file.as_mut().unwrap().write_all(&self.buffer)?;
        self.clear_buff();

        Ok(())
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

    fn sync(&mut self) -> io::Result<()> {
        self.file.as_mut().unwrap().save()
    }
}

/// Joins a given page [content](Content) into a contiguous memory space.
pub(in crate::core) fn reassemble_content<File: Seek + Write + Read + FileOperations>(
    pager: &mut Pager<File>,
    page_number: PageNumber,
    slot_id: SlotId,
) -> io::Result<Content> {
    let cell = pager.get(page_number)?.cell(slot_id);

    if !cell.header.is_overflow {
        return Ok(Content::PageRef(
            &pager.get(page_number)?.cell(slot_id).content,
        ));
    }

    let mut overflow = cell.overflow_page();
    let mut content = Vec::from(&cell.content[..cell.content.len() - size_of::<PageNumber>()]);

    while overflow != 0 {
        let page = pager.get_as::<OverflowPage>(overflow)?;
        content.extend_from_slice(page.content());
        let page_header = page.buffer.header();

        let next = page_header.next;
        debug_assert_ne!(
            next, overflow,
            "Overflow Page points to itself, causing an infinity loop {page_header:#?}",
        );

        overflow = next
    }

    Ok(Content::Reassembled(content.into()))
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
