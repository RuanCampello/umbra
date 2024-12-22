//! IO operations based on "blocks" to suit a database.

use crate::core::PageNumber;
use std::fs::File;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::{fs, io};

pub(in crate::core) struct BlockIo<IO> {
    /// Wrapped I/O operator.
    io: IO,
    block_size: usize,
    page_size: usize,
}

const DEVELOPMENT_IO_LIMIT: usize = 150 << 20;

/// Generic file related operations that are not implemented by [`io`].
pub(in crate::core) trait FileOperations {
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized;

    fn delete(path: impl AsRef<Path>) -> io::Result<()>;

    /// Tries to store the data on disk to its respective [`Path`].
    fn save(&self) -> io::Result<()>;
}

impl FileOperations for File {
    fn create(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        if let Some(parent_dir) = path.as_ref().parent() {
            fs::create_dir_all(parent_dir)?;
        }

        File::options()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
    }

    fn open(path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        File::options().read(true).write(false).open(path)
    }

    fn delete(path: impl AsRef<Path>) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn save(&self) -> io::Result<()> {
        self.sync_all()
    }
}

/// This is a memory buffer implementation used primarily for test, but it can be used as the most primitive form of in-memory database.
impl FileOperations for Cursor<Vec<u8>> {
    fn create(_path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        Ok(Cursor::new(Vec::new()))
    }
    fn delete(_path: impl AsRef<Path>) -> io::Result<()> {
        Ok(())
    }
    fn open(_path: impl AsRef<Path>) -> io::Result<Self>
    where
        Self: Sized,
    {
        Ok(Cursor::new(Vec::new()))
    }
    fn save(&self) -> io::Result<()> {
        Ok(())
    }
}

impl<IO> BlockIo<IO> {
    pub fn new(io: IO, block_size: usize, page_size: usize) -> Self {
        Self {
            io,
            block_size,
            page_size,
        }
    }

    /// Checks the length of a buffer and its size on development to prevent chaos.
    fn assert_args(&self, page_number: &PageNumber, buffer: &[u8]) {
        debug_assert!(
            buffer.len() == self.page_size,
            "Buffer with incorrect length: {} for page with size {}",
            buffer.len(),
            self.page_size,
        );

        debug_assert!(
            self.page_size * (*page_number as usize) < DEVELOPMENT_IO_LIMIT,
            "Page number {page_number} too big for page with size {}. Limit of 150 MiB.",
            self.page_size
        )
    }
}

impl<IO: Seek + Read> BlockIo<IO> {
    pub fn read(&mut self, page_number: PageNumber, buffer: &mut [u8]) -> io::Result<usize> {
        self.assert_args(&page_number, buffer);

        // The block offset and the inner page body offset.
        let (cap, block_offset, body_offset) = {
            let page_number = page_number as usize;

            let Self {
                page_size,
                block_size,
                ..
            } = *self;

            if page_size >= block_size {
                (page_size, page_size * page_number, 0)
            } else {
                let offset = (page_size * page_number) & !(block_size - 1);
                (block_size, page_size, page_number - offset)
            }
        };

        self.io.seek(SeekFrom::Start(block_offset as u64))?;

        // Reads the page into the memory.
        if self.page_size >= self.block_size {
            return self.io.read(buffer);
        }

        // If the page is not greater than the block, multiple pages can be read at once.
        let mut block: Vec<u8> = vec![0; cap];
        let _ = self.io.read(&mut block)?;

        buffer.copy_from_slice(&block[body_offset..body_offset + self.page_size]);
        Ok(self.page_size)
    }
}

impl<IO: Write> BlockIo<IO> {
    pub fn flush(&mut self) -> io::Result<()> {
        self.io.flush()
    }
}

impl<IO: FileOperations> BlockIo<IO> {
    pub fn save(&self) -> io::Result<()> {
        self.io.save()
    }
}
