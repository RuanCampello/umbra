// ! Page's cache implementation.
// !
//! This module provides an in-memory page cache with an eviction policy.
//! It must return owned values by indexing, preventing common Rust's borrowing errors.

use crate::core::page::MemoryPage;
use crate::core::pager::DEFAULT_PAGE_SIZE;
use crate::core::PageNumber;
use crate::method_builder;
use std::collections::HashMap;

// TODO: document this
pub(in crate::core) struct Cache {
    /// See [Pager](super::pager::Pager).
    pub page_size: usize,
    /// The maximum number of pages that this cache can handle.
    max_size: usize,
    pinned_pages: usize,
    /// The buffer pool.
    buffer: Vec<Frame>,
    /// Our babies, the page table. Map with the [`PageNumber`] and
    /// its index on the buffer.
    pages: HashMap<PageNumber, FrameId>,
}

/// Holds a page and bits representing flags' list.
struct Frame {
    flags: u8,
    page_number: PageNumber,
    page: MemoryPage,
}

type FrameId = usize;

const REFERENCE_FLAG: u8 = 0b001;
const DIRTY_FLAG: u8 = 0b010;
const PINNED_FLAG: u8 = 0b100;

const DEFAULT_MAX: usize = 1024;
const DEFAULT_MIN: usize = 2;

impl Cache {
    pub fn default() -> Self {
        Self {
            page_size: DEFAULT_PAGE_SIZE,
            max_size: DEFAULT_MAX,
            buffer: Vec::with_capacity(DEFAULT_MAX),
            pages: HashMap::with_capacity(DEFAULT_MAX),
            pinned_pages: 0,
        }
    }

    pub fn with_max_size(max_size: usize) -> Self {
        assert!(
            max_size >= DEFAULT_MIN,
            "Buffer pool must be at least {DEFAULT_MIN}"
        );

        Self {
            page_size: DEFAULT_PAGE_SIZE,
            max_size,
            buffer: Vec::with_capacity(max_size),
            pages: HashMap::with_capacity(max_size),
            pinned_pages: 0,
        }
    }

    /// Returns the [`FrameId`] of a given page, if exists.
    /// If not or the cache has been invalidated before its call, returns [`None`].
    pub fn get(&mut self, page_number: PageNumber) -> Option<FrameId> {
        self.reference_page(page_number)
    }

    /// Perform the same as [`Self::get`], but marks the flag as [dirty](DIRTY_FLAG).
    pub fn get_mut(&mut self, page_number: PageNumber) -> Option<FrameId> {
        self.reference_page(page_number).inspect(|id| {
            self.buffer[*id].set(DIRTY_FLAG);
        })
    }

    /// Returns the [`FrameId`] of a given page, if exists, and marks it flag as [referenced](REFERENCE_FLAG).
    ///
    /// Otherwise, returns [`None`].
    fn reference_page(&mut self, page_number: PageNumber) -> Option<FrameId> {
        self.pages.get(&page_number).map(|id| {
            self.buffer[*id].set(REFERENCE_FLAG);
            *id
        })
    }

    /// Checks if the given page is cached.
    pub fn contains(&self, page_number: &PageNumber) -> bool {
        self.pages.contains_key(page_number)
    }

    method_builder!(page_size, usize);
    method_builder!(max_size, usize);
    method_builder!(pinned_pages, usize);
}

impl Frame {
    fn new(page_number: PageNumber, page: MemoryPage) -> Self {
        Self {
            flags: 0,
            page_number,
            page,
        }
    }

    fn is_set(&self, flags: u8) -> bool {
        self.flags & flags == flags
    }

    fn set(&mut self, flags: u8) {
        self.flags |= flags
    }

    fn unset(&mut self, flags: u8) {
        self.flags &= !flags
    }
}

impl std::ops::Index<FrameId> for Cache {
    type Output = MemoryPage;
    fn index(&self, index: FrameId) -> &Self::Output {
        &self.buffer[index].page
    }
}
