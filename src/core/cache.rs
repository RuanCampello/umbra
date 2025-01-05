//! Page's cache implementation.
//!
//! This module provides an in-memory page cache with an eviction policy.
//! It must return owned values by indexing, preventing common Rust's borrowing errors.

use crate::core::page::MemoryPage;
use crate::core::pager::DEFAULT_PAGE_SIZE;
use crate::core::PageNumber;
use crate::method_builder;
use std::collections::HashMap;

/// # Clock-Based Page Cache
///
/// Implements a page cache with a clock eviction policy.
///
/// ## Structure
///
/// The cache uses a **buffer pool** to store pages, with each page wrapped in
/// a [`Frame`], which includes metadata like a reference bit and dirty flag.
///
/// A **page table** maps page numbers to buffer pool indexes using a
/// [`HashMap`]. When the buffer reaches its `max_size`, the clock algorithm
/// decides which page to evict.
///
/// Example of the cache with `max_size = 4` after loading pages `[1, 2, 3, 4]`:
///
/// ```text
///    PAGE TABLE                      BUFFER POOL
/// +------+-------+               +---------------+
/// | PAGE | FRAME |               | PAGE 1        |
/// +------+-------+               | ref: 0 dty: 0 |
/// |  1   |   0   |               +---------------+
/// +------+-------+                       ^
/// |  2   |   1   |                       |
/// +------+-------+   +---------------+   +----------+   +---------------+
/// |  3   |   2   |   | PAGE 4        |   | CLOCK: 0 |   | PAGE 2        |
/// +------+-------+   | ref: 0 dty: 0 |   +----------+   | ref: 0 dty: 0 |
/// |  4   |   3   |   +---------------+                  +---------------+
/// +------+-------+
///                                   +---------------+
///                                   | PAGE 3        |
///                                   | ref: 0 dty: 0 |
///                                   +---------------+
/// ```
///
/// ## Eviction Policy
///
/// When the buffer is full, the **clock algorithm** evicts pages:
/// - The clock pointer cycles through frames.
/// - If a page's reference bit is `0`, it is evicted.
/// - If a page's reference bit is `1`, it is reset to `0` for future eviction.
///
/// Example: After loading page 5, page 1 is evicted:
///
/// ```text
///    PAGE TABLE                      BUFFER POOL
/// +------+-------+               +---------------+
/// | PAGE | FRAME |               | PAGE 5        |
/// +------+-------+               | ref: 1 dty: 0 |
/// |  5   |   0   |               +---------------+
/// +------+-------+                       ^
/// |  2   |   1   |                       |
/// +------+-------+   +---------------+   +----------+   +---------------+
/// |  3   |   2   |   | PAGE 4        |   | CLOCK: 0 |   | PAGE 2        |
/// +------+-------+   | ref: 0 dty: 0 |   +----------+   | ref: 0 dty: 0 |
/// |  4   |   3   |   +---------------+                  +---------------+
/// +------+-------+
///                                   +---------------+
///                                   | PAGE 3        |
///                                   | ref: 0 dty: 0 |
///                                   +---------------+
/// ```
///
/// ## Reference & Dirty Bits
///
/// - **Reference Bit**: Tracks whether a page was recently used.
/// - **Dirty Flag**: Set when a page is modified (via mutable access).
///
/// ## Key Properties
///
/// - **Efficient Eviction**: Approximation of Least Recently Used (LRU).
/// - **Write Tracking**: Dirty pages are identified for disk writes.
///
pub(in crate::core) struct Cache {
    /// See [Pager](super::pager::Pager).
    pub page_size: usize,
    /// The maximum number of pages that this cache can handle.
    max_size: usize,
    pinned_pages: usize,
    clock: FrameId,
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
            clock: 0,
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
            clock: 0,
        }
    }

    /// Invalidates the given page.
    pub fn invalidate(&mut self, page_number: PageNumber) {
        if let Some(id) = self.pages.remove(&page_number) {
            self.buffer[id].flags = 0;
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

    /// Maps a given [`PageNumber`] to a [`FrameId`].
    pub fn map(&mut self, page_number: PageNumber) -> FrameId {
        // best case: the page is already cached
        if let Some(id) = self.pages.get(&page_number) {
            return *id;
        }

        // if the buffer is not full, we can alloc the new page and return its id
        if self.buffer.len() < self.max_size {
            let id = self.buffer.len();
            self.pages.insert(page_number, id);

            self.buffer
                .push(Frame::new(page_number, MemoryPage::alloc(id)))
        }

        // the buffer is full, so we must evict
        self.cycle_clock();

        let frame = &mut self.buffer[self.clock];
        self.pages.remove(&frame.page_number);

        // the current frame now holds the new page
        frame.page_number = page_number;
        frame.set(REFERENCE_FLAG);
        self.pages.insert(page_number, self.clock);

        self.clock
    }

    /// [Ticks](Self::tick) clock until it points to a page that can be evicted.
    fn cycle_clock(&mut self) {
        let initial_round = self.clock;
        let mut rounds = 0;

        while !self.is_evictable(self.clock) {
            self.buffer[self.clock].unset(REFERENCE_FLAG);
            self.tick();

            #[cfg(debug_assertions)]
            {
                if rounds == 10 {
                    panic!("clock has go {rounds} full round and didn't found any page that could be evicted")
                }

                if self.clock == initial_round {
                    rounds += 1;
                }
            }
        }
    }

    /// Moves the clock to buffer's next frame.
    fn tick(&mut self) {
        self.clock = (self.clock + 1) % self.buffer.len();
    }

    /// Returns `true` if the page at the given [`FrameId`] can be evicted safely.
    fn is_evictable(&self, frame_id: FrameId) -> bool {
        let frame = &self.buffer[frame_id];

        !frame.is_set(REFERENCE_FLAG) && !frame.is_set(PINNED_FLAG) && !frame.page.is_overflow()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::page::*;

    impl Cache {
        pub fn with_pages(number_of_pages: usize, max_size: usize) -> (Self, Vec<MemoryPage>) {
            const TEST_PAGE_SIZE: usize = 256;

            let pages = (0..number_of_pages).map(|idx| {
                let mut page = Page::alloc(TEST_PAGE_SIZE);
                let content = vec![idx as u8; Page::ideal_max_content_size(TEST_PAGE_SIZE, 1)];

                page.push(Cell::new(content));
                MemoryPage::Ordinary(page)
            });

            let mut cache = Self::default();
            let iterator = (0..pages.len()).zip(pages.clone()).take(number_of_pages);

            iterator.for_each(|(page_number, mem_page)| {
                let id = cache.map(page_number as _);
                cache.buffer[id].page = mem_page;
            });

            (cache, pages.collect())
        }
    }
}
