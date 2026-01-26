//! Page's cache implementation.
//!
//! This module provides an in-memory page cache with an eviction policy.
//! It must return owned values by indexing, preventing common Rust's borrowing errors.

#![allow(private_interfaces, unused)]

use crate::collections::hash::{BuildHasher, HashMap};
use crate::storage::page::MemoryPage;
use crate::storage::page::PageNumber;
use crate::storage::pagination::pager::DEFAULT_PAGE_SIZE;
use crate::method_builder;
use std::mem;

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
#[derive(Debug, PartialEq)]
pub(crate) struct Cache {
    /// See [Pager](super::pager::Pager).
    pub page_size: usize,
    /// The maximum number of pages that this cache can handle.
    pub max_size: usize,
    pinned_pages: usize,
    /// The maximum percentage of pages that can be [pinned](Cache::pin) at once.
    max_pinned_percentage: f32,
    clock: FrameId,
    /// The buffer pool.
    buffer: Vec<Frame>,
    /// Our babies, the page table. Map with the [`PageNumber`] and
    /// its index on the buffer.
    pages: HashMap<PageNumber, FrameId>,
}

/// Holds a page and bits representing flags' list.
#[derive(Debug, PartialEq)]
struct Frame {
    flags: u8,
    page_number: PageNumber,
    page: MemoryPage,
}

pub(in crate::storage) type FrameId = usize;

const REFERENCE_FLAG: u8 = 0b001;
const DIRTY_FLAG: u8 = 0b010;
const PINNED_FLAG: u8 = 0b100;

const DEFAULT_MAX: usize = 1024;
const DEFAULT_MIN: usize = 2;
const DEFAULT_MAX_PINNED_PERCENTAGE: f32 = 60.0;

impl Cache {
    pub fn default() -> Self {
        Self {
            pinned_pages: 0,
            clock: 0,
            page_size: DEFAULT_PAGE_SIZE,
            max_size: DEFAULT_MAX,
            max_pinned_percentage: DEFAULT_MAX_PINNED_PERCENTAGE,
            buffer: Vec::with_capacity(DEFAULT_MAX),
            pages: HashMap::with_capacity_and_hasher(DEFAULT_MAX, BuildHasher),
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
            pages: HashMap::with_capacity_and_hasher(max_size, BuildHasher),
            max_pinned_percentage: DEFAULT_MAX_PINNED_PERCENTAGE,
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

    /// Loads a page from memory into the buffer and returns the evicted page.
    pub fn load(&mut self, page_number: PageNumber, page: MemoryPage) -> MemoryPage {
        let id = self.map(page_number);
        mem::replace(&mut self.buffer[id].page, page)
    }

    /// Pin is meant to mark a given page as `unevictable`.
    /// Returns `true` if the page exists and was pinned.
    pub fn pin(&mut self, page_number: PageNumber) -> bool {
        let pinned_percentage = (self.pinned_pages as f32 / self.max_size as f32) * 100.0;

        if pinned_percentage >= self.max_pinned_percentage {
            return false;
        }

        let pinned = self.set_flags(page_number, PINNED_FLAG);
        if pinned {
            self.pinned_pages += 1
        }

        pinned
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
                .push(Frame::new(page_number, MemoryPage::alloc(self.page_size)));

            return id;
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

    /// Checks if the current page in the clock buffer should be evicted.
    /// Returns `true` only if the current clock page is [dirty](DIRTY_FLAG) and the `buffer` is full.
    pub fn must_evict_dirty_page(&mut self) -> bool {
        if self.buffer.len() < self.max_size {
            return false;
        }

        self.cycle_clock();
        self.buffer[self.clock].is_set(DIRTY_FLAG)
    }

    pub fn mark_dirty(&mut self, page_number: PageNumber) -> bool {
        self.set_flags(page_number, DIRTY_FLAG)
    }

    pub fn mark_clean(&mut self, page_number: PageNumber) -> bool {
        self.unset_flags(page_number, DIRTY_FLAG)
    }

    fn unset_flags(&mut self, page_number: PageNumber, flags: u8) -> bool {
        self.pages.get(&page_number).map_or(false, |frame_id| {
            self.buffer[*frame_id].unset(flags);

            true
        })
    }

    fn set_flags(&mut self, page_number: PageNumber, flags: u8) -> bool {
        self.pages.get(&page_number).map_or(false, |id| {
            self.buffer[*id].set(PINNED_FLAG);
            true
        })
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
                if rounds == 3 {
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
    method_builder!(max_pinned_percentage, f32);
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

impl std::ops::IndexMut<FrameId> for Cache {
    fn index_mut(&mut self, index: FrameId) -> &mut Self::Output {
        &mut self.buffer[index].page
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::page::*;

    enum Fetch {
        All,
        UntilBufferEnd,
    }

    impl Cache {
        fn with_pages(
            number_of_pages: usize,
            max_size: usize,
            fetch: Fetch,
        ) -> (Self, Vec<MemoryPage>) {
            const PAGE_SIZE: usize = 256;
            let pages = (0..number_of_pages).map(|idx| {
                let mut page = Page::alloc(PAGE_SIZE);
                let ideal_size = Page::ideal_max_content_size(PAGE_SIZE, 1);
                let content = vec![idx as u8; ideal_size];
                page.push(Cell::new(content));

                MemoryPage::Ordinary(page)
            });

            let mut cache = Self::with_max_size(max_size);

            let iterator = (0..pages.len()).zip(pages.clone()).take(match fetch {
                Fetch::All => number_of_pages,
                Fetch::UntilBufferEnd => cache.max_size,
            });

            iterator.for_each(|(page_number, mem_page)| {
                let id = cache.map(page_number as _);
                cache.buffer[id].page = mem_page
            });

            (cache, pages.collect())
        }
    }

    #[test]
    fn test_reference_page() {
        let (mut cache, pages) = Cache::with_pages(3, 3, Fetch::All);

        pages.into_iter().enumerate().for_each(|(idx, page)| {
            let id = cache.get(idx as _);

            // See [`BufferWithHeader`] `PartialEq` implementation.
            assert_eq!(page, cache.buffer[idx].page);
            assert_eq!(id, Some(idx));
            assert_eq!(REFERENCE_FLAG, cache.buffer[idx].flags);
        })
    }

    #[test]
    fn test_mark_as_dirty() {
        let (mut cache, pages) = Cache::with_pages(3, 3, Fetch::UntilBufferEnd);

        pages.into_iter().enumerate().for_each(|(idx, page)| {
            let id = cache.get_mut(idx as _);

            // See [`BufferWithHeader`] `PartialEq` implementation.
            assert_eq!(page, cache.buffer[idx].page);
            assert_eq!(id, Some(idx));
            assert_eq!(REFERENCE_FLAG | DIRTY_FLAG, cache.buffer[idx].flags);
        })
    }

    #[test]
    fn test_evict_first_unreferenced_page() {
        let (mut cache, pages) = Cache::with_pages(4, 3, Fetch::UntilBufferEnd);

        (0..2).for_each(|idx| {
            cache.get(idx);
        }); // make all pages but the last have `REFERENCE_FLAG`

        let evicted = cache.load(3, pages[3].clone()); // this must evict the 2nd page then replace it with the 3rd

        assert_eq!(evicted, pages[2].clone());
        assert_eq!(cache.clock, cache.max_size - 1);
        assert_eq!(
            cache.buffer[cache.max_size - 1].page,
            pages[pages.len() - 1]
        );
        assert_eq!(cache.pages[&3], cache.max_size - 1);

        for (idx, page) in pages[..cache.max_size - 1].iter().enumerate() {
            assert_eq!(*page, cache.buffer[idx].page);
            assert_eq!(idx, cache.pages[&(idx as _)]);
        }
    }

    #[test]
    fn test_non_eviction_of_pinned_pages() {
        let (mut cache, pages) = Cache::with_pages(4, 3, Fetch::UntilBufferEnd);
        let pinned = cache.pin(0);

        cache.load(3, pages[3].clone()); // must evict but not the first page cause its pinned

        assert!(pinned);
        assert_eq!(PINNED_FLAG, cache.buffer[0].flags);
        assert_eq!(1, cache.clock);

        // checking the pages' integrity
        assert_eq!(pages[0], cache.buffer[0].page);
        assert_eq!(pages[3], cache.buffer[1].page);
        assert_eq!(pages[2], cache.buffer[2].page);
    }

    #[test]
    fn test_eviction_on_pinned_pages_limit() {
        let (mut cache, pages) = Cache::with_pages(10, 10, Fetch::UntilBufferEnd);
        cache.max_pinned_percentage = 30.0;

        (0..=2).for_each(|idx| {
            cache.pin(idx);
        });

        let pinned = cache.pin(3);

        assert!(!pinned);
        assert_eq!(0, cache.buffer[3].flags);
        assert_eq!(3, cache.pinned_pages);
    }
}
