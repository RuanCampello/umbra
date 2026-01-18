//! Thread-local batch accumulator.
//!
//! Retired objects are grouped into batches before reclamation. Each entry
//! stores a pointer and a reclaim function, allowing heterogeneous types.

use std::alloc::{self, Layout};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

/// Maximum entries per batch.
pub const BATCH_SIZE: usize = 64; // Default, can be larger dynamically if we changed allocator

/// A batch of retired objects awaiting reclamation.
#[repr(C)]
pub struct Batch {
    /// Entries in this batch.
    pub entries: [Entry; BATCH_SIZE],
    /// Number of entries in use.
    pub len: usize,
    /// Reference count (number of active guards referencing this batch).
    pub refcount: AtomicUsize,
}

/// An entry in a batch.
#[repr(C)]
pub struct Entry {
    /// Pointer to the retired object.
    pub ptr: *mut u8,
    /// Function to reclaim the object.
    pub reclaim: Option<unsafe fn(*mut u8, *const ())>,
    /// Pointer to the next entry in the debt list.
    pub next: *mut Entry,
    /// Pointer to the reservation head this entry is linked to.
    pub head: *const AtomicPtr<Entry>,
    /// Pointer to the batch this entry belongs to.
    pub batch: *mut Batch,
}

/// Thread-local batch state.
pub struct LocalBatch {
    /// Current batch being filled, or null if none.
    pub batch: *mut Batch,
}

/// Sentinel value indicating batch is being dropped.
pub const DROP: *mut Batch = usize::MAX as *mut Batch;

impl Batch {
    pub fn alloc() -> *mut Self {
        let layout = Layout::new::<Self>();
        let ptr = unsafe { alloc::alloc_zeroed(layout) as *mut Self };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }
        ptr
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.len >= BATCH_SIZE
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub unsafe fn push(&mut self, ptr: *mut u8, reclaim: unsafe fn(*mut u8, *const ())) {
        debug_assert!(!self.is_full());
        self.entries[self.len] = Entry {
            ptr,
            reclaim: Some(reclaim),
            next: ptr::null_mut(),
            head: ptr::null(),
            batch: self as *mut Batch,
        };
        self.len += 1;
    }
}

impl LocalBatch {
    pub const fn new() -> Self {
        Self {
            batch: ptr::null_mut(),
        }
    }

    #[inline]
    pub fn is_active(&self) -> bool {
        !self.batch.is_null() && self.batch != DROP
    }

    pub unsafe fn free(batch: *mut Batch) {
        if batch.is_null() || batch == DROP {
            return;
        }
        let layout = Layout::new::<Batch>();
        alloc::dealloc(batch as *mut u8, layout);
    }
}

impl Default for LocalBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl Entry {
    pub const fn empty() -> Self {
        Self {
            ptr: ptr::null_mut(),
            reclaim: None,
            next: ptr::null_mut(),
            head: ptr::null(),
            batch: ptr::null_mut(),
        }
    }
}

impl Default for Entry {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn empty_reclaim(_ptr: *mut u8, _collector: *const ()) {}

    #[test]
    fn test_batch_alloc_free() {
        let batch = Batch::alloc();
        assert!(!batch.is_null());
        unsafe {
            assert!((*batch).is_empty());
            assert!(!(*batch).is_full());
            LocalBatch::free(batch);
        }
    }

    #[test]
    fn test_batch_push() {
        let batch = Batch::alloc();
        let mut data = 42u64;

        unsafe {
            (*batch).push(&mut data as *mut _ as *mut u8, empty_reclaim);
            assert_eq!((*batch).len(), 1);
            LocalBatch::free(batch);
        }
    }
}
