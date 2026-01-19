//! Thread-local storage abstraction.
//!
//! Provides efficient per-thread storage based on a bucket-based design.
//! This avoids the overhead of the standard library's `thread_local!` macro
//! and gives more control over allocation and layout.

use std::alloc::{self, Layout};
use std::cell::{Cell, UnsafeCell};
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering};

/// Thread-local storage container.
///
/// Provides efficient per-thread storage without requiring the standard
/// library's `thread_local!` macro. Storage is organised into buckets,
/// where each bucket contains a fixed number of entries.
pub struct ThreadLocal<T> {
    buckets: [AtomicPtr<Entry<T>>; BUCKETS],
}

/// Thread identifier for accessing thread-local state.
///
/// Each thread has a unique `Thread` handle used to access its thread-local state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Thread {
    pub id: usize,
    pub entry: usize,
    pub bucket: usize,
}

/// An entry in the thread-local storage.
#[repr(C)]
struct Entry<T> {
    present: AtomicBool,
    value: UnsafeCell<MaybeUninit<T>>,
}

/// An iterator over a [`ThreadLocal`].
pub struct Iter<'a, T> {
    bucket: usize,
    index: usize,
    bucket_size: usize,
    thread_local: &'a ThreadLocal<T>,
}

const ENTRIES_PER_BUCKET: usize = 64;

pub const BUCKETS: usize = 64;

// Global thread ID counter
static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static CURRENT_THREAD: Cell<Option<Thread>> = const { Cell::new(None) };
}

impl Thread {
    /// Returns the current thread, initialising if necessary.
    #[inline]
    pub fn current() -> Self {
        CURRENT_THREAD.with(|thread| {
            if let Some(t) = thread.get() {
                t
            } else {
                let t = Self::init();
                thread.set(Some(t));
                t
            }
        })
    }

    fn init() -> Self {
        let id = THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self::new(id)
    }

    /// Creates a new owned thread ID (not tied to TLS).
    /// Used for `OwnedGuard` which owns its thread slot.
    pub fn create_owned() -> Self {
        let id = THREAD_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
        Self::new(id)
    }

    #[inline]
    pub fn new(id: usize) -> Self {
        Self {
            id,
            entry: id % ENTRIES_PER_BUCKET,
            bucket: id / ENTRIES_PER_BUCKET,
        }
    }

    #[inline]
    pub fn bucket_capacity(_bucket: usize) -> usize {
        ENTRIES_PER_BUCKET
    }
}

impl<T> ThreadLocal<T> {
    pub const fn new() -> Self {
        const NULL: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());
        Self {
            buckets: unsafe { std::mem::transmute_copy(&[NULL; BUCKETS]) },
        }
    }

    pub fn get_or<F>(&self, thread: Thread, init: F) -> &T
    where
        F: FnOnce() -> T,
    {
        let bucket = self.get_or_alloc_bucket(thread.bucket);
        let entry = unsafe { &*bucket.add(thread.entry) };

        if entry.present.load(Ordering::Acquire) {
            return unsafe { (*entry.value.get()).assume_init_ref() };
        }

        unsafe { (*entry.value.get()).write(init()) };
        entry.present.store(true, Ordering::Release);

        unsafe { (*entry.value.get()).assume_init_ref() }
    }

    pub fn get(&self, thread: Thread) -> Option<&T> {
        let bucket_ptr = self.buckets[thread.bucket].load(Ordering::Acquire);
        if bucket_ptr.is_null() {
            return None;
        }

        let entry = unsafe { &*bucket_ptr.add(thread.entry) };
        match entry.present.load(Ordering::Acquire) {
            true => Some(unsafe { (*entry.value.get()).assume_init_ref() }),
            _ => None,
        }
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter {
            bucket: 0,
            index: 0,
            bucket_size: ENTRIES_PER_BUCKET,
            thread_local: self,
        }
    }

    fn get_or_alloc_bucket(&self, bucket_idx: usize) -> *mut Entry<T> {
        let bucket_ptr = self.buckets[bucket_idx].load(Ordering::Acquire);
        if !bucket_ptr.is_null() {
            return bucket_ptr;
        }

        let layout = Layout::array::<Entry<T>>(ENTRIES_PER_BUCKET).unwrap();
        let new_bucket = unsafe { alloc::alloc_zeroed(layout) as *mut Entry<T> };

        if new_bucket.is_null() {
            alloc::handle_alloc_error(layout);
        }

        for i in 0..ENTRIES_PER_BUCKET {
            unsafe { new_bucket.add(i).write(Entry::new()) };
        }

        match self.buckets[bucket_idx].compare_exchange(
            ptr::null_mut(),
            new_bucket,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => new_bucket,
            Err(existing) => {
                unsafe { alloc::dealloc(new_bucket as *mut u8, layout) };
                existing
            }
        }
    }
}

impl<T> Default for ThreadLocal<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        for (i, bucket) in self.buckets.iter_mut().enumerate() {
            let bucket_ptr = *bucket.get_mut();

            if bucket_ptr.is_null() {
                continue;
            }

            let bucket_size = Thread::bucket_capacity(i);

            let _ =
                unsafe { Box::from_raw(ptr::slice_from_raw_parts_mut(bucket_ptr, bucket_size)) };
        }
    }
}

unsafe impl<T: Send> Send for ThreadLocal<T> {}
unsafe impl<T: Send + Sync> Sync for ThreadLocal<T> {}

impl<T> Entry<T> {
    const fn new() -> Self {
        Self {
            present: AtomicBool::new(false),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

impl<T> Drop for Entry<T> {
    fn drop(&mut self) {
        if *self.present.get_mut() {
            unsafe { ptr::drop_in_place((*self.value.get()).as_mut_ptr()) };
        }
    }
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        while self.bucket < BUCKETS {
            let bucket_ptr = self.thread_local.buckets[self.bucket].load(Ordering::Acquire);

            if bucket_ptr.is_null() {
                self.bucket += 1;
                self.index = 0;
                continue;
            }

            while self.index < self.bucket_size {
                let entry = unsafe { &*bucket_ptr.add(self.index) };
                self.index += 1;

                if entry.present.load(Ordering::Acquire) {
                    return Some(unsafe { (*entry.value.get()).assume_init_ref() });
                }
            }

            self.bucket += 1;
            self.index = 0;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_current() {
        let t1 = Thread::current();
        let t2 = Thread::current();
        assert_eq!(t1, t2);
    }

    #[test]
    fn test_thread_new() {
        let t0 = Thread::new(0);
        assert_eq!(t0.id, 0);
        assert_eq!(t0.bucket, 0);
        assert_eq!(t0.entry, 0);

        let t64 = Thread::new(64);
        assert_eq!(t64.id, 64);
        assert_eq!(t64.bucket, 1);
        assert_eq!(t64.entry, 0);

        let t65 = Thread::new(65);
        assert_eq!(t65.id, 65);
        assert_eq!(t65.bucket, 1);
        assert_eq!(t65.entry, 1);
    }

    #[test]
    fn test_get_or() {
        let tl: ThreadLocal<u64> = ThreadLocal::new();
        let t0 = Thread::new(0);
        let t1 = Thread::new(1);

        let v0 = tl.get_or(t0, || 42);
        assert_eq!(*v0, 42);

        let v0_again = tl.get_or(t0, || 999);
        assert_eq!(*v0_again, 42);

        let v1 = tl.get_or(t1, || 100);
        assert_eq!(*v1, 100);
    }

    #[test]
    fn test_get() {
        let tl: ThreadLocal<u64> = ThreadLocal::new();
        let t0 = Thread::new(0);

        assert!(tl.get(t0).is_none());

        tl.get_or(t0, || 42);
        assert_eq!(tl.get(t0), Some(&42));
    }

    #[test]
    fn test_iter() {
        let tl: ThreadLocal<u64> = ThreadLocal::new();

        tl.get_or(Thread::new(0), || 1);
        tl.get_or(Thread::new(1), || 2);
        tl.get_or(Thread::new(64), || 3);

        let values: Vec<_> = tl.iter().copied().collect();
        assert_eq!(values.len(), 3);
        assert!(values.contains(&1));
        assert!(values.contains(&2));
        assert!(values.contains(&3));
    }
}
