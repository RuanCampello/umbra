//! Thread-local storage abstraction.
//!
//! Provides efficient per-thread storage based on a bucket-based design.
//! This avoids the overhead of the standard library's `thread_local!` macro
//! and gives more control over allocation and layout.

use std::alloc::{self, Layout};
use std::cell::{Cell, UnsafeCell};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::mem::MaybeUninit;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Mutex, OnceLock};

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

/// Manages thread ID allocation with freelist for efficient reuse.
#[derive(Default)]
struct ThreadIdManager {
    free_from: usize,
    free_list: BinaryHeap<Reverse<usize>>,
}

/// Guard to ensure thread ID is released when thread exits.
struct ThreadGuard {
    id: Cell<usize>,
}

const ENTRIES_PER_BUCKET: usize = 64;

pub const BUCKETS: usize = 64;

fn thread_id_manager() -> &'static Mutex<ThreadIdManager> {
    static THREAD_ID_MANAGER: OnceLock<Mutex<ThreadIdManager>> = OnceLock::new();
    THREAD_ID_MANAGER.get_or_init(Default::default)
}

thread_local! {
    static CURRENT_THREAD: Cell<Option<Thread>> = const { Cell::new(None) };
    static THREAD_GUARD: ThreadGuard = const { ThreadGuard { id: Cell::new(0) } };
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

impl Thread {
    #[inline]
    pub fn new(id: usize) -> Self {
        Self {
            id,
            entry: id % ENTRIES_PER_BUCKET,
            bucket: id / ENTRIES_PER_BUCKET,
        }
    }

    /// Returns the current thread, initialising if necessary.
    #[inline]
    pub fn current() -> Self {
        CURRENT_THREAD.with(|thread| {
            if let Some(t) = thread.get() {
                t
            } else {
                Self::init_slow(thread)
            }
        })
    }

    /// Creates a new thread ID.
    pub fn create() -> Self {
        Self::new(thread_id_manager().lock().unwrap().alloc())
    }

    /// Releases a thread ID for reuse.
    pub fn release(id: usize) {
        thread_id_manager().lock().unwrap().free(id);
    }

    #[inline]
    pub fn bucket_capacity(_bucket: usize) -> usize {
        ENTRIES_PER_BUCKET
    }

    #[cold]
    #[inline(never)]
    fn init_slow(thread: &Cell<Option<Thread>>) -> Thread {
        let new = Thread::create();
        thread.set(Some(new));
        THREAD_GUARD.with(|guard| guard.id.set(new.id));
        new
    }
}

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

impl ThreadIdManager {
    fn alloc(&mut self) -> usize {
        if let Some(id) = self.free_list.pop() {
            id.0
        } else {
            let id = self.free_from;
            self.free_from = self
                .free_from
                .checked_add(1)
                .expect("Ran out of thread IDs");
            id
        }
    }

    fn free(&mut self, id: usize) {
        self.free_list.push(Reverse(id));
    }
}

impl Drop for ThreadGuard {
    fn drop(&mut self) {
        let _ = CURRENT_THREAD.try_with(|thread| thread.set(None));
        Thread::release(self.id.get());
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
