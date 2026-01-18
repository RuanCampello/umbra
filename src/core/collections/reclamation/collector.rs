//! Crystalline-L memory reclamation collector.
//!
//! The collector is the main entry point for memory reclamation. It manages
//! thread-local batches and reservation state.

use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use super::batch::{Batch, LocalBatch, DROP};
use super::node::{is_invptr, Node, INVPTR};
use super::reservation::Reservation;
use super::thread::{Thread, ThreadLocal};

/// Crystalline-L memory reclamation collector.
pub struct Collector {
    batches: ThreadLocal<UnsafeCell<LocalBatch>>,
    reservations: ThreadLocal<Reservation>,
    pub(crate) id: usize,
    pub(crate) batch_size: usize,
}

/// RAII guard for a pinned critical section.
pub struct Guard<'a> {
    collector: &'a Collector,
    thread: Thread,
}

impl Collector {
    pub fn new() -> Self {
        static COLLECTOR_ID: AtomicUsize = AtomicUsize::new(0);

        Self {
            batches: ThreadLocal::new(),
            reservations: ThreadLocal::new(),
            id: COLLECTOR_ID.fetch_add(1, Ordering::Relaxed),
            batch_size: 64,
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[inline]
    pub fn id(&self) -> usize {
        self.id
    }

    /// Pins the current thread, returning a guard for the critical section.
    pub fn pin(&self) -> Guard<'_> {
        let thread = Thread::current();
        let reservation = self.reservations.get_or(thread, Reservation::new);

        if reservation.guard_count() == 0 {
            reservation.activate();
        }
        reservation.enter();

        Guard {
            collector: self,
            thread,
        }
    }

    /// Retires a pointer with a reclaim function.
    ///
    /// # Safety
    ///
    /// - The pointer must no longer be accessible from shared data structures
    /// - The reclaim function must correctly handle the pointer type
    /// - The pointer must not be retired more than once
    pub unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        let thread = Thread::current();
        let local = self
            .batches
            .get_or(thread, || UnsafeCell::new(LocalBatch::new()));
        let local_batch = &mut *local.get();

        if local_batch.batch.is_null() {
            local_batch.batch = Batch::alloc();
        }

        let batch = &mut *local_batch.batch;

        // Type-erase for storage
        let reclaim_erased: unsafe fn(*mut u8, *const ()) = std::mem::transmute(reclaim);
        batch.push(ptr as *mut u8, reclaim_erased);

        if batch.is_full() {
            self.try_retire(local_batch);
        }
    }

    /// Flushes the current thread's batch, attempting reclamation.
    pub unsafe fn flush(&self) {
        let thread = Thread::current();
        let local = match self.batches.get(thread) {
            Some(l) => l,
            None => return,
        };

        let local_batch = &mut *local.get();
        self.try_retire(local_batch);
    }

    unsafe fn try_retire(&self, local: &mut LocalBatch) {
        let batch = local.batch;
        if batch.is_null() || batch == DROP {
            return;
        }

        self.free_batch(batch);
        local.batch = ptr::null_mut();
    }

    unsafe fn free_batch(&self, batch: *mut Batch) {
        if batch.is_null() || batch == DROP {
            return;
        }

        let batch_ref = &mut *batch;
        for i in 0..batch_ref.len {
            let entry = &batch_ref.entries[i];
            let reclaim: unsafe fn(*mut u8, &Collector) = std::mem::transmute(entry.reclaim);
            reclaim(entry.ptr, self);
        }

        LocalBatch::free(batch);
    }

    /// Reclaims all pending retired objects.
    ///
    /// # Safety
    ///
    /// Must only be called when no threads are actively using the collector.
    pub unsafe fn reclaim_all(&self) {
        for local in self.batches.iter() {
            let local_batch = &mut *local.get();
            let batch = local_batch.batch;

            if batch.is_null() || batch == DROP {
                continue;
            }

            local_batch.batch = DROP;
            self.free_batch(batch);
            local_batch.batch = ptr::null_mut();
        }
    }

    unsafe fn clear(&self, thread: Thread) {
        let reservation = match self.reservations.get(thread) {
            Some(r) => r,
            None => return,
        };

        let list = reservation.swap_head(INVPTR);

        if !is_invptr(list) && !list.is_null() {
            self.traverse(list);
        }
    }

    unsafe fn traverse(&self, mut next: *mut Node) {
        while !next.is_null() && !is_invptr(next) {
            let curr = next;
            let curr_node = &*curr;
            next = curr_node.next();
        }
    }
}

impl Default for Collector {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for Collector {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Collector {}

impl Drop for Collector {
    fn drop(&mut self) {
        unsafe { self.reclaim_all() };
    }
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl<'a> Guard<'a> {
    #[inline]
    pub fn thread(&self) -> Thread {
        self.thread
    }

    #[inline]
    pub fn collector(&self) -> &'a Collector {
        self.collector
    }

    pub fn protect<T>(&self, ptr: &AtomicPtr<T>) -> *mut T {
        ptr.load(Ordering::Acquire)
    }

    pub fn flush(&mut self) {
        unsafe { self.collector.flush() };
    }

    pub fn refresh(&mut self) {}

    /// Retires a pointer with a reclaim function.
    pub unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        self.collector.retire(ptr, reclaim);
    }
}

impl Drop for Guard<'_> {
    fn drop(&mut self) {
        let reservation = match self.collector.reservations.get(self.thread) {
            Some(r) => r,
            None => return,
        };

        reservation.exit();

        if reservation.guard_count() == 0 {
            unsafe { self.collector.clear(self.thread) };
        }
    }
}

/// Default reclaim function that drops a Box.
pub unsafe fn reclaim_boxed<T>(ptr: *mut T, _collector: &Collector) {
    drop(Box::from_raw(ptr));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collector_new() {
        let c = Collector::new();
        assert_eq!(c.batch_size, 64);
    }

    #[test]
    fn test_pin_unpin() {
        let c = Collector::new();
        let _guard = c.pin();
    }

    #[test]
    fn test_retire_and_reclaim() {
        let c = Collector::new();
        let mut guard = c.pin();

        for i in 0..10u64 {
            let ptr = Box::into_raw(Box::new(i));
            unsafe { guard.retire(ptr, reclaim_boxed) };
        }

        guard.flush();
    }

    #[test]
    fn test_protect() {
        let c = Collector::new();
        let data = Box::into_raw(Box::new(42u64));
        let atomic = AtomicPtr::new(data);

        let guard = c.pin();
        let protected = guard.protect(&atomic);
        assert_eq!(protected, data);

        unsafe { drop(Box::from_raw(data)) };
    }
}
