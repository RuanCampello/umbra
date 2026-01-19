//! Crystalline-L memory reclamation collector.
//!
//! The collector is the main entry point for memory reclamation. It manages
//! global reclamation state using a debt-based strategy (active reservations).

use super::batch::{Batch, Entry, LocalBatch, DROP};
use super::reservation::{Reservation, INACTIVE};
use super::thread::{Thread, ThreadLocal};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{self, AtomicUsize, Ordering};

/// Crystalline-L memory reclamation collector.
pub struct Collector {
    batches: ThreadLocal<UnsafeCell<LocalBatch>>,
    reservations: ThreadLocal<Reservation>,
    pub(crate) id: usize,
    pub(crate) batch_size: usize,
}

/// A guard that protects objects for its lifetime.
///
/// This trait provides common functionality implemented by [`LocalGuard`] and
/// [`OwnedGuard`].
pub trait Guard {
    /// Returns the collector this guard was created from.
    fn collector(&self) -> &Collector;

    /// Returns the thread handle for this guard.
    fn thread(&self) -> Thread;

    /// Protects the load of an atomic pointer.
    fn protect<T>(&self, ptr: &atomic::AtomicPtr<T>) -> *mut T {
        ptr.load(Ordering::Acquire)
    }

    /// Retires a pointer with a reclaim function.
    ///
    /// # Safety
    ///
    /// - The pointer must no longer be accessible from shared data structures
    /// - The reclaim function must correctly handle the pointer type
    unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        self.collector()
            .retire_internal(self.thread(), ptr, reclaim);
    }

    /// Flushes the current thread's batch, attempting reclamation.
    fn flush(&mut self);

    /// Refreshes the guard, potentially allowing more reclamation.
    fn refresh(&mut self);
}

/// A guard that keeps the current thread marked as active.
///
/// Local guards are created by calling [`Collector::pin`]. Unlike
/// [`OwnedGuard`], a local guard is tied to the current thread and does not
/// implement `Send`. This makes local guards relatively cheap to create and
/// destroy.
pub struct LocalGuard<'a> {
    collector: &'a Collector,
    thread: Thread,
    reservation: *const Reservation,
    _unsend: PhantomData<*mut ()>,
}

/// A guard that protects objects for its lifetime, independent of the current
/// thread.
///
/// Unlike [`LocalGuard`], an owned guard is independent of the current thread,
/// allowing it to implement `Send` and `Sync`. This is useful for holding
/// guards across `.await` points in work-stealing schedulers.
pub struct OwnedGuard<'a> {
    collector: &'a Collector,
    thread: Thread,
    reservation: *const Reservation,
}

// OwnedGuard owns its thread slot and is not tied to any thread-locals.
unsafe impl Send for OwnedGuard<'_> {}
unsafe impl Sync for OwnedGuard<'_> {}

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

    /// Pins the current thread, returning a local guard.
    pub fn pin(&self) -> LocalGuard<'_> {
        let thread = Thread::current();
        let reservation = self.reservations.get_or(thread, Reservation::new);

        if reservation.guard_count() == 0 {
            reservation.activate();
            atomic::fence(Ordering::SeqCst);
        }
        reservation.enter();

        LocalGuard {
            collector: self,
            thread,
            reservation,
            _unsend: PhantomData,
        }
    }

    /// Pins with an owned guard (independent thread ID).
    pub fn pin_owned(&self) -> OwnedGuard<'_> {
        let thread = Thread::create_owned();
        let reservation = self.reservations.get_or(thread, Reservation::new);

        reservation.activate();
        atomic::fence(Ordering::SeqCst);
        reservation.enter();

        OwnedGuard {
            collector: self,
            thread,
            reservation,
        }
    }

    /// Retires a pointer with a reclaim function (uses current thread).
    pub unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &Collector)) {
        self.retire_internal(Thread::current(), ptr, reclaim);
    }

    /// Internal retire with explicit thread.
    unsafe fn retire_internal<T>(
        &self,
        thread: Thread,
        ptr: *mut T,
        reclaim: unsafe fn(*mut T, &Collector),
    ) {
        let local = self
            .batches
            .get_or(thread, || UnsafeCell::new(LocalBatch::new()));
        let local_batch = &mut *local.get();

        if local_batch.batch.is_null() || local_batch.batch == DROP {
            local_batch.batch = Batch::alloc();
        }

        let batch = &mut *local_batch.batch;

        let reclaim_erased: unsafe fn(*mut u8, *const ()) = std::mem::transmute(reclaim);
        batch.push(ptr as *mut u8, reclaim_erased);

        if batch.len() >= self.batch_size || batch.is_full() {
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

    unsafe fn try_retire(&self, local_batch: &mut LocalBatch) {
        atomic::fence(Ordering::SeqCst);

        let batch = local_batch.batch;
        if batch.is_null() || batch == DROP {
            return;
        }

        let batch_ref = &mut *batch;
        let mut active_count = 0;
        let mut used_entries = 0;

        for reservation in self.reservations.iter() {
            if reservation.is_inactive() {
                continue;
            }

            // ensure we have enough entries in the batch to serve as debt nodes
            if used_entries >= batch_ref.len {
                // batch is full effectively.
                // for now, we return and delay retirement.
                return;
            }

            let entry = &mut batch_ref.entries[used_entries];
            entry.head = &reservation.head;

            used_entries += 1;
            active_count += 1;
        }

        if active_count == 0 {
            local_batch.batch = Batch::alloc();
            self.free_batch(batch);
            return;
        }

        batch_ref.refcount.store(active_count, Ordering::Relaxed);
        local_batch.batch = Batch::alloc();
        atomic::fence(Ordering::Acquire);

        let mut decrements = 0;

        for i in 0..used_entries {
            let entry = &mut batch_ref.entries[i];
            let head_ptr = entry.head;
            let head = &*head_ptr;

            let mut prev = head.load(Ordering::Relaxed);

            loop {
                if prev == INACTIVE {
                    decrements += 1;
                    break;
                }

                entry.next = prev;
                match head.compare_exchange_weak(prev, entry, Ordering::Release, Ordering::Relaxed)
                {
                    Ok(_) => break,
                    Err(found) => prev = found,
                }
            }
        }

        if decrements > 0 {
            let old = batch_ref.refcount.fetch_sub(decrements, Ordering::AcqRel);
            if old == decrements {
                self.free_batch(batch);
            }
        }
    }

    unsafe fn free_batch(&self, batch: *mut Batch) {
        if batch.is_null() || batch == DROP {
            return;
        }

        let batch_ref = &mut *batch;

        for i in 0..batch_ref.len {
            let entry = &batch_ref.entries[i];
            if let Some(reclaim) = entry.reclaim {
                let reclaim_typed: unsafe fn(*mut u8, &Collector) = std::mem::transmute(reclaim);
                reclaim_typed(entry.ptr, self);
            }
        }

        LocalBatch::free(batch);
    }

    unsafe fn clear(&self, thread: Thread) {
        let reservation = match self.reservations.get(thread) {
            Some(r) => r,
            None => return,
        };

        let mut node = reservation.head.swap(INACTIVE, Ordering::SeqCst);

        while !node.is_null() && node != INACTIVE {
            let entry = &mut *node;
            let next = entry.next;
            let batch = entry.batch;

            debug_assert!(!batch.is_null());

            let batch_ref = &*batch;
            let old_rc = batch_ref.refcount.fetch_sub(1, Ordering::AcqRel);
            if old_rc == 1 {
                self.free_batch(batch);
            }

            node = next;
        }
    }

    pub unsafe fn reclaim_all(&self) {
        for local in self.batches.iter() {
            let local_batch = &mut *local.get();

            loop {
                let batch = local_batch.batch;
                if batch.is_null() || batch == DROP {
                    break;
                }

                local_batch.batch = ptr::null_mut();
                self.free_batch(batch);
            }
        }

        for reservation in self.reservations.iter() {
            if !reservation.is_inactive() {
                let mut node = reservation.head.swap(INACTIVE, Ordering::SeqCst);
                while !node.is_null() && node != INACTIVE {
                    let entry = &mut *node;
                    let next = entry.next;
                    let batch = entry.batch;
                    let batch_ref = &*batch;
                    let old_rc = batch_ref.refcount.fetch_sub(1, Ordering::AcqRel);
                    if old_rc == 1 {
                        self.free_batch(batch);
                    }
                    node = next;
                }
            }
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

impl std::fmt::Debug for Collector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Collector")
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl Drop for Collector {
    fn drop(&mut self) {
        unsafe { self.reclaim_all() };
    }
}

unsafe impl Send for Collector {}
unsafe impl Sync for Collector {}

impl Guard for LocalGuard<'_> {
    #[inline]
    fn collector(&self) -> &Collector {
        self.collector
    }

    #[inline]
    fn thread(&self) -> Thread {
        self.thread
    }

    fn flush(&mut self) {
        unsafe { self.collector.flush() };
    }

    fn refresh(&mut self) {
        let reservation = unsafe { &*self.reservation };

        if reservation.guard_count() == 1 {
            unsafe { self.collector.clear(self.thread) };
            reservation.activate();
            atomic::fence(Ordering::SeqCst);
        }
    }
}

impl Drop for LocalGuard<'_> {
    fn drop(&mut self) {
        let reservation = unsafe { &*self.reservation };

        reservation.exit();

        if reservation.guard_count() == 0 {
            unsafe { self.collector.clear(self.thread) };
        }
    }
}

impl Guard for OwnedGuard<'_> {
    #[inline]
    fn collector(&self) -> &Collector {
        self.collector
    }

    #[inline]
    fn thread(&self) -> Thread {
        self.thread
    }

    fn flush(&mut self) {
        unsafe { self.collector.flush() };
    }

    fn refresh(&mut self) {
        let reservation = unsafe { &*self.reservation };
        unsafe { self.collector.clear(self.thread) };
        reservation.activate();
        atomic::fence(Ordering::SeqCst);
    }
}

impl Drop for OwnedGuard<'_> {
    fn drop(&mut self) {
        let reservation = unsafe { &*self.reservation };

        reservation.exit();
        unsafe { self.collector.clear(self.thread) };
    }
}

/// Default reclaim function that drops a Box.
pub unsafe fn reclaim_boxed<T>(ptr: *mut T, _collector: &Collector) {
    drop(Box::from_raw(ptr));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicPtr;

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
    fn equality() {
        let a = Collector::new();
        let b = Collector::new();

        assert_eq!(a, a);
        assert_eq!(b, b);
        assert_ne!(a, b);

        assert_eq!(*a.pin().collector(), a);
        assert_ne!(*a.pin().collector(), b);

        assert_eq!(*b.pin().collector(), b);
        assert_ne!(*b.pin().collector(), a);
    }
}
