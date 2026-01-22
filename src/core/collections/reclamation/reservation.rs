//! Per-thread reservation state.
//!
//! Each thread maintains a reservation that tracks retired batches attached
//! to it. The reservation becomes active when the thread enters a critical
//! section and is cleared when it exits.

use std::cell::Cell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};

use super::batch::Entry;

/// Sentinel value indicating an inactive reservation.
pub const INACTIVE: *mut Entry = usize::MAX as *mut Entry;

/// Per-thread reservation state.
///
/// Tracks a list of debt nodes (Entries) from retired batches.
pub struct Reservation {
    pub head: AtomicPtr<Entry>,
    guards: Cell<usize>,
}

impl Reservation {
    #[inline]
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(INACTIVE),
            guards: Cell::new(0),
        }
    }

    #[inline]
    pub fn is_inactive(&self) -> bool {
        self.head.load(Ordering::Acquire) == INACTIVE
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        head.is_null() || head == INACTIVE
    }

    #[inline]
    pub fn activate(&self) {
        self.head.store(ptr::null_mut(), Ordering::SeqCst);
    }

    #[inline]
    pub fn deactivate(&self) {
        self.head.store(INACTIVE, Ordering::SeqCst);
    }

    #[inline]
    pub fn guard_count(&self) -> usize {
        self.guards.get()
    }

    #[inline]
    pub fn enter(&self) {
        self.guards.set(self.guards.get() + 1);
    }

    #[inline]
    pub fn exit(&self) {
        debug_assert!(self.guards.get() > 0, "guard underflow");
        self.guards.set(self.guards.get() - 1);
    }
}

impl Default for Reservation {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for Reservation {}
unsafe impl Sync for Reservation {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_reservation() {
        let r = Reservation::new();
        assert!(r.is_inactive());
        assert_eq!(r.guard_count(), 0);
    }

    #[test]
    fn test_activation() {
        let r = Reservation::new();

        r.activate();
        assert!(!r.is_inactive());
        assert!(r.is_empty());

        r.deactivate();
        assert!(r.is_inactive());
    }

    #[test]
    fn test_guard_counting() {
        let r = Reservation::new();

        r.enter();
        assert_eq!(r.guard_count(), 1);

        r.enter();
        assert_eq!(r.guard_count(), 2);

        r.exit();
        assert_eq!(r.guard_count(), 1);

        r.exit();
        assert_eq!(r.guard_count(), 0);
    }
}
