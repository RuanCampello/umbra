//! Per-thread reservation state.
//!
//! Each thread maintains a reservation that tracks retired batches attached
//! to it. The reservation becomes active when the thread enters a critical
//! section and is cleared when it exits.

use std::cell::Cell;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use super::node::{Node, INVPTR};


/// Per-thread reservation state.
///
/// Tracks a list of retired batches and the era when last updated.
/// The `head` is [`INVPTR`] when inactive, `null` or a valid list when active.
pub struct Reservation {
    head: AtomicPtr<Node>,
    era: AtomicU64,
    guards: Cell<u64>,
}


impl Reservation {
    #[inline]
    pub const fn new() -> Self {
        Self {
            head: AtomicPtr::new(INVPTR),
            era: AtomicU64::new(0),
            guards: Cell::new(0),
        }
    }

    #[inline]
    pub fn is_inactive(&self) -> bool {
        self.head.load(Ordering::Acquire) == INVPTR
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire).is_null()
    }

    #[inline]
    pub fn activate(&self) {
        self.head.store(std::ptr::null_mut(), Ordering::Release);
    }

    #[inline]
    pub fn deactivate(&self) {
        self.head.store(INVPTR, Ordering::Release);
    }

    #[inline]
    pub fn head(&self) -> *mut Node {
        self.head.load(Ordering::Acquire)
    }

    #[inline]
    pub fn swap_head(&self, new: *mut Node) -> *mut Node {
        self.head.swap(new, Ordering::AcqRel)
    }

    #[inline]
    pub fn compare_exchange_head(
        &self,
        current: *mut Node,
        new: *mut Node,
    ) -> Result<*mut Node, *mut Node> {
        self.head
            .compare_exchange(current, new, Ordering::AcqRel, Ordering::Acquire)
    }

    #[inline]
    pub fn era(&self) -> u64 {
        self.era.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_era(&self, era: u64) {
        self.era.store(era, Ordering::Release);
    }

    #[inline]
    pub fn guard_count(&self) -> u64 {
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
        assert!(!r.is_empty());
        assert_eq!(r.era(), 0);
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

    #[test]
    fn test_era() {
        let r = Reservation::new();

        r.set_era(42);
        assert_eq!(r.era(), 42);

        r.set_era(100);
        assert_eq!(r.era(), 100);
    }
}
