//! Crystalline-L Memory Reclamation
//!
//! A lock-free, snapshot-free memory reclamation scheme based on
//! type-erased batched reclamation.
//!
//! # References
//!
//! Nikolaev & Ravindran, "Wait-Free Memory Reclamation", arXiv:2108.02763

mod batch;
mod collector;
mod node;
mod reservation;
mod thread;

pub use collector::{reclaim_boxed, Collector, Guard};
pub use node::Node;
pub use thread::Thread;

/// Era increment frequency — increment global era every N allocations.
pub(crate) const ALLOC_FREQ: usize = 110;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicPtr;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_retire() {
        let collector = Collector::new();

        let guard = collector.pin();

        for i in 0..10u64 {
            let ptr = Box::into_raw(Box::new(i));
            unsafe { guard.retire(ptr, reclaim_boxed) };
        }
    }

    #[test]
    fn protect() {
        let collector = Collector::new();

        let node = Box::into_raw(Box::new(Node::new(1)));
        let atomic = AtomicPtr::new(node);

        let guard = collector.pin();
        let protected = guard.protect(&atomic);

        assert_eq!(protected, node);

        unsafe { drop(Box::from_raw(node)) };
    }

    #[test]
    fn concurrent_access() {
        let collector = Arc::new(Collector::new());
        let node = Box::into_raw(Box::new(Node::new(1)));
        let atomic = Arc::new(AtomicPtr::new(node));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let c = Arc::clone(&collector);
                let a = Arc::clone(&atomic);
                thread::spawn(move || {
                    for _ in 0..1000 {
                        let guard = c.pin();
                        let _ptr = guard.protect(&a);
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        unsafe { drop(Box::from_raw(node)) };
    }

    #[test]
    fn flush_and_refresh() {
        let collector = Collector::new();

        let mut guard = collector.pin();

        for i in 0..10u64 {
            let ptr = Box::into_raw(Box::new(i));
            unsafe { guard.retire(ptr, reclaim_boxed) };
        }

        guard.flush();
        guard.refresh();
    }
}
