//! Crystalline-L Memory Reclamation
//!
//! A lock-free, snapshot-free memory reclamation scheme based on
//! type-erased batched reclamation.
//!
//! # References
//!
//! Nikolaev & Ravindran, "Wait-Free Memory Reclamation", arXiv:2108.02763

#![allow(unused)]

mod barrier;
mod batch;
mod collector;
mod node;
mod reservation;
mod thread;

pub use collector::{reclaim_boxed, Collector, Guard, LocalGuard, OwnedGuard};
pub use node::Node;
pub use thread::Thread;

struct DropTracker(std::sync::Arc<std::sync::atomic::AtomicUsize>);

struct UnsafeSend<T>(T);
unsafe impl<T> Send for UnsafeSend<T> {}

impl Drop for DropTracker {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Era increment frequency — increment global era every N allocations.
pub(crate) const ALLOC_FREQ: usize = 110;

fn boxed<T>(value: T) -> *mut T {
    Box::into_raw(Box::new(value))
}

#[cfg(test)]
mod tests {
    use super::Node as Entry;
    use super::*;
    use std::mem::ManuallyDrop;
    use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::thread;

    #[cfg(not(miri))]
    mod var {
        pub const ITER: usize = 50;
        pub const ITEMS: usize = 10_000;
        pub const THREADS: usize = 32;
    }

    #[cfg(miri)]
    mod var {
        pub const ITER: usize = 4;
        pub const ITEMS: usize = 100;
        pub const THREADS: usize = 4;
    }

    #[test]
    fn basic_retire() {
        let collector = Collector::new();

        let guard = collector.pin();

        for i in 0..10u64 {
            let ptr = boxed(i);
            unsafe { guard.retire(ptr, reclaim_boxed) };
        }
    }

    #[test]
    fn recursive_retire() {
        struct Recursive {
            _v: usize,
            ptrs: Vec<*mut usize>,
        }

        let collector = Collector::new().batch_size(1);
        let ptr = boxed(Recursive {
            _v: 0,
            ptrs: (0..var::ITEMS).map(boxed).collect(),
        });

        unsafe {
            collector.retire(ptr, |ptr, collector| {
                let v = Box::from_raw(ptr);
                for ptr in v.ptrs {
                    collector.retire(ptr, reclaim_boxed);
                    let mut guard = collector.pin();

                    guard.flush();
                    guard.refresh();

                    drop(guard)
                }
            });

            collector.pin().flush();
        }
    }

    #[test]
    fn protect() {
        let collector = Collector::new();

        let node = boxed(Entry::new(1));
        let atomic = AtomicPtr::new(node);

        let guard = collector.pin();
        let protected = guard.protect(&atomic);

        assert_eq!(protected, node);

        unsafe { drop(Box::from_raw(node)) };
    }

    #[test]
    fn concurrent_access() {
        let collector = Arc::new(Collector::new());
        let node = boxed(Entry::new(1));
        let atomic = Arc::new(AtomicPtr::new(node));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let c = Arc::clone(&collector);
                let a = Arc::clone(&atomic);
                thread::spawn(move || {
                    for _ in 0..var::ITEMS {
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
            let ptr = boxed(i);
            unsafe { guard.retire(ptr, reclaim_boxed) };
        }

        guard.flush();
        guard.refresh();
    }

    #[test]
    fn reclaim_all() {
        let collector = Collector::new().batch_size(2);

        for _ in 0..var::ITER {
            let drop = Arc::new(AtomicUsize::new(0));

            let items: Vec<_> = (0..var::ITEMS)
                .map(|_| AtomicPtr::new(boxed(DropTracker(drop.clone()))))
                .collect();

            items.iter().for_each(|item| unsafe {
                collector.retire(item.load(Ordering::Relaxed), reclaim_boxed);
            });

            unsafe {
                collector.reclaim_all();
            }

            assert_eq!(drop.load(Ordering::Relaxed), var::ITEMS);
        }
    }

    #[test]
    fn recursive_retire_reclaim_all() {
        struct Recursive {
            _v: usize,
            pointers: Vec<*mut DropTracker>,
        }

        unsafe {
            let collector = Collector::new().batch_size(var::ITEMS * 2);
            let dropped = Arc::new(AtomicUsize::new(0));

            let ptr = boxed(Recursive {
                _v: 0,
                pointers: (0..var::ITEMS)
                    .map(|_| boxed(DropTracker(dropped.clone())))
                    .collect(),
            });

            collector.retire(ptr, |ptr: *mut Recursive, collector| {
                let value = Box::from_raw(ptr);
                for pointer in value.pointers {
                    (*collector).retire(pointer, reclaim_boxed);
                }
            });

            collector.reclaim_all();
            assert_eq!(dropped.load(Ordering::Relaxed), var::ITEMS);
        }
    }

    #[test]
    fn reentrant() {
        let collector = Arc::new(Collector::new().batch_size(5));
        let dropped = Arc::new(AtomicUsize::new(0));

        let objects: UnsafeSend<Vec<_>> = UnsafeSend(
            (0..5)
                .map(|_| boxed(DropTracker(dropped.clone())))
                .collect(),
        );

        assert_eq!(dropped.load(Ordering::Relaxed), 0);

        let guard1 = collector.pin();
        let guard2 = collector.pin();
        let guard3 = collector.pin();

        thread::spawn({
            let collector = collector.clone();

            move || {
                let guard = collector.pin();
                for object in { objects }.0 {
                    unsafe { guard.retire(object, reclaim_boxed) }
                }
            }
        })
        .join()
        .unwrap();

        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(guard1);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(guard2);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(guard3);
        assert_eq!(dropped.load(Ordering::Relaxed), 5);

        let dropped = Arc::new(AtomicUsize::new(0));

        let objects: UnsafeSend<Vec<_>> = UnsafeSend(
            (0..5)
                .map(|_| boxed(DropTracker(dropped.clone())))
                .collect(),
        );

        assert_eq!(dropped.load(Ordering::Relaxed), 0);

        let mut guard1 = collector.pin();
        let mut guard2 = collector.pin();
        let mut guard3 = collector.pin();

        thread::spawn({
            let collector = collector.clone();

            move || {
                let guard = collector.pin();
                for object in { objects }.0 {
                    unsafe { guard.retire(object, reclaim_boxed) }
                }
            }
        })
        .join()
        .unwrap();

        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        guard1.refresh();
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(guard1);
        guard2.refresh();
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        drop(guard2);
        assert_eq!(dropped.load(Ordering::Relaxed), 0);
        guard3.refresh();
        assert_eq!(dropped.load(Ordering::Relaxed), 5);
    }

    /// this stack testing is directly from: [seize](https://github.com/ibraheemdev/seize/blob/master/tests/lib.rs)
    #[derive(Debug)]
    pub struct Stack<T> {
        head: AtomicPtr<Node<T>>,
        collector: Collector,
    }

    #[derive(Debug)]
    struct Node<T> {
        data: ManuallyDrop<T>,
        next: *mut Node<T>,
    }

    impl<T> Stack<T> {
        pub fn new(batch_size: usize) -> Stack<T> {
            Stack {
                head: AtomicPtr::new(std::ptr::null_mut()),
                collector: Collector::new().batch_size(batch_size),
            }
        }

        pub fn push<G: Guard>(&self, value: T, guard: &G) {
            let new = boxed(Node {
                data: ManuallyDrop::new(value),
                next: std::ptr::null_mut(),
            });

            loop {
                let head = guard.protect(&self.head);
                unsafe { (*new).next = head }

                if self
                    .head
                    .compare_exchange(head, new, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }

        pub fn pop<G: Guard>(&self, guard: &G) -> Option<T> {
            loop {
                let head = guard.protect(&self.head);

                if head.is_null() {
                    return None;
                }

                let next = unsafe { (*head).next };

                if self
                    .head
                    .compare_exchange(head, next, Ordering::Relaxed, Ordering::Relaxed)
                    .is_ok()
                {
                    unsafe {
                        let data = std::ptr::read(&(*head).data);
                        self.collector.retire(head, reclaim_boxed);
                        return Some(ManuallyDrop::into_inner(data));
                    }
                }
            }
        }

        pub fn is_empty(&self) -> bool {
            self.head.load(Ordering::Relaxed).is_null()
        }
    }

    impl<T> Drop for Stack<T> {
        fn drop(&mut self) {
            let guard = self.collector.pin();
            while self.pop(&guard).is_some() {}
        }
    }

    #[test]
    fn stress() {
        for _ in 0..var::ITER {
            let stack = Arc::new(Stack::new(1));

            thread::scope(|s| {
                for i in 0..var::ITEMS {
                    stack.push(i, &stack.collector.pin());
                    stack.pop(&stack.collector.pin());
                }

                for _ in 0..var::THREADS {
                    s.spawn(|| {
                        for i in 0..var::ITEMS {
                            stack.push(i, &stack.collector.pin());
                            stack.pop(&stack.collector.pin());
                        }
                    });
                }
            });

            assert!(stack.pop(&stack.collector.pin()).is_none());
            assert!(stack.is_empty());
        }
    }

    #[test]
    fn shared_stress() {
        for _ in 0..var::ITER {
            let stack = Arc::new(Stack::new(1));
            let guard = &stack.collector.pin_owned();

            thread::scope(|s| {
                for i in 0..var::ITEMS {
                    stack.push(i, guard);
                    stack.pop(guard);
                }

                for _ in 0..var::THREADS {
                    s.spawn(|| {
                        for i in 0..var::ITEMS {
                            stack.push(i, guard);
                            stack.pop(guard);
                        }
                    });
                }
            });

            assert!(stack.pop(guard).is_none());
            assert!(stack.is_empty());
        }
    }

    #[test]
    fn owned_stress() {
        for _ in 0..var::ITER {
            let stack = Arc::new(Stack::new(1));

            thread::scope(|s| {
                for i in 0..var::ITEMS {
                    let guard = &stack.collector.pin_owned();
                    stack.push(i, guard);
                    stack.pop(guard);
                }

                for _ in 0..var::THREADS {
                    s.spawn(|| {
                        for i in 0..var::ITEMS {
                            let guard = &stack.collector.pin_owned();
                            stack.push(i, guard);
                            stack.pop(guard);
                        }
                    });
                }
            });

            assert!(stack.pop(&stack.collector.pin_owned()).is_none());
            assert!(stack.is_empty());
        }
    }
}
