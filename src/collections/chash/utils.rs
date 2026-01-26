use crate::collections::hash::HashMap;
use crate::collections::reclamation;
use std::{
    sync::{
        atomic::{AtomicIsize, AtomicPtr, AtomicUsize, Ordering},
        Mutex, OnceLock,
    },
    thread::Thread,
};

pub struct Counter(Box<[AtomicIsize]>);

#[derive(Default)]
pub struct Parker {
    pending: AtomicUsize,
    state: Mutex<State>,
}

pub struct Stack<T> {
    head: AtomicPtr<Node<T>>,
}

pub struct Pin<P>(P);

#[derive(Default)]
struct State {
    count: usize,
    threads: HashMap<usize, HashMap<u64, Thread>>,
}

struct Node<T> {
    value: T,
    next: *mut Node<T>,
}

#[derive(Default)]
pub struct Probe {
    i: usize,
    len: usize,
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

impl<P> Pin<P> {
    pub unsafe fn new(pin: P) -> Pin<P> {
        Pin(pin)
    }

    pub unsafe fn from_ref(pin: &P) -> &Pin<P> {
        unsafe { &*(pin as *const P as *const Pin<P>) }
    }
}

impl<P> reclamation::Guard for Pin<P>
where
    P: reclamation::Guard,
{
    fn collector(&self) -> &reclamation::Collector {
        self.0.collector()
    }

    fn thread(&self) -> reclamation::Thread {
        self.0.thread()
    }

    fn protect<T>(&self, ptr: &std::sync::atomic::AtomicPtr<T>) -> *mut T {
        self.0.protect(ptr)
    }

    unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &reclamation::Collector)) {
        self.0.retire(ptr, reclaim);
    }

    fn refresh(&mut self) {
        self.0.refresh();
    }

    fn flush(&mut self) {
        self.0.flush();
    }
}

impl Probe {
    #[inline]
    pub fn new(hash: usize, mask: usize) -> Self {
        Probe {
            i: hash & mask,
            len: 0,
        }
    }

    #[inline]
    pub fn next(&mut self, mask: usize) {
        self.len += 1;
        self.i = (self.i + self.len) & mask;
    }

    #[inline]
    pub fn limit(capacity: usize) -> usize {
        5 * ((usize::BITS as usize) - (capacity.leading_zeros() as usize) - 1)
    }

    #[inline]
    pub fn entries_for(capacity: usize) -> usize {
        (capacity.checked_mul(8).expect("capacity must not overflow") / 6).next_power_of_two()
    }
}

impl Counter {
    #[inline]
    pub fn sum(&self) -> usize {
        self.0
            .iter()
            .map(|x| x.load(Ordering::Relaxed))
            .sum::<isize>()
            .try_into()
            .unwrap_or(0)
    }

    #[inline]
    pub fn get(&self, guard: &impl reclamation::Guard) -> &AtomicIsize {
        let i = guard.thread().id & (self.0.len() - 1);

        &self.0[i]
    }
}

impl Default for Counter {
    fn default() -> Self {
        static CPU_N: OnceLock<usize> = OnceLock::new();
        let num_of_cups = *CPU_N.get_or_init(|| {
            std::thread::available_parallelism()
                .map(Into::into)
                .unwrap_or(1)
        });

        let shards = (0..num_of_cups.next_power_of_two())
            .map(|_| Default::default())
            .collect();

        Counter(shards)
    }
}
