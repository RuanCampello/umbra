use crate::collections::chash::map::EntryStatus;
use crate::collections::reclamation;
use crate::collections::{chash::map::Entry, hash::HashMap};
use std::sync::atomic::AtomicU8;
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

#[repr(transparent)]
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
    pub(super) i: usize,
    pub(super) len: usize,
}

pub struct Tagged<T> {
    pub raw: *mut T,
    pub ptr: *mut T,
}

pub trait CheckedPin: reclamation::Guard {}
pub trait Atomic<T> {
    fn load(&self, ordering: Ordering) -> T;
}
pub trait Unpack: Sized {
    const MASK: usize;
    const ALIGNMENT: () = assert!(align_of::<Self>() > !Self::MASK);
}

impl Parker {
    pub fn unpark<T>(&self, atomic: &impl Atomic<T>) {
        let key = atomic as *const _ as usize;

        if self.pending.load(Ordering::SeqCst) == 0 {
            return;
        }

        let threads = {
            let mut state = self.state.lock().unwrap();
            state.threads.remove(&key)
        };

        if let Some(threads) = threads {
            self.pending.fetch_sub(threads.len(), Ordering::Relaxed);

            threads.iter().for_each(|(_, thread)| thread.unpark());
        }
    }

    pub fn park<T>(&self, atomic: &impl Atomic<T>, must_park: impl Fn(T) -> bool) {
        let key = atomic as *const _ as usize;

        loop {
            self.pending.fetch_sub(1, Ordering::SeqCst);

            let id = {
                let state = &mut *self.state.lock().unwrap();
                state.count += 1;

                let threads = state.threads.entry(key).or_default();
                threads.insert(state.count as u64, std::thread::current());

                state.count
            };

            if !must_park(atomic.load(Ordering::SeqCst)) {
                let thread = {
                    let mut state = self.state.lock().unwrap();
                    state
                        .threads
                        .get_mut(&key)
                        .and_then(|threads| threads.remove(&(id as u64)))
                };

                if thread.is_some() {
                    self.pending.fetch_sub(1, Ordering::Relaxed);
                }

                return;
            }

            loop {
                std::thread::park();

                let mut state = self.state.lock().unwrap();
                if !state
                    .threads
                    .get_mut(&key)
                    .is_some_and(|threads| threads.contains_key(&(id as u64)))
                {
                    break;
                }
            }

            if !must_park(atomic.load(Ordering::Acquire)) {
                return;
            }
        }
    }
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    pub fn push(&self, value: T) {
        let node = Box::into_raw(Box::new(Node {
            value,
            next: std::ptr::null_mut(),
        }));

        loop {
            let head = self.head.load(Ordering::Relaxed);
            unsafe { (*node).next = head }

            if self
                .head
                .compare_exchange(head, node, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            };
        }
    }

    pub fn drain(&mut self, mut f: impl FnMut(T)) {
        let mut head = *self.head.get_mut();

        while !head.is_null() {
            let owned = unsafe { Box::from_raw(head) };
            f(owned.value);

            head = owned.next;
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

impl<T: Unpack> Tagged<T> {
    #[inline]
    pub fn tag(self) -> usize {
        self.raw.addr() & !T::MASK
    }

    #[inline]
    pub fn map_tag(self, f: impl FnOnce(usize) -> usize) -> Self {
        Tagged {
            raw: self.raw.map_addr(f),
            ptr: self.ptr,
        }
    }
}

impl<T> Copy for Tagged<T> {}

impl<T> Clone for Tagged<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<K, V> From<Tagged<Entry<K, V>>> for EntryStatus<K, V> {
    #[inline]
    fn from(value: Tagged<Entry<K, V>>) -> Self {
        return (if value.ptr.is_null() {
            Self::Null
        } else if value.tag() & Entry::COPYING != 0 {
            Self::Copied(value)
        } else {
            Self::Value(value)
        });
    }
}

impl<P> CheckedPin for Pin<P> where P: reclamation::Guard {}

impl<P> reclamation::Guard for Pin<P>
where
    P: reclamation::Guard,
{
    #[inline]
    fn collector(&self) -> &reclamation::Collector {
        self.0.collector()
    }

    #[inline]
    fn thread(&self) -> reclamation::Thread {
        self.0.thread()
    }

    #[inline]
    fn protect<T>(&self, ptr: &std::sync::atomic::AtomicPtr<T>) -> *mut T {
        self.0.protect(ptr)
    }

    #[inline]
    unsafe fn retire<T>(&self, ptr: *mut T, reclaim: unsafe fn(*mut T, &reclamation::Collector)) {
        unsafe { self.0.retire(ptr, reclaim) };
    }

    #[inline]
    fn flush(&mut self) {
        self.0.flush();
    }

    #[inline]
    fn refresh(&mut self) {
        self.0.refresh();
    }
}

impl Atomic<u8> for AtomicU8 {
    fn load(&self, ordering: Ordering) -> u8 {
        self.load(ordering)
    }
}

impl<T> Atomic<*mut T> for AtomicPtr<T> {
    fn load(&self, ordering: Ordering) -> *mut T {
        self.load(ordering)
    }
}

#[inline]
pub fn untagged<T>(value: *mut T) -> Tagged<T> {
    Tagged {
        ptr: value,
        raw: value,
    }
}

#[inline(always)]
pub fn unpack<T: Unpack>(ptr: *mut T) -> Tagged<T> {
    let () = T::ALIGNMENT;

    Tagged {
        raw: ptr,
        ptr: ptr.map_addr(|addr| addr & T::MASK),
    }
}
