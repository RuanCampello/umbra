use crate::collections::reclamation::{Collector, OwnedGuard};
use crate::collections::{
    chash::{
        allocation::{RawTable, Table},
        utils::{self, Counter, Parker, Pin, Probe, Stack},
    },
    reclamation::LocalGuard,
};
use std::sync::{
    atomic::{AtomicPtr, AtomicU8, AtomicUsize},
    Mutex,
};

pub struct HashMap<K, V, S> {
    table: AtomicPtr<RawTable<Entry<K, V>>>,
    counter: utils::Counter,
    resize: Resize,
    capacity: usize,

    collector: Collector,

    hasher: S,
}

#[repr(C, align(8))]
pub struct Entry<K, V> {
    key: K,
    value: V,
}

pub struct State<T> {
    next: AtomicPtr<RawTable<T>>,
    allocating: Mutex<()>,
    copied: AtomicUsize,
    claim: AtomicUsize,
    status: AtomicU8,
    parker: Parker,
    deffered: Stack<*mut T>,
}

#[derive(Debug)]
/// [https://github.com/ibraheemdev/papaya/blob/58dac23f2d93a0764bf37dd890fa0ef892f48c0f/src/map.rs#L155]
pub enum Resize {
    Incremental(usize),
    Blocking,
}

impl<K, V, S> HashMap<K, V, S> {
    #[inline]
    pub fn new(capacity: usize, collector: Collector, hasher: S, resize: Resize) -> Self {
        if capacity == 0 {
            return Self {
                table: AtomicPtr::new(std::ptr::null_mut()),
                counter: Counter::default(),
                collector,
                resize,
                capacity,
                hasher,
            };
        };

        let mut table = Table::alloc(Probe::entries_for(capacity));
        *table.mut_state().status.get_mut() = State::PROMOTED;

        HashMap {
            hasher,
            resize,
            capacity,
            collector,
            counter: Counter::default(),
            table: AtomicPtr::new(table.raw),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.counter.sum()
    }

    pub fn pin(&self) -> Pin<LocalGuard<'_>> {
        unsafe { Pin::new(self.collector.pin()) }
    }

    pub fn pin_owned(&self) -> Pin<OwnedGuard<'_>> {
        unsafe { Pin::new(self.collector.pin_owned()) }
    }
}

impl State<()> {
    const PENDING: u8 = 0;
    const ABORTED: u8 = 1;
    const PROMOTED: u8 = 2;
}

impl<T> Default for State<T> {
    fn default() -> Self {
        State {
            next: AtomicPtr::new(std::ptr::null_mut()),
            allocating: Mutex::new(()),
            copied: AtomicUsize::new(0),
            claim: AtomicUsize::new(0),
            status: AtomicU8::new(State::PENDING),
            parker: Parker::default(),
            deffered: Stack::new(),
        }
    }
}

impl Default for Resize {
    fn default() -> Self {
        Self::Incremental(1 << 6)
    }
}
