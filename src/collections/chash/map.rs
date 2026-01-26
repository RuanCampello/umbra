use crate::collections::chash::utils::{unpack, CheckedPin};
use crate::collections::reclamation::{Collector, OwnedGuard};
use crate::collections::{
    chash::{
        allocation::{RawTable, Table},
        utils::{self, Counter, Parker, Pin, Probe, Stack},
    },
    reclamation::LocalGuard,
};
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::Ordering;
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
    pub(super) next: AtomicPtr<RawTable<T>>,
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

mod metadata {
    use std::mem;

    pub const EMPTY: u8 = 0x80;
    pub const TOMBSTONE: u8 = u8::MAX;

    #[inline]
    pub fn first(hash: u64) -> usize {
        hash as usize
    }

    #[inline]
    pub fn second(hash: u64) -> u8 {
        const MIN_HASH_LEN: usize = match mem::size_of::<usize>() < mem::size_of::<u64>() {
            true => mem::size_of::<usize>(),
            _ => mem::size_of::<u64>(),
        };

        let top = hash >> (MIN_HASH_LEN * 8 - 7);
        (top & 0x7f) as u8
    }
}

impl<K, V, S> HashMap<K, V, S>
where
    K: Eq + Hash,
    S: BuildHasher,
{
    #[inline]
    pub fn get<'p, Q>(&self, key: &Q, pin: &'p impl CheckedPin) -> Option<(&'p K, &'p V)>
    where
        K: std::borrow::Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        let mut table = self.root(pin);
        if table.raw.is_null() {
            return None;
        }

        let (h1, h2) = self.hash(key);

        loop {
            let mut probe = Probe::new(h1, table.mask);

            'p: while probe.len <= table.limit {
                let metadata = unsafe { table.metadata(probe.i).load(Ordering::Acquire) };

                if metadata == metadata::EMPTY {
                    return None;
                }

                if metadata == h2 {
                    let entry = unpack(pin.protect(unsafe { table.entry(probe.i) }));

                    if entry.ptr.is_null() {
                        probe.next(table.mask);
                        continue 'p;
                    }

                    let entry_ref = unsafe { &(*entry.ptr) };
                    if key == entry_ref.key.borrow() {
                        if entry.tag() & Entry::COPIED != 0 {
                            break 'p;
                        }

                        return Some((&entry_ref.key, &entry_ref.value));
                    }
                }

                probe.next(table.mask)
            }

            if matches!(self.resize, Resize::Incremental(_)) {
                if let Some(next) = table.next() {
                    table = next;
                    continue;
                }
            }

            return None;
        }
    }

    #[inline]
    fn hash<Q: Hash + ?Sized>(&self, key: &Q) -> (usize, u8) {
        let hash = self.hasher.hash_one(key);
        (metadata::first(hash), metadata::second(hash))
    }
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

    #[inline]
    pub fn root(&self, pin: &impl CheckedPin) -> Table<Entry<K, V>> {
        let raw = pin.protect(&self.table);
        unsafe { Table::from(raw) }
    }

    pub fn pin(&self) -> Pin<LocalGuard<'_>> {
        unsafe { Pin::new(self.collector.pin()) }
    }

    pub fn pin_owned(&self) -> Pin<OwnedGuard<'_>> {
        unsafe { Pin::new(self.collector.pin_owned()) }
    }
}

impl<K, V> super::utils::Unpack for Entry<K, V> {
    const MASK: usize = !(Entry::COPYING | Entry::COPIED | Entry::BORROWED);
}

impl Entry<(), ()> {
    const COPYING: usize = 0b001;
    const COPIED: usize = 0b010;
    const BORROWED: usize = 0b100;
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
