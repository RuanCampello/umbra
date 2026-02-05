use crate::collections::chash::utils::{unpack, untagged, CheckedPin, Tagged};
use crate::collections::reclamation::{self, Collector, OwnedGuard};
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

pub enum InsertResult<'p, V> {
    Inserted(&'p V),
    Replaced(&'p V),
    Error { current: &'p V, non_inserted: V },
}

enum RawInsertResult<'g, K, V> {
    Inserted(&'g V),
    Replaced(&'g V),
    Error {
        current: &'g V,
        not_inserted: *mut Entry<K, V>,
    },
}

pub(super) enum EntryStatus<K, V> {
    Null,
    Copied(Tagged<Entry<K, V>>),
    Value(Tagged<Entry<K, V>>),
}

enum InsertStatus<K, V> {
    Inserted,
    Found(EntryStatus<K, V>),
}

enum UpdateStatus<K, V> {
    Replace(Tagged<Entry<K, V>>),
    Found(EntryStatus<K, V>),
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

    fn wrapping_insert<'p>(
        &self,
        key: K,
        value: V,
        replace: bool,
        pin: &'p impl CheckedPin,
    ) -> InsertResult<'p, V> {
        let raw = self.raw_insert(key, value, replace, pin);

        match raw {
            RawInsertResult::Replaced(value) => InsertResult::Replaced(value),
            RawInsertResult::Inserted(value) => {
                self.counter.get(pin).fetch_add(1, Ordering::Relaxed);
                InsertResult::Inserted(value)
            }
            RawInsertResult::Error {
                current,
                not_inserted,
            } => {
                let non_inserted = unsafe { Box::from_raw(not_inserted) }.value;
                InsertResult::Error {
                    current,
                    non_inserted,
                }
            }
        }
    }

    fn raw_insert<'p>(
        &self,
        key: K,
        value: V,
        replace: bool,
        pin: &'p impl CheckedPin,
    ) -> RawInsertResult<'p, K, V> {
        let new = untagged(Box::into_raw(Box::new(Entry { key, value })));
        let new_ref = unsafe { &(*new.ptr) };
        let mut table = self.root(pin);

        if table.raw.is_null() {
            table = self.init(None);
        }

        let (h1, h2) = self.hash(&new_ref.key);
        let mut copy = true;

        loop {
            let mut probe = Probe::new(h1, table.mask);

            let copying = 'p: loop {
                if probe.len > table.limit {
                    break None;
                }

                let metadata = unsafe { table.metadata(probe.i) }.load(Ordering::Acquire);
                let entry = if metadata == metadata::EMPTY {
                    match unsafe { self.insert_at(probe.i, h2, new.raw, table, pin) } {
                        InsertStatus::Inserted => return RawInsertResult::Inserted(&new_ref.value),
                        InsertStatus::Found(EntryStatus::Value(found))
                        | InsertStatus::Found(EntryStatus::Copied(found)) => found,
                        InsertStatus::Found(EntryStatus::Null) => {
                            probe.next(table.mask);
                            continue 'p;
                        }
                    }
                } else if metadata == h2 {
                    let entry = unpack(pin.protect(unsafe { table.entry(probe.i) }));

                    if entry.ptr.is_null() {
                        probe.next(table.mask);
                        continue 'p;
                    }

                    entry
                } else {
                    probe.next(table.mask);
                    continue 'p;
                };

                let entry_ref = unsafe { &(*entry.ptr) };
                if entry_ref.key != new_ref.key {
                    probe.next(table.mask);
                    continue 'p;
                }

                if entry.tag() & Entry::COPYING != 0 {
                    break 'p Some(probe.i);
                }

                if !replace {
                    return RawInsertResult::Error {
                        current: &entry_ref.value,
                        not_inserted: new.ptr,
                    };
                }

                match unsafe { self.insert_slow(probe.i, entry, new.raw, table, pin) } {
                    UpdateStatus::Replace(entry) => {
                        let value = unsafe { &(*entry.ptr).value };
                        return RawInsertResult::Replaced(value);
                    }
                    UpdateStatus::Found(EntryStatus::Copied(_)) => break 'p Some(probe.i),
                    UpdateStatus::Found(EntryStatus::Null) => {
                        probe.next(table.mask);
                        continue 'p;
                    }
                    UpdateStatus::Found(EntryStatus::Value(_)) => {}
                }
            };

            todo!()
        }
        todo!()
    }

    #[inline]
    fn insert_at(
        &self,
        i: usize,
        metadata: u8,
        new_entry: *mut Entry<K, V>,
        table: Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> InsertStatus<K, V> {
        let entry = unsafe { table.entry(i) };
        let metadata_store = unsafe { table.metadata(i) };

        let found = match pin.compare_exchange(
            entry,
            std::ptr::null_mut(),
            new_entry,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                metadata_store.store(metadata, Ordering::Release);
                return InsertStatus::Inserted;
            }
            Err(found) => unpack(found),
        };

        let (metadata, status) = match EntryStatus::from(found) {
            EntryStatus::Value(_) | EntryStatus::Copied(_) => {
                let key = unsafe { &(*found.ptr).key };
                let hash = self.hasher.hash_one(key);

                (metadata::second(hash), EntryStatus::Value(found))
            }
            EntryStatus::Null => (metadata::TOMBSTONE, EntryStatus::Null),
        };

        if metadata_store.load(Ordering::Relaxed) == metadata::EMPTY {
            metadata_store.store(metadata, Ordering::Release);
        }

        InsertStatus::Found(status)
    }

    #[inline]
    unsafe fn update_at(
        &self,
        i: usize,
        current: Tagged<Entry<K, V>>,
        new_entry: *mut Entry<K, V>,
        table: Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> UpdateStatus<K, V> {
        let entry = unsafe { table.entry(i) };

        let found = match pin.compare_exchange(
            entry,
            current.raw,
            new_entry,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => unsafe {
                self.retire(current, &table, pin);

                return UpdateStatus::Replace(current);
            },
            Err(found) => unpack(found),
        };

        UpdateStatus::Found(EntryStatus::from(found))
    }

    #[cold]
    #[inline(never)]
    fn insert_slow(
        &self,
        i: usize,
        mut entry: Tagged<Entry<K, V>>,
        new_entry: *mut Entry<K, V>,
        table: Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> UpdateStatus<K, V> {
        loop {
            match unsafe { self.update_at(i, entry, new_entry, table, pin) } {
                UpdateStatus::Found(EntryStatus::Value(found)) => entry = found,
                status => return status,
            }
        }
    }

    #[inline]
    unsafe fn retire(
        &self,
        entry: Tagged<Entry<K, V>>,
        table: &Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) {
        match self.resize {
            Resize::Blocking => pin.retire(entry.ptr, reclamation::reclaim_boxed),
            Resize::Incremental(_) => {
                if entry.tag() & Entry::BORROWED == 0 {
                    unsafe { pin.retire(entry.ptr, reclamation::reclaim_boxed) };
                    return;
                }

                let root = self.root(pin);

                let mut next = Some(*table);
                while let Some(table) = next {
                    if table.raw == root.raw {
                        unsafe { pin.retire(entry.ptr, reclamation::reclaim_boxed) };
                        return;
                    }

                    next = table.next();
                }

                let mut previous = root;

                loop {
                    let next = previous.next().unwrap();
                    if next.raw == table.raw {
                        previous.state().deffered.push(entry.ptr);
                        return;
                    }

                    previous = next;
                }
            }
        }
    }

    #[cold]
    #[inline(never)]
    fn init(&self, capacity: Option<usize>) -> Table<Entry<K, V>> {
        const CAPACITY: usize = 1 << 5;

        let mut new = Table::alloc(capacity.unwrap_or(CAPACITY));
        *new.mut_state().status.get_mut() = State::PROMOTED;

        match self.table.compare_exchange(
            std::ptr::null_mut(),
            new.raw,
            Ordering::Release,
            Ordering::Acquire,
        ) {
            Ok(_) => new,
            Err(found) => {
                unsafe { Table::dealloc(new) }
                unsafe { Table::from(found) }
            }
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
    pub(super) const COPYING: usize = 0b001;
    pub(super) const COPIED: usize = 0b010;
    pub(super) const BORROWED: usize = 0b100;
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
