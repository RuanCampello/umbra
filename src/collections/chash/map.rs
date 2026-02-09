use crate::collections::chash::utils::{unpack, untagged, CheckedPin, Tagged};
use crate::collections::reclamation::{self, Collector, OwnedGuard};
use crate::collections::{
    chash::{
        allocation::{RawTable, Table},
        utils::{self, Counter, Parker, Pin, Probe, Stack},
    },
    reclamation::LocalGuard,
};
use std::hash::{BuildHasher, Hash, RandomState};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::{
    atomic::{AtomicPtr, AtomicU8, AtomicUsize},
    Mutex,
};

pub struct HashMap<K, V, S = RandomState> {
    table: AtomicPtr<RawTable<Entry<K, V>>>,
    counter: utils::Counter,
    resize: Resize,
    capacity: usize,

    collector: Collector,

    hasher: S,
}

pub struct HashMapBuilder<K, V, S = RandomState> {
    hasher: S,
    capacity: usize,
    collector: Collector,
    _kv: PhantomData<(K, V)>,
    resize: Resize,
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

pub(in crate::collections::chash) mod metadata {
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
    fn get<'p, Q>(&self, key: &Q, pin: &'p impl reclamation::Guard) -> Option<&'p V>
    where
        K: 'p + std::borrow::Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        match self.raw_get(key, self.verify(pin)) {
            Some((_, value)) => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub fn insert<'p>(&self, key: K, value: V, pin: &'p impl reclamation::Guard) -> Option<&'p V> {
        match self.wrapping_insert(key, value, true, self.verify(pin)) {
            InsertResult::Inserted(_) => None,
            InsertResult::Replaced(value) => Some(value),
            InsertResult::Error { .. } => unreachable!(),
        }
    }

    #[inline]
    pub fn remove<'p, Q>(&self, key: &Q, pin: &'p impl reclamation::Guard) -> Option<&'p V>
    where
        K: 'p + std::borrow::Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        match self.raw_remove(key, self.verify(pin)) {
            Some((_, value)) => Some(value),
            None => None,
        }
    }

    #[inline]
    pub fn remove_entry<'p, Q>(
        &self,
        key: &Q,
        pin: &'p impl reclamation::Guard,
    ) -> Option<(&'p K, &'p V)>
    where
        K: 'p + std::borrow::Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        self.raw_remove(key, self.verify(pin))
    }

    #[inline]
    pub fn verify<'p, P>(&self, pin: &'p P) -> &'p Pin<P>
    where
        P: reclamation::Guard,
    {
        assert_eq!(
            *pin.collector(),
            self.collector,
            "Tried to access map with incompatible guard"
        );

        unsafe { Pin::from_ref(pin) }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn clear(&self, pin: &impl reclamation::Guard) {
        self.raw_clear(self.verify(pin));
    }

    #[inline]
    fn raw_get<'p, Q>(&self, key: &Q, pin: &'p impl CheckedPin) -> Option<(&'p K, &'p V)>
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
    fn raw_remove<'p, Q>(&self, key: &Q, pin: &'p impl CheckedPin) -> Option<(&'p K, &'p V)>
    where
        K: 'p + std::borrow::Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        unsafe { self.remove_if(key, |_, _| true, pin).unwrap_unchecked() }
    }

    #[inline]
    fn remove_if<'p, Q, F>(
        &self,
        key: &Q,
        mut must_remove: F,
        pin: &'p impl CheckedPin,
    ) -> Result<Option<(&'p K, &'p V)>, (&'p K, &'p V)>
    where
        K: 'p + std::borrow::Borrow<Q>,
        Q: Eq + Hash + ?Sized,
        F: FnMut(&K, &V) -> bool,
    {
        let mut table = self.root(pin);
        if table.raw.is_null() {
            return Ok(None);
        }

        let (h1, h2) = self.hash(key);
        let mut copy = true;

        loop {
            let mut probe = Probe::new(h1, table.mask);

            let copying = 'p: loop {
                if probe.len > table.limit {
                    break None;
                }

                let metadata = unsafe { table.metadata(probe.i).load(Ordering::Acquire) };
                if metadata == metadata::EMPTY {
                    return Ok(None);
                }

                if metadata != h2 {
                    probe.next(table.mask);
                    continue 'p;
                }

                let mut entry = unpack(pin.protect(unsafe { table.entry(probe.i) }));

                if entry.ptr.is_null() {
                    probe.next(table.mask);
                    continue 'p;
                }

                if &key != unsafe { &(*entry.ptr).key.borrow() } {
                    probe.next(table.mask);
                    continue 'p;
                }

                if entry.tag() & Entry::COPYING != 0 {
                    break 'p Some(probe.i);
                }

                loop {
                    let entry_ref = unsafe { &(*entry.ptr) };
                    if !must_remove(&entry_ref.key, &entry_ref.value) {
                        return Err((&entry_ref.key, &entry_ref.value));
                    }

                    let status =
                        unsafe { self.update_at(probe.i, entry, Entry::TOMBSTONE, table, pin) };

                    match status {
                        UpdateStatus::Replace(_) => {
                            unsafe {
                                table
                                    .metadata(probe.i)
                                    .store(metadata::TOMBSTONE, Ordering::Release)
                            };

                            self.counter.get(pin).fetch_sub(1, Ordering::Relaxed);

                            return Ok(Some((&entry_ref.key, &entry_ref.value)));
                        }

                        UpdateStatus::Found(EntryStatus::Copied(_)) => break 'p Some(probe.i),
                        UpdateStatus::Found(EntryStatus::Null) => return Ok(None),
                        UpdateStatus::Found(EntryStatus::Value(found)) => entry = found,
                    }
                }
            };

            table = match self.retry(copying, &mut copy, table, pin) {
                Some(table) => table,
                None => return Ok(None),
            }
        }
    }

    #[inline]
    fn raw_clear(&self, pin: &impl CheckedPin) {
        let mut table = self.root(pin);
        if table.raw.is_null() {
            return;
        }

        loop {
            table = self.linearise(table, pin);
            let mut copying = false;

            'p: for idx in 0..table.len() {
                let mut entry = unpack(pin.protect(unsafe { table.entry(idx) }));

                loop {
                    if entry.ptr.is_null() {
                        continue 'p;
                    }

                    if entry.tag() & Entry::COPYING != 0 {
                        copying = true;
                        continue 'p;
                    }

                    let result = unsafe {
                        table.entry(idx).compare_exchange(
                            entry.raw,
                            Entry::TOMBSTONE,
                            Ordering::Release,
                            Ordering::Acquire,
                        )
                    };

                    match result {
                        Ok(_) => {
                            unsafe {
                                table
                                    .metadata(idx)
                                    .store(metadata::TOMBSTONE, Ordering::Release)
                            };

                            self.counter.get(pin).fetch_sub(1, Ordering::Relaxed);
                            unsafe { self.retire(entry, &table, pin) };
                            continue 'p;
                        }
                        Err(found) => entry = unpack(found),
                    }
                }
            }

            if !copying {
                break;
            }

            table = self.copying(true, &table, pin);
        }
    }

    #[inline]
    fn linearise(
        &self,
        mut table: Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> Table<Entry<K, V>> {
        if matches!(self.resize, Resize::Incremental(_)) {
            while table.next().is_some() {
                table = self.copying(true, &table, pin);
            }
        }

        table
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

            table = self.retry_insert(copying, &mut copy, table, pin)
        }
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

    #[cold]
    #[inline(never)]
    fn retry_insert(
        &self,
        copying: Option<usize>,
        copy: &mut bool,
        table: Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> Table<Entry<K, V>> {
        let mut next = self.get_or_alloc(None, table);

        let next = match self.resize {
            Resize::Blocking => self.copying(true, &table, pin),
            Resize::Incremental(_) => {
                if *copy {
                    next = self.copying(false, &table, pin);
                }

                if let Some(idx) = copying {
                    self.wait_copy(idx, &table);
                }

                next
            }
        };

        *copy = false;
        next
    }

    #[cold]
    fn retry(
        &self,
        copying: Option<usize>,
        copy: &mut bool,
        table: Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> Option<Table<Entry<K, V>>> {
        let table = match self.resize {
            Resize::Blocking => match copying {
                Some(_) => self.copying(true, &table, pin),
                _ => return None,
            },
            Resize::Incremental(_) => {
                let next = table.next()?;
                if *copy {
                    self.copying(false, &table, pin);
                }

                if let Some(idx) = copying {
                    self.wait_copy(idx, &table);
                }

                next
            }
        };

        *copy = false;
        Some(table)
    }

    #[cold]
    #[inline(never)]
    fn wait_copy(&self, idx: usize, table: &Table<Entry<K, V>>) {
        const SPIN: usize = match cfg!(any(test, debug_assertions)) {
            true => 1,
            _ => 5,
        };

        let entry = unsafe { table.entry(idx) };

        for spin in 0..SPIN {
            let entry = unpack(entry.load(Ordering::Acquire));
            if entry.tag() & Entry::COPIED != 0 {
                return;
            }

            for _ in 0..(spin * spin) {
                std::hint::spin_loop();
            }
        }

        let parker = &table.state().parker;
        parker.park(entry, |entry| entry.addr() & Entry::COPIED == 0);
    }

    #[cold]
    #[inline(never)]
    fn get_or_alloc(
        &self,
        capacity: Option<usize>,
        table: Table<Entry<K, V>>,
    ) -> Table<Entry<K, V>> {
        const SPIN: usize = match cfg!(any(test, debug_assertions)) {
            true => 1,
            _ => 7,
        };

        if let Some(next) = table.next() {
            return next;
        }

        let state = table.state();

        let allocating = match state.allocating.try_lock() {
            Ok(lock) => lock,
            Err(_) => {
                let mut spun = 0;

                while spun <= SPIN {
                    for _ in 0..(spun * spun) {
                        std::hint::spin_loop();
                    }

                    if let Some(next) = table.next() {
                        return next;
                    }

                    spun += 1;
                }

                state.allocating.lock().unwrap()
            }
        };

        if let Some(table) = table.next() {
            return table;
        }

        let current_capacity = table.len();
        let active_entries = self.len();

        #[allow(unexpected_cfgs)]
        let next_capacity = match cfg!(stress) {
            true => current_capacity,
            false if active_entries >= (current_capacity >> 1) => current_capacity << 1,
            false if active_entries <= (current_capacity >> 3) => {
                self.capacity.max(current_capacity >> 1)
            }
            false => current_capacity,
        };

        let capacity = capacity.unwrap_or(next_capacity);
        assert!(
            next_capacity <= isize::MAX as usize,
            "HashMap exceeded maximum capacity"
        );

        let next = Table::alloc(next_capacity);
        state.next.store(next.raw, Ordering::Release);
        drop(allocating);

        next
    }

    #[cold]
    #[inline(never)]
    fn copying(
        &self,
        copy: bool,
        table: &Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> Table<Entry<K, V>> {
        match self.resize {
            Resize::Blocking => self.copy_blocking(table, pin),
            Resize::Incremental(chunk) => {
                let copied_to = self.copy_incremental(chunk, copy, pin);
                if !copy {
                    return table.next().unwrap();
                }

                copied_to
            }
        }
    }

    fn copy_incremental(
        &self,
        chunk: usize,
        block: bool,
        pin: &impl CheckedPin,
    ) -> Table<Entry<K, V>> {
        let table = self.root(pin);
        let Some(next) = table.next() else {
            return table;
        };

        loop {
            if self.promote(&table, &next, 0, pin) {
                return next;
            }

            loop {
                if next.state().claim.load(Ordering::Relaxed) <= table.len() {
                    break;
                }

                let start = next.state().claim.fetch_add(chunk, Ordering::Relaxed);
                let mut copied = 0;

                for idx in 0..chunk {
                    let idx = start + idx;
                    if idx >= table.len() {
                        break;
                    }

                    unsafe { self.copy_at_blocking(idx, &table, &next, pin) };
                    copied += 1;
                }

                if self.promote(&table, &next, copied, pin) || !block {
                    return next;
                }
            }

            if !block {
                return next;
            }

            let state = next.state();
            for spin in 0.. {
                const SPIN: usize = match cfg!(any(test, debug_assertions)) {
                    true => 1,
                    _ => 7,
                };

                let status = state.status.load(Ordering::Acquire);
                if status == State::PROMOTED {
                    return next;
                }

                if spin <= SPIN {
                    for _ in 0..(spin * spin) {
                        std::hint::spin_loop();
                    }

                    continue;
                }

                state
                    .parker
                    .park(&state.status, |status| status == State::PENDING);
            }
        }
    }

    fn copy_blocking(
        &self,
        table: &Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> Table<Entry<K, V>> {
        let mut next = table.next().unwrap();

        'copy: loop {
            while next.state().status.load(Ordering::Relaxed) == State::ABORTED {
                next = self.get_or_alloc(None, next);
            }

            if self.promote(table, &next, 0, pin) {
                return next;
            }

            let chunk = table.len().min(1 << 12);

            loop {
                if next.state().claim.load(Ordering::Relaxed) >= table.len() {
                    break;
                }

                let start = next.state().claim.fetch_add(chunk, Ordering::Relaxed);
                let mut copied = 0;

                for idx in 0..chunk {
                    let idx = start + idx;
                    if idx > table.len() {
                        break;
                    }

                    if unsafe { !self.copy_at_blocking(idx, table, &next, pin) } {
                        next.state().status.store(State::ABORTED, Ordering::SeqCst);
                        let alloc = self.get_or_alloc(None, next);
                        let state = table.state();
                        state.parker.unpark(&state.status);

                        next = alloc;
                        continue 'copy;
                    }

                    copied += 1;
                }

                if self.promote(table, &next, copied, pin) {
                    return next;
                }

                if next.state().status.load(Ordering::Relaxed) == State::ABORTED {
                    continue 'copy;
                }
            }

            let state = next.state();
            for spun in 0.. {
                const SPIN: usize = match cfg!(any(test, debug_assertions)) {
                    true => 1,
                    _ => 7,
                };

                let status = state.status.load(Ordering::Acquire);
                if status == State::ABORTED {
                    continue 'copy;
                }

                if status == State::PROMOTED {
                    return next;
                }

                if spun <= SPIN {
                    for _ in 0..(spun * spun) {
                        std::hint::spin_loop();
                    }

                    continue;
                }

                state
                    .parker
                    .park(&state.status, |status| status == State::PENDING)
            }
        }
    }

    fn promote(
        &self,
        table: &Table<Entry<K, V>>,
        next: &Table<Entry<K, V>>,
        copied: usize,
        pin: &impl CheckedPin,
    ) -> bool {
        let state = next.state();

        let copied = match copied > 0 {
            true => state.copied.fetch_add(copied, Ordering::AcqRel) + copied,
            _ => state.copied.load(Ordering::Acquire),
        };

        if copied == table.len() {
            let root = self.table.load(Ordering::Relaxed);

            if table.raw == root {
                if self
                    .table
                    .compare_exchange(table.raw, next.raw, Ordering::Release, Ordering::Acquire)
                    .is_ok()
                {
                    state.status.store(State::PROMOTED, Ordering::SeqCst);

                    unsafe {
                        pin.retire(table.raw, |table, collector| {
                            drop_table(Table::from(table), collector)
                        });
                    }
                }
            }

            state.parker.unpark(&state.status);
            return true;
        }

        false
    }

    unsafe fn copy_at_blocking(
        &self,
        idx: usize,
        table: &Table<Entry<K, V>>,
        next: &Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> bool {
        let entry = unpack(unsafe { table.entry(idx) }.fetch_or(Entry::COPYING, Ordering::AcqRel));
        if entry.raw == Entry::TOMBSTONE {
            return true;
        }

        if entry.ptr.is_null() {
            unsafe { table.metadata(idx) }.store(metadata::TOMBSTONE, Ordering::Release);
            return true;
        }

        unsafe {
            self.insert_copying(unpack(entry.ptr), false, next, pin)
                .is_some()
        }
    }

    unsafe fn insert_copying(
        &self,
        new_entry: Tagged<Entry<K, V>>,
        resize: bool,
        table: &Table<Entry<K, V>>,
        pin: &impl CheckedPin,
    ) -> Option<(Table<Entry<K, V>>, usize)> {
        let key = unsafe { &(*new_entry.ptr).key };
        let mut table = *table;
        let (h1, h2) = self.hash(key);

        loop {
            let mut probe = Probe::new(h1, table.mask);

            while probe.len <= table.limit {
                let metadata_entry = unsafe { table.metadata(probe.i) };
                let metadata = metadata_entry.load(Ordering::Acquire);

                if metadata == metadata::EMPTY {
                    let entry = unsafe { table.entry(probe.i) };
                    match pin.compare_exchange(
                        entry,
                        std::ptr::null_mut(),
                        new_entry.raw,
                        Ordering::Release,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            metadata_entry.store(h2, Ordering::Release);
                            return Some((table, probe.i));
                        }
                        Err(found) => {
                            let found = unpack(found);
                            let metadata = match found.ptr.is_null() {
                                true => metadata::TOMBSTONE,
                                _ => {
                                    let found_ref = unsafe { &(*found.ptr) };
                                    let hash = self.hasher.hash_one(&found_ref.key);

                                    metadata::second(hash)
                                }
                            };

                            if metadata_entry.load(Ordering::Relaxed) == metadata::EMPTY {
                                metadata_entry.store(metadata, Ordering::Release);
                            }
                        }
                    }
                }

                probe.next(table.mask);
            }

            if !resize {
                return None;
            }

            table = self.get_or_alloc(None, table)
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

impl<K, V> HashMap<K, V> {
    pub fn builder() -> HashMapBuilder<K, V> {
        HashMapBuilder {
            capacity: 0,
            hasher: RandomState::default(),
            collector: Collector::new(),
            _kv: PhantomData::default(),
            resize: Resize::default(),
        }
    }
}

impl<K, V, S> HashMapBuilder<K, V, S> {
    pub fn build(self) -> HashMap<K, V, S> {
        HashMap::new(self.capacity, self.collector, self.hasher, self.resize)
    }

    pub fn collector(self, collector: Collector) -> Self {
        HashMapBuilder {
            collector,
            hasher: self.hasher,
            capacity: self.capacity,
            resize: self.resize,
            _kv: PhantomData,
        }
    }

    pub fn capacity(self, capacity: usize) -> HashMapBuilder<K, V, S> {
        HashMapBuilder {
            capacity,
            hasher: self.hasher,
            collector: self.collector,
            resize: self.resize,
            _kv: PhantomData,
        }
    }

    pub fn resize(self, resize: Resize) -> Self {
        Self {
            resize,
            hasher: self.hasher,
            capacity: self.capacity,
            collector: self.collector,
            _kv: PhantomData,
        }
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

impl<K, V> Entry<K, V> {
    const TOMBSTONE: *mut Entry<K, V> = Entry::COPIED as _;
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

unsafe fn drop_table<K, V>(mut table: Table<Entry<K, V>>, collector: &Collector) {
    table
        .mut_state()
        .deffered
        .drain(|entry| unsafe { collector.retire(entry, reclamation::reclaim_boxed) });

    unsafe { Table::dealloc(table) };
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_map<K, V>(mut test: impl FnMut(&dyn Fn() -> HashMap<K, V>)) {
        let collector = || Collector::new().batch_size(128);

        #[allow(unexpected_cfgs)]
        if !cfg!(stress) {
            test(
                &(|| {
                    HashMap::builder()
                        .collector(collector())
                        .resize(Resize::Blocking)
                        .build()
                }),
            )
        }

        test(
            &(|| {
                HashMap::builder()
                    .collector(collector())
                    .resize(Resize::Incremental(1))
                    .build()
            }),
        );

        test(
            &(|| {
                HashMap::builder()
                    .collector(collector())
                    .resize(Resize::Incremental(128))
                    .build()
            }),
        );
    }

    #[test]
    fn insert() {
        with_map::<usize, usize>(|map| {
            let map = map();
            let pin = map.pin();
            let old = map.insert(69, 0, &pin);

            assert!(old.is_none());
        });
    }

    #[test]
    fn get_none() {
        with_map::<usize, usize>(|map| {
            let map = map();
            let pin = map.pin();
            let i = map.get(&69, &pin);

            assert!(i.is_none())
        });
    }

    #[test]
    fn clear() {
        with_map::<usize, usize>(|map| {
            let map = map();
            let pin = map.pin();
            {
                map.insert(0, 1, &pin);
                map.insert(1, 1, &pin);
                map.insert(2, 1, &pin);
                map.insert(3, 1, &pin);
                map.insert(4, 1, &pin);
            }

            map.clear(&pin);
            assert!(map.is_empty());
        })
    }

    #[test]
    fn insert_and_get() {
        with_map::<usize, usize>(|map| {
            let map = map();
            map.insert(69, 0, &map.pin());

            {
                let pin = map.pin();
                let i = map.get(&69, &pin).unwrap();
                assert_eq!(i, &0);
            }
        });
    }

    #[test]
    fn remove_none() {
        with_map::<usize, usize>(|map| {
            let map = map();
            let pin = map.pin();
            let old = map.remove(&69, &pin);

            assert!(old.is_none());
        });
    }

    #[test]
    fn insert_and_remove() {
        with_map::<usize, usize>(|map| {
            let map = map();
            let pin = map.pin();

            map.insert(69, 0, &pin);

            let old = map.remove(&69, &pin).unwrap();

            assert_eq!(old, &0);
            assert!(map.get(&69, &pin).is_none());
        });
    }
}
