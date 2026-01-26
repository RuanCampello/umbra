use crate::collections::chash::{map::State, utils::Probe};
use std::{
    alloc::{self, Layout},
    marker::PhantomData,
    mem,
    sync::atomic::{AtomicPtr, AtomicU8, Ordering},
};

#[repr(transparent)]
/// From:
/// [https://github.com/ibraheemdev/papaya/blob/58dac23f2d93a0764bf37dd890fa0ef892f48c0f/src/raw/alloc.rs#L8C1-L13C44.]
pub struct RawTable<T>(u8, PhantomData<T>);

#[repr(C)]
struct TableLayout<T> {
    mask: usize,
    limit: usize,
    state: State<T>,
    metadata: [AtomicU8; 0],
    entries: [AtomicPtr<T>; 0],
}

#[repr(C)]
pub struct Table<T> {
    pub(in crate::collections::chash) mask: usize,
    pub(in crate::collections::chash) limit: usize,
    pub(in crate::collections::chash) raw: *mut RawTable<T>,
}

impl<T> Table<T> {
    pub fn alloc(len: usize) -> Self {
        assert!(len.is_power_of_two());

        let len = len.max(mem::align_of::<AtomicPtr<T>>());
        let mask = len - 1;
        let limit = Probe::limit(len);
        let layout = Table::<T>::layout(len);

        let ptr = unsafe { alloc::alloc_zeroed(layout) };
        if ptr.is_null() {
            alloc::handle_alloc_error(layout);
        }

        unsafe {
            ptr.cast::<TableLayout<T>>().write(TableLayout {
                mask,
                limit,
                state: State::default(),
                metadata: [],
                entries: [],
            });

            ptr.add(mem::size_of::<TableLayout<T>>())
                .cast::<u8>()
                .write_bytes(0, len);
        }

        Self {
            mask,
            limit,
            raw: ptr.cast::<RawTable<T>>(),
        }
    }

    pub unsafe fn dealloc(table: Table<T>) {
        let layout = Self::layout(table.len());

        unsafe {
            std::ptr::drop_in_place(table.raw.cast::<TableLayout<T>>());
            alloc::dealloc(table.raw.cast::<u8>(), layout);
        }
    }

    #[inline]
    pub fn next(&self) -> Option<Self> {
        let next = self.state().next.load(Ordering::Acquire);

        if !next.is_null() {
            return unsafe { Some(Table::from(next)) };
        }

        None
    }

    #[inline]
    pub unsafe fn metadata(&self, i: usize) -> &AtomicU8 {
        debug_assert!(i < self.len());

        unsafe {
            let metadata = self.raw.add(mem::size_of::<TableLayout<T>>());
            &*metadata.cast::<AtomicU8>().add(i)
        }
    }

    #[inline]
    pub unsafe fn entry(&self, i: usize) -> &AtomicPtr<T> {
        debug_assert!(i < self.len());

        unsafe {
            let metadata = self.raw.add(mem::size_of::<TableLayout<T>>());
            let entries = metadata.add(self.len()).cast::<AtomicPtr<T>>();
            &*entries.add(i)
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.mask + 1
    }

    #[inline]
    pub fn mut_state(&mut self) -> &mut State<T> {
        unsafe { &mut (*self.raw.cast::<TableLayout<T>>()).state }
    }

    #[inline]
    pub fn state(&self) -> &State<T> {
        unsafe { &(*self.raw.cast::<TableLayout<T>>()).state }
    }

    fn layout(len: usize) -> Layout {
        let size = mem::size_of::<TableLayout<T>>()
            + (mem::size_of::<u8>() * len)
            + (mem::size_of::<AtomicPtr<T>>() * len);

        Layout::from_size_align(size, mem::size_of::<TableLayout<T>>()).unwrap()
    }
}

impl<T> From<*mut RawTable<T>> for Table<T> {
    fn from(raw: *mut RawTable<T>) -> Self {
        if raw.is_null() {
            return Table {
                raw,
                mask: 0,
                limit: 0,
            };
        }

        let layout = unsafe { &*raw.cast::<TableLayout<T>>() };
        Table {
            raw,
            limit: layout.mask,
            mask: layout.mask,
        }
    }
}

impl<T> Clone for Table<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Table<T> {}
