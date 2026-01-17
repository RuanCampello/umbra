use std::{
    alloc::{self, Layout},
    marker::PhantomData,
    mem,
    sync::atomic::{AtomicPtr, AtomicU8},
};

use crate::core::collections::chash::{map::State, utils::Probe};

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
    mask: usize,
    limit: usize,
    raw: *mut RawTable<T>,
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

    fn layout(len: usize) -> Layout {
        let size = mem::size_of::<TableLayout<T>>()
            + (mem::size_of::<u8>() * len)
            + (mem::size_of::<AtomicPtr<T>>() * len);

        Layout::from_size_align(size, mem::size_of::<TableLayout<T>>()).unwrap()
    }
}

impl<T> Clone for Table<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T> Copy for Table<T> {}
