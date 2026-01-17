use std::{
    alloc::Layout,
    marker::PhantomData,
    sync::atomic::{AtomicPtr, AtomicU8},
};

#[repr(transparent)]
/// From:
/// [https://github.com/ibraheemdev/papaya/blob/58dac23f2d93a0764bf37dd890fa0ef892f48c0f/src/raw/alloc.rs#L8C1-L13C44.]
pub struct RawTable<T>(u8, PhantomData<T>);

#[repr(C)]
struct TableLayout<T> {
    mask: usize,
    limit: usize,
    state: (),
    metadata: [AtomicU8; 0],
    entries: [AtomicPtr<T>; 0],
}
