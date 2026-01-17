use super::{allocation, utils};
use std::sync::atomic::AtomicPtr;

pub struct HashMap<K, V, S> {
    table: AtomicPtr<allocation::RawTable<Entry<K, V>>>,
    counter: utils::Counter,
    resize: Resize,
    capacity: usize,

    hasher: S,
}

#[repr(C, align(8))]
pub struct Entry<K, V> {
    key: K,
    value: V,
}

#[derive(Debug)]
/// [https://github.com/ibraheemdev/papaya/blob/58dac23f2d93a0764bf37dd890fa0ef892f48c0f/src/map.rs#L155]
pub enum Resize {
    Incremental(usize),
    Blocking,
}

impl Default for Resize {
    fn default() -> Self {
        Self::Incremental(1 << 6)
    }
}
