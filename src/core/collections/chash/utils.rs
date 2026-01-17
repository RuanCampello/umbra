use crate::core::HashMap;
use std::{
    sync::{
        atomic::{AtomicIsize, AtomicPtr, AtomicUsize},
        Mutex, OnceLock,
    },
    thread::Thread,
};

pub struct Counter(Box<[AtomicIsize]>);

pub struct Parker {
    pending: AtomicUsize,
    state: Mutex<State>,
}

pub struct Stack<T> {
    head: AtomicPtr<Node<T>>,
}

#[derive(Default)]
struct State {
    count: usize,
    threads: HashMap<usize, HashMap<u64, Thread>>,
}

struct Node<T> {
    value: T,
    next: *mut Node<T>,
}

impl<T> Stack<T> {
    pub fn new() -> Self {
        Self {
            head: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

impl Stack<()> {
    const PENDING: u8 = 0;
    const ABORTED: u8 = 1;
    const PROMOTED: u8 = 2;
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
