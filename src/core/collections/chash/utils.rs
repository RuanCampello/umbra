use std::sync::{atomic::AtomicIsize, OnceLock};

pub struct Counter(Box<[AtomicIsize]>);

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
