use criterion::{criterion_group, criterion_main, Criterion};

use std::{cell::RefCell, fs::File};
use umbra::db::Database;

thread_local! {
    static DISK_DB: RefCell<Option<Database<File>>> = RefCell::new(None);
}

fn disk_database() -> &'static Database<File> {
    DISK_DB.with(|cell| {
        let mut db_ref = cell.borrow_mut();
        if db_ref.is_none() {
            let _ = std::fs::remove_file("benchmark.db");
            *db_ref = Some(
                Database::init("benchmark.db").expect("Could not initialize benchmark database"),
            );
        }

        // SAFETY: We're extending the lifetime to 'static because:
        // 1. The data lives in thread-local storage
        // 2. The database will live as long as the thread
        // 3. This is for benchmarking only :/
        unsafe { &*(db_ref.as_ref().unwrap() as *const Database<File>) }
    })
}
