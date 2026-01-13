//! This is the heart of the database, responsible for durability and data access.
//! It is built in layers:
//!
//! 1. **IO Layer** (`pagination::io`):
//!    Handles reading and writing raw bytes to the filesystem.
//!
//! 2. **Page Layer** (`pagination::pager` & `page`):
//!    - The database file is split into fixed-size blocks called **Pages** (default 4KB).
//!    - The **Pager** manages loading these pages from disk into memory.
//!    - The **Cache** (`pagination::cache`) keeps hot pages in memory using a Clock eviction algorithm.
//!    - Pages use a **Slotted Page** layout: Header at the start, Data grows from the end, Pointers grow from the front.
//!
//! 3. **B-Tree Layer** (`btree`):
//!    - Tables and Indexes are stored as B+Trees.
//!    - Internal nodes contain keys and pointers to child pages.
//!    - Leaf nodes contain the actual **Cells** (serialized rows).
//!
//! 4. **Tuple Layer** (`tuple`):
//!    - Handles serialising Rust types (`Value`) into the compact binary format stored in Cells.

pub(crate) mod btree;
pub(crate) mod mvcc;
pub(crate) mod page;
pub(crate) mod pagination;
pub(crate) mod tuple;
pub(crate) mod wal;

#[allow(unused)]
pub(crate) use crate::core::storage::btree::MemoryBuffer;
