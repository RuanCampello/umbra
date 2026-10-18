#![allow(clippy::result_large_err)]

//! # Umbra
//!
//! A relational database built from scratch in Rust, without dependencies
//!
//! - [sql]: tokenizer, parser, analyser, rule-based optimiser and statement preparation
//! - [executor]: plans and runs statements inside a session's transaction: scans, index lookups,
//!   joins, grouping, sorting, spilling to disk when memory runs short
//! - [vm]: expression evaluation
//! - [storage::engine]: the MVCC engine. Rows live in memory as version chains inside chunks of
//!   1024 slots. Commits go through a group-committed redo log, checkpoints write images of the
//!   tables, and recovery loads the newest image and replays the log after it. Snapshot isolation by
//!   default, READ COMMITTED and SERIALIZABLE on request, and no transaction ever waits
//! - [db]: sessions ([db::MvccDatabase]), results and errors
//! - [tcp]: the wire protocol and server
//! - [core]: the value types (numeric, date, interval, UUID, JSONB) and CRC32C

use db::DatabaseError;

mod collections;
mod core;
pub mod db;
pub(crate) mod executor;
pub mod sql;
pub(crate) mod storage;
pub mod tcp;
mod vm;

pub type Result<T> = std::result::Result<T, DatabaseError>;
pub use core::uuid::Uuid;
