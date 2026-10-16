//! Storage: the MVCC engine and the value encoding shared with the wire protocol
//!
//! - [engine]: in-memory row tables with versioned rows, transactions, the commit log, checkpoints
//! - [tuple]: compact binary encoding of values for the TCP protocol

pub(crate) mod engine;
pub(crate) mod tuple;
