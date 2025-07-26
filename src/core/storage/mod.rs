//! This is the module designed for take care of the lowest level part of the database: the disk
//! operations.
//! Here we have all the important data structures. Mainly the btree implementation, the cache
//! dealing as well as serialisation and deserialisation from memory.

pub(crate) mod btree;
pub(crate) mod page;
pub(crate) mod pagination;
pub(crate) mod tuple;

#[allow(unused)]
pub(crate) use crate::core::storage::btree::MemoryBuffer;
