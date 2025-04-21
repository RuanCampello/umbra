//! Pagination implementation.
//!
//! This module contains all data structures that deal with IO.
//! It also has an in-memory cache implementation.

mod cache;
pub(in crate::core) mod io;
pub(in crate::core) mod pager;
