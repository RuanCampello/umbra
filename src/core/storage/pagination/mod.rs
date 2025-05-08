//! Pagination implementation.
//!
//! This module contains all data structures that deal with IO.
//! It also has an in-memory cache implementation.

mod cache;
pub(in crate::core::storage) mod io;
pub(crate) mod pager;
