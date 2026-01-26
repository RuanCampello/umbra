//! Pagination implementation.
//!
//! This module contains all data structures that deal with IO.
//! It also has an in-memory cache implementation.

#![allow(unused)]

mod cache;
pub(crate) mod io;
pub(crate) mod pager;

pub(crate) use cache::Cache;
pub(in crate::storage::pagination) use pager::DEFAULT_PAGE_SIZE;
