//! Concurrent hash-map implementation.
//!
//! Portions of this module are derived from the `papaya` crate
//! (https://github.com/ibraheemdev/papaya),
//! licensed under the MIT License.
//! Copyright © the papaya contributors.
//!
//! Hardly based and copied from [papaya](https://github.com/ibraheemdev/papaya) implementation

#![allow(unused)]

mod allocation;
mod map;
mod utils;

pub use map::{HashMap, HashMapBuilder};
