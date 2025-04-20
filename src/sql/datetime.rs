//! The date-time's type implementation.
//!
//! Here, we're mostly based on [postgres](https://www.postgresql.org/docs/17/datatype-datetime.html)'s 17.4 specification, but simpler.

use crate::core::date::NaiveDateTime;
use std::time::SystemTime;

#[derive(Debug)]
pub struct Timestamp {
    utc: NaiveDateTime,
}

impl Timestamp {
    pub fn now() -> Self {
        todo!()
    }
}
