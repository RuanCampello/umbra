//! SQL mathematical logics in Rust.
//! Sometimes, you need to implement `sqrt` from scratch, to make it work.

use crate::{db::SqlError, sql::statement::Value};

pub(super) fn abs(value: &Value) -> Result<Value, SqlError> {
    todo!()
}
