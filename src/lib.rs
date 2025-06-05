use db::DatabaseError;

mod core;
pub mod db;
pub(crate) mod os;
pub mod sql;
pub mod tcp;
mod vm;

pub type Result<T> = std::result::Result<T, DatabaseError>;
