use db::DatabaseError;

mod core;
pub(crate) mod db;
pub(crate) mod os;
mod sql;
pub mod tcp;
mod vm;

pub type Result<T> = std::result::Result<T, DatabaseError>;
