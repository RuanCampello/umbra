use error::Error as JsonError;

mod cache;
pub mod error;
mod jsonb;
mod ops;
mod path;

pub type Result<T> = std::result::Result<T, JsonError>;
