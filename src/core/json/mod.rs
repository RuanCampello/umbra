use error::Error as JsonError;

pub mod error;
mod path;

pub type Result<T> = std::result::Result<T, JsonError>;

