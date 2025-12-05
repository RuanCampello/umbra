//! Network related code.

mod pool;
mod protocol;
pub mod server;

pub use protocol::{deserialize, serialize, EncodingError, Response};
