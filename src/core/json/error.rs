#![allow(unused)]

use std::fmt::{self, Display};

#[derive(Clone, Debug, PartialEq)]
pub enum Error {
    Message {
        msg: String,
        location: Option<usize>,
    },
    Internal(String),
}

#[macro_export]
macro_rules! parse_error {
    ($($arg:tt)*) => {
        return Err($crate::core::json::error::Error::Message {
            msg: format!($($arg)*),
            location: None,
        })
    };
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Message {
            msg: value.to_string(),
            location: None,
        }
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(value: std::str::Utf8Error) -> Self {
        Self::Message {
            msg: value.to_string(),
            location: None,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message { ref msg, .. } => write!(f, "{msg}"),
            Self::Internal(ref msg) => write!(f, "Internal JSON error: {msg}"),
        }
    }
}
