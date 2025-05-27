//! This module provides a simple logging API.

#![allow(static_mut_refs)]

use std::{
    fmt::Display,
    fs::{File, OpenOptions},
    io::{self, Write},
    panic,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Once,
    },
};

#[repr(usize)]
#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Level {
    /// Designates serious errors.
    Error = 1,
    /// Designates hazadarous situations.
    Warn,
    /// Designates useful information.
    Info,
    /// Designates lower priority information.
    Debug,
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum ParseError<'p> {
    InvalidString(&'p str),
}

static INIT: Once = Once::new();
static LOG_LEVEL: AtomicUsize = AtomicUsize::new(Level::Info as usize);
static mut LOG_FILE: Option<File> = None;

#[macro_export]
macro_rules! error {
    ($($args:tt)*) => {
        match $crate::core::log::log(
            $crate::core::log::Level::Error,
            &format!("{}:{} - {}", file!(), line!(), format_args!($($args)*))
        ) {
            Ok(_) => (),
            Err(e) => eprintln!("Failed to log: {}", e),
        }
    };
}

pub fn init(level: Level) -> io::Result<()> {
    const LOG_FILE_PATH: &str = "umbra.log";
    let mut result = Ok(());

    INIT.call_once(|| {
        let original_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            cleanup();
            original_hook(panic_info);
        }));

        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(LOG_FILE_PATH)
        {
            Ok(file) => unsafe {
                LOG_FILE = Some(file);
            },
            Err(e) => result = Err(e),
        };

        LOG_LEVEL.store(level as usize, Ordering::Release);
    });

    result
}

fn cleanup() {
    unsafe {
        if let Some(file) = LOG_FILE.take() {
            drop(file)
        }
    }
}

fn log(level: Level, message: &str) -> io::Result<()> {
    let global_level = LOG_LEVEL.load(Ordering::Acquire);
    if (level.clone() as usize) > global_level {
        return Ok(());
    }

    unsafe {
        match &mut LOG_FILE {
            Some(file) => {
                writeln!(file, "[{level:#?}]: {message}")?;
                file.flush()
            }
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "Attemped to write to logger not initialised",
            )),
        }
    }
}

impl<'p> TryFrom<&'p str> for Level {
    type Error = ParseError<'p>;

    fn try_from(s: &'p str) -> Result<Self, ParseError<'p>> {
        match s.to_lowercase().as_str() {
            "error" => Ok(Level::Error),
            "warn" => Ok(Level::Warn),
            "info" => Ok(Level::Info),
            "debug" => Ok(Level::Debug),
            _ => Err(ParseError::InvalidString(s)),
        }
    }
}

impl<'p> Display for ParseError<'p> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::InvalidString(s) => {
                writeln!(
                    f,
                    "Attemped to convert a string {s} that doesn't match an log level"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_using_initialised_log() {
        let err = log(super::Level::Error, "something really useful").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other)
    }
}
