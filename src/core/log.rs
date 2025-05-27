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

pub const LOG_FILE_PATH: &str = "umbra.log";

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

impl Display for Level {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            Self::Error => "ERROR",
            Self::Warn => "WARN",
            Self::Info => "INFO",
            Self::Debug => "DEBUG",
        };

        f.write_str(string)
    }
}

pub fn init(level: Level) -> io::Result<()> {
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
    use std::io::{BufRead, BufReader};

    use super::*;

    fn assert_log_content(level: Level, content: &str) -> io::Result<Vec<String>> {
        let level = level.to_string();
        let level = format!("[{}{}]", &level[..1], level[1..].to_lowercase().as_str());

        let log_file = File::open(LOG_FILE_PATH)?;

        let reader = BufReader::new(log_file);

        let lines = reader
            .lines()
            .inspect(|res| match res {
                Ok(line) => {
                    assert!(line.contains(&level));
                    assert!(line.contains(content));
                }
                Err(err) => panic!("error reading line: {}", err),
            })
            .collect();

        // ensure clean state for tests
        File::create(LOG_FILE_PATH)?;

        lines
    }

    #[test]
    fn test_using_initialised_log() {
        let err = log(Level::Error, "something really useful").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other)
    }

    #[test]
    fn test_error_content() {
        init(Level::Debug).unwrap();

        let content = "some really constructive error log";
        error!("{}", content);

        assert_log_content(Level::Error, content).unwrap();
    }
}
