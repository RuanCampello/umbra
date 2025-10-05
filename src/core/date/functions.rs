//! Date related functions.

#![allow(dead_code)]

use std::fmt::Display;

use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime};
use crate::sql::statement::Temporal;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ExtractKind {
    Century,
    Day,
    Decade,
    Hour,
    Minute,
    Second,
    Month,
    Quarter,
    Year,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ExtractError {
    UnsupportedUnitForType { unit: ExtractKind, r#type: Temporal },
}

pub trait Extract {
    fn extract(&self, kind: ExtractKind) -> Result<f64, ExtractError>;
}

impl Extract for NaiveDateTime {
    fn extract(&self, kind: ExtractKind) -> Result<f64, ExtractError> {
        kind.from_timestamp(self)
            .ok_or(ExtractError::UnsupportedUnitForType {
                unit: kind,
                r#type: Temporal::DateTime(*self),
            })
    }
}

impl Extract for NaiveDate {
    fn extract(&self, kind: ExtractKind) -> Result<f64, ExtractError> {
        kind.from_date(self)
            .ok_or(ExtractError::UnsupportedUnitForType {
                unit: kind,
                r#type: Temporal::Date(*self),
            })
    }
}

impl Extract for NaiveTime {
    fn extract(&self, kind: ExtractKind) -> Result<f64, ExtractError> {
        kind.from_time(self)
            .ok_or(ExtractError::UnsupportedUnitForType {
                unit: kind,
                r#type: Temporal::Time(*self),
            })
    }
}

impl ExtractKind {
    fn from_timestamp(&self, timestamp: &NaiveDateTime) -> Option<f64> {
        self.from_date(&timestamp.date)
            .or(self.from_time(&timestamp.time))
    }

    fn from_date(&self, date: &NaiveDate) -> Option<f64> {
        match self {
            Self::Century => Some(((date.year() as f64 - 1.0) / 100.0).floor() + 1.0),
            Self::Decade => Some((date.year() as f64 / 10.0).floor()),
            Self::Year => Some(date.year() as _),
            Self::Quarter => Some((date.month() as f64 - 1.0) / 3.0 + 1.0),
            Self::Month => Some(date.month() as _),
            Self::Day => Some(date.day() as _),
            Self::Hour | Self::Minute | Self::Second => None,
        }
    }

    fn from_time(&self, time: &NaiveTime) -> Option<f64> {
        match self {
            Self::Hour => Some(time.hour() as f64),
            Self::Minute => Some(time.minute() as f64),
            Self::Second => Some(time.second() as f64),
            Self::Year | Self::Month | Self::Day | Self::Quarter | Self::Century | Self::Decade => {
                None
            }
        }
    }
}

impl Display for ExtractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedUnitForType { unit, r#type } => {
                write!(f, "Unit {unit} is not supported for {type}")
            }
        }
    }
}

impl Display for ExtractKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Century => "century",
            Self::Day => "day",
            Self::Decade => "decade",
            Self::Hour => "hour",
            Self::Minute => "minute",
            Self::Second => "second",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        };
        write!(f, "{}", s)
    }
}

