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
            Self::Quarter => Some(((date.month() as f64 - 1.0) / 3.0 + 1.0).floor()),
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

#[cfg(test)]
mod tests {
    use crate::core::date::{functions::ExtractKind, NaiveDate, NaiveDateTime, NaiveTime, Parse};

    #[test]
    fn test_from_date() {
        let date = NaiveDate::parse_str("2023-12-25").unwrap();

        assert_eq!(ExtractKind::Year.from_date(&date), Some(2023.0));
        assert_eq!(ExtractKind::Month.from_date(&date), Some(12.0));
        assert_eq!(ExtractKind::Day.from_date(&date), Some(25.0));
        assert_eq!(ExtractKind::Quarter.from_date(&date), Some(4.0));
        assert_eq!(ExtractKind::Century.from_date(&date), Some(21.0));
        assert_eq!(ExtractKind::Decade.from_date(&date), Some(202.0));

        assert_eq!(ExtractKind::Hour.from_date(&date), None);
        assert_eq!(ExtractKind::Minute.from_date(&date), None);
        assert_eq!(ExtractKind::Second.from_date(&date), None);
    }

    #[test]
    fn test_from_time() {
        let time = NaiveTime::parse_str("13:30:15").unwrap();

        assert_eq!(ExtractKind::Hour.from_time(&time), Some(13.0));
        assert_eq!(ExtractKind::Minute.from_time(&time), Some(30.0));
        assert_eq!(ExtractKind::Second.from_time(&time), Some(15.0));

        assert_eq!(ExtractKind::Year.from_time(&time), None);
        assert_eq!(ExtractKind::Month.from_time(&time), None);
        assert_eq!(ExtractKind::Day.from_time(&time), None);
        assert_eq!(ExtractKind::Quarter.from_time(&time), None);
    }

    #[test]
    fn test_from_datetime() {
        let date = NaiveDate::parse_str("2002-02-09").unwrap();
        let time = NaiveTime::parse_str("00:37:14").unwrap();
        let datetime = NaiveDateTime { date, time };

        assert_eq!(ExtractKind::Year.from_timestamp(&datetime), Some(2002.0));
        assert_eq!(ExtractKind::Month.from_timestamp(&datetime), Some(02.0));
        assert_eq!(ExtractKind::Quarter.from_timestamp(&datetime), Some(1.0));
        assert_eq!(ExtractKind::Day.from_timestamp(&datetime), Some(9.0));

        assert_eq!(ExtractKind::Hour.from_timestamp(&datetime), Some(00.0));
        assert_eq!(ExtractKind::Minute.from_timestamp(&datetime), Some(37.0));
        assert_eq!(ExtractKind::Second.from_timestamp(&datetime), Some(14.0));
    }
}

