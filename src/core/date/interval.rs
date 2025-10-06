//! Interval data type implemetation.

use std::{fmt::Display, ops::Add};

use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime, CUMULATIVE_DAYS};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

impl Interval {
    pub fn new(months: i32, days: i32, microseconds: i64) -> Self {
        Self {
            months,
            days,
            microseconds,
        }
    }

    pub fn from_days(days: i32) -> Self {
        Self {
            days,
            months: 0,
            microseconds: 0,
        }
    }
}

impl Add<Interval> for NaiveDateTime {
    type Output = Self;

    fn add(self, rhs: Interval) -> Self::Output {
        let date = self.date + rhs;
        let time = self.time + rhs;

        Self { date, time }
    }
}

impl Add<Interval> for NaiveDate {
    type Output = Self;

    fn add(self, rhs: Interval) -> Self::Output {
        let mut year = self.year() + rhs.months / 12;
        let mut month = self.month() as i32 + rhs.months % 12;
        let mut day = self.day() as i32 + rhs.days;

        if month > 12 {
            year += 1;
            month -= 12;
        } else if month <= 0 {
            year -= 1;
            month += 12;
        }

        loop {
            let day_in_month = Self::days_in_month(year, month as u16) as i32;

            match day > day_in_month {
                true => {
                    day -= day_in_month;
                    month += 1;

                    if month > 12 {
                        year += 1;
                        month -= 12;
                    }
                }
                _ => break,
            }
        }

        let mut ordinal = CUMULATIVE_DAYS[month as usize] as i32 + day;
        if month > 2 && Self::is_leap_year(year) {
            ordinal += 1;
        }

        Self::new(year, ordinal as u16)
    }
}

impl Add<Interval> for NaiveTime {
    type Output = Self;

    fn add(self, rhs: Interval) -> Self::Output {
        let secs = self.hour() as i64 * 3600
            + self.minute() as i64 * 60
            + self.second() as i64
            + rhs.microseconds / 1_000_000;

        let hour = (secs / 3600) % 24;
        let minute = (secs % 3600) / 60;
        let secs = secs % 60;

        Self::new_unchecked(hour as u8, minute as u8, secs as u8)
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.months != 0 {
            write!(f, "{} months ", self.months)?;
        }
        if self.days != 0 {
            write!(f, "{} days ", self.days)?;
        }
        if self.microseconds != 0 {
            write!(f, "{} microseconds", self.microseconds)?;
        }

        Ok(())
    }
}
