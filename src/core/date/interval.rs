//! Interval data type implemetation.

use std::{
    fmt::Display,
    ops::{Add, Sub},
};

use crate::{
    core::date::{NaiveDate, NaiveDateTime, NaiveTime, CUMULATIVE_DAYS},
    sql::statement::Temporal,
};

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

impl Add<Interval> for Temporal {
    type Output = Self;

    fn add(self, rhs: Interval) -> Self::Output {
        match self {
            Self::Date(date) => Self::Date(date + rhs),
            Self::Time(time) => Self::Time(time + rhs),
            Self::DateTime(datetime) => Self::DateTime(datetime + rhs),
        }
    }
}

impl Sub<Interval> for Temporal {
    type Output = Self;

    fn sub(self, rhs: Interval) -> Self::Output {
        self + (-rhs)
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
        // Step 1: Add months to year/month
        let mut year = self.year() + rhs.months / 12;
        let mut month = self.month() as i32 + rhs.months % 12;

        // Step 2: Adjust for month overflow/underflow
        if month > 12 {
            year += 1;
            month -= 12;
        } else if month <= 0 {
            year -= 1;
            month += 12;
        }

        // Step 3: Clamp day to max days in target month (e.g., Jan 31 + 1 month = Feb 29 in leap year)
        let max_days_in_target_month = Self::days_in_month(year, month as u16);
        let mut day = self.day().min(max_days_in_target_month) as i32;

        // Step 4: Add interval days
        day += rhs.days;

        // Step 5: Handle day overflow/underflow
        let mut current_year = year;
        let mut current_month = month as u16;

        // Handle day overflow (day > max days in month)
        while day > Self::days_in_month(current_year, current_month) as i32 {
            day -= Self::days_in_month(current_year, current_month) as i32;
            current_month += 1;
            if current_month > 12 {
                current_year += 1;
                current_month = 1;
            }
        }

        // Handle day underflow (day <= 0)
        while day <= 0 {
            current_month -= 1;
            if current_month == 0 {
                current_year -= 1;
                current_month = 12;
            }
            day += Self::days_in_month(current_year, current_month) as i32;
        }

        // Step 6: Calculate final ordinal (same logic as parse_str)
        let mut ordinal = CUMULATIVE_DAYS[current_month as usize] as i32 + day;
        if current_month > 2 && Self::is_leap_year(current_year) {
            ordinal += 1;
        }

        Self::new(current_year, ordinal as u16)
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
