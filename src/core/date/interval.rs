//! Interval data type implemetation.

use std::{
    fmt::Display,
    ops::{Add, Sub},
};

use crate::{
    core::date::{NaiveDate, NaiveDateTime, NaiveTime},
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
        const SECS_PER_MINUTE: i64 = 60;
        const SECS_PER_HOUR: i64 = 3600;
        const SECS_PER_DAY: i64 = 86_400;
        const MICROS_PER_SEC: i64 = 1_000_000;

        let date = self.date + rhs;

        let seconds = self.time.hour() as i64 * SECS_PER_HOUR
            + self.time.minute() as i64 * SECS_PER_MINUTE
            + self.time.second() as i64;
        let interval = rhs.microseconds / MICROS_PER_SEC;
        let total_secs = seconds + interval;

        let overflow = match total_secs >= 0 {
            true => total_secs / SECS_PER_DAY,
            _ => (total_secs - (SECS_PER_DAY - 1)) / SECS_PER_DAY,
        };

        let secs = match total_secs >= 0 {
            true => total_secs % SECS_PER_DAY,
            _ => ((total_secs % SECS_PER_DAY) / SECS_PER_DAY) % SECS_PER_DAY,
        };

        let hour = (secs / SECS_PER_HOUR) as u8;
        let minute = ((secs % SECS_PER_HOUR) / SECS_PER_MINUTE) as u8;
        let second = (secs % SECS_PER_MINUTE) as u8;
        let time = NaiveTime::new_unchecked(hour, minute, second);

        let date = match overflow != 0 {
            true => date + Interval::from_days(overflow as i32),
            _ => date,
        };

        Self { date, time }
    }
}

impl Add<Interval> for NaiveDate {
    type Output = Self;

    fn add(self, rhs: Interval) -> Self::Output {
        let total_months = self.year() * 12 + (self.month() as i32 - 1) + rhs.months;
        let mut year = total_months / 12;
        let mut month = (total_months % 12) + 1;

        if month <= 0 {
            year -= 1;
            month += 12;
        }

        let max_days_in_target_month = Self::days_in_month(year, month as u16);
        let mut day = self.day().min(max_days_in_target_month) as i32;
        day += rhs.days;

        let mut current_year = year;
        let mut current_month = month as u16;

        while day > Self::days_in_month(current_year, current_month) as i32 {
            day -= Self::days_in_month(current_year, current_month) as i32;
            current_month += 1;
            if current_month > 12 {
                current_year += 1;
                current_month = 1;
            }
        }

        while day <= 0 {
            current_month -= 1;
            if current_month == 0 {
                current_year -= 1;
                current_month = 12;
            }
            day += Self::days_in_month(current_year, current_month) as i32;
        }

        let ordinal = Self::cumul_days(current_year)[current_month as usize] as i32 + day;
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
