//! Interval data type implemetation.

use std::{
    fmt::Display,
    ops::{Add, Sub},
    str::FromStr,
};

use crate::{
    core::date::{ExtractError, NaiveDate, NaiveDateTime, NaiveTime, SECS_IN_DAY},
    sql::statement::{Temporal, Type},
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub microseconds: i64,
}

#[derive(Debug, PartialEq)]
pub enum IntervalParseError {
    InvalidFormat(String),
    InvalidNumber(i64),
    InvalidUnit(String),
}

const MICROSECS_IN_HOUR: i64 = 3_600_000_000;
const MICROSECS_IN_MIN: i64 = 60_000_000;
const MICROSECS_IN_SECS: i64 = 1_000_000;

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

    pub const fn hours(&self) -> i64 {
        self.microseconds / MICROSECS_IN_HOUR
    }

    pub const fn minutes(&self) -> i64 {
        (self.microseconds % MICROSECS_IN_HOUR) / MICROSECS_IN_MIN
    }

    pub const fn seconds(&self) -> i64 {
        (self.microseconds % MICROSECS_IN_MIN) / MICROSECS_IN_SECS
    }

    pub const fn microseconds(&self) -> i64 {
        self.microseconds % MICROSECS_IN_SECS
    }

    pub const fn total_seconds(&self) -> i64 {
        const SECONDS_IN_MONTH: i64 = 30 * SECS_IN_DAY;

        let secs_in_day = self.days as i64 * SECS_IN_DAY;
        let secs_in_month = self.months as i64 * SECONDS_IN_MONTH;
        let secs_in_micro = self.microseconds / MICROSECS_IN_SECS;

        secs_in_day + secs_in_month + secs_in_micro
    }
}

impl super::Extract for Interval {
    fn extract(&self, kind: super::ExtractKind) -> Result<f64, super::ExtractError> {
        use super::ExtractKind as Kind;

        match kind {
            Kind::Year => Ok((self.months / 12) as _),
            Kind::Month => Ok((self.months) as _),
            Kind::Day => Ok((self.days) as _),
            Kind::Hour => Ok(self.hours() as _),
            Kind::Minute => Ok(self.minutes() as _),
            Kind::Second => Ok(self.seconds() as _),
            Kind::Epoch => Ok(self.total_seconds() as _),
            _ => Err(ExtractError::UnsupportedUnitForType {
                unit: kind,
                r#type: Type::Interval,
            }),
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

        let date = self.date + rhs;

        let seconds = self.time.hour() as i64 * SECS_PER_HOUR
            + self.time.minute() as i64 * SECS_PER_MINUTE
            + self.time.second() as i64;
        let interval = rhs.microseconds / MICROSECS_IN_SECS;
        let total_secs = seconds + interval;

        let overflow = match total_secs >= 0 {
            true => total_secs / SECS_IN_DAY,
            _ => (total_secs - (SECS_IN_DAY - 1)) / SECS_IN_DAY,
        };

        let secs = ((total_secs % SECS_IN_DAY) + SECS_IN_DAY) % SECS_IN_DAY;

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

impl FromStr for Interval {
    type Err = IntervalParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut months = 0;
        let mut days = 0;
        let mut microseconds = 0;

        let parts = value.split_whitespace().collect::<Vec<_>>();

        if parts.len() % 2 != 0 {
            return Err(IntervalParseError::InvalidFormat(value.to_string()));
        }

        for chunk in parts.chunks_exact(2) {
            let num: i64 = match chunk[0].parse() {
                Ok(num) => num,
                Err(_) => {
                    let invalid_num = chunk[0].parse::<i64>().unwrap_or(0);
                    return Err(IntervalParseError::InvalidNumber(invalid_num));
                }
            };

            let unit = chunk[1].to_lowercase();
            match unit.as_str() {
                "year" | "years" => months += (num * 12) as i32,
                "month" | "months" => months += num as i32,
                "week" | "weeks" => days += (num * 7) as i32,
                "day" | "days" => days += num as i32,
                "hour" | "hours" => microseconds += num * MICROSECS_IN_HOUR,
                "minute" | "minutes" => microseconds += num * MICROSECS_IN_MIN,
                "second" | "seconds" => microseconds += num * MICROSECS_IN_SECS,
                "microsecond" | "microseconds" => microseconds += num,
                _ => return Err(IntervalParseError::InvalidUnit(unit)),
            }
        }

        Ok(Interval::new(months, days, microseconds))
    }
}

impl Display for Interval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let is_negative = self.months < 0 || self.days < 0 || self.microseconds < 0;
        if is_negative {
            write!(f, "-")?;
        }

        let months = self.months.abs();
        let days = self.days.abs();
        let micros = self.microseconds.abs();

        if months == 0 && days == 0 && micros == 0 {
            write!(f, "00:00:00")?;
        }

        if months > 0 {
            write!(f, "{} mon{}", months, if months == 1 { "" } else { "s" })?;
        }
        if days > 0 {
            write!(f, "{} day{}", days, if days == 1 { "" } else { "s" })?;
        }

        write!(
            f,
            "{:02}:{:02}:{:02}",
            self.hours(),
            self.minutes(),
            self.seconds()
        )?;

        if self.microseconds() > 0 {
            write!(f, ".{:06}", self.microseconds())?;
        }

        Ok(())
    }
}

impl Display for IntervalParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidFormat(format) => write!(f, "Invalid interval format: {format}"),
            Self::InvalidNumber(num) => write!(f, "Invalid number in interval: {num}"),
            Self::InvalidUnit(unit) => write!(f, "Invalid interval unit: {unit}"),
        }
    }
}
