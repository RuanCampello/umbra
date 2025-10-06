//! Interval data type implemetation.

use std::fmt::Display;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub(crate) struct Interval {
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
