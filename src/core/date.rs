//! A pure Rust date-handling library, providing essential date operations.  
//!  
//! This is a simplified reimplementation of [chrono](https://docs.rs/chrono/0.4.40/chrono/index.html)'s date functionality, designed to
//! essential date operations.

use std::num::NonZeroI32;

/// [ISO 8601](https://www.loc.gov/standards/datetime/iso-tc154-wg5_n0038_iso_wd_8601-1_2016-02-16.pdf) date and time without timezone.
pub(crate) struct NaiveDateTime {
    date: NaiveDate,
    time: NaiveTime,
}

/// [ISO 8601](https://www.loc.gov/standards/datetime/iso-tc154-wg5_n0038_iso_wd_8601-1_2016-02-16.pdf) calendar date without timezone compact date representation storing year and ordinal day (1-366) in a single `NonZeroI32`.
///
/// Uses bit-packing:
/// - **Upper 19 bits**: Year (`year << 13`)
/// - **Lower 13 bits**: Day of the year (ordinal, 1-366)
///
/// Enables efficient storage (four bytes) and operations while avoiding heap allocations.
/// `NonZeroI32` allows `Option<NaiveDate>` to be space-optimised.
#[derive(Debug, PartialEq)]
struct NaiveDate {
    yof: NonZeroI32,
}

struct NaiveTime {}

#[derive(Debug, PartialEq)]
enum DateParseError {
    InvalidDate,
    InvalidTime,
    InvalidYear,
    InvalidMonthDay,
    InvalidMonth,
    InvalidDay,
}

type DateError<T> = Result<T, DateParseError>;

/// Cumulative day offsets for each month (non-leap year), starting at a 1-based index.
/// E.g. March 1st is the 59th day → CUMULATIVE_DAYS = 59.
const CUMULATIVE_DAYS: [u16; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

impl NaiveDate {
    #[inline(always)]
    pub fn new(year: i32, ordinal: u16) -> Self {
        debug_assert!((1..=366).contains(&ordinal));
        let packed = (year << 13) | (ordinal as i32);
        debug_assert!(packed != 0);

        Self {
            yof: unsafe { NonZeroI32::new_unchecked(packed) },
        }
    }

    #[inline(always)]
    pub fn parse_str(date: &str) -> DateError<NaiveDate> {
        let bytes = date.as_bytes();
        if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
            return Err(DateParseError::InvalidDate);
        }

        let year = Self::parse_year(&bytes[0..4])?;
        let month = Self::parse_month_or_day(&bytes[5..7])?;
        let day = Self::parse_month_or_day(&bytes[8..10])?;

        if !(1..=12).contains(&month) {
            return Err(DateParseError::InvalidMonthDay);
        }

        let mut ordinal = CUMULATIVE_DAYS[month as usize] + day;
        if month > 2 && Self::is_leap_year(year) {
            ordinal += 1
        }

        Ok(NaiveDate::new(year, ordinal))
    }

    #[inline(always)]
    fn year(&self) -> i32 {
        self.yof.get() >> 13
    }

    #[inline(always)]
    fn ordinal(&self) -> u16 {
        (self.yof.get() & 0x1fff) as u16
    }

    #[inline(always)]
    fn parse_year(bytes: &[u8]) -> DateError<i32> {
        if bytes.len() != 4 || !bytes.iter().all(|&b| b.is_ascii_digit()) {
            return Err(DateParseError::InvalidYear);
        }

        Ok(((bytes[0] - b'0') as i32) * 1000
            + ((bytes[1] - b'0') as i32) * 100
            + ((bytes[2] - b'0') as i32) * 10
            + ((bytes[3] - b'0') as i32))
    }

    #[inline(always)]
    fn parse_month_or_day(bytes: &[u8]) -> Result<u16, DateParseError> {
        if bytes.len() != 2 || !bytes.iter().all(|b| b.is_ascii_digit()) {
            return Err(DateParseError::InvalidMonthDay);
        }
        Ok((bytes[0] - b'0') as u16 * 10 + (bytes[1] - b'0') as u16)
    }

    #[inline(always)]
    fn is_leap_year(year: i32) -> bool {
        ((year & 3) == 0) && ((year % 25 != 0) || ((year & 15) == 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let str = "2020-01-01";
        let date = NaiveDate::parse_str(str).unwrap();

        assert_eq!(date.year(), 2020);
        assert_eq!(date.ordinal(), 1);
    }
}
