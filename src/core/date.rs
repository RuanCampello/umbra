//! A pure Rust date-handling library, providing essential date operations.  
//!  
//! This is a simplified reimplementation of [chrono](https://docs.rs/chrono/0.4.40/chrono/index.html)'s date functionality, designed for
//! essential date operations.
//!
//! Do not make those packed bit manipulations at home, this isn't meant to be as safe as possible, it is more an exploration than anything else.

use std::num::NonZeroI32;

#[derive(Debug, PartialEq)]
struct NaiveDateTime {
    date: NaiveDate,
    time: NaiveTime,
}

/// [ISO 8601](https://www.loc.gov/standards/datetime/iso-tc154-wg5_n0038_iso_wd_8601-1_2016-02-16.pdf)
/// calendar date without timezone compact date representation storing year and ordinal day
/// (1-366) in a single [`NonZeroI32`].
///
/// Uses bit-packing:
/// - **Upper 19 bits**: Year (`year << 13`)
/// - **Lower 13 bits**: Day of the year (ordinal, 1-366)
///
/// Enables efficient storage (four bytes) and operations while avoiding heap allocations.
/// [`NonZeroI32`] allows [`Option<NaiveDate>`] to be space-optimised.
#[derive(Debug, PartialEq)]
struct NaiveDate {
    yof: NonZeroI32,
}

/// Packed time representation, stored in exactly three bytes.
#[repr(transparent)]
#[derive(Debug, PartialEq)]
struct NaiveTime {
    hms: u24,
}

#[repr(transparent)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq)]
struct u24([u8; 3]);

#[allow(clippy::enum_variant_names, dead_code)]
#[derive(Debug, PartialEq)]
pub enum DateParseError {
    InvalidDateTime,
    InvalidDate,
    InvalidTime,
    InvalidYear,
    InvalidMonthDay,
    InvalidMonth,
    InvalidDay,
    InvalidHour,
    InvalidMinute,
    InvalidSecond,
}

type DateError<T> = Result<T, DateParseError>;

/// Cumulative day offsets for each month (non-leap year), starting at a 1-based index.
/// E.g. March 1st is the 59th day → CUMULATIVE_DAYS = 59.
const CUMULATIVE_DAYS: [u16; 13] = [0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334];

pub trait Parse: Sized {
    fn parse_str(date: &str) -> DateError<Self>;
}

impl Parse for NaiveDateTime {
    fn parse_str(date: &str) -> DateError<Self> {
        if date.len() != 19 || date.as_bytes()[10] != b'T' {
            return Err(DateParseError::InvalidDateTime);
        }

        let time = NaiveTime::parse_str(&date[11..])?;
        let date = NaiveDate::parse_str(&date[0..10])?;

        Ok(NaiveDateTime { date, time })
    }
}

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
    fn year(&self) -> i32 {
        self.yof.get() >> 13
    }

    #[inline(always)]
    fn ordinal(&self) -> u16 {
        (self.yof.get() & 0x1fff) as u16
    }

    #[inline(always)]
    fn is_leap_year(year: i32) -> bool {
        ((year & 3) == 0) && ((year % 25 != 0) || ((year & 15) == 0))
    }
}

impl Parse for NaiveDate {
    #[inline(always)]
    fn parse_str(date: &str) -> DateError<NaiveDate> {
        let bytes = date.as_bytes();
        if bytes.len() != 10 || bytes[4] != b'-' || bytes[7] != b'-' {
            return Err(DateParseError::InvalidDate);
        }

        let year = parse_four_digit(&bytes[0..4])?;
        let month = parse_two_digit(&bytes[5..7])?;
        let day = parse_two_digit(&bytes[8..10])?;

        if !(1..=12).contains(&month) {
            return Err(DateParseError::InvalidMonthDay);
        }

        let mut ordinal = CUMULATIVE_DAYS[month as usize] + day;
        if month > 2 && Self::is_leap_year(year) {
            ordinal += 1
        }

        Ok(NaiveDate::new(year, ordinal))
    }
}

impl NaiveTime {
    #[inline(always)]
    pub fn new(hour: u8, minute: u8, second: u8) -> DateError<Self> {
        if !(0..23).contains(&hour) {
            return Err(DateParseError::InvalidHour);
        };

        if !(0..59).contains(&minute) {
            return Err(DateParseError::InvalidMinute);
        }

        if !(0..59).contains(&second) {
            return Err(DateParseError::InvalidSecond);
        }

        // pack layout: [hour:5 bits <<17] | [minute:6 bits <<11] | [second:6 bits <<5]
        let val = ((hour as u32) << 17) | ((minute as u32) << 11) | ((second as u32) << 5);

        Ok(Self { hms: u24::new(val) })
    }
}

impl Parse for NaiveTime {
    #[inline(always)]
    fn parse_str(date: &str) -> DateError<Self> {
        let bytes = date.as_bytes();
        if bytes.len() != 8 || bytes[2] != b':' || bytes[5] != b':' {
            return Err(DateParseError::InvalidTime);
        };

        let hour = parse_two_digit(&bytes[0..2])? as u8;
        let minute = parse_two_digit(&bytes[3..5])? as u8;
        let second = parse_two_digit(&bytes[6..8])? as u8;

        NaiveTime::new(hour, minute, second)
    }
}

#[inline(always)]
fn parse_four_digit(bytes: &[u8]) -> DateError<i32> {
    if bytes.len() != 4 || !bytes.iter().all(|&b| b.is_ascii_digit()) {
        return Err(DateParseError::InvalidYear);
    }

    Ok(((bytes[0] - b'0') as i32) * 1000
        + ((bytes[1] - b'0') as i32) * 100
        + ((bytes[2] - b'0') as i32) * 10
        + ((bytes[3] - b'0') as i32))
}

#[inline(always)]
fn parse_two_digit(bytes: &[u8]) -> Result<u16, DateParseError> {
    if bytes.len() != 2 || !bytes.iter().all(|b| b.is_ascii_digit()) {
        return Err(DateParseError::InvalidMonthDay);
    }
    Ok((bytes[0] - b'0') as u16 * 10 + (bytes[1] - b'0') as u16)
}

impl u24 {
    #[inline(always)]
    const fn new(val: u32) -> Self {
        let low = (val & 0xFF) as u8;
        let mid = ((val >> 8) & 0xFF) as u8;
        let high = ((val >> 16) & 0xFF) as u8;
        u24([low, mid, high])
    }

    #[inline(always)]
    const fn get(&self) -> u32 {
        let [low, mid, high] = self.0;
        (low as u32) | ((mid as u32) << 8) | ((high as u32) << 16)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_date_parse() {
        let str = "2020-01-01";
        let date = NaiveDate::parse_str(str).unwrap();

        assert_eq!(date.year(), 2020);
        assert_eq!(date.ordinal(), 1);
    }

    #[test]
    fn test_date_parse_with_leap() {
        let leap = "2004-06-27";
        let date = NaiveDate::parse_str(leap).unwrap();

        assert_eq!(date.year(), 2004);
        assert_eq!(date.ordinal(), 179);

        let not_leap = "2005-06-27";
        let date = NaiveDate::parse_str(not_leap).unwrap();

        assert_eq!(date.year(), 2005);
        assert_eq!(date.ordinal(), 178);
    }

    #[test]
    fn test_size_and_align_of_time() {
        assert_eq!(size_of::<NaiveTime>(), 3);
        assert_eq!(size_of::<u24>(), 3);
        debug_assert_eq!(align_of::<NaiveTime>(), 1, "NaiveTime must be packed");
        debug_assert_eq!(align_of::<u24>(), 1, "u24 must be packed");
    }

    #[test]
    fn test_no_ub_u24() {
        let val = 0x00_FF_0A;
        let u_24 = u24::new(val);
        assert_eq!(u_24.get(), val);

        let val_b = u_24.0;
        let reconstructed = u24(val_b);
        assert_eq!(reconstructed.get(), val);
    }
}
