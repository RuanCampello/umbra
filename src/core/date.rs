//! A pure Rust date-handling library, providing essential date operations.  
//!  
//! This is a simplified reimplementation of [chrono](https://docs.rs/chrono/0.4.40/chrono/index.html)'s date functionality, designed for
//! essential date operations.
//!
//! Do not make those packed bit manipulations at home, this isn't meant to be as safe as possible, it is more an exploration than anything else.

use std::fmt::{Display, Formatter};
use std::num::NonZeroI32;

/// A combined date and time representation without timezone information.
///
/// This struct combines [`NaiveDate`] (4 bytes)
/// and [`NaiveTime`] (3 bytes) into a single datetime value,
/// following the ISO 8601 format (e.g. "2023-01-15T14:30:00").
///
/// # Memory Layout
/// - `date`: 4 bytes (packed [`NaiveDate`] with year and ordinal day)
/// - `time`: 3 bytes (packed [`NaiveTime`] with hours, minutes, seconds)
/// - Total size: 8 bytes (due to alignment padding after the 3-byte time)
///
/// # Features
/// - Parsing from ISO 8601 formatted strings ("YYYY-MM-DDTHH:MM:SS")
/// - Conversion to Unix timestamp (seconds since 1970-01-01 00:00:00 UTC)
/// - Display formatting following ISO 8601 conventions
///
/// # Notes
/// While the raw data requires only 7 bytes, the struct occupies 8 bytes due to:
/// 1. Rust's memory alignment requirements
/// 2. The following field (`time: NaiveTime`) is 3 bytes
/// 3. Natural padding added by the compiler for optimal access
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct NaiveDateTime {
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
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct NaiveDate {
    yof: NonZeroI32,
}

/// Packed time representation, stored in exactly three bytes.
#[repr(transparent)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub struct NaiveTime {
    hms: u24,
}

#[repr(transparent)]
#[allow(non_camel_case_types)]
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
struct u24([u8; 3]);

#[allow(clippy::enum_variant_names, dead_code)]
#[derive(Debug, PartialEq, Clone)]
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
    fn timestamp(&self) -> i64;
}

/// This serializes the date types into a compact binary format.
pub trait Serialize {
    fn serialize(&self, buff: &mut Vec<u8>);
}

impl Parse for NaiveDateTime {
    fn parse_str(date: &str) -> DateError<Self> {
        if date.len() != 19 {
            return Err(DateParseError::InvalidDateTime);
        }

        // check that byte 10 is either 'T' or ' '
        match date.as_bytes()[10] {
            b'T' | b' ' => { /* ok */ }
            _ => return Err(DateParseError::InvalidDateTime),
        }

        let time = NaiveTime::parse_str(&date[11..])?;
        let date = NaiveDate::parse_str(&date[0..10])?;

        Ok(NaiveDateTime { date, time })
    }

    #[inline(always)]
    fn timestamp(&self) -> i64 {
        self.date.timestamp() + self.time.timestamp()
    }
}

impl Serialize for NaiveDateTime {
    fn serialize(&self, buff: &mut Vec<u8>) {
        self.date.serialize(buff);
        self.time.serialize(buff);
    }
}

impl TryFrom<([u8; 4], [u8; 3])> for NaiveDateTime {
    type Error = std::io::Error;
    fn try_from((date_bytes, time_bytes): ([u8; 4], [u8; 3])) -> Result<Self, Self::Error> {
        Ok(Self {
            date: NaiveDate::try_from(date_bytes)?,
            time: NaiveTime::from(time_bytes),
        })
    }
}

impl From<NaiveDateTime> for NaiveDate {
    fn from(value: NaiveDateTime) -> Self {
        value.date
    }
}

impl From<NaiveDateTime> for NaiveTime {
    fn from(value: NaiveDateTime) -> Self {
        value.time
    }
}

impl Display for NaiveDateTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{} {}", self.date, self.time))
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
    fn days_in_month(year: i32, month: u16) -> u16 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            #[rustfmt::skip]
            2 => if NaiveDate::is_leap_year(year) { 29 } else { 28 },
            _ => 0,
        }
    }

    #[inline(always)]
    fn year(&self) -> i32 {
        self.yof.get() >> 13
    }

    #[inline(always)]
    fn month(&self) -> u16 {
        let ordinal = self.ordinal();

        (1..=12)
            .rev()
            .find(|&m| self.cumul_days()[m] < ordinal)
            .unwrap_or(1) as u16
    }

    #[inline(always)]
    fn day(&self) -> u16 {
        let ordinal = self.ordinal();
        let m = self.month();

        ordinal - self.cumul_days()[m as usize]
    }

    #[inline(always)]
    fn ordinal(&self) -> u16 {
        (self.yof.get() & 0x1fff) as u16
    }

    fn cumul_days(&self) -> [u16; 13] {
        const CUMULATIVE_DAYS_LEAP: [u16; 13] =
            [0, 0, 31, 60, 91, 121, 152, 182, 213, 244, 275, 306, 336];

        match NaiveDate::is_leap_year(self.year()) {
            true => CUMULATIVE_DAYS_LEAP,
            false => CUMULATIVE_DAYS,
        }
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

        if day == 0 || day > 31 {
            return Err(DateParseError::InvalidDay);
        }

        if !(1..=12).contains(&month) {
            return Err(DateParseError::InvalidMonth);
        }

        if Self::days_in_month(year, month) < day {
            return Err(DateParseError::InvalidMonthDay);
        }

        let mut ordinal = CUMULATIVE_DAYS[month as usize] + day;
        if month > 2 && Self::is_leap_year(year) {
            ordinal += 1
        }

        Ok(NaiveDate::new(year, ordinal))
    }

    #[inline(always)]
    fn timestamp(&self) -> i64 {
        let mut days = 0i64;

        let year = self.year();
        let ordinal = self.ordinal() as i64;

        match year >= 1970 {
            true => {
                (1970..year).for_each(|y| days += if Self::is_leap_year(y) { 366 } else { 365 });

                days += ordinal - 1
            }
            false => {
                (year..1970).for_each(|y| days -= if Self::is_leap_year(y) { 366 } else { 365 });

                days += ordinal - 1;
            }
        }

        days * 86400
    }
}

impl Serialize for NaiveDate {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.extend_from_slice(&self.yof.get().to_le_bytes());
    }
}

impl TryFrom<[u8; 4]> for NaiveDate {
    type Error = std::io::Error;
    fn try_from(bytes: [u8; 4]) -> Result<Self, Self::Error> {
        let yof = i32::from_le_bytes(bytes);
        let date = NonZeroI32::new(yof).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidFilename,
                "Received zero value in date",
            )
        })?;

        Ok(Self { yof: date })
    }
}

impl Display for NaiveDate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{:02}-{:02}", self.year(), self.month(), self.day())
    }
}

impl NaiveTime {
    #[inline(always)]
    pub fn new(hour: u8, minute: u8, second: u8) -> DateError<Self> {
        if !(0..=23).contains(&hour) {
            return Err(DateParseError::InvalidHour);
        };

        if !(0..=59).contains(&minute) {
            return Err(DateParseError::InvalidMinute);
        }

        if !(0..=59).contains(&second) {
            return Err(DateParseError::InvalidSecond);
        }

        // pack layout: [hour:5 bits <<17] | [minute:6 bits <<11] | [second:6 bits <<5]
        let val = ((hour as u32) << 12) | ((minute as u32) << 6) | (second as u32);

        Ok(Self { hms: u24::new(val) })
    }

    fn hour(&self) -> u8 {
        ((self.hms.get() >> 12) & 0x1f) as u8
    }

    fn minute(&self) -> u8 {
        ((self.hms.get() >> 6) & 0x3f) as u8
    }

    fn second(&self) -> u8 {
        (self.hms.get() & 0x3f) as u8
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

    #[inline(always)]
    fn timestamp(&self) -> i64 {
        (self.hour() as i64) * 3600 + (self.minute() as i64) * 60 + (self.second() as i64)
    }
}

impl Serialize for NaiveTime {
    fn serialize(&self, buff: &mut Vec<u8>) {
        buff.extend_from_slice(&self.hms.0);
    }
}

impl From<[u8; 3]> for NaiveTime {
    fn from(bytes: [u8; 3]) -> Self {
        Self { hms: u24(bytes) }
    }
}

impl Display for NaiveTime {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:02}:{:02}:{:02}",
            self.hour(),
            self.minute(),
            self.second()
        )
    }
}

impl Display for DateParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            DateParseError::InvalidDateTime => "invalid date-time format",
            DateParseError::InvalidDate => "invalid date format",
            DateParseError::InvalidTime => "invalid time format",
            DateParseError::InvalidYear => "invalid year value",
            DateParseError::InvalidMonthDay => "invalid month/day combination",
            DateParseError::InvalidMonth => "invalid month value (must be 1-12)",
            DateParseError::InvalidDay => "invalid day value",
            DateParseError::InvalidHour => "invalid hour value (must be 0-23)",
            DateParseError::InvalidMinute => "invalid minute value (must be 0-59)",
            DateParseError::InvalidSecond => "invalid second value (must be 0-59)",
        };
        write!(f, "{}", message)
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
        assert_eq!(date.month(), 6);
        assert_eq!(date.day(), 27);
        assert_eq!(date.ordinal(), 179);

        let not_leap = "2005-06-27";
        let date = NaiveDate::parse_str(not_leap).unwrap();

        assert_eq!(date.year(), 2005);
        assert_eq!(date.month(), 6);
        assert_eq!(date.day(), 27);
        assert_eq!(date.ordinal(), 178);

        let leap = "2008-02-29";
        let date = NaiveDate::parse_str(leap).unwrap();

        assert_eq!(date.year(), 2008);
        assert_eq!(date.month(), 2);
        assert_eq!(date.day(), 29);
        assert_eq!(date.ordinal(), 60);
    }

    #[test]
    fn test_size_and_align_of() {
        assert_eq!(size_of::<NaiveTime>(), 3);
        assert_eq!(size_of::<u24>(), 3);

        assert_eq!(size_of::<NaiveDate>(), 4);
        assert_eq!(align_of::<NaiveDate>(), 4);

        assert_eq!(size_of::<NaiveDateTime>(), 8);
        assert_eq!(align_of::<NaiveDateTime>(), 4);
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

    #[test]
    fn test_parse_time() {
        let str = "12:34:56";
        let time = NaiveTime::parse_str(str).unwrap();

        assert_eq!(time, NaiveTime::new(12, 34, 56).unwrap());
        assert_eq!(time.hour(), 12);
        assert_eq!(time.minute(), 34);
        assert_eq!(time.second(), 56);
    }

    #[test]
    fn test_datetime_parse_invalid() {
        assert!(NaiveDateTime::parse_str("2020-02-30T12:00:00").is_err()); // invalid date
        assert!(NaiveDateTime::parse_str("2020-02-28T25:00:00").is_err()); // invalid time
        assert!(NaiveDateTime::parse_str("bad-string").is_err()); // totally invalid
        assert!(NaiveDateTime::parse_str("2020-02-28X12:00:00").is_err()); // wrong delimiter
    }

    #[test]
    fn test_datetime_timestamp() {
        let dt = NaiveDateTime::parse_str("1970-01-01T00:00:00").unwrap();
        assert_eq!(dt.timestamp(), 0);

        let dt = NaiveDateTime::parse_str("2023-01-15T14:30:00").unwrap();
        assert_eq!(dt.to_string(), "2023-01-15 14:30:00");
        assert_eq!(dt.timestamp(), 1673793000);

        let dt = NaiveDateTime::parse_str("1970-01-02T00:00:00").unwrap();
        assert_eq!(dt.timestamp(), 86400);

        let dt = NaiveDateTime::parse_str("2025-04-20T10:01:23").unwrap();
        assert_eq!(dt.timestamp(), 1745143283);

        let date = NaiveDate::parse_str("1914-06-28").unwrap();
        assert_eq!(date.timestamp(), -1751846400);
    }

    #[test]
    fn test_datetime_ordering() {
        let dt1 = NaiveDateTime::parse_str("2023-01-01T00:00:00").unwrap();
        let dt2 = NaiveDateTime::parse_str("2023-01-02T00:00:00").unwrap();
        let dt3 = NaiveDateTime::parse_str("2023-01-01T00:00:00").unwrap();

        assert!(dt1 < dt2);
        assert!(dt2 > dt1);
        assert!(dt1 <= dt3);
        assert!(dt1 >= dt3);
        assert_eq!(dt1.partial_cmp(&dt2), Some(std::cmp::Ordering::Less));
        assert_eq!(dt1.partial_cmp(&dt3), Some(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_date_ordering() {
        let date1 = NaiveDate::parse_str("2023-01-01").unwrap();
        let date2 = NaiveDate::parse_str("2023-01-02").unwrap();
        let date3 = NaiveDate::parse_str("2023-01-01").unwrap();

        assert!(date1 < date2);
        assert!(date2 > date1);
        assert!(date1 <= date3);
        assert!(date1 >= date3);
        assert_eq!(date1.partial_cmp(&date2), Some(std::cmp::Ordering::Less));
        assert_eq!(date1.partial_cmp(&date3), Some(std::cmp::Ordering::Equal));
    }

    #[test]
    fn test_time_ordering() {
        let time0 = NaiveTime::parse_str("00:00:00").unwrap();
        let time1 = NaiveTime::new(12, 0, 0).unwrap();
        let time2 = NaiveTime::new(13, 0, 0).unwrap();
        let time3 = NaiveTime::new(12, 0, 0).unwrap();
        let time4 = NaiveTime::parse_str("10:01:23").unwrap();

        assert!(time0 < time1);
        assert!(time1 < time2);
        assert!(time2 > time1);
        assert!(time1 <= time3);
        assert!(time1 >= time3);
        assert!(time4 > time0);
        assert_eq!(time1.partial_cmp(&time2), Some(std::cmp::Ordering::Less));
        assert_eq!(time1.partial_cmp(&time3), Some(std::cmp::Ordering::Equal));
    }
}
