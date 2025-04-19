/// [ISO 8601](https://www.loc.gov/standards/datetime/iso-tc154-wg5_n0038_iso_wd_8601-1_2016-02-16.pdf) date and time without timezone.
pub(crate) enum DateTime {
    Timestamp(String),
    Time(String),
}
