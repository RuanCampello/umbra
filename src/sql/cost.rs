use std::ops::Add;

/// A wrapper over f64 to represent the estimated cost of an operation.
/// Lower is better.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct Cost(pub f64);

impl Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost(self.0 + other.0)
    }
}

impl Default for Cost {
    fn default() -> Self {
        Cost(0.0)
    }
}

pub const ESTIMATED_ROWS_PER_PAGE: usize = 50;
pub const DEFAULT_TABLE_ROW_COUNT: usize = 1_000_000;

impl Cost {
    /// Estimates the cost of scanning a table with `row_count` rows.
    pub fn estimate_scan(row_count: usize) -> Self {
        if row_count == 0 {
            return Self(1.0);
        }

        let pages = (row_count as f64 / ESTIMATED_ROWS_PER_PAGE as f64).ceil();
        Self(pages)
    }
}
