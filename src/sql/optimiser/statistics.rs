//! Statistics calculation for [cost](crate::sql::optimiser::cost) model optimisations.
//!
//! To get an overview of the possible and used algorithms and data structures,
//! I much recommend this [lecture] of CMU's database query optimisation course.
//!
//! [lecture]: https://youtu.be/gpmKJazbOiU?si=dmdql95-Qu7AFC5G
//!

use crate::sql::Value;

/// Equi-depth range histogram.
///
/// Example:
/// Given the sorted values:
///
///     [0, 0, 1, 1, 2, 2, 3, 4]
///
/// With `n = 4` buckets, boundaries are chosen so that each bucket contains
/// approximately the same number of rows:
///
///     boundaries = [0, 1, 2, 3, 4]
///     rows_per_bucket ≈ 2
///
/// Buckets:
/// - `[0, 1) → ~2 rows`
/// - `[1, 2) → ~2 rows`
/// - `[2, 3) → ~2 rows`
/// - `[3, 4] → ~2 rows`
///
/// Bucket widths vary; what is equalised is the number of rows per bucket.
///
/// This model is sufficient for coarse range selectivity estimation but is
/// less accurate than full statistics models (e.g. histograms with per-bucket
/// counts or most-common-value tracking).
#[derive(Debug, PartialEq)]
pub(crate) struct Histogram {
    /// Bucket boundaries (`n + 1` values).
    ///
    /// For bucket `i`:
    /// - `boundaries[i]` is the inclusive lower bound
    /// - `boundaries[i + 1]` is the exclusive upper bound
    ///
    /// Boundaries are chosen to equalise row counts across buckets rather than
    /// to enforce uniform value widths.
    boundaries: Vec<Value>,

    /// Total number of rows represented.
    rows: usize,

    /// Approximate number of rows per bucket.
    rows_per_bucket: usize,
}

#[derive(Debug, Default)]
pub(crate) struct TableStatistics {
    pub(super) rows: usize,
    pub(super) pages: usize,
    /// Average size in bytes of a row.
    pub(super) row_size: usize,
}

#[derive(Debug, Default)]
pub(crate) struct ColumnStatistics {
    pub(super) nulls: usize,
    pub(super) distincts: usize,
    pub(super) min: Option<Value>,
    pub(super) max: Option<Value>,

    /// Average width in bytes.
    pub(super) width: usize,
    pub(super) histogram: Option<Histogram>,
}

pub(crate) struct SelectivityEstimator;

impl ColumnStatistics {
    pub fn is_empty(&self) -> bool {
        self.distincts == 0 && self.min.is_none() && self.max.is_none()
    }
}

impl SelectivityEstimator {
    pub fn join_cardinality(
        left: usize,
        right: usize,
        left_distinct: usize,
        right_distinct: usize,
    ) -> usize {
        let max = left_distinct.max(right_distinct).max(1);
        (left as u128 * right as u128 / max as u128) as usize // we do that to prevent overflowing
    }

    pub const fn equality(distinct: usize) -> f64 {
        match distinct > 0 {
            true => 1.0 / distinct as f64,
            _ => 0.1,
        }
    }
}
