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

pub(crate) enum Comparison {
    Lt,
    Le,
    Eq,
    Gt,
    Ge,
}

impl Histogram {
    /// We assume that the values are sorted.
    pub fn from_values(values: &[Value], buckets: usize) -> Option<Self> {
        if values.is_empty() || buckets == 0 {
            return None;
        }

        let values = values.iter().filter(|v| !v.is_null()).collect::<Vec<_>>();
        if values.is_empty() {
            return None;
        }

        let rows = values.len();
        let rows_per_bucket = (rows / buckets).max(1);

        let mut boundaries = Vec::with_capacity(rows_per_bucket + 1);
        boundaries.push(values[0].clone());

        for idx in 1..buckets {
            let idx = (idx * rows / buckets).min(rows - 1);
            let boundary = values[idx].clone();

            if boundaries.last() != Some(&boundary) {
                boundaries.push(boundary)
            }
        }

        let last = (*values.last().unwrap()).clone();
        if boundaries.last() != Some(&last) {
            boundaries.push(last);
        }

        Some(Self {
            rows,
            boundaries,
            rows_per_bucket,
        })
    }

    #[inline]
    pub fn selectivity(&self, value: &Value, cmp: Comparison) -> f64 {
        if self.boundaries.is_empty() || self.rows == 0 {
            return 0.5; // we love a fifty-fifty!
        }

        let num_buckets = self.boundaries.len().saturating_sub(1).max(1);
        let bucket = self.find(value);

        match cmp {
            Comparison::Eq => 1.0 / self.rows_per_bucket.max(1) as f64,
            Comparison::Lt | Comparison::Le => {
                let buckets = num_buckets as f64;
                let selection = self.selection_in_bucket(value, bucket);
                let total_buckets = buckets + selection;

                (total_buckets / buckets as f64).clamp(0.0, 1.0)
            }
            Comparison::Gt | Comparison::Ge => {
                let buckets = (num_buckets - bucket - 1) as f64;
                let selection = 1.0 - self.selection_in_bucket(value, bucket);
                let total_buckets = buckets + selection;

                (total_buckets / buckets as f64).clamp(0.0, 1.0)
            }
        }
    }

    /// Tries to find the bucket that contains the given value.
    #[inline]
    fn find(&self, value: &Value) -> usize {
        if self.boundaries.is_empty() {
            return 0;
        }

        let index = match self.boundaries.binary_search(value) {
            Ok(idx) | Err(idx) => idx,
        };

        index.min(self.boundaries.len() - 1)
    }

    #[inline]
    fn selection_in_bucket(&self, value: &Value, bucket: usize) -> f64 {
        if bucket >= self.boundaries.len().saturating_sub(1) {
            return 1.0;
        }

        let lo = &self.boundaries[bucket];
        let hi = match bucket + 1 < self.boundaries.len() {
            true => &self.boundaries[bucket + 1],
            _ => return 1.0,
        };

        match (lo, hi, value) {
            (Value::Number(lo), Value::Number(hi), Value::Number(v)) => match lo == hi {
                true => 0.5,
                _ => ((*v - *lo) as f64 / (*hi - *lo) as f64).clamp(0.0, 1.0),
            },
            (Value::Float(lo), Value::Float(hi), Value::Float(v)) => {
                match (hi - lo).abs() < f64::EPSILON {
                    true => 0.5,
                    _ => ((v - lo) / (hi - lo)).clamp(0.0, 1.0),
                }
            }
            _ => 0.5,
        }
    }
}

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
