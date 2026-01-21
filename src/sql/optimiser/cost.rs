//! Cost model for query optimisation.
//!
//! Based on the wonderful lectures of [Andy Pavlo] for the CMU.
//! I hardly recommend it, with the given literature that's also a lot of help.
//!
//! The paper of [microsoft] on that is also very relevant to get an overview of optimisers,
//! transformations and cost estimation.
//!
//! There's also a precursor of cost-based optimisation by [IBM] engineers for System R.
//!
//! [microsoft]: https://www.microsoft.com/en-us/research/wp-content/uploads/2024/12/Extensible-Query-Optimizers-in-Practice.pdf
//! [Andy Pavlo]: https://youtube.com/playlist?list=PLSE8ODhjZXjYCZfIbmEWH7f6MnYqyPwCE&si=VLrF3yUvfH5877Np
//! [IBM]: https://courses.cs.duke.edu/spring03/cps216/papers/selinger-etal-1979.pdf

use super::statistics::{SelectivityEstimator, TableStatistics};
use crate::core::storage::pagination::pager::DEFAULT_PAGE_SIZE;
use std::borrow::Cow;

/// Query cost estimator derived from [constants](self::Constants)
pub(crate) struct Estimator {
    constants: Constants,
}

/// The estimation of a query plan cost.
#[derive(PartialEq)]
pub(crate) struct Cost<'c> {
    total: f64,
    /// The cost of each row.
    per_row: f64,
    /// The startup cost before yielding the first tuple.
    startup: f64,

    // rows and page estimated of being returned/accessed
    rows: usize,
    pages: usize,

    explanation: Cow<'c, str>,
}

/// Those are the constants for cost estimation.
///
/// That's discussed on IBM paper's four chapter.
/// The values might change on different enviroments.
#[derive(Debug, PartialEq)]
pub(crate) struct Constants {
    /// Cost of sequentially scan a page.
    sequential: f64,
    /// Cost of randomly scan a page.
    random: f64,

    /// Size of a page in bytes.
    page_size: usize,
    btree_height: usize,

    /// Cost of doing a index look-up, which should be O(1).
    index_lookup: f64,

    /// Threshold for the minimum cost.
    min: f64,

    cpu: Cpu,
    join: Join,
}

/// Cost constants CPU-related.
#[derive(Debug, PartialEq)]
pub(crate) struct Cpu {
    /// Cost to processe a tuple.
    tuple: f64,
    /// Cost to process an indexed tuple.
    tuple_index: f64,
    /// Cost to process an operation.
    operation: f64,
}

/// Join algoritms related cost.
#[derive(Debug, PartialEq)]
pub(crate) struct Join {
    /// Cost of comparing to merge join keys.
    merge: f64,
    /// Cost of sorting a row.
    sort: f64,
    /// Cost of comparsion in a nested loop join.
    nested_loop: f64,
    hash: Hash,
}

/// Cost constants related to hashing.
#[derive(Debug, PartialEq)]
pub(crate) struct Hash {
    /// Build cost of the hash.
    build: f64,
    /// Memory factor overhead of creating the hash table.
    memory: f64,
    /// Cost of probing the hash table.
    probe: f64,
}

#[derive(Debug, PartialEq)]
pub(crate) enum JoinAlgorithm {
    /// Hash join: `O(N + M)`
    Hash {
        side: Side,
        /// Estimated build rows from the build side.
        rows: usize,
        /// Estimated probe rows from the probing side.
        probe: usize,
    },

    /// Sort-merge join: `O(N + M)`
    /// with sorting: `O(N log N + M log M)`
    SortMerge {
        left: usize,
        right: usize,
        is_left_sorted: bool,
        is_right_sorted: bool,
    },

    /// Nested loop join: `O(N * M)`
    NestedLoop { outer: usize, inner: usize },

    Anti {
        join: Box<Self>,
        left: usize,
        selectivity: f64,
    },

    Semi {
        join: Box<Self>,
        left: usize,
        selectivity: f64,
    },
}

#[derive(Debug)]
pub(crate) struct JoinStatistics {
    left: TableStatistics,
    right: TableStatistics,

    left_distinct: usize,
    right_distinct: usize,
}

/// Side which the hash table is going to be constructed.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Side {
    Left,
    Right,
}

impl Estimator {
    /// Threshold for `LIMIT` clauses.
    /// In cases where it's less or equal to this, the preference is a [nested
    /// loop](self::JoinAlgorithm::NestedLoop)
    const LIMIT: usize = 1 << 7;

    pub fn new() -> Self {
        Self {
            constants: Constants::default(),
        }
    }

    pub fn estimate_sequential_scan<'c>(&self, table: &TableStatistics) -> Cost<'c> {
        let (rows, pages) = (table.rows, table.pages);
        debug_assert!(
            pages >= 1,
            "The estimated number of pages should be non-zero positive"
        );

        let io = pages as f64 * self.constants.sequential;
        let cpu = rows as f64 * self.constants.cpu.tuple;
        let total = cpu + io;

        let explanation = format!(
            "Sequential Scan: {pages} * {:.2} + {rows} * {:.4} = {total:.2}",
            self.constants.sequential, self.constants.cpu.tuple
        );

        Cost::new(self.constants.cpu.tuple, io, rows, pages, explanation)
    }

    pub fn estimate_sequential_scan_filtered<'c>(
        &self,
        table: &TableStatistics,
        selectivity: f64,
    ) -> Cost<'c> {
        let (rows, pages) = (table.rows, table.pages);
        debug_assert!(
            pages >= 1,
            "The estimated number of pages should be non-zero positive"
        );

        let io = pages as f64 * self.constants.sequential;
        let per_row = self.constants.cpu.tuple + self.constants.cpu.operation;
        let cpu = rows as f64 * per_row;
        let total = io + cpu;

        let output = (rows as f64 * selectivity) as usize;

        let explanation = format!("Sequential Scan with Filter: {rows} rows * {selectivity:.2} selectivity = output {output} with cost {total:.2}");
        Cost::with_total(total, per_row, io, output, pages, explanation)
    }

    pub fn estimate_index_scan<'c>(
        &self,
        table: &TableStatistics,
        selectivity: f64,
        index: &str,
    ) -> Cost<'c> {
        let (rows, pages) = (table.rows, table.pages);
        debug_assert!(
            pages >= 1,
            "The estimated number of pages should be non-zero positive"
        );
        let estimated_rows = (rows as f64 * selectivity).max(1.0) as usize;

        let (index_io, leaf_io) = {
            let height = self.constants.btree_height as f64;
            let btree_cost = height * self.constants.random;

            let leaf = (table.pages as f64 * selectivity).max(1.0);
            let leaf_cost = leaf * self.constants.random;
            (btree_cost, leaf_cost)
        };

        let index_cpu = estimated_rows as f64 * self.constants.cpu.tuple_index;
        let pages = match estimated_rows >= table.pages {
            true => table.pages,
            _ => estimated_rows.min(table.pages),
        };

        let table_cpu = estimated_rows as f64 * self.constants.cpu.tuple;
        let table_io = pages as f64 * self.constants.random;

        let total = index_io + leaf_io + index_cpu + table_io + table_cpu;
        let leaf_pages = (table.pages as f64 * selectivity).max(1.0) as usize;
        let total_pages = leaf_pages + self.constants.btree_height + pages;

        let explanation = format!("Index Scan: {index}: selectivity {selectivity:.4} -> {estimated_rows} rows, cost {total:.2}");
        Cost::with_total(
            total,
            self.constants.cpu.tuple_index + self.constants.cpu.tuple,
            index_io, // the startup is basically the traverse cost of the first leaf
            estimated_rows,
            total_pages,
            explanation,
        )
    }

    pub fn must_use_index(&self, table: &TableStatistics, selectivity: f64) -> bool {
        if table.rows < 100 {
            return false; // sequential will be always fast in small set due to locallity
        }

        if selectivity < 0.01 {
            return true; // very selective, always use index
        }

        let sequential = self.estimate_sequential_scan_filtered(table, selectivity);
        let index = self.estimate_index_scan(table, selectivity, "index");

        index < sequential
    }

    pub fn estimate_primary_key_lookup<'c>(&self) -> Cost<'c> {
        Cost::new(
            0.0,
            self.constants.index_lookup,
            1,
            1,
            format!(
                "Primary key lookup: cost {:.2}",
                self.constants.index_lookup
            ),
        )
    }

    pub fn estimate_nested_loop<'c>(&self, join: &JoinStatistics) -> Cost<'c> {
        let (left, right) = (join.left.rows, join.right.rows);

        // we wanna use the smaller as outer to
        // have less possible iterations
        let (outer, inner) = join.outer_and_inner();
        let comparisons = outer as u128 * inner as u128;
        let comparison_cost = self.constants.cpu.tuple + self.constants.join.nested_loop;

        let pages = join.right.pages.max(1);
        let io = outer as f64 * pages as f64 * self.constants.sequential * 0.1;
        let cpu = comparisons as f64 * comparison_cost;
        let total = io + cpu;

        let left_distinct = match join.left_distinct > 0 {
            true => join.left_distinct,
            _ => left.max(1),
        };

        let right_distinct = match join.right_distinct > 0 {
            true => join.right_distinct,
            _ => right.max(1),
        };

        let output =
            SelectivityEstimator::join_cardinality(left, right, left_distinct, right_distinct);
        let explanation = format!("Nested Loop: {outer} * {inner} = {comparisons} comparisons -> {output} rows, cost {total:.2}");

        Cost::with_total(total, comparison_cost, 0.0, output, inner, explanation)
    }

    pub fn estimate_hash<'c>(&self, join: &JoinStatistics) -> Cost<'c> {
        let (left, right) = (join.left.rows, join.right.rows);

        let (build, probe, side) = join.outer_and_inner_with_side();
        let build_cost = build as f64 * (self.constants.cpu.tuple + self.constants.join.hash.build);
        let probe_cost = probe as f64
            * (self.constants.cpu.tuple
                + self.constants.join.hash.probe
                + self.constants.cpu.operation);

        let row_size = join.left.row_size.max(32) as f64;
        let table_size = build as f64 * row_size * self.constants.join.hash.memory;
        let pages = (table_size / self.constants.page_size as f64).max(1.0) as usize;

        let left_distinct = match join.left_distinct > 0 {
            true => join.left_distinct,
            _ => left.max(1),
        };

        let right_distinct = match join.right_distinct > 0 {
            true => join.right_distinct,
            _ => right.max(1),
        };

        let output =
            SelectivityEstimator::join_cardinality(left, right, left_distinct, right_distinct);
        let total = build_cost + probe_cost;

        let explanation = format!(
            "Hash Join: build from {} with rows {build}: probe {probe} rows -> {output} rows cost {total:.2}",
            side.to_string(),
        );

        Cost::with_total(
            total,
            self.constants.cpu.tuple + self.constants.join.hash.probe,
            build_cost,
            output,
            pages,
            explanation,
        )
    }

    pub fn estimate_sort_merge<'c>(
        &self,
        join: &JoinStatistics,
        is_left_sorted: bool,
        is_right_sorted: bool,
    ) -> Cost<'c> {
        let (left, right) = (join.left.rows, join.right.rows);

        let left_cost = match is_left_sorted {
            true => 0.0,
            _ => left as f64 * (left as f64).log2().max(1.0) * self.constants.join.sort,
        };

        let right_cost = match is_right_sorted {
            true => 0.0,
            _ => right as f64 * (right as f64).log2().max(1.0) * self.constants.join.sort,
        };

        let merge_cost = (left + right) as f64 * self.constants.join.merge;
        let cpu = (left + right) as f64 * self.constants.cpu.tuple;
        let total = merge_cost + cpu + left_cost + right_cost;

        let left_distinct = join.left_distinct.max(1);
        let right_distinct = join.right_distinct.max(1);

        let output =
            SelectivityEstimator::join_cardinality(left, right, left_distinct, right_distinct);
        let pages = join.left.pages + join.right.pages;
        let explanation = format!("Sort Merge Join: {left} + {right}, sort cost: {left_cost:.2} + {right_cost:.2} merge cost: {merge_cost:.2} -> {output} rows total {total:.2}");

        Cost::with_total(
            total,
            self.constants.join.merge,
            left_cost + right_cost,
            output,
            pages,
            explanation,
        )
    }

    pub fn choose_join_algorithm<'c>(
        &self,
        join: &JoinStatistics,
        has_equality: bool,
    ) -> (JoinAlgorithm, Cost<'c>) {
        if !has_equality {
            let cost = self.estimate_nested_loop(join);
            let (outer, inner) = join.outer_and_inner();

            return (JoinAlgorithm::NestedLoop { outer, inner }, cost);
        }

        let nested_loop = self.estimate_nested_loop(join);
        let hash = self.estimate_hash(join);

        match hash < nested_loop {
            true => {
                let (build, probe, side) = join.outer_and_inner_with_side();
                (
                    JoinAlgorithm::Hash {
                        side,
                        rows: build,
                        probe,
                    },
                    hash,
                )
            }

            _ => {
                let (outer, inner) = join.outer_and_inner();
                (JoinAlgorithm::NestedLoop { outer, inner }, nested_loop)
            }
        }
    }

    pub fn choose_join_with_limit<'c>(
        &self,
        join: &JoinStatistics,
        has_equality: bool,
        limit: Option<usize>,
    ) -> (JoinAlgorithm, Cost<'c>) {
        let (left, right) = (join.left.rows, join.right.rows);

        if !has_equality {
            let cost = self.estimate_nested_loop(join);
            let (outer, inner) = join.outer_and_inner();

            return (JoinAlgorithm::NestedLoop { outer, inner }, cost);
        }

        let Some(limit) = limit else {
            return self.choose_join_algorithm(join, has_equality);
        };

        if limit < Self::LIMIT {
            let outer = limit.min(left.min(right));
            let explanation = format!("Nested Join with Limit {limit}: {outer}");
            let nested = Cost::with_total(
                outer as f64 * self.constants.cpu.tuple,
                self.constants.cpu.tuple,
                0.0,
                limit,
                outer,
                explanation,
            );

            let hash = self.estimate_hash(join);
            if nested.total < (hash.total * 0.5) {
                let (outer, inner) = join.outer_and_inner();
                return (JoinAlgorithm::NestedLoop { outer, inner }, nested);
            }
        }

        self.choose_join_algorithm(join, has_equality)
    }
}

impl<'c> Cost<'c> {
    pub const fn zero() -> Self {
        Self {
            total: 0.0,
            per_row: 0.0,
            startup: 0.0,
            rows: 0,
            pages: 0,
            explanation: Cow::Borrowed(""),
        }
    }

    pub fn new(
        per_row: f64,
        startup: f64,
        rows: usize,
        pages: usize,
        explanation: impl Into<Cow<'c, str>>,
    ) -> Self {
        let total = startup + (per_row * rows as f64);

        Self {
            total,
            startup,
            rows,
            per_row,
            pages,
            explanation: explanation.into(),
        }
    }

    pub fn with_total(
        total: f64,
        per_row: f64,
        startup: f64,
        rows: usize,
        pages: usize,
        explanation: impl Into<Cow<'c, str>>,
    ) -> Self {
        Self {
            total,
            per_row,
            startup,
            rows,
            pages,
            explanation: explanation.into(),
        }
    }
}

impl JoinStatistics {
    const fn outer_and_inner(&self) -> (usize, usize) {
        let (left, right) = (self.left.rows, self.right.rows);
        match left <= right {
            true => (left, right),
            _ => (right, left),
        }
    }

    const fn outer_and_inner_with_side(&self) -> (usize, usize, Side) {
        let (left, right) = (self.left.rows, self.right.rows);
        match left <= right {
            true => (left, right, Side::Left),
            _ => (right, left, Side::Right),
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Left => f.write_str("left"),
            Self::Right => f.write_str("right"),
        }
    }
}

impl PartialOrd for Cost<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.total.partial_cmp(&other.total)
    }
}

impl Default for Constants {
    fn default() -> Self {
        Self {
            sequential: 1.0,
            random: 2.0,
            page_size: DEFAULT_PAGE_SIZE,
            index_lookup: 1.0,
            btree_height: 3, // average b-tree height for indexes.
            min: 0.0001,
            cpu: Cpu::default(),
            join: Join::default(),
        }
    }
}

impl Default for Cpu {
    fn default() -> Self {
        Self {
            tuple: 0.01,
            tuple_index: 0.005,
            operation: 0.0025,
        }
    }
}

impl Default for Join {
    fn default() -> Self {
        Self {
            merge: 0.005,
            nested_loop: 0.01,
            sort: 0.03,
            hash: Hash::default(),
        }
    }
}

impl Default for Hash {
    fn default() -> Self {
        Self {
            build: 0.02,
            probe: 0.01,
            memory: 1.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const fn table_stats(rows: usize, pages: usize) -> TableStatistics {
        TableStatistics {
            pages,
            rows,
            row_size: 1 << 7,
        }
    }

    fn join_stats(
        left: usize,
        right: usize,
        left_distinct: usize,
        right_distinct: usize,
    ) -> JoinStatistics {
        let left = TableStatistics {
            rows: left,
            pages: (left / 100).max(1),
            row_size: 1 << 7,
        };

        let right = TableStatistics {
            rows: right,
            pages: (right / 100).max(1),
            row_size: 1 << 7,
        };

        JoinStatistics {
            left,
            right,
            left_distinct,
            right_distinct,
        }
    }

    #[test]
    fn sequential_scan() {
        let estimator = Estimator::new();
        let table = table_stats(10000, 100);
        let cost = estimator.estimate_sequential_scan(&table);

        assert!(cost.total > 0.0);
        assert_eq!((table.rows, table.pages), (cost.rows, cost.pages));
    }

    #[test]
    fn index_scan() {
        let estimator = Estimator::new();
        let table = table_stats(10000, 100);
        let cost = estimator.estimate_index_scan(&table, 0.01, "index");

        assert!(cost.total > 0.0);
        assert_eq!(cost.rows, 100);
    }

    #[test]
    fn index_against_sequential_scan() {
        let estimator = Estimator::new();
        let table = table_stats(10000, 100);

        // in high selectivity, index must always win
        let selectivity = 0.001;
        let seq = estimator.estimate_sequential_scan_filtered(&table, selectivity);
        let idx = estimator.estimate_index_scan(&table, selectivity, "index");
        assert!(seq > idx);

        // low selectivity, sequential might win
        let selectivity = 0.5;
        let seq = estimator.estimate_sequential_scan_filtered(&table, selectivity);
        let idx = estimator.estimate_index_scan(&table, selectivity, "index");
        assert!(seq < idx);
    }

    #[test]
    fn nested_loop_join() {
        let estimator = Estimator::new();
        let join = join_stats(100, 50, 100, 50);
        let cost = estimator.estimate_nested_loop(&join);

        assert!(cost.total > 0.0);
        assert!(cost.explanation.contains("Nested Loop"));
    }

    #[test]
    fn hash_join() {
        let estimator = Estimator::new();
        let join = join_stats(10000, 1000, 1000, 1000);
        let cost = estimator.estimate_hash(&join);

        assert!(cost.total > 0.0);
        assert_eq!(cost.rows, 10000);
        assert!(cost.explanation.contains("Hash"));
    }

    #[test]
    fn join_with_limit() {
        let estimator = Estimator::new();
        let join = join_stats(10_000, 10_000, 10_000, 10_000);

        let (algorithm, cost) = estimator.choose_join_with_limit(&join, true, None);
        assert!(matches!(algorithm, JoinAlgorithm::Hash { .. }));
        assert!(cost.total > 0.0);

        let (algorithm, cost) = estimator.choose_join_with_limit(&join, true, Some(10));
        assert!(matches!(algorithm, JoinAlgorithm::NestedLoop { .. }));
        assert!(cost.total > 0.0);

        let (algorithm, cost) = estimator.choose_join_with_limit(&join, true, Some(1 << 24));
        assert!(matches!(algorithm, JoinAlgorithm::Hash { .. }));
        assert!(cost.total > 0.0);

        let (algorithm, cost) = estimator.choose_join_with_limit(&join, false, Some(10));
        assert!(matches!(algorithm, JoinAlgorithm::NestedLoop { .. }));
        assert!(cost.total > 0.0);

        let (algorithm, cost) = estimator.choose_join_with_limit(&join, false, None);
        assert!(matches!(algorithm, JoinAlgorithm::NestedLoop { .. }));
        assert!(cost.total > 0.0);
    }
}
