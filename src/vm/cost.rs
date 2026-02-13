use crate::{
    storage::pagination::io::FileOperations,
    vm::planner::{
        Aggregate, ExactMatch, Filter, HashJoin, IndexNestedLoopJoin, Limit, Planner, Project,
        RangeScan, SeqScan, Sort, Values,
    },
};

/// A wrapper to represent the estimated cost and cardinality of an operation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Cost {
    /// Abstract cost unit (IO + CPU). Lower is better.
    pub value: f64,
    /// Estimated number of rows produced by this operation.
    pub rows: usize,
}

pub(crate) trait CostEstimator {
    fn estimate(&self) -> Cost;
}

impl Cost {
    pub const ESTIMATED_ROWS_PER_PAGE: usize = 50;
    // Arbitrary CPU penalty for processing a tuple in memory
    pub const CPU_TUPLE_COST: f64 = 0.01;

    pub fn new(value: f64, rows: usize) -> Self {
        Self { value, rows }
    }

    pub fn estimate_scan(row_count: usize) -> Self {
        let pages = (row_count as f64 / Self::ESTIMATED_ROWS_PER_PAGE as f64)
            .ceil()
            .max(1.0);
        Self::new(pages, row_count)
    }
}

impl Default for Cost {
    fn default() -> Self {
        Cost {
            value: 0.0,
            rows: 0,
        }
    }
}

impl std::ops::Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost {
            value: self.value + other.value,
            rows: std::cmp::max(self.rows, other.rows),
        }
    }
}

impl<File> CostEstimator for SeqScan<File> {
    fn estimate(&self) -> Cost {
        let rows = self.table.count;
        Cost::estimate_scan(rows)
    }
}

impl<File> CostEstimator for ExactMatch<File> {
    fn estimate(&self) -> Cost {
        Cost::new(4.0, 1)
    }
}

impl<File> CostEstimator for RangeScan<File> {
    fn estimate(&self) -> Cost {
        let estimated_rows = 100;
        let io_cost = (estimated_rows as f64 / Cost::ESTIMATED_ROWS_PER_PAGE as f64).ceil();
        Cost::new(io_cost, estimated_rows)
    }
}

impl CostEstimator for Values {
    fn estimate(&self) -> Cost {
        let rows = self.values.len();
        Cost::new(rows as f64 * Cost::CPU_TUPLE_COST, rows)
    }
}

impl<File: FileOperations> CostEstimator for Filter<File> {
    fn estimate(&self) -> Cost {
        let source_cost = self.source.estimate();

        let rows = source_cost.rows / 2;
        let cpu_cost = source_cost.rows as f64 * Cost::CPU_TUPLE_COST;

        Cost::new(source_cost.value + cpu_cost, rows)
    }
}

impl<File: FileOperations> CostEstimator for Limit<File> {
    fn estimate(&self) -> Cost {
        let source_cost = self.source.estimate();
        let rows = std::cmp::min(source_cost.rows, self.limit);

        let ratio = match source_cost.rows > 0 {
            true => rows as f64 / source_cost.rows as f64,
            _ => 1.0,
        };

        Cost::new(source_cost.value * ratio, rows)
    }
}

impl<File: FileOperations> CostEstimator for Project<File> {
    fn estimate(&self) -> Cost {
        self.source.estimate()
    }
}

impl<File: FileOperations> CostEstimator for Sort<File> {
    fn estimate(&self) -> Cost {
        let source = self.collection.source.estimate();
        let rows = source.rows;

        // o(n log n) cpu cost
        let cpu = match rows > 0 {
            true => (rows as f64) * (rows as f64).log2() * Cost::CPU_TUPLE_COST,
            _ => 0.0,
        };

        Cost::new(source.value + cpu, rows)
    }
}

impl<File: FileOperations> CostEstimator for Aggregate<File> {
    fn estimate(&self) -> Cost {
        let source = self.source.estimate();
        let out_rows = match self.group_by.is_empty() {
            true => 1,
            _ => source.rows / 10,
        };

        Cost::new(
            source.value + (source.rows as f64 * Cost::CPU_TUPLE_COST),
            out_rows,
        )
    }
}

impl<File: FileOperations> CostEstimator for HashJoin<File> {
    fn estimate(&self) -> Cost {
        let left = self.left.estimate();
        let right = self.right.estimate();

        // cost = build(right) + probe(left)
        let io = left.value + right.value;
        let cpu_cost = (left.rows + right.rows) as f64 * Cost::CPU_TUPLE_COST;

        // cardinality: naive estimation max(left, right)
        let rows = std::cmp::max(left.rows, right.rows);

        Cost::new(io + cpu_cost, rows)
    }
}

impl<File: FileOperations> CostEstimator for IndexNestedLoopJoin<File> {
    fn estimate(&self) -> Cost {
        let left = self.left.estimate();
        let right_rows_total = self.right_table.count.max(1);

        // cost of one lookup = log2(n) pages
        let lookup = (right_rows_total as f64).log2().max(1.0);

        // total cost = scan left + (left rows * lookup cost)
        let total = left.value + (left.rows as f64 * lookup);

        Cost::new(total, left.rows)
    }
}

impl<File: FileOperations> CostEstimator for Planner<File> {
    fn estimate(&self) -> Cost {
        use crate::vm::planner::Planner::*;

        match self {
            SeqScan(n) => n.estimate(),
            ExactMatch(n) => n.estimate(),
            RangeScan(n) => n.estimate(),
            Filter(n) => n.estimate(),
            HashJoin(n) => n.estimate(),
            IndexNestedLoopJoin(n) => n.estimate(),
            Limit(n) => n.estimate(),
            Project(n) => n.estimate(),
            Sort(n) => n.estimate(),
            Aggregate(n) => n.estimate(),
            Values(n) => n.estimate(),
            KeyScan(n) => n.source.estimate(),
            LogicalScan(n) => n
                .scans
                .iter()
                .map(|s| s.estimate())
                .fold(Cost::default(), |acc, c| acc + c),
            Collect(n) => n.source.estimate(),
            _ => Cost::default(),
        }
    }
}
