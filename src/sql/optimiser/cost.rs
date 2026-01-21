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

#[derive(Debug, Default)]
pub(crate) struct TableStatistics {
    rows: usize,
    pages: usize,
    /// Average size in bytes of a row.
    row_size: usize,
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

impl Estimator {
    pub fn new() -> Self {
        Self {
            constants: Constants::default(),
        }
    }

    pub fn estimate_sequential_scan<'c>(&'c self, table: &TableStatistics) -> Cost<'c> {
        let rows = table.rows;
        let pages = table.pages;
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
