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
