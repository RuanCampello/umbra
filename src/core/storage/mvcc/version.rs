use crate::{
    core::{smallvec::SmallVec, storage::mvcc::get_timestamp},
    vm::planner::Tuple,
};

type Version = SmallVec<RowVersion, 1>;

#[derive(Debug, Clone)]
pub(crate) struct RowVersion {
    pub txn_id: i64, // Transaction that created this version (min_txn_id / xmin)
    pub deleted_at_txn_id: i64, // Transaction that deleted this version (max_txn_id / xmax)
    pub data: Option<Tuple>, // TODO: we could compress this one
    pub row_id: i64, // Or RowId
    pub created_at: i64, // Timestamp
}

impl RowVersion {
    pub fn new(txn_id: i64, row_id: i64, data: Option<Tuple>) -> Self {
        Self {
            txn_id,
            row_id,
            data,
            deleted_at_txn_id: 0,
            created_at: get_timestamp(),
        }
    }

    pub fn with_timestamp(txn_id: i64, row_id: i64, data: Option<Tuple>, created_at: i64) -> Self {
        Self {
            txn_id,
            row_id,
            data,
            created_at,
            deleted_at_txn_id: 0,
        }
    }
}
