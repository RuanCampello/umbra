//! Secondary indexes: ordered `(key, row id)` entries for the key of every live version
//!
//! Entries carry no visibility, readers re-check the visible version's key
//! Lock order is always index -> chunk: callbacks given to [Index::insert] and [Index::remove_if] may latch chunks, but no
//! thread takes an index lock while holding a chunk latch

use crate::sql::Value;
use crate::storage::engine::{
    error::{EngineError, Result},
    table::RowId,
};
use std::collections::BTreeSet;
use std::ops::Bound;
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

pub(crate) struct Index {
    pub name: String,
    pub column: usize,
    pub unique: bool,
    /// Stamp of the transaction that created the index, as on a version (0 for recovered ones);
    /// only checkpoints read it: any registered index serves every reader
    created: AtomicU64,
    entries: RwLock<BTreeSet<(Value, RowId)>>,
}

/// Who keeps a key alive in another row, as seen by a unique check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::engine) enum Holder {
    Free,
    Committed,
    Uncommitted,
}

type EntryBound = Bound<(Value, RowId)>;

impl Index {
    pub fn new(name: impl Into<String>, column: usize, unique: bool) -> Self {
        Self {
            name: name.into(),
            column,
            unique,
            created: AtomicU64::new(0),
            entries: RwLock::new(BTreeSet::new()),
        }
    }

    #[inline]
    pub fn created(&self) -> u64 {
        self.created.load(Ordering::Acquire)
    }

    pub fn stamp_created(&self, stamp: u64) {
        self.created.store(stamp, Ordering::Release);
    }

    /// adds `(key, id)`, a unique index first asks `holder` about every other row under `key`
    /// returns the rows found [Holder::Free]: their entries may be garbage worth settling
    pub(super) fn insert(
        &self,
        key: &Value,
        id: RowId,
        holder: impl Fn(RowId) -> Holder,
    ) -> Result<Vec<RowId>> {
        let mut entries = self.entries.write().expect("index lock poisoned");
        let mut free = Vec::new();

        if self.unique && *key != Value::Null {
            let others = entries
                .range(bounds(Bound::Included(key), Bound::Included(key)))
                .map(|&(_, other)| other)
                .filter(|&other| other != id);

            for other in others {
                match holder(other) {
                    Holder::Free => free.push(other),
                    Holder::Committed => return Err(EngineError::DuplicatedKey(key.clone())),
                    Holder::Uncommitted => return Err(EngineError::WriteConflict),
                }
            }
        }

        entries.insert((key.clone(), id));
        Ok(free)
    }

    /// adds `(key, id)` without any check (rebuilds, back-fill of older versions)
    pub fn insert_unchecked(&self, key: Value, id: RowId) {
        self.entries.write().expect("index lock poisoned").insert((key, id));
    }

    /// removes `(key, id)` when `unused` confirms, under the index lock, that no version needs it
    pub fn remove_if(&self, key: &Value, id: RowId, unused: impl FnOnce() -> bool) {
        let mut entries = self.entries.write().expect("index lock poisoned");
        let entry = (key.clone(), id);

        if entries.contains(&entry) && unused() {
            entries.remove(&entry);
        }
    }

    /// calls `f` for every entry within the bounds, in key order, under the read lock
    pub fn visit(
        &self,
        start: Bound<&Value>,
        end: Bound<&Value>,
        mut f: impl FnMut(&Value, RowId),
    ) {
        let entries = self.entries.read().expect("index lock poisoned");
        for (key, id) in entries.range(bounds(start, end)) {
            f(key, *id);
        }
    }

    /// up to `limit` entries with keys within the bounds, strictly after `after` (or from the start)
    /// in ascending order, or strictly before it in descending order, under the read lock
    pub fn page(
        &self,
        (start, end): (Bound<&Value>, Bound<&Value>),
        after: Option<&(Value, RowId)>,
        descending: bool,
        limit: usize,
        mut f: impl FnMut(&Value, RowId),
    ) {
        let (mut low, mut high) = bounds(start, end);
        match (after, descending) {
            (Some(after), false) => low = Bound::Excluded(after.clone()),
            (Some(after), true) => high = Bound::Excluded(after.clone()),
            _ => {},
        }
        if empty(&low, &high) {
            return;
        }

        let entries = self.entries.read().expect("index lock poisoned");
        let range = entries.range((low, high));
        let page: Box<dyn Iterator<Item = &(Value, RowId)>> = match descending {
            false => Box::new(range),
            true => Box::new(range.rev()),
        };

        for (key, id) in page.take(limit) {
            f(key, *id);
        }
    }

    pub fn clear(&self) {
        self.entries.write().expect("index lock poisoned").clear();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.read().expect("index lock poisoned").len()
    }
}

/// maps key bounds onto `(key, row id)` bounds covering every row id of the boundary keys
fn bounds(start: Bound<&Value>, end: Bound<&Value>) -> (EntryBound, EntryBound) {
    let start = match start {
        Bound::Included(key) => Bound::Included((key.clone(), RowId::MIN)),
        Bound::Excluded(key) => Bound::Excluded((key.clone(), RowId::MAX)),
        Bound::Unbounded => Bound::Unbounded,
    };

    let end = match end {
        Bound::Included(key) => Bound::Included((key.clone(), RowId::MAX)),
        Bound::Excluded(key) => Bound::Excluded((key.clone(), RowId::MIN)),
        Bound::Unbounded => Bound::Unbounded,
    };

    (start, end)
}

/// whether no entry lies between `low` and `high`
fn empty(low: &EntryBound, high: &EntryBound) -> bool {
    match (low, high) {
        (Bound::Included(low), Bound::Included(high)) => low > high,
        (Bound::Included(low) | Bound::Excluded(low), Bound::Excluded(high))
        | (Bound::Excluded(low), Bound::Included(high)) => low >= high,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(index: &Index, start: Bound<&Value>, end: Bound<&Value>) -> Vec<RowId> {
        let mut ids = Vec::new();
        index.visit(start, end, |_, id| ids.push(id));
        ids
    }

    #[test]
    fn ranges_cover_every_row_of_boundary_keys() {
        let index = Index::new("i", 0, false);
        for (key, id) in [(1, 10), (2, 20), (2, 21), (3, 30)] {
            index.insert_unchecked(Value::Number(key), id);
        }

        let two = Value::Number(2);
        assert_eq!(keys(&index, Bound::Included(&two), Bound::Included(&two)), [20, 21]);
        assert_eq!(keys(&index, Bound::Excluded(&two), Bound::Unbounded), [30]);
        assert_eq!(keys(&index, Bound::Unbounded, Bound::Excluded(&two)), [10]);
    }

    #[test]
    fn unique_insert_consults_holders_of_other_rows() {
        let index = Index::new("u", 0, true);
        let key = Value::Number(7);

        index.insert(&key, 1, |_| unreachable!()).unwrap();
        index.insert(&key, 1, |_| unreachable!()).unwrap();
        assert!(matches!(
            index.insert(&key, 2, |_| Holder::Committed),
            Err(EngineError::DuplicatedKey(_))
        ));
        assert!(matches!(
            index.insert(&key, 2, |_| Holder::Uncommitted),
            Err(EngineError::WriteConflict)
        ));
        index.insert(&key, 2, |_| Holder::Free).unwrap();
        index.insert(&Value::Null, 3, |_| unreachable!()).unwrap();
        index.insert(&Value::Null, 4, |_| unreachable!()).unwrap();
        assert_eq!(index.len(), 4);
    }

    #[test]
    fn removal_is_conditional() {
        let index = Index::new("i", 0, false);
        let key = Value::Number(1);
        index.insert_unchecked(key.clone(), 1);

        index.remove_if(&key, 1, || false);
        assert_eq!(index.len(), 1);
        index.remove_if(&key, 1, || true);
        assert_eq!(index.len(), 0);
    }
}
