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
use std::sync::{
    Arc, RwLock,
    atomic::{AtomicU64, Ordering},
};

// TODO: Implement this with a better and more low level Btree implementation
// For this, there's the FB+-tree discussed in this paper: https://arxiv.org/html/2503.23397v1#bib.bib44
// This one is based on another quite interesting one, but older: https://db.in.tum.de/~leis/papers/artsync.pdf
// Both (an others) are to be taken in consideration, implementing a lock or latch free tree might
// improve things, instead of a bare RwLock
pub(crate) struct Index {
    pub name: String,
    /// The indexed columns, in key order
    pub columns: Box<[usize]>,
    pub unique: bool,
    /// Stamp of the transaction that created the index, as on a version (0 for recovered ones);
    /// only checkpoints read it: any registered index serves every reader
    created: AtomicU64,
    entries: RwLock<BTreeSet<(Key, RowId)>>,
}

/// The values of an index's columns in a row, in key order
///
/// Keys compare value by value over their common length, so a key shorter than the index is a
/// prefix bound: equal to every key it begins, `Included(prefix)..=Included(prefix)` spans them all
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Key {
    One(Value),
    /// shared, so cursors copy entries out of the index without allocating
    Many(Arc<[Value]>),
}

/// Who keeps a key alive in another row, as seen by a unique check
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::storage::engine) enum Holder {
    Free,
    Committed,
    Uncommitted,
}

// PERFORMANCE: an entry is a key and a row id, the composite form rides in `Value`'s niche
const _: () = assert!(size_of::<Key>() == size_of::<Value>());

type EntryBound = Bound<(Key, RowId)>;

impl Index {
    pub fn new(name: impl Into<String>, columns: impl Into<Box<[usize]>>, unique: bool) -> Self {
        let columns = columns.into();
        assert!(!columns.is_empty(), "an index has a column");

        Self {
            name: name.into(),
            columns,
            unique,
            created: AtomicU64::new(0),
            entries: RwLock::new(BTreeSet::new()),
        }
    }

    /// the first indexed column, the one a single-value key bounds
    #[inline]
    pub fn leading(&self) -> usize {
        self.columns[0]
    }

    /// `row`'s key in this index
    #[inline]
    pub fn key(&self, row: &[Value]) -> Key {
        match &*self.columns {
            [column] => Key::One(row[*column].clone()),
            columns => Key::Many(columns.iter().map(|&column| row[column].clone()).collect()),
        }
    }

    /// whether `row` holds `key` (or begins with it, for a prefix)
    #[inline]
    pub fn holds(&self, row: &[Value], key: &Key) -> bool {
        key.values()
            .iter()
            .zip(&*self.columns)
            .all(|(value, &column)| row[column] == *value)
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
        key: &Key,
        id: RowId,
        holder: impl Fn(RowId) -> Result<Holder>,
    ) -> Result<Vec<RowId>> {
        let mut entries = self.entries.write().expect("index lock poisoned");
        let mut free = Vec::new();

        if self.unique && !key.has_null() {
            let others = entries
                .range(bounds(Bound::Included(key), Bound::Included(key)))
                .map(|&(_, other)| other)
                .filter(|&other| other != id);

            for other in others {
                match holder(other)? {
                    Holder::Free => free.push(other),
                    Holder::Committed => return Err(EngineError::DuplicatedKey(key.to_value())),
                    Holder::Uncommitted => return Err(EngineError::WriteConflict),
                }
            }
        }

        entries.insert((key.clone(), id));
        Ok(free)
    }

    /// adds `(key, id)` without any check (rebuilds, back-fill of older versions)
    pub fn insert_unchecked(&self, key: Key, id: RowId) {
        self.entries.write().expect("index lock poisoned").insert((key, id));
    }

    /// removes `(key, id)` when `unused` confirms, under the index lock, that no version needs it
    pub fn remove_if(&self, key: &Key, id: RowId, unused: impl FnOnce() -> bool) {
        let mut entries = self.entries.write().expect("index lock poisoned");
        let entry = (key.clone(), id);

        if entries.contains(&entry) && unused() {
            entries.remove(&entry);
        }
    }

    /// calls `f` for every entry within the bounds, in key order, under the read lock
    pub fn visit(&self, start: Bound<&Key>, end: Bound<&Key>, mut f: impl FnMut(&Key, RowId)) {
        let entries = self.entries.read().expect("index lock poisoned");
        for (key, id) in entries.range(bounds(start, end)) {
            f(key, *id);
        }
    }

    /// up to `limit` entries with keys within the bounds, strictly after `after` (or from the start)
    /// in ascending order, or strictly before it in descending order, under the read lock
    pub fn page(
        &self,
        (start, end): (Bound<&Key>, Bound<&Key>),
        after: Option<&(Key, RowId)>,
        descending: bool,
        limit: usize,
        mut f: impl FnMut(&Key, RowId),
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
        let page: Box<dyn Iterator<Item = &(Key, RowId)>> = match descending {
            false => Box::new(range),
            true => Box::new(range.rev()),
        };

        for (key, id) in page.take(limit) {
            f(key, *id);
        }
    }

    /// how many entries lie within the bounds, counting no further than `cap`
    pub fn count_within(&self, (start, end): (Bound<&Key>, Bound<&Key>), cap: usize) -> usize {
        let (low, high) = bounds(start, end);
        if empty(&low, &high) {
            return 0;
        }

        self.entries
            .read()
            .expect("index lock poisoned")
            .range((low, high))
            .take(cap)
            .count()
    }

    pub fn clear(&self) {
        self.entries.write().expect("index lock poisoned").clear();
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entries.read().expect("index lock poisoned").len()
    }
}

impl Key {
    #[inline]
    pub fn values(&self) -> &[Value] {
        match self {
            Self::One(value) => std::slice::from_ref(value),
            Self::Many(values) => values,
        }
    }

    #[inline]
    pub fn has_null(&self) -> bool {
        self.values().iter().any(Value::is_null)
    }

    /// the key as one value, for messages: a composite key reads `(a, b)`
    pub fn to_value(&self) -> Value {
        match self {
            Self::One(value) => value.clone(),
            Self::Many(values) => {
                let values: Vec<_> = values.iter().map(ToString::to_string).collect();
                Value::String(format!("({})", values.join(", ")).into())
            },
        }
    }
}

impl Ord for Key {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.values()
            .iter()
            .zip(other.values())
            .map(|(a, b)| a.cmp(b))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for Key {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl From<Value> for Key {
    #[inline]
    fn from(value: Value) -> Self {
        Self::One(value)
    }
}

impl From<Vec<Value>> for Key {
    fn from(mut values: Vec<Value>) -> Self {
        match values.len() {
            1 => Self::One(values.pop().unwrap()),
            _ => Self::Many(values.into()),
        }
    }
}

/// maps key bounds onto `(key, row id)` bounds covering every row id of the boundary keys
fn bounds(start: Bound<&Key>, end: Bound<&Key>) -> (EntryBound, EntryBound) {
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
/// a prefix ties with the keys it begins, yet as a start it lies below them and as an end above
/// them: bounds of different lengths are never found empty here
fn empty(low: &EntryBound, high: &EntryBound) -> bool {
    let (Bound::Included((low_key, _)) | Bound::Excluded((low_key, _))) = low else {
        return false;
    };
    let (Bound::Included((high_key, _)) | Bound::Excluded((high_key, _))) = high else {
        return false;
    };
    if low_key.values().len() != high_key.values().len() && low_key.cmp(high_key).is_eq() {
        return false;
    }

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

    fn keys(index: &Index, start: Bound<&Key>, end: Bound<&Key>) -> Vec<RowId> {
        let mut ids = Vec::new();
        index.visit(start, end, |_, id| ids.push(id));
        ids
    }

    fn pair(a: i64, b: i64) -> Key {
        Key::Many([Value::Number(a), Value::Number(b)].into())
    }

    #[test]
    fn ranges_cover_every_row_of_boundary_keys() {
        let index = Index::new("i", [0], false);
        for (key, id) in [(1, 10), (2, 20), (2, 21), (3, 30)] {
            index.insert_unchecked(Value::Number(key).into(), id);
        }

        let two = Key::One(Value::Number(2));
        assert_eq!(keys(&index, Bound::Included(&two), Bound::Included(&two)), [20, 21]);
        assert_eq!(keys(&index, Bound::Excluded(&two), Bound::Unbounded), [30]);
        assert_eq!(keys(&index, Bound::Unbounded, Bound::Excluded(&two)), [10]);
    }

    #[test]
    fn a_prefix_bounds_every_key_it_begins() {
        let index = Index::new("i", [0, 1], false);
        for (a, b, id) in [(1, 5, 10), (2, 1, 20), (2, 7, 21), (2, 9, 22), (3, 0, 30)] {
            index.insert_unchecked(pair(a, b), id);
        }

        let two = Key::One(Value::Number(2));
        assert_eq!(keys(&index, Bound::Included(&two), Bound::Included(&two)), [20, 21, 22]);
        assert_eq!(keys(&index, Bound::Excluded(&two), Bound::Unbounded), [30]);
        assert_eq!(keys(&index, Bound::Unbounded, Bound::Excluded(&two)), [10]);
        assert_eq!(keys(&index, Bound::Excluded(&pair(2, 1)), Bound::Included(&two)), [21, 22]);
        assert_eq!(keys(&index, Bound::Included(&two), Bound::Excluded(&pair(2, 9))), [20, 21]);

        let mut ids = Vec::new();
        let page = (Bound::Excluded(&pair(2, 1)), Bound::Included(&two));
        index.page(page, None, true, 10, |_, id| ids.push(id));
        assert_eq!(ids, [22, 21]);
        ids.clear();
        let range = (Bound::Included(&two), Bound::Excluded(&pair(2, 9)));
        index.page(range, None, false, 10, |_, id| ids.push(id));
        assert_eq!(ids, [20, 21]);
    }

    #[test]
    fn unique_insert_consults_holders_of_other_rows() {
        let index = Index::new("u", [0], true);
        let key = Key::One(Value::Number(7));

        index.insert(&key, 1, |_| unreachable!()).unwrap();
        index.insert(&key, 1, |_| unreachable!()).unwrap();
        assert!(matches!(
            index.insert(&key, 2, |_| Ok(Holder::Committed)),
            Err(EngineError::DuplicatedKey(_))
        ));
        assert!(matches!(
            index.insert(&key, 2, |_| Ok(Holder::Uncommitted)),
            Err(EngineError::WriteConflict)
        ));
        index.insert(&key, 2, |_| Ok(Holder::Free)).unwrap();
        index.insert(&Key::One(Value::Null), 3, |_| unreachable!()).unwrap();
        index.insert(&Key::One(Value::Null), 4, |_| unreachable!()).unwrap();
        assert_eq!(index.len(), 4);
    }

    #[test]
    fn a_composite_key_with_a_null_is_never_a_duplicate() {
        let index = Index::new("u", [0, 1], true);
        let partial = Key::Many([Value::Number(1), Value::Null].into());

        index.insert(&pair(1, 2), 1, |_| unreachable!()).unwrap();
        assert!(matches!(
            index.insert(&pair(1, 2), 2, |_| Ok(Holder::Committed)),
            Err(EngineError::DuplicatedKey(_))
        ));
        index.insert(&pair(1, 3), 2, |_| unreachable!()).unwrap();
        index.insert(&partial, 3, |_| unreachable!()).unwrap();
        index.insert(&partial, 4, |_| unreachable!()).unwrap();
        assert_eq!(index.len(), 4);
    }

    #[test]
    fn removal_is_conditional() {
        let index = Index::new("i", [0], false);
        let key = Key::One(Value::Number(1));
        index.insert_unchecked(key.clone(), 1);

        index.remove_if(&key, 1, || false);
        assert_eq!(index.len(), 1);
        index.remove_if(&key, 1, || true);
        assert_eq!(index.len(), 0);
    }
}
