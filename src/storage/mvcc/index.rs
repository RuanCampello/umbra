use crate::{
    collections::{hash::HashMap, smallvec::SmallVec},
    sql::Value,
};
use std::{
    collections::BTreeMap,
    io::{Error, ErrorKind},
    ops::Bound,
    sync::RwLock,
};

/// MVCC-aware index trait.
///
/// Implementations must be `Send + Sync` for concurrent access from
/// multiple transactions via `Arc<dyn Index>`.
pub(crate) trait Index: Send + Sync {
    /// Returns the index name.
    fn name(&self) -> &str;

    /// Returns the column position in the tuple that this index covers.
    fn column_index(&self) -> usize;

    /// Whether this index enforces uniqueness.
    fn is_unique(&self) -> bool;

    /// Adds a mapping from `value` to `row_id`.
    ///
    /// For unique indexes, returns an error if the value already exists.
    fn add(&self, value: &Value, row_id: i64) -> Result<()>;

    /// Removes the mapping from `value` to `row_id`.
    fn remove(&self, value: &Value, row_id: i64);

    /// Exact-match lookup: returns all row IDs associated with `value`.
    fn find(&self, value: &Value) -> Vec<i64>;

    /// Range scan: returns all row IDs whose keys fall within the given bounds.
    fn find_range(&self, start: Bound<&Value>, end: Bound<&Value>) -> Vec<i64>;

    /// Clears all entries from the index (used by TRUNCATE).
    fn clear(&self);
}

/// In-memory B-tree index backed by `BTreeMap<Value, SmallVec<i64, 1>>`.
///
/// Uses `Value::Ord` for key ordering, which correctly handles all SQL types.
/// Both the forward map (`entries`) and reverse map (`row_to_value`) share a
/// single `RwLock` to avoid split-brain inconsistency.
pub(crate) struct BTreeIndex {
    name: String,
    column_index: usize,
    unique: bool,
    data: RwLock<IndexData>,
}

/// Combined entries + reverse map under a single lock.
struct IndexData {
    /// Ordered mapping from column value to row IDs.
    entries: BTreeMap<Value, SmallVec<i64, 1>>,
    /// Reverse mapping for efficient removal during UPDATE/DELETE.
    row_to_value: HashMap<i64, Value>,
}

type Result<T> = std::result::Result<T, Error>;

impl BTreeIndex {
    pub fn new(name: impl Into<String>, column_index: usize, unique: bool) -> Self {
        Self {
            name: name.into(),
            column_index,
            unique,
            data: RwLock::new(IndexData {
                entries: BTreeMap::new(),
                row_to_value: HashMap::default(),
            }),
        }
    }

    /// Removes a row_id from the index using the reverse map to find the key.
    ///
    /// Useful when the caller doesn't have the old value (e.g. blind deletes).
    pub fn remove_by_row_id(&self, row_id: i64) {
        let mut data = self.data.write().unwrap();

        let Some(value) = data.row_to_value.remove(&row_id) else {
            return;
        };

        if let Some(row_ids) = data.entries.get_mut(&value) {
            row_ids.retain(|&id| id != row_id);
            if row_ids.is_empty() {
                data.entries.remove(&value);
            }
        }
    }
}

impl Index for BTreeIndex {
    #[inline]
    fn name(&self) -> &str {
        &self.name
    }

    #[inline]
    fn column_index(&self) -> usize {
        self.column_index
    }

    #[inline]
    fn is_unique(&self) -> bool {
        self.unique
    }

    #[inline]
    fn add(&self, value: &Value, row_id: i64) -> Result<()> {
        let mut data = self.data.write().unwrap();

        if self.unique {
            if let Some(existing) = data.entries.get(value) {
                if !existing.is_empty() {
                    return Err(Error::new(
                        ErrorKind::AlreadyExists,
                        format!(
                            "unique index '{}' violation: duplicate key '{value}'",
                            self.name
                        ),
                    ));
                }
            }
        }

        data.entries
            .entry(value.clone())
            .or_insert_with(SmallVec::new)
            .push(row_id);

        data.row_to_value.insert(row_id, value.clone());
        Ok(())
    }

    #[inline]
    fn remove(&self, value: &Value, row_id: i64) {
        let mut data = self.data.write().unwrap();

        if let Some(row_ids) = data.entries.get_mut(value) {
            row_ids.retain(|&id| id != row_id);

            if row_ids.is_empty() {
                data.entries.remove(value);
            }
        }

        data.row_to_value.remove(&row_id);
    }

    #[inline]
    fn find(&self, value: &Value) -> Vec<i64> {
        let data = self.data.read().unwrap();

        data.entries
            .get(value)
            .map(|ids| ids.to_vec())
            .unwrap_or_default()
    }

    #[inline]
    fn find_range(&self, start: Bound<&Value>, end: Bound<&Value>) -> Vec<i64> {
        let data = self.data.read().unwrap();

        data.entries
            .range((start.cloned(), end.cloned()))
            .flat_map(|(_, ids)| ids.iter().copied())
            .collect()
    }

    #[inline]
    fn clear(&self) {
        let mut data = self.data.write().unwrap();
        data.entries.clear();
        data.row_to_value.clear();
    }
}

impl IndexData {
    /// Lookup the value associated with a row ID via the reverse map.
    fn value_for_row(&self, row_id: i64) -> Option<&Value> {
        self.row_to_value.get(&row_id)
    }
}

trait ClonedBound {
    fn cloned(&self) -> Bound<Value>;
}

impl ClonedBound for Bound<&Value> {
    fn cloned(&self) -> Bound<Value> {
        match self {
            Bound::Included(v) => Bound::Included((*v).clone()),
            Bound::Excluded(v) => Bound::Excluded((*v).clone()),
            Bound::Unbounded => Bound::Unbounded,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_find() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::Number(10), 1).unwrap();
        index.add(&Value::Number(20), 2).unwrap();
        index.add(&Value::Number(10), 3).unwrap();

        assert_eq!(index.find(&Value::Number(10)), vec![1, 3]);
        assert_eq!(index.find(&Value::Number(20)), vec![2]);
        assert_eq!(index.find(&Value::Number(30)), Vec::<i64>::new());
    }

    #[test]
    fn remove() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::Number(10), 1).unwrap();
        index.add(&Value::Number(10), 2).unwrap();

        index.remove(&Value::Number(10), 1);

        assert_eq!(index.find(&Value::Number(10)), vec![2]);
    }

    #[test]
    fn remove_by_row_id() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::Number(42), 1).unwrap();
        index.add(&Value::Number(99), 2).unwrap();

        index.remove_by_row_id(1);

        assert_eq!(index.find(&Value::Number(42)), Vec::<i64>::new());
        assert_eq!(index.find(&Value::Number(99)), vec![2]);
    }

    #[test]
    fn remove_last_entry_cleans_up() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::Number(10), 1).unwrap();
        index.remove(&Value::Number(10), 1);

        // the key should be completely gone
        assert!(index.data.read().unwrap().entries.is_empty());
        assert!(index.data.read().unwrap().row_to_value.is_empty());
    }

    #[test]
    fn range_query() {
        let index = BTreeIndex::new("test_idx", 0, false);

        for i in 1..=10 {
            index.add(&Value::Number(i), i as i64).unwrap();
        }

        // inclusive [3, 7]
        let result = index.find_range(
            Bound::Included(&Value::Number(3)),
            Bound::Included(&Value::Number(7)),
        );
        assert_eq!(result, vec![3, 4, 5, 6, 7]);

        // exclusive (3, 7)
        let result = index.find_range(
            Bound::Excluded(&Value::Number(3)),
            Bound::Excluded(&Value::Number(7)),
        );
        assert_eq!(result, vec![4, 5, 6]);

        // half-open [5, ∞)
        let result = index.find_range(Bound::Included(&Value::Number(5)), Bound::Unbounded);
        assert_eq!(result, vec![5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn unique_violation() {
        let index = BTreeIndex::new("test_idx", 0, true);

        index.add(&Value::Number(1), 1).unwrap();

        let err = index.add(&Value::Number(1), 2);
        assert!(err.is_err());
        assert!(err
            .unwrap_err()
            .to_string()
            .contains("unique index 'test_idx' violation"));
    }

    #[test]
    fn unique_allows_reinsert_after_removal() {
        let index = BTreeIndex::new("test_idx", 0, true);

        index.add(&Value::Number(1), 1).unwrap();
        index.remove(&Value::Number(1), 1);

        // should succeed since the value was removed
        index.add(&Value::Number(1), 2).unwrap();
        assert_eq!(index.find(&Value::Number(1)), vec![2]);
    }

    #[test]
    fn duplicate_values_non_unique() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::Number(42), 1).unwrap();
        index.add(&Value::Number(42), 2).unwrap();
        index.add(&Value::Number(42), 3).unwrap();

        assert_eq!(index.find(&Value::Number(42)), vec![1, 2, 3]);
    }

    #[test]
    fn string_keys() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::String("banana".into()), 1).unwrap();
        index.add(&Value::String("apple".into()), 2).unwrap();
        index.add(&Value::String("cherry".into()), 3).unwrap();

        assert_eq!(index.find(&Value::String("banana".into())), vec![1]);

        // range query should be sorted by string Ord (alphabetical)
        let result = index.find_range(
            Bound::Included(&Value::String("apple".into())),
            Bound::Included(&Value::String("cherry".into())),
        );
        // apple(2), banana(1), cherry(3) in alphabetical order
        assert_eq!(result, vec![2, 1, 3]);
    }

    #[test]
    fn clear() {
        let index = BTreeIndex::new("test_idx", 0, false);

        index.add(&Value::Number(1), 1).unwrap();
        index.add(&Value::Number(2), 2).unwrap();

        index.clear();

        assert_eq!(index.find(&Value::Number(1)), Vec::<i64>::new());
        assert!(index.data.read().unwrap().entries.is_empty());
        assert!(index.data.read().unwrap().row_to_value.is_empty());
    }
}
