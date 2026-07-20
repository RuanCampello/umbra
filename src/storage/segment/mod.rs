//! Cold storage: immutable columnar segments plus the per-table store
//! that manages them
//!
//! [SegmentStore] owns one table's frozen segments and their manifest
//! Rows are frozen out of the hot version store at checkpoint time, so
//! every frozen row is committed and visible to all future transactions.
//! Later modifications never touch the files:
//!
//! - **updates** put a fresh version in the hot store, which shadows the
//!   frozen copy on every read path
//! - **deletes** leave a deleted version in the hot store, once it ages past the
//!   cleanup retention it is converted into a persistent tombstone here
//! - **re-frozen rows** live in a newer segment, readers
//!   walk segments newest-first, so the newest frozen copy wins

mod format;

pub(crate) use format::FrozenSegment;

use crate::collections::hash::HashSet;
use crate::db::SchemaNew;
use crate::sql::Value;
use crate::storage::mvcc::fnv1a;
use crate::vm::planner::Tuple;
use std::io::{self, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// A table's cold half: frozen segments, tombstones and their manifest
pub(crate) struct SegmentStore {
    dir: PathBuf,
    /// Ascending by id, readers walk it in reverse so newer segments
    /// shadow older copies of a re-frozen row
    segments: RwLock<Vec<(u64, Arc<FrozenSegment>)>>,
    /// Row ids deleted after being frozen. Only ever grows, rows vanish
    /// from here when compaction rewrites their segments without them
    tombstones: RwLock<HashSet<i64>>,
    next_id: AtomicU64,
}

const MANIFEST_MAGIC: [u8; 4] = *b"UMF1";
const MANIFEST_VERSION: u16 = 1;
const MANIFEST_FILE: &str = "manifest.bin";

impl SegmentStore {
    /// Opens the store rooted at `dir`, loading every segment the manifest references
    pub fn open(dir: PathBuf) -> io::Result<Self> {
        std::fs::create_dir_all(&dir)?;

        let manifest = dir.join(MANIFEST_FILE);
        let (ids, tombstones) = match manifest.exists() {
            true => decode_manifest(&std::fs::read(&manifest)?)?,
            false => (Vec::new(), HashSet::default()),
        };

        let mut segments = Vec::with_capacity(ids.len());
        for id in ids {
            let segment = FrozenSegment::read(&segment_path(&dir, id))?;
            segments.push((id, Arc::new(segment)));
        }
        segments.sort_unstable_by_key(|(id, _)| *id);

        let next_id = segments.last().map(|(id, _)| id + 1).unwrap_or(0);

        Ok(Self {
            dir,
            segments: RwLock::new(segments),
            tombstones: RwLock::new(tombstones),
            next_id: AtomicU64::new(next_id),
        })
    }

    /// Freezes `rows` into a new segment file and records it in the manifest
    pub fn seal(&self, schema: &SchemaNew, rows: Vec<(i64, Tuple)>) -> io::Result<()> {
        assert!(!rows.is_empty(), "sealing an empty batch");

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let segment = FrozenSegment::build(schema, rows);
        segment.write(&segment_path(&self.dir, id))?;

        self.segments.write().unwrap().push((id, Arc::new(segment)));
        self.persist_manifest()
    }

    /// Records permanent tombstones and persists the manifest
    pub fn add_tombstones(&self, row_ids: impl IntoIterator<Item = i64>) -> io::Result<()> {
        self.tombstones.write().unwrap().extend(row_ids);
        self.persist_manifest()
    }

    /// Merges every segment into one, keeping the newest copy per row and
    /// dropping tombstoned rows for good
    pub fn compact(&self, schema: &SchemaNew, threshold: usize) -> io::Result<bool> {
        {
            let segments = self.segments.read().unwrap();
            let tombstones = self.tombstones.read().unwrap();
            let due =
                segments.len() > threshold || (!tombstones.is_empty() && !segments.is_empty());
            if !due {
                return Ok(false);
            }
        }

        let mut rows = self.collect(|_| false);
        rows.sort_unstable_by_key(|&(row_id, _)| row_id);

        let old: Vec<u64> = {
            let segments = self.segments.read().unwrap();
            segments.iter().map(|(id, _)| *id).collect()
        };

        let merged = match rows.is_empty() {
            true => None,
            false => {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let segment = FrozenSegment::build(schema, rows);
                segment.write(&segment_path(&self.dir, id))?;
                Some((id, Arc::new(segment)))
            }
        };

        {
            let mut segments = self.segments.write().unwrap();
            let mut tombstones = self.tombstones.write().unwrap();
            *segments = merged.into_iter().collect();
            tombstones.clear();
        }
        self.persist_manifest()?;

        for id in old {
            let _ = std::fs::remove_file(segment_path(&self.dir, id));
        }

        Ok(true)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.segments.read().unwrap().is_empty()
    }

    #[inline]
    pub fn is_tombstoned(&self, row_id: i64) -> bool {
        self.tombstones.read().unwrap().contains(&row_id)
    }

    /// whether a live frozen copy of `row_id` exists
    pub fn contains(&self, row_id: i64) -> bool {
        !self.is_tombstoned(row_id) && self.is_frozen(row_id)
    }

    /// whether any segment holds `row_id`, tombstoned or not
    pub fn is_frozen(&self, row_id: i64) -> bool {
        self.segments
            .read()
            .unwrap()
            .iter()
            .any(|(_, segment)| segment.position(row_id).is_some())
    }

    /// The newest frozen copy of `row_id`, unless tombstoned.
    pub fn get(&self, row_id: i64) -> Option<Tuple> {
        if self.is_tombstoned(row_id) {
            return None;
        }

        let segments = self.segments.read().unwrap();
        segments
            .iter()
            .rev()
            .find_map(|(_, segment)| segment.position(row_id).map(|index| segment.tuple(index)))
    }

    pub fn collect(&self, mut shadowed: impl FnMut(i64) -> bool) -> Vec<(i64, Tuple)> {
        let tombstones = self.tombstones.read().unwrap();
        let segments = self.segments.read().unwrap();

        let mut seen: HashSet<i64> = HashSet::default();
        let mut rows = Vec::new();

        for (_, segment) in segments.iter().rev() {
            for (index, &row_id) in segment.row_ids().iter().enumerate() {
                if seen.contains(&row_id) || tombstones.contains(&row_id) || shadowed(row_id) {
                    continue;
                }

                seen.insert(row_id);
                rows.push((row_id, segment.tuple(index)));
            }
        }

        rows
    }

    pub fn max_row_id(&self) -> Option<i64> {
        self.segments
            .read()
            .unwrap()
            .iter()
            .filter_map(|(_, segment)| segment.row_ids().last().copied())
            .max()
    }

    pub fn max_number_at(&self, column: usize) -> Option<i128> {
        self.segments
            .read()
            .unwrap()
            .iter()
            .filter_map(|(_, segment)| match segment.zone(column).max() {
                Some(Value::Number(n)) => Some(*n),
                _ => None,
            })
            .max()
    }

    fn persist_manifest(&self) -> io::Result<()> {
        let segments = self.segments.read().unwrap();
        let tombstones = self.tombstones.read().unwrap();

        let mut buff = Vec::new();
        buff.extend_from_slice(&MANIFEST_MAGIC);
        buff.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());

        buff.extend_from_slice(&(segments.len() as u32).to_le_bytes());
        for (id, _) in segments.iter() {
            buff.extend_from_slice(&id.to_le_bytes());
        }

        buff.extend_from_slice(&(tombstones.len() as u32).to_le_bytes());
        for row_id in tombstones.iter() {
            buff.extend_from_slice(&row_id.to_le_bytes());
        }

        buff.extend_from_slice(&fnv1a(&buff).to_le_bytes());

        drop(segments);
        drop(tombstones);

        let path = self.dir.join(MANIFEST_FILE);
        let tmp = path.with_extension("tmp");
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)?;
            file.write_all(&buff)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        std::fs::File::open(&self.dir)?.sync_all()?;

        Ok(())
    }
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.seg"))
}

fn decode_manifest(bytes: &[u8]) -> io::Result<(Vec<u64>, HashSet<i64>)> {
    let corrupt = |message: &str| Error::new(ErrorKind::InvalidData, message.to_string());

    if bytes.len() < 4 + 2 + 4 + 4 + 4 {
        return Err(corrupt("manifest too short"));
    }

    let (payload, tail) = bytes.split_at(bytes.len() - 4);
    let stored = u32::from_le_bytes(tail.try_into().unwrap());
    if fnv1a(payload) != stored {
        return Err(corrupt("manifest checksum mismatch"));
    }

    if payload[..4] != MANIFEST_MAGIC {
        return Err(corrupt("bad manifest magic"));
    }
    match u16::from_le_bytes(payload[4..6].try_into().unwrap()) {
        MANIFEST_VERSION => {}
        other => return Err(corrupt(&format!("unsupported manifest version {other}"))),
    }

    let mut cursor = 6usize;
    let mut take = |len: usize| -> io::Result<&[u8]> {
        match payload.get(cursor..cursor + len) {
            Some(slice) => {
                cursor += len;
                Ok(slice)
            }
            None => Err(Error::new(ErrorKind::InvalidData, "truncated manifest")),
        }
    };

    let segment_count = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
    let mut ids = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        ids.push(u64::from_le_bytes(take(8)?.try_into().unwrap()));
    }

    let tombstone_count = u32::from_le_bytes(take(4)?.try_into().unwrap()) as usize;
    let mut tombstones = HashSet::default();
    for _ in 0..tombstone_count {
        tombstones.insert(i64::from_le_bytes(take(8)?.try_into().unwrap()));
    }

    Ok((ids, tombstones))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::SchemaBuilder;
    use crate::sql::statement::{Column as AstColumn, Constraint, Type};

    fn schema() -> SchemaNew {
        SchemaBuilder::new("cold").from_ast_columns(&[
            AstColumn {
                name: "id".into(),
                data_type: Type::Integer,
                constraints: vec![Constraint::PrimaryKey],
                type_def: None,
            },
            AstColumn {
                name: "name".into(),
                data_type: Type::Varchar(64),
                constraints: vec![Constraint::Nullable],
                type_def: None,
            },
        ])
    }

    fn row(id: i64, name: &str) -> (i64, Tuple) {
        (
            id,
            vec![Value::Number(id as i128), Value::String(name.into())],
        )
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("umbra_segment_store_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn seal_lookup_and_shadowing() {
        let dir = scratch("seal");
        let store = SegmentStore::open(dir.clone()).unwrap();
        let schema = schema();

        store
            .seal(&schema, vec![row(1, "alice"), row(2, "bob")])
            .unwrap();
        // row 2 updated after freezing, then frozen again
        store
            .seal(&schema, vec![row(2, "bobby"), row(3, "carol")])
            .unwrap();

        assert_eq!(store.get(1), Some(row(1, "alice").1));
        assert_eq!(store.get(2), Some(row(2, "bobby").1));
        assert!(store.contains(3));
        assert!(!store.contains(9));

        let rows = store.collect(|row_id| row_id == 3);
        let mut ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
        assert!(rows.contains(&row(2, "bobby")));

        assert_eq!(store.max_row_id(), Some(3));
        assert_eq!(store.max_number_at(0), Some(3));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tombstones_hide_rows_and_survive_reopen() {
        let dir = scratch("tombstone");
        let store = SegmentStore::open(dir.clone()).unwrap();
        let schema = schema();

        store
            .seal(
                &schema,
                vec![row(1, "alice"), row(2, "bob"), row(3, "carol")],
            )
            .unwrap();
        store.add_tombstones([2]).unwrap();

        assert!(store.is_tombstoned(2));
        assert_eq!(store.get(2), None);
        assert!(!store.contains(2));
        assert_eq!(store.collect(|_| false).len(), 2);

        drop(store);
        let reopened = SegmentStore::open(dir.clone()).unwrap();
        assert!(reopened.is_tombstoned(2));
        assert_eq!(reopened.get(1), Some(row(1, "alice").1));
        assert_eq!(reopened.get(3), Some(row(3, "carol").1));
        assert_eq!(reopened.max_row_id(), Some(3));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_merges_segments_and_drops_tombstones() {
        let dir = scratch("compact");
        let store = SegmentStore::open(dir.clone()).unwrap();
        let schema = schema();

        store
            .seal(&schema, vec![row(1, "alice"), row(2, "bob")])
            .unwrap();
        store
            .seal(&schema, vec![row(2, "bobby"), row(3, "carol")])
            .unwrap();
        store.add_tombstones([1]).unwrap();

        assert!(store.compact(&schema, 4).unwrap());
        assert_eq!(store.segments.read().unwrap().len(), 1);
        assert!(store.tombstones.read().unwrap().is_empty());

        assert_eq!(store.get(1), None);
        assert_eq!(store.get(2), Some(row(2, "bobby").1));
        assert_eq!(store.get(3), Some(row(3, "carol").1));

        // idle store: nothing to merge
        assert!(!store.compact(&schema, 4).unwrap());

        // old segment files are gone, the merged one survives a reopen
        drop(store);
        let reopened = SegmentStore::open(dir.clone()).unwrap();
        assert_eq!(reopened.get(2), Some(row(2, "bobby").1));
        assert_eq!(reopened.get(1), None);
        assert_eq!(
            std::fs::read_dir(&dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .path()
                        .extension()
                        .is_some_and(|ext| ext == "seg")
                })
                .count(),
            1
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn compaction_of_fully_tombstoned_store_empties_it() {
        let dir = scratch("compact-empty");
        let store = SegmentStore::open(dir.clone()).unwrap();
        let schema = schema();

        store.seal(&schema, vec![row(1, "alice")]).unwrap();
        store.add_tombstones([1]).unwrap();

        assert!(store.compact(&schema, 4).unwrap());
        assert!(store.is_empty());
        assert!(!store.is_tombstoned(1));
        assert_eq!(store.get(1), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_manifest_is_rejected() {
        let dir = scratch("corrupt");
        let store = SegmentStore::open(dir.clone()).unwrap();
        store.seal(&schema(), vec![row(1, "alice")]).unwrap();
        drop(store);

        let manifest = dir.join(MANIFEST_FILE);
        let mut bytes = std::fs::read(&manifest).unwrap();
        let middle = bytes.len() / 2;
        bytes[middle] ^= 0xff;
        std::fs::write(&manifest, bytes).unwrap();

        assert!(SegmentStore::open(dir.clone()).is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
