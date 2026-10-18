//! Checkpoint images and the manifest naming the current one
//!
//! Checkpoint image:
//!
//! | Field          | Type     | Notes                                   |
//! |----------------|----------|-----------------------------------------|
//! | magic          | `[u8;4]` | `"UIM1"`                                |
//! | version        | `u16`    |                                         |
//! | checkpoint     | `u64`    | `C`                                     |
//! | table id       | `u32`    |                                         |
//! | schema         | blob     |                                         |
//! | index count    | `u16`    |                                         |
//! | indexes        | repeated | `column u16`, `unique u8`, `name`       |
//! | next row id    | `i64`    |                                         |
//! | serial count   | `u16`    |                                         |
//! | serials        | repeated | `column u16`, `value i64`               |
//! | rows           | repeated | `1 u8`, `row id i64`, `row blob`        |
//! | end of rows    | `u8`     | `0`                                     |
//! | row count      | `u64`    |                                         |
//! | checksum       | `u32`    | crc32c                                  |
//!
//! `MANIFEST`:
//!
//! | Field          | Type     | Notes                                   |
//! |----------------|----------|-----------------------------------------|
//! | magic          | `[u8;4]` | `"UMN1"`                                |
//! | version        | `u16`    |                                         |
//! | checkpoint     | `u64`    | `C`                                     |
//! | start lsn      | `u64`    |                                         |
//! | table count    | `u32`    |                                         |
//! | table ids      | `u32`    | repeated `table count` times            |
//! | checksum       | `u32`    | crc32c                                  |
//!
//! Every file is written whole, synced, and only then referenced: images before their directory is
//! synced, the manifest through `MANIFEST.tmp` -> rename -> directory sync
//! Nothing is overwritten in place, so a torn write can only hit a file nothing points at yet

use crate::core::crc32c::{crc32c, crc32c_append};
use crate::db::Schema;
use crate::storage::engine::table::{Row, RowId, Table, View};
use crate::storage::engine::{
    chunk::CHUNK_SIZE,
    codec::{Cursor, Stream, put_blob, put_row, put_string},
    error::{EngineError, Result},
    index::Index,
    io::Storage,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub(crate) struct Manifest {
    pub ts: u64,
    pub start_lsn: u64,
    pub tables: Vec<u32>,
}

const IMAGE_MAGIC: [u8; 4] = *b"UIM1";
const MANIFEST_MAGIC: [u8; 4] = *b"UMN1";
const VERSION: u16 = 1;
/// an index lists its columns (`count u8 | column u16*`), 1 had a single `column u16`
const IMAGE_VERSION: u16 = 2;

const ROW: u8 = 1;
const END: u8 = 0;
/// Rows are appended to an image file in buffers of about this size
const FLUSH_BYTES: usize = 1 << 20;

pub(crate) const MANIFEST: &str = "MANIFEST";
pub(crate) const CHECKPOINTS: &str = "ckpt";

impl Manifest {
    pub fn read(storage: &dyn Storage, dir: &Path) -> Result<Option<Self>> {
        let path = dir.join(MANIFEST);
        if !storage.exists(&path) {
            return Ok(None);
        }

        let bytes = storage.read(&path)?;
        let mut cursor = Cursor::new(checked(&bytes, "manifest")?, "manifest");

        if cursor.bytes(4)? != MANIFEST_MAGIC || cursor.u16()? != VERSION {
            return Err(cursor.corrupt("unknown format"));
        }

        let ts = cursor.u64()?;
        let start_lsn = cursor.u64()?;
        let tables = (0..cursor.u32()?).map(|_| cursor.u32()).collect::<Result<_>>()?;

        Ok(Some(Self { ts, start_lsn, tables }))
    }

    /// atomically replaces the manifest
    pub fn write(&self, storage: &dyn Storage, dir: &Path) -> Result<()> {
        let mut buf = MANIFEST_MAGIC.to_vec();
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&self.ts.to_le_bytes());
        buf.extend_from_slice(&self.start_lsn.to_le_bytes());
        buf.extend_from_slice(&(self.tables.len() as u32).to_le_bytes());
        for table in &self.tables {
            buf.extend_from_slice(&table.to_le_bytes());
        }

        let tmp = dir.join("MANIFEST.tmp");
        write_whole(storage, &tmp, buf)?;
        storage.rename(&tmp, &dir.join(MANIFEST))?;
        storage.sync_dir(dir)?;

        Ok(())
    }
}

/// the directory holding the images of checkpoint `ts`
pub(crate) fn directory(dir: &Path, ts: u64) -> PathBuf {
    dir.join(CHECKPOINTS).join(format!("{ts:020}"))
}

pub(crate) fn image_path(dir: &Path, ts: u64, table: u32) -> PathBuf {
    directory(dir, ts).join(format!("{table}.img"))
}

/// writes and syncs the image of `table`, streaming the rows `next_batch` yields (visible at
/// `ts`, ascending ids) in bounded buffers
pub(crate) fn write_image(
    storage: &dyn Storage,
    path: &Path,
    table: &Table,
    ts: u64,
    mut next_batch: impl FnMut(&mut Vec<(RowId, Row)>) -> Result<bool>,
) -> Result<()> {
    let mut buf = IMAGE_MAGIC.to_vec();
    buf.extend_from_slice(&IMAGE_VERSION.to_le_bytes());
    buf.extend_from_slice(&ts.to_le_bytes());
    buf.extend_from_slice(&table.id.to_le_bytes());
    put_blob(&mut buf, &table.schema.encode(table.name()));

    // an index whose creator had not committed by `ts` is not part of the image
    let pinned = View { snapshot: ts, marker: 0 };
    let mut indexes = table.indexes();
    indexes.retain(|index| pinned.sees(index.created()));
    buf.extend_from_slice(&(indexes.len() as u16).to_le_bytes());
    for index in &indexes {
        buf.push(index.columns.len() as u8);
        for &column in &index.columns {
            buf.extend_from_slice(&(column as u16).to_le_bytes());
        }
        buf.push(index.unique as u8);
        put_string(&mut buf, &index.name);
    }

    buf.extend_from_slice(&table.next_row_id().to_le_bytes());
    let serials = table.serials();
    buf.extend_from_slice(&(serials.len() as u16).to_le_bytes());
    for (column, value) in serials {
        buf.extend_from_slice(&(column as u16).to_le_bytes());
        buf.extend_from_slice(&value.to_le_bytes());
    }

    let mut file = storage.create(path)?;
    let mut crc = 0;
    let mut flush = |buf: &mut Vec<u8>, crc: &mut u32| -> Result<()> {
        *crc = crc32c_append(*crc, buf);
        file.append(buf)?;
        Ok(buf.clear())
    };

    let mut rows = Vec::new();
    let mut count = 0u64;
    let mut encoded = Vec::new();
    while next_batch(&mut rows)? {
        for (id, row) in rows.drain(..) {
            encoded.clear();
            put_row(&mut encoded, &row);

            buf.push(ROW);
            buf.extend_from_slice(&id.to_le_bytes());
            put_blob(&mut buf, &encoded);
            count += 1;
        }

        if buf.len() >= FLUSH_BYTES {
            flush(&mut buf, &mut crc)?;
        }
    }

    buf.push(END);
    buf.extend_from_slice(&count.to_le_bytes());
    flush(&mut buf, &mut crc)?;

    drop(flush);
    file.append(&crc.to_le_bytes())?;
    file.sync()?;
    Ok(())
}

/// rebuilds a table from its image, streaming it
/// rows are stamped `ts`, indexes are registered empty, and `settle` runs after
/// every full chunk so a memory budget can evict while loading
pub(crate) fn read_image(
    storage: &dyn Storage,
    path: &Path,
    new_table: impl FnOnce(u32, String, Schema) -> Arc<Table>,
    mut settle: impl FnMut(&Arc<Table>),
) -> Result<Arc<Table>> {
    let mut stream = Stream::new(storage.reader(path)?, "image");

    if stream.exact::<4>()? != IMAGE_MAGIC {
        return Err(stream.corrupt("unknown format"));
    }
    let version = match stream.u16()? {
        version @ (1 | IMAGE_VERSION) => version,
        _ => return Err(stream.corrupt("unknown format")),
    };

    let ts = stream.u64()?;
    let id = stream.u32()?;
    let (name, schema) = Schema::decode(&stream.blob()?)
        .map_err(|error| EngineError::Corrupt(format!("schema in image of table {id}: {error}")))?;
    let table = new_table(id, name, schema);

    for _ in 0..stream.u16()? {
        let count = match version {
            1 => 1,
            _ => stream.u8()?,
        };
        let columns =
            (0..count).map(|_| Ok(stream.u16()? as usize)).collect::<Result<Box<[_]>>>()?;
        let unique = stream.u8()? != 0;
        let name = stream.string()?;

        if table.index(&name).is_none() {
            table.add_index(Arc::new(Index::new(name, columns, unique)));
        }
    }

    let next_row_id = stream.i64()?;
    for _ in 0..stream.u16()? {
        let column = stream.u16()? as usize;
        table.raise_serial(column, stream.i64()?);
    }

    let mut loaded = 0u64;
    loop {
        match stream.u8()? {
            ROW => {
                let row_id = stream.i64()?;
                let bytes = stream.blob()?;
                table.recover_put(row_id, Cursor::new(&bytes, "image row").row()?, ts)?;
                loaded += 1;

                if loaded % CHUNK_SIZE as u64 == 0 {
                    settle(&table);
                }
            },
            END if stream.u64()? == loaded => break,
            END => return Err(stream.corrupt("row count mismatch")),
            _ => return Err(stream.corrupt("unknown frame")),
        }
    }

    table.raise_row_id(next_row_id);
    stream.finish()?;
    Ok(table)
}

/// splits off and verifies the CRC32C footer
fn checked<'b>(bytes: &'b [u8], what: &str) -> Result<&'b [u8]> {
    let split = bytes
        .len()
        .checked_sub(4)
        .ok_or_else(|| EngineError::Corrupt(format!("{what} is truncated")))?;
    let (content, footer) = bytes.split_at(split);

    match crc32c(content) == u32::from_le_bytes(footer.try_into().unwrap()) {
        true => Ok(content),
        _ => Err(EngineError::Corrupt(format!("{what} checksum mismatch"))),
    }
}

fn write_whole(storage: &dyn Storage, path: &Path, mut buf: Vec<u8>) -> Result<()> {
    buf.extend_from_slice(&crc32c(&buf).to_le_bytes());

    let mut file = storage.create(path)?;
    file.append(&buf)?;
    file.sync()?;

    Ok(())
}
