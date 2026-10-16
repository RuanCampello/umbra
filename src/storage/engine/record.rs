//! Payloads of log frames
//!
//! ```text
//! Commit = ts u64 | count u32 | (table u32 | row id i64 | op u8 | row if op ≠ delete)*
//! Ddl    = ts u64 | op u8 | body
//!          create table: table u32 | schema blob
//!          drop table:   table u32
//!          create index: table u32 | column u16 | unique u8 | name
//! ```

use crate::db::Schema;
use crate::storage::engine::codec::{Cursor, put_blob, put_row, put_string};
use crate::storage::engine::error::{EngineError, Result};
use crate::storage::engine::table::{Row, RowId};
use crate::storage::engine::transaction::Write;

pub(crate) enum Change {
    Put(Row),
    Delete,
}

pub(crate) enum Ddl {
    CreateTable { table: u32, name: String, schema: Schema },
    DropTable { table: u32 },
    CreateIndex { table: u32, column: usize, unique: bool, name: String },
}

const INSERT: u8 = 1;
const UPDATE: u8 = 2;
const DELETE: u8 = 3;

const CREATE_TABLE: u8 = 1;
const DROP_TABLE: u8 = 2;
const CREATE_INDEX: u8 = 3;

/// encodes a write set with a zero timestamp, set later by [stamp_timestamp]
pub(crate) fn encode_commit(writes: &[Write]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12 + writes.len() * 32);
    buf.extend_from_slice(&0u64.to_le_bytes());
    buf.extend_from_slice(&(writes.len() as u32).to_le_bytes());

    for write in writes {
        buf.extend_from_slice(&write.table.id.to_le_bytes());
        buf.extend_from_slice(&write.id.to_le_bytes());

        match (&write.before, &write.after) {
            (_, None) => buf.push(DELETE),
            (None, Some(row)) => {
                buf.push(INSERT);
                put_row(&mut buf, row);
            },
            (Some(_), Some(row)) => {
                buf.push(UPDATE);
                put_row(&mut buf, row);
            },
        }
    }

    buf
}

pub(crate) fn encode_ddl(ddl: &Ddl) -> Vec<u8> {
    let mut buf = 0u64.to_le_bytes().to_vec();

    match ddl {
        Ddl::CreateTable { table, name, schema } => {
            buf.push(CREATE_TABLE);
            buf.extend_from_slice(&table.to_le_bytes());
            put_blob(&mut buf, &schema.encode(name));
        },
        Ddl::DropTable { table } => {
            buf.push(DROP_TABLE);
            buf.extend_from_slice(&table.to_le_bytes());
        },
        Ddl::CreateIndex { table, column, unique, name } => {
            buf.push(CREATE_INDEX);
            buf.extend_from_slice(&table.to_le_bytes());
            buf.extend_from_slice(&(*column as u16).to_le_bytes());
            buf.push(*unique as u8);
            put_string(&mut buf, name);
        },
    }

    buf
}

/// writes the commit timestamp into an encoded payload
#[inline]
pub(crate) fn stamp_timestamp(payload: &mut [u8], ts: u64) {
    payload[..8].copy_from_slice(&ts.to_le_bytes());
}

/// decodes a commit, handing every change and the commit timestamp to `apply`
/// returns the timestamp
pub(crate) fn decode_commit(
    payload: &[u8],
    mut apply: impl FnMut(u64, u32, RowId, Change) -> Result<()>,
) -> Result<u64> {
    let mut cursor = Cursor::new(payload, "commit record");
    let ts = cursor.u64()?;

    for _ in 0..cursor.u32()? {
        let table = cursor.u32()?;
        let id = cursor.i64()?;
        let change = match cursor.u8()? {
            INSERT | UPDATE => Change::Put(cursor.row()?),
            DELETE => Change::Delete,
            _ => return Err(cursor.corrupt("unknown change")),
        };

        apply(ts, table, id, change)?;
    }

    match cursor.is_empty() {
        true => Ok(ts),
        false => Err(cursor.corrupt("trailing bytes")),
    }
}

pub(crate) fn decode_ddl(payload: &[u8]) -> Result<(u64, Ddl)> {
    let mut cursor = Cursor::new(payload, "ddl record");
    let ts = cursor.u64()?;

    let ddl = match cursor.u8()? {
        CREATE_TABLE => {
            let table = cursor.u32()?;
            let (name, schema) = Schema::decode(cursor.blob()?).map_err(|error| {
                EngineError::Corrupt(format!("schema of table {table}: {error}"))
            })?;
            Ddl::CreateTable { table, name, schema }
        },
        DROP_TABLE => Ddl::DropTable { table: cursor.u32()? },
        CREATE_INDEX => Ddl::CreateIndex {
            table: cursor.u32()?,
            column: cursor.u16()? as usize,
            unique: cursor.u8()? != 0,
            name: cursor.string()?,
        },
        _ => return Err(cursor.corrupt("unknown ddl")),
    };

    Ok((ts, ddl))
}
