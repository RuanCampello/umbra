//! Payloads of log frames
//!
//! ```text
//! Commit = ts u64 | ddl count u16 | ddl* | count u32 | (table u32 | row id i64 | op u8 | row if op ≠ delete)*
//! ddl    = create table: 1 u8 | table u32 | schema blob
//!          drop table:   2 u8 | table u32
//!          create index: 3 u8 | table u32 | column u16 | unique u8 | name
//!          create index over several columns: 4 u8 | table u32 | count u8 | column u16* | unique u8 | name
//! ```
//!
//! A transaction's catalogue changes precede its row writes, in the order they ran

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
    CreateIndex { table: u32, columns: Box<[usize]>, unique: bool, name: String },
}

/// A decoded commit whose row writes are still to be read, once its DDL is applied
pub(crate) struct Commit<'a> {
    pub ts: u64,
    pub ddl: Vec<Ddl>,
    writes: Cursor<'a>,
}

const INSERT: u8 = 1;
const UPDATE: u8 = 2;
const DELETE: u8 = 3;

const CREATE_TABLE: u8 = 1;
const DROP_TABLE: u8 = 2;
const CREATE_INDEX: u8 = 3;
const CREATE_COMPOSITE_INDEX: u8 = 4;

/// encodes a transaction's catalogue changes and write set with a zero timestamp, set later by
/// [stamp_timestamp]
pub(crate) fn encode_commit<'a>(
    ddl: impl ExactSizeIterator<Item = &'a Ddl>,
    writes: &[Write],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(14 + writes.len() * 32);
    buf.extend_from_slice(&0u64.to_le_bytes());

    buf.extend_from_slice(&(ddl.len() as u16).to_le_bytes());
    ddl.for_each(|ddl| encode_ddl(&mut buf, ddl));

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
            (_, Some(row)) => {
                buf.push(UPDATE);
                put_row(&mut buf, row);
            },
        }
    }

    buf
}

/// writes the commit timestamp into an encoded payload
#[inline]
pub(crate) fn stamp_timestamp(payload: &mut [u8], ts: u64) {
    payload[..8].copy_from_slice(&ts.to_le_bytes());
}

/// decodes a commit's timestamp and catalogue changes
pub(crate) fn decode_commit(payload: &[u8]) -> Result<Commit<'_>> {
    let mut cursor = Cursor::new(payload, "commit record");
    let ts = cursor.u64()?;
    let ddl = (0..cursor.u16()?).map(|_| decode_ddl(&mut cursor)).collect::<Result<_>>()?;

    Ok(Commit { ts, ddl, writes: cursor })
}

impl Commit<'_> {
    /// hands every row write to `apply`, in record order
    pub fn writes(mut self, mut apply: impl FnMut(u32, RowId, Change) -> Result<()>) -> Result<()> {
        let cursor = &mut self.writes;
        for _ in 0..cursor.u32()? {
            let table = cursor.u32()?;
            let id = cursor.i64()?;
            let change = match cursor.u8()? {
                INSERT | UPDATE => Change::Put(cursor.row()?),
                DELETE => Change::Delete,
                _ => return Err(cursor.corrupt("unknown change")),
            };

            apply(table, id, change)?;
        }

        match cursor.is_empty() {
            true => Ok(()),
            _ => Err(cursor.corrupt("trailing bytes")),
        }
    }
}

fn encode_ddl(buf: &mut Vec<u8>, ddl: &Ddl) {
    match ddl {
        Ddl::CreateTable { table, name, schema } => {
            buf.push(CREATE_TABLE);
            buf.extend_from_slice(&table.to_le_bytes());
            put_blob(buf, &schema.encode(name));
        },
        Ddl::DropTable { table } => {
            buf.push(DROP_TABLE);
            buf.extend_from_slice(&table.to_le_bytes());
        },
        Ddl::CreateIndex { table, columns, unique, name } => {
            match &**columns {
                [column] => {
                    buf.push(CREATE_INDEX);
                    buf.extend_from_slice(&table.to_le_bytes());
                    buf.extend_from_slice(&(*column as u16).to_le_bytes());
                },
                columns => {
                    buf.push(CREATE_COMPOSITE_INDEX);
                    buf.extend_from_slice(&table.to_le_bytes());
                    buf.push(columns.len() as u8);
                    for &column in columns {
                        buf.extend_from_slice(&(column as u16).to_le_bytes());
                    }
                },
            }
            buf.push(*unique as u8);
            put_string(buf, name);
        },
    }
}

fn decode_ddl(cursor: &mut Cursor) -> Result<Ddl> {
    Ok(match cursor.u8()? {
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
            columns: [cursor.u16()? as usize].into(),
            unique: cursor.u8()? != 0,
            name: cursor.string()?,
        },
        CREATE_COMPOSITE_INDEX => {
            let table = cursor.u32()?;
            let columns = (0..cursor.u8()?)
                .map(|_| Ok(cursor.u16()? as usize))
                .collect::<Result<Box<[_]>>>()?;
            Ddl::CreateIndex {
                table,
                columns,
                unique: cursor.u8()? != 0,
                name: cursor.string()?,
            }
        },
        _ => return Err(cursor.corrupt("unknown ddl")),
    })
}
