//! Plan trees implementation to execute the actual queries.

use crate::core::db::{DatabaseError, TableMetadata};
use crate::core::storage::btree::Cursor;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::{reassemble_content, Pager};
use crate::core::storage::tuple::deserialize;
use crate::sql::statement::Value;
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

#[derive(Debug)]
pub(crate) enum Planner<File> {
    SeqScan(SeqScan<File>),
}

#[derive(Debug)]
struct SeqScan<File> {
    pub table: TableMetadata,
    pager: Rc<RefCell<Pager<File>>>,
    cursor: Cursor,
}

type Tuple = Vec<Value>;

impl<File: Seek + Read + Write + FileOperations> SeqScan<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let mut pager = self.pager.borrow_mut();

        let Some((page, slot)) = self.cursor.try_next(&mut pager)? else {
            return Ok(None);
        };

        Ok(Some(deserialize(
            reassemble_content(&mut pager, page, slot)?.as_ref(),
            &self.table.schema,
        )))
    }
}
