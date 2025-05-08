//! Plan trees implementation to execute the actual queries.

use crate::core::db::{DatabaseError, IndexMetadata, Relation, Schema, TableMetadata};
use crate::core::storage::btree::{BTree, BTreeKeyCmp, Cursor};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::{reassemble_content, Pager};
use crate::core::storage::tuple::{self, deserialize};
use crate::sql::statement::{Expression, Value};
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::rc::Rc;

#[derive(Debug)]
pub(crate) enum Planner<File> {
    SeqScan(SeqScan<File>),
    ExactMatch(ExactMatch<File>),
}

#[derive(Debug)]
struct SeqScan<File> {
    pub table: TableMetadata,
    pager: Rc<RefCell<Pager<File>>>,
    cursor: Cursor,
}

#[derive(Debug, PartialEq)]
struct ExactMatch<File> {
    relation: Relation,
    key: Vec<u8>,
    expr: Expression,
    pager: Rc<RefCell<Pager<File>>>,
    done: bool,
    emit_only_key: bool,
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

impl<File: Seek + Read + Write + FileOperations> ExactMatch<File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        if self.done {
            return Ok(None);
        }

        self.done = true;

        let mut pager = self.pager.borrow_mut();
        let mut btree = BTree::new(&mut pager, self.relation.root(), self.relation.comp());

        let Some(entry) = btree.get(&self.key)? else {
            return Ok(None);
        };

        let mut tuple = tuple::deserialize(entry.as_ref(), self.relation.schema());
        if self.emit_only_key {
            let table_idx = self.relation.index();
            tuple.drain(table_idx + 1..);
            tuple.drain(..table_idx);
        }

        Ok(Some(tuple))
    }
}
