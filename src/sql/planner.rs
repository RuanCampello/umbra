//! Plan trees implementation to execute the actual queries.

use crate::core::db::TableMetadata;
use crate::core::storage::pagination::pager::Pager;
use std::cell::RefCell;
use std::io::Cursor;
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
