use std::io::{Read, Seek, Write};

use crate::{
    core::storage::pagination::io::FileOperations,
    db::{Database, DatabaseError},
    sql::statement::Statement,
};

pub(crate) fn exec<File: Seek + Read + Write + FileOperations>(
    statement: Statement,
    db: &mut Database<File>,
) -> Result<usize, DatabaseError> {
    let sql = statement.to_string();
    todo!()
}
