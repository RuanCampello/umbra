use std::{
    collections::{HashMap, VecDeque},
    io::{Read, Seek, Write},
    ops::Bound,
    rc::Rc,
};

use crate::{
    core::storage::{btree::Cursor, pagination::io::FileOperations},
    db::{Ctx, Database, DatabaseError},
    sql::statement::{Expression, Value},
    vm::planner::{PlanExecutor, Planner, SeqScan},
};

type IndexBounds<'value> = (Bound<&'value Value>, Bound<&'value Value>);

pub(in crate::sql::query) fn generate_seq_plan<File: PlanExecutor>(
    table: &str,
    mut filter: Option<Expression>,
    db: &mut Database<File>,
) -> Result<Planner<File>, DatabaseError> {
    //   let source = match
    todo!()
}

fn generate_optimised_seq_plan<File: PlanExecutor>(
    table: &str,
    db: &mut Database<File>,
    filter: &mut Option<Expression>,
) -> Result<Option<Planner<File>>, DatabaseError> {
    let Some(expr) = filter else {
        return Ok(None);
    };
    let table = db.metadata(table)?.clone();

    let paths: HashMap<&str, VecDeque<IndexBounds>> = HashMap::new();

    todo!()
}
