#![allow(unused_imports)]

mod metadata;
mod schema;

pub(crate) use metadata::{IndexMetadata, Relation, TableMetadata};
pub(crate) use schema::Schema;

use crate::core::date::DateParseError;
use crate::core::storage::btree::{BTree, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::Pager;
use crate::core::storage::tuple;
use crate::db::schema::umbra_schema;
use crate::os::{self, FileSystemBlockSize, Open};
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::{Parser, ParserError};
use crate::sql::query;
use crate::sql::statement::{Column, Constraint, Create, Statement, Type, Value};
use crate::vm;
use crate::vm::expression::{TypeError, VmError};
use crate::vm::planner::{Execute, Planner, Tuple};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

#[derive(Debug)]
pub(crate) struct Database<File> {
    pub(crate) pager: Rc<RefCell<Pager<File>>>,
    context: Context,
    pub(crate) work_dir: PathBuf,
    transaction_state: TransactionState,
}

#[derive(Debug)]
pub(crate) struct Context {
    tables: HashMap<String, TableMetadata>,
    max_size: Option<usize>,
}

struct PreparedStatement<'db, File: FileOperations> {
    db: &'db mut Database<File>,
    exec: Option<Exec<File>>,
    autocommit: bool,
}

pub(crate) struct QuerySet {
    tuples: Vec<Vec<Value>>,
    schema: Schema,
}

#[derive(Debug, PartialEq)]
enum TransactionState {
    Active,
    Aborted,
    Terminated,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Exec<File: FileOperations> {
    Statement(Statement),
    Plan(Planner<File>),
    Explain(VecDeque<String>),
}

#[derive(Debug)]
pub(crate) enum DatabaseError {
    Parser(ParserError),
    Sql(SqlError),
    Io(std::io::Error),
    /// Something went wrong with the underlying storage (db or journal file).
    Corrupted(String),
    Other(String),
    NoMemory,
}

#[derive(Debug, PartialEq)]
pub(crate) enum SqlError {
    /// Database table isn't found or somewhat corrupted.
    InvalidTable(String),
    /// Column isn't found or not usable in the given context.
    InvalidColumn(String),
    /// Duplicated UNIQUE or PRIMARY KEY col.
    DuplicatedKey(Value),
    /// [Analyzer error](AnalyzerError).
    Analyzer(AnalyzerError),
    Type(TypeError),
    Vm(VmError),
    Other(String),
}

pub(crate) type RowId = u64;

/// The identifier of [row id](https://www.sqlite.org/rowidtable.html) column.
pub(crate) const ROW_COL_ID: &str = "row_id";
pub(crate) const DB_METADATA: &str = "umbra_db_meta";
const DEFAULT_CACHE_SIZE: usize = 512;

pub(crate) trait Ctx {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError>;
}

impl Database<File> {
    fn init(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
        let file = os::Fs::options()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .sync_on_write(false)
            .lock(true)
            .bypass_cache(true)
            .open(&path)?;

        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::new(io::ErrorKind::Unsupported, "This is not a file").into());
        }

        let block_size = os::Fs::block_size(&path)?;
        let db_path = path.as_ref().canonicalize()?;
        let work_dir = db_path.parent().unwrap().to_path_buf();

        let mut ext = db_path
            .extension()
            .unwrap_or(&OsString::new())
            .to_os_string();
        ext.push(".journal");

        let journal_path = db_path.with_extension(ext);

        let mut pager = Pager::<File>::new(file)
            .block_size(block_size)
            .journal_path_file(journal_path);
        pager.init()?;

        Ok(Database::new(Rc::new(RefCell::new(pager)), work_dir))
    }
}

impl<File: Seek + Read + Write + FileOperations> Database<File> {
    pub(crate) fn exec(&mut self, input: &str) -> Result<QuerySet, DatabaseError> {
        println!("executing database...");
        let (schema, mut prepared) = self.prepare(input)?;
        let mut query_set = QuerySet::new(schema, vec![]);
        let mut total_size = 0;

        while let Some(tuple) = prepared.try_next()? {
            println!("tuple in exec {tuple:#?}");
            total_size += tuple::size_of(&tuple, &query_set.schema);

            if total_size > 1 << 30 {
                self.rollback()?;
                return Err(DatabaseError::NoMemory);
            }

            query_set.tuples.push(tuple)
        }

        Ok(query_set)
    }

    fn prepare(
        &mut self,
        sql: &str,
    ) -> Result<(Schema, PreparedStatement<'_, File>), DatabaseError> {
        let statement = crate::sql::pipeline(sql, self)?;
        println!("statement after pipeline {statement:#?}");
        // CHECKOUT
        let mut schema = Schema::empty();

        let exec = match statement {
            Statement::Create(_)
            | Statement::Drop(_)
            | Statement::StartTransaction
            | Statement::Commit
            | Statement::Rollback => Exec::Statement(statement),

            Statement::Explain(inner) => match &*inner {
                Statement::Select { .. }
                | Statement::Insert { .. }
                | Statement::Update { .. }
                | Statement::Delete { .. } => {
                    schema = Schema::new(vec![Column::new("Query Plan", Type::Varchar(255))]);
                    let planner = query::planner::generate_plan(*inner, self)?;
                    Exec::Explain(format!("{planner}").lines().map(String::from).collect())
                }

                _ => {
                    return Err(DatabaseError::Other(
                        String::from("EXPLAIN is meant to work only with SELECT, INSERT, UPDATE and DELETE statements"
                        )));
                }
            },
            _ => {
                println!("statement in exec {statement:#?}");
                let planner = query::planner::generate_plan(statement, self)?;
                println!("planner in exec {planner}");
                if let Some(planner_schema) = planner.schema() {
                    schema = planner_schema;
                }

                Exec::Plan(planner)
            }
        };

        let prepared_statement = PreparedStatement {
            db: self,
            autocommit: false,
            exec: Some(exec),
        };

        Ok((schema, prepared_statement))
    }
    fn commit(&mut self) -> io::Result<()> {
        self.transaction_state = TransactionState::Terminated;
        self.pager.borrow_mut().commit()
    }

    fn rollback(&mut self) -> Result<usize, DatabaseError> {
        todo!()
    }

    fn load_metadata(&mut self, table: &str) -> Result<TableMetadata, DatabaseError> {
        if table == DB_METADATA {
            let mut schema = umbra_schema();
            println!("schema {schema:#?}");
            schema.prepend_id();

            println!("schema prepared {schema:#?}");
            let row_id = self.load_next_row_id(0)?;
            println!("next_row_id = {row_id:#?}");
            return Ok(TableMetadata {
                root: 0,
                name: String::from(table),
                row_id,
                indexes: vec![],
                schema,
            });
        }

        let mut metadata = TableMetadata {
            root: 1,
            row_id: 1,
            name: String::from(table),
            schema: Schema::empty(),
            indexes: Vec::new(),
        };

        let mut found_table_def = false;

        let (schema, mut results) = self.prepare(&format!(
            "SELECT root, sql FROM {DB_METADATA} WHERE table_name = '{table}';"
        ))?;

        let corrupted_err = || {
            DatabaseError::Corrupted(format!(
                "{DB_METADATA} table is corrupted or contains wrong data"
            ))
        };

        while let Some(tuple) = results.try_next()? {
            let Value::Number(root) = &tuple[schema.index_of("root").ok_or(corrupted_err())?]
            else {
                return Err(corrupted_err());
            };

            match &tuple[schema.index_of("sql").ok_or(corrupted_err())?] {
                Value::String(sql) => match Parser::new(sql).parse_statement()? {
                    Statement::Create(Create::Table { columns, .. }) => {
                        assert!(!found_table_def, "Multiple definitions of table {table}");

                        metadata.root = *root as PageNumber;
                        metadata.schema = Schema::new(columns);

                        if !metadata.schema.has_btree_key() {
                            metadata.schema.prepend_id();
                        }

                        found_table_def = true;
                    }
                    Statement::Create(Create::Index {
                        name,
                        column,
                        unique,
                        ..
                    }) => {
                        let col_idx =
                            metadata
                                .schema
                                .index_of(&column)
                                .ok_or(SqlError::Other(format!(
                                    "Couldn't find index column {column} in table {table}"
                                )))?;

                        let idx_col = metadata.schema.columns[col_idx].clone();

                        metadata.indexes.push(IndexMetadata {
                            root: *root as PageNumber,
                            name,
                            column: idx_col.clone(),
                            schema: Schema::new(vec![idx_col, metadata.schema.columns[0].clone()]),
                            unique,
                        });
                    }
                    _ => return Err(corrupted_err()),
                },
                _ => return Err(corrupted_err()),
            }
        }

        if !found_table_def {
            println!("Table {table} not found in {DB_METADATA} table");
            return Err(DatabaseError::Sql(SqlError::InvalidColumn(table.into())));
        }

        if metadata.schema.columns[0].name.eq(&ROW_COL_ID) {
            metadata.row_id = self.load_next_row_id(metadata.root)?;
        }

        Ok(metadata)
    }

    fn load_next_row_id(&mut self, root: PageNumber) -> Result<RowId, DatabaseError> {
        let mut pager = self.pager.borrow_mut();
        let mut btree = BTree::new(&mut pager, root, FixedSizeCmp::new::<RowId>());

        let row_id = match btree.max()? {
            Some(max) => tuple::deserialize_row_id(max.as_ref()) + 1,
            None => 1,
        };

        Ok(row_id)
    }
}

impl<File: Seek + Read + Write + FileOperations> Ctx for Database<File> {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
        if !self.context.contains(table) {
            println!("database context {:#?}", self.context);
            let metadata = self.load_metadata(table)?;
            self.context.insert(metadata);
        }

        self.context.metadata(table)
    }
}

impl<File> Database<File> {
    pub(crate) fn new(pager: Rc<RefCell<Pager<File>>>, work_dir: PathBuf) -> Self {
        Self {
            pager,
            work_dir,
            context: Context::with_size(DEFAULT_CACHE_SIZE),
            transaction_state: TransactionState::Terminated,
        }
    }

    fn start_transaction(&mut self) {
        self.transaction_state = TransactionState::Active
    }

    fn active_transaction(&self) -> bool {
        matches!(
            self.transaction_state,
            TransactionState::Active | TransactionState::Aborted
        )
    }

    fn aborted_transaction(&self) -> bool {
        self.transaction_state.eq(&TransactionState::Aborted)
    }
}

impl Context {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            max_size: None,
        }
    }

    pub fn with_size(size: usize) -> Self {
        Self {
            tables: HashMap::with_capacity(size),
            max_size: Some(size),
        }
    }

    pub fn insert(&mut self, metadata: TableMetadata) {
        if self.max_size.is_some_and(|size| self.tables.len() >= size) {
            let evict = self.tables.keys().next().unwrap().clone();
            self.tables.remove(&evict);
        }

        self.tables.insert(metadata.name.to_string(), metadata);
    }

    fn contains(&self, table: &str) -> bool {
        self.tables.contains_key(table)
    }
}

/// Test-only implementation: clones everything for simplicity
#[cfg(test)]
impl TryFrom<&[&str]> for Context {
    type Error = DatabaseError;

    fn try_from(statements: &[&str]) -> Result<Self, Self::Error> {
        let mut context = Self::new();
        let mut root = 1;

        for sql in statements {
            let statement = Parser::new(sql).parse_statement()?;
            match statement {
                Statement::Create(Create::Table { name, columns }) => {
                    let mut schema = Schema::from(&columns);
                    schema.prepend_id();

                    let mut metadata = TableMetadata {
                        root,
                        name: name.clone(),
                        row_id: 1,
                        schema,
                        indexes: vec![],
                    };
                    root += 1;

                    columns.iter().for_each(|col| {
                        col.constraints.iter().for_each(|constraint| {
                            let index = match constraint {
                                Constraint::Unique => format!("{}_uq_index", col.name),
                                Constraint::PrimaryKey => format!("{}_pk_index", col.name),
                            };

                            metadata.indexes.push(IndexMetadata {
                                column: col.clone(),
                                schema: Schema::new(vec![col.clone(), columns[0].clone()]),
                                name: index,
                                root,
                                unique: true,
                            });

                            root += 1;
                        })
                    });

                    context.insert(metadata);
                }
                Statement::Create(Create::Index { name, column, unique, .. }) if unique => {
                    let table = context.metadata(&name)?;
                    let index_col = &table.schema.columns[table.schema.index_of(&column).unwrap()];

                    table.indexes.push(IndexMetadata {
                        column: index_col.clone(),
                        schema: Schema::new(vec![index_col.clone(), table.schema.columns[0].clone()]),
                        name,
                        root,
                        unique,
                    });

                    root += 1;
                }

                statement => {
                    return Err(SqlError::Other(format!("Only create unique index and create table should be called by test context, but found {statement:#?}")).into())
                }
            }
        }

        Ok(context)
    }
}

impl Ctx for Context {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
        println!("metadata for table {table} in context {self:#?}");
        self.tables
            .get_mut(table)
            .ok_or_else(|| SqlError::InvalidTable(table.to_string()).into())
    }
}

impl<'db, File: Seek + Write + Read + FileOperations> PreparedStatement<'db, File> {
    fn try_next(&mut self) -> Result<Option<Tuple>, DatabaseError> {
        let Some(exec) = self.exec.as_mut() else {
            return Ok(None);
        };

        if self.db.aborted_transaction()
            && !matches!(
                exec,
                Exec::Statement(Statement::Commit) | Exec::Statement(Statement::Rollback)
            )
        {
            return Err(DatabaseError::Other(
                "Current transaction is aborted due to previous errors".into(),
            ));
        }

        if let Exec::Statement(Statement::StartTransaction) = exec {
            if self.db.active_transaction() {
                return Err(DatabaseError::Other(
                    "Cannot start a transaction while there's one in progress".into(),
                ));
            }

            self.db.start_transaction();
            return Ok(None);
        }

        if !self.db.active_transaction() {
            self.db.start_transaction();
            self.autocommit = true;
        }

        let tuple = match exec {
            Exec::Statement(_) => {
                let Some(Exec::Statement(statement)) = self.exec.take() else {
                    println!("trying to take statement from None");
                    unreachable!()
                };

                println!("passed unreachable");

                let mut affected_rows = 0;
                match statement {
                    Statement::Commit => {
                        if self.db.transaction_state.eq(&TransactionState::Aborted) {
                            self.db.rollback()?;
                        } else {
                            self.db.commit()?;
                        }
                    }
                    Statement::Rollback => {
                        self.db.rollback()?;
                    }
                    Statement::Drop(_) | Statement::Create(_) => {
                        println!("trying create statement {statement:#?}");
                        match vm::statement::exec(statement, self.db) {
                            Ok(rows) => affected_rows = rows,
                            Err(e) => {
                                self.abort_transaction()?;
                                return Err(e);
                            }
                        }
                    }
                    _ => unreachable!(),
                };
                Some(vec![Value::Number(affected_rows as i128)])
            }
            Exec::Plan(planner) => {
                println!("planner {planner}");
                match planner.try_next() {
                    Ok(tuple) => {
                        println!("on tuple {tuple:#?}");
                        tuple
                    }
                    Err(e) => {
                        self.exec.take();
                        self.abort_transaction()?;
                        return Err(e);
                    }
                }
            }
            Exec::Explain(lines) => {
                let lines = lines.pop_front().map(|line| vec![Value::String(line)]);

                if lines.is_none() {
                    self.exec.take();
                }

                lines
            }
        };

        if tuple.is_none() || self.exec.is_none() {
            println!(
                "tuple is none or exec is none {} {:#?}",
                self.autocommit, self.db.context
            );
            self.exec.take();

            if self.autocommit {
                self.db.commit()?;
            }
        }

        Ok(tuple)
    }

    fn abort_transaction(&mut self) -> Result<(), DatabaseError> {
        match self.autocommit {
            true => {
                self.db.rollback()?;
            }
            false => self.db.transaction_state = TransactionState::Aborted,
        };

        Ok(())
    }
}

impl QuerySet {
    fn new(schema: Schema, tuples: Vec<Vec<Value>>) -> Self {
        Self { schema, tuples }
    }
}
impl<'c, Col: IntoIterator<Item = &'c Column>> From<Col> for Schema {
    fn from(columns: Col) -> Self {
        Self::new(Vec::from_iter(columns.into_iter().cloned()))
    }
}

impl From<AnalyzerError> for SqlError {
    fn from(value: AnalyzerError) -> Self {
        SqlError::Analyzer(value)
    }
}

impl From<VmError> for SqlError {
    fn from(value: VmError) -> Self {
        SqlError::Vm(value)
    }
}

impl From<TypeError> for SqlError {
    fn from(value: TypeError) -> Self {
        SqlError::Type(value)
    }
}

impl From<DateParseError> for SqlError {
    fn from(value: DateParseError) -> Self {
        TypeError::InvalidDate(value).into()
    }
}

impl From<SqlError> for DatabaseError {
    fn from(value: SqlError) -> Self {
        DatabaseError::Sql(value)
    }
}

impl From<TypeError> for DatabaseError {
    fn from(value: TypeError) -> Self {
        SqlError::from(value).into()
    }
}

impl From<AnalyzerError> for DatabaseError {
    fn from(value: AnalyzerError) -> Self {
        SqlError::from(value).into()
    }
}

impl From<std::io::Error> for DatabaseError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<ParserError> for DatabaseError {
    fn from(value: ParserError) -> Self {
        DatabaseError::Parser(value)
    }
}
