//! This is where the code actually starts running.
//! The [Database] structure owns everything and delegate it to the other modules.

#![allow(unused)]

mod metadata;
mod schema;

use metadata::SequenceMetadata;
pub(crate) use metadata::{IndexMetadata, Relation, TableMetadata};
pub(crate) use schema::has_btree_key;
pub use schema::{umbra_schema, Schema};

use crate::core::date::DateParseError;
use crate::core::storage::btree::{BTree, Cursor, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::{reassemble_content, Pager};
use crate::core::storage::tuple;
use crate::core::uuid::UuidError;
use crate::os::{self, FileSystemBlockSize, Open};
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::{Parser, ParserError};
use crate::sql::query;
use crate::sql::statement::{Column, Constraint, Create, Statement, Type, Value};
use crate::vm::expression::{TypeError, VmError};
use crate::vm::planner::{Execute, Planner, Tuple};
use crate::{index, vm};
use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::ffi::OsString;
use std::fmt::Display;
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::AtomicU64;

#[derive(Debug)]
pub struct Database<File> {
    pub(crate) pager: Rc<RefCell<Pager<File>>>,
    pub(crate) context: Context,
    pub(crate) work_dir: PathBuf,
    transaction_state: TransactionState,
}

#[derive(Debug)]
pub(crate) struct Context {
    tables: HashMap<String, TableMetadata>,
    max_size: Option<usize>,
}

#[derive(Debug)]
struct PreparedStatement<'db, File: FileOperations> {
    db: &'db mut Database<File>,
    exec: Option<Exec<File>>,
    autocommit: bool,
}

#[derive(Debug, PartialEq)]
pub struct QuerySet {
    pub tuples: Vec<Vec<Value>>,
    pub schema: Schema,
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
pub enum DatabaseError {
    Parser(ParserError),
    Sql(SqlError),
    Io(std::io::Error),
    /// Something went wrong with the underlying storage (db or journal file).
    Corrupted(String),
    Other(String),
    NoMemory,
}

#[derive(Debug, PartialEq)]
pub enum SqlError {
    /// Database table isn't found or somewhat corrupted.
    InvalidTable(String),
    /// Column isn't found or not usable in the given context.
    InvalidColumn(String),
    /// Column does not appear in `group by` or isn't used in aggregation.
    InvalidGroupBy(String),
    /// Duplicated UNIQUE or PRIMARY KEY col.
    DuplicatedKey(Value),
    /// [Analyzer error](AnalyzerError).
    Analyzer(AnalyzerError),
    /// Invalid function arguments. Expected x but found y.
    InvalidFuncArgs(usize, usize),
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

#[macro_export]
macro_rules! temporal {
    ($time_str:expr) => {
        $time_str.try_into().map(Value::Temporal)
    };
}

impl Database<File> {
    pub fn init(path: impl AsRef<Path>) -> Result<Self, DatabaseError> {
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
    pub fn exec(&mut self, input: &str) -> Result<QuerySet, DatabaseError> {
        let (schema, mut prepared) = self.prepare(input)?;
        let mut query_set = QuerySet::new(schema, vec![]);
        let mut total_size = 0;

        while let Some(tuple) = prepared.try_next()? {
            total_size += tuple::size_of(&tuple, &query_set.schema);

            if total_size > 1 << 30 {
                self.rollback()?;
                return Err(DatabaseError::NoMemory);
            }

            query_set.tuples.push(tuple)
        }

        Ok(query_set)
    }

    pub(crate) fn index_metadata(&mut self, index: &str) -> Result<IndexMetadata, DatabaseError> {
        let query = self.exec(&format!(
            "SELECT table_name FROM {DB_METADATA} WHERE name = '{index}' AND type = 'index';"
        ))?;

        if query.is_empty() {
            return Err(SqlError::Other(format!("Index '{index}' does not exists")).into());
        }

        let table_name = query
            .get(0, "table_name")
            .map(|value| match value {
                Value::String(name) => name,
                _ => unreachable!(),
            })
            .unwrap();

        let metadata = self.metadata(table_name)?;

        // FIXME: make this more efficient
        Ok(metadata
            .indexes
            .iter()
            .find(|idx| idx.name.eq(index))
            .unwrap()
            .clone())
    }

    fn sequence_metadata(&mut self, sequence: &str) -> Result<SequenceMetadata, DatabaseError> {
        todo!()
    }

    fn prepare(
        &mut self,
        sql: &str,
    ) -> Result<(Schema, PreparedStatement<'_, File>), DatabaseError> {
        let statement = crate::sql::pipeline(sql, self)?;
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
                let planner = query::planner::generate_plan(statement, self)?;
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

    pub(crate) fn rollback(&mut self) -> Result<usize, DatabaseError> {
        self.transaction_state = TransactionState::Terminated;
        self.pager.borrow_mut().rollback()
    }

    fn load_metadata(&mut self, table: &str) -> Result<TableMetadata, DatabaseError> {
        if table == DB_METADATA {
            let mut schema = umbra_schema();
            schema.prepend_id();

            let row_id = self.load_next_row_id(0)?;
            return Ok(TableMetadata {
                root: 0,
                name: String::from(table),
                row_id,
                indexes: vec![],
                serials: HashMap::new(),
                schema,
            });
        }

        let mut metadata = TableMetadata {
            root: 1,
            row_id: 1,
            name: String::from(table),
            schema: Schema::empty(),
            indexes: Vec::new(),
            serials: HashMap::new(),
        };

        let mut serials_to_load = Vec::new();
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
                    Statement::Create(Create::Sequence {
                        name,
                        table,
                        r#type,
                    }) => {
                        let root = *root as PageNumber;
                        metadata.serials.insert(
                            name.clone(),
                            SequenceMetadata {
                                root,
                                name: name.clone(),
                                value: AtomicU64::new(0),
                                data_type: r#type.clone(),
                            },
                        );

                        serials_to_load.push((name, table, r#type));
                    }
                    _ => return Err(corrupted_err()),
                },
                _ => return Err(corrupted_err()),
            }
        }

        if !found_table_def {
            return Err(DatabaseError::Sql(SqlError::InvalidTable(table.into())));
        }

        if metadata.schema.columns[0].name.eq(&ROW_COL_ID) {
            metadata.row_id = self.load_next_row_id(metadata.root)?;
        }

        for (name, table, data_type) in serials_to_load {
            let column = name
                .strip_prefix(&format!("{}_", table))
                .expect("Sequence name doesn't start with table name")
                .strip_suffix("_seq")
                .expect("Sequence name doesn't end with '_seq'");

            let col_idx = metadata
                .schema
                .index_of(column)
                .expect("No column found with this name");

            let mut pager = self.pager.borrow_mut();
            let mut btree = BTree::new(
                &mut pager,
                metadata.root,
                FixedSizeCmp::try_from(&data_type).unwrap_or(FixedSizeCmp::new::<RowId>()),
            );

            let curr_val = match btree.max()? {
                Some(max_row) => {
                    let row = tuple::deserialize(max_row.as_ref(), &metadata.schema);
                    match &row[col_idx] {
                        Value::Number(n) => *n as u64,
                        _ => 0,
                    }
                }
                None => 0,
            };

            if let Some(seq) = metadata.serials.get_mut(&name) {
                seq.value = AtomicU64::new(curr_val);
            }
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

    pub(crate) fn active_transaction(&self) -> bool {
        matches!(
            self.transaction_state,
            TransactionState::Active | TransactionState::Aborted
        )
    }

    fn aborted_transaction(&self) -> bool {
        self.transaction_state.eq(&TransactionState::Aborted)
    }
}

unsafe impl Send for Database<File> {}

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

    pub fn invalidate(&mut self, table: &str) {
        self.tables.remove(table);
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
                        serials: HashMap::new(),
                    };
                    root += 1;

                    columns.iter().for_each(|col| {
                        col.constraints.iter().for_each(|constraint| {
                            let index = match constraint {
                                Constraint::Unique => index!(unique on name (col.name)),
                                Constraint::PrimaryKey => index!(primary on (name)),
                                _ => unreachable!("This ain't a index")
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
                    unreachable!()
                };
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
            Exec::Plan(planner) => match planner.try_next() {
                Ok(tuple) => tuple,
                Err(e) => {
                    self.exec.take();
                    self.abort_transaction()?;
                    return Err(e);
                }
            },
            Exec::Explain(lines) => {
                let lines = lines.pop_front().map(|line| vec![Value::String(line)]);

                if lines.is_none() {
                    self.exec.take();
                }

                lines
            }
        };

        if tuple.is_none() || self.exec.is_none() {
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
    pub fn new(schema: Schema, tuples: Vec<Vec<Value>>) -> Self {
        Self { schema, tuples }
    }

    pub(crate) fn empty() -> Self {
        Self {
            schema: Schema::empty(),
            tuples: Vec::new(),
        }
    }

    fn get(&self, row: usize, column: &str) -> Option<&Value> {
        self.tuples.get(row)?.get(self.schema.index_of(column)?)
    }

    pub fn is_empty(&self) -> bool {
        self.tuples.iter().all(|tuple| tuple.is_empty())
    }
}
impl<'c, Col: IntoIterator<Item = &'c Column>> From<Col> for Schema {
    fn from(columns: Col) -> Self {
        Self::new(Vec::from_iter(columns.into_iter().cloned()))
    }
}

impl Display for DatabaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(err) => write!(f, "{err}"),
            Self::Parser(err) => write!(f, "{err}"),
            Self::Sql(err) => write!(f, "{err}"),
            Self::Corrupted(message) => f.write_str(message),
            Self::Other(message) => f.write_str(message),
            Self::NoMemory => f.write_str("Out of memory"),
        }
    }
}

impl Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidTable(table) => write!(f, "Invalid table '{table}'"),
            Self::InvalidColumn(column) => write!(f, "Invalid column '{column}'"),
            Self::InvalidGroupBy(column) => write!(f, "Column '{column}' must appear in GROUP BY clause or be used in an aggregate function"),
            Self::DuplicatedKey(key) => write!(f, "Duplicated key {key}"),
            Self::Analyzer(err) => write!(f, "{err}"),
            Self::Vm(err) => write!(f, "{err}"),
            Self::Type(err) => write!(f, "{err}"),
            Self::InvalidFuncArgs(expected, got) => write!(
                f,
                "Invalid number of arguments. Expected {expected}, got {got}"
            ),
            Self::Other(other) => f.write_str(other),
        }
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

impl From<DateParseError> for DatabaseError {
    fn from(value: DateParseError) -> Self {
        value.into()
    }
}

impl From<UuidError> for SqlError {
    fn from(value: UuidError) -> Self {
        TypeError::UuidError(value).into()
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

impl Database<File> {
    pub fn assert_index_contains(
        &mut self,
        index: &str,
        expected: &[Vec<Value>],
    ) -> Result<(), DatabaseError> {
        let index = self.index_metadata(index)?;

        let mut pager = self.pager.borrow_mut();
        let mut cursor = Cursor::new(index.root, 0);

        let mut entries = Vec::new();
        while let Some((page, slot_id)) = cursor.try_next(&mut pager)? {
            let entry = reassemble_content(&mut pager, page, slot_id)?;
            entries.push(tuple::deserialize(entry.as_ref(), &index.schema));
        }

        assert_eq!(entries, expected);

        Ok(())
    }
}

impl PartialEq for DatabaseError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Io(a), Self::Io(b)) => a.kind().eq(&b.kind()),
            (Self::Parser(a), Self::Parser(b)) => a.eq(b),
            (Self::Sql(a), Self::Sql(b)) => a.eq(b),
            _ => false,
        }
    }
}
