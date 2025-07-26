//! This is where the code actually starts running.
//! The [Database] structure owns everything and delegate it to the other modules.

#![allow(unused)]

mod metadata;
mod schema;

use metadata::SequenceMetadata;
pub(crate) use metadata::{IndexMetadata, Relation, TableMetadata};
pub(crate) use schema::{has_btree_key, umbra_schema, Schema};

use crate::core::date::DateParseError;
use crate::core::storage::btree::{BTree, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::Pager;
use crate::core::storage::tuple;
use crate::core::uuid::UuidError;
use crate::os::{self, FileSystemBlockSize, Open};
use crate::sql::analyzer::AnalyzerError;
use crate::sql::parser::{Parser, ParserError};
use crate::sql::query;
use crate::sql::statement::{Column, Constraint, Create, Statement, Type, Value};
use crate::vm::expression::{TypeError, VmError, DEFAULT_NUM_EXPECTED_TYPES};
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

    fn index_metadata(&mut self, index: &str) -> Result<IndexMetadata, DatabaseError> {
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
    pub(crate) fn new(schema: Schema, tuples: Vec<Vec<Value>>) -> Self {
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

    fn is_empty(&self) -> bool {
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::core::{
        date::{NaiveDate, Parse},
        storage::{
            btree::Cursor,
            pagination::{
                pager::{reassemble_content, DEFAULT_PAGE_SIZE},
                Cache,
            },
            MemoryBuffer,
        },
        uuid::Uuid,
    };

    use super::*;

    struct Configuration {
        page_size: usize,
        cache_size: usize,
    }

    type DatabaseResult = Result<(), DatabaseError>;
    impl Default for Database<MemoryBuffer> {
        fn default() -> Self {
            new_db(Configuration {
                cache_size: DEFAULT_CACHE_SIZE,
                page_size: DEFAULT_PAGE_SIZE,
            })
            .expect("Couldn't create table with default configuration")
        }
    }

    impl Database<MemoryBuffer> {
        fn assert_index_contains(
            &mut self,
            index: &str,
            expected: &[Vec<Value>],
        ) -> DatabaseResult {
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

    fn new_db(config: Configuration) -> io::Result<Database<MemoryBuffer>> {
        let Configuration {
            page_size,
            cache_size,
        } = config;
        let mut pager = Pager::<MemoryBuffer>::default()
            .page_size(page_size)
            .block_size(page_size)
            .cache(Cache::with_max_size(cache_size))
            .file(io::Cursor::new(Vec::<u8>::new()));

        pager.init()?;

        Ok(Database::new(Rc::new(RefCell::new(pager)), PathBuf::new()))
    }

    #[test]
    fn test_create_simple_table() -> DatabaseResult {
        let mut db = Database::default();
        let sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(255));";

        db.exec(sql)?;

        let query = db.exec("SELECT * FROM umbra_db_meta;")?;

        assert_eq!(
            query,
            QuerySet::new(
                umbra_schema(),
                vec![vec![
                    Value::String("table".into()),
                    Value::String("users".into()),
                    Value::Number(1),
                    Value::String("users".into()),
                    Value::String(Parser::new(sql).parse_statement()?.to_string())
                ]]
            )
        );
        Ok(())
    }

    #[test]
    fn test_create_table_with_forced_pk() -> DatabaseResult {
        let mut db = Database::default();
        let sql = "CREATE TABLE users (name VARCHAR(255), id INTEGER PRIMARY KEY);";

        db.exec(sql)?;
        let query = db.exec("SELECT * FROM umbra_db_meta;")?;

        assert_eq!(
            query,
            QuerySet::new(
                umbra_schema(),
                vec![
                    vec![
                        Value::String("table".into()),
                        Value::String("users".into()),
                        Value::Number(1),
                        Value::String("users".into()),
                        Value::String(Parser::new(sql).parse_statement()?.to_string()),
                    ],
                    vec![
                        Value::String("index".into()),
                        Value::String(index!(primary on users)),
                        Value::Number(2),
                        Value::String("users".into()),
                        Value::String(
                            Parser::new("CREATE UNIQUE INDEX users_pk_index ON users(id);")
                                .parse_statement()?
                                .to_string()
                        )
                    ],
                ]
            )
        );

        Ok(())
    }

    #[test]
    fn test_insert_on_table() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255));")?;
        db.exec("INSERT INTO employees (id, name) VALUES (1, 'John Doe'), (2, 'Mary Dove');")?;

        let query = db.exec("SELECT * FROM employees;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(255))
                ]),
                tuples: vec![
                    vec![Value::Number(1), Value::String("John Doe".into())],
                    vec![Value::Number(2), Value::String("Mary Dove".into())]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_insert_with_expression() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE products (id INT PRIMARY KEY, validity DATE, price INT);")?;
        db.exec("INSERT INTO products (id, validity, price) VALUES (1, '2030-12-24', 2*30), (2, '2029-02-13', 100 / (3+2));")?;

        let query = db.exec("SELECT * FROM products;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("validity", Type::Date),
                    Column::new("price", Type::Integer)
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        temporal!("2030-12-24")?,
                        Value::Number(60),
                    ],
                    vec![
                        Value::Number(2),
                        temporal!("2029-02-13")?,
                        Value::Number(20),
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_select_where() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE products (id INT PRIMARY KEY, validity DATE, price INT);")?;
        db.exec(
            r#"
            INSERT INTO products (id, validity, price) 
            VALUES (1, '2030-12-24', 2*30), (2, '2029-02-13', 100 / (3+2)), (3, '2028-07-02', 25);
        "#,
        )?;

        let query = db.exec("SELECT * FROM products WHERE price > 20;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("validity", Type::Date),
                    Column::new("price", Type::Integer)
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        temporal!("2030-12-24")?,
                        Value::Number(60),
                    ],
                    vec![
                        Value::Number(3),
                        temporal!("2028-07-02")?,
                        Value::Number(25),
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_select_with_different_order() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(135), age INT);")?;
        db.exec(
            r#"
            INSERT INTO users (id, name, age) VALUES (1, 'John Doe', 22), (2, 'Mary Dove', 27);
        "#,
        )?;

        let query = db.exec("SELECT age, name FROM users;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::new("age", Type::Integer),
                    Column::new("name", Type::Varchar(135)),
                ]),
                tuples: vec![
                    vec![Value::Number(22), Value::String("John Doe".into())],
                    vec![Value::Number(27), Value::String("Mary Dove".into())]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_select_many() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135));")?;

        let mut expected = Vec::new();
        for i in 1..500 {
            expected.push(vec![
                Value::Number(i),
                Value::String(format!("employee_{i}")),
            ]);
        }

        for employee in expected.iter().rev() {
            db.exec(&format!(
                "INSERT INTO employees (id, name) VALUES ({}, {});",
                employee[0], employee[1]
            ))?;
        }

        let query = db.exec("SELECT * FROM employees;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(135))
                ]),
                tuples: expected,
            }
        );

        Ok(())
    }

    #[test]
    fn test_select_with_order_by() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135), age INT);")?;
        db.exec(
            r#"
            INSERT INTO employees(id, name, age) 
            VALUES (3, 'John Doe', 27), (2, 'Mary Dove', 22), (1, 'Paul Dean', 20);
        "#,
        )?;

        let query = db.exec("SELECT * FROM employees ORDER BY age, name;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(135)),
                    Column::new("age", Type::Integer),
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        Value::String("Paul Dean".into()),
                        Value::Number(20)
                    ],
                    vec![
                        Value::Number(2),
                        Value::String("Mary Dove".into()),
                        Value::Number(22)
                    ],
                    vec![
                        Value::Number(3),
                        Value::String("John Doe".into()),
                        Value::Number(27)
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_create_unique_index_duplicated() -> DatabaseResult {
        let mut db = Database::default();
        let create_query = "CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255));";

        db.exec(create_query)?;
        db.exec(
            r#"
            INSERT INTO users (id, email) VALUES
            (1, 'johndoe@email.com'),
            (2, 'marydove@email.com'),
            (3, 'johndoe@email.com');
        "#,
        )?;

        let query = db.exec("CREATE UNIQUE INDEX email_uq ON users(email);");
        assert_eq!(
            query,
            Err(SqlError::DuplicatedKey(Value::String("johndoe@email.com".into())).into()),
        );

        let query = db.exec("SELECT * FROM umbra_db_meta;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: umbra_schema(),

                tuples: vec![vec![
                    Value::String("table".into()),
                    Value::String("users".into()),
                    Value::Number(1),
                    Value::String("users".into()),
                    Value::String(Parser::new(create_query).parse_statement()?.to_string())
                ]]
            }
        );

        Ok(())
    }

    #[test]
    fn test_insert_duplicated() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), birth_date DATE);",
        )?;
        db.exec(
            r#"
            INSERT INTO employees (id, name, birth_date) VALUES 
            (1, 'John Doe', '1995-03-01'),
            (2, 'Mary Dove', '2000-04-24'),
            (3, 'Paul Dean', '1999-01-27');
        "#,
        )?;

        let query = db.exec(
            r#"
            INSERT INTO employees (id, name, birth_date) VALUES (2, 'Philip Dahmer', '2005-12-07');
        "#,
        );
        assert_eq!(query, Err(SqlError::DuplicatedKey(Value::Number(2)).into()));

        let query = db.exec("SELECT * FROM employees;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(255)),
                    Column::new("birth_date", Type::Date)
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        Value::String("John Doe".into()),
                        temporal!("1995-03-01")?,
                    ],
                    vec![
                        Value::Number(2),
                        Value::String("Mary Dove".into()),
                        temporal!("2000-04-24")?,
                    ],
                    vec![
                        Value::Number(3),
                        Value::String("Paul Dean".into()),
                        temporal!("1999-01-27")?,
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_insert_temporal_values() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            "CREATE TABLE logs (
            id INT PRIMARY KEY,
            log_time TIME,
            created_at TIMESTAMP
        );",
        )?;

        db.exec(
            r#"
        INSERT INTO logs (id, log_time, created_at) VALUES 
        (1, '13:45:30', '2023-12-01T13:45:30'),
        (2, '00:00:00', '2020-01-01T00:00:00'),
        (3, '23:59:59', '1999-12-31T23:59:59');
    "#,
        )?;

        let query = db.exec("SELECT * FROM logs;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("log_time", Type::Time),
                    Column::new("created_at", Type::DateTime),
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        temporal!("13:45:30")?,
                        temporal!("2023-12-01T13:45:30")?,
                    ],
                    vec![
                        Value::Number(2),
                        temporal!("00:00:00")?,
                        temporal!("2020-01-01T00:00:00")?,
                    ],
                    vec![
                        Value::Number(3),
                        temporal!("23:59:59")?,
                        temporal!("1999-12-31T23:59:59")?,
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_inserting_smallint() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            r#"
            CREATE TABLE product_inventory (
                id INT UNSIGNED PRIMARY KEY,
                name VARCHAR(100),
                stock SMALLINT UNSIGNED
            );"#,
        )?;

        db.exec(
            "INSERT INTO product_inventory (id, name, stock)
            VALUES (1, 'Laptop', 3), (3, 'Mechanical keyboard', 5);",
        )?;

        let query = db.exec(
            "INSERT INTO product_inventory (id, name, stock) VALUES (2, 'Wireless mouse', -2);",
        );

        let underflow_value = -2;
        assert!(query.is_err());
        assert_eq!(
            query.unwrap_err(),
            AnalyzerError::Overflow(Type::UnsignedSmallInt, underflow_value as _).into()
        );

        let query = db.exec("SELECT * FROM product_inventory;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::UnsignedInteger),
                    Column::new("name", Type::Varchar(100)),
                    Column::new("stock", Type::UnsignedSmallInt)
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        Value::String("Laptop".into()),
                        Value::Number(3)
                    ],
                    vec![
                        Value::Number(3),
                        Value::String("Mechanical keyboard".into()),
                        Value::Number(5)
                    ]
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn test_serial_bounds() -> DatabaseResult {
        let mut db = Database::default();
        let max = Type::SmallSerial.max();

        db.exec("CREATE TABLE users (id SMALLSERIAL PRIMARY KEY, name VARCHAR(50));")?;

        for serial in (0..max) {
            db.exec(&format!(
                "INSERT INTO users (name) VALUES ('user_{serial}');"
            ))?;
        }

        let query = db.exec("SELECT * FROM users;")?;
        assert_eq!(query.tuples.len(), max);
        assert_eq!(
            query.schema,
            Schema::new(vec![
                Column::primary_key("id", Type::SmallSerial),
                Column::new("name", Type::Varchar(50))
            ])
        );

        let query = db.exec("INSERT INTO users (name) VALUES ('some cool name');");
        assert!(query.is_err());
        assert_eq!(
            query.unwrap_err(),
            AnalyzerError::Overflow(Type::SmallSerial, max).into()
        );

        Ok(())
    }

    #[test]
    fn test_delete() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135), age INT);")?;
        db.exec(
            r#"
            INSERT INTO employees (id, name, age) VALUES 
            (1, 'John Doe', 25),
            (2, 'Mary Dove', 37),
            (3, 'Paul Dean', 19);
        "#,
        )?;

        let query = db.exec("DELETE FROM employees;")?;
        assert!(query.is_empty());

        Ok(())
    }

    #[test]
    fn test_delete_with_where() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135), birth_date DATE);",
        )?;
        db.exec(
            r#"
            INSERT INTO employees (id, name, birth_date) VALUES 
            (1, 'John Doe', '1995-03-01'),
            (2, 'Mary Dove', '2000-04-24'),
            (3, 'Paul Dean', '1999-01-27');
        "#,
        )?;
        db.exec("DELETE FROM employees WHERE id = 2;")?;

        let query = db.exec("SELECT * FROM employees;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(135)),
                    Column::new("birth_date", Type::Date)
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        Value::String("John Doe".into()),
                        temporal!("1995-03-01")?,
                    ],
                    vec![
                        Value::Number(3),
                        Value::String("Paul Dean".into()),
                        temporal!("1999-01-27")?,
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_delete_where_with_range() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135), age INT, email VARCHAR(255) UNIQUE);")?;
        const SEEDING_QUERY: &str = r#"
            INSERT INTO employees (id, name, age, email) VALUES 
            (1, 'John Doe', 25, 'johndoe@email.com'),
            (2, 'Mary Dove', 37, 'marydove@email.com'),
            (3, 'Paul Dean', 19, 'pauldean@email.com');
        "#;
        const SELECT_QUERY: &str = "SELECT * FROM employees;";
        let schema = Schema::new(vec![
            Column::primary_key("id", Type::Integer),
            Column::new("name", Type::Varchar(135)),
            Column::new("age", Type::Integer),
            Column::unique("email", Type::Varchar(255)),
        ]);

        db.exec(SEEDING_QUERY)?;
        db.exec("DELETE FROM employees WHERE age <= 30;")?;

        let query = db.exec(SELECT_QUERY)?;
        assert_eq!(
            query,
            QuerySet {
                schema: schema.clone(),
                tuples: vec![vec![
                    Value::Number(2),
                    Value::String("Mary Dove".into()),
                    Value::Number(37),
                    Value::String("marydove@email.com".into())
                ]]
            }
        );

        db.exec("DELETE FROM employees;")?;
        let query = db.exec(SELECT_QUERY)?;
        assert!(query.is_empty());

        db.exec(SEEDING_QUERY)?;
        db.exec("DELETE FROM employees WHERE email <= 'marydove@email.com';")?;

        let query = db.exec(SELECT_QUERY)?;
        assert_eq!(
            query,
            QuerySet {
                schema,
                tuples: vec![vec![
                    Value::Number(3),
                    Value::String("Paul Dean".into()),
                    Value::Number(19),
                    Value::String("pauldean@email.com".into())
                ]]
            }
        );

        Ok(())
    }

    #[test]
    fn test_delete_with_auto_index() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("
            CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(135), age INT, email VARCHAR(255) UNIQUE);
        ")?;
        db.exec(
            r#"INSERT INTO employees (id, name, age, email) VALUES 
            (1, 'John Doe', 25, 'johndoe@email.com'),
            (2, 'Mary Dove', 37, 'marydove@email.com'),
            (3, 'Paul Dean', 19, 'pauldean@email.com');
        "#,
        )?;

        let query = db.exec("SELECT * FROM employees WHERE id >= 2;")?;
        assert_eq!(query.tuples.len(), 2);

        db.exec("DELETE FROM employees WHERE id >= 2;")?;
        let query = db.exec("SELECT * FROM employees;")?;

        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(135)),
                    Column::new("age", Type::Integer),
                    Column::unique("email", Type::Varchar(255)),
                ]),
                tuples: vec![vec![
                    Value::Number(1),
                    Value::String("John Doe".into()),
                    Value::Number(25),
                    Value::String("johndoe@email.com".into())
                ]]
            }
        );

        db.assert_index_contains(
            &index!(unique on employees (email)),
            &[vec![
                Value::String("johndoe@email.com".into()),
                Value::Number(1),
            ]],
        )?;

        Ok(())
    }

    #[test]
    fn test_delete_without_match() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), birth_date DATE, email VARCHAR(255) UNIQUE);",
        )?;
        db.exec(
            r#"
            INSERT INTO employees (id, name, birth_date, email) VALUES 
            (1, 'John Doe', '1995-03-01', 'johndoe@email.com'),
            (2, 'Mary Dove', '2000-04-24', 'marydove@email.com'),
            (3, 'Paul Dean', '1999-01-27', 'pauldean@email.com');
        "#,
        )?;
        db.exec("DELETE FROM employees WHERE id >= 4;")?;

        let query = db.exec("SELECT * FROM employees;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(255)),
                    Column::new("birth_date", Type::Date),
                    Column::unique("email", Type::Varchar(255))
                ]),
                tuples: vec![
                    vec![
                        Value::Number(1),
                        Value::String("John Doe".into()),
                        temporal!("1995-03-01")?,
                        Value::String("johndoe@email.com".into())
                    ],
                    vec![
                        Value::Number(2),
                        Value::String("Mary Dove".into()),
                        temporal!("2000-04-24")?,
                        Value::String("marydove@email.com".into())
                    ],
                    vec![
                        Value::Number(3),
                        Value::String("Paul Dean".into()),
                        temporal!("1999-01-27")?,
                        Value::String("pauldean@email.com".into())
                    ]
                ]
            }
        );

        db.assert_index_contains(
            &index!(unique on employees (email)),
            &[
                vec![Value::String("johndoe@email.com".into()), Value::Number(1)],
                vec![Value::String("marydove@email.com".into()), Value::Number(2)],
                vec![Value::String("pauldean@email.com".into()), Value::Number(3)],
            ],
        )?;

        Ok(())
    }

    #[test]
    fn test_delete_where_with_multiple_ranges() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE users (id INT PRIMARY KEY, email VARCHAR(255) UNIQUE);")?;
        db.exec(
            r#"
            INSERT INTO users (id, email) VALUES
            (1, 'johndoe@email.com'),
            (2, 'marydove@email.com'),
            (3, 'pauldean@email.com'),
            (4, 'philipdahmer@email.com'),
            (5, 'katedavis@email.com');
        "#,
        )?;
        db.exec("DELETE FROM users WHERE id >= 2 AND id <= 3 OR id > 4;")?;

        let query = db.exec("SELECT * FROM users;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::unique("email", Type::Varchar(255))
                ]),
                tuples: vec![
                    vec![Value::Number(1), Value::String("johndoe@email.com".into())],
                    vec![
                        Value::Number(4),
                        Value::String("philipdahmer@email.com".into())
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_delete_from_empty_table() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(69), email VARCHAR(135) UNIQUE);",
        )?;
        db.exec("CREATE UNIQUE INDEX name_idx ON users(name);")?;
        db.exec("DELETE FROM users WHERE id >= 100;")?;

        let query = db.exec("SELECT * FROM users;")?;
        assert!(query.is_empty());

        db.assert_index_contains(&index!(unique on users (email)), &[])?;
        db.assert_index_contains("name_idx", &[])?;

        Ok(())
    }

    #[test]
    fn test_transaction_commit() -> DatabaseResult {
        let mut db = Database::default();
        db.exec("CREATE TABLE products (id INT PRIMARY KEY, price INT, name VARCHAR(30), discount INT);")?;

        db.exec("BEGIN TRANSACTION;")?;

        db.exec("INSERT INTO products (id, name, price, discount) VALUES (1, 'coffee', 18, 0);")?;
        db.exec("INSERT INTO products (id, name, price, discount) VALUES (2, 'tea', 12, 0);")?;
        db.exec("INSERT INTO products (id, name, price, discount) VALUES (3, 'soda', 10, 1);")?;

        db.exec("UPDATE products SET discount = 2 WHERE name = 'coffee';")?;
        db.exec("UPDATE products SET price = 11 WHERE id = 3;")?;

        db.exec("COMMIT;")?;

        let query = db.exec("SELECT id, name, price, discount FROM products ORDER BY price;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(30)),
                    Column::new("price", Type::Integer),
                    Column::new("discount", Type::Integer),
                ]),
                tuples: vec![
                    vec![
                        Value::Number(3),
                        Value::String("soda".into()),
                        Value::Number(11),
                        Value::Number(1)
                    ],
                    vec![
                        Value::Number(2),
                        Value::String("tea".into()),
                        Value::Number(12),
                        Value::Number(0)
                    ],
                    vec![
                        Value::Number(1),
                        Value::String("coffee".into()),
                        Value::Number(18),
                        Value::Number(2),
                    ]
                ]
            }
        );

        Ok(())
    }

    #[test]
    fn test_transaction_abort() -> DatabaseResult {
        let mut db = Database::default();
        db.exec("CREATE TABLE products (id INT PRIMARY KEY, price INT, name VARCHAR(30), discount INT);")?;

        db.exec("BEGIN TRANSACTION;")?;

        db.exec("INSERT INTO products (id, name, price, discount) VALUES (1, 'coffee', 18, 0);")?;
        db.exec("INSERT INTO products (id, name, price, discount) VALUES (2, 'tea', 12, 0);")?;
        db.exec("INSERT INTO products (id, name, price, discount) VALUES (3, 'soda', 10, 1);")?;

        db.exec("UPDATE products SET discount = 2 WHERE name = 'coffee';")?;
        db.exec("UPDATE products SET price = 11 WHERE id = 3;")?;

        db.exec("ROLLBACK;")?;

        let query = db.exec("SELECT id, name, price, discount FROM products ORDER BY price;")?;
        assert!(query.is_empty());
        assert_eq!(
            query.schema,
            Schema::new(vec![
                Column::primary_key("id", Type::Integer),
                Column::new("name", Type::Varchar(30)),
                Column::new("price", Type::Integer),
                Column::new("discount", Type::Integer),
            ]),
        );

        Ok(())
    }

    #[test]
    fn test_inserting_int_overflow() -> DatabaseResult {
        let mut db = Database::default();
        const INT_MAX: i64 = i32::MAX as i64 + 1;
        const INT_MIN: i64 = i32::MIN as i64 - 1;

        db.exec("CREATE TABLE coffee_shops (id INT PRIMARY KEY, address VARCHAR(100));")?;
        let query = db.exec(&format!(
            "INSERT INTO coffee_shops (id, address) VALUES ({}, 'Mongibello');",
            INT_MAX
        ));

        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::Integer, INT_MAX as _).into())
        );

        let query = db.exec(&format!(
            "INSERT INTO coffee_shops (id, address) VALUES ({}, 'Mongibello');",
            INT_MIN
        ));

        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::Integer, INT_MIN as _).into())
        );

        Ok(())
    }

    #[test]
    fn test_inserting_big_int_overflow() -> DatabaseResult {
        let mut db = Database::default();
        const BIG_INT_MAX: i128 = i64::MAX as i128 + 1;
        const BIG_INT_MIN: i128 = i64::MIN as i128 - 1;

        db.exec("CREATE TABLE coffee_shops (id BIGINT PRIMARY KEY, address VARCHAR(100));")?;
        let query = db.exec(&format!(
            "INSERT INTO coffee_shops (id, address) VALUES ({}, 'Mongibello');",
            BIG_INT_MAX
        ));

        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::BigInteger, BIG_INT_MAX as _).into())
        );

        let query = db.exec(&format!(
            "INSERT INTO coffee_shops (id, address) VALUES ({}, 'Mongibello');",
            BIG_INT_MIN
        ));

        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::BigInteger, BIG_INT_MIN as _).into())
        );

        Ok(())
    }

    #[test]
    fn test_inserting_uint_overflow() -> DatabaseResult {
        let mut db = Database::default();
        const UINT_MAX: u64 = u32::MAX as u64 + 1;
        const NEGATIVE_VALUE: i64 = -1;

        db.exec(
            r#"
            CREATE TABLE companies (
                id INT PRIMARY KEY,
                years INT UNSIGNED,
                name VARCHAR(50)
            );
        "#,
        )?;

        let query = db.exec(&format!(
            "INSERT INTO companies (id, years, name) VALUES (69, {}, 'Mongibello');",
            UINT_MAX
        ));

        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedInteger, UINT_MAX as usize).into())
        );

        let query = db.exec(&format!(
            "INSERT INTO companies (id, years, name) VALUES (69, {}, 'Sanremo');",
            NEGATIVE_VALUE
        ));
        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedInteger, NEGATIVE_VALUE as usize).into())
        );

        Ok(())
    }

    #[test]
    fn test_inserting_ubigint_overflow() -> DatabaseResult {
        let mut db = Database::default();
        const UBIG_INT_MAX: u128 = u64::MAX as u128 + 1;
        const NEGATIVE_VALUE: i128 = -1;

        db.exec(
            r#"
            CREATE TABLE companies (
                id INT PRIMARY KEY,
                years BIGINT UNSIGNED,
                name VARCHAR(50)
            );
        "#,
        )?;

        let query = db.exec(&format!(
            "INSERT INTO companies (id, years, name) VALUES (69, {}, 'Mongibello');",
            UBIG_INT_MAX
        ));

        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedBigInteger, UBIG_INT_MAX as usize).into())
        );

        let query = db.exec(&format!(
            "INSERT INTO companies (id, years, name) VALUES (69, {}, 'Sanremo');",
            NEGATIVE_VALUE
        ));
        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedBigInteger, NEGATIVE_VALUE as usize).into())
        );

        Ok(())
    }

    #[test]
    fn test_drop_table() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            r#"
            CREATE TABLE companies (
                id INT PRIMARY KEY,
                years BIGINT UNSIGNED,
                name VARCHAR(50)
            );
        "#,
        )?;

        #[rustfmt::skip]
        db.exec("INSERT INTO companies (id, years, name) VALUES (69, 142, 'Mongibello');")?;
        let query = db.exec("SELECT * FROM companies;")?;
        assert!(!query.is_empty());

        db.exec("DROP TABLE companies;")?;
        let query = db.exec("SELECT * FROM companies;");
        assert!(query.is_err());
        assert_eq!(
            query.unwrap_err(),
            DatabaseError::Sql(SqlError::InvalidTable("companies".into()))
        );

        Ok(())
    }

    #[test]
    fn test_incrementing_serial() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(15));")?;

        (0..15).for_each(|user| {
            db.exec(&format!(
                "INSERT INTO users (name) VALUES ('user_{}');",
                user
            ))
            .unwrap();
        });

        let query = db.exec("SELECT id FROM users;")?;
        query
            .tuples
            .iter()
            .enumerate()
            .for_each(|(id, user)| assert_eq!(user, &vec![Value::Number((id + 1) as i128)]));

        Ok(())
    }

    #[test]
    fn test_serial_during_transactions() -> DatabaseResult {
        let mut db = Database::default();

        db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(30));")?;
        // 1st successful insert
        db.exec("BEGIN TRANSACTION;")?;
        db.exec("INSERT INTO users(name) VALUES ('Alice');")?;
        db.exec("COMMIT;")?;

        // failed insertion
        db.exec("BEGIN TRANSACTION;")?;
        db.exec("INSERT INTO users(name) VALUES ('Milena');")?;
        db.exec("ROLLBACK;")?;

        // 2nd successful insert
        db.exec("BEGIN TRANSACTION;")?;
        db.exec("INSERT INTO users(name) VALUES ('Carla');")?;
        db.exec("COMMIT;")?;

        let ids: Vec<i128> = db
            .exec("SELECT id FROM users;")?
            .tuples
            .iter()
            .map(|row| match row[0] {
                Value::Number(num) => num,
                _ => panic!("Should be a number"),
            })
            .collect();

        assert_eq!(ids, vec![1, 3]);

        Ok(())
    }

    #[test]
    fn test_float_types() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            r#"
            CREATE TABLE scientific (
                id SERIAL PRIMARY KEY,
                precise_temperature DOUBLE PRECISION,
                co2_levels DOUBLE PRECISION,
                measurement_time TIMESTAMP
            );
        "#,
        )?;

        db.exec(
            r#"
            INSERT INTO scientific (precise_temperature, co2_levels, measurement_time)
            VALUES
                (23.456789, 415.123456789, '2024-02-03 10:00:00'),
                (20.123456, 417.123789012, '2024-02-03 11:00:00'),
                (22.789012, 418.456123789, '2024-02-03 12:00:00');
        "#,
        )?;

        let query = db.exec("SELECT * FROM scientific WHERE precise_temperature >= 23;")?;
        assert_eq!(
            query,
            QuerySet {
                schema: Schema::new(vec![
                    Column::primary_key("id", Type::Serial),
                    Column::new("precise_temperature", Type::DoublePrecision),
                    Column::new("co2_levels", Type::DoublePrecision),
                    Column::new("measurement_time", Type::DateTime),
                ]),
                tuples: vec![vec![
                    Value::Number(1),
                    Value::Float(23.456789),
                    Value::Float(415.123456789),
                    temporal!("2024-02-03 10:00:00").unwrap()
                ]]
            }
        );

        Ok(())
    }

    #[test]
    fn test_implicit_cast() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            r#"
            CREATE TABLE measurements (
                id SERIAL PRIMARY KEY,
                reading DOUBLE PRECISION,
                sensor_a INTEGER,
                sensor_b SMALLINT
            );
        "#,
        )?;

        db.exec(
            r#"
            INSERT INTO measurements (reading, sensor_a, sensor_b)
            VALUES
                (25.7, 10, 15),
                (20.0, 5, 12);
        "#,
        )?;

        let query = db.exec("SELECT * FROM measurements WHERE sensor_a > reading;")?;
        assert_eq!(
            query.schema,
            Schema::new(vec![
                Column::primary_key("id", Type::Serial),
                Column::new("reading", Type::DoublePrecision),
                Column::new("sensor_a", Type::Integer),
                Column::new("sensor_b", Type::SmallInt)
            ]),
        );
        assert!(query.is_empty());

        let query = db.exec("SELECT reading FROM measurements WHERE sensor_a <= sensor_b;")?;
        assert_eq!(2, query.tuples.len());

        Ok(())
    }

    #[test]
    fn test_arithmetic_op_on_floats() -> DatabaseResult {
        let mut db = Database::default();

        // TODO: correct sequence for underline table/column names
        db.exec(
            r#"
           CREATE TABLE prices (
                id SERIAL PRIMARY KEY,
                base_price REAL,
                discount REAL,
                tax_rate DOUBLE PRECISION
            );"#,
        )?;
        db.exec(
            r#"
            INSERT INTO prices (base_price, discount, tax_rate) 
            VALUES
                (100.00, 20.00, 0.0825),
                (49.99, 5.00, 0.0725),
                (199.95, 0.00, 0.0625);
        "#,
        )?;

        let query = db.exec(
            r#"
            SELECT
                base_price - discount,
                (base_price - discount) * (1.0 + tax_rate)
                FROM prices;
            "#,
        )?;

        let expected = vec![(80.00, 86.60), (44.99, 48.25), (199.95, 212.44)];
        // we could just do move precise expected values, but this tolorance is fine
        let threshold = 0.01;

        query
            .tuples
            .iter()
            .enumerate()
            .for_each(|(idx, row)| match row[..] {
                [Value::Float(a), Value::Float(b)] => {
                    let (expected_a, expected_b) = expected[idx];

                    assert!(
                        (a - expected_a).abs() < threshold,
                        "Expected {a} ≈ {expected_a}",
                    );
                    assert!(
                        (b - expected_b).abs() < threshold,
                        "Expected {b} ≈ {expected_b}",
                    )
                }
                _ => panic!("Invalid row pattern"),
            });

        Ok(())
    }

    #[test]
    fn test_comparison_on_floats() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            r#"
            CREATE TABLE temperature_readings (
                reading_id SERIAL PRIMARY KEY,
                sensor_a REAL,
                sensor_b DOUBLE PRECISION
            );"#,
        )?;

        db.exec(
            r#"
            INSERT INTO temperature_readings(sensor_a, sensor_b) VALUES
                (25.5, 25.5000001),
                (25.5, 25.5),
                (0.3, 0.1 + 0.2),
                (100.0, 99.9999999);
        "#,
        );

        let query = db.exec(
            r#"
            SELECT 
                sensor_a = sensor_b,
                sensor_a > sensor_b,
                sensor_a < sensor_b
                FROM temperature_readings;
            "#,
        )?;

        assert_eq!(
            query.tuples,
            vec![
                vec![
                    Value::Boolean(false),
                    Value::Boolean(false),
                    Value::Boolean(true)
                ],
                vec![
                    Value::Boolean(true),
                    Value::Boolean(false),
                    Value::Boolean(false)
                ],
                vec![
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::Boolean(false)
                ],
                vec![
                    Value::Boolean(false),
                    Value::Boolean(true),
                    Value::Boolean(false)
                ]
            ]
        );

        Ok(())
    }

    #[test]
    fn insert_negative_floats() -> DatabaseResult {
        let mut db = Database::default();
        db.exec("CREATE TABLE location (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), lat REAL, lon REAL);")?;

        db.exec(
            r#"
            INSERT INTO location (name, lat, lon) VALUES
            ('Rio de Janeiro, Brazil', -22.906847, -43.172897),
            ('London, United Kingdom', 51.507351, -0.127758);
        "#,
        )?;

        Ok(())
    }

    #[test]
    fn date_ordering() -> DatabaseResult {
        let mut db = Database::default();
        db.exec(
            r#"
            CREATE TABLE temporal_data (
                id SERIAL PRIMARY KEY,
                event_name VARCHAR(100),
                event_datetime TIMESTAMP,
                event_date DATE,
                event_time TIME
            );
        "#,
        )?;
        db.exec(
            r#"
            INSERT INTO temporal_data (event_name, event_datetime, event_date, event_time) 
            VALUES
                ('Unix Epoch', '1970-01-01 00:00:00', '1970-01-01', '00:00:00'),
                ('Future Event', '2025-04-20 10:01:23', '2025-04-20', '10:01:23'),
                ('WWI Start', '1914-06-28 00:00:00', '1914-06-28', '00:00:00'),
                ('Midday', '2023-01-01 12:00:00', '2023-01-01', '12:00:00'),
                ('Midnight', '2023-01-01 00:00:00', '2023-01-01', '00:00:00');
            "#,
        )?;

        let query =
            db.exec("SELECT event_name FROM temporal_data WHERE event_time > '00:00:00';")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("Future Event".into())],
                vec![Value::String("Midday".into())]
            ]
        );

        Ok(())
    }

    #[test]
    #[ignore = "what the hell is happening here?"]
    fn select_with_between() -> DatabaseResult {
        let mut db = Database::default();
        db.exec(
            r#"
            CREATE TABLE invoice (
                invoice_id     SERIAL PRIMARY KEY,
                client_id      INTEGER,
                total          REAL,
                invoice_date   TIMESTAMP
            );
            "#,
        )?;
        db.exec(
            r#"
            INSERT INTO invoice (client_id, total, invoice_date) VALUES
                (101, 55.20, '2023-08-01 09:15:00'),
                (102, 12.75, '2023-08-02 11:30:45'),
                (103, 89.90, '2023-08-03 14:05:10'),
                (104, 10.00, '2023-08-04 10:20:00'),
                (105, 33.30, '2023-08-05 16:45:30'),
                (106, 75.00, '2023-08-06 13:55:15'),
                (107, 9.99,  '2023-08-07 08:00:00');
            "#,
        )?;

        let query = db.exec(
            r#"
            SELECT
                client_id, invoice_id, total, invoice_date
            FROM invoice
            WHERE
                invoice_date BETWEEN '2023-08-02' AND '2023-08-06'
            ORDER BY invoice_date;
            "#,
        )?;

        assert_eq!(
            query.tuples,
            vec![
                vec![
                    Value::Number(103),
                    Value::Number(3),
                    Value::Float(89.90),
                    temporal!("2023-08-03 14:05:10")?
                ],
                vec![
                    Value::Number(105),
                    Value::Number(5),
                    Value::Float(33.30),
                    temporal!("2023-08-05 16:45:30")?
                ],
                vec![
                    Value::Number(106),
                    Value::Number(6),
                    Value::Float(75.00),
                    temporal!("2023-08-06 13:55:15")?
                ]
            ]
        );

        Ok(())
    }

    #[test]
    fn select_with_in() -> DatabaseResult {
        let mut db = Database::default();

        db.exec(
            "CREATE TABLE actors (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(50));",
        )?;
        let names = [
            ("Meryl", "Allen"),
            ("Cuba", "Allen"),
            ("Kim", "Allen"),
            ("Jon", "Chase"),
            ("Ed", "Chase"),
            ("Susan", "Davis"),
            ("Jennifer", "Davis"),
            ("Susan", "Davis"),
            ("Alex", "Johnson"),
        ];

        for (name, last_name) in names {
            db.exec(
                format!("INSERT INTO actors (name, last_name) VALUES ('{name}', '{last_name}');")
                    .as_str(),
            )?;
        }

        let query = db.exec(
            "SELECT name, last_name FROM actors WHERE last_name IN ('Allen', 'Chase', 'Davis');",
        )?;

        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("Meryl".into()), Value::String("Allen".into())],
                vec![Value::String("Cuba".into()), Value::String("Allen".into())],
                vec![Value::String("Kim".into()), Value::String("Allen".into())],
                vec![Value::String("Jon".into()), Value::String("Chase".into())],
                vec![Value::String("Ed".into()), Value::String("Chase".into())],
                vec![Value::String("Susan".into()), Value::String("Davis".into())],
                vec![
                    Value::String("Jennifer".into()),
                    Value::String("Davis".into())
                ],
                vec![Value::String("Susan".into()), Value::String("Davis".into())],
            ]
        );

        Ok(())
    }

    #[test]
    fn select_with_like() -> DatabaseResult {
        let mut db = Database::default();
        db.exec("CREATE TABLE customer (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(50));")?;
        db.exec(
            r#"
        INSERT INTO customer (name, last_name) VALUES 
            ('Jennifer', 'Smith'),
            ('Jenny', 'Johnson'),
            ('Benjamin', 'Brown'),
            ('Jessica', 'Jones'),
            ('Jenifer', 'Miller'),
            ('Michael', 'Davis');
            "#,
        )?;
        let query = db.exec("SELECT name, last_name FROM customer WHERE name LIKE 'Jen%';")?;

        assert_eq!(
            query.tuples,
            vec![
                vec![
                    Value::String("Jennifer".into()),
                    Value::String("Smith".into()),
                ],
                vec![
                    Value::String("Jenny".into()),
                    Value::String("Johnson".into())
                ],
                vec![
                    Value::String("Jenifer".into()),
                    Value::String("Miller".into())
                ]
            ],
        );

        Ok(())
    }

    #[test]
    fn insert_uuids() -> DatabaseResult {
        let mut db = Database::default();
        let uuid = Uuid::from_str("d111ff02-e19f-4e6c-ac44-5804f72f7e8d").unwrap();

        db.exec("CREATE TABLE contracts (id UUID PRIMARY KEY, name VARCHAR(30));")?;
        db.exec("INSERT INTO contracts (name) VALUES ('IT consulting'), ('Market agency');")?;
        db.exec(&format!(
            "INSERT INTO contracts (id, name) VALUES ('{uuid}', 'Residency rental');"
        ))?;

        let query = db.exec("SELECT id FROM contracts ORDER BY name;")?;
        assert_eq!(query.tuples.len(), 3);
        assert_eq!(query.tuples.last(), Some(&vec![Value::Uuid(uuid)]));

        let query = db.exec(&format!("SELECT name FROM contracts WHERE id = '{uuid}';"))?;
        assert_eq!(
            query.tuples,
            vec![vec![Value::String("Residency rental".into())]]
        );
        Ok(())
    }

    #[test]
    fn sort_uuids() -> DatabaseResult {
        let mut db = Database::default();

        let uuids: [Uuid; 3] = [
            Uuid::from_str("d111ff02-e19f-4e6c-ac44-5804f72f7e8d").unwrap(),
            Uuid::from_str("a0000000-0000-0000-0000-000000000000").unwrap(),
            Uuid::from_str("ffffffff-ffff-ffff-ffff-ffffffffffff").unwrap(),
        ];

        db.exec("CREATE TABLE users (id UUID PRIMARY KEY, name VARCHAR(30), age INT UNSIGNED);")?;
        db.exec(&format!(
            r#"
            INSERT INTO users (id, name, age) VALUES
            ('{}', 'John Doe', 30),
            ('{}', 'Mary Dove', 27),
            ('{}', 'Richard Dahmer', 31);
        "#,
            uuids[0], uuids[1], uuids[2]
        ))?;

        let query = db.exec("SELECT id, name, age FROM users ORDER BY id;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![
                    Value::Uuid(uuids[1]),
                    Value::String("Mary Dove".into()),
                    Value::Number(27)
                ],
                vec![
                    Value::Uuid(uuids[0]),
                    Value::String("John Doe".into()),
                    Value::Number(30)
                ],
                vec![
                    Value::Uuid(uuids[2]),
                    Value::String("Richard Dahmer".into()),
                    Value::Number(31)
                ]
            ]
        );

        Ok(())
    }

    #[test]
    fn substring_function() -> DatabaseResult {
        let mut db = Database::default();
        db.exec("CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(70));")?;
        db.exec(
            r#"
            INSERT INTO customers (name) VALUES
            ('Jared'), ('Mary'), ('Patricia'), ('Linda');
            "#,
        )?;

        let query = db.exec("SELECT SUBSTRING(name FROM 1 FOR 1) FROM customers;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("J".into())],
                vec![Value::String("M".into())],
                vec![Value::String("P".into())],
                vec![Value::String("L".into())],
            ]
        );

        let query = db.exec("SELECT SUBSTRING(name FROM 100 FOR 2) FROM customers;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("".into())],
                vec![Value::String("".into())],
                vec![Value::String("".into())],
                vec![Value::String("".into())],
            ]
        );

        let query = db.exec("SELECT SUBSTRING(name FROM 2 FOR 0) FROM customers;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("".into())],
                vec![Value::String("".into())],
                vec![Value::String("".into())],
                vec![Value::String("".into())],
            ]
        );

        let query = db.exec("SELECT SUBSTRING(name FROM 3) FROM customers;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("red".into())],
                vec![Value::String("ry".into())],
                vec![Value::String("tricia".into())],
                vec![Value::String("nda".into())],
            ]
        );

        Ok(())
    }

    #[test]
    fn ascii_function() -> DatabaseResult {
        let mut db = Database::default();
        db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(30));")?;
        db.exec(
            r#"
            INSERT INTO users (name) VALUES
            ('Alice'), ('Αλέξανδρος'), ('Zoe'), ('Émile'), ('Chloé');
            "#,
        )?;

        let query = db.exec("SELECT ASCII(name) FROM users ORDER BY name;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::Number(65)],
                vec![Value::Number(67)],
                vec![Value::Number(90)],
                vec![Value::Number(201)],
                vec![Value::Number(913)],
            ]
        );

        Ok(())
    }

    #[test]
    fn concat_function() -> DatabaseResult {
        let mut db = Database::default();
        db.exec(
            r#"
            CREATE TABLE contacts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(15)
            );
        "#,
        )?;
        db.exec(
            r#"
            INSERT INTO contacts (name, email, phone)
            VALUES
                ('John Doe', 'john.doe@example.com', '123-456-7890'),
                ('Jane Smith', 'jane.smith@example.com', '987-654-3210'),
                ('Bob Johnson', 'bob.johnson@example.com', '555-1234'),
                ('Alice Brown', 'alice.brown@example.com', '555-1235'),
                ('Charlie Davis', 'charlie.davis@example.com', '987-654-3210');
        "#,
        )?;

        let query =
            db.exec("SELECT CONCAT(name, ' ', '(', email, ')', ' ', phone) FROM contacts;")?;

        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String(
                    "John Doe (john.doe@example.com) 123-456-7890".into()
                )],
                vec![Value::String(
                    "Jane Smith (jane.smith@example.com) 987-654-3210".into()
                )],
                vec![Value::String(
                    "Bob Johnson (bob.johnson@example.com) 555-1234".into()
                )],
                vec![Value::String(
                    "Alice Brown (alice.brown@example.com) 555-1235".into()
                )],
                vec![Value::String(
                    "Charlie Davis (charlie.davis@example.com) 987-654-3210".into()
                )]
            ]
        );

        Ok(())
    }

    #[test]
    fn position_function() -> DatabaseResult {
        let mut db = Database::default();
        db.exec(
            r#"
            CREATE TABLE films (
                id SERIAL PRIMARY KEY,
                title VARCHAR(100),
                description VARCHAR(255)
            );
        "#,
        )?;

        let inserts = [
            "INSERT INTO films (title, description) VALUES ('The Matrix', 'A computer hacker learns about the true nature of reality.');",
            "INSERT INTO films (title, description) VALUES ('Inception', 'A thief who steals corporate secrets through dream-sharing technology.');",
            "INSERT INTO films (title, description) VALUES ('Interstellar', 'A team of explorers travel through a wormhole in space.');",
            "INSERT INTO films (title, description) VALUES ('The Prestige', 'Two stage magicians engage in a battle to create the ultimate illusion.');",
            "INSERT INTO films (title, description) VALUES ('Memento', 'A man with short-term memory loss attempts to track down his wifes murderer.');"
        ];

        for insert in inserts {
            db.exec(insert)?;
        }

        let query = db.exec("SELECT title, POSITION('the' IN description) FROM films;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::String("The Matrix".into()), Value::Number(32)],
                vec![Value::String("Inception".into()), Value::Number(0)],
                vec![Value::String("Interstellar".into()), Value::Number(0)],
                vec![Value::String("The Prestige".into()), Value::Number(50)],
                vec![Value::String("Memento".into()), Value::Number(0)],
            ]
        );

        Ok(())
    }

    #[test]
    fn math_functions() -> DatabaseResult {
        let mut db = Database::default();
        db.exec(
            r#"
            CREATE TABLE employees (
                employee_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                salary DOUBLE PRECISION,
                bonus_percentage REAL,
                tax_deduction DOUBLE PRECISION
            );"#,
        )?;

        db.exec(
            r#"
            INSERT INTO employees (employee_id, first_name, last_name, salary, bonus_percentage, tax_deduction)
            VALUES
            (101, 'John', 'Smith', 75000.00, 15.00, -12500.00),
            (102, 'Sarah', 'Johnson', 68000.50, 12.50, -10200.75),
            (103, 'Michael', 'Williams', 92000.00, 20.00, -18400.00),
            (104, 'Emily', 'Brown', 55000.25, 10.25, -8250.38),
            (105, 'David', 'Jones', 110000.00, 25.00, -27500.00);
        "#,
        )?;

        let query = db.exec("SELECT first_name, salary, TRUNC(SQRT(salary), 2) FROM employees;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec!["John".into(), 75000f64.into(), 273.86f64.into()],
                vec!["Sarah".into(), 68000.50f64.into(), 260.76f64.into()],
                vec!["Michael".into(), 92000f64.into(), 303.31.into()],
                vec!["Emily".into(), 55000.25f64.into(), 234.52f64.into()],
                vec!["David".into(), 110000.00f64.into(), 331.66f64.into()],
            ]
        );

        let query = db.exec("SELECT SIGN(tax_deduction) FROM employees;")?;
        for row in query.tuples {
            assert!(row.iter().all(|i| i.eq(&Value::Number(-1))));
        }

        let query = db.exec("SELECT salary, TRUNC(salary/10000) FROM employees;")?;
        assert_eq!(
            query.tuples,
            vec![
                vec![75000f64.into(), 7f64.into()],
                vec![68000.5f64.into(), 6f64.into()],
                vec![92000f64.into(), 9f64.into()],
                vec![55000.25f64.into(), 5f64.into()],
                vec![110000f64.into(), 11f64.into()]
            ]
        );

        Ok(())
    }
}
