#![allow(unused)]

mod metadata;
mod schema;

pub(crate) use metadata::{IndexMetadata, Relation, TableMetadata};
pub(crate) use schema::{has_btree_key, umbra_schema, Schema};

use crate::core::date::DateParseError;
use crate::core::storage::btree::{BTree, FixedSizeCmp};
use crate::core::storage::page::PageNumber;
use crate::core::storage::pagination::io::FileOperations;
use crate::core::storage::pagination::pager::Pager;
use crate::core::storage::tuple;
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
use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::rc::Rc;

#[derive(Debug)]
pub(crate) struct Database<File> {
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

struct PreparedStatement<'db, File: FileOperations> {
    db: &'db mut Database<File>,
    exec: Option<Exec<File>>,
    autocommit: bool,
}

#[derive(Debug, PartialEq)]
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

macro_rules! temporal {
    ($time_str:expr) => {
        $time_str.try_into().map(Value::Temporal)
    };
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

    fn rollback(&mut self) -> Result<usize, DatabaseError> {
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
            return Err(DatabaseError::Sql(SqlError::InvalidTable(table.into())));
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
                    };
                    root += 1;

                    columns.iter().for_each(|col| {
                        col.constraints.iter().for_each(|constraint| {
                            let index = match constraint {
                                Constraint::Unique => index!(unique on name (col.name)),
                                Constraint::PrimaryKey => index!(primary on (name)),
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
    fn new(schema: Schema, tuples: Vec<Vec<Value>>) -> Self {
        Self { schema, tuples }
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
        println!("query {query:#?}");

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

        db.exec("CREATE TABLE shops (id INT UNSIGNED PRIMARY KEY, name VARCHAR(69));")?;

        let query = db.exec(&format!(
            "INSERT INTO shops (id, name) VALUES ({}, 'Shop A');",
            UINT_MAX
        ));
        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedInteger, UINT_MAX as _).into())
        );

        let query = db.exec(&format!(
            "INSERT INTO shops (id, name) VALUES ({}, 'Shop B');",
            NEGATIVE_VALUE
        ));
        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedInteger, NEGATIVE_VALUE as _).into())
        );

        Ok(())
    }

    #[test]
    fn test_inserting_ubigint_overflow() -> DatabaseResult {
        let mut db = Database::default();
        const UBIGINT_MAX: u128 = u64::MAX as u128 + 1;
        const NEGATIVE_VALUE: i64 = -1;

        db.exec("CREATE TABLE big_shops (id BIGINT UNSIGNED PRIMARY KEY, name VARCHAR(100));")?;

        let query = db.exec(&format!(
            "INSERT INTO big_shops (id, name) VALUES ({}, 'Big Shop A');",
            UBIGINT_MAX
        ));
        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedBigInteger, UBIGINT_MAX as _).into())
        );

        let query = db.exec(&format!(
            "INSERT INTO big_shops (id, name) VALUES ({}, 'Big Shop B');",
            NEGATIVE_VALUE
        ));
        assert!(query.is_err());
        assert_eq!(
            query,
            Err(AnalyzerError::Overflow(Type::UnsignedBigInteger, NEGATIVE_VALUE as _).into())
        );

        Ok(())
    }
}
