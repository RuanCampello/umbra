use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::db::{Ctx, DatabaseError, Schema, SqlError, TableMetadata, DB_METADATA, ROW_COL_ID};
use crate::sql::statement::{
    BinaryOperator, Constraint, Create, Drop, Expression, Statement, Type, UnaryOperator, Value,
};
use crate::vm::expression::{TypeError, VmType};
use std::collections::HashSet;
use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub(crate) enum AnalyzerError {
    MissingCols,
    DuplicateCols(String),
    MultiplePrimaryKeys,
    AlreadyExists(AlreadyExists),
    /// Attempt to assign Row Id special column manually.
    RowIdAssignment,
    MetadataAssignment,
    Overflow(Type, usize),
}

#[derive(Debug, PartialEq)]
pub(crate) enum AlreadyExists {
    Table(String),
    Index(String),
}

// TODO: we'll actually have a database error for this later
type AnalyzerResult<'exp, T> = Result<T, DatabaseError>;

pub(in crate::sql) fn analyze<'s>(
    statement: &'s Statement,
    ctx: &'s mut impl Ctx,
) -> AnalyzerResult<'s, ()> {
    match statement {
        Statement::Create(Create::Table { name, columns }) => {
            match ctx.metadata(name) {
                Err(DatabaseError::Sql(SqlError::InvalidTable(_))) => {}
                Err(e) => return Err(e),
                Ok(_) => {
                    return Err(SqlError::Analyzer(AnalyzerError::AlreadyExists(
                        AlreadyExists::Table(name.into()),
                    ))
                    .into())
                }
            };

            let mut primary_key = false;
            let mut duplicates = HashSet::new();

            for col in columns {
                if !duplicates.insert(&col.name) {
                    return Err(AnalyzerError::DuplicateCols(col.name.to_string()).into());
                }

                if col.name.eq(ROW_COL_ID) {
                    return Err(AnalyzerError::RowIdAssignment.into());
                }

                if col.constraints.contains(&Constraint::PrimaryKey) {
                    if primary_key {
                        return Err(AnalyzerError::MultiplePrimaryKeys.into());
                    }

                    primary_key = true;
                }
            }
        }

        Statement::Create(Create::Index {
            table,
            unique,
            name,
            ..
        }) => {
            if !unique {
                return Err(SqlError::Other("Non-unique index is not yet supported".into()).into());
            }

            let metadata = ctx.metadata(table)?;

            if metadata.indexes.iter().any(|idx| idx.name.eq(name)) {
                return Err(AnalyzerError::AlreadyExists(AlreadyExists::Index(name.into())).into());
            }
        }

        Statement::Insert {
            into,
            values: rows,
            columns,
        } => {
            let metadata = ctx.metadata(into)?;
            if into.eq(DB_METADATA) {
                return Err(AnalyzerError::MetadataAssignment.into());
            }

            let mut columns = columns.as_slice();
            let schema_columns: Vec<String>;

            if columns.is_empty() {
                schema_columns = metadata.schema.columns_ids();
                columns = schema_columns.as_slice();

                if columns[0].eq(ROW_COL_ID) {
                    columns = &schema_columns[1..];
                }
            }

            for row in rows {
                if row.len() != columns.len() {
                    return Err(AnalyzerError::MissingCols.into());
                }

                let mut seen = HashSet::new();
                for col in columns {
                    if metadata.schema.index_of(col).is_none() {
                        return Err(SqlError::InvalidColumn(col.into()).into());
                    }
                    if !seen.insert(col) {
                        return Err(AnalyzerError::DuplicateCols(col.into()).into());
                    }
                    if col.eq(ROW_COL_ID) {
                        return Err(AnalyzerError::MetadataAssignment.into());
                    }
                }

                let schema_len = if metadata.schema.columns[0].name.eq(ROW_COL_ID) {
                    metadata.schema.columns.len() - 1
                } else {
                    metadata.schema.columns.len()
                };
                if schema_len != columns.len() {
                    return Err(AnalyzerError::MissingCols.into());
                }

                for (expr, col) in row.iter().zip(columns) {
                    analyze_assignment(metadata, col, expr, false)?;
                }
            }
        }

        Statement::Select {
            columns,
            from,
            order_by,
            r#where,
        } => {
            let metadata = ctx.metadata(from)?;

            for expr in columns {
                if expr.ne(&Expression::Wildcard) {
                    analyze_expression(&metadata.schema, None, expr)?;
                }
            }

            analyze_where(&metadata.schema, r#where)?;

            for expr in order_by {
                analyze_expression(&metadata.schema, None, expr)?;
            }
        }

        Statement::Delete { from, r#where } => {
            let metadata = ctx.metadata(from)?;

            if from.eq(DB_METADATA) {
                return Err(AnalyzerError::MetadataAssignment.into());
            }

            analyze_where(&metadata.schema, r#where)?;
        }

        Statement::Update {
            r#where,
            table,
            columns,
        } => {
            let metadata = ctx.metadata(table)?;
            if table.eq(DB_METADATA) {
                return Err(AnalyzerError::MetadataAssignment.into());
            }

            for col in columns {
                analyze_assignment(metadata, &col.identifier, &col.value, true)?;
            }
            analyze_where(&metadata.schema, r#where)?;
        }

        Statement::Drop(Drop::Table(name)) => {
            ctx.metadata(name)?;
        }

        Statement::Explain(statement) => analyze(statement, ctx)?,
        _ => {}
    }

    Ok(())
}

fn analyze_assignment<'exp, 'id>(
    table: &'exp TableMetadata,
    column: &str,
    value: &'exp Expression,
    allow_id: bool,
) -> Result<(), SqlError> {
    if column.eq(ROW_COL_ID) {
        return Err(AnalyzerError::RowIdAssignment.into());
    }

    let idx = table
        .schema
        .index_of(column)
        .ok_or(SqlError::InvalidColumn(column.into()))?;
    let data_type = &table.schema.columns[idx].data_type;
    let expect_type = VmType::from(data_type);

    let evaluate_type = match allow_id {
        true => analyze_expression(&table.schema, Some(data_type), value)?,
        false => analyze_expression(&Schema::empty(), Some(data_type), value)?,
    };

    if expect_type.ne(&evaluate_type) {
        return Err(SqlError::Type(TypeError::ExpectedType {
            expected: expect_type,
            found: value.clone(),
        }));
    }

    if let Type::Varchar(max) = data_type {
        if let Expression::Value(Value::String(str)) = value {
            if &str.chars().count() > max {
                return Err(AnalyzerError::Overflow(data_type.to_owned(), *max).into());
            }
        }
    }

    Ok(())
}

fn analyze_expression<'exp, 'sch>(
    schema: &'sch Schema,
    col_type: Option<&Type>,
    expr: &'exp Expression,
) -> Result<VmType, SqlError> {
    Ok(match expr {
        Expression::Value(value) => analyze_value(value, col_type)?,
        Expression::Identifier(ident) => {
            let idx = schema
                .index_of(ident)
                .ok_or(SqlError::InvalidColumn(ident.to_string()))?;

            match schema.columns[idx].data_type {
                Type::Boolean => VmType::Bool,
                Type::Varchar(_) => VmType::String,
                Type::Date | Type::DateTime | Type::Time => VmType::Date,
                _ => VmType::Number,
            }
        }
        Expression::BinaryOperation {
            operator,
            left,
            right,
        } => {
            let left_type = analyze_expression(schema, col_type, left)?;
            let right_type = analyze_expression(schema, col_type, right)?;

            let mis_type = || {
                SqlError::Type(TypeError::CannotApplyBinary {
                    left: *left.clone(),
                    right: *right.clone(),
                    operator: *operator,
                })
            };

            if left_type.ne(&right_type) {
                return Err(mis_type());
            }

            match operator {
                BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => VmType::Bool,
                BinaryOperator::And | BinaryOperator::Or if left_type.eq(&VmType::Bool) => {
                    VmType::Bool
                }
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Div
                | BinaryOperator::Mul
                    if left_type.eq(&VmType::Number) =>
                {
                    VmType::Number
                }
                _ => Err(mis_type())?,
            }
        }
        Expression::UnaryOperation { operator, expr } => {
            if let (Some(data_type), UnaryOperator::Minus, Expression::Value(Value::Number(num))) =
                (col_type, operator, &**expr)
            {
                analyze_number(&-num, data_type)?;
                return Ok(VmType::Number);
            }

            match analyze_expression(schema, col_type, expr)? {
                VmType::Number => VmType::Number,
                _ => Err(TypeError::ExpectedType {
                    expected: VmType::Number,
                    found: *expr.clone(),
                })?,
            }
        }
        Expression::Nested(expr) => analyze_expression(schema, col_type, expr)?,
        Expression::Wildcard => {
            return Err(SqlError::Other("Unexpected wildcard expression (*)".into()))
        }
    })
}

fn analyze_value<'exp>(value: &Value, col_type: Option<&Type>) -> Result<VmType, SqlError> {
    let result: Result<VmType, SqlError> = match value {
        Value::Boolean(_) => Ok(VmType::Bool),
        Value::Number(n) => {
            if let Some(col_type) = col_type {
                analyze_number(n, col_type)?;
            }
            Ok(VmType::Number)
        }
        Value::String(s) => match col_type {
            // if the column has a type, delegate to analyze_string,
            // which will return VmType::Date for valid dates,
            // or VmType::String otherwise.
            Some(ty) => analyze_string(s, ty),

            // if we don’t know the target type, we can only
            // treat it as a plain string.
            None => Ok(VmType::String),
        },
        _ => Ok(VmType::Date),
    };

    result
}

fn analyze_where<'exp>(
    schema: &'exp Schema,
    r#where: &'exp Option<Expression>,
) -> Result<(), DatabaseError> {
    let Some(expr) = r#where else { return Ok(()) };

    if let VmType::Bool = analyze_expression(schema, None, expr)? {
        return Ok(());
    }

    Err(TypeError::ExpectedType {
        expected: VmType::Bool,
        found: expr.clone(),
    }
    .into())
}

fn analyze_string<'exp>(s: &str, expected_type: &Type) -> Result<VmType, SqlError> {
    match expected_type {
        Type::Date => {
            NaiveDate::parse_str(s)?;
            Ok(VmType::Date)
        }
        Type::DateTime => {
            NaiveDateTime::parse_str(s)?;
            Ok(VmType::Date)
        }
        Type::Time => {
            NaiveTime::parse_str(s)?;
            Ok(VmType::Date)
        }
        _ => Ok(VmType::String),
    }
}

fn analyze_number(integer: &i128, data_type: &Type) -> Result<(), AnalyzerError> {
    if let Type::Integer | Type::BigInteger | Type::UnsignedInteger | Type::UnsignedBigInteger =
        data_type
    {
        if !data_type.is_integer_in_bounds(integer) {
            // TODO: this is a bit hacky, we should probably have a better way to get the max size of the type
            return Err(AnalyzerError::Overflow(
                data_type.clone(),
                *integer as usize,
            ));
        }
    }

    Ok(())
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalyzerError::MissingCols => write!(f, "all columns must be specified"),
            AnalyzerError::DuplicateCols(col) => write!(f, "column {col} was already declared"),
            AnalyzerError::AlreadyExists(name) => f.write_str(&name.to_string()),
            AnalyzerError::MultiplePrimaryKeys => {
                write!(f, "a table can only have one primary key")
            }
            AnalyzerError::RowIdAssignment => {
                write!(f, "row id column cannot be manually assigned")
            }
            AnalyzerError::MetadataAssignment => {
                write!(
                    f,
                    "table {DB_METADATA} is reserved for internal use, it cannot be manually assigned"
                )
            }
            AnalyzerError::Overflow(data_type, max) => {
                write!(f, "value of type {data_type} is larger than max size {max}")
            }
        }
    }
}
impl Display for AlreadyExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlreadyExists::Table(table_name) => write!(f, "table {table_name} already exists"),
            AlreadyExists::Index(index_name) => write!(f, "index {index_name} already exists"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::date::DateParseError;
    use crate::db::*;
    use crate::sql::parser::*;

    struct Analyze<'sql> {
        sql: &'sql str,
        ctx: &'sql [&'sql str],
        expected: Result<(), DatabaseError>,
    }

    impl<'sql> Analyze<'sql> {
        fn assert(&self) -> Result<(), DatabaseError> {
            let statement = Parser::new(self.sql).parse_statement()?;
            let mut ctx = Context::try_from(self.ctx)?;

            assert_eq!(analyze(&statement, &mut ctx), self.expected);
            Ok(())
        }
    }

    type AnalyzerResult = Result<(), DatabaseError>;

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

    #[test]
    fn select_from_invalid_table() -> AnalyzerResult {
        // we cannot select from a table not created yet
        Analyze {
            sql: "SELECT * FROM users;",
            ctx: &[],
            expected: Err(SqlError::InvalidTable("users".into()).into()),
        }
        .assert()
    }

    #[test]
    fn select_from_invalid_column() -> AnalyzerResult {
        Analyze {
            sql: "SELECT last_name FROM customer;",
            ctx: &["CREATE TABLE customer (id INT PRIMARY KEY, first_name VARCHAR(69));"],
            expected: Err(SqlError::InvalidColumn("last_name".into()).into()),
        }
        .assert()
    }

    #[test]
    fn select_from_valid_table() -> AnalyzerResult {
        Analyze {
            sql: "SELECT * FROM users;",
            ctx: &["CREATE TABLE users (id INT PRIMARY KEY);"],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn select_from_valid_column() -> AnalyzerResult {
        Analyze {
            sql: "SELECT last_name FROM customer;",
            ctx: &["CREATE TABLE customer (id INT PRIMARY KEY, first_name VARCHAR(69), last_name VARCHAR(69));"],
            expected: Ok(()),
        }
            .assert()
    }

    #[test]
    #[ignore]
    // FIXME: make this be parsable
    fn insert_without_columns_targeting() -> AnalyzerResult {
        Analyze {
            sql: "INSERT INTO users VALUES ('string', 5, 6);",
            ctx: &["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255) UNIQUE);"],
            expected: Err(TypeError::ExpectedType {
                expected: VmType::Number,
                found: Expression::Value(Value::String("1".into())),
            }
            .into()),
        }
        .assert()
    }

    #[test]
    fn column_values_mismatch() -> AnalyzerResult {
        Analyze {
            sql: "INSERT INTO employees (id, name, birth_date) VALUES (24, 'Mary Dove');",
            ctx: &[
                "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), birth_date DATE);",
            ],
            expected: Err(AnalyzerError::MissingCols.into()),
        }
        .assert()
    }

    #[test]
    fn column_type_mismatch() -> AnalyzerResult {
        const CTX: &[&str; 2] = &[
            "CREATE TABLE products (id INT, name VARCHAR(255), price INT);",
            "CREATE TABLE users (id INT, username VARCHAR(255));",
        ];

        Analyze {
            sql: "INSERT INTO users (id, username) VALUES (1, 123);",
            expected: Err(TypeError::ExpectedType {
                expected: VmType::String,
                found: Expression::Value(Value::Number(123)),
            }
            .into()),
            ctx: CTX,
        }
        .assert()?;

        Analyze {
            sql: "INSERT INTO products (id, name, price) VALUES (1, 123, 69);",
            expected: Err(TypeError::ExpectedType {
                expected: VmType::String,
                found: Expression::Value(Value::Number(123)),
            }
            .into()),
            ctx: CTX,
        }
        .assert()?;

        Ok(())
    }

    #[test]
    fn insert_date() -> AnalyzerResult {
        Analyze {
            ctx: &["CREATE TABLE employees (name VARCHAR(255), birth_date DATE);"],
            sql: "INSERT INTO employees (name, birth_date) VALUES ('John Doe', '2004-06-27');",
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_date_time() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE events (
                event_id    INTEGER PRIMARY KEY,
                title       VARCHAR(200),
                occurred_at TIMESTAMP
            );
        "#;

        let sql = r#"
            INSERT INTO events (event_id, title, occurred_at)
            VALUES (420, 'Meeting', '2021-01-01 12:00:00'), (69, 'Data backup', '2025-05-07 23:59:59');
        "#;

        let analyze = Analyze {
            sql,
            ctx: &[CTX],
            expected: Ok(()),
        };

        analyze.assert()
    }

    #[test]
    fn insert_invalid_time_format() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE logs (
                log_id INT PRIMARY KEY,
                logged_at TIME
            );
        "#;
        let ctx = &[CTX];

        #[rustfmt::skip]
        let cases = [
            ("INSERT INTO logs (log_id, logged_at) VALUES (1, '25:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidHour)),
            ("INSERT INTO logs (log_id, logged_at) VALUES (1, '12:60:00');",
             TypeError::InvalidDate(DateParseError::InvalidMinute)),
            ("INSERT INTO logs (log_id, logged_at) VALUES (1, '12:00:60');",
             TypeError::InvalidDate(DateParseError::InvalidSecond)),
        ];

        for (sql, err) in cases {
            let analyze = Analyze {
                sql,
                ctx,
                expected: Err(SqlError::Type(err).into()),
            };

            analyze.assert()?;
        }

        Ok(())
    }

    #[test]
    fn insert_invalid_datetime_format() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE events (
                id INT,
                happened_at TIMESTAMP
            );
        "#;
        let ctx = &[CTX];

        #[rustfmt::skip]
        let cases = [
            ("INSERT INTO events (id, happened_at) VALUES (1, '2020-13-01 12:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidMonth)),
            ("INSERT INTO events (id, happened_at) VALUES (2, '2020-00-01 12:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidMonth)),
            ("INSERT INTO events (id, happened_at) VALUES (3, '2020-12-32 12:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidDay)),
            ("INSERT INTO events (id, happened_at) VALUES (4, '2023-04-31 10:00:00');",
            TypeError::InvalidDate(DateParseError::InvalidMonthDay)),
            ("INSERT INTO events (id, happened_at) VALUES (5, 'not-a-date');",
            TypeError::InvalidDate(DateParseError::InvalidDateTime)),
        ];

        for (sql, err) in cases {
            let analyze = Analyze {
                sql,
                ctx,
                expected: Err(SqlError::Type(err).into()),
            };

            analyze.assert()?;
        }

        Ok(())
    }

    #[test]
    fn multiple_primary_keys() -> AnalyzerResult {
        let analyze = Analyze {
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) PRIMARY KEY);",
            ctx: &[],
            expected: Err(AnalyzerError::MultiplePrimaryKeys.into()),
        };

        analyze.assert()
    }

    #[test]
    fn non_boolean_where() -> AnalyzerResult {
        const CTX: &str = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));";
        let found = Expression::BinaryOperation {
            operator: BinaryOperator::Plus,
            left: Box::new(Expression::Value(Value::Number(1))),
            right: Box::new(Expression::Value(Value::Number(1))),
        };

        Analyze {
            ctx: &[CTX],
            sql: "SELECT * FROM users WHERE 1 + 1;",
            expected: Err(TypeError::ExpectedType {
                expected: VmType::Bool,
                found,
            }
            .into()),
        }
        .assert()
    }
}
