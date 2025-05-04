use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::core::db::{
    Ctx, DatabaseError, Schema, SqlError, TableMetadata, DB_METADATA, ROW_COL_ID,
};
use crate::sql::statement::{
    BinaryOperator, Constraint, Create, Expression, Statement, Type, UnaryOperator, Value,
};
use crate::vm::{TypeError, VmType};
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
type AnalyzerResult<'exp, T> = Result<T, DatabaseError<'exp>>;

pub(in crate::sql) fn analyze<'s>(
    statement: &'s Statement,
    ctx: &'s mut impl Ctx<'s>,
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
            values,
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

            if columns.len() != values.len() {
                return Err(AnalyzerError::MissingCols.into());
            }

            let mut duplicates = HashSet::new();
            for col in columns {
                if metadata.schema.index_of(col).is_none() {
                    return Err(SqlError::InvalidColumn(col.into()).into());
                }

                if !duplicates.insert(col) {
                    return Err(AnalyzerError::DuplicateCols(col.into()).into());
                }

                if col.eq(ROW_COL_ID) {
                    return Err(AnalyzerError::MetadataAssignment.into());
                }
            }

            let schema_len = match metadata.schema.columns[0].name.eq(ROW_COL_ID) {
                true => &metadata.schema.columns.len() - 1,
                false => metadata.schema.columns.len(),
            };

            if schema_len != columns.len() {
                return Err(AnalyzerError::MissingCols.into());
            }

            for (expr, col) in values.iter().zip(columns) {
                analyze_assignment(metadata, col, expr, false)?;
            }
        }
        _ => {}
    }

    todo!()
}

fn analyze_assignment<'exp>(
    table: &TableMetadata,
    column: &str,
    value: &'exp Expression,
    allow_id: bool,
) -> Result<(), SqlError<'exp>> {
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
            found: &value,
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

fn analyze_expression<'exp>(
    schema: &Schema,
    col_type: Option<&Type>,
    expr: &'exp Expression,
) -> Result<VmType, SqlError<'exp>> {
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
        Expression::BinaryOperator {
            operator,
            left,
            right,
        } => {
            let left_type = analyze_expression(schema, col_type, left)?;
            let right_type = analyze_expression(schema, col_type, right)?;

            let mis_type = || {
                SqlError::Type(TypeError::CannotApplyBinary {
                    left,
                    right,
                    operator,
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
        Expression::UnaryOperator { operator, expr } => {
            if let (Some(data_type), UnaryOperator::Minus, Expression::Value(Value::Number(num))) =
                (col_type, operator, &**expr)
            {
                analyze_number(&-num, Some(data_type))?;
                return Ok(VmType::Number);
            }

            match analyze_expression(schema, col_type, expr)? {
                VmType::Number => VmType::Number,
                _ => Err(TypeError::ExpectedType {
                    expected: VmType::Number,
                    found: expr,
                })?,
            }
        }
        Expression::Nested(expr) => analyze_expression(schema, col_type, expr)?,
        Expression::Wildcard => {
            return Err(SqlError::Other("Unexpected wildcard expression (*)".into()))
        }
    })
}

fn analyze_value<'exp>(value: &Value, col_type: Option<&Type>) -> Result<VmType, SqlError<'exp>> {
    match value {
        Value::Boolean(_) => Ok(VmType::Bool),
        Value::Number(n) => {
            analyze_number(n, col_type)?;
            Ok(VmType::Number)
        }
        Value::String(s) => analyze_string(s, col_type),
        _ => Ok(VmType::Date),
    }
}

fn analyze_string<'exp>(s: &str, expected_type: Option<&Type>) -> Result<VmType, SqlError<'exp>> {
    match expected_type {
        Some(Type::Date) => {
            NaiveDate::parse_str(s)?;
            Ok(VmType::Date)
        }
        Some(Type::DateTime) => {
            NaiveDateTime::parse_str(s)?;
            Ok(VmType::Date)
        }
        Some(Type::Time) => {
            NaiveTime::parse_str(s)?;
            Ok(VmType::Date)
        }
        _ => Ok(VmType::String),
    }
}

fn analyze_number(integer: &i128, data_type: Option<&Type>) -> Result<VmType, AnalyzerError> {
    todo!()
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
