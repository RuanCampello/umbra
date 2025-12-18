//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST.

#![allow(unused)]

use super::Keyword;
use crate::core::date::interval::Interval;
use crate::core::date::{DateParseError, NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::core::numeric::Numeric;
use crate::core::uuid::Uuid;
use crate::db::{IndexMetadata, TableMetadata};
use crate::index;
use crate::vm::expression::{TypeError, VmType};
use std::borrow::Borrow;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter, Write};
use std::hash::Hash;
use std::ops::{Add, Neg};
use std::str::FromStr;

/// SQL statements.
#[derive(Debug, PartialEq)]
pub(crate) enum Statement {
    Create(Create),
    Select(Select),
    Update(Update),
    Insert(Insert),
    Delete(Delete),
    Drop(Drop),
    Source(String),
    Commit,
    StartTransaction,
    Rollback,
    Explain(Box<Self>),
}

/// The `UPDATE` assignment instruction.
#[derive(Debug, PartialEq)]
pub(crate) struct Assignment {
    pub identifier: String,
    pub value: Expression,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    pub name: String,
    pub data_type: Type,
    pub constraints: Vec<Constraint>,

    pub type_def: Option<Vec<String>>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Create {
    Database(String),
    Sequence {
        name: String,
        r#type: Type,
        table: String,
    },
    Table {
        name: String,
        columns: Vec<Column>,
    },
    Index {
        name: String,
        table: String,
        column: String,
        unique: bool,
    },
}

#[derive(Debug, PartialEq)]
pub(crate) struct Select {
    pub columns: Vec<Expression>,
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub r#where: Option<Expression>,
    pub order_by: Vec<OrderBy>,
    pub group_by: Vec<Expression>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Default, PartialEq)]
pub struct SelectBuilder {
    columns: Vec<Expression>,
    from: TableRef,
    joins: Vec<JoinClause>,
    r#where: Option<Expression>,
    order_by: Vec<OrderBy>,
    group_by: Vec<Expression>,
    limit: Option<usize>,
    offset: Option<usize>,
}

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Update {
    pub table: String,
    pub columns: Vec<Assignment>,
    pub r#where: Option<Expression>,
    pub returning: Vec<Expression>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Insert {
    pub into: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<Expression>>,
    pub returning: Vec<Expression>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct Delete {
    pub from: String,
    pub r#where: Option<Expression>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Drop {
    Table(String),
    Database(String),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub enum Expression {
    Identifier(String),
    QualifiedIdentifier {
        table: String,
        column: String,
    },
    Value(Value),
    Wildcard,
    UnaryOperation {
        operator: UnaryOperator,
        expr: Box<Self>,
    },
    BinaryOperation {
        operator: BinaryOperator,
        left: Box<Self>,
        right: Box<Self>,
    },
    Function {
        func: Function,
        args: Vec<Self>,
    },
    Alias {
        expr: Box<Self>,
        alias: String,
    },
    IsNull {
        expr: Box<Self>,
        negated: bool,
    },
    Nested(Box<Self>),
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub struct OrderBy {
    pub expr: Expression,
    pub direction: OrderDirection,
}

#[derive(Debug, Default, PartialEq, PartialOrd, Clone, Copy)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, PartialEq)]
pub struct JoinClause {
    pub table: TableRef,
    pub on: Expression,
    pub join_type: JoinType,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Full,
}

/// Date/Time related types.
///
/// This enum wraps actual values of date/time types, such as a specific calendar date or a time of day.
/// It distinguishes between `DATE`, `TIME`, and `TIMESTAMP` at the value level.
///
/// Values of this type are stored in the `Value::Temporal` variant.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Copy)]
pub enum Temporal {
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Time(NaiveTime),
}

/// A runtime value stored in a table row or returned in a query.
///
/// This enum represents a *concrete value* associated with a column,
/// typically used in query results, expression evaluation, and row serialization.
///
/// For example:
/// - A row with a `VARCHAR` column might store `Value::String("hello".to_string())`.
/// - A `DATE` column would store `Value::Temporal(Temporal::Date(...))`.
///
/// `Value` is the counterpart to `Type`:  
/// - `Type` defines what *kind* of value is allowed.  
/// - `Value` holds the *actual* data.
///
/// This separation allows the system to validate values against schema definitions at runtime.
///
/// ## Equality semantics
///
/// `Value` implements [`PartialEq`] for deep equality:
/// - Two values are equal only if they are the same variant and hold exactly equal contents.
///   For example, `Value::Number(5) == Value::Number(5)` is true,  
///   but `Value::Number(5) == Value::Float(5.0)` is also true, because they can be coerced
///   losslessly from integer to float.
///
/// This enable coercing comparisons (e.g., treating `Number` and `Float` as “same kind”)
/// during query planning.
///
/// ```rust
/// use umbra::sql::statement::Value;
///
/// assert_eq!(Value::Number(5), Value::Number(5));
/// assert_ne!(Value::Number(5), Value::Float(5.1));
/// assert!(Value::Number(5).eq(&Value::Float(5.0))); // both numeric kinds and lossless coercible
/// ```
#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Number(i128),
    Float(f64),
    Boolean(bool),
    Temporal(Temporal),
    Uuid(Uuid),
    Interval(Interval),
    Numeric(Numeric),
    Enum(u8),
    Blob(Vec<u8>),
    Null,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Constraint {
    PrimaryKey,
    Unique,
    Nullable,
    Default(Expression),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum BinaryOperator {
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Mul,
    Div,
    And,
    Or,
    Like,
}

/// SQL data types.
///
/// This enum describes the logical *type* of a column in a table schema.
/// It is used during planning, validation, and schema definition.
///
/// For example:
/// - A column defined as `VARCHAR(255)` will be represented as `Type::Varchar(255)`.
/// - A column of `DATE` will be represented as `Type::Date`.
#[derive(Debug, PartialEq, Eq, Clone, Copy, Hash)]
pub enum Type {
    /// 2-byte signed integer
    SmallInt,
    /// 2-byte unsigned integer
    UnsignedSmallInt,
    /// 4-byte signed integer
    Integer,
    /// 4-byte unsigned integer
    UnsignedInteger,
    /// 8-byte signed integer
    BigInteger,
    /// 8-byte unsigned integer
    UnsignedBigInteger,
    /// Auto-incrementing 2-byte signed integer (serial) backed by a sequence.
    /// Behaves like PostgreSQL `SERIAL`: uses `next_serial_id`, which is atomic and
    /// **not** rolled back on transaction abort, so gaps can occur.
    /// ([ftp.postgresql.kr](https://ftp.postgresql.kr/docs/9.2/functions-sequence.html))
    SmallSerial,
    /// Auto-incrementing 4-byte signed integer (serial) backed by a sequence.
    /// Behaves like PostgreSQL `SERIAL`: uses `next_serial_id`, which is atomic and
    /// **not** rolled back on transaction abort, so gaps can occur.
    /// ([ftp.postgresql.kr](https://ftp.postgresql.kr/docs/9.2/functions-sequence.html))
    Serial,
    /// Auto-incrementing 8-byte signed integer (bigserial) backed by a sequence.
    /// Behaves like PostgreSQL `BIGSERIAL`: uses `next_serial_id`, which is atomic and
    /// **not** rolled back on transaction abort, so gaps can occur.
    /// ([ftp.postgresql.kr](https://ftp.postgresql.kr/docs/9.2/functions-sequence.html))
    BigSerial,
    /// Boolean type (true/false)
    Boolean,
    /// Variable length character type with a limit
    Varchar(usize),
    /// Variable unlimited length
    Text,
    /// 4-byte variable-precision floating point type.
    Real,
    /// 8-byte variable-precision floating point type.
    DoublePrecision,
    /// 8-byte Universal Unique Identifier defined by [RFC 4122](https://datatracker.ietf.org/doc/html/rfc4122).
    Uuid,
    /// Arbitrary-precision numeric type (like PostgreSQL `NUMERIC`/`DECIMAL`).
    /// Uses optimised bit-packing for small values and Base-10000 for arbitrary precision.
    Numeric(usize, usize),
    Date,
    Time,
    DateTime,
    Interval,
    Enum(u32),
}

/// Subset of `SQL` functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub enum Function {
    /// Extracts the `string` to the `length` at the `start`th character (if specified) and stop
    /// after the `count` character. Must provide at least of of `start` and `count`.
    /// ```sql
    /// SUBSTRING(string text [FROM start text][FOR count int]) -> text;
    /// ```
    Substring,
    /// Concatenates the string representation of all arguments.
    Concat,
    /// Returns the numeric representation of argument's first character.
    Ascii,
    /// Returns the first index of the specified `substring` within the given `string`. Returns
    /// zero if it's not present.
    /// ```sql
    /// POSITION(substring text IN string text) -> int;
    /// ```
    Position,
    Abs,
    Sign,
    Sqrt,
    Power,
    Trunc,
    /// Computes the number of input rows.
    Count,
    /// Computes the average (arithmetic mean) of all input values.
    Avg,
    /// Computes the sum of the input values.
    Sum,
    /// Computes the minimum of the input values.
    Min,
    /// Computes the maximum of the input values.
    Max,
    /// Returns the data type of any value.
    TypeOf,
    /// Returns the first non-null argument.
    /// ```sql
    /// COALESCE(value1, value2, ..., valueN) -> T;
    /// ```
    Coalesce,
    Extract,
    UuidV4,
}

#[derive(Debug)]
pub(crate) enum ArithmeticPair<'p> {
    Numeric(f64, f64),
    Temporal(Temporal, Interval),
    Arbitrary(Cow<'p, Numeric>, Cow<'p, Numeric>),
}

const NULL_HASH: u32 = 0x4E554C4C;
pub const NUMERIC_ANY: usize = usize::MAX;

impl Column {
    pub fn new(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![],
            type_def: None,
        }
    }

    pub fn primary_key(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![Constraint::PrimaryKey],
            type_def: None,
        }
    }

    pub fn unique(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![Constraint::Unique],
            type_def: None,
        }
    }

    pub fn nullable(name: &str, data_type: Type) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![Constraint::Nullable],
            type_def: None,
        }
    }

    pub fn is_nullable(&self) -> bool {
        self.constraints.contains(&Constraint::Nullable)
    }

    pub fn with_enum(name: &str, data_type: Type, variants: Vec<String>) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            constraints: vec![],
            type_def: Some(variants),
        }
    }
}

impl Type {
    pub fn is_integer_in_bounds(&self, int: &i128) -> bool {
        let bound = match self {
            Self::SmallInt => i16::MIN as i128..=i16::MAX as i128,
            Self::UnsignedSmallInt => u16::MIN as i128..=u16::MAX as i128,
            Self::SmallSerial => 1..=(i16::MAX as i128),
            Self::Integer => i32::MIN as i128..=i32::MAX as i128,
            Self::UnsignedInteger => 0..=u32::MAX as i128,
            Self::Serial => 1..=(i32::MAX as i128),
            Self::BigInteger => i64::MIN as i128..=i64::MAX as i128,
            Self::UnsignedBigInteger => 0..=u64::MAX as i128,
            Self::BigSerial => 1..=(i64::MAX as i128),
            other => panic!("bound checking must be used only for integer: {other:#?}"),
        };

        bound.contains(int)
    }

    pub const fn is_float_in_bounds(&self, float: &f64) -> bool {
        match self {
            Self::Real => *float >= f32::MIN as f64 && *float <= f32::MAX as f64,
            Self::DoublePrecision => float.is_finite(),
            other => panic!("bound checking must be used only for floats"),
        }
    }

    pub const fn is_integer(&self) -> bool {
        match self {
            Self::SmallInt
            | Self::UnsignedSmallInt
            | Self::Integer
            | Self::UnsignedInteger
            | Self::BigInteger
            | Self::UnsignedBigInteger => true,
            _ => self.is_serial(),
        }
    }

    pub const fn is_serial(&self) -> bool {
        match self {
            Self::SmallSerial | Self::Serial | Self::BigSerial => true,
            _ => false,
        }
    }

    pub const fn is_float(&self) -> bool {
        match self {
            Self::Real | Self::DoublePrecision => true,
            _ => false,
        }
    }

    /// Returns true if is any numeric type.
    pub const fn is_number(&self) -> bool {
        matches!(self, Self::Uuid) || self.is_serial() || self.is_integer()
    }

    // Returns true if this `Type` can be auto-generated
    pub const fn can_be_autogen(&self) -> bool {
        matches!(self, Self::Uuid) || self.is_serial()
    }

    pub const fn max(&self) -> usize {
        match self {
            Self::SmallSerial => 32767usize,
            Self::Serial => 2147483647usize,
            Self::BigSerial => 9223372036854775807usize,
            _ => panic!("MAX function is meant to be used only for serial types"),
        }
    }
}

impl Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statement::Create(create) => match create {
                Create::Sequence {
                    name,
                    r#type,
                    table,
                } => write!(f, "CREATE SEQUENCE {name} AS {type} OWNED BY {table}")?,
                Create::Table { name, columns } => {
                    write!(f, "CREATE TABLE {name} ({})", join(columns, ", "))?;
                }

                Create::Database(name) => {
                    write!(f, "CREATE DATABASE {name}")?;
                }

                Create::Index {
                    name,
                    table,
                    column,
                    unique,
                } => {
                    let unique = if *unique { "UNIQUE" } else { "" };
                    write!(f, "CREATE {unique} INDEX {name} ON {table}({column})")?;
                }
            },

            Statement::Select(Select {
                columns,
                from,
                r#where,
                joins,
                order_by,
                group_by,
                limit,
                offset,
            }) => {
                write!(f, "SELECT {} FROM {}", join(columns, ", "), from.name)?;
                if let Some(alias) = &from.alias {
                    write!(f, " AS {alias}")?;
                }

                for join in joins {
                    write!(f, " JOIN {}", join.table.name)?;

                    if let Some(alias) = &join.table.alias {
                        write!(f, " AS {alias}")?;
                    }

                    write!(f, " ON {}", join.on)?;
                }

                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }

                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", join(order_by, ", "))?;
                }

                if !group_by.is_empty() {
                    write!(f, " GROUP BY {}", join(group_by, ", "))?;
                }

                if let Some(limit) = limit {
                    write!(f, " LIMIT {limit}")?;
                }

                if let Some(offset) = offset {
                    write!(f, " OFFSET {offset}")?;
                }
            }

            Statement::Delete(Delete { from, r#where }) => {
                write!(f, "DELETE FROM {from}")?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
            }

            Statement::Update(Update {
                table,
                columns,
                r#where,
                returning,
            }) => {
                write!(f, "UPDATE {table} SET {}", join(columns, ", "))?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }

                if !returning.is_empty() {
                    write!(f, "RETURNING {}", join(returning, ", "))?;
                }
            }

            Statement::Insert(Insert {
                into,
                columns,
                values,
                returning,
            }) => {
                let columns = match columns.is_empty() {
                    true => String::from(" "),
                    false => format!(" ({}) ", join(columns, ", ")),
                };

                let values = join(
                    &values
                        .iter()
                        .map(|row| format!("({})", join(row, ", ")))
                        .collect::<Vec<_>>(),
                    ", ",
                );

                write!(f, "INSERT INTO {into}{columns}VALUES ({})", values)?;
                if !returning.is_empty() {
                    write!(f, "RETURNING {}", join(returning, ", "))?;
                }
            }

            Statement::Drop(drop) => {
                match drop {
                    Drop::Table(name) => write!(f, "DROP TABLE {name}")?,
                    Drop::Database(name) => write!(f, "DROP DATABASE {name}")?,
                };
            }

            Statement::StartTransaction => f.write_str("BEGIN TRANSACTION")?,
            Statement::Commit => f.write_str("COMMIT")?,
            Statement::Rollback => f.write_str("ROLLBACK")?,
            Statement::Source(source) => write!(f, "SOURCE '{source}'")?,
            Statement::Explain(statement) => write!(f, "EXPLAIN {statement}")?,
        };

        f.write_char(';')
    }
}

impl Expression {
    pub(in crate::sql) fn unwrap_alias(&self) -> &Self {
        match self {
            Expression::Alias { ref expr, .. } => &**expr,
            _ => self,
        }
    }

    pub(in crate::sql) fn unwrap_name(&self) -> Cow<'_, str> {
        match self {
            Expression::Alias { alias, .. } => Cow::Borrowed(&alias),
            Expression::Nested(expr) => expr.unwrap_name(),
            expr => Cow::Owned(expr.to_string()),
        }
    }

    pub(in crate::sql) fn is_aggr_fn(&self) -> bool {
        matches!(self, Expression::Function { func, .. } if func.is_aggr())
    }

    pub(in crate::sql) fn as_identifier(&self) -> Option<&str> {
        if let Self::Identifier(alias) = self {
            return Some(alias);
        };

        None
    }
}

impl Default for Expression {
    fn default() -> Self {
        Expression::Wildcard
    }
}

impl JoinClause {
    pub(in crate::sql) fn index_cadidate(
        &self,
        right_table: &TableMetadata,
    ) -> Option<(IndexMetadata, Expression)> {
        match &self.on {
            Expression::BinaryOperation {
                left: l,
                operator: BinaryOperator::Eq,
                right: r,
            } => {
                let (right_key_expr, left_key_expr) = match (
                    Self::is_col_of_table(l, &self.table.key()),
                    Self::is_col_of_table(r, &self.table.key()),
                ) {
                    (true, _) => (Some(l.as_ref()), r.as_ref()),
                    (_, true) => (Some(r.as_ref()), l.as_ref()),
                    _ => (None, l.as_ref()), // left_key_expr value here doesn't matter since right_key_expr is None
                };

                match right_key_expr {
                    Some(right_key) => match right_key {
                        Expression::Identifier(col_name)
                        | Expression::QualifiedIdentifier {
                            column: col_name, ..
                        } => {
                            // find if there is an index on this column in the right table
                            if let Some(idx) = right_table
                                .indexes
                                .iter()
                                .find(|idx| idx.column.name == *col_name)
                            {
                                return Some((idx.clone(), left_key_expr.clone()));
                            }

                            // check if the column is the primary key of the right table
                            // if so, we can treat the table itself as an index
                            if right_table.schema.columns[0].name == *col_name {
                                let pk_index = IndexMetadata {
                                    name: index!(primary on (right_table.name)),
                                    root: right_table.root,
                                    column: right_table.schema.columns[0].clone(),
                                    schema: right_table.schema.clone(),
                                    unique: true,
                                };
                                return Some((pk_index, left_key_expr.clone()));
                            }

                            None
                        }
                        _ => None,
                    },
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn is_col_of_table(expr: &Expression, table: &str) -> bool {
        match expr {
            Expression::QualifiedIdentifier { table: t, .. } => t == table,
            // TODO: maybe we need to verify better this
            Expression::Identifier(_) => true,
            _ => false,
        }
    }
}

impl Value {
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Tries to convert a pair of `Value`s into a representation suitable for arithmetic.
    pub(crate) fn as_arithmetic_pair<'p>(&'p self, other: &'p Self) -> Option<ArithmeticPair<'p>> {
        use crate::core::numeric::Numeric as Num;
        use std::borrow::Cow::*;
        use ArithmeticPair as Pair;
        use Value::*;

        match (self, other) {
            (Float(a), Float(b)) => Some(Pair::Numeric(*a, *b)),
            (Number(a), Float(b)) => Some(Pair::Numeric(*a as f64, *b)),
            (Float(a), Number(b)) => Some(Pair::Numeric(*a, *b as f64)),
            (Number(a), Number(b)) => Some(Pair::Numeric(*a as f64, *b as f64)),

            (Temporal(t), Interval(i)) => Some(Pair::Temporal(*t, *i)),
            (Interval(i), Temporal(t)) => Some(Pair::Temporal(*t, *i)),

            (Numeric(a), Numeric(b)) => Some(Pair::Arbitrary(Borrowed(a), Borrowed(b))),
            (Numeric(a), Number(b)) => Some(Pair::Arbitrary(Borrowed(a), Owned(Num::from(*b)))),
            (Number(a), Numeric(b)) => Some(Pair::Arbitrary(Owned(Num::from(*a)), Borrowed(b))),

            (Numeric(a), Float(b)) => Num::try_from(*b)
                .ok()
                .map(|nb| Pair::Arbitrary(Borrowed(a), Owned(nb))),
            (Float(a), Numeric(b)) => Num::try_from(*a)
                .ok()
                .map(|na| Pair::Arbitrary(Owned(na), Borrowed(b))),

            _ => None,
        }
    }
}

impl Temporal {
    pub(crate) fn try_coerce(self, other: Self) -> (Self, Self) {
        match (self, other) {
            (Self::Time(_), Self::DateTime(timestamp)) => (self, Self::Time(timestamp.into())),
            (Self::DateTime(timestamp), Self::Time(_)) => (Self::Time(timestamp.into()), other),
            (Self::Date(_), Self::DateTime(timestamp)) => (self, Self::Date(timestamp.into())),
            (Self::DateTime(timestamp), Self::Date(_)) => (Self::Date(timestamp.into()), other),
            _ => (self, other),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use self::Value::{Numeric as Arbitrary, *};
        use crate::core::numeric::Numeric;

        match (self, other) {
            (Number(a), Number(b)) => a == b,
            (Float(a), Float(b)) => a == b,
            // we do this because we can coerce them later to do a comparison between floats and
            // integers
            (Number(a), Float(b)) => (*a as f64) == *b,
            (Float(a), Number(b)) => *a == (*b as f64),
            (Arbitrary(a), Arbitrary(b)) => a == b,
            (Arbitrary(a), Number(n)) | (Number(n), Arbitrary(a)) => a == &Numeric::from(*n),
            (Arbitrary(a), Float(f)) | (Float(f), Arbitrary(a)) => {
                Numeric::try_from(*f).map(|f| &f == a).unwrap_or(false)
            }
            (String(a), String(b)) => a == b,
            (Boolean(a), Boolean(b)) => a == b,
            (Temporal(a), Temporal(b)) => a == b,
            (Interval(a), Interval(b)) => a == b,
            (Uuid(a), Uuid(b)) => a == b,
            // For grouping and hashing, NULL values should be equal.
            // SQL semantics (NULL = NULL returns NULL) is handled separetly.
            (Null, Null) => true,
            (Null, _) | (_, Null) => false,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Number(a), Value::Number(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Temporal(a), Value::Temporal(b)) => a.cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
            (Value::Interval(a), Value::Interval(b)) => a.cmp(b),
            (Value::Numeric(a), Value::Numeric(b)) => a.cmp(b),
            (Value::Enum(a), Value::Enum(b)) => a.cmp(b),
            (Value::Blob(a), Value::Blob(b)) => a.cmp(b),
            (Value::Blob(_), _) => std::cmp::Ordering::Greater,
            (_, Value::Blob(_)) => std::cmp::Ordering::Less,
            // For sorting, NULL values are considered equal to each other
            // and sort after all non-NULL values.
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Greater,
            (_, Value::Null) => Ordering::Less,
            _ => panic!("these values are not comparable"),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        fn encode_float(f: &f64) -> u64 {
            // normalize -0.0 and 0.0 to the same bits, and treat all nans the same.
            let mut bits = f.to_bits();
            if bits == (-0.0f64).to_bits() {
                bits = 0.0f64.to_bits();
            }
            if f.is_nan() {
                // all nans are hashed as the same.
                bits = 0x7ff8000000000000u64;
            } else if bits >> 63 != 0 {
                // for negatives, flip all bits for total ordering.
                bits = !bits;
            }

            bits
        }

        match self {
            Self::Float(f) => encode_float(f).hash(state),
            Value::Number(n) => n.hash(state),
            Value::String(s) => s.hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::Temporal(t) => t.hash(state),
            Value::Uuid(u) => u.hash(state),
            Value::Interval(i) => i.hash(state),
            Value::Enum(e) => e.hash(state),
            Value::Null => NULL_HASH.hash(state),
            Value::Blob(blob) => blob.hash(state),
            Value::Numeric(n) => todo!(),
        }
    }
}

impl Neg for Value {
    type Output = Result<Self, TypeError>;
    fn neg(self) -> Self::Output {
        match self {
            Value::Number(num) => Ok(Value::Number(-num)),
            Value::Float(float) => Ok(Value::Float(-float)),
            Value::Numeric(num) => Ok(Value::Numeric(-num)),
            Value::Null => Ok(Value::Null),
            v => Err(TypeError::CannotApplyUnary {
                operator: UnaryOperator::Minus,
                value: v,
            }),
        }
    }
}

impl Neg for Interval {
    type Output = Self;

    fn neg(self) -> Self::Output {
        Self {
            months: -self.months,
            days: -self.days,
            microseconds: -self.microseconds,
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self.type_def {
            Some(variants) => {
                write!(f, "{} ", self.name)?;
                let def = variants
                    .iter()
                    .map(|v| format!("\"{v}\""))
                    .collect::<Vec<_>>()
                    .join(" | ");

                write!(f, "{def}")?;
            }
            None => write!(f, "{} {}", self.name, self.data_type)?,
        };

        for constraint in &self.constraints {
            f.write_char(' ')?;
            f.write_str(match constraint {
                Constraint::PrimaryKey => "PRIMARY KEY",
                Constraint::Unique => "UNIQUE",
                Constraint::Nullable => "NULLABLE",
                Constraint::Default(value) => "DEFAULT {value}",
            })?;
        }

        Ok(())
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(ident) => f.write_str(ident),
            Self::QualifiedIdentifier { table, column } => write!(f, "{table}.{column}"),
            Self::Value(value) => write!(f, "{value}"),
            Self::Wildcard => f.write_char('*'),
            Self::BinaryOperation {
                operator,
                left,
                right,
            } => {
                write!(f, "{left} {operator} {right}")
            }
            Self::UnaryOperation { operator, expr } => {
                write!(f, "{operator}{expr}")
            }
            Self::Function { func, args } => write!(f, "{func}"),
            Self::Alias { expr, alias } => write!(f, "{expr} AS {alias}"),
            Self::IsNull { expr, negated } => {
                write!(f, "{expr} IS ");

                let is = if *negated { "NOT NULL" } else { "NULL" };
                f.write_str(is)
            }
            Self::Nested(expr) => write!(f, "({expr})"),
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plus => "+",
            Self::Minus => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Eq => "=",
            Self::Neq => "!=",
            Self::Gt => ">",
            Self::GtEq => ">=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Like => "LIKE",
        })
    }
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char(match self {
            Self::Minus => '-',
            Self::Plus => '+',
        })
    }
}

impl Display for Assignment {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.identifier, self.value)
    }
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Boolean => f.write_str("BOOLEAN"),
            Type::SmallInt => f.write_str("SMALLINT"),
            Type::UnsignedSmallInt => f.write_str("SMALLINT UNSIGNED"),
            Type::Integer => f.write_str("INT"),
            Type::UnsignedInteger => f.write_str("INT UNSIGNED"),
            Type::BigInteger => f.write_str("BIGINT"),
            Type::UnsignedBigInteger => f.write_str("BIGINT UNSIGNED"),
            Type::SmallSerial => f.write_str("SMALLSERIAL"),
            Type::Serial => f.write_str("SERIAL"),
            Type::BigSerial => f.write_str("BIGSERIAL"),
            Type::Real => f.write_str("REAL"),
            Type::DoublePrecision => f.write_str("DOUBLE PRECISION"),
            Type::Uuid => f.write_str("UUID"),
            Type::DateTime => f.write_str("TIMESTAMP"),
            Type::Time => f.write_str("TIME"),
            Type::Date => f.write_str("DATE"),
            Type::Varchar(max) => write!(f, "VARCHAR({max})"),
            Type::Text => write!(f, "TEXT"),
            Type::Enum(_) => write!(f, "ENUM"),
            Type::Interval => f.write_str("INTERVAL"),
            Type::Numeric(precision, scale) => match (precision, scale) {
                (&NUMERIC_ANY, _) => write!(f, "NUMERIC"),
                (precision, 0) => write!(f, "NUMERIC({precision})"),
                (precision, scale) => write!(f, "NUMERIC({precision}, {scale})"),
            },
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(string) => write!(f, "\"{string}\""),
            Value::Number(number) => write!(f, "{number}"),
            Value::Float(float) => write!(f, "{float}"),
            Value::Boolean(bool) => f.write_str(if *bool { "TRUE" } else { "FALSE" }),
            Value::Temporal(temporal) => write!(f, "{temporal}"),
            Value::Uuid(uuid) => write!(f, "{uuid}"),
            Value::Interval(interval) => write!(f, "{interval}"),
            Value::Numeric(numeric) => write!(f, "{numeric}"),
            Value::Enum(index) => write!(f, "{index}"),
            Value::Blob(blob) => write!(f, "{blob:?}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl Function {
    /// Returns respectively the minimum and the maximum (if there's any) number of function's arguments.
    pub const fn size_of_args(&self) -> Option<(usize, usize)> {
        const UNARY: Option<(usize, usize)> = Some((1, 1));
        match self {
            Self::Substring => Some((2, 3)),
            Self::Concat => Some((1, usize::MAX)),
            Self::Position => Some((2, 2)),
            Self::Power => Some((2, 2)),
            Self::Extract => Some((2, 2)),
            Self::Trunc => Some((1, 2)),
            Self::Ascii => UNARY,
            func if func.is_math() => UNARY,
            func if func.is_unary() => UNARY,
            _ => None,
        }
    }

    /// Returns the `VmType` that this function returns.
    pub const fn return_type(&self, input: &VmType) -> VmType {
        match self {
            Self::Substring | Self::Concat | Self::TypeOf => VmType::String,

            Self::Count | Self::UuidV4 | Self::Ascii | Self::Position | Self::Extract => {
                VmType::Number
            }

            Self::Sqrt | Self::Power => match input {
                VmType::Numeric => VmType::Numeric,
                _ => VmType::Float,
            },

            Self::Avg | Self::Sum => match input {
                VmType::Number | VmType::Numeric => VmType::Numeric,
                _ => *input,
            },

            Self::Abs | Self::Sign | Self::Trunc | Self::Min | Self::Max | Self::Coalesce => *input,
        }
    }

    pub const fn is_math(&self) -> bool {
        return matches!(
            self,
            Self::Trunc | Self::Abs | Self::Sqrt | Self::Sign | Self::Power
        );
    }

    pub const fn is_aggr(&self) -> bool {
        matches!(
            self,
            Self::Count | Self::Sum | Self::Avg | Self::Min | Self::Max
        )
    }

    pub(in crate::sql) const fn is_unary(&self) -> bool {
        matches!(self, Self::Ascii | Self::TypeOf) || self.is_aggr()
    }
}

impl Display for Function {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.borrow())
    }
}

macro_rules! define_function_mapping {
    ($($variant:ident => $str:expr),* $(,)?) => {
        impl std::borrow::Borrow<str> for Function {
            fn borrow(&self) -> &str {
                match self {
                    $(Function::$variant => $str,)*
                }
            }
        }

        impl std::str::FromStr for Function {
            type Err = ();

            fn from_str(s: &str) -> Result<Self, Self::Err> {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    $($str => Ok(Function::$variant),)*
                    _ => Err(()),
                }
            }
        }
    };
}

define_function_mapping! {
    Substring => "SUBSTRING",
    Concat => "CONCAT",
    Ascii => "ASCII",
    Position => "POSITION",
    Power => "POWER",
    Abs => "ABS",
    Trunc => "TRUNC",
    Sign => "SIGN",
    Sqrt => "SQRT",
    Min => "MIN",
    Sum => "SUM",
    Avg => "AVG",
    Count => "COUNT",
    TypeOf => "TYPEOF",
    Max => "MAX",
    Coalesce => "COALESCE",
    Extract => "EXTRACT",
    UuidV4 => "UUIDV4",
}

impl SelectBuilder {
    pub fn from(mut self, from: impl Into<String>) -> Self {
        self.from.name = from.into();
        self
    }

    pub fn select(mut self, expression: Expression) -> Self {
        self.columns.push(expression);
        self
    }

    pub fn columns(mut self, expressions: impl IntoIterator<Item = Expression>) -> Self {
        self.columns.extend(expressions);
        self
    }

    pub fn r#where(mut self, condition: Expression) -> Self {
        self.r#where = Some(condition);
        self
    }

    pub fn join(mut self, join: JoinClause) -> Self {
        self.joins.push(join);
        self
    }

    pub fn group_by(mut self, expression: Expression) -> Self {
        self.group_by.push(expression);
        self
    }

    pub fn order_by(mut self, order_expr: OrderBy) -> Self {
        self.order_by.push(order_expr);
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }
}

impl From<SelectBuilder> for Select {
    fn from(value: SelectBuilder) -> Self {
        Self {
            columns: value.columns,
            joins: value.joins,
            from: value.from,
            r#where: value.r#where,
            order_by: value.order_by,
            group_by: value.group_by,
            limit: value.limit,
            offset: value.offset,
        }
    }
}

impl TableRef {
    pub fn new(name: String, alias: Option<String>) -> Self {
        Self { name, alias }
    }

    /// Returns the key for lookups in the context.
    /// Alias if present, otherwise table name.
    pub fn key(&self) -> &str {
        self.alias.as_ref().unwrap_or(&self.name)
    }
}

impl<S> From<S> for TableRef
where
    S: AsRef<str>,
{
    fn from(value: S) -> Self {
        TableRef::new(value.as_ref().into(), None)
    }
}

impl Select {
    pub fn builder() -> SelectBuilder {
        SelectBuilder::default()
    }
}

impl From<Function> for Keyword {
    fn from(value: Function) -> Self {
        match value {
            Function::Sqrt => Self::Sqrt,
            Function::Ascii => Self::Ascii,
            Function::Position => Self::Position,
            Function::Power => Self::Power,
            Function::Substring => Self::Substring,
            Function::Sign => Self::Sign,
            Function::Abs => Self::Abs,
            Function::Concat => Self::Concat,
            Function::Trunc => Self::Trunc,
            Function::Min => Self::Min,
            Function::Max => Self::Max,
            Function::Count => Self::Count,
            Function::TypeOf => Self::TypeOf,
            Function::Avg => Self::Avg,
            Function::Sum => Self::Sum,
            Function::Coalesce => Self::Coalesce,
            Function::Extract => Self::Extract,
            Function::UuidV4 => unimplemented!(),
        }
    }
}

impl From<Keyword> for JoinType {
    fn from(value: Keyword) -> Self {
        match value {
            Keyword::Left => Self::Left,
            Keyword::Right => Self::Right,
            Keyword::Inner => Self::Inner,
            Keyword::Full => Self::Full,
            _ => Self::Inner,
        }
    }
}

impl TryFrom<&Keyword> for Function {
    type Error = ();

    fn try_from(value: &Keyword) -> Result<Self, Self::Error> {
        match value {
            Keyword::Sqrt => Ok(Self::Sqrt),
            Keyword::Ascii => Ok(Self::Ascii),
            Keyword::Position => Ok(Self::Position),
            Keyword::Power => Ok(Self::Power),
            Keyword::Substring => Ok(Self::Substring),
            Keyword::Sign => Ok(Self::Sign),
            Keyword::Abs => Ok(Self::Abs),
            Keyword::Concat => Ok(Self::Concat),
            Keyword::Trunc => Ok(Self::Trunc),
            Keyword::Count => Ok(Self::Count),
            Keyword::Sum => Ok(Self::Sum),
            Keyword::Avg => Ok(Self::Avg),
            Keyword::Min => Ok(Self::Min),
            Keyword::Max => Ok(Self::Max),
            Keyword::TypeOf => Ok(Self::TypeOf),
            Keyword::Coalesce => Ok(Self::Coalesce),
            Keyword::Extract => Ok(Self::Extract),
            _ => Err(()),
        }
    }
}

impl Display for Temporal {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DateTime(datetime) => Display::fmt(datetime, f),
            Self::Date(date) => Display::fmt(date, f),
            Self::Time(time) => Display::fmt(time, f),
        }
    }
}

impl Display for OrderBy {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.direction {
            OrderDirection::Asc => write!(f, "{} ASC", self.expr),
            OrderDirection::Desc => write!(f, "{} DESC", self.expr),
        }
    }
}

impl From<Expression> for OrderBy {
    fn from(expr: Expression) -> Self {
        Self {
            expr,
            direction: Default::default(),
        }
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Inner => "INNER",
            Self::Full => "FULL",
        })
    }
}

impl From<i128> for Value {
    fn from(value: i128) -> Self {
        Self::Number(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::Float(value as f64)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_string())
    }
}

impl From<Temporal> for Value {
    fn from(value: Temporal) -> Self {
        Self::Temporal(value)
    }
}

impl From<Interval> for Value {
    fn from(value: Interval) -> Self {
        Self::Interval(value)
    }
}

impl From<NaiveDate> for Temporal {
    fn from(value: NaiveDate) -> Self {
        Self::Date(value)
    }
}

impl From<NaiveDateTime> for Temporal {
    fn from(value: NaiveDateTime) -> Self {
        Self::DateTime(value)
    }
}

impl From<NaiveTime> for Temporal {
    fn from(value: NaiveTime) -> Self {
        Self::Time(value)
    }
}

impl From<Temporal> for Type {
    fn from(value: Temporal) -> Self {
        match value {
            Temporal::Date(_) => Self::Date,
            Temporal::Time(_) => Self::Time,
            Temporal::DateTime(_) => Self::DateTime,
        }
    }
}

impl TryFrom<&str> for Temporal {
    type Error = DateParseError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if let Ok(dt) = NaiveDateTime::parse_str(value) {
            return Ok(Temporal::DateTime(dt));
        }

        if let Ok(date) = NaiveDate::parse_str(value) {
            return Ok(Temporal::Date(date));
        }

        if let Ok(time) = NaiveTime::parse_str(value) {
            return Ok(Temporal::Time(time));
        }

        Err(DateParseError::InvalidDateTime)
    }
}

pub(crate) fn join<'t, T: Display + 't>(
    values: impl IntoIterator<Item = &'t T>,
    separator: &str,
) -> String {
    let mut joined = String::new();
    let mut iter = values.into_iter();

    if let Some(value) = iter.next() {
        write!(joined, "{}", &value).unwrap();
    }

    for value in iter {
        joined.push_str(separator);
        write!(joined, "{value}").unwrap();
    }

    joined
}
