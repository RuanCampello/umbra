//! SQL statements and types declaration.
//! See [this](https://en.wikipedia.org/wiki/Abstract_syntax_tree)
//! to reach out about statements and/or AST

use super::Keyword;
use crate::core::date::interval::Interval;
use crate::vm::expression::VmType;
use std::borrow::Borrow;
use std::fmt::{self, Debug, Display, Formatter, Write};
use std::hash::Hash;
use std::ops::Neg;

/// SQL statements
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
    /// `BEGIN`/`START TRANSACTION`, optionally with its isolation level
    StartTransaction(Option<IsolationLevel>),
    /// `SET TRANSACTION …` (`session: false`) or `SET SESSION CHARACTERISTICS AS TRANSACTION …`
    SetIsolation {
        level: IsolationLevel,
        session: bool,
    },
    Checkpoint,
    Rollback,
    Savepoint(String),
    /// `ROLLBACK TO [SAVEPOINT] name`
    RollbackTo(String),
    /// `RELEASE [SAVEPOINT] name`
    Release(String),
    Explain(Box<Self>),
}

/// Transaction isolation levels
/// `REPEATABLE READ` is an alias of `SNAPSHOT`
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub enum IsolationLevel {
    ReadCommitted,
    #[default]
    Snapshot,
    Serializable,
}

/// The `UPDATE` assignment instruction
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
    /// over `columns`, compared in that order
    Index {
        name: String,
        table: String,
        columns: Vec<String>,
        unique: bool,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct Select {
    /// `SELECT DISTINCT`: duplicate result rows are dropped
    pub distinct: bool,
    pub columns: Vec<Expression>,
    pub from: TableRef,
    pub joins: Vec<JoinClause>,
    pub r#where: Option<Expression>,
    pub order_by: Vec<OrderBy>,
    pub group_by: Vec<Expression>,
    pub having: Option<Expression>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    /// `UNION`, `INTERSECT` and `EXCEPT` members, in order (`true` for `ALL`, which keeps
    /// duplicates); the `ORDER BY`, `LIMIT` and `OFFSET` above then apply to the whole
    pub combined: Vec<(SetOperator, bool, Select)>,
}

#[cfg(test)]
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

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Hash, Clone)]
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

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Clone)]
pub enum Expression {
    Identifier(String),
    QualifiedIdentifier {
        table: String,
        column: String,
    },
    Path {
        head: String,
        segments: Vec<PathSegment>,
    },
    Value(Value),
    #[default]
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
    /// `(SELECT ...)`: the session runs it and puts its result in its place before analysis
    Subquery(Box<Select>),
    /// a column reference the executor bound to its position in the rows it evaluates
    /// never produced by the parser
    Column(usize),
    /// a json path into a bound column (`$.key[0]...`), built once by the executor
    /// never produced by the parser
    JsonPath {
        column: usize,
        path: String,
    },
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub enum PathSegment {
    Key(String),
    Index(isize),
    Wildcard,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct OrderBy {
    pub expr: Expression,
    pub direction: OrderDirection,
}

/// How a member combines with the rows before it
/// `INTERSECT` binds tighter than the others
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Default, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum OrderDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone)]
pub struct JoinClause {
    pub table: TableRef,
    pub on: Expression,
    pub join_type: JoinType,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum JoinType {
    Left,
    Right,
    Inner,
    Full,
}

// Re-export Value and Temporal from the value module
pub use super::value::{Coerce, Temporal, Value};

#[derive(Debug, PartialEq, Clone)]
pub enum Constraint {
    PrimaryKey,
    Unique,
    Nullable,
    Default(Expression),
    /// A predicate every row of the table satisfies (or leaves unknown)
    /// a table-level `CHECK` is carried by the first column
    Check(Expression),
    /// The column's value, when not NULL, is the `column` of some row of `table`
    References {
        table: String,
        column: String,
        on_delete: OnDelete,
    },
}

/// What deleting a referenced row does to the rows referring to it
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
pub enum OnDelete {
    /// The delete fails (`RESTRICT`, `NO ACTION`)
    #[default]
    Restrict,
    Cascade,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Copy)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
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
    Modulo,
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
    Jsonb,
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
    /// `x IN (v1, ..., vn)` as `[x, v1, ..., vn]`: true when `x` equals one, else NULL when one is NULL, else false
    In,
    /// `EXISTS (SELECT ...)` over its one [Expression::Subquery]
    Exists,
    /// `CAST(x AS type)` as `[x, type]`, the type a blob in the schema's type encoding
    Cast,
    /// `COUNT(DISTINCT x)`: the distinct non-null values of `x`
    CountDistinct,
    /// `CASE WHEN c1 THEN r1 ... ELSE e END` as `[c1, r1, ..., e]`, the conditions evaluated in
    /// order and only the chosen result (`CASE x WHEN v` is parsed into `x = v` conditions)
    Case,
}

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
    pub fn is_integer_in_bounds(&self, int: &i64) -> bool {
        let bound = match self {
            Self::SmallInt => i16::MIN as i64..=i16::MAX as i64,
            Self::UnsignedSmallInt => u16::MIN as i64..=u16::MAX as i64,
            Self::SmallSerial => 1..=(i16::MAX as i64),
            Self::Integer => i32::MIN as i64..=i32::MAX as i64,
            Self::UnsignedInteger => 0..=u32::MAX as i64,
            Self::Serial => 1..=(i32::MAX as i64),
            Self::BigInteger => i64::MIN..=i64::MAX,
            Self::UnsignedBigInteger => 0..=i64::MAX,
            Self::BigSerial => 1..=(i64::MAX),
            other => panic!("bound checking must be used only for integer: {other:#?}"),
        };

        bound.contains(int)
    }

    pub const fn is_float_in_bounds(&self, float: &f64) -> bool {
        match self {
            Self::Real => *float >= f32::MIN as f64 && *float <= f32::MAX as f64,
            Self::DoublePrecision => float.is_finite(),
            _ => panic!("bound checking must be used only for floats"),
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
        matches!(self, Self::SmallSerial | Self::Serial | Self::BigSerial)
    }

    pub const fn is_float(&self) -> bool {
        matches!(self, Self::Real | Self::DoublePrecision)
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
                Create::Sequence { name, r#type, table } => {
                    write!(f, "CREATE SEQUENCE {name} AS {type} OWNED BY {table}")?
                },
                Create::Table { name, columns } => {
                    write!(f, "CREATE TABLE {name} ({})", join(columns, ", "))?
                },
                Create::Database(name) => write!(f, "CREATE DATABASE {name}")?,
                Create::Index { name, table, columns, unique } => {
                    let unique = match unique {
                        true => "UNIQUE",
                        _ => "",
                    };
                    write!(f, "CREATE {unique} INDEX {name} ON {table}({})", columns.join(", "))?;
                },
            },

            Statement::Select(Select {
                distinct,
                columns,
                from,
                r#where,
                joins,
                order_by,
                group_by,
                having,
                limit,
                offset,
                combined,
            }) => {
                let distinct = match *distinct {
                    true => "DISTINCT ",
                    _ => "",
                };
                write!(f, "SELECT {distinct}{} FROM {}", join(columns, ", "), from.name)?;
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

                if !group_by.is_empty() {
                    write!(f, " GROUP BY {}", join(group_by, ", "))?;
                }

                if let Some(expr) = having {
                    write!(f, " HAVING {expr}")?;
                }

                if !order_by.is_empty() {
                    write!(f, " ORDER BY {}", join(order_by, ", "))?;
                }

                if let Some(limit) = limit {
                    write!(f, " LIMIT {limit}")?;
                }

                if let Some(offset) = offset {
                    write!(f, " OFFSET {offset}")?;
                }

                for (operator, all, member) in combined {
                    let all = match *all {
                        true => " ALL",
                        _ => "",
                    };
                    write!(f, " {operator}{all} {}", Statement::Select(member.clone()))?;
                }
            },

            Statement::Delete(Delete { from, r#where }) => {
                write!(f, "DELETE FROM {from}")?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }
            },

            Statement::Update(Update { table, columns, r#where, returning }) => {
                write!(f, "UPDATE {table} SET {}", join(columns, ", "))?;
                if let Some(expr) = r#where {
                    write!(f, " WHERE {expr}")?;
                }

                if !returning.is_empty() {
                    write!(f, "RETURNING {}", join(returning, ", "))?;
                }
            },

            Statement::Insert(Insert { into, columns, values, returning }) => {
                let columns = match columns.is_empty() {
                    true => String::from(" "),
                    false => format!(" ({}) ", join(columns, ", ")),
                };

                let values = join(
                    &values.iter().map(|row| format!("({})", join(row, ", "))).collect::<Vec<_>>(),
                    ", ",
                );

                write!(f, "INSERT INTO {into}{columns}VALUES ({})", values)?;
                if !returning.is_empty() {
                    write!(f, "RETURNING {}", join(returning, ", "))?;
                }
            },
            Statement::Drop(drop) => {
                match drop {
                    Drop::Table(name) => write!(f, "DROP TABLE {name}")?,
                    Drop::Database(name) => write!(f, "DROP DATABASE {name}")?,
                };
            },
            Statement::StartTransaction(None) => f.write_str("BEGIN TRANSACTION")?,
            Statement::StartTransaction(Some(level)) => {
                write!(f, "BEGIN TRANSACTION ISOLATION LEVEL {level}")?
            },
            Statement::SetIsolation { level, session: false } => {
                write!(f, "SET TRANSACTION ISOLATION LEVEL {level}")?
            },
            Statement::SetIsolation { level, session: true } => {
                write!(f, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level}")?
            },
            Statement::Checkpoint => f.write_str("CHECKPOINT")?,
            Statement::Commit => f.write_str("COMMIT")?,
            Statement::Rollback => f.write_str("ROLLBACK")?,
            Statement::Savepoint(name) => write!(f, "SAVEPOINT {name}")?,
            Statement::RollbackTo(name) => write!(f, "ROLLBACK TO SAVEPOINT {name}")?,
            Statement::Release(name) => write!(f, "RELEASE SAVEPOINT {name}")?,
            Statement::Source(source) => write!(f, "SOURCE '{source}'")?,
            Statement::Explain(statement) => write!(f, "EXPLAIN {statement}")?,
        };

        f.write_char(';')
    }
}

impl Statement {
    /// the expression trees of the statement
    pub(crate) fn expressions_mut(&mut self) -> Vec<&mut Expression> {
        match self {
            Statement::Select(select) => select
                .columns
                .iter_mut()
                .chain(&mut select.r#where)
                .chain(&mut select.having)
                .chain(&mut select.group_by)
                .chain(select.order_by.iter_mut().map(|order| &mut order.expr))
                .chain(select.joins.iter_mut().map(|join| &mut join.on))
                .collect(),
            Statement::Update(update) => update
                .columns
                .iter_mut()
                .map(|assignment| &mut assignment.value)
                .chain(&mut update.r#where)
                .chain(&mut update.returning)
                .collect(),
            Statement::Delete(delete) => delete.r#where.iter_mut().collect(),
            Statement::Insert(insert) => {
                insert.values.iter_mut().flatten().chain(&mut insert.returning).collect()
            },
            Statement::Explain(statement) => statement.expressions_mut(),
            _ => Vec::new(),
        }
    }
}

impl Expression {
    pub(crate) fn contains_subquery(&self) -> bool {
        match self {
            Self::Subquery(_) => true,
            Self::Function { args, .. } => args.iter().any(Self::contains_subquery),
            Self::BinaryOperation { left, right, .. } => {
                left.contains_subquery() || right.contains_subquery()
            },
            Self::UnaryOperation { expr, .. }
            | Self::Nested(expr)
            | Self::Alias { expr, .. }
            | Self::IsNull { expr, .. } => expr.contains_subquery(),
            _ => false,
        }
    }

    pub(crate) fn unwrap_alias(&self) -> &Self {
        match self {
            Expression::Alias { ref expr, .. } => expr,
            _ => self,
        }
    }

    pub(in crate::sql) fn as_identifier(&self) -> Option<&str> {
        if let Self::Identifier(alias) = self {
            return Some(alias);
        };

        None
    }

    /// recursively substitutes identifiers that refer to aliases with their actual expressions
    /// this enables using `SELECT` aliases in `WHERE/ORDER BY/GROUP BY` clauses
    pub(in crate::sql) fn substitute_aliases<F>(&self, resolve_alias: F) -> Self
    where
        F: Fn(&str) -> Option<Expression> + Copy,
    {
        use Expression::*;
        match self {
            Identifier(ident) => match resolve_alias(ident) {
                Some(aliased_expr) => aliased_expr.substitute_aliases(resolve_alias),
                _ => self.clone(),
            },
            BinaryOperation { operator, left, right } => BinaryOperation {
                operator: *operator,
                left: Box::new(left.substitute_aliases(resolve_alias)),
                right: Box::new(right.substitute_aliases(resolve_alias)),
            },
            UnaryOperation { operator, expr } => UnaryOperation {
                operator: *operator,
                expr: Box::new(expr.substitute_aliases(resolve_alias)),
            },
            IsNull { expr, negated } => IsNull {
                expr: Box::new(expr.substitute_aliases(resolve_alias)),
                negated: *negated,
            },
            Function { func, args } => Function {
                func: *func,
                args: args.iter().map(|a| a.substitute_aliases(resolve_alias)).collect(),
            },
            Alias { expr, alias } => Alias {
                expr: Box::new(expr.substitute_aliases(resolve_alias)),
                alias: alias.clone(),
            },
            Nested(expr) => Nested(Box::new(expr.substitute_aliases(resolve_alias))),
            _ => self.clone(),
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
                let variants = variants.iter().map(|v| format!("\"{v}\"")).collect::<Vec<_>>();
                let def = variants.join(" | ");

                write!(f, "{def}")?;
            },
            None => write!(f, "{} {}", self.name, self.data_type)?,
        };

        for constraint in &self.constraints {
            match constraint {
                Constraint::PrimaryKey => f.write_str(" PRIMARY KEY")?,
                Constraint::Unique => f.write_str(" UNIQUE")?,
                Constraint::Nullable => f.write_str(" NULLABLE")?,
                Constraint::Default(expr) => write!(f, " DEFAULT {expr}")?,
                Constraint::Check(expr) => write!(f, " CHECK ({expr})")?,
                Constraint::References { table, column, on_delete } => {
                    write!(f, " REFERENCES {table}({column})")?;
                    if *on_delete == OnDelete::Cascade {
                        f.write_str(" ON DELETE CASCADE")?;
                    }
                },
            }
        }

        Ok(())
    }
}

impl Expression {
    /// the expression as SQL the parser reads back to an equivalent tree, fully parenthesised
    pub fn to_sql(&self) -> Option<String> {
        Some(match self {
            Self::Identifier(name) => name.clone(),
            Self::Value(value) => match value {
                Value::Null => "NULL".into(),
                Value::Boolean(boolean) => boolean.to_string().to_uppercase(),
                Value::Number(number) => number.to_string(),
                Value::Float(float) => float.to_string(),
                Value::Numeric(numeric) => numeric.to_string(),
                Value::String(text) => match (text.contains('\''), text.contains('"')) {
                    (false, _) => format!("'{text}'"),
                    (true, false) => format!("\"{text}\""),
                    (true, true) => return None,
                },
                _ => return None,
            },
            Self::UnaryOperation { operator, expr } => format!("({operator}{})", expr.to_sql()?),
            Self::BinaryOperation { operator, left, right } => {
                format!("({} {operator} {})", left.to_sql()?, right.to_sql()?)
            },
            Self::Function { func: Function::In, args } => {
                let args = args.iter().map(Self::to_sql).collect::<Option<Vec<_>>>()?;
                format!("({} IN ({}))", args[0], args[1..].join(", "))
            },
            Self::Function { func: Function::Case, args } => {
                let args = args.iter().map(Self::to_sql).collect::<Option<Vec<_>>>()?;
                let (branches, default) = args.split_at(args.len() - 1);
                let branches: Vec<_> = branches
                    .chunks(2)
                    .map(|pair| format!("WHEN {} THEN {}", pair[0], pair[1]))
                    .collect();
                format!("(CASE {} ELSE {} END)", branches.join(" "), default[0])
            },
            Self::Function { func, args } => match func {
                Function::Substring | Function::Position | Function::Extract => return None,
                _ => {
                    let args = args.iter().map(Self::to_sql).collect::<Option<Vec<_>>>()?;
                    format!("{func}({})", args.join(", "))
                },
            },
            Self::IsNull { expr, negated } => match negated {
                false => format!("({} IS NULL)", expr.to_sql()?),
                true => format!("({} IS NOT NULL)", expr.to_sql()?),
            },
            Self::Nested(expr) => expr.to_sql()?,
            _ => return None,
        })
    }
}

impl Display for Expression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(ident) => f.write_str(ident),
            Self::QualifiedIdentifier { table, column } => write!(f, "{table}.{column}"),
            Self::Column(position) => write!(f, "#{position}"),
            Self::Subquery(select) => write!(f, "({})", Statement::Select(select.as_ref().clone())),
            Self::JsonPath { column, path } => write!(f, "#{column}{}", &path[1..]),
            Self::Value(value) => write!(f, "{value}"),
            Self::Wildcard => f.write_str("*"),
            Self::UnaryOperation { operator, expr } => write!(f, "{operator}{expr}"),
            Self::Nested(expr) => write!(f, "({expr})"),
            Self::BinaryOperation { operator, left, right } => {
                write!(f, "{left} {operator} {right}")
            },
            Self::Path { head, segments } => {
                f.write_str(head)?;
                Ok(for seg in segments {
                    match seg {
                        PathSegment::Key(key) => write!(f, ".{key}")?,
                        PathSegment::Index(idx) => write!(f, "[{idx}]")?,
                        PathSegment::Wildcard => write!(f, "*")?,
                    }
                })
            },
            Self::Function { func: Function::In, args } => {
                write!(f, "{} IN ({})", args[0], join(&args[1..], ", "))
            },
            Self::Function { func: Function::CountDistinct, args } => {
                write!(f, "COUNT(DISTINCT {})", args[0])
            },
            Self::Function { func: Function::Cast, args } => match &args[1] {
                Self::Value(Value::Blob(target)) => {
                    write!(f, "CAST({} AS {})", args[0], crate::db::decode_type(target))
                },
                other => write!(f, "CAST({} AS {other})", args[0]),
            },
            Self::Function { func: Function::Case, args } => {
                f.write_str("CASE")?;
                for pair in args[..args.len() - 1].chunks(2) {
                    write!(f, " WHEN {} THEN {}", pair[0], pair[1])?;
                }
                write!(f, " ELSE {} END", args[args.len() - 1])
            },
            Self::Function { func, args } => {
                let mut args = args.iter();
                write!(f, "{func}(")?;
                if let Some(arg) = args.next() {
                    write!(f, "{arg}")?;
                    for arg in args {
                        write!(f, ", {arg}")?;
                    }
                }
                write!(f, ")")
            },
            Self::Alias { expr, alias } => write!(f, "{expr} AS {alias}"),
            Self::IsNull { expr, negated } => match negated {
                false => write!(f, "{expr} IS NULL"),
                _ => write!(f, "{expr} IS NOT NULL"),
            },
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
            Self::Modulo => "%",
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
        f.write_str(match self {
            Self::Minus => "-",
            Self::Plus => "+",
            Self::Not => "NOT ",
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
            Type::Jsonb => f.write_str("JSONB"),
            Type::Numeric(precision, scale) => match (precision, scale) {
                (&NUMERIC_ANY, _) => write!(f, "NUMERIC"),
                (precision, 0) => write!(f, "NUMERIC({precision})"),
                (precision, scale) => write!(f, "NUMERIC({precision}, {scale})"),
            },
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

    /// returns the `VmType` that this function returns
    pub const fn return_type(&self, input: &VmType) -> VmType {
        use Function::*;
        match self {
            Substring | Concat | TypeOf => VmType::String,
            Count | CountDistinct | UuidV4 | Ascii | Position | Extract => VmType::Number,
            Sqrt | Power => match input {
                VmType::Numeric => VmType::Numeric,
                _ => VmType::Float,
            },
            Avg => match input {
                VmType::Number | VmType::Numeric => VmType::Numeric,
                _ => *input,
            },
            Sum => *input,
            Abs | Sign | Trunc | Min | Max | Coalesce | Case => *input,
            In | Exists => VmType::Bool,
            Cast => *input,
        }
    }

    pub const fn is_math(&self) -> bool {
        matches!(self, Self::Trunc | Self::Abs | Self::Sqrt | Self::Sign | Self::Power)
    }

    pub const fn is_aggr(&self) -> bool {
        matches!(
            self,
            Self::Count | Self::CountDistinct | Self::Sum | Self::Avg | Self::Min | Self::Max
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
                Self::from_uppercase(&s.to_uppercase()).ok_or(())
            }
        }

        impl Function {
            /// every function by its upper-case name
            pub(crate) const NAMES: &'static [(&'static str, Function)] = &[$(($str, Function::$variant),)*];

            /// the function named `upper`, already in upper case
            pub(crate) fn from_uppercase(upper: &str) -> Option<Self> {
                match upper {
                    $($str => Some(Function::$variant),)*
                    _ => None,
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
    Case => "CASE",
    In => "IN",
    Exists => "EXISTS",
    Cast => "CAST",
    CountDistinct => "COUNT DISTINCT",
}

#[cfg(test)]
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

#[cfg(test)]
impl From<SelectBuilder> for Select {
    fn from(value: SelectBuilder) -> Self {
        Self {
            distinct: false,
            columns: value.columns,
            joins: value.joins,
            from: value.from,
            r#where: value.r#where,
            order_by: value.order_by,
            group_by: value.group_by,
            having: None,
            limit: value.limit,
            offset: value.offset,
            combined: Vec::new(),
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

#[cfg(test)]
impl Select {
    pub fn builder() -> SelectBuilder {
        SelectBuilder::default()
    }
}

impl From<Function> for Keyword {
    fn from(value: Function) -> Self {
        match value {
            Function::Case => Self::Case,
            Function::In => Self::In,
            Function::Exists => Self::Exists,
            Function::Cast => Self::Cast,
            Function::CountDistinct => Self::Count,
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
            Function::UuidV4 => Self::None,
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
        Self { expr, direction: Default::default() }
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

impl From<Temporal> for Type {
    fn from(value: Temporal) -> Self {
        match value {
            Temporal::Date(_) => Self::Date,
            Temporal::Time(_) => Self::Time,
            Temporal::DateTime(_) => Self::DateTime,
        }
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

impl Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ReadCommitted => "READ COMMITTED",
            Self::Snapshot => "SNAPSHOT",
            Self::Serializable => "SERIALIZABLE",
        })
    }
}

impl Display for SetOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.write_str(match self {
            Self::Union => "UNION",
            Self::Intersect => "INTERSECT",
            Self::Except => "EXCEPT",
        })
    }
}
