//! Token's definitions for SQL.
//!
//! You may consider checking out [IBM documentation](https://www.ibm.com/docs/en/db2/11.5.0?topic=elements-tokens) on that.

use std::{
    borrow::Borrow,
    fmt::{Display, Formatter, Write},
    str::FromStr,
};

/// SQL tokens.
#[derive(Debug, PartialEq)]
pub(in crate::sql) enum Token {
    Whitespace(Whitespace),
    Identifier(String),
    Keyword(Keyword),
    String(String),
    Number(String),
    Eq,
    Neq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    Mul,
    Div,
    Plus,
    Minus,
    Comma,
    Semicolon,
    LeftParen,
    RightParen,
    Dot,
    /// That's not an actual SQL Token, but helps mark the end of token's stream.
    Eof,
}

#[derive(Debug, PartialEq)]
pub(in crate::sql) enum Whitespace {
    Tab,
    Space,
    Newline,
}

/// A little subset of SQL keywords.
///
/// [Here](https://www.ibm.com/docs/en/informix-servers/12.10.0?topic=appendixes-keywords-sql-informix) there are much more than that, if you want to check out.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Keyword {
    Select,
    Create,
    Update,
    Delete,
    Insert,
    Source,
    Into,
    Values,
    Set,
    Drop,
    Where,
    From,
    And,
    Or,
    Between,
    In,
    Like,
    Substring,
    Ascii,
    Concat,
    Abs,
    Sqrt,
    Trunc,
    Power,
    Sign,
    Count,
    Avg,
    Min,
    Max,
    Sum,
    Coalesce,
    Position,
    TypeOf,
    Extract,
    For,
    Primary,
    Key,
    Unique,
    Nullable,
    Null,
    Is,
    Not,
    Table,
    Database,
    Sequence,
    Default,
    As,
    Owned,
    Serial,
    SmallSerial,
    BigSerial,
    SmallInt,
    Int,
    BigInt,
    Real,
    Double,
    Precision,
    Uuid,
    Unsigned,
    Varchar,
    Text,
    Bool,
    Interval,
    True,
    False,
    Index,
    Order,
    Asc,
    Desc,
    Inner,
    Full,
    Left,
    Right,
    Join,
    Group,
    By,
    On,
    Limit,
    Offset,
    Start,
    Transaction,
    Rollback,
    Commit,
    Explain,
    Timestamp,
    Date,
    Time,
    /// This is not an actual SQL keyword, but it's used for convenience.
    None,
}

impl Token {
    pub(in crate::sql) fn is_keyword(c: &char) -> bool {
        c.is_ascii_lowercase() || c.is_ascii_uppercase() || c.is_ascii_digit() || *c == '_'
    }
}

impl Display for Token {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Whitespace(whitespace) => f.write_char(whitespace.as_char()),
            Self::Identifier(identifier) => f.write_str(identifier),
            Self::Keyword(keyword) => f.write_str(keyword.borrow()),
            Self::Number(number) => f.write_str(number),
            Self::String(string) => write!(f, "\"{string}\""),
            token => {
                let s = match token {
                    Self::Eq => "=",
                    Self::Neq => "!=",
                    Self::Gt => ">",
                    Self::Lt => "<",
                    Self::GtEq => ">=",
                    Self::LtEq => "<=",
                    Self::Mul => "*",
                    Self::Div => "/",
                    Self::Plus => "+",
                    Self::Minus => "-",
                    Self::Comma => ",",
                    Self::Semicolon => ";",
                    Self::Dot => ".",
                    Self::LeftParen => "(",
                    Self::RightParen => ")",
                    _ => unreachable!(),
                };
                f.write_str(s)
            }
        }
    }
}

impl Whitespace {
    pub(in crate::sql) fn as_char(&self) -> char {
        match self {
            Self::Tab => '\t',
            Self::Space => ' ',
            Self::Newline => '\n',
        }
    }
}

impl Display for Whitespace {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_char(self.as_char())
    }
}

impl Keyword {
    pub fn as_optional(&self) -> Option<Keyword> {
        match self {
            Self::None => None,
            _ => Some(*self),
        }
    }

    pub fn is_function(&self) -> bool {
        use crate::sql::statement::Function;

        Function::try_from(self).is_ok()
    }
}

impl Borrow<str> for Keyword {
    fn borrow(&self) -> &str {
        match self {
            Self::Select => "SELECT",
            Self::Create => "CREATE",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Insert => "INSERT",
            Self::Source => "SOURCE",
            Self::Into => "INTO",
            Self::Values => "VALUES",
            Self::Set => "SET",
            Self::Drop => "DROP",
            Self::Where => "WHERE",
            Self::From => "FROM",
            Self::As => "AS",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Between => "BETWEEN",
            Self::In => "IN",
            Self::Like => "LIKE",
            Self::Min => "MIN",
            Self::Max => "MAX",
            Self::Sum => "SUM",
            Self::Coalesce => "COALESCE",
            Self::Avg => "AVG",
            Self::TypeOf => "TYPEOF",
            Self::Extract => "EXTRACT",
            Self::For => "FOR",
            Self::Primary => "PRIMARY",
            Self::Key => "KEY",
            Self::Unique => "UNIQUE",
            Self::Nullable => "NULLABLE",
            Self::Null => "NULL",
            Self::Is => "IS",
            Self::Not => "NOT",
            Self::Table => "TABLE",
            Self::Database => "DATABASE",
            Self::Sequence => "SEQUENCE",
            Self::Default => "DEFAULT",
            Self::Owned => "OWNED",
            Self::SmallSerial => "SMALLSERIAL",
            Self::Serial => "SERIAL",
            Self::BigSerial => "BIGSERIAL",
            Self::SmallInt => "SMALLINT",
            Self::Int => "INT",
            Self::BigInt => "BIGINT",
            Self::Real => "REAL",
            Self::Double => "DOUBLE",
            Self::Precision => "PRECISION",
            Self::Uuid => "UUID",
            Self::Unsigned => "UNSIGNED",
            Self::Varchar => "VARCHAR",
            Self::Text => "TEXT",
            Self::Bool => "BOOLEAN",
            Self::True => "TRUE",
            Self::False => "FALSE",
            Self::Index => "INDEX",
            Self::Order => "ORDER",
            Self::Asc => "ASC",
            Self::Desc => "DESC",
            Self::Group => "GROUP",
            Self::Inner => "INNER",
            Self::Full => "FULL",
            Self::Left => "LEFT",
            Self::Right => "RIGHT",
            Self::Join => "JOIN",
            Self::By => "BY",
            Self::On => "ON",
            Self::Limit => "LIMIT",
            Self::Offset => "OFFSET",
            Self::Start => "BEGIN",
            Self::Transaction => "TRANSACTION",
            Self::Rollback => "ROLLBACK",
            Self::Commit => "COMMIT",
            Self::Explain => "EXPLAIN",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::Interval => "INTERVAL",
            Self::Count => "COUNT",
            Self::Abs => "ABS",
            Self::Sqrt => "SQRT",
            Self::Concat => "CONCAT",
            Self::Substring => "SUBSTRING",
            Self::Ascii => "ASCII",
            Self::Power => "POWER",
            Self::Trunc => "TRUNC",
            Self::Sign => "SIGN",
            Self::Position => "POSITION",
            Self::None => "_",
        }
    }
}

impl FromStr for Keyword {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        use crate::sql::statement::Function;

        if let Ok(function) = s.parse::<Function>() {
            return Ok(Keyword::from(function));
        }

        let keyword = match s.to_uppercase().as_str() {
            "SELECT" => Keyword::Select,
            "CREATE" => Keyword::Create,
            "UPDATE" => Keyword::Update,
            "DELETE" => Keyword::Delete,
            "INSERT" => Keyword::Insert,
            "SOURCE" => Keyword::Source,
            "VALUES" => Keyword::Values,
            "INTO" => Keyword::Into,
            "SET" => Keyword::Set,
            "DROP" => Keyword::Drop,
            "FROM" => Keyword::From,
            "TABLE" => Keyword::Table,
            "WHERE" => Keyword::Where,
            "AND" => Keyword::And,
            "OR" => Keyword::Or,
            "BETWEEN" => Keyword::Between,
            "IN" => Keyword::In,
            "LIKE" => Keyword::Like,
            "SUBSTRING" => Keyword::Substring,
            "ASCII" => Keyword::Ascii,
            "CONCAT" => Keyword::Concat,
            "POSITION" => Keyword::Position,
            "COUNT" => Keyword::Count,
            "AVG" => Keyword::Avg,
            "SUM" => Keyword::Sum,
            "COALESCE" => Keyword::Coalesce,
            "MIN" => Keyword::Min,
            "MAX" => Keyword::Max,
            "TYPEOF" => Keyword::TypeOf,
            "FOR" => Keyword::For,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UNIQUE" => Keyword::Unique,
            "NULLABLE" => Keyword::Nullable,
            "NULL" => Keyword::Null,
            "IS" => Keyword::Is,
            "NOT" => Keyword::Not,
            "SMALLSERIAL" => Keyword::SmallSerial,
            "SERIAL" => Keyword::Serial,
            "BIGSERIAL" => Keyword::BigSerial,
            "SMALLINT" => Keyword::SmallInt,
            "INT" | "INTEGER" => Keyword::Int,
            "BIGINT" => Keyword::BigInt,
            "REAL" => Keyword::Real,
            "DOUBLE" => Keyword::Double,
            "PRECISION" => Keyword::Precision,
            "UUID" => Keyword::Uuid,
            "BY" => Keyword::By,
            "DATABASE" => Keyword::Database,
            "SEQUENCE" => Keyword::Sequence,
            "DEFAULT" => Keyword::Default,
            "AS" => Keyword::As,
            "OWNED" => Keyword::Owned,
            "UNSIGNED" => Keyword::Unsigned,
            "VARCHAR" => Keyword::Varchar,
            "TEXT" => Keyword::Text,
            "BOOLEAN" => Keyword::Bool,
            "INDEX" => Keyword::Index,
            "ORDER" => Keyword::Order,
            "ASC" => Keyword::Asc,
            "DESC" => Keyword::Desc,
            "GROUP" => Keyword::Group,
            "ON" => Keyword::On,
            "JOIN" => Keyword::Join,
            "FULL" => Keyword::Full,
            "INNER" => Keyword::Inner,
            "LEFT" => Keyword::Left,
            "RIGHT" => Keyword::Right,
            "LIMIT" => Keyword::Limit,
            "OFFSET" => Keyword::Offset,
            "BEGIN" => Keyword::Start,
            "TRANSACTION" => Keyword::Transaction,
            "ROLLBACK" => Keyword::Rollback,
            "COMMIT" => Keyword::Commit,
            "EXPLAIN" => Keyword::Explain,
            "TIMESTAMP" => Keyword::Timestamp,
            "INTERVAL" => Keyword::Interval,
            "DATE" => Keyword::Date,
            "TIME" => Keyword::Time,
            _ => Keyword::None,
        };

        Ok(keyword)
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.borrow())
    }
}

impl From<&Keyword> for Token {
    fn from(keyword: &Keyword) -> Self {
        Token::Keyword(*keyword)
    }
}
