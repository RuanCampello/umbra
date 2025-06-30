//! Token's definitions for SQL.
//!
//! You may consider checking out [IBM documentation](https://www.ibm.com/docs/en/db2/11.5.0?topic=elements-tokens) on that.

use std::{
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
    For,
    Primary,
    Key,
    Unique,
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
    Bool,
    True,
    False,
    Index,
    Order,
    By,
    On,
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
            Self::Keyword(keyword) => f.write_str(keyword.as_str()),
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

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Create => "CREATE",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Insert => "INSERT",
            Self::Into => "INTO",
            Self::Values => "VALUES",
            Self::Set => "SET",
            Self::Drop => "DROP",
            Self::Where => "WHERE",
            Self::From => "FROM",
            Self::And => "AND",
            Self::Or => "OR",
            Self::Between => "BETWEEN",
            Self::In => "IN",
            Self::Like => "LIKE",
            Self::Substring => "SUBSTRING",
            Self::Ascii => "ASCII",
            Self::Concat => "CONCAT",
            Self::For => "FOR",
            Self::Primary => "PRIMARY",
            Self::Key => "KEY",
            Self::Unique => "UNIQUE",
            Self::Table => "TABLE",
            Self::Database => "DATABASE",
            Self::Sequence => "SEQUENCE",
            Self::Default => "DEFAULT",
            Self::As => "AS",
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
            Self::Bool => "BOOLEAN",
            Self::True => "TRUE",
            Self::False => "FALSE",
            Self::Index => "INDEX",
            Self::Order => "ORDER",
            Self::By => "BY",
            Self::On => "ON",
            Self::Start => "BEGIN",
            Self::Transaction => "TRANSACTION",
            Self::Rollback => "ROLLBACK",
            Self::Commit => "COMMIT",
            Self::Explain => "EXPLAIN",
            Self::Date => "DATE",
            Self::Time => "TIME",
            Self::Timestamp => "TIMESTAMP",
            Self::None => "_",
        }
    }
}

impl FromStr for Keyword {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let keyword = match s.to_uppercase().as_str() {
            "SELECT" => Keyword::Select,
            "CREATE" => Keyword::Create,
            "UPDATE" => Keyword::Update,
            "DELETE" => Keyword::Delete,
            "INSERT" => Keyword::Insert,
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
            "FOR" => Keyword::For,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UNIQUE" => Keyword::Unique,
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
            "BOOLEAN" => Keyword::Bool,
            "INDEX" => Keyword::Index,
            "ORDER" => Keyword::Order,
            "ON" => Keyword::On,
            "BEGIN" => Keyword::Start,
            "TRANSACTION" => Keyword::Transaction,
            "ROLLBACK" => Keyword::Rollback,
            "COMMIT" => Keyword::Commit,
            "EXPLAIN" => Keyword::Explain,
            "TIMESTAMP" => Keyword::Timestamp,
            "DATE" => Keyword::Date,
            "TIME" => Keyword::Time,
            _ => Keyword::None,
        };

        Ok(keyword)
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&Keyword> for Token {
    fn from(keyword: &Keyword) -> Self {
        Token::Keyword(*keyword)
    }
}
