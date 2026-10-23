//! That where we convert the raw SQL query into a tree of statements.
//! The so-called AST.
//!
//! That involves the simplest validation in the SQL pipeline.
//! We don't do the concrete type validation/coercion here, so, for example,
//! [DateTime] will be parser, here, like a common [String].
//!
//! It's mostly inspired by the way postgres'
//! [parser stage](https://www.postgresql.org/docs/17/parser-stage.html) works.

mod queries;
mod tokenizer;
mod tokens;

use crate::core::date::ExtractKind;
use crate::core::date::interval::Interval;
use crate::core::json;
use crate::sql::parser::tokenizer::{Location, TokenWithLocation, Tokenizer, TokenizerError};
use crate::sql::parser::tokens::Token;
use crate::sql::statement::{
    Assignment, BinaryOperator, Column, Constraint, Create, Delete, Drop, Expression, Function,
    Insert, JoinClause, JoinType, PathSegment, Select, Statement, Type, UnaryOperator, Update,
    Value,
};
use crate::sql::statement::{IsolationLevel, NUMERIC_ANY, OnDelete, SetOperator, TableRef};

use std::borrow::Cow;
use std::fmt::Display;
use std::iter::Peekable;
use std::str::FromStr;

pub use tokens::Keyword;

pub(crate) struct Parser<'input> {
    input: &'input str,
    tokenizer: Peekable<tokenizer::IntoIter<'input>>,
    location: Location,
}

#[derive(Debug, PartialEq)]
pub struct ParserError {
    kind: ErrorKind,
    location: Location,
    input: String,
}

#[derive(Debug, PartialEq)]
enum ErrorKind {
    TokenizerError(tokenizer::ErrorKind),
    Expected { expected: Token, found: Token },
    ExpectedOneOf { expected: Vec<Token>, found: Token },
    UnexpectedEof,
    FormatError(String),
}

pub(in crate::sql) type ParserResult<T> = Result<T, ParserError>;

const UNARY_ARITHMETIC_OPERATOR: u8 = 50;
/// binds looser than comparisons and tighter than `AND`
const UNARY_NOT_OPERATOR: u8 = 15;

trait Sql<'sql>: Sized {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self>;
}

impl<'input> Parser<'input> {
    pub fn new(input: &'input str) -> Self {
        Self {
            input,
            tokenizer: Tokenizer::without_whitespace(input).into_iter().peekable(),
            location: Location::default(),
        }
    }

    pub fn try_parse(&mut self) -> ParserResult<Vec<Statement>> {
        let mut statements = Vec::new();

        loop {
            match self.peek_token() {
                Some(Ok(Token::Eof)) | None => return Ok(statements),
                _ => statements.push(self.parse_statement()?),
            }
        }
    }

    pub fn parse_statement(&mut self) -> ParserResult<Statement> {
        let statement = match self.expect_one(&Self::supported_statements())? {
            Keyword::Select => Statement::Select(self.parse_union()?),
            Keyword::Create => Statement::Create(Create::parse(self)?),
            Keyword::Insert => Statement::Insert(Insert::parse(self)?),
            Keyword::Update => Statement::Update(Update::parse(self)?),
            Keyword::Drop => Statement::Drop(Drop::parse(self)?),
            Keyword::Delete => Statement::Delete(Delete::parse(self)?),
            Keyword::Begin => {
                self.consume_optional(Token::Keyword(Keyword::Transaction));
                let level = self
                    .consume_optional(Token::Keyword(Keyword::Isolation))
                    .then(|| self.parse_isolation_level())
                    .transpose()?;

                Statement::StartTransaction(level)
            },
            Keyword::Set => {
                let session = self.consume_optional(Token::Keyword(Keyword::Session));
                if session {
                    self.expect_keyword(Keyword::Characteristics)?;
                    self.expect_keyword(Keyword::As)?;
                }

                self.expect_keyword(Keyword::Transaction)?;
                self.expect_keyword(Keyword::Isolation)?;
                Statement::SetIsolation { level: self.parse_isolation_level()?, session }
            },
            Keyword::Commit => Statement::Commit,
            Keyword::Rollback => self
                .consume_optional(Token::Keyword(Keyword::To))
                .then(|| -> Result<_, ParserError> {
                    self.expect_keyword(Keyword::Savepoint)?;
                    Ok(Statement::RollbackTo(self.parse_ident()?))
                })
                .transpose()?
                .unwrap_or(Statement::Rollback),
            Keyword::Checkpoint => Statement::Checkpoint,
            Keyword::Savepoint => Statement::Savepoint(self.parse_ident()?),
            Keyword::Release => {
                self.expect_keyword(Keyword::Savepoint)?;
                Statement::Release(self.parse_ident()?)
            },
            Keyword::Explain => Statement::Explain(Box::new(self.parse_statement()?)),
            Keyword::Source => {
                let path = match self.next_token()? {
                    Token::String(s) => s,
                    token => {
                        return Err(self.error(ErrorKind::Expected {
                            expected: Token::String(String::new()),
                            found: token,
                        }));
                    },
                };
                Statement::Source(path)
            },
            _ => unreachable!(),
        };

        self.expect_token(Token::Semicolon)?;
        Ok(statement)
    }

    /// `LEVEL <level>`, after `ISOLATION`
    fn parse_isolation_level(&mut self) -> ParserResult<IsolationLevel> {
        self.expect_keyword(Keyword::Level)?;
        Ok(
            match self.expect_one(&[Keyword::Read, Keyword::Repeatable, Keyword::Serializable])? {
                Keyword::Read => {
                    self.expect_keyword(Keyword::Committed)?;
                    IsolationLevel::ReadCommitted
                },
                Keyword::Repeatable => {
                    self.expect_keyword(Keyword::Read)?;
                    IsolationLevel::Snapshot
                },
                _ => IsolationLevel::Serializable,
            },
        )
    }

    fn parse_ident(&mut self) -> ParserResult<String> {
        self.next_token().and_then(|token| match token {
            Token::Identifier(identifier) => Ok(identifier),
            Token::Keyword(keyword) if keyword.is_unreserved() => Ok(keyword.as_name()),
            _ => Err(self.error(ErrorKind::Expected {
                expected: Token::Identifier(Default::default()),
                found: token,
            })),
        })
    }

    pub(crate) fn parse_expr(&mut self, precedence: Option<u8>) -> ParserResult<Expression> {
        let mut expr = self.parse_pref()?;
        let mut next = self.get_precedence();

        while precedence.unwrap_or(0) < next {
            expr = self.parse_infix(expr, next)?;
            next = self.get_precedence();
        }

        Ok(expr)
    }

    fn parse_by_separated_keyword<T>(
        &mut self,
        keyword: Keyword,
        mut parse_elem: impl FnMut(&mut Self) -> ParserResult<T>,
    ) -> ParserResult<Vec<T>> {
        if !self.consume_optional(Token::Keyword(keyword)) {
            return Ok(vec![]);
        }
        self.expect_keyword(Keyword::By)?;
        self.parse_separated_tokens(|p| parse_elem(p), false)
    }

    fn parse_infix(&mut self, left: Expression, precedence: u8) -> ParserResult<Expression> {
        let op = match self.next_token()? {
            Token::Eq => BinaryOperator::Eq,
            Token::Neq => BinaryOperator::Neq,
            Token::Gt => BinaryOperator::Gt,
            Token::GtEq => BinaryOperator::GtEq,
            Token::Lt => BinaryOperator::Lt,
            Token::LtEq => BinaryOperator::LtEq,
            Token::Plus => BinaryOperator::Plus,
            Token::Minus => BinaryOperator::Minus,
            Token::Mul => BinaryOperator::Mul,
            Token::Div => BinaryOperator::Div,
            Token::Percent => BinaryOperator::Modulo,
            Token::Keyword(Keyword::And) => BinaryOperator::And,
            Token::Keyword(Keyword::Or) => BinaryOperator::Or,
            Token::Keyword(Keyword::Like) => BinaryOperator::Like,
            Token::Keyword(Keyword::Between) => {
                let start = self.parse_expr(Some(precedence))?;
                self.expect_keyword(Keyword::And)?;
                let end = self.parse_expr(Some(precedence))?;

                return Ok(Expression::BinaryOperation {
                    operator: BinaryOperator::And,
                    left: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::GtEq,
                        left: Box::new(left.clone()),
                        right: Box::new(start),
                    }),
                    right: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::LtEq,
                        left: Box::new(left),
                        right: Box::new(end),
                    }),
                });
            },
            Token::Keyword(Keyword::In) => {
                self.next_token()?;
                if self.consume_optional(Token::Keyword(Keyword::Select)) {
                    let args = vec![left, self.parse_subquery()?];
                    return Ok(Expression::Function { func: Function::In, args });
                }

                let values =
                    self.parse_separated_tokens(|parser| parser.parse_expr(None), false)?;
                self.expect_token(Token::RightParen)?;

                let args = std::iter::once(left).chain(values).collect();
                return Ok(Expression::Function { func: Function::In, args });
            },
            Token::Keyword(Keyword::Not) => {
                let negated = self.parse_infix(left, precedence)?;
                return Ok(Expression::UnaryOperation {
                    operator: UnaryOperator::Not,
                    expr: Box::new(negated),
                });
            },
            Token::Keyword(Keyword::Is) => {
                let negated = self.consume_optional(Token::Keyword(Keyword::Not));
                self.expect_keyword(Keyword::Null)?;

                return Ok(Expression::IsNull { expr: Box::new(left), negated });
            },
            token => {
                return Err(self.error(ErrorKind::Expected { expected: Token::Eq, found: token }));
            },
        };

        let right = Box::new(self.parse_expr(Some(precedence))?);
        Ok(Expression::BinaryOperation { operator: op, left: Box::new(left), right })
    }

    fn parse_pref(&mut self) -> ParserResult<Expression> {
        let token = match self.next_token()? {
            Token::Keyword(keyword) if keyword.is_unreserved() => Token::Identifier(keyword.as_name()),
            token => token,
        };
        match token {
            Token::String(string) => Ok(Expression::Value(Value::String(string.into()))),
            Token::Keyword(Keyword::True) => Ok(Expression::Value(Value::Boolean(true))),
            Token::Keyword(Keyword::False) => Ok(Expression::Value(Value::Boolean(false))),
            Token::Keyword(Keyword::Null) => Ok(Expression::Value(Value::Null)),
            Token::LeftBrace => {
                let json_text = self.parse_json_object()?;
                let jsonb =
                    json::from_value_to_jsonb(&Value::String(json_text.into()), json::Conv::Strict)
                        .map_err(|e| self.error(ErrorKind::FormatError(e.to_string())))?;
                Ok(Expression::Value(Value::Blob(jsonb.data().into())))
            },
            Token::LeftBracket => {
                let json_text = self.parse_json_array()?;
                let jsonb =
                    json::from_value_to_jsonb(&Value::String(json_text.into()), json::Conv::Strict)
                        .map_err(|e| self.error(ErrorKind::FormatError(e.to_string())))?;
                Ok(Expression::Value(Value::Blob(jsonb.data().into())))
            },
            Token::Keyword(Keyword::Interval) => self.parse_interval(),
            Token::Keyword(keyword @ (Keyword::Date | Keyword::Timestamp | Keyword::Time)) => {
                self.parse_datetime(keyword)
            },
            Token::Keyword(func) if func.is_function() => {
                self.expect_token(Token::LeftParen)?;
                self.parse_func(func)
            },

            Token::Mul => Ok(Expression::Wildcard),
            Token::LeftParen => match self.consume_optional(Token::Keyword(Keyword::Select)) {
                true => self.parse_subquery(),
                _ => {
                    let expr = self.parse_expr(None)?;
                    self.expect_token(Token::RightParen)?;

                    Ok(Expression::Nested(Box::new(expr)))
                },
            },
            Token::Keyword(Keyword::Exists) => {
                self.expect_token(Token::LeftParen)?;
                self.expect_keyword(Keyword::Select)?;
                Ok(Expression::Function {
                    func: Function::Exists,
                    args: vec![self.parse_subquery()?],
                })
            },
            Token::Number(number) => match number.contains(".") {
                true => Ok(Expression::Value(Value::Float(
                    number.parse::<f64>().map_err(|_| self.error(ErrorKind::UnexpectedEof))?,
                ))),
                // an integer beyond 64 bits is a NUMERIC
                _ => match number.parse() {
                    Ok(integer) => Ok(Expression::Value(Value::Number(integer))),
                    Err(_) => Ok(Expression::Value(Value::Numeric(
                        number.parse().map_err(|_| self.error(ErrorKind::UnexpectedEof))?,
                    ))),
                },
            },
            Token::Identifier(identifier) => {
                let mut segments = Vec::new();

                loop {
                    if self.consume_optional(Token::Dot) {
                        if self.consume_optional(Token::Mul) {
                            segments.push(PathSegment::Wildcard);
                            continue;
                        }

                        let key = self.parse_ident()?;
                        segments.push(PathSegment::Key(key));
                        continue;
                    }

                    if self.consume_optional(Token::LeftBracket) {
                        let token = self.next_token()?;
                        match token {
                            Token::Number(n) if !n.contains('.') => {
                                let idx = n.parse::<isize>().map_err(|_| {
                                    self.error(ErrorKind::Expected {
                                        expected: Token::Number(Default::default()),
                                        found: Token::Number(n),
                                    })
                                })?;
                                segments.push(PathSegment::Index(idx));
                            },
                            Token::Minus => {
                                let token = self.next_token()?;
                                let idx = match token {
                                    Token::Number(n) if !n.contains('.') => {
                                        n.parse::<isize>().map_err(|_| {
                                            self.error(ErrorKind::Expected {
                                                expected: Token::Number(Default::default()),
                                                found: Token::Number(n),
                                            })
                                        })?
                                    },
                                    other => {
                                        return Err(self.error(ErrorKind::Expected {
                                            expected: Token::Number(Default::default()),
                                            found: other,
                                        }));
                                    },
                                };
                                segments.push(PathSegment::Index(-idx));
                            },
                            Token::Mul => segments.push(PathSegment::Wildcard),
                            other => {
                                return Err(self.error(ErrorKind::Expected {
                                    expected: Token::Number(Default::default()),
                                    found: other,
                                }));
                            },
                        };

                        self.expect_token(Token::RightBracket)?;
                        continue;
                    }

                    break;
                }

                match segments.as_slice() {
                    [] => Ok(Expression::Identifier(identifier)),
                    [PathSegment::Key(column)] => Ok(Expression::QualifiedIdentifier {
                        table: identifier,
                        column: column.clone(),
                    }),
                    _ => Ok(Expression::Path { head: identifier, segments }),
                }
            },
            Token::Keyword(Keyword::Not) => {
                let expr = Box::new(self.parse_expr(Some(UNARY_NOT_OPERATOR))?);
                Ok(Expression::UnaryOperation { operator: UnaryOperator::Not, expr })
            },
            Token::Keyword(Keyword::Case) => self.parse_case(),
            Token::Keyword(Keyword::Cast) => {
                self.expect_token(Token::LeftParen)?;
                let expr = self.parse_expr(None)?;
                self.expect_keyword(Keyword::As)?;
                let target = self.parse_type()?;
                self.expect_token(Token::RightParen)?;

                let target = Expression::Value(Value::Blob(crate::db::encode_type(target).into()));
                Ok(Expression::Function { func: Function::Cast, args: vec![expr, target] })
            },
            token @ (Token::Minus | Token::Plus) => {
                let op = match token {
                    Token::Minus => UnaryOperator::Minus,
                    Token::Plus => UnaryOperator::Plus,
                    _ => unsafe { std::hint::unreachable_unchecked() },
                };

                let expr = Box::new(self.parse_expr(Some(UNARY_ARITHMETIC_OPERATOR))?);
                Ok(Expression::UnaryOperation { operator: op, expr })
            },
            token => Err(self.error(ErrorKind::ExpectedOneOf {
                expected: vec![
                    Token::Identifier(Default::default()),
                    Token::Number(Default::default()),
                    Token::String(Default::default()),
                    Token::Mul,
                    Token::Minus,
                    Token::Plus,
                    Token::LeftBrace,
                    Token::LeftBracket,
                    Token::LeftParen,
                ],
                found: token,
            })),
        }
    }

    fn json_escape(s: &str) -> String {
        let mut out = String::with_capacity(s.len() + 2);

        for ch in s.chars() {
            match ch {
                '\\' => out.push_str("\\\\"),
                '"' => out.push_str("\\\""),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                other => out.push(other),
            }
        }

        out
    }

    fn parse_json_value(&mut self) -> ParserResult<String> {
        Ok(match self.next_token()? {
            Token::LeftBrace => self.parse_json_object()?,
            Token::LeftBracket => self.parse_json_array()?,
            Token::String(s) => format!("\"{}\"", Self::json_escape(&s)),
            Token::Number(n) => n,
            Token::Keyword(Keyword::True) => "true".into(),
            Token::Keyword(Keyword::False) => "false".into(),
            Token::Keyword(Keyword::Null) => "null".into(),
            other => {
                return Err(self.error(ErrorKind::ExpectedOneOf {
                    expected: vec![
                        Token::LeftBrace,
                        Token::LeftBracket,
                        Token::String(Default::default()),
                        Token::Number(Default::default()),
                        Token::Keyword(Keyword::True),
                        Token::Keyword(Keyword::False),
                        Token::Keyword(Keyword::Null),
                    ],
                    found: other,
                }));
            },
        })
    }

    fn parse_json_object(&mut self) -> ParserResult<String> {
        let mut out = String::from("{");

        if self.consume_optional(Token::RightBrace) {
            out.push('}');
            return Ok(out);
        }

        loop {
            let key = match self.next_token()? {
                Token::Identifier(id) => id,
                Token::String(s) => s,
                Token::Keyword(k) if k.is_unreserved() => k.as_name(),
                Token::Keyword(k) => k.to_string(),
                other => {
                    return Err(self.error(ErrorKind::ExpectedOneOf {
                        expected: vec![
                            Token::Identifier(Default::default()),
                            Token::String(Default::default()),
                            Token::Keyword(Keyword::None),
                        ],
                        found: other,
                    }));
                },
            };

            self.expect_token(Token::Colon)?;
            let value = self.parse_json_value()?;

            out.push('"');
            out.push_str(&Self::json_escape(&key));
            out.push_str("\":");
            out.push_str(&value);

            if self.consume_optional(Token::Comma) {
                out.push(',');
                continue;
            }

            self.expect_token(Token::RightBrace)?;
            out.push('}');

            return Ok(out);
        }
    }

    fn parse_json_array(&mut self) -> ParserResult<String> {
        let mut out = String::from("[");

        if self.consume_optional(Token::RightBracket) {
            out.push(']');
            return Ok(out);
        }

        loop {
            let value = self.parse_json_value()?;
            out.push_str(&value);

            if self.consume_optional(Token::Comma) {
                out.push(',');
                continue;
            }

            self.expect_token(Token::RightBracket)?;
            out.push(']');

            return Ok(out);
        }
    }

    fn parse_closing_expr(&mut self) -> ParserResult<Expression> {
        let pref = self.parse_expr(None)?;
        self.expect_token(Token::RightParen)?;

        Ok(pref)
    }

    fn parse_col(&mut self) -> ParserResult<Column> {
        let name = self.parse_ident()?;
        let (data_type, type_def) = self.parse_type_and_def()?;

        let options = [
            Keyword::Primary,
            Keyword::Unique,
            Keyword::Nullable,
            Keyword::Null,
            Keyword::Not,
            Keyword::Default,
            Keyword::Check,
            Keyword::References,
        ];
        let mut constraints = Vec::new();

        loop {
            let constraint = match self.consume_one(&options).as_optional() {
                Some(Keyword::Primary) => {
                    self.expect_keyword(Keyword::Key)?;
                    Constraint::PrimaryKey
                },
                Some(Keyword::Unique) => Constraint::Unique,
                Some(Keyword::Nullable | Keyword::Null) => Constraint::Nullable,
                Some(Keyword::Not) => {
                    self.expect_keyword(Keyword::Null)?;
                    continue;
                },
                Some(Keyword::Default) => Constraint::Default(self.parse_expr(None)?),
                Some(Keyword::Check) => {
                    self.expect_token(Token::LeftParen)?;
                    Constraint::Check(self.parse_closing_expr()?)
                },
                Some(Keyword::References) => self.parse_references()?,
                Some(_) => unreachable!(),
                _ => break,
            };
            constraints.push(constraint);
        }

        Ok(Column { name, data_type, constraints, type_def })
    }

    /// `table(column) [ON DELETE CASCADE | RESTRICT | NO ACTION]`, after `REFERENCES`
    fn parse_references(&mut self) -> ParserResult<Constraint> {
        let table = self.parse_ident()?;
        self.expect_token(Token::LeftParen)?;
        let column = self.parse_ident()?;
        self.expect_token(Token::RightParen)?;

        let mut on_delete = OnDelete::Restrict;
        if self.consume_optional(Token::Keyword(Keyword::On)) {
            self.expect_keyword(Keyword::Delete)?;
            on_delete =
                match self.expect_one(&[Keyword::Cascade, Keyword::Restrict, Keyword::No])? {
                    Keyword::Cascade => OnDelete::Cascade,
                    Keyword::Restrict => OnDelete::Restrict,
                    _ => {
                        self.expect_keyword(Keyword::Action)?;
                        OnDelete::Restrict
                    },
                };
        }

        Ok(Constraint::References { table, column, on_delete })
    }

    /// the parenthesised elements of `CREATE TABLE`: columns and table constraints, which are
    /// attached to columns (a `CHECK` to the first, a `FOREIGN KEY` to its column)
    pub(super) fn parse_table_columns(&mut self) -> ParserResult<Vec<Column>> {
        enum Element {
            Column(Column),
            Check(Expression),
            ForeignKey(String, Constraint),
        }

        let elements = self.parse_separated_tokens(
            |parser| match parser.consume_one(&[Keyword::Check, Keyword::Foreign]) {
                Keyword::Check => {
                    parser.expect_token(Token::LeftParen)?;
                    Ok(Element::Check(parser.parse_closing_expr()?))
                },
                Keyword::Foreign => {
                    parser.expect_keyword(Keyword::Key)?;
                    parser.expect_token(Token::LeftParen)?;
                    let column = parser.parse_ident()?;
                    parser.expect_token(Token::RightParen)?;
                    parser.expect_keyword(Keyword::References)?;
                    Ok(Element::ForeignKey(column, parser.parse_references()?))
                },
                _ => parser.parse_col().map(Element::Column),
            },
            true,
        )?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();
        for element in elements {
            match element {
                Element::Column(column) => columns.push(column),
                Element::Check(expr) => constraints.push((None, Constraint::Check(expr))),
                Element::ForeignKey(column, references) => {
                    constraints.push((Some(column), references))
                },
            }
        }

        for (target, constraint) in constraints {
            let column = match &target {
                None => columns.first_mut(),
                Some(name) => columns.iter_mut().find(|column| column.name == *name),
            };
            match column {
                Some(column) => column.constraints.push(constraint),
                _ => {
                    let name = target.unwrap_or_default();
                    let msg = format!("no column {name} to constrain");
                    return Err(self.error(ErrorKind::FormatError(msg)));
                },
            }
        }

        Ok(columns)
    }

    fn parse_type(&mut self) -> ParserResult<Type> {
        match self.expect_one(&Self::supported_types())? {
            int if Self::is_integer(&int) => {
                let unsigned = self.consume_optional(Token::Keyword(Keyword::Unsigned));

                match (int, unsigned) {
                    (Keyword::SmallInt, true) => Ok(Type::UnsignedSmallInt),
                    (Keyword::SmallInt, false) => Ok(Type::SmallInt),
                    (Keyword::Int, true) => Ok(Type::UnsignedInteger),
                    (Keyword::Int, false) => Ok(Type::Integer),
                    (Keyword::BigInt, true) => Ok(Type::UnsignedBigInteger),
                    (Keyword::BigInt, false) => Ok(Type::BigInteger),
                    (Keyword::SmallSerial, _) => Ok(Type::SmallSerial),
                    (Keyword::Serial, _) => Ok(Type::Serial),
                    (Keyword::BigSerial, _) => Ok(Type::BigSerial),
                    (Keyword::Uuid, _) => Ok(Type::Uuid),
                    _ => unreachable!("unknown integer"),
                }
            },
            Keyword::Varchar => {
                self.expect_token(Token::LeftParen)?;

                let len = match self.next_token()? {
                    Token::Number(number) => number.parse().map_err(|_| {
                        // TODO: add correct error to incorrect varchar length
                        self.error(ErrorKind::UnexpectedEof)
                    })?,
                    token => {
                        return Err(self.error(ErrorKind::Expected {
                            expected: Token::Number(Default::default()),
                            found: token,
                        }))?;
                    },
                };

                self.expect_token(Token::RightParen)?;
                Ok(Type::Varchar(len))
            },
            Keyword::Double => {
                self.expect_keyword(Keyword::Precision)?;
                Ok(Type::DoublePrecision)
            },
            Keyword::Numeric => self.parse_numeric(),
            Keyword::Real => Ok(Type::Real),
            Keyword::Bool => Ok(Type::Boolean),
            Keyword::Timestamp => Ok(Type::DateTime),
            Keyword::Date => Ok(Type::Date),
            Keyword::Time => Ok(Type::Time),
            Keyword::Interval => Ok(Type::Interval),
            Keyword::Text => Ok(Type::Text),
            Keyword::Jsonb => Ok(Type::Jsonb),
            keyword => unreachable!("unexpected column token: {keyword}"),
        }
    }

    fn parse_type_and_def(&mut self) -> ParserResult<(Type, Option<Vec<String>>)> {
        if let Some(Ok(Token::String(_))) = self.peek_token() {
            let mut variants = Vec::new();
            if let Token::String(str) = self.next_token()? {
                variants.push(str);
            }

            while self.consume_optional(Token::Pipe) {
                match self.next_token()? {
                    Token::String(str) => variants.push(str),
                    t => {
                        return Err(self.error(ErrorKind::Expected {
                            expected: Token::String(String::new()),
                            found: t,
                        }));
                    },
                }
            }

            return Ok((Type::Enum(0), Some(variants)));
        }

        let typ = self.parse_type()?;
        Ok((typ, None))
    }

    fn parse_interval(&mut self) -> ParserResult<Expression> {
        // either a string (e.g., '30 days') or an identifier
        let value = match self.next_token()? {
            Token::String(s) => s,
            Token::Identifier(ident) => ident,
            token => {
                return Err(self.error(ErrorKind::Expected {
                    expected: Token::String(String::new()),
                    found: token,
                }));
            },
        };

        let interval = Interval::from_str(value.as_str())
            .map_err(|e| self.error(ErrorKind::FormatError(e.to_string())))?;

        Ok(Expression::Value(Value::Interval(interval)))
    }

    fn parse_numeric(&mut self) -> ParserResult<Type> {
        Ok(match self.consume_optional(Token::LeftParen) {
            true => {
                let Some(precision) = self.parse_usize()? else {
                    let found = self.peek_token().and_then(|res| res.ok()).cloned();
                    return Err(self.error(ErrorKind::Expected {
                        expected: Token::Number(String::new()),
                        found: found.unwrap(),
                    }));
                };

                let scale = self
                    .consume_optional(Token::Comma)
                    .then(|| self.parse_usize())
                    .and_then(|result| result.ok())
                    .flatten()
                    .unwrap_or(0);

                self.expect_token(Token::RightParen)?;
                Type::Numeric(precision, scale)
            },

            _ => Type::Numeric(NUMERIC_ANY, NUMERIC_ANY),
        })
    }

    fn parse_assign(&mut self) -> ParserResult<Assignment> {
        let identifier = self.parse_ident()?;
        self.expect_token(Token::Eq)?;
        let value = self.parse_expr(None)?;

        Ok(Assignment { identifier, value })
    }

    fn parse_join(&mut self) -> ParserResult<Option<JoinClause>> {
        if self.consume_optional(Token::Keyword(Keyword::Cross)) {
            self.expect_keyword(Keyword::Join)?;
            let table = self.parse_ident()?;
            let table = TableRef::new(table, self.parse_alias()?);
            let on = Expression::Value(Value::Boolean(true));
            return Ok(Some(JoinClause { join_type: JoinType::Inner, table, on }));
        }

        let join_type: JoinType =
            self.consume_one(&[Keyword::Full, Keyword::Left, Keyword::Right]).into();

        if !self.consume_optional(Token::Keyword(Keyword::Join)) {
            return Ok(None);
        };

        let table = self.parse_ident()?;
        let table_alias = self.parse_alias()?;
        let table = TableRef::new(table, table_alias);

        self.expect_keyword(Keyword::On)?;
        let on = self.parse_expr(None)?;

        Ok(Some(JoinClause { join_type, table, on }))
    }

    fn parse_usize_by_keyword(&mut self, keyword: Keyword) -> ParserResult<Option<usize>> {
        if !self.consume_optional(Token::Keyword(keyword)) {
            return Ok(None);
        }

        self.parse_usize()?
            .ok_or(ParserError {
                kind: ErrorKind::Expected {
                    expected: Token::Number(String::new()),
                    found: self.peek_token().and_then(|res| res.ok()).cloned().unwrap(),
                },
                location: self.location,
                input: self.input.into(),
            })
            .map(Some)
    }

    fn parse_usize(&mut self) -> ParserResult<Option<usize>> {
        if let Some(Ok(Token::Number(_))) = self.peek_token() {
            let token = self.next_token()?;
            if let Token::Number(s) = token {
                let n = s.parse::<usize>().map_err(|_| {
                    self.error(ErrorKind::FormatError(format!("Invalid number: {s}")))
                })?;
                return Ok(Some(n));
            }
        }

        Ok(None)
    }

    fn parse_alias(&mut self) -> ParserResult<Option<String>> {
        match self.consume_optional(Token::Keyword(Keyword::As)) {
            true => self.parse_ident().map(Some),
            false => Ok(None),
        }
    }

    fn parse_returning(&mut self) -> ParserResult<Vec<Expression>> {
        match self.consume_optional(Token::Keyword(Keyword::Returning)) {
            false => Ok(vec![]),
            _ => self.parse_separated_tokens(
                |p| {
                    let expr = p.parse_expr(None)?;
                    Ok(match p.consume_optional(Token::Keyword(Keyword::As)) {
                        false => expr,
                        _ => Expression::Alias { alias: p.parse_ident()?, expr: Box::new(expr) },
                    })
                },
                false,
            ),
        }
    }

    /// Operator's precedence from the [PostgreSQL documentation](https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-PRECEDENCE)
    fn get_precedence(&mut self) -> u8 {
        let Some(Ok(token)) = self.peek_token() else {
            return 0;
        };

        match token {
            Token::Keyword(Keyword::Or) => 5,
            Token::Keyword(Keyword::And) => 10,
            Token::Eq
            | Token::Neq
            | Token::Gt
            | Token::GtEq
            | Token::Lt
            | Token::LtEq
            | Token::Keyword(Keyword::Between)
            | Token::Keyword(Keyword::In)
            | Token::Keyword(Keyword::Like)
            | Token::Keyword(Keyword::Is) => 20,
            Token::Keyword(Keyword::Not) => match self.peek_second() {
                Some(Token::Keyword(Keyword::In | Keyword::Between | Keyword::Like)) => 20,
                _ => 0,
            },
            Token::Plus | Token::Minus => 30,
            Token::Mul | Token::Div | Token::Percent => 40,
            _ => 0,
        }
    }

    fn parse_separated_tokens<T>(
        &mut self,
        mut sub_parser: impl FnMut(&mut Self) -> ParserResult<T>,
        parentheses: bool,
    ) -> ParserResult<Vec<T>> {
        if parentheses {
            self.expect_token(Token::LeftParen)?;
        }

        let mut parsed = vec![sub_parser(self)?];
        while self.consume_optional(Token::Comma) {
            parsed.push(sub_parser(self)?);
        }

        if parentheses {
            self.expect_token(Token::RightParen)?;
        }

        Ok(parsed)
    }

    fn parse_from_and_where(&mut self) -> ParserResult<(String, Option<Expression>)> {
        let from = self.parse_ident()?;
        let r#where = match self.consume_optional(Token::Keyword(Keyword::Where)) {
            true => Some(self.parse_expr(None)?),
            false => None,
        };

        Ok((from, r#where))
    }

    fn parse_datetime(&mut self, keyword: Keyword) -> ParserResult<Expression> {
        use crate::sql::statement::Temporal;

        let value_str = self.parse_ident()?;

        let parsed = Temporal::try_from(value_str.as_str()).map_err(|e| {
            self.error(ErrorKind::FormatError(format!("Invalid date/time format: {e:?}")))
        })?;

        let valid_for_keyword = matches!(
            (keyword, parsed),
            (Keyword::Date, Temporal::Date(_))
                | (Keyword::Time, Temporal::Time(_))
                | (Keyword::Timestamp, Temporal::DateTime(_))
        );

        if !valid_for_keyword {
            return Err(self.error(ErrorKind::FormatError(format!(
                "Invalid {keyword} format: {value_str} - parsed as {parsed:?}",
            ))));
        }

        Ok(Expression::Value(Value::String(value_str.into())))
    }

    fn parse_func(&mut self, function: Keyword) -> ParserResult<Expression> {
        match function {
            Keyword::Substring => {
                let expr = self.parse_expr(None)?;
                let from = match self.consume_optional(Token::Keyword(Keyword::From)) {
                    true => Some(self.parse_expr(None)?),
                    _ => None,
                };
                let r#for = match self.consume_optional(Token::Keyword(Keyword::For)) {
                    true => Some(self.parse_expr(None)?),
                    _ => None,
                };

                if r#for.is_none() && from.is_none() {
                    return Err(self.error(ErrorKind::FormatError(
                        "SUBSTRING requires at least FROM or FOR arguments".into(),
                    )));
                }

                self.expect_token(Token::RightParen)?;
                Ok(Expression::Function {
                    func: Function::Substring,
                    args: vec![
                        expr,
                        from.unwrap_or(Expression::Value(Value::Number(0))),
                        r#for.unwrap_or(Expression::Value(Value::Number(-1))),
                    ],
                })
            },

            Keyword::Extract => {
                let kind_str = self.parse_ident()?;
                // TODO: this way we need to parse the kind here to check and when executing, but I
                // don't know any better way without changing the enums to a special case or just
                // making an execution error instead of a parsing, which isn't the ideal
                ExtractKind::try_from(kind_str.as_str()).or(Err(self.error(
                    ErrorKind::FormatError(format!(
                        "Unit '{kind_str}' is not recognised for date type"
                    )),
                )))?;

                self.expect_keyword(Keyword::From)?;
                let from = self.parse_expr(None)?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Function {
                    func: Function::Extract,
                    args: vec![Expression::Value(Value::String(kind_str.into())), from],
                })
            },

            keyword
                if matches!(
                    keyword,
                    Keyword::Concat | Keyword::Power | Keyword::Trunc | Keyword::Coalesce
                ) =>
            {
                let args = self.parse_separated_tokens(|p| p.parse_expr(None), false)?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Function { func: Function::try_from(&keyword).unwrap(), args })
            },
            Keyword::Ascii => self.parse_unary_func(Function::Ascii),
            Keyword::Position => {
                let needle = self.parse_pref()?;
                self.expect_keyword(Keyword::In)?;
                let haystack = self.parse_pref()?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Function { func: Function::Position, args: vec![needle, haystack] })
            },
            Keyword::Count if self.peek_token() == Some(Ok(&Token::Keyword(Keyword::Distinct))) => {
                self.next_token()?;
                self.parse_unary_func(Function::CountDistinct)
            },
            keyword if keyword.is_function() => {
                let value = self.parse_closing_expr()?;

                Ok(Expression::Function {
                    func: Function::try_from(&keyword).unwrap(),
                    args: vec![value],
                })
            },
            Keyword::Sum => self.parse_unary_func(Function::Sum),
            Keyword::Avg => self.parse_unary_func(Function::Avg),
            Keyword::Min => self.parse_unary_func(Function::Min),
            Keyword::Max => self.parse_unary_func(Function::Max),
            Keyword::TypeOf => self.parse_unary_func(Function::TypeOf),
            func => unreachable!("invalid function: {func}"),
        }
    }

    fn parse_unary_func(&mut self, func: Function) -> ParserResult<Expression> {
        let expr = self.parse_expr(None)?;
        self.expect_token(Token::RightParen)?;
        Ok(Expression::Function { func, args: vec![expr] })
    }

    fn peek_token(&mut self) -> Option<Result<&Token, &TokenizerError>> {
        self.skip_whitespaces();
        self.peek_token_stream()
    }

    /// a SELECT and the `UNION [ALL]` members after it, `SELECT` consumed: the last member's
    /// `ORDER BY`, `LIMIT` and `OFFSET` order and limit the whole union
    fn parse_union(&mut self) -> ParserResult<Select> {
        let mut head = Select::parse(self)?;
        loop {
            let operator = match self.peek_token() {
                Some(Ok(Token::Keyword(Keyword::Union))) => SetOperator::Union,
                Some(Ok(Token::Keyword(Keyword::Intersect))) => SetOperator::Intersect,
                Some(Ok(Token::Keyword(Keyword::Except))) => SetOperator::Except,
                _ => break,
            };
            self.next_token()?;
            let all = self.consume_optional(Token::Keyword(Keyword::All));
            self.expect_keyword(Keyword::Select)?;
            head.combined.push((operator, all, Select::parse(self)?));
        }

        let Some(last) = head.combined.last_mut().map(|(_, _, last)| last) else {
            return Ok(head);
        };
        let (order_by, limit, offset) =
            (std::mem::take(&mut last.order_by), last.limit.take(), last.offset.take());

        let ordered = |select: &Select| {
            !select.order_by.is_empty() || select.limit.is_some() || select.offset.is_some()
        };
        if ordered(&head) || head.combined.iter().any(|(_, _, member)| ordered(member)) {
            let msg = "ORDER BY, LIMIT and OFFSET of a set operation go after its last member";
            return Err(self.error(ErrorKind::FormatError(msg.into())));
        }

        (head.order_by, head.limit, head.offset) = (order_by, limit, offset);
        Ok(head)
    }

    /// the rest of `(SELECT ...)`, its opening parenthesis and `SELECT` consumed
    fn parse_subquery(&mut self) -> ParserResult<Expression> {
        let select = self.parse_union()?;
        self.expect_token(Token::RightParen)?;
        Ok(Expression::Subquery(Box::new(select)))
    }

    /// `CASE [operand] WHEN .. THEN .. [ELSE ..] END`, `CASE` consumed
    fn parse_case(&mut self) -> ParserResult<Expression> {
        let operand = match self.peek_token() {
            Some(Ok(Token::Keyword(Keyword::When))) => None,
            _ => Some(self.parse_expr(None)?),
        };

        let mut args = Vec::new();
        while self.consume_optional(Token::Keyword(Keyword::When)) {
            let condition = self.parse_expr(None)?;
            self.expect_keyword(Keyword::Then)?;
            args.push(match &operand {
                Some(operand) => Expression::BinaryOperation {
                    operator: BinaryOperator::Eq,
                    left: Box::new(operand.clone()),
                    right: Box::new(condition),
                },
                None => condition,
            });
            args.push(self.parse_expr(None)?);
        }

        if args.is_empty() {
            self.expect_keyword(Keyword::When)?;
        }

        args.push(match self.consume_optional(Token::Keyword(Keyword::Else)) {
            true => self.parse_expr(None)?,
            _ => Expression::Value(Value::Null),
        });
        self.expect_keyword(Keyword::End)?;

        Ok(Expression::Function { func: Function::Case, args })
    }

    /// the token after the next one, whitespace skipped
    fn peek_second(&mut self) -> Option<Token> {
        self.skip_whitespaces();
        let mut ahead = self.tokenizer.clone().skip(1);
        ahead.find_map(|token| match token {
            Ok(TokenWithLocation { token: Token::Whitespace(_), .. }) => None,
            Ok(token) => Some(Some(token.token)),
            _ => Some(None),
        })?
    }

    fn next_token(&mut self) -> ParserResult<Token> {
        self.skip_whitespaces();
        self.next_token_stream()
    }

    fn expect_token(&mut self, expected: Token) -> ParserResult<Token> {
        self.next_token().and_then(|token| match token.eq(&expected) {
            true => Ok(token),
            _ => Err(self.error(ErrorKind::Expected { expected, found: token })),
        })
    }

    fn expect_keyword(&mut self, expected: Keyword) -> ParserResult<Keyword> {
        self.expect_token(Token::Keyword(expected)).map(|_| expected)
    }

    fn skip_whitespaces(&mut self) {
        while let Some(Ok(Token::Whitespace(_))) = self.peek_token_stream() {
            let _ = self.next_token_stream();
        }
    }

    fn peek_token_stream(&mut self) -> Option<Result<&Token, &TokenizerError>> {
        self.tokenizer
            .peek()
            .map(|token| token.as_ref().map(TokenWithLocation::token_ref))
    }

    fn next_token_stream(&mut self) -> ParserResult<Token> {
        match self.tokenizer.peek() {
            None => Err(self.error(ErrorKind::UnexpectedEof)),
            _ => {
                let token = self.tokenizer.next().unwrap()?;
                self.location = token.location;
                Ok(token.token)
            },
        }
    }

    fn expect_one<'key, K>(&mut self, keywords: &'key K) -> ParserResult<Keyword>
    where
        &'key K: IntoIterator<Item = &'key Keyword>,
    {
        match self.consume_one(keywords) {
            Keyword::None => {
                let token = self.next_token()?;
                Err(self.error(ErrorKind::ExpectedOneOf {
                    expected: Self::tokens_from_keywords(keywords),
                    found: token,
                }))
            },
            keyword => Ok(keyword),
        }
    }

    /// Consumes the next token if it matches any of the given [keywords](Keyword).
    /// Returns the matched [`Keyword`] or [`Keyword::None`] if no match is found.
    fn consume_one<'key, K>(&mut self, keywords: &'key K) -> Keyword
    where
        &'key K: IntoIterator<Item = &'key Keyword>,
    {
        let mut consume_keyword =
            |op: &Keyword| -> bool { self.consume_optional(Token::Keyword(*op)) };

        *keywords.into_iter().find(|key| consume_keyword(key)).unwrap_or(&Keyword::None)
    }

    fn consume_optional(&mut self, optional: Token) -> bool {
        match self.peek_token() {
            Some(Ok(token)) if token == &optional => {
                let _ = self.next_token();
                true
            },
            _ => false,
        }
    }

    fn error(&self, kind: ErrorKind) -> ParserError {
        ParserError { kind, input: self.input.to_string(), location: self.location }
    }

    fn tokens_from_keywords<'key, K>(keywords: &'key K) -> Vec<Token>
    where
        &'key K: IntoIterator<Item = &'key Keyword>,
    {
        keywords.into_iter().map(From::from).collect()
    }

    const fn supported_statements() -> [Keyword; 15] {
        [
            Keyword::Set,
            Keyword::Select,
            Keyword::Create,
            Keyword::Update,
            Keyword::Insert,
            Keyword::Delete,
            Keyword::Drop,
            Keyword::Begin,
            Keyword::Rollback,
            Keyword::Checkpoint,
            Keyword::Savepoint,
            Keyword::Release,
            Keyword::Commit,
            Keyword::Explain,
            Keyword::Source,
        ]
    }

    const fn supported_types() -> [Keyword; 18] {
        [
            Keyword::SmallSerial,
            Keyword::Serial,
            Keyword::BigSerial,
            Keyword::SmallInt,
            Keyword::Int,
            Keyword::BigInt,
            Keyword::Real,
            Keyword::Double,
            Keyword::Uuid,
            Keyword::Bool,
            Keyword::Varchar,
            Keyword::Text,
            Keyword::Jsonb,
            Keyword::Time,
            Keyword::Date,
            Keyword::Timestamp,
            Keyword::Interval,
            Keyword::Numeric,
        ]
    }

    const fn is_integer(keyword: &Keyword) -> bool {
        matches!(
            keyword,
            Keyword::SmallInt
                | Keyword::Int
                | Keyword::BigInt
                | Keyword::SmallSerial
                | Keyword::Serial
                | Keyword::BigSerial
                | Keyword::Uuid
        )
    }
}

impl Display for ParserError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Parser error at line {} column {}: {}",
            self.location.line, self.location.col, self.kind
        )?;

        let whitespaces = match self.input.lines().nth(self.location.line.saturating_sub(1)) {
            Some(line) => {
                f.write_str(line)?;
                self.location.col.saturating_sub(1)
            },
            None => {
                let line = self.input.lines().last().unwrap();
                f.write_str(line)?;
                line.chars().count()
            },
        };

        write!(f, "\n{}^", String::from(" ").repeat(whitespaces))
    }
}

impl<'s> ErrorKind {
    fn expected_token_str(token: &'s Token) -> Cow<'s, str> {
        match token {
            Token::Identifier(_) => Cow::Borrowed("identifier"),
            Token::Number(_) => Cow::Borrowed("number"),
            Token::String(_) => Cow::Borrowed("string"),
            _ => Cow::Owned(token.to_string()),
        }
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorKind::FormatError(err) => f.write_str(err),
            ErrorKind::TokenizerError(err) => write!(f, "{err}"),
            ErrorKind::UnexpectedEof => write!(f, "Unexpected end of file"),
            ErrorKind::Expected { expected, found } => write!(
                f,
                "Expected '{}' but found '{found}' instead",
                ErrorKind::expected_token_str(expected)
            ),
            ErrorKind::ExpectedOneOf { expected, found } => {
                let mut one_of = String::new();

                one_of.push_str(&ErrorKind::expected_token_str(&expected[0]));
                (expected[1..expected.len() - 1]).iter().for_each(|token| {
                    one_of.push_str(", ");
                    one_of.push_str(&ErrorKind::expected_token_str(token));
                });

                if expected.len() > 1 {
                    one_of.push_str(" or ");
                    one_of.push_str(&ErrorKind::expected_token_str(&expected[expected.len() - 1]));
                }

                write!(f, "Expected {one_of} but found '{found}' instead")
            },
        }
    }
}

impl From<TokenizerError> for ParserError {
    fn from(TokenizerError { kind, location, input }: TokenizerError) -> Self {
        Self { input, kind: ErrorKind::TokenizerError(kind), location }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::statement::{Expression, OrderBy, OrderDirection, SelectBuilder};

    #[test]
    fn test_parse_select() {
        let input = "SELECT id, price FROM bills;";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select(
                Select::builder()
                    .from("bills")
                    .columns(vec![
                        Expression::Identifier("id".to_string()),
                        Expression::Identifier("price".to_string())
                    ])
                    .into()
            ))
        );
    }

    #[test]
    fn test_parse_select_where() {
        let input = "SELECT title, author FROM books WHERE author = 'Agatha Christie';";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select(
                SelectBuilder::default()
                    .from("books")
                    .columns(vec![
                        Expression::Identifier("title".to_string()),
                        Expression::Identifier("author".to_string())
                    ])
                    .r#where(Expression::BinaryOperation {
                        operator: BinaryOperator::Eq,
                        left: Box::new(Expression::Identifier("author".to_string())),
                        right: Box::new(Expression::Value(Value::String("Agatha Christie".into()))),
                    })
                    .into()
            )),
        );
    }

    #[test]
    fn test_parse_asterisk() {
        let input = "SELECT * FROM users;";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select(
                SelectBuilder::default().from("users").select(Expression::Wildcard).into()
            ))
        );
    }

    #[test]
    fn test_create_table() {
        let input = r#"
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name VARCHAR(255),
                age INTEGER,
                is_manager BOOLEAN
            );
        "#;

        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Table {
                name: "employees".to_string(),
                columns: vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("name", Type::Varchar(255)),
                    Column::new("age", Type::Integer),
                    Column::new("is_manager", Type::Boolean),
                ],
            }))
        );
    }

    #[test]
    fn test_create_table_with_unsigned_keys() {
        let sql = r#"
            CREATE TABLE companies (
                id INTEGER UNSIGNED PRIMARY KEY,
                name VARCHAR(50),
                address VARCHAR(255),
                phone VARCHAR(10)
            );
        "#;

        let statement = Parser::new(sql).parse_statement();
        assert_eq!(
            statement.unwrap(),
            Statement::Create(Create::Table {
                columns: vec![
                    Column::primary_key("id", Type::UnsignedInteger),
                    Column::new("name", Type::Varchar(50)),
                    Column::new("address", Type::Varchar(255)),
                    Column::new("phone", Type::Varchar(10))
                ],
                name: "companies".into()
            })
        );
    }

    #[test]
    fn test_update_where() {
        let sql = r#"
            UPDATE employees
            SET
                salary = salary * 2,
                bonus = 2000
            WHERE
                department = 'engineering'
                AND performance_score >= 80
                AND (years_of_service > 3 OR is_team_lead = TRUE);
        "#;
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Update(Update {
                table: "employees".to_string(),
                columns: vec![
                    Assignment {
                        identifier: "salary".to_string(),
                        value: Expression::BinaryOperation {
                            operator: BinaryOperator::Mul,
                            left: Box::new(Expression::Identifier("salary".to_string())),
                            right: Box::new(Expression::Value(Value::Number(2))),
                        },
                    },
                    Assignment {
                        identifier: "bonus".to_string(),
                        value: Expression::Value(Value::Number(2000)),
                    },
                ],
                r#where: Some(Expression::BinaryOperation {
                    operator: BinaryOperator::And,
                    left: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::And,
                        left: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("department".to_string())),
                            right: Box::new(Expression::Value(
                                Value::String("engineering".into(),)
                            )),
                        }),
                        right: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::GtEq,
                            left: Box::new(Expression::Identifier("performance_score".to_string())),
                            right: Box::new(Expression::Value(Value::Number(80))),
                        }),
                    }),
                    right: Box::new(Expression::Nested(Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::Or,
                        left: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Gt,
                            left: Box::new(Expression::Identifier("years_of_service".to_string())),
                            right: Box::new(Expression::Value(Value::Number(3))),
                        }),
                        right: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("is_team_lead".to_string())),
                            right: Box::new(Expression::Value(Value::Boolean(true))),
                        }),
                    })))
                }),
                returning: vec![],
            }))
        );
    }

    /// a function with no SQL spelling made its name panic the tokenizer
    #[test]
    fn a_word_naming_an_internal_function_is_an_identifier() {
        let statement = Parser::new("SELECT uuidv4 FROM t;").parse_statement().unwrap();
        let Statement::Select(select) = statement else {
            panic!("a select")
        };
        assert_eq!(select.columns, [Expression::Identifier("uuidv4".into())]);
    }

    #[test]
    fn test_unique_index() {
        let sql = "CREATE UNIQUE INDEX content_idx ON pages(content);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Index {
                name: "content_idx".to_string(),
                table: "pages".to_string(),
                columns: vec!["content".to_string()],
                unique: true,
            }))
        );
    }

    #[test]
    fn test_insert() {
        let sql = "INSERT INTO departments (id, name) VALUES (1, 'HR');";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Insert(Insert {
                into: "departments".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                values: vec![vec![
                    // outer vec = rows, inner vec = expressions in that row
                    Expression::Value(Value::Number(1)),
                    Expression::Value(Value::String("HR".into()))
                ]],
                returning: vec![],
            }))
        )
    }

    #[test]
    fn test_multiple_statements() -> ParserResult<()> {
        let sql = r#"
            DROP TABLE bills;
            CREATE TABLE departments (
                id INTEGER UNSIGNED PRIMARY KEY,
                name VARCHAR(69)
            );
            SELECT * FROM employees;
        "#;
        let statements = Parser::new(sql).try_parse()?;

        assert_eq!(statements.len(), 3);
        assert_eq!(
            statements,
            vec![
                Statement::Drop(Drop::Table("bills".to_string())),
                Statement::Create(Create::Table {
                    name: "departments".to_string(),
                    columns: vec![
                        Column::primary_key("id", Type::UnsignedInteger),
                        Column::new("name", Type::Varchar(69)),
                    ],
                }),
                Statement::Select(
                    Select::builder().from("employees").select(Expression::Wildcard).into()
                )
            ]
        );

        Ok(())
    }

    #[test]
    fn test_parse_time() {
        let sql = "SELECT * FROM schedule WHERE start_time < '12:00:00';";
        let statement = Parser::new(sql).parse_statement();
        // the parser should parse the time as a string
        // cause its to early in the pipeline to cast it

        assert_eq!(
            statement,
            Ok(Statement::Select(
                Select::builder()
                    .from("schedule")
                    .select(Expression::Wildcard)
                    .r#where(Expression::BinaryOperation {
                        operator: BinaryOperator::Lt,
                        left: Box::new(Expression::Identifier("start_time".to_string())),
                        right: Box::new(Expression::Value(Value::String("12:00:00".into()))),
                    })
                    .into()
            ))
        );
    }

    #[test]
    fn test_parse_date() {
        let sql = r#"
            CREATE TABLE events (
            event_id INTEGER PRIMARY KEY,
            event_name VARCHAR(100),
            event_date DATE
        );
        "#;
        let statement = Parser::new(sql).parse_statement();
        // here, however, the parser should parse the date as a date
        // because it was an explicit date type in the sql

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Table {
                name: "events".into(),
                columns: vec![
                    Column::primary_key("event_id", Type::Integer),
                    Column::new("event_name", Type::Varchar(100)),
                    Column::new("event_date", Type::Date),
                ]
            }))
        )
    }

    #[test]
    fn test_expected_one() {
        let sql = "DROP VALUES employees";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Err(ParserError {
                input: sql.into(),
                location: Location::new(1, 6),
                kind: ErrorKind::ExpectedOneOf {
                    expected: vec![
                        Token::Keyword(Keyword::Database),
                        Token::Keyword(Keyword::Table)
                    ],
                    found: Token::Keyword(Keyword::Values)
                }
            })
        )
    }

    #[test]
    fn test_invalid_type() {
        let sql = "CREATE TABLE employees (id SOMETHING, name VARCHAR)";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Err(ParserError {
                input: sql.into(),
                location: Location::new(1, 28),
                kind: ErrorKind::ExpectedOneOf {
                    expected: Parser::tokens_from_keywords(&Parser::supported_types()),
                    found: Token::Identifier("SOMETHING".into())
                },
            })
        )
    }

    #[test]
    fn test_missing_parenthesis() {
        let sql = "CREATE TABLE employees id INTEGER, name VARCHAR(255);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Err(ParserError {
                input: sql.into(),
                location: Location::new(1, 24),
                kind: ErrorKind::Expected {
                    expected: Token::LeftParen,
                    found: Token::Identifier("id".into())
                }
            })
        )
    }

    #[test]
    fn test_parse_small_int() {
        let sql = r#"
        CREATE TABLE books (
            book_id INTEGER PRIMARY KEY,
            title VARCHAR (255),
            pages SMALLINT
        );"#;

        let statement = Parser::new(sql).parse_statement().unwrap();

        assert_eq!(
            statement,
            Statement::Create(Create::Table {
                name: "books".into(),
                columns: vec![
                    Column::primary_key("book_id", Type::Integer),
                    Column::new("title", Type::Varchar(255)),
                    Column::new("pages", Type::SmallInt)
                ]
            })
        )
    }

    #[test]
    fn test_create_table_with_serials() {
        let sql = r#"CREATE TABLE cursed_items (
            item_id BIGSERIAL PRIMARY KEY,
            name VARCHAR(255),
            darkness_level SMALLINT UNSIGNED,
            soul_count BIGINT UNSIGNED
        );"#;

        let statement = Parser::new(sql).parse_statement().unwrap();
        assert_eq!(
            statement,
            Statement::Create(Create::Table {
                name: "cursed_items".into(),
                columns: vec![
                    Column::primary_key("item_id", Type::BigSerial),
                    Column::new("name", Type::Varchar(255)),
                    Column::new("darkness_level", Type::UnsignedSmallInt),
                    Column::new("soul_count", Type::UnsignedBigInteger)
                ]
            })
        )
    }

    #[test]
    fn test_missing_identifier() {
        let sql = "INSERT INTO 1 VALUES (2);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Err(ParserError {
                location: Location::new(1, 13),
                input: sql.into(),
                kind: ErrorKind::Expected {
                    expected: Token::Identifier(Default::default()),
                    found: Token::Number("1".into()),
                }
            })
        )
    }

    #[test]
    fn test_invalid_varchar_len() {
        let sql = "CREATE TABLE employees (id INTEGER, name VARCHAR(SOMETHING));";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Err(ParserError {
                location: Location::new(1, 50),
                input: sql.into(),
                kind: ErrorKind::Expected {
                    expected: Token::Number(Default::default()),
                    found: Token::Identifier("SOMETHING".into()),
                }
            })
        )
    }

    #[test]
    fn test_parse_real_and_double() {
        let sql = r#"
            CREATE TABLE weather_data (
                reading_id SERIAL PRIMARY KEY,
                temperature REAL,
                humidity DOUBLE PRECISION
            );
        "#;
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Table {
                name: "weather_data".into(),
                columns: vec![
                    Column::primary_key("reading_id", Type::Serial),
                    Column::new("temperature", Type::Real),
                    Column::new("humidity", Type::DoublePrecision)
                ]
            }))
        )
    }

    #[test]
    fn test_insert_real_and_double() {
        let sql = r#"
            INSERT INTO scientific_data (precise_temperature, co2_levels, measurement_time)
            VALUES
                (23.456789, 415.123456789, '2024-02-03 10:00:00'),
                (20.123456, 417.123789012, '2024-02-03 11:00:00'),
                (22.789012, 418.456123789, '2024-02-03 12:00:00');
        "#;
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Insert(Insert {
                into: "scientific_data".into(),
                columns: vec![
                    "precise_temperature".into(),
                    "co2_levels".into(),
                    "measurement_time".into()
                ],
                values: vec![
                    vec![
                        Expression::Value(Value::Float(23.456789)),
                        Expression::Value(Value::Float(415.123456789)),
                        Expression::Value(Value::String("2024-02-03 10:00:00".into())),
                    ],
                    vec![
                        Expression::Value(Value::Float(20.123456)),
                        Expression::Value(Value::Float(417.123789012)),
                        Expression::Value(Value::String("2024-02-03 11:00:00".into()))
                    ],
                    vec![
                        Expression::Value(Value::Float(22.789012)),
                        Expression::Value(Value::Float(418.456123789)),
                        Expression::Value(Value::String("2024-02-03 12:00:00".into()))
                    ]
                ],
                returning: vec![],
            }))
        )
    }

    #[test]
    fn test_invalid_between() {
        let sql = "SELECT amount FROM payments WHERE payment_id BETWEEN 24;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap_err(),
            ParserError {
                input: sql.to_string(),
                location: Location::new(1, 56),
                kind: ErrorKind::Expected {
                    expected: Token::Keyword(Keyword::And),
                    found: Token::Semicolon
                }
            }
        );
    }

    #[test]
    fn test_in_operator() {
        let sql = "SELECT film_id, title FROM film WHERE film_id IN (1, 2, 3);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("film")
                    .columns(vec![
                        Expression::Identifier("film_id".into()),
                        Expression::Identifier("title".into())
                    ])
                    .r#where(Expression::Function {
                        func: Function::In,
                        args: vec![
                            Expression::Identifier("film_id".to_string()),
                            Expression::Value(Value::Number(1)),
                            Expression::Value(Value::Number(2)),
                            Expression::Value(Value::Number(3)),
                        ],
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_not_in() {
        let sql = "SELECT title FROM films WHERE film_id NOT IN (1, 2);";
        let Ok(Statement::Select(select)) = Parser::new(sql).parse_statement() else {
            panic!("a select")
        };

        assert_eq!(
            select.r#where,
            Some(Expression::UnaryOperation {
                operator: UnaryOperator::Not,
                expr: Box::new(Expression::Function {
                    func: Function::In,
                    args: vec![
                        Expression::Identifier("film_id".into()),
                        Expression::Value(Value::Number(1)),
                        Expression::Value(Value::Number(2)),
                    ],
                }),
            })
        );
    }

    #[test]
    fn test_like_operator() {
        let sql = "SELECT name, last_name FROM customer WHERE name LIKE 'Jen%';";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("customer")
                    .columns(vec![
                        Expression::Identifier("name".into()),
                        Expression::Identifier("last_name".into())
                    ])
                    .r#where(Expression::BinaryOperation {
                        operator: BinaryOperator::Like,
                        left: Box::new(Expression::Identifier("name".into())),
                        right: Box::new(Expression::Value(Value::String("Jen%".into())))
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_uuid() {
        let sql = "CREATE TABLE contracts (id UUID PRIMARY KEY, name VARCHAR(30));";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Create(Create::Table {
                name: "contracts".into(),
                columns: vec![
                    Column::primary_key("id", Type::Uuid),
                    Column::new("name", Type::Varchar(30))
                ]
            })
        )
    }

    #[test]
    fn test_substring_func() {
        let sql = "SELECT SUBSTRING(name FROM 1 FOR 8) FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .select(Expression::Function {
                        func: Function::Substring,
                        args: vec![
                            Expression::Identifier("name".into()),
                            Expression::Value(Value::Number(1)),
                            Expression::Value(Value::Number(8)),
                        ]
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_ascii_func() {
        let sql = "SELECT name FROM users ORDER BY ASCII(name);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .select(Expression::Identifier("name".into()))
                    .order_by(OrderBy {
                        expr: Expression::Function {
                            func: Function::Ascii,
                            args: vec![Expression::Identifier("name".into())]
                        },
                        direction: Default::default(),
                    })
                    .from("users")
                    .into()
            )
        );
    }

    #[test]
    fn test_concat_func() {
        let sql = "SELECT CONCAT(name, middle_name) FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .select(Expression::Function {
                        func: Function::Concat,
                        args: vec![
                            Expression::Identifier("name".into()),
                            Expression::Identifier("middle_name".into()),
                        ]
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_position_func() {
        let sql = "SELECT POSITION('fateful' IN description) FROM films;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("films")
                    .select(Expression::Function {
                        func: Function::Position,
                        args: vec![
                            Expression::Value(Value::String("fateful".into())),
                            Expression::Identifier("description".into()),
                        ]
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_count_func() {
        let sql = "SELECT COUNT(*) FROM payments;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("payments")
                    .select(Expression::Function {
                        func: Function::Count,
                        args: vec![Expression::Wildcard]
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_avg_func() {
        let sql = "SELECT AVG(price) FROM products;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .columns(vec![Expression::Function {
                        func: Function::Avg,
                        args: vec![Expression::Identifier("price".into())]
                    }])
                    .from("products")
                    .into(),
            )
        );
    }

    #[test]
    fn test_group_by() {
        let sql = "SELECT id, SUM(price) FROM sales GROUP BY id;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("sales")
                    .columns(vec![
                        Expression::Identifier("id".into()),
                        Expression::Function {
                            func: Function::Sum,
                            args: vec![Expression::Identifier("price".into())],
                        }
                    ])
                    .group_by(Expression::Identifier("id".into()))
                    .into()
            )
        );
    }

    #[test]
    fn test_typeof() {
        let sql = "SELECT name, TYPEOF(id) FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .columns(vec![
                        Expression::Identifier("name".into()),
                        Expression::Function {
                            func: Function::TypeOf,
                            args: vec![Expression::Identifier("id".into())]
                        }
                    ])
                    .into()
            )
        );
    }

    #[test]
    fn test_abs_func() {
        let sql = "SELECT ABS(amount) FROM payments;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("payments")
                    .select(Expression::Function {
                        func: Function::Abs,
                        args: vec![Expression::Identifier("amount".into())],
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_power_func() {
        let sql = "SELECT POWER(base, exponent) FROM numbers;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("numbers")
                    .select(Expression::Function {
                        func: Function::Power,
                        args: vec![
                            Expression::Identifier("base".into()),
                            Expression::Identifier("exponent".into())
                        ],
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_trunc_func() {
        let sql = "SELECT TRUNC(amount) FROM payments;";
        let statement = Parser::new(sql).parse_statement();
        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("payments")
                    .select(Expression::Function {
                        func: Function::Trunc,
                        args: vec![Expression::Identifier("amount".into())],
                    })
                    .into()
            )
        );

        let sql = "SELECT TRUNC(amount, 4) FROM payments WHERE customer_id > 10;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("payments")
                    .select(Expression::Function {
                        func: Function::Trunc,
                        args: vec![
                            Expression::Identifier("amount".into()),
                            Expression::Value(4i64.into())
                        ],
                    })
                    .r#where(Expression::BinaryOperation {
                        operator: BinaryOperator::Gt,
                        left: Box::new(Expression::Identifier("customer_id".into())),
                        right: Box::new(Expression::Value(10i64.into()))
                    })
                    .into()
            )
        )
    }

    #[test]
    fn test_explicit_order_by() {
        let sql = "SELECT CONCAT(first_name, ' ', last_name) FROM users ORDER BY last_name DESC;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .select(Expression::Function {
                        func: Function::Concat,
                        args: vec![
                            Expression::Identifier("first_name".into()),
                            Expression::Value(Value::String(" ".into())),
                            Expression::Identifier("last_name".into()),
                        ]
                    })
                    .order_by(OrderBy {
                        direction: OrderDirection::Desc,
                        expr: Expression::Identifier("last_name".into())
                    })
                    .into()
            )
        )
    }

    #[test]
    fn test_aliases() {
        let sql = "SELECT salary + bonus AS total, name AS employee_name FROM employees;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("employees")
                    .columns(vec![
                        Expression::Alias {
                            expr: Box::new(Expression::BinaryOperation {
                                operator: BinaryOperator::Plus,
                                left: Box::new(Expression::Identifier("salary".into())),
                                right: Box::new(Expression::Identifier("bonus".into()))
                            }),
                            alias: "total".into()
                        },
                        Expression::Alias {
                            expr: Box::new(Expression::Identifier("name".into())),
                            alias: "employee_name".into()
                        }
                    ])
                    .into()
            )
        )
    }

    #[test]
    fn test_nested_aliases() {
        let sql = "SELECT (salary * 2) + (bonus / 2) as value FROM employees;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("employees")
                    .select(Expression::Alias {
                        expr: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Plus,
                            left: Box::new(Expression::Nested(Box::new(
                                Expression::BinaryOperation {
                                    operator: BinaryOperator::Mul,
                                    left: Box::new(Expression::Identifier("salary".into())),
                                    right: Box::new(Expression::Value(Value::Number(2)))
                                }
                            ))),
                            right: Box::new(Expression::Nested(Box::new(
                                Expression::BinaryOperation {
                                    operator: BinaryOperator::Div,
                                    left: Box::new(Expression::Identifier("bonus".into())),
                                    right: Box::new(Expression::Value(Value::Number(2)))
                                }
                            )))
                        }),
                        alias: "value".into()
                    })
                    .into()
            )
        );
    }

    #[test]
    fn test_named_function_alias() {
        let sql = "SELECT COUNT(*) as user_count FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .select(Expression::Alias {
                        alias: "user_count".into(),
                        expr: Box::new(Expression::Function {
                            func: Function::Count,
                            args: vec![Expression::Wildcard]
                        })
                    })
                    .into()
            )
        )
    }
    #[test]
    fn test_text_column() {
        let sql = "CREATE TABLE notes (id SERIAL PRIMARY KEY, content TEXT, created_at TIMESTAMP);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Create(Create::Table {
                name: "notes".into(),
                columns: vec![
                    Column::primary_key("id", Type::Serial),
                    Column::new("content", Type::Text),
                    Column::new("created_at", Type::DateTime)
                ]
            })
        )
    }

    #[test]
    fn test_nullable_column() {
        let sql = "CREATE TABLE users (id SERIAL PRIMARY KEY, email TEXT UNIQUE, profile_url TEXT NULLABLE);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Create(Create::Table {
                name: "users".into(),
                columns: vec![
                    Column::primary_key("id", Type::Serial),
                    Column::unique("email", Type::Text),
                    Column::nullable("profile_url", Type::Text)
                ]
            })
        )
    }

    #[test]
    fn test_nullable_condition() {
        let sql = "SELECT * FROM customer WHERE email IS NOT NULL;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("customer")
                    .select(Expression::Wildcard)
                    .r#where(Expression::IsNull {
                        expr: Box::new(Expression::Identifier("email".into())),
                        negated: true
                    })
                    .into()
            )
        );

        let sql = "SELECT * FROM customer WHERE email IS NULL;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("customer")
                    .select(Expression::Wildcard)
                    .r#where(Expression::IsNull {
                        expr: Box::new(Expression::Identifier("email".into())),
                        negated: false
                    })
                    .into()
            ),
        )
    }

    #[test]
    fn test_extract_function() {
        let sql = "SELECT EXTRACT(QUARTER FROM joined) FROM employees;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .columns(vec![Expression::Function {
                        func: Function::Extract,
                        args: vec![
                            Expression::Value("QUARTER".into()),
                            Expression::Identifier("joined".into())
                        ],
                    }])
                    .from("employees")
                    .into()
            ),
        );

        let sql = "SELECT EXTRACT(SOMETHING FROM joined) FROM employees;";
        let statement = Parser::new(sql).parse_statement();
        assert_eq!(
            statement.unwrap_err(),
            ParserError {
                kind: ErrorKind::FormatError(
                    "Unit 'SOMETHING' is not recognised for date type".into()
                ),
                location: Location::new(1, 16),
                input: sql.to_string(),
            }
        )
    }

    #[test]
    fn test_interval() {
        let sql = "SELECT order_date, order_date + INTERVAL '30 days' AS due_date FROM orders;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("orders")
                    .columns(vec![
                        Expression::Identifier("order_date".into()),
                        Expression::Alias {
                            alias: "due_date".into(),
                            expr: Box::new(Expression::BinaryOperation {
                                operator: BinaryOperator::Plus,
                                left: Box::new(Expression::Identifier("order_date".into())),
                                right: Box::new(Expression::Value(Value::Interval(
                                    Interval::from_days(30)
                                )))
                            })
                        }
                    ])
                    .into()
            )
        )
    }

    #[test]
    fn test_join() {
        let sql = "SELECT name, title FROM users JOIN posts ON user_id = post_user_id;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .columns(vec![
                        Expression::Identifier("name".into()),
                        Expression::Identifier("title".into())
                    ])
                    .join(JoinClause {
                        table: "posts".into(),
                        join_type: JoinType::Inner,
                        on: Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("user_id".into())),
                            right: Box::new(Expression::Identifier("post_user_id".into())),
                        }
                    })
                    .into()
            )
        )
    }

    #[test]
    fn test_multiple_joins() {
        let sql = "SELECT name, title, content FROM users JOIN posts ON user_id = post_user_id LEFT JOIN comments ON post_id = comment_post_id;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .columns(vec![
                        Expression::Identifier("name".into()),
                        Expression::Identifier("title".into()),
                        Expression::Identifier("content".into()),
                    ])
                    .join(JoinClause {
                        table: "posts".into(),
                        join_type: JoinType::Inner,
                        on: Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("user_id".into())),
                            right: Box::new(Expression::Identifier("post_user_id".into()))
                        }
                    })
                    .join(JoinClause {
                        table: "comments".into(),
                        join_type: JoinType::Left,
                        on: Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("post_id".into())),
                            right: Box::new(Expression::Identifier("comment_post_id".into()))
                        }
                    })
                    .into()
            )
        )
    }

    #[test]
    fn test_join_with_where() {
        let sql = "SELECT name, title FROM users JOIN posts ON user_id = post_user_id WHERE active = true;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .from("users")
                    .columns(vec![
                        Expression::Identifier("name".into()),
                        Expression::Identifier("title".into()),
                    ])
                    .join(JoinClause {
                        table: "posts".into(),
                        join_type: JoinType::Inner,
                        on: Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("user_id".into())),
                            right: Box::new(Expression::Identifier("post_user_id".into())),
                        }
                    })
                    .r#where(Expression::BinaryOperation {
                        operator: BinaryOperator::Eq,
                        left: Box::new(Expression::Identifier("active".into())),
                        right: Box::new(Expression::Value(Value::Boolean(true))),
                    })
                    .into()
            )
        )
    }

    #[test]
    fn test_parse_limit() {
        let sql = "SELECT name FROM users LIMIT 5;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .select(Expression::Identifier("name".into()))
                    .from("users")
                    .limit(5)
                    .into()
            )
        )
    }

    #[test]
    fn test_parse_offset() {
        let sql = "SELECT name FROM users OFFSET 10;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .select(Expression::Identifier("name".into()))
                    .from("users")
                    .offset(10)
                    .into()
            )
        )
    }

    #[test]
    fn test_parse_limit_with_order() {
        let sql = "SELECT id, name FROM products ORDER BY name DESC LIMIT 10;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(
                Select::builder()
                    .columns(vec![
                        Expression::Identifier("id".into()),
                        Expression::Identifier("name".into())
                    ])
                    .order_by(OrderBy {
                        direction: OrderDirection::Desc,
                        expr: Expression::Identifier("name".into())
                    })
                    .from("products")
                    .limit(10)
                    .into()
            )
        )
    }

    #[test]
    fn test_numeric_type_parsing() {
        let sql = "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance NUMERIC, rate NUMERIC);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Table {
                name: "accounts".into(),
                columns: vec![
                    Column::primary_key("id", Type::Integer),
                    Column::new("balance", Type::Numeric(NUMERIC_ANY, NUMERIC_ANY)),
                    Column::new("rate", Type::Numeric(NUMERIC_ANY, NUMERIC_ANY))
                ]
            }))
        );
    }

    #[test]
    fn test_create_table_with_enum() {
        let sql =
            "CREATE TABLE tasks (id INTEGER PRIMARY KEY, status 'todo' | 'in_progress' | 'done');";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Table {
                name: "tasks".into(),
                columns: vec![
                    Column::primary_key("id", Type::Integer),
                    Column::with_enum(
                        "status",
                        Type::Enum(0),
                        vec!["todo".into(), "in_progress".into(), "done".into()]
                    )
                ],
            }))
        )
    }
}
