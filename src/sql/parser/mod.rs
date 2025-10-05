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

use crate::core::date::{
    ExtractKind, NaiveDate as Date, NaiveDateTime as DateTime, NaiveTime as Time, Parse,
};
use crate::sql::statement::{
    Assignment, BinaryOperator, Column, Constraint, Create, Drop, Expression, Function, Statement,
    Type, UnaryOperator, Value,
};
use std::borrow::Cow;
use std::fmt::Display;
use std::iter::Peekable;
use tokenizer::{Location, TokenWithLocation, Tokenizer, TokenizerError};
use tokens::Token;

pub use tokens::Keyword;

use super::statement::{Delete, Insert, Select, Update};

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
#[allow(unused)]
enum ErrorKind {
    TokenizerError(tokenizer::ErrorKind),
    Expected { expected: Token, found: Token },
    ExpectedOneOf { expected: Vec<Token>, found: Token },
    Unsupported(Token),
    UnexpectedEof,
    FormatError(String),
}

pub(in crate::sql) type ParserResult<T> = Result<T, ParserError>;

const UNARY_ARITHMETIC_OPERATOR: u8 = 50;

trait Sql<'sql>: Sized {
    fn parse(parser: &mut Parser<'sql>) -> ParserResult<Self>;
}

#[allow(unused)]
impl<'input> Parser<'input> {
    pub fn new(input: &'input str) -> Self {
        Self {
            input,
            tokenizer: Tokenizer::new(input).into_iter().peekable(),
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
            Keyword::Select => Statement::Select(Select::parse(self)?),
            Keyword::Create => Statement::Create(Create::parse(self)?),
            Keyword::Insert => Statement::Insert(Insert::parse(self)?),
            Keyword::Update => Statement::Update(Update::parse(self)?),
            Keyword::Drop => Statement::Drop(Drop::parse(self)?),
            Keyword::Delete => Statement::Delete(Delete::parse(self)?),
            Keyword::Start => {
                self.expect_keyword(Keyword::Transaction)?;
                Statement::StartTransaction
            }
            Keyword::Commit => Statement::Commit,
            Keyword::Rollback => Statement::Rollback,
            Keyword::Explain => return Ok(Statement::Explain(Box::new(self.parse_statement()?))),

            _ => unreachable!(),
        };

        self.expect_token(Token::Semicolon)?;
        Ok(statement)
    }

    fn parse_ident(&mut self) -> ParserResult<String> {
        self.next_token().and_then(|token| match token {
            Token::Identifier(identifier) => Ok(identifier),
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
            }
            Token::Keyword(Keyword::In) => {
                self.next_token()?;
                let values =
                    self.parse_separated_tokens(|parser| parser.parse_expr(None), false)?;
                self.expect_token(Token::RightParen)?;

                let mut iter = values.into_iter();
                let first = match iter.next() {
                    Some(first) => Expression::BinaryOperation {
                        operator: BinaryOperator::Eq,
                        left: Box::new(left.clone()),
                        right: Box::new(first),
                    },
                    None => unreachable!(),
                };

                return Ok(iter.fold(first, |acc, value| Expression::BinaryOperation {
                    operator: BinaryOperator::Or,
                    left: Box::new(acc),
                    right: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::Eq,
                        left: Box::new(left.clone()),
                        right: Box::new(value),
                    }),
                }));
            }
            Token::Keyword(Keyword::Is) => {
                let negated = self.consume_optional(Token::Keyword(Keyword::Not));
                self.expect_keyword(Keyword::Null)?;

                return Ok(Expression::IsNull {
                    expr: Box::new(left),
                    negated,
                });
            }
            token => {
                return Err(self.error(ErrorKind::Expected {
                    expected: Token::Eq,
                    found: token,
                }))
            }
        };

        let right = Box::new(self.parse_expr(Some(precedence))?);
        Ok(Expression::BinaryOperation {
            operator: op,
            left: Box::new(left),
            right,
        })
    }

    fn parse_pref(&mut self) -> ParserResult<Expression> {
        match self.next_token()? {
            Token::Identifier(identifier) => Ok(Expression::Identifier(identifier)),
            Token::String(string) => Ok(Expression::Value(Value::String(string))),
            Token::Keyword(Keyword::True) => Ok(Expression::Value(Value::Boolean(true))),
            Token::Keyword(Keyword::False) => Ok(Expression::Value(Value::Boolean(false))),
            Token::Keyword(Keyword::Null) => Ok(Expression::Value(Value::Null)),
            Token::Keyword(keyword @ (Keyword::Date | Keyword::Timestamp | Keyword::Time)) => {
                self.parse_datetime(keyword)
            }
            Token::Keyword(func) if func.is_function() => {
                self.expect_token(Token::LeftParen)?;
                self.parse_func(func)
            }

            Token::Mul => Ok(Expression::Wildcard),
            Token::LeftParen => {
                let expr = self.parse_expr(None)?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Nested(Box::new(expr)))
            }
            Token::Number(number) => match number.contains(".") {
                true => Ok(Expression::Value(Value::Float(
                    number
                        .parse::<f64>()
                        .map_err(|_| self.error(ErrorKind::UnexpectedEof))?,
                ))),
                _ => Ok(Expression::Value(Value::Number(
                    number
                        .parse()
                        .map_err(|_| self.error(ErrorKind::UnexpectedEof))?,
                ))),
            },
            token @ (Token::Minus | Token::Plus) => {
                let op = match token {
                    Token::Minus => UnaryOperator::Minus,
                    Token::Plus => UnaryOperator::Plus,
                    _ => unreachable!(),
                };

                let expr = Box::new(self.parse_expr(Some(UNARY_ARITHMETIC_OPERATOR))?);
                Ok(Expression::UnaryOperation { operator: op, expr })
            }
            token => Err(self.error(ErrorKind::ExpectedOneOf {
                expected: vec![
                    Token::Identifier(Default::default()),
                    Token::Number(Default::default()),
                    Token::String(Default::default()),
                    Token::Mul,
                    Token::Minus,
                    Token::Plus,
                    Token::LeftParen,
                ],
                found: token,
            })),
        }
    }

    fn parse_closing_expr(&mut self) -> ParserResult<Expression> {
        let pref = self.parse_expr(None)?;
        self.expect_token(Token::RightParen)?;

        Ok(pref)
    }

    fn parse_col(&mut self) -> ParserResult<Column> {
        let name = self.parse_ident()?;
        let data_type = self.parse_type()?;

        let mut constraints = Vec::new();
        while let Some(constraint) = self
            .consume_one(&[Keyword::Primary, Keyword::Unique, Keyword::Nullable])
            .as_optional()
        {
            match constraint {
                Keyword::Primary => {
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(Constraint::PrimaryKey);
                }
                Keyword::Unique => constraints.push(Constraint::Unique),
                Keyword::Nullable => constraints.push(Constraint::Nullable),
                _ => unreachable!(),
            }
        }

        Ok(Column {
            name,
            data_type,
            constraints,
        })
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
            }
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
                        }))?
                    }
                };

                self.expect_token(Token::RightParen)?;
                Ok(Type::Varchar(len))
            }
            Keyword::Double => {
                self.expect_keyword(Keyword::Precision)?;
                Ok(Type::DoublePrecision)
            }
            Keyword::Real => Ok(Type::Real),
            Keyword::Bool => Ok(Type::Boolean),
            Keyword::Timestamp => Ok(Type::DateTime),
            Keyword::Date => Ok(Type::Date),
            Keyword::Time => Ok(Type::Time),
            Keyword::Text => Ok(Type::Text),
            keyword => unreachable!("unexpected column token: {keyword}"),
        }
    }

    fn parse_assign(&mut self) -> ParserResult<Assignment> {
        let identifier = self.parse_ident()?;
        self.expect_token(Token::Eq)?;
        let value = self.parse_expr(None)?;

        Ok(Assignment { identifier, value })
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
            Token::Plus | Token::Minus => 30,
            Token::Mul | Token::Div => 40,
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
        let value_str = self.parse_ident()?;

        let value = match keyword {
            Keyword::Date => Date::parse_str(&value_str)
                .map(|x| ToString::to_string(&x))
                .map_err(|e| {
                    self.error(ErrorKind::FormatError(format!(
                        "Invalid date format: {:?}",
                        e
                    )))
                })?,
            Keyword::Time => Time::parse_str(&value_str)
                .map(|x| ToString::to_string(&x))
                .map_err(|e| {
                    self.error(ErrorKind::FormatError(format!(
                        "Invalid time format: {:?}",
                        e
                    )))
                })?,
            Keyword::Timestamp => DateTime::parse_str(&value_str)
                .map(|x| ToString::to_string(&x))
                .map_err(|e| {
                    self.error(ErrorKind::FormatError(format!(
                        "Invalid timestamp format: {:?}",
                        e
                    )))
                })?,
            _ => unreachable!(),
        };

        Ok(Expression::Value(Value::String(value)))
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
            }
            Keyword::Extract => {
                let kind_str = self.parse_ident()?;
                // TODO: this way we need to parse the kind here to check and when executing, but I
                // don't know any better way without changing the enums to a special case or just
                // making an execution error instead of a parsing, which isn't the ideal
                let kind = ExtractKind::try_from(kind_str.as_str()).or(Err(self.error(
                    ErrorKind::FormatError(format!(
                        "Unit '{kind_str}' is not recognised for date type"
                    )),
                )))?;

                self.expect_keyword(Keyword::From)?;
                let from = self.parse_ident()?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Function {
                    func: Function::Extract,
                    args: vec![
                        Expression::Value(Value::String(kind_str)),
                        Expression::Identifier(from),
                    ],
                })
            }

            keyword
                if matches!(
                    keyword,
                    Keyword::Concat | Keyword::Power | Keyword::Trunc | Keyword::Coalesce
                ) =>
            {
                let args = self.parse_separated_tokens(|p| p.parse_expr(None), false)?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Function {
                    func: Function::try_from(&keyword).unwrap(),
                    args,
                })
            }
            Keyword::Ascii => self.parse_unary_func(Function::Ascii),
            Keyword::Position => {
                let needle = self.parse_pref()?;
                self.expect_keyword(Keyword::In)?;
                let haystack = self.parse_pref()?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Function {
                    func: Function::Position,
                    args: vec![needle, haystack],
                })
            }
            keyword if keyword.is_function() => {
                let value = self.parse_closing_expr()?;

                Ok(Expression::Function {
                    func: Function::try_from(&keyword).unwrap(),
                    args: vec![value],
                })
            }
            Keyword::Count => self.parse_unary_func(Function::Count),
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
        Ok(Expression::Function {
            func,
            args: vec![expr],
        })
    }

    fn peek_token(&mut self) -> Option<Result<&Token, &TokenizerError>> {
        self.skip_whitespaces();
        self.peek_token_stream()
    }

    fn next_token(&mut self) -> ParserResult<Token> {
        self.skip_whitespaces();
        self.next_token_stream()
    }

    fn expect_token(&mut self, expected: Token) -> ParserResult<Token> {
        self.next_token()
            .and_then(|token| match token.eq(&expected) {
                true => Ok(token),
                false => Err(self.error(ErrorKind::Expected {
                    expected,
                    found: token,
                })),
            })
    }

    fn expect_keyword(&mut self, expected: Keyword) -> ParserResult<Keyword> {
        self.expect_token(Token::Keyword(expected))
            .map(|_| expected)
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
            }
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
            }
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

        *keywords
            .into_iter()
            .find(|key| consume_keyword(key))
            .unwrap_or(&Keyword::None)
    }

    fn consume_optional(&mut self, optional: Token) -> bool {
        match self.peek_token() {
            Some(Ok(token)) if token == &optional => {
                let _ = self.next_token();
                true
            }
            _ => false,
        }
    }

    fn error(&self, kind: ErrorKind) -> ParserError {
        ParserError {
            kind,
            input: self.input.to_string(),
            location: self.location,
        }
    }

    fn tokens_from_keywords<'key, K>(keywords: &'key K) -> Vec<Token>
    where
        &'key K: IntoIterator<Item = &'key Keyword>,
    {
        keywords.into_iter().map(From::from).collect()
    }

    const fn supported_statements() -> [Keyword; 10] {
        [
            Keyword::Select,
            Keyword::Create,
            Keyword::Update,
            Keyword::Insert,
            Keyword::Delete,
            Keyword::Drop,
            Keyword::Start,
            Keyword::Rollback,
            Keyword::Commit,
            Keyword::Explain,
        ]
    }

    const fn supported_types() -> [Keyword; 15] {
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
            Keyword::Time,
            Keyword::Date,
            Keyword::Timestamp,
        ]
    }

    const fn is_integer(keyword: &Keyword) -> bool {
        match keyword {
            Keyword::SmallInt
            | Keyword::Int
            | Keyword::BigInt
            | Keyword::SmallSerial
            | Keyword::Serial
            | Keyword::BigSerial
            | Keyword::Uuid => true,
            _ => false,
        }
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
            }
            None => {
                let line = self.input.lines().last().unwrap();
                f.write_str(line)?;
                line.chars().count()
            }
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
            ErrorKind::Unsupported(token) => write!(f, "Unexpected or unsupported token: {token}"),
            ErrorKind::Expected { expected, found } => write!(
                f,
                "Expected {} but found '{found}' instead",
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
                    one_of.push_str(&&ErrorKind::expected_token_str(
                        &expected[expected.len() - 1],
                    ));
                }

                write!(f, "Expected {one_of} but found '{found}' instead")
            }
        }
    }
}

impl From<TokenizerError> for ParserError {
    fn from(
        TokenizerError {
            kind,
            location,
            input,
        }: TokenizerError,
    ) -> Self {
        Self {
            input,
            kind: ErrorKind::TokenizerError(kind),
            location,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::statement::{Expression, OrderBy, OrderDirection};

    #[test]
    fn test_parse_select() {
        let input = "SELECT id, price FROM bills;";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select(Select {
                columns: vec![
                    Expression::Identifier("id".to_string()),
                    Expression::Identifier("price".to_string())
                ],
                from: "bills".to_string(),
                r#where: None,
                order_by: vec![],
                group_by: vec![],
            }))
        );
    }

    #[test]
    fn test_parse_select_where() {
        let input = "SELECT title, author FROM books WHERE author = 'Agatha Christie';";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select(Select {
                columns: vec![
                    Expression::Identifier("title".to_string()),
                    Expression::Identifier("author".to_string())
                ],
                from: "books".to_string(),
                r#where: Some(Expression::BinaryOperation {
                    operator: BinaryOperator::Eq,
                    left: Box::new(Expression::Identifier("author".to_string())),
                    right: Box::new(Expression::Value(Value::String(
                        "Agatha Christie".to_string()
                    ))),
                }),
                order_by: vec![],
                group_by: vec![],
            }))
        );
    }

    #[test]
    fn test_parse_asterisk() {
        let input = "SELECT * FROM users;";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select(Select {
                columns: vec![Expression::Wildcard],
                from: "users".to_string(),
                r#where: None,
                order_by: vec![],
                group_by: vec![],
            }))
        );
    }

    #[test]
    fn test_create_table() {
        let input = r#"
            CREATE TABLE employees (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                age INT,
                is_manager BOOLEAN
            );
        "#;

        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Create(Create::Table {
                name: "employees".to_string(),
                columns: vec![
                    Column {
                        name: "id".to_string(),
                        data_type: Type::Integer,
                        constraints: vec![Constraint::PrimaryKey],
                    },
                    Column {
                        name: "name".to_string(),
                        data_type: Type::Varchar(255),
                        constraints: vec![],
                    },
                    Column {
                        name: "age".to_string(),
                        data_type: Type::Integer,
                        constraints: vec![],
                    },
                    Column {
                        name: "is_manager".to_string(),
                        data_type: Type::Boolean,
                        constraints: vec![],
                    },
                ],
            }))
        );
    }

    #[test]
    fn test_create_table_with_unsigned_keys() {
        let sql = r#"
            CREATE TABLE companies (
                id INT UNSIGNED PRIMARY KEY,
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
                            right: Box::new(Expression::Value(Value::String(
                                "engineering".to_string(),
                            ))),
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
            }))
        );
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
                column: "content".to_string(),
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
                    Expression::Value(Value::String("HR".to_string()))
                ],],
            }))
        )
    }

    #[test]
    fn test_multiple_statements() -> ParserResult<()> {
        let sql = r#"
            DROP TABLE bills;
            CREATE TABLE departments (
                id INT UNSIGNED PRIMARY KEY,
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
                        Column {
                            name: "id".to_string(),
                            data_type: Type::UnsignedInteger,
                            constraints: vec![Constraint::PrimaryKey],
                        },
                        Column {
                            name: "name".to_string(),
                            data_type: Type::Varchar(69),
                            constraints: vec![],
                        },
                    ],
                }),
                Statement::Select(Select {
                    columns: vec![Expression::Wildcard],
                    from: "employees".to_string(),
                    r#where: None,
                    order_by: vec![],
                    group_by: vec![],
                })
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
            Ok(Statement::Select(Select {
                columns: vec![Expression::Wildcard],
                from: "schedule".to_string(),
                r#where: Some(Expression::BinaryOperation {
                    operator: BinaryOperator::Lt,
                    left: Box::new(Expression::Identifier("start_time".to_string())),
                    right: Box::new(Expression::Value(Value::String("12:00:00".into()))),
                }),
                order_by: vec![],
                group_by: vec![],
            }))
        );
    }

    #[test]
    fn test_parse_date() {
        let sql = r#"
            CREATE TABLE events (
            event_id INT PRIMARY KEY,
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
                    Column {
                        name: "event_id".into(),
                        data_type: Type::Integer,
                        constraints: vec![Constraint::PrimaryKey],
                    },
                    Column {
                        name: "event_name".into(),
                        data_type: Type::Varchar(100),
                        constraints: vec![]
                    },
                    Column {
                        name: "event_date".into(),
                        data_type: Type::Date,
                        constraints: vec![]
                    }
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
        let sql = "CREATE TABLE employees id INT, name VARCHAR(255);";
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
            book_id INT PRIMARY KEY,
            title VARCHAR (255),
            pages SMALLINT
        );"#;

        let statement = Parser::new(sql).parse_statement().unwrap();

        assert_eq!(
            statement,
            Statement::Create(Create::Table {
                name: "books".into(),
                columns: vec![
                    Column {
                        name: "book_id".into(),
                        data_type: Type::Integer,
                        constraints: vec![Constraint::PrimaryKey],
                    },
                    Column {
                        name: "title".into(),
                        data_type: Type::Varchar(255),
                        constraints: vec![]
                    },
                    Column {
                        name: "pages".into(),
                        data_type: Type::SmallInt,
                        constraints: vec![]
                    }
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
        let sql = "CREATE TABLE employees (id INT, name VARCHAR(SOMETHING));";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement,
            Err(ParserError {
                location: Location::new(1, 46),
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
            Statement::Select(Select {
                from: "film".to_string(),
                order_by: vec![],
                group_by: vec![],
                columns: vec![
                    Expression::Identifier("film_id".into()),
                    Expression::Identifier("title".into())
                ],
                r#where: Some(Expression::BinaryOperation {
                    operator: BinaryOperator::Or,
                    left: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::Or,
                        left: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("film_id".to_string())),
                            right: Box::new(Expression::Value(Value::Number(1))),
                        }),
                        right: Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("film_id".to_string())),
                            right: Box::new(Expression::Value(Value::Number(2))),
                        }),
                    }),
                    right: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::Eq,
                        left: Box::new(Expression::Identifier("film_id".to_string())),
                        right: Box::new(Expression::Value(Value::Number(3))),
                    }),
                }),
            })
        );
    }

    #[test]
    fn test_in_expansion() {
        let sql = "SELECT film_id, title FROM films WHERE film_id IN (1, 2, 3);";
        let equivalent_sql =
            "SELECT film_id, title FROM films WHERE film_id = 1 OR film_id = 2 OR film_id = 3;";

        let statement = Parser::new(sql).parse_statement();
        let equivalent_statement = Parser::new(equivalent_sql).parse_statement();

        assert_eq!(statement, equivalent_statement);
    }

    #[test]
    fn test_like_operator() {
        let sql = "SELECT name, last_name FROM customer WHERE name LIKE 'Jen%';";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                order_by: vec![],
                group_by: vec![],
                from: "customer".into(),
                columns: vec![
                    Expression::Identifier("name".into()),
                    Expression::Identifier("last_name".into())
                ],
                r#where: Some(Expression::BinaryOperation {
                    operator: BinaryOperator::Like,
                    left: Box::new(Expression::Identifier("name".into())),
                    right: Box::new(Expression::Value(Value::String("Jen%".into())))
                })
            })
        )
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
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Substring,
                    args: vec![
                        Expression::Identifier("name".into()),
                        Expression::Value(Value::Number(1)),
                        Expression::Value(Value::Number(8)),
                    ]
                }],
                from: "users".into(),
                order_by: vec![],
                group_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_ascii_func() {
        let sql = "SELECT name FROM users ORDER BY ASCII(name);";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Identifier("name".into())],
                from: "users".into(),
                r#where: None,
                order_by: vec![OrderBy {
                    expr: Expression::Function {
                        func: Function::Ascii,
                        args: vec![Expression::Identifier("name".into())]
                    },
                    direction: Default::default(),
                }],
                group_by: vec![],
            })
        )
    }

    #[test]
    fn test_concat_func() {
        let sql = "SELECT CONCAT(name, middle_name) FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Concat,
                    args: vec![
                        Expression::Identifier("name".into()),
                        Expression::Identifier("middle_name".into()),
                    ]
                }],
                from: "users".into(),
                order_by: vec![],
                group_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_position_func() {
        let sql = "SELECT POSITION('fateful' IN description) FROM films;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Position,
                    args: vec![
                        Expression::Value(Value::String("fateful".into())),
                        Expression::Identifier("description".into()),
                    ]
                }],
                from: "films".into(),
                order_by: vec![],
                group_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_count_func() {
        let sql = "SELECT COUNT(*) FROM payments;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Count,
                    args: vec![Expression::Wildcard]
                }],
                from: "payments".into(),
                order_by: vec![],
                group_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_avg_func() {
        let sql = "SELECT AVG(price) FROM products;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Avg,
                    args: vec![Expression::Identifier("price".into())]
                }],
                from: "products".into(),
                order_by: vec![],
                group_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_group_by() {
        let sql = "SELECT id, SUM(price) FROM sales GROUP BY id;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![
                    Expression::Identifier("id".into()),
                    Expression::Function {
                        func: Function::Sum,
                        args: vec![Expression::Identifier("price".into())]
                    }
                ],
                from: "sales".into(),
                group_by: vec![Expression::Identifier("id".into())],
                order_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_typeof() {
        let sql = "SELECT name, TYPEOF(id) FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![
                    Expression::Identifier("name".into()),
                    Expression::Function {
                        func: Function::TypeOf,
                        args: vec![Expression::Identifier("id".into())]
                    }
                ],
                from: "users".into(),
                group_by: vec![],
                order_by: vec![],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_abs_func() {
        let sql = "SELECT ABS(amount) FROM payments;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Abs,
                    args: vec![Expression::Identifier("amount".into())],
                }],
                from: "payments".into(),
                group_by: vec![],
                order_by: vec![],
                r#where: None,
            })
        );
    }

    #[test]
    fn test_power_func() {
        let sql = "SELECT POWER(base, exponent) FROM numbers;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Power,
                    args: vec![
                        Expression::Identifier("base".into()),
                        Expression::Identifier("exponent".into())
                    ],
                }],
                from: "numbers".into(),
                group_by: vec![],
                order_by: vec![],
                r#where: None,
            })
        );
    }

    #[test]
    fn test_trunc_func() {
        let sql = "SELECT TRUNC(amount) FROM payments;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Trunc,
                    args: vec![Expression::Identifier("amount".into())],
                }],
                from: "payments".into(),
                group_by: vec![],
                order_by: vec![],
                r#where: None,
            })
        );

        let sql = "SELECT TRUNC(amount, 4) FROM payments WHERE customer_id > 10;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Trunc,
                    args: vec![
                        Expression::Identifier("amount".into()),
                        Expression::Value(4i128.into())
                    ],
                }],
                from: "payments".into(),
                order_by: vec![],
                group_by: vec![],
                r#where: Some(Expression::BinaryOperation {
                    operator: BinaryOperator::Gt,
                    left: Box::new(Expression::Identifier("customer_id".into())),
                    right: Box::new(Expression::Value(10i128.into()))
                }),
            })
        )
    }

    #[test]
    fn test_explicit_order_by() {
        let sql = "SELECT CONCAT(first_name, ' ', last_name) FROM users ORDER BY last_name DESC;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Concat,
                    args: vec![
                        Expression::Identifier("first_name".into()),
                        Expression::Value(Value::String(" ".into())),
                        Expression::Identifier("last_name".into()),
                    ]
                }],
                from: "users".into(),
                group_by: vec![],
                order_by: vec![OrderBy {
                    direction: OrderDirection::Desc,
                    expr: Expression::Identifier("last_name".into())
                }],
                r#where: None,
            })
        )
    }

    #[test]
    fn test_aliases() {
        let sql = "SELECT salary + bonus AS total, name AS employee_name FROM employees;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![
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
                ],
                from: "employees".into(),
                r#where: None,
                order_by: vec![],
                group_by: vec![],
            })
        )
    }

    #[test]
    fn test_nested_aliases() {
        let sql = "SELECT (salary * 2) + (bonus / 2) as value FROM employees;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Alias {
                    expr: Box::new(Expression::BinaryOperation {
                        operator: BinaryOperator::Plus,
                        left: Box::new(Expression::Nested(Box::new(Expression::BinaryOperation {
                            operator: BinaryOperator::Mul,
                            left: Box::new(Expression::Identifier("salary".into())),
                            right: Box::new(Expression::Value(Value::Number(2)))
                        }))),
                        right: Box::new(Expression::Nested(Box::new(
                            Expression::BinaryOperation {
                                operator: BinaryOperator::Div,
                                left: Box::new(Expression::Identifier("bonus".into())),
                                right: Box::new(Expression::Value(Value::Number(2)))
                            }
                        )))
                    }),
                    alias: "value".into()
                }],
                from: "employees".into(),
                r#where: None,
                order_by: vec![],
                group_by: vec![],
            })
        );
    }

    #[test]
    fn test_named_function_alias() {
        let sql = "SELECT COUNT(*) as user_count FROM users;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Alias {
                    alias: "user_count".into(),
                    expr: Box::new(Expression::Function {
                        func: Function::Count,
                        args: vec![Expression::Wildcard]
                    })
                }],
                from: "users".into(),
                r#where: None,
                order_by: vec![],
                group_by: vec![],
            })
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
            Statement::Select(Select {
                columns: vec![Expression::Wildcard],
                from: "customer".into(),
                r#where: Some(Expression::IsNull {
                    expr: Box::new(Expression::Identifier("email".into())),
                    negated: true
                }),
                order_by: vec![],
                group_by: vec![],
            })
        );

        let sql = "SELECT * FROM customer WHERE email IS NULL;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Wildcard],
                from: "customer".into(),
                r#where: Some(Expression::IsNull {
                    expr: Box::new(Expression::Identifier("email".into())),
                    negated: false
                }),
                order_by: vec![],
                group_by: vec![],
            })
        )
    }

    #[test]
    fn test_extract_function() {
        let sql = "SELECT EXTRACT(QUARTER FROM joined) FROM employees;";
        let statement = Parser::new(sql).parse_statement();

        assert_eq!(
            statement.unwrap(),
            Statement::Select(Select {
                columns: vec![Expression::Function {
                    func: Function::Extract,
                    args: vec![
                        Expression::Value("QUARTER".into()),
                        Expression::Identifier("joined".into())
                    ],
                }],
                from: "employees".into(),
                r#where: None,
                order_by: vec![],
                group_by: vec![]
            })
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
}
