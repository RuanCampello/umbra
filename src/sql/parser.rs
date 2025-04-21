use crate::core::date::{NaiveDate as Date, NaiveDateTime as DateTime, NaiveTime as Time, Parse};
use crate::sql::statement::{
    Assignment, BinaryOperator, Column, Constraint, Create, Drop, Expression, Statement, Type,
    UnaryOperator, Value,
};
use crate::sql::tokenizer::{self, Location, TokenWithLocation, Tokenizer, TokenizerError};
use crate::sql::tokens::{Keyword, Token};
use std::iter::Peekable;

pub(in crate::sql) struct Parser<'input> {
    input: &'input str,
    tokenizer: Peekable<tokenizer::IntoIter<'input>>,
    location: Location,
}

#[derive(Debug, PartialEq)]
pub(in crate::sql) struct ParserError {
    kind: ErrorKind,
    location: Location,
    input: String,
}

#[derive(Debug, PartialEq)]
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
            Keyword::Select => {
                let cols = self.parse_separated_tokens(|parser| parser.parse_expr(None), false)?;
                self.expect_keyword(Keyword::From)?;
                let (from, r#where) = self.parse_from_and_where()?;
                // TODO: parse order by
                Statement::Select {
                    columns: cols,
                    from,
                    r#where,
                    order_by: vec![],
                }
            }
            Keyword::Create => {
                let keyword = self.expect_one(&[
                    Keyword::Database,
                    Keyword::Table,
                    Keyword::Unique,
                    Keyword::Index,
                ])?;

                Statement::Create(match keyword {
                    Keyword::Database => Create::Database(self.parse_ident()?),
                    Keyword::Table => Create::Table {
                        name: self.parse_ident()?,
                        columns: self.parse_separated_tokens(Self::parse_col, true)?,
                    },
                    Keyword::Unique | Keyword::Index => {
                        let unique = keyword.eq(&Keyword::Unique);
                        if unique {
                            self.expect_keyword(Keyword::Index)?;
                        }

                        let name = self.parse_ident()?;
                        self.expect_keyword(Keyword::On)?;
                        let table = self.parse_ident()?;
                        self.expect_token(Token::LeftParen)?;
                        let column = self.parse_ident()?;
                        self.expect_token(Token::RightParen)?;

                        Create::Index {
                            name,
                            table,
                            column,
                            unique,
                        }
                    }
                    _ => unreachable!(),
                })
            }
            Keyword::Insert => {
                self.expect_keyword(Keyword::Into)?;
                let into = self.parse_ident()?;
                let columns: Vec<String> = match self.peek_token() {
                    Some(_) => self.parse_separated_tokens(Self::parse_ident, true),
                    None => Ok(vec![]),
                }?;

                self.expect_keyword(Keyword::Values)?;
                let values = self.parse_separated_tokens(|parser| parser.parse_expr(None), true)?;

                Statement::Insert {
                    into,
                    columns,
                    values,
                }
            }
            Keyword::Update => {
                let table = self.parse_ident()?;
                self.expect_keyword(Keyword::Set)?;

                let columns = self.parse_separated_tokens(Self::parse_assign, false)?;
                let r#where = match self.consume_optional(Token::Keyword(Keyword::Where)) {
                    true => Some(self.parse_expr(None)?),
                    false => None,
                };

                Statement::Update {
                    columns,
                    r#where,
                    table,
                }
            }
            Keyword::Drop => {
                let keyword = self.expect_one(&[Keyword::Database, Keyword::Table])?;
                let identifier = self.parse_ident()?;

                Statement::Drop(match keyword {
                    Keyword::Database => Drop::Database(identifier),
                    Keyword::Table => Drop::Table(identifier),
                    _ => unreachable!(),
                })
            }
            Keyword::Delete => {
                self.expect_keyword(Keyword::From)?;
                let (from, r#where) = self.parse_from_and_where()?;

                Statement::Delete { from, r#where }
            }
            Keyword::Start => {
                self.expect_keyword(Keyword::Transaction)?;
                // TODO: start transaction
                todo!()
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

    fn parse_expr(&mut self, precedence: Option<u8>) -> ParserResult<Expression> {
        let mut expr = self.parse_pref()?;
        let mut next = self.get_precedence();

        while precedence.unwrap_or(0) < next {
            expr = self.parse_infix(expr, next)?;
            next = self.get_precedence();
        }

        Ok(expr)
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
            token => {
                return Err(self.error(ErrorKind::Expected {
                    expected: Token::Eq,
                    found: token,
                }))
            }
        };

        let right = Box::new(self.parse_expr(Some(precedence))?);
        Ok(Expression::BinaryOperator {
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
            Token::Keyword(keyword @ (Keyword::Date | Keyword::Timestamp | Keyword::Time)) => {
                self.parse_datetime(keyword)
            }

            Token::Mul => Ok(Expression::Wildcard),
            Token::LeftParen => {
                let expr = self.parse_expr(None)?;
                self.expect_token(Token::RightParen)?;

                Ok(Expression::Nested(Box::new(expr)))
            }
            Token::Number(number) => Ok(Expression::Value(Value::Number(
                number
                    .parse()
                    // TODO: add proper error of int out of range
                    .map_err(|_| self.error(ErrorKind::UnexpectedEof))?,
            ))),
            token @ (Token::Minus | Token::Plus) => {
                let op = match token {
                    Token::Minus => UnaryOperator::Minus,
                    Token::Plus => UnaryOperator::Plus,
                    _ => unreachable!(),
                };

                let expr = Box::new(self.parse_expr(Some(UNARY_ARITHMETIC_OPERATOR))?);
                Ok(Expression::UnaryOperator { operator: op, expr })
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

    fn parse_col(&mut self) -> ParserResult<Column> {
        let name = self.parse_ident()?;

        let data_type = match self.expect_one(&Self::supported_types())? {
            int @ (Keyword::Int | Keyword::BigInt) => {
                let unsigned = self.consume_optional(Token::Keyword(Keyword::Unsigned));

                match (int, unsigned) {
                    (Keyword::Int, true) => Type::UnsignedInteger,
                    (Keyword::Int, false) => Type::Integer,
                    (Keyword::BigInt, true) => Type::UnsignedBigInteger,
                    (Keyword::BigInt, false) => Type::BigInteger,
                    _ => unreachable!(),
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
                Type::Varchar(len)
            }
            Keyword::Bool => Type::Boolean,
            Keyword::Timestamp => Type::DateTime,
            Keyword::Date => Type::Date,
            Keyword::Time => Type::Time,
            _ => unreachable!(),
        };

        let mut constraints = Vec::new();
        while let Some(constraint) = self
            .consume_one(&[Keyword::Primary, Keyword::Unique])
            .as_optional()
        {
            match constraint {
                Keyword::Primary => {
                    self.expect_keyword(Keyword::Key)?;
                    constraints.push(Constraint::PrimaryKey);
                }
                Keyword::Unique => constraints.push(Constraint::Unique),
                _ => unreachable!(),
            }
        }

        Ok(Column {
            name,
            data_type,
            constraints,
        })
    }

    fn parse_assign(&mut self) -> ParserResult<Assignment> {
        let identifier = self.parse_ident()?;
        self.expect_token(Token::Eq)?;
        let value = self.parse_expr(None)?;

        Ok(Assignment { identifier, value })
    }

    fn get_precedence(&mut self) -> u8 {
        let Some(Ok(token)) = self.peek_token() else {
            return 0;
        };

        match token {
            Token::Keyword(Keyword::Or) => 5,
            Token::Keyword(Keyword::And) => 10,
            Token::Eq | Token::Neq | Token::Gt | Token::GtEq | Token::Lt | Token::LtEq => 20,
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
        println!("here");
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

    fn supported_statements() -> Vec<Keyword> {
        vec![
            Keyword::Create,
            Keyword::Update,
            Keyword::Insert,
            Keyword::Delete,
            Keyword::Select,
            Keyword::Drop,
            Keyword::Start,
            Keyword::Commit,
            Keyword::Rollback,
            Keyword::Explain,
        ]
    }

    fn supported_types() -> Vec<Keyword> {
        vec![
            Keyword::Int,
            Keyword::BigInt,
            Keyword::Bool,
            Keyword::Varchar,
            Keyword::Time,
            Keyword::Date,
            Keyword::Timestamp,
        ]
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
    use crate::sql::statement::Expression;

    #[test]
    fn test_parse_select() {
        let input = "SELECT id, price FROM bills;";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select {
                columns: vec![
                    Expression::Identifier("id".to_string()),
                    Expression::Identifier("price".to_string())
                ],
                from: "bills".to_string(),
                r#where: None,
                order_by: vec![],
            })
        );
    }

    #[test]
    fn test_parse_select_where() {
        let input = "SELECT title, author FROM books WHERE author = 'Agatha Christie';";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select {
                columns: vec![
                    Expression::Identifier("title".to_string()),
                    Expression::Identifier("author".to_string())
                ],
                from: "books".to_string(),
                r#where: Some(Expression::BinaryOperator {
                    operator: BinaryOperator::Eq,
                    left: Box::new(Expression::Identifier("author".to_string())),
                    right: Box::new(Expression::Value(Value::String(
                        "Agatha Christie".to_string()
                    ))),
                }),
                order_by: vec![],
            })
        );
    }

    #[test]
    fn test_parse_asterisk() {
        let input = "SELECT * FROM users;";
        let statement = Parser::new(input).parse_statement();

        assert_eq!(
            statement,
            Ok(Statement::Select {
                columns: vec![Expression::Wildcard],
                from: "users".to_string(),
                r#where: None,
                order_by: vec![],
            })
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
            Ok(Statement::Update {
                table: "employees".to_string(),
                columns: vec![
                    Assignment {
                        identifier: "salary".to_string(),
                        value: Expression::BinaryOperator {
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
                r#where: Some(Expression::BinaryOperator {
                    operator: BinaryOperator::And,
                    left: Box::new(Expression::BinaryOperator {
                        operator: BinaryOperator::And,
                        left: Box::new(Expression::BinaryOperator {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("department".to_string())),
                            right: Box::new(Expression::Value(Value::String(
                                "engineering".to_string(),
                            ))),
                        }),
                        right: Box::new(Expression::BinaryOperator {
                            operator: BinaryOperator::GtEq,
                            left: Box::new(Expression::Identifier("performance_score".to_string())),
                            right: Box::new(Expression::Value(Value::Number(80))),
                        }),
                    }),
                    right: Box::new(Expression::Nested(Box::new(Expression::BinaryOperator {
                        operator: BinaryOperator::Or,
                        left: Box::new(Expression::BinaryOperator {
                            operator: BinaryOperator::Gt,
                            left: Box::new(Expression::Identifier("years_of_service".to_string())),
                            right: Box::new(Expression::Value(Value::Number(3))),
                        }),
                        right: Box::new(Expression::BinaryOperator {
                            operator: BinaryOperator::Eq,
                            left: Box::new(Expression::Identifier("is_team_lead".to_string())),
                            right: Box::new(Expression::Value(Value::Boolean(true))),
                        }),
                    })))
                }),
            })
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
            Ok(Statement::Insert {
                into: "departments".to_string(),
                columns: vec!["id".to_string(), "name".to_string()],
                values: vec![
                    Expression::Value(Value::Number(1)),
                    Expression::Value(Value::String("HR".to_string())),
                ],
            })
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
                Statement::Select {
                    columns: vec![Expression::Wildcard],
                    from: "employees".to_string(),
                    r#where: None,
                    order_by: vec![],
                }
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
            Ok(Statement::Select {
                columns: vec![Expression::Wildcard],
                from: "schedule".to_string(),
                r#where: Some(Expression::BinaryOperator {
                    operator: BinaryOperator::Lt,
                    left: Box::new(Expression::Identifier("start_time".to_string())),
                    right: Box::new(Expression::Value(Value::String("12:00:00".into()))),
                }),
                order_by: vec![],
            })
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
}
