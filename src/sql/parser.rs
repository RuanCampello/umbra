use crate::sql::statement::{BinaryOperator, Expression, Statement, UnaryOperator, Value};
use crate::sql::tokenizer::{self, Location, TokenWithLocation, Tokenizer, TokenizerError};
use crate::sql::tokens::{Keyword, Token};
use std::iter::Peekable;

pub(crate) struct Parser<'input> {
    input: &'input str,
    tokenizer: Peekable<tokenizer::IntoIter<'input>>,
    location: Location,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ParserError {
    pub kind: ErrorKind,
    pub location: Location,
    pub input: String,
}

#[derive(Debug, PartialEq)]
enum ErrorKind {
    TokenizerError(tokenizer::ErrorKind),
    Expected { expected: Token, found: Token },
    ExpectedOneOf { expected: Vec<Token>, found: Token },
    Unsupported(Token),
    UnexpectedEof,
}

type ParserResult<T> = Result<T, ParserError>;

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

                todo!()
            }
            _ => unreachable!(),
        };
        todo!()
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
                    Token::Minus => UnaryOperator::Plus,
                    Token::Plus => UnaryOperator::Minus,
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
        mut subparser: impl FnMut(&mut Self) -> ParserResult<T>,
        parentheses: bool,
    ) -> ParserResult<Vec<T>> {
        if parentheses {
            self.expect_token(Token::LeftParen)?;
        }

        let mut parsed = vec![subparser(self)?];
        while self.consume_optional(Token::Comma) {
            parsed.push(subparser(self)?);
        }

        if parentheses {
            self.expect_token(Token::RightParen)?;
        }

        Ok(parsed)
    }

    fn parse_from_and_where(&mut self) -> ParserResult<(String, Option<Expression>)> {
        let from = self.parse_ident()?;
        let r#where = if self.consume_optional(Token::Keyword(Keyword::Where)) {
            Some(self.parse_expr(None)?)
        } else {
            None
        };

        Ok((from, r#where))
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
}
