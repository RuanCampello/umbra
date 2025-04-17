use crate::sql::statement::Statement;
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

    fn parse_statement(&mut self) -> ParserResult<Statement> {
        todo!()
    }

    fn parse_separated_tokens<T>(
        &mut self,
        mut subparser: impl FnMut(&mut Self) -> ParserResult<T>,
        parentesis: bool,
    ) -> ParserResult<Vec<T>> {
        if parentesis {
            self.expect_token(Token::LeftParen)?;
        }

        let mut parsed = vec![subparser(self)?];
        while self.consume_optional(Token::Comma) {
            parsed.push(subparser(self)?);
        }

        if parentesis {
            self.expect_token(Token::RightParen)?;
        }

        Ok(parsed)
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
