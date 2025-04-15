use std::iter::Peekable;
use crate::sql::statement::Statement;
use crate::sql::tokenizer::{self, Location, TokenWithLocation, Tokenizer, TokenizerError};
use crate::sql::tokens::Token;

pub(crate) struct Parser<'input> {
    input: &'input str,
    tokenizer: Peekable<tokenizer::IntoIter<'input>>,
    location: Location,
}

pub(crate) struct ParserError {
    pub kind: ErrorKind,
    pub location: Location,
    pub input: String,
}

enum ErrorKind {
    TokenizerError(tokenizer::ErrorKind),
    Expected { expected: Token, found: Token },
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

    fn peek_token(&mut self) -> Option<Result<&Token, &TokenizerError>> {
        self.skip_whitespaces();
        self.peek_token_stream()
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

    fn error(&self, kind: ErrorKind) -> ParserError {
        ParserError {
            kind,
            input: self.input.to_string(),
            location: self.location,
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
