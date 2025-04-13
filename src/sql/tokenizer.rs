//! SQL tokenizer that generates ours [tokens](Token) instances.

use crate::sql::tokens::{Keyword, Token, Whitespace};
use std::iter::Peekable;
use std::str::Chars;

struct Tokenizer<'input> {
    stream: Stream<'input>,
    reached_eof: bool,
}

struct TokenWithLocation {
    pub token: Token,
    pub location: Location,
}

/// A peekable chars stream of [tokens](Token), so we can read through it without consuming.
struct Stream<'input> {
    input: &'input str,
    location: Location,
    chars: Peekable<Chars<'input>>,
}

#[derive(PartialEq)]
struct TokenizerError {
    kind: ErrorKind,
    location: Location,
    input: String,
}

struct TakeWhile<'stream, 'input, Pred> {
    stream: &'stream mut Stream<'input>,
    predicate: Pred,
}

struct Iter<'token, 'input> {
    tokenizer: &'token mut Tokenizer<'input>,
}

struct IntoIter<'input> {
    tokenizer: Tokenizer<'input>,
}

#[derive(Clone, Copy, PartialEq)]
struct Location {
    line: usize,
    col: usize,
}

#[derive(PartialEq)]
enum ErrorKind {
    UnexpectedToken(char),
    UnexpectedOperator { unexpected: char, operator: Token },
    OperatorUnclosed(Token),
    StringUnclosed,
    Other(String),
}

type TokenizerResult = Result<Token, TokenizerError>;

impl<'input> Tokenizer<'input> {
    pub fn new(input: &'input str) -> Self {
        Self {
            stream: Stream::new(input),
            reached_eof: false,
        }
    }

    pub fn tokenizer(&mut self) -> Result<Vec<Token>, TokenizerError> {
        self.iter()
            .map(|token| token.map(TokenWithLocation::token_only))
            .collect()
    }

    fn consume(&mut self, token: Token) -> TokenizerResult {
        self.stream.next();
        Ok(token)
    }

    fn error(&self, kind: ErrorKind) -> TokenizerResult {
        Err(TokenizerError {
            kind,
            location: self.stream.location(),
            input: self.stream.input.to_string(),
        })
    }

    fn iter<'token>(&'token mut self) -> Iter<'token, 'input> {
        self.into_iter()
    }

    fn next_token(&mut self) -> Result<Token, TokenizerError> {
        let Some(c) = self.stream.peek() else {
            self.reached_eof = true;
            return Ok(Token::Eof);
        };

        match c {
            ' ' => self.consume(Token::Whitespace(Whitespace::Space)),
            '\t' => self.consume(Token::Whitespace(Whitespace::Tab)),
            '\n' => self.consume(Token::Whitespace(Whitespace::Newline)),
            '\r' => match self.stream.peek_next() {
                Some('\n') => self.consume(Token::Whitespace(Whitespace::Newline)),
                _ => Ok(Token::Whitespace(Whitespace::Newline)),
            },
            '<' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::Eq),
                _ => Ok(Token::Lt),
            },
            '>' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::Eq),
                _ => Ok(Token::Gt),
            },
            '!' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::Neq),
                Some(unexpected) => {
                    let error_kind = ErrorKind::UnexpectedOperator {
                        unexpected: *unexpected,
                        operator: Token::Neq,
                    };
                    self.error(error_kind)
                }

                None => self.error(ErrorKind::OperatorUnclosed(Token::Neq)),
            },
            '=' => self.consume(Token::Eq),
            ',' => self.consume(Token::Comma),
            '(' => self.consume(Token::LeftParen),
            ')' => self.consume(Token::RightParen),
            ';' => self.consume(Token::Semicolon),
            _ if Token::is_keyword(c) => self.tokenize_keyword(),
            _ => {
                let err = ErrorKind::UnexpectedToken(*c);
                self.error(err)
            }
        }
    }

    fn tokenize_keyword(&mut self) -> TokenizerResult {
        let value: String = self.stream.take_while(Token::is_keyword).collect();

        let keyword = match value.to_uppercase().as_str() {
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
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            "PRIMARY" => Keyword::Primary,
            "KEY" => Keyword::Key,
            "UNIQUE" => Keyword::Unique,
            "INT" => Keyword::Int,
            "BIGINT" => Keyword::BigInt,
            "BY" => Keyword::By,
            "DATABASE" => Keyword::Database,
            "UNSIGNED" => Keyword::Unsigned,
            "VARCHAR" => Keyword::Varchar,
            "BOOL" => Keyword::Bool,
            "INDEX" => Keyword::Index,
            "ORDER" => Keyword::Order,
            "ON" => Keyword::On,
            "START" => Keyword::Start,
            "TRANSACTION" => Keyword::Transaction,
            "ROLLBACK" => Keyword::Rollback,
            "COMMIT" => Keyword::Commit,
            "EXPLAIN" => Keyword::Explain,
            _ => Keyword::None,
        };

        Ok(match keyword {
            Keyword::None => Token::Identifier(value),
            _ => Token::Keyword(keyword),
        })
    }

    fn next_token_with_location(&mut self) -> Result<TokenWithLocation, TokenizerError> {
        let location = self.stream.location();

        self.next_token()
            .map(|token| TokenWithLocation { token, location })
    }

    fn op_next_token_with_location(&mut self) -> Option<Result<TokenWithLocation, TokenizerError>> {
        if self.reached_eof {
            return None;
        }

        Some(self.next_token_with_location())
    }
}

impl<'input> IntoIterator for Tokenizer<'input> {
    type Item = Result<TokenWithLocation, TokenizerError>;
    type IntoIter = IntoIter<'input>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter { tokenizer: self }
    }
}

impl<'token, 'input> IntoIterator for &'token mut Tokenizer<'input> {
    type Item = Result<TokenWithLocation, TokenizerError>;
    type IntoIter = Iter<'token, 'input>;

    fn into_iter(self) -> Self::IntoIter {
        Iter { tokenizer: self }
    }
}

impl TokenWithLocation {
    /// Returns the [Token] discarding the [Location]
    fn token_only(self) -> Token {
        self.token
    }

    fn token_ref(&self) -> &Token {
        &self.token
    }
}

impl<'input> Stream<'input> {
    fn new(input: &'input str) -> Self {
        Self {
            input,
            location: Location::default(),
            chars: input.chars().peekable(),
        }
    }

    /// Updates the [Location] and consumes the next char.
    fn next(&mut self) -> Option<char> {
        self.chars.next().inspect(|c| {
            match *c == Whitespace::Newline.as_char() {
                true => {
                    self.location.line += 1;
                    self.location.col += 1;
                }
                false => self.location.col += 1,
            };
        })
    }

    fn take_while<P: FnMut(&char) -> bool>(&mut self, predicate: P) -> TakeWhile<'_, 'input, P> {
        TakeWhile {
            stream: self,
            predicate,
        }
    }

    fn peek(&mut self) -> Option<&char> {
        self.chars.peek()
    }

    fn peek_next(&mut self) -> Option<&char> {
        self.next();
        self.peek()
    }

    fn location(&self) -> Location {
        self.location
    }
}

impl<'stream, 'input, P: FnMut(&char) -> bool> Iterator for TakeWhile<'stream, 'input, P> {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        if (self.predicate)(self.stream.peek()?) {
            self.stream.next()
        } else {
            None
        }
    }
}

impl<'token, 'input> Iterator for Iter<'token, 'input> {
    type Item = Result<TokenWithLocation, TokenizerError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokenizer.op_next_token_with_location()
    }
}

impl<'input> Iterator for IntoIter<'input> {
    type Item = Result<TokenWithLocation, TokenizerError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.tokenizer.op_next_token_with_location()
    }
}

impl Default for Location {
    fn default() -> Self {
        // yeah, Lua-esque, Brazil mentioned
        Location { line: 1, col: 1 }
    }
}
