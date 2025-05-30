//! SQL tokenizer that generates ours [tokens](Token) instances.

#![allow(unused)]

use crate::sql::tokens::{Keyword, Token, Whitespace};
use std::iter::Peekable;
use std::str::Chars;

pub(in crate::sql) struct Tokenizer<'input> {
    stream: Stream<'input>,
    reached_eof: bool,
}

pub(in crate::sql) struct TokenWithLocation {
    pub token: Token,
    pub location: Location,
}

/// A peekable chars stream of [tokens](Token), so we can read through it without consuming.
struct Stream<'input> {
    input: &'input str,
    location: Location,
    chars: Peekable<Chars<'input>>,
}

#[derive(Debug, PartialEq)]
pub(in crate::sql) struct TokenizerError {
    pub kind: ErrorKind,
    pub location: Location,
    pub input: String,
}

struct TakeWhile<'stream, 'input, Pred> {
    stream: &'stream mut Stream<'input>,
    predicate: Pred,
}

pub(in crate::sql) struct Iter<'token, 'input> {
    tokenizer: &'token mut Tokenizer<'input>,
}

pub(in crate::sql) struct IntoIter<'input> {
    tokenizer: Tokenizer<'input>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::sql) struct Location {
    line: usize,
    col: usize,
}

#[derive(Debug, PartialEq)]
pub(in crate::sql) enum ErrorKind {
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

    pub fn tokenize(&mut self) -> Result<Vec<Token>, TokenizerError> {
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
                Some('=') => self.consume(Token::LtEq),
                _ => Ok(Token::Lt),
            },
            '>' => match self.stream.peek_next() {
                Some('=') => self.consume(Token::GtEq),
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
            '*' => self.consume(Token::Mul),
            '/' => self.consume(Token::Div),
            '+' => self.consume(Token::Plus),
            '-' => self.consume(Token::Minus),
            '=' => self.consume(Token::Eq),
            ',' => self.consume(Token::Comma),
            '(' => self.consume(Token::LeftParen),
            ')' => self.consume(Token::RightParen),
            ';' => self.consume(Token::Semicolon),
            '"' | '\'' => self.tokenize_string(),
            '0'..='9' => self.tokenize_number(),
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
            "SMALLSERIAL" => Keyword::SmallSerial,
            "SERIAL" => Keyword::Serial,
            "BIGSERIAL" => Keyword::BigSerial,
            "SMALLINT" => Keyword::SmallInt,
            "INT" | "INTEGER" => Keyword::Int,
            "BIGINT" => Keyword::BigInt,
            "BY" => Keyword::By,
            "DATABASE" => Keyword::Database,
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

        Ok(match keyword {
            Keyword::None => Token::Identifier(value),
            _ => Token::Keyword(keyword),
        })
    }

    fn tokenize_number(&mut self) -> TokenizerResult {
        Ok(Token::Number(
            self.stream.take_while(char::is_ascii_digit).collect(),
        ))
    }

    fn tokenize_string(&mut self) -> TokenizerResult {
        let quote = self.stream.next().unwrap();
        let string = self.stream.take_while(|c| *c != quote).collect();

        match self.stream.next().is_some_and(|c| c == quote) {
            true => Ok(Token::String(string)),
            false => self.error(ErrorKind::StringUnclosed),
        }
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
    pub fn token_ref(&self) -> &Token {
        &self.token
    }

    /// Returns the [Token] discarding the [Location]
    fn token_only(self) -> Token {
        self.token
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

impl Location {
    pub fn new(line: usize, col: usize) -> Self {
        Self { line, col }
    }
}

impl Default for Location {
    fn default() -> Self {
        // yeah, Lua-esque, Brazil mentioned
        Location { line: 1, col: 1 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize_select() {
        let sql = "SELECT id, name FROM users;";

        assert_eq!(
            Tokenizer::new(sql).tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("id".into()),
                Token::Comma,
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("name".into()),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::From),
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("users".into()),
                Token::Semicolon,
                Token::Eof,
            ])
        )
    }

    #[test]
    fn test_tokenize_where() {
        let sql = "SELECT id, price, discount FROM products WHERE price >= 100;";

        assert_eq!(
            Tokenizer::new(sql).tokenize(),
            Ok(vec![
                Token::Keyword(Keyword::Select),
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("id".into()),
                Token::Comma,
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("price".into()),
                Token::Comma,
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("discount".into()),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::From),
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("products".into()),
                Token::Whitespace(Whitespace::Space),
                Token::Keyword(Keyword::Where),
                Token::Whitespace(Whitespace::Space),
                Token::Identifier("price".into()),
                Token::Whitespace(Whitespace::Space),
                Token::GtEq,
                Token::Whitespace(Whitespace::Space),
                Token::Number("100".into()),
                Token::Semicolon,
                Token::Eof,
            ])
        )
    }

    #[test]
    fn test_unsupported_token() {
        let sql = "SELECT id, name FROM users WHERE name = @";

        assert_eq!(
            Tokenizer::new(sql).tokenize(),
            Err(TokenizerError {
                kind: ErrorKind::UnexpectedToken('@'),
                location: Location { line: 1, col: 41 },
                input: sql.into(),
            })
        )
    }

    #[test]
    fn test_tokenize_string() {
        let sql = "SELECT name FROM users WHERE name = 'John Doe";

        assert_eq!(
            Tokenizer::new(sql).tokenize(),
            Err(TokenizerError {
                kind: ErrorKind::StringUnclosed,
                location: Location { line: 1, col: 46 },
                input: sql.into(),
            })
        )
    }

    #[test]
    fn test_tokenize_double_string() {
        let sql = "SELECT name FROM users WHERE name = \"John Doe";

        assert_eq!(
            Tokenizer::new(sql).tokenize(),
            Err(TokenizerError {
                kind: ErrorKind::StringUnclosed,
                location: Location { line: 1, col: 46 },
                input: sql.into(),
            })
        )
    }
}
