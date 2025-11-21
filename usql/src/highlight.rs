use rustyline::{Completer, Helper, Hinter, Validator, highlight::Highlighter};
use std::{borrow::Cow, str::FromStr};
use umbra::sql::Keyword;

pub(crate) const KEYWORD_COLOUR: &str = "\x1b[38;5;183m";
pub(crate) const STRING_COLOUR: &str = "\x1b[38;5;222m";
pub(crate) const RESET_COLOUR: &str = "\x1b[0m";
pub(crate) const TIME_COLOUR: &str = "\x1b[38;5;111m";

#[derive(Completer, Helper, Hinter, Validator)]
pub(crate) struct SqlHighlighter;

impl Highlighter for SqlHighlighter {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        let mut result = String::new();
        let mut current_word = String::new();
        let mut in_string = None;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' | '\'' => {
                    if let Some(quote) = in_string {
                        match quote == ch {
                            true => {
                                current_word.push(ch);
                                result.push_str(STRING_COLOUR);
                                result.push_str(&current_word);
                                result.push_str(RESET_COLOUR);
                                current_word.clear();
                                in_string = None;
                            }
                            _ => current_word.push(ch),
                        }
                    } else {
                        if !current_word.is_empty() {
                            Self::flush_word(&mut result, &current_word);
                            current_word.clear();
                        }
                        in_string = Some(ch);
                        current_word.push(ch);
                    }
                }
                _ if in_string.is_some() => {
                    current_word.push(ch);
                }
                ' ' | '\n' | '\t' | '(' | ')' | ',' | ';' | '=' | '<' | '>' | '+' | '-' | '*'
                | '/' => {
                    if !current_word.is_empty() {
                        Self::flush_word(&mut result, &current_word);
                        current_word.clear();
                    }
                    result.push(ch);
                }
                _ => {
                    current_word.push(ch);
                }
            }
        }

        if in_string.is_some() {
            result.push_str(STRING_COLOUR);
            result.push_str(&current_word);
            result.push_str(RESET_COLOUR);
        } else if !current_word.is_empty() {
            Self::flush_word(&mut result, &current_word);
        }

        Cow::Owned(result)
    }

    fn highlight_char(
        &self,
        _line: &str,
        _pos: usize,
        _kind: rustyline::highlight::CmdKind,
    ) -> bool {
        true
    }
}

impl SqlHighlighter {
    fn flush_word(result: &mut String, word: &str) {
        if let Ok(keyword) = Keyword::from_str(word) {
            if !matches!(keyword, Keyword::None) {
                result.push_str(KEYWORD_COLOUR);
                result.push_str(word);
                result.push_str(RESET_COLOUR);
                return;
            }
        }

        result.push_str(word);
    }
}
