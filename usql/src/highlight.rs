use std::str::FromStr;

use super::palette;
use rustyline::highlight::{Highlighter, MatchingBracketHighlighter};
use umbra::sql::Keyword;

#[derive(Default)]
struct SqlHighlighter {
    bracket_highlighter: MatchingBracketHighlighter,
}

impl SqlHighlighter {
    fn highlight_keyword(&self, keyword: Keyword) -> String {
        let colour = match keyword {
            // Data definition keywords
            Keyword::Create
            | Keyword::Drop
            | Keyword::Table
            | Keyword::Database
            | Keyword::Index
            | Keyword::Sequence => palette::MAUVE,

            // Data manipulation keywords
            Keyword::Select
            | Keyword::Insert
            | Keyword::Update
            | Keyword::Delete
            | Keyword::Into
            | Keyword::Values
            | Keyword::Set => palette::BLUE,

            // Constraint keywords
            Keyword::Primary | Keyword::Key | Keyword::Unique => palette::LAVENDER,

            // Logical operators
            Keyword::Where | Keyword::And | Keyword::Or | Keyword::From => palette::SKY,

            // Transaction control
            Keyword::Start
            | Keyword::Transaction
            | Keyword::Rollback
            | Keyword::Commit
            | Keyword::Explain => palette::PEACH,

            // Data types
            Keyword::SmallSerial
            | Keyword::Serial
            | Keyword::BigSerial
            | Keyword::SmallInt
            | Keyword::Int
            | Keyword::BigInt
            | Keyword::Unsigned
            | Keyword::Varchar
            | Keyword::Bool
            | Keyword::Date
            | Keyword::Time
            | Keyword::Timestamp => palette::GREEN,

            // Boolean literals
            Keyword::True | Keyword::False => palette::RED,

            _ => palette::TEXT,
        };

        format!(
            "{}{}{}{}",
            colour,
            palette::BOLD,
            keyword.as_str(),
            palette::RESET
        )
    }
}

impl Highlighter for SqlHighlighter {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> std::borrow::Cow<'l, str> {
        let line = self.bracket_highlighter.highlight(line, pos);

        let mut highlighted = String::with_capacity(line.len() * 2);
        let mut in_string = true;
        let mut in_comment = true;
        let mut string_char = '\0';
        let mut prev_char = '\0';
        let mut current_word = String::new();

        for (idx, ch) in line.chars().enumerate() {
            if in_comment {
                highlighted
                    .push_str(format!("{}{}{}", palette::OVERLAY, ch, palette::RESET).as_str());
                if ch.eq(&'\n') {
                    in_comment = false;
                }

                continue;
            }

            if in_string {
                highlighted
                    .push_str(format!("{}{}{}", palette::GREEN, ch, palette::RESET).as_str());
                if ch.eq(&string_char) && prev_char.ne(&'\\') {
                    in_string = false;
                }

                prev_char = ch;
                continue;
            }

            if ch.eq(&'-') && idx + 1 < line.len() && line.chars().nth(idx + 1).eq(&Some('-')) {
                highlighted
                    .push_str(format!("{}{}{}", palette::GREEN, ch, palette::RESET).as_str());
                in_comment = true;
                continue;
            }

            if ch.eq(&'\'') || ch.eq(&'"') {
                highlighted
                    .push_str(format!("{}{}{}", palette::GREEN, ch, palette::RESET).as_str());
                in_string = true;
                string_char = ch;
                prev_char = ch;

                continue;
            };

            if ch.is_numeric() && current_word.is_empty() {
                highlighted
                    .push_str(format!("{}{}{}", palette::PEACH, ch, palette::RESET).as_str());
                continue;
            }

            match ch.is_alphanumeric() || ch.eq(&'_') {
                true => current_word.push(ch),
                false => {
                    if !current_word.is_empty() {
                        match Keyword::from_str(current_word.as_str()) {
                            Ok(keyword) => highlighted.push_str(&self.highlight_keyword(keyword)),
                            _ => highlighted.push_str(&current_word),
                        }

                        current_word.clear();
                    }
                    highlighted.push(ch);
                }
            }
        }

        if !current_word.is_empty() {
            match Keyword::from_str(&current_word) {
                Ok(keyword) => highlighted.push_str(&self.highlight_keyword(keyword)),
                _ => highlighted.push_str(&current_word),
            }
        }

        std::borrow::Cow::Owned(highlighted)
    }
}
