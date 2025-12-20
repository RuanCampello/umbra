use super::JsonError;
use crate::parse_error;
use std::borrow::Cow;

#[derive(Debug, PartialEq, Clone)]
pub struct JsonPath<'p> {
    pub elements: Vec<PathElement<'p>>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum PathElement<'p> {
    Root,
    Key(Cow<'p, str>, RawString),
    ArrayLocator(Option<i32>),
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum State {
    Start,
    AfterRoot,
    InKey,
    InArrayIndex,
    ExpectDotOrBracket,
}

#[derive(Debug, PartialEq, Clone, Copy)]
enum ArrayIndexState {
    Start,
    AfterHash,
    CollectingNumbers,
    IsMax,
}

type RawString = bool;
type IsMaxNumber = bool;
type IdxChar = (usize, char);

pub fn json_path(path: &str) -> super::Result<JsonPath<'_>> {
    if path.is_empty() {
        parse_error!("Bad JSON path: {path}");
    }

    let state = &mut State::Start;
    let index_state = &mut ArrayIndexState::Start;
    let key = &mut 0;
    let index = &mut 0;
    let mut parts = Vec::with_capacity(capacitiy(path));
    let path_iter = &mut path.char_indices();

    while let Some(c) = path_iter.next() {
        match state {
            State::Start => start(c, state, &mut parts, path)?,
            State::AfterRoot => after_root(c, state, index_state, key, index, path)?,
            #[rustfmt::skip]
            State::InKey => in_key(c, state, index_state, key, index, &mut parts, path_iter, path)?,
            #[rustfmt::skip]
            State::ExpectDotOrBracket => expect_dot_or_bracket(c, state, index_state, key, index, path)?,
            #[rustfmt::skip]
            State::InArrayIndex => array_index(c, state, index_state, index, &mut parts, path_iter, path)?,
        }
    }

    finalise(state, *key, &mut parts, path)?;
    Ok(JsonPath { elements: parts })
}

fn start(
    c: IdxChar,
    state: &mut State,
    parts: &mut Vec<PathElement>,
    path: &str,
) -> super::Result<()> {
    match c {
        (_, '$') => {
            parts.push(PathElement::Root);
            *state = State::AfterRoot;
            Ok(())
        }
        _ => parse_error!("Bad JSON path: {path}"),
    }
}

fn after_root(
    c: IdxChar,
    state: &mut State,
    index_state: &mut ArrayIndexState,
    key: &mut usize,
    index: &mut i128,
    path: &str,
) -> super::Result<()> {
    match c {
        (idx, '.') => {
            *state = State::InKey;
            *key = idx + c.1.len_utf8();

            Ok(())
        }
        (_, '[') => {
            *state = State::InArrayIndex;
            *index_state = ArrayIndexState::Start;
            *index = 0;

            Ok(())
        }
        _ => parse_error!("Bad JSON path: {path}"),
    }
}

fn in_key<'p>(
    c: IdxChar,
    state: &mut State,
    index_state: &mut ArrayIndexState,
    key_start: &mut usize,
    index: &mut i128,
    parts: &mut Vec<PathElement<'p>>,
    path_iter: &mut std::str::CharIndices,
    path: &'p str,
) -> super::Result<()> {
    match c {
        (idx, '.' | '[') => {
            let end = idx;

            match end > *key_start {
                false => parse_error!("Bad JSON path: {path}"),
                _ => {
                    let key = &path[*key_start..end];

                    match c.1 == '[' {
                        false => *key_start = idx + c.1.len_utf8(),
                        _ => {
                            *state = State::InArrayIndex;
                            *index_state = ArrayIndexState::Start;
                            *index = 0;
                        }
                    }

                    parts.push(PathElement::Key(Cow::Borrowed(key), false));
                }
            }
        }
        (_, '"') => quoted_key(state, key_start, parts, path_iter, path)?,
        _ => (),
    }

    Ok(())
}

fn array_index(
    c: IdxChar,
    state: &mut State,
    index_state: &mut ArrayIndexState,
    index: &mut i128,
    parts: &mut Vec<PathElement<'_>>,
    path_iter: &mut std::str::CharIndices,
    path: &str,
) -> super::Result<()> {
    match (&index_state, c.1) {
        (ArrayIndexState::Start, '#') => *index_state = ArrayIndexState::AfterHash,
        (ArrayIndexState::Start, '0'..='9') => {
            *index = c.1.to_digit(10).ok_or(JsonError::Message {
                msg: format!("failed to parse digit: {}", c.1),
                location: None,
            })? as i128;

            *index_state = ArrayIndexState::CollectingNumbers;
        }
        (ArrayIndexState::AfterHash, '-') => negative_index(index_state, index, path_iter, path)?,
        (ArrayIndexState::AfterHash, ']') => {
            *state = State::ExpectDotOrBracket;
            parts.push(PathElement::ArrayLocator(None));
        }
        (ArrayIndexState::CollectingNumbers, '0'..='9') => {
            let adding = c.1.to_digit(10).ok_or(JsonError::Message {
                msg: format!("failed to parse digit: {}", c.1),
                location: None,
            })? as i128;
            let (num, is_max) = collect_num(*index, adding, *index < 0);

            #[rustfmt::skip]
            if is_max { *index_state = ArrayIndexState::IsMax };

            *index = num;
        }
        (ArrayIndexState::IsMax, '0'..='9') => (),
        (ArrayIndexState::CollectingNumbers | ArrayIndexState::IsMax, ']') => {
            *state = State::ExpectDotOrBracket;
            parts.push(PathElement::ArrayLocator(Some(*index as i32)));
        }
        _ => parse_error!("Bad JSON path: {path}"),
    }

    Ok(())
}

fn expect_dot_or_bracket(
    c: IdxChar,
    state: &mut State,
    index_state: &mut ArrayIndexState,
    key: &mut usize,
    index: &mut i128,
    path: &str,
) -> super::Result<()> {
    match c {
        (idx, '.') => {
            *key = idx + c.1.len_utf8();
            *state = State::InKey;

            Ok(())
        }
        (_, '[') => {
            *state = State::InArrayIndex;
            *index_state = ArrayIndexState::Start;
            *index = 0;

            Ok(())
        }
        _ => parse_error!("Bad JSON path: {path}"),
    }
}

fn finalise<'p>(
    state: &mut State,
    key_start: usize,
    parts: &mut Vec<PathElement<'p>>,
    path: &'p str,
) -> super::Result<()> {
    Ok(match state {
        State::InArrayIndex => parse_error!("Bad JSON path: {path}"),
        State::InKey => match key_start < path.len() {
            false => parse_error!("Bad JSON path: {path}"),
            _ => {
                let key = &path[key_start..];

                let is_quoted = key.starts_with('"') && key.ends_with('"');
                if is_quoted && key.len() > 1 || is_quoted && key.len() == 1 {
                    parse_error!("Bad JSON path: {path}")
                }

                parts.push(PathElement::Key(Cow::Borrowed(key), false))
            }
        },
        _ => (),
    })
}

fn quoted_key<'p>(
    state: &mut State,
    key_start: &mut usize,
    parts: &mut Vec<PathElement<'p>>,
    path_iter: &mut std::str::CharIndices,
    path: &'p str,
) -> super::Result<()> {
    while let Some((idx, c)) = path_iter.next() {
        match c {
            '\\' => {
                path_iter.next();
            }
            '"' => {
                if *key_start < idx {
                    let key = &path[*key_start + 1..idx];
                    parts.push(PathElement::Key(Cow::Borrowed(key), true));
                    *state = State::ExpectDotOrBracket;

                    return Ok(());
                }
            }
            _ => continue,
        }
    }

    Ok(())
}

fn negative_index(
    index_state: &mut ArrayIndexState,
    index: &mut i128,
    path_iter: &mut std::str::CharIndices,
    path: &str,
) -> super::Result<()> {
    match path_iter.next() {
        Some((_, c)) => {
            *index = -(c.to_digit(10).ok_or(JsonError::Message {
                msg: format!("failed to parse digit: {c}"),
                location: None,
            })? as i128);
            *index_state = ArrayIndexState::CollectingNumbers;

            Ok(())
        }
        None => parse_error!("Bad JSON path: {path}"),
    }
}

const fn collect_num(current: i128, adding: i128, negative: bool) -> (i128, IsMaxNumber) {
    let ten = 10;

    let result = match negative {
        true => current.saturating_mul(ten).saturating_sub(adding),
        _ => current.saturating_mul(ten).saturating_add(adding),
    };

    let is_max = result == i128::MAX || result == i128::MIN;
    (result, is_max)
}

const fn capacitiy(input: &str) -> usize {
    1 + (input.len() - 1) / 2
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_root() {
        let path = json_path("$").unwrap();

        assert!(path.elements.len() == 1);
        assert_eq!(path.elements.first(), Some(&PathElement::Root));
    }

    #[test]
    fn json_path_basic() {
        let path = json_path("$.store.book[0].description").unwrap();

        assert!(path.elements.len() == 5);
        assert_eq!(path.elements.first(), Some(&PathElement::Root));
        assert_eq!(
            path.elements[1],
            PathElement::Key(Cow::Borrowed("store"), false)
        );
        assert_eq!(
            path.elements[2],
            PathElement::Key(Cow::Borrowed("book"), false)
        );
        assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(0)));
        assert_eq!(
            path.elements[4],
            PathElement::Key(Cow::Borrowed("description"), false)
        )
    }

    #[test]
    fn nested_path() {
        let path = json_path("$[0][1][2].key[3].other").unwrap();

        assert!(path.elements.len() == 7);
        assert_eq!(path.elements[0], PathElement::Root);
        assert_eq!(path.elements[1], PathElement::ArrayLocator(Some(0)));
        assert_eq!(path.elements[2], PathElement::ArrayLocator(Some(1)));
        assert_eq!(path.elements[3], PathElement::ArrayLocator(Some(2)));
        assert_eq!(
            path.elements[4],
            PathElement::Key(Cow::Borrowed("key"), false)
        );
        assert_eq!(path.elements[5], PathElement::ArrayLocator(Some(3)));
    }

    #[test]
    fn error_cases() {
        assert!(json_path("$.").is_err());
        assert!(json_path("$..key").is_err());

        assert!(json_path("$[0").is_err());
        assert!(json_path("$[").is_err());
        assert!(json_path("$[-1]").is_err());
    }
}
