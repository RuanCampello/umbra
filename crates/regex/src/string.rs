//! Compiled representation of regular expression for searching.

use core::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

#[derive(Clone)]
pub struct Regex {
    regex: crate::fsm::Regex,
    pattern: Arc<str>,
}

/// Represents a single match of the regex in a given haystack.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Match<'h> {
    haystack: &'h str,
    start: usize,
    end: usize,
}

impl Regex {
    pub fn find_at<'h>(&self, haystack: &'h str, from: usize) -> Option<Match<'h>> {
        todo!()
    }

    pub fn find<'h>(&self, haystack: &'h str) -> Option<Match<'h>> {
        self.find_at(haystack, 0)
    }

    #[inline]
    pub fn as_str(&self) -> &str {
        &self.pattern
    }
}

impl<'h> Match<'h> {
    #[inline]
    pub fn new(haystack: &'h str, start: usize, end: usize) -> Self {
        Self {
            haystack,
            start,
            end,
        }
    }

    #[inline]
    /// Returns the string that the haystack matched.
    pub fn as_str(&self) -> &'h str {
        &self.haystack[self.start..self.end]
    }
}

impl Display for Regex {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Debug for Regex {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Regex").field(&self.as_str()).finish()
    }
}

impl<'h> Debug for Match<'h> {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Match")
            .field("string", &self.haystack)
            .field("start", &self.start)
            .field("end", &self.end)
            .finish()
    }
}

impl<'h> From<Match<'h>> for core::ops::Range<usize> {
    fn from(value: Match<'h>) -> Self {
        value.start..value.end
    }
}
