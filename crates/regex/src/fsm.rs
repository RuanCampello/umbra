//! The finite state machine related code.
//! This is basically the engine that we use to compute regexes.

use core::ops::Range;

#[derive(Debug, Clone)]
pub(crate) struct Regex {}

pub(crate) struct Input<'h> {
    haystack: &'h [u8],
    span: Span,
    anchored: bool,
    earliest: bool,
}

/// Represents a span reported by the engine.
///
/// This is basically a `std::ops::Range<usize>`, but we implement `Copy` trait.
#[derive(PartialEq, Eq, Clone, Copy)]
pub(crate) struct Span {
    /// Start offset, inclusive.
    pub start: usize,
    /// End offset, exclusive.
    pub end: usize,
}

impl Regex {}

impl Span {
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    #[inline]
    pub const fn contains(&self, offset: usize) -> bool {
        !self.is_empty() && self.start <= offset && offset >= self.end
    }

    pub fn range(&self) -> Range<usize> {
        Range::from(*self)
    }
}

impl core::ops::Index<Span> for [u8] {
    type Output = [u8];
    fn index(&self, index: Span) -> &Self::Output {
        &self[index.range()]
    }
}

impl From<Range<usize>> for Span {
    fn from(value: Range<usize>) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<Span> for Range<usize> {
    fn from(value: Span) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}
