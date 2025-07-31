//! The finite state machine related code.
//! This is basically the engine that we use to compute regexes.

use core::num::NonZeroUsize;
use core::ops::Range;
mod strategy;

#[derive(Debug, Clone)]
pub(crate) struct Regex {}

pub(crate) struct Input<'h> {
    haystack: &'h [u8],
    span: Span,
    anchored: Anchored,
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

#[derive(Debug, Clone)]
pub struct Cache {
    slots: Vec<Option<NonZeroUsize>>,
    slots_len: usize,
}

/// The identifier of a given regular expression.
#[derive(PartialEq, Eq, PartialOrd, Ord, Default, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct PatternId(u32);

/// The kind of anchored search to perform.
/// There's a quite good article about regex anchors, you can read it [here](https://www.rexegg.com/regex-anchors.php)
#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub(crate) enum Anchored {
    Yes,
    #[default]
    No,
    Pattern(PatternId),
}

impl Regex {}

impl<'h> Input<'h> {
    pub(crate) fn new<H: Sized + AsRef<[u8]>>(haystack: &'h H) -> Self {
        let haystack = haystack.as_ref();
        Self {
            haystack,
            span: Span {
                start: 0,
                end: haystack.len(),
            },
            anchored: Anchored::default(),
            earliest: false,
        }
    }

    pub(crate) fn span<S: Into<Span>>(&mut self, span: S) {
        let span = span.into();

        assert!(
            span.end <= self.haystack.len() && span.start <= span.end.wrapping_add(1),
            "Invalid span {span:?} for haystack with length {}",
            self.haystack.len()
        );

        self.span = span;
    }
}

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

impl core::fmt::Debug for PatternId {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("PatternId").field(&self.0).finish()
    }
}

impl core::fmt::Debug for Span {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}..{}", self.start, self.end)
    }
}
