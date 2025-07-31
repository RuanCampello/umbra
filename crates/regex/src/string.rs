//! Compiled representation of regular expression for searching.

use std::sync::Arc;

#[derive(Clone)]
pub struct Regex {
    regex: crate::fsm::Regex,
    pattern: Arc<str>,
}

impl Regex {
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.pattern
    }
}

impl core::fmt::Display for Regex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl core::fmt::Debug for Regex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_tuple("Regex").field(&self.as_str()).finish()
    }
}
