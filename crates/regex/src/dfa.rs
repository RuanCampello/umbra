//! A deterministic finite automaton (DFA) building module.

use crate::search::MatchKind;

pub(crate) struct Dfa {
    config: Config,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct Config {
    match_kind: Option<MatchKind>,
    starts_for_pattern: Option<bool>,
    byte_classes: Option<bool>,
    limit: Option<Option<usize>>,
}

impl Config {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn overwrite(&self, o: Config) -> Config {
        Config {
            match_kind: o.match_kind.or(self.match_kind),
            starts_for_pattern: o.starts_for_pattern.or(self.starts_for_pattern),
            byte_classes: o.byte_classes.or(self.byte_classes),
            limit: o.limit.or(self.limit),
        }
    }

    pub fn match_kind(mut self, match_kind: MatchKind) -> Self {
        self.match_kind = Some(match_kind);
        self
    }

    pub fn starts_for_pattern(mut self, yes: bool) -> Self {
        self.starts_for_pattern = Some(yes);
        self
    }

    pub fn byte_classes(mut self, yes: bool) -> Self {
        self.byte_classes = Some(yes);
        self
    }

    pub fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn get_match_kind(&self) -> MatchKind {
        self.match_kind.unwrap_or(MatchKind::default())
    }

    pub fn get_starts_for_pattern(&self) -> bool {
        self.starts_for_pattern.unwrap_or(false)
    }

    pub fn get_byte_classes(&self) -> bool {
        self.byte_classes.unwrap_or(true)
    }

    pub fn get_limit(&self) -> Option<usize> {
        self.limit.unwrap_or(None)
    }
}
