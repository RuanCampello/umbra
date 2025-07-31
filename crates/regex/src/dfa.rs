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
    fn new() -> Self {
        Self::default()
    }
}
