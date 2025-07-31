#[derive(Debug, Default, Clone)]
pub(crate) enum MatchKind {
    All,
    #[default]
    LeftmostFirst,
}
