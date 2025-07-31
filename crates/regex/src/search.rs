#[derive(Debug, Default, Clone, Copy)]
pub(crate) enum MatchKind {
    All,
    #[default]
    LeftmostFirst,
}
