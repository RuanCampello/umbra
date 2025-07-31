use super::{Cache, Input};
use crate::string::Match;
use core::{
    fmt::Debug,
    panic::{RefUnwindSafe, UnwindSafe},
};

/// Represents a single strategy. This provides a way of dynamic dispatch over choices.
///
/// This could be implemented with a enum for static dispatch, but whatever, we can't inline most
/// things in a regex engine.
pub(crate) trait Strategy:
    Debug + Send + Sync + RefUnwindSafe + UnwindSafe + 'static
{
    fn search(&self, cache: &mut Cache, input: &Input<'_>) -> Option<Match>;
}
