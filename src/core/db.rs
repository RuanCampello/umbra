use crate::core::storage::page::PageNumber;

/// The metadata of a table we might need during runtime.
#[derive(Debug, PartialEq)]
pub(crate) struct TableMetadata {
    root: PageNumber,
}

pub(crate) trait Context {
    fn metadata(&self) -> &str;
}
