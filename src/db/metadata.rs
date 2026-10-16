use crate::db::Schema;

/// What the analyzer and statement preparation know about a table
#[derive(Debug, PartialEq, Clone)]
pub(crate) struct TableMetadata {
    pub name: String,
    pub schema: Schema,
    pub indexes: Vec<IndexMetadata>,
}

#[derive(Debug, PartialEq, Clone)]
pub(crate) struct IndexMetadata {
    pub name: String,
    pub(crate) unique: bool,
}

#[macro_export]
/// — `primary on table_name`
/// — `primary on (table_name_variable)`
///
/// — `unique  on (table_name)(column_name)`
///
/// You can pass anything into `table_name` or `column_name` on `unique`
/// a string, variable etc, if you pass an non scoped variable or non string
/// it will make it a string.
///
/// On `primary` you can pass anything, but variables. To pass variables, put them inside
/// parenthesis.
macro_rules! index {
    // ── PRIMARY ───────────────────────────────────────────────────────
    (primary on ( $table:expr )) => {{ format!("{}_pk_index", $table) }};

    (primary on $table:ident) => {{ format!("{}_pk_index", stringify!($table)) }};

    // ── UNIQUE ────────────────────────────────────────────────────────
    (unique on ( $table:expr ) ( $col:expr )) => {{ format!("{}_{}_uq_index", $table, $col) }};

    (unique on $table:ident ( $col:ident )) => {{ format!("{}_{}_uq_index", stringify!($table), stringify!($col)) }};

    (unique on $table:ident ( $col:expr )) => {{ format!("{}_{}_uq_index", stringify!($table), $col) }};

    (unique on ( $table:expr ) ( $col:ident )) => {{ format!("{}_{}_uq_index", $table, stringify!($col)) }};
}
