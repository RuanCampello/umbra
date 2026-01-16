use crate::{
    core::HashMap,
    sql::{statement::Type, Value},
};
use core::fmt;
use std::{
    cell::OnceCell,
    fmt::Display,
    sync::{Arc, OnceLock},
};

/// The representation of the table schema.
#[derive(Debug, Default, Clone)]
pub struct Schema {
    pub name: String,
    pub columns: Vec<Column>,
    pub created_at: u64,
    pub updated_at: u64,

    // Cached column names, computed on the first access
    column_names: OnceLock<Arc<Vec<String>>>,

    /// Cached column index map, computed on the first access
    /// column name -> index
    column_index: OnceLock<HashMap<String, usize>>,
}

#[derive(Debug, PartialEq, Clone)]
pub struct Column {
    id: usize, // TODO: SUPPORT OTHER TYPES OF ID
    name: String,
    nullable: bool,
    r#type: Type,
    /// Whether this column is a part of a primary key
    primary_key: bool,
    increment: bool,
    default: Option<String>,
    default_value: Option<Value>,
}

impl Schema {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        let now = crate::core::date::now() as u64;
        let column_names = OnceLock::new();
        let column_index = OnceLock::new();

        let _ = column_names.set(Arc::new(
            columns.iter().map(|col| col.name.to_string()).collect(),
        ));
        let _ = column_index.set(
            columns
                .iter()
                .enumerate()
                .map(|(idx, col)| (col.name.clone(), idx))
                .collect(),
        );

        Self {
            columns,
            name: name.into(),
            column_index,
            column_names,
            created_at: now,
            updated_at: now,
        }
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let headers = vec!["Column", "Type", "Nullable"];

        let rows: Vec<Vec<String>> = self
            .columns
            .iter()
            .map(|col| {
                vec![
                    col.name.clone(),
                    col.r#type.to_string(),
                    match col.nullable {
                        false => "not null".to_string(),
                        _ => "".to_string(),
                    },
                ]
            })
            .collect();

        let widths: Vec<usize> = headers
            .iter()
            .enumerate()
            .map(|(i, header)| {
                let max_data_len = rows.iter().map(|row| row[i].len()).max().unwrap_or(0);
                header.len().max(max_data_len)
            })
            .collect();

        let border: String = widths
            .iter()
            .map(|&w| "-".repeat(w + 2))
            .collect::<Vec<_>>()
            .join("+");
        let full_border = format!("+{}+", border);

        let header_str = headers
            .iter()
            .zip(&widths)
            .map(|(h, &w)| format!(" {:<width$} ", h, width = w))
            .collect::<Vec<_>>()
            .join("|");

        writeln!(f, "{}", full_border)?;
        writeln!(f, "|{}|", header_str)?;
        writeln!(f, "{}", full_border)?;

        if self.columns.is_empty() {
            let total_width = widths.iter().sum::<usize>() + (widths.len() * 3) - 1;
            writeln!(
                f,
                "| {:<width$} |",
                "(No columns defined)",
                width = total_width
            )?;
        } else {
            for row in rows {
                let row_str = row
                    .iter()
                    .zip(&widths)
                    .enumerate()
                    .map(|(i, (val, &width))| match i == 2 {
                        true => format!(" {:^width$} ", val, width = width),
                        _ => format!(" {:<width$} ", val, width = width),
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                writeln!(f, "|{}|", row_str)?;
            }
        }

        writeln!(f, "{}", full_border)?;
        Ok(())
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.created_at == other.created_at
            && self.updated_at == other.updated_at
            && self.columns == other.columns
    }
}
