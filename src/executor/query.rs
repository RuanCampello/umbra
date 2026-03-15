//! SELECT query pipeline builder.
//!
//! Translates a parsed `Select` AST node into an operator pipeline:
//! `Scan → Filter → Sort → Project → Limit`.

use super::operator::{self, Operator};
use super::Executor;
use crate::db::{DatabaseError, Schema};
use crate::sql::statement::{self, Expression, OrderDirection};
use crate::vm::planner::Tuple;

impl Executor {
    /// Builds and drains a SELECT operator pipeline, returning all result rows.
    pub fn execute_select(
        &self,
        txn_id: i64,
        select: statement::Select,
    ) -> Result<Vec<Tuple>, DatabaseError> {
        let table = &select.from.name;

        let schema = self.resolve_schema(table)?;
        let mut pipeline: Box<dyn Operator> =
            Box::new(operator::Scan::new(&self.engine, txn_id, table)?);

        if let Some(predicate) = select.r#where {
            pipeline = Box::new(operator::Filter::new(pipeline, schema.clone(), predicate));
        }

        if !select.order_by.is_empty() {
            let sort_schema = schema.clone();
            let order_by = select.order_by;

            let comparator = build_comparator(&sort_schema, &order_by);
            pipeline = Box::new(operator::Sort::new(pipeline, comparator)?);
        }

        let project_indices = resolve_projection(&schema, &select.columns);
        if let Some(indices) = project_indices {
            pipeline = Box::new(operator::Project::new(pipeline, indices));
        }

        if select.limit.is_some() || select.offset.is_some() {
            let limit = select.limit.unwrap_or(usize::MAX);
            let offset = select.offset.unwrap_or(0);
            pipeline = Box::new(operator::Limit::new(pipeline, limit, offset));
        }

        let mut results = Vec::new();
        while let Some(tuple) = pipeline.next()? {
            results.push(tuple);
        }

        Ok(results)
    }

    /// Resolves the old `Schema` (column list) for a table from the engine's stored schema.
    fn resolve_schema(&self, table: &str) -> Result<Schema, DatabaseError> {
        let schema_new = self.engine.schema(table)?;

        let columns: Vec<statement::Column> = schema_new
            .columns
            .iter()
            .map(|col| statement::Column::new(col.name(), col.column_type()))
            .collect();

        Ok(Schema::new(columns))
    }
}

fn resolve_projection(schema: &Schema, columns: &[Expression]) -> Option<Vec<usize>> {
    if columns.len() == 1 && columns[0] == Expression::Wildcard {
        return None;
    }

    let indices = columns
        .iter()
        .filter_map(|expr| match expr {
            Expression::Identifier(name) => schema.index_of(name),
            _ => None,
        })
        .collect();

    Some(indices)
}

fn build_comparator(
    schema: &Schema,
    order_by: &[statement::OrderBy],
) -> Box<dyn Fn(&Tuple, &Tuple) -> std::cmp::Ordering> {
    let sort_keys: Vec<(usize, bool)> = order_by
        .iter()
        .filter_map(|ob| {
            let col_name = match &ob.expr {
                Expression::Identifier(name) => name.as_str(),
                _ => return None,
            };

            let idx = schema.index_of(col_name)?;
            let desc = ob.direction == OrderDirection::Desc;

            Some((idx, desc))
        })
        .collect();

    Box::new(move |a: &Tuple, b: &Tuple| {
        for &(idx, desc) in &sort_keys {
            let ord = a[idx]
                .partial_cmp(&b[idx])
                .unwrap_or(std::cmp::Ordering::Equal);
            if ord != std::cmp::Ordering::Equal {
                return if desc { ord.reverse() } else { ord };
            }
        }

        std::cmp::Ordering::Equal
    })
}
