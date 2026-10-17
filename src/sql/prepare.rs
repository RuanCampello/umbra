//! Here we do the final step preparation before [plan](crate::vm::planner::Planner) generation.

use super::statement::{Expression, Insert, Select, Statement, Type, Value};
use crate::{
    core::uuid::Uuid,
    db::{Ctx, DatabaseError, ROW_COL_ID},
    sql::statement::Update,
};

/// Takes a given [statement](crate::sql::statement::Statement) and prepares it to the plan
/// generation.
///
/// Here we basic do four things:
///
/// 1.Remove wildcards for [select](crate::sql::statement::Select) statements.
/// 2.Reorder the values in [insert](crate::sql::statement::Insert) statements to match the [schema](crate::db::Schema) ordering.
/// 3.Increment `SERIAL` (and its variants) columns.
/// 4.Combine [schemas](crate::db::Schema) of possible table joins.
///
/// ```sql
/// -- table
/// CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(255), age INT UNSIGNED);
///
/// -- select
/// SELECT * FROM employees;
///
/// -- prepared select statement
/// SELECT id, name, age FROM employees;
///
/// -- insert
/// INSERT INTO employees (age, name) VALUES (22, 'John Doe');
///
/// -- prepared select statement
/// INSERT INTO employees (id, name, age) VALUES (x, 'John Doe', 22) -- where 'x' is the next_val() of this given `SERIAL`.
/// ```
pub(crate) fn prepare(statement: &mut Statement, ctx: &mut impl Ctx) -> Result<(), DatabaseError> {
    match statement {
        Statement::Select(Select { columns, from, joins, .. })
            if columns.iter().any(|expr| expr.eq(&Expression::Wildcard)) =>
        {
            // with joins, names repeat across tables: each column is qualified by its table and
            // keeps its bare name as the output name
            let qualify = !joins.is_empty();
            let reference = |table: &str, column: &str| match qualify {
                true => Expression::Alias {
                    alias: column.to_string(),
                    expr: Box::new(Expression::QualifiedIdentifier {
                        table: table.to_string(),
                        column: column.to_string(),
                    }),
                },
                _ => Expression::Identifier(column.to_string()),
            };

            let mut identifiers = Vec::new();
            for table in std::iter::once(&*from).chain(joins.iter().map(|join| &join.table)) {
                let metadata = ctx.metadata(&table.name)?;
                identifiers.extend(
                    metadata
                        .schema
                        .columns
                        .iter()
                        .filter(|col| col.name() != ROW_COL_ID)
                        .map(|col| reference(table.key(), col.name())),
                );
            }

            expand_wildcard_with_identifier(columns, identifiers);
        },
        Statement::Insert(Insert { values, columns, into, returning }) => {
            let metadata = ctx.metadata(into)?;

            if columns.is_empty() {
                *columns = metadata.schema.columns_ids();
            }

            // omitted columns take their default, an auto generated value (UUID or SERIAL), or
            // NULL when nullable
            let auto_inserts: Vec<_> = metadata
                .schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    let named = columns.iter().any(|column| column == col.name());
                    let filled = col.default().is_some()
                        || col.column_type().can_be_autogen()
                        || col.is_nullable();
                    match filled && !named {
                        true => Some((idx, col.name().to_string(), col.column_type(), col.default().cloned())),
                        false => None,
                    }
                })
                .collect();

            for (idx, name, ..) in &auto_inserts {
                columns.insert(*idx, name.clone());
            }

            // insert generated values into EACH row of values
            // serials route through the context so the MVCC session draws from the shared
            // engine counter rather than a per-session one
            for row in values.iter_mut() {
                for (idx, _, r#type, default) in &auto_inserts {
                    let expr = match (default, r#type) {
                        (Some(default), _) => Expression::Value(default.clone()),
                        (None, Type::Uuid) => Expression::Value(Value::Uuid(Uuid::new_v4())),
                        (None, r#type) if r#type.can_be_autogen() => {
                            Expression::Value(ctx.next_serial(into.as_ref(), *idx)?.into())
                        },
                        (None, _) => Expression::Value(Value::Null),
                    };

                    row.insert(*idx, expr);
                }
            }

            let metadata = ctx.metadata(into)?;

            (0..columns.len()).for_each(|idx| {
                if let Some(sorted_idx) = {
                    let sorted_idx = metadata.schema.index_of(&columns[idx]).unwrap();
                    (idx != sorted_idx).then_some(sorted_idx)
                } {
                    columns.swap(idx, sorted_idx);
                    values.iter_mut().for_each(|values| values.swap(idx, sorted_idx));
                }
            });

            let identifiers: Vec<_> = metadata
                .schema
                .columns
                .iter()
                .map(|col| Expression::Identifier(col.name().to_string()))
                .collect();

            expand_wildcard_with_identifier(returning, identifiers);
        },
        Statement::Update(Update { table, returning, .. }) => {
            let metadata = ctx.metadata(table)?;

            let identifiers: Vec<_> = metadata
                .schema
                .columns
                .iter()
                .map(|col| Expression::Identifier(col.name().to_string()))
                .collect();

            expand_wildcard_with_identifier(returning, identifiers);
        },
        Statement::Explain(inner) => prepare(&mut *inner, ctx)?,
        _ => {},
    };

    Ok(())
}

fn expand_wildcard_with_identifier(
    expressions: &mut Vec<Expression>,
    identifiers: Vec<Expression>,
) {
    let mut expanded = Vec::new();

    for expr in expressions.drain(..) {
        match expr.eq(&Expression::Wildcard) {
            true => expanded.extend(identifiers.iter().cloned()),
            false => expanded.push(expr),
        }
    }

    *expressions = expanded
}
