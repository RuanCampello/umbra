//! Here we do the preparation for [plan](crate::vm::planner::Planner) generation.

use crate::db::{Ctx, DatabaseError, ROW_COL_ID};

use super::statement::{Expression, Insert, Select, Statement, Type, Value};

/// Takes a given [statement](crate::sql::statement::Statement) and prepares it to the plan
/// generation.
///
/// Here we basic do three things:
///
/// 1.Remove wildcards for [select](crate::sql::statement::Select) statements.
/// 2.Reorder the values in [insert](crate::sql::statement::Insert) statements to match the [schema](crate::db::Schema) ordering.
/// 3.Increment `SERIAL` (and its variants) columns.
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
        Statement::Select(Select { columns, from, .. })
            if columns.iter().any(|expr| expr.eq(&Expression::Wildcard)) =>
        {
            let metadata = ctx.metadata(from)?;
            let identifiers: Vec<Expression> = metadata
                .schema
                .columns
                .iter()
                .filter(|&col| col.name.ne(&ROW_COL_ID))
                .cloned()
                .map(|col| Expression::Identifier(col.name))
                .collect();

            let mut wildcards = Vec::new();

            for expr in columns.drain(..) {
                match expr.eq(&Expression::Wildcard) {
                    true => wildcards.extend(identifiers.iter().cloned()),
                    false => wildcards.push(expr),
                }
            }

            *columns = wildcards
        }
        Statement::Insert(Insert {
            values,
            columns,
            into,
        }) => {
            let metadata = ctx.metadata(into)?;

            if columns.is_empty() {
                *columns = metadata.schema.columns_ids();
            }

            if metadata.schema.columns[0].name.eq(&ROW_COL_ID) {
                if columns[0].ne(&ROW_COL_ID) {
                    columns.insert(0, ROW_COL_ID.to_string());
                }

                values.iter_mut().for_each(|values| {
                    let row_id = metadata.next_id();
                    values.insert(0, Expression::Value(Value::Number(row_id.into())));
                });
            }

            // increment the `SERIAL` columns

            // missing serial columns and their schema index
            let serial_inserts: Vec<(usize, String)> = metadata
                .schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    match matches!(
                        col.data_type,
                        Type::SmallSerial | Type::Serial | Type::BigSerial
                    ) && !columns.contains(&col.name)
                    {
                        true => Some((idx, col.name.clone())),
                        false => None,
                    }
                })
                .collect();

            // insert them into the columns list
            for (idx, name) in &serial_inserts {
                columns.insert(*idx, name.clone());
            }

            // insert generated serial values into EACH row of values
            for row in values.iter_mut() {
                for (idx, name) in &serial_inserts {
                    let next_val = metadata.next_val(into.as_ref(), name)?;
                    row.insert(*idx, Expression::Value(Value::Number(next_val.into())));
                }
            }

            values.iter_mut().for_each(|values| {
                (0..columns.len()).for_each(|idx| {
                    let sorted_idx = metadata.schema.index_of(&columns[idx]).unwrap();

                    columns.swap(idx, sorted_idx);
                    values.swap(idx, sorted_idx);
                });
            })
        }
        Statement::Explain(inner) => prepare(&mut *inner, ctx)?,
        _ => {}
    };

    Ok(())
}
