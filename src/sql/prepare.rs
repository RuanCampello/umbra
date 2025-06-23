//! Here we do the final step preparation before [plan](crate::vm::planner::Planner) generation.

use crate::{
    core::uuid::Uuid,
    db::{Ctx, DatabaseError, ROW_COL_ID},
};

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

            // columns needing auto generated values (UUID or SERIAl variants)
            let auto_inserts: Vec<(usize, String, Type)> = metadata
                .schema
                .columns
                .iter()
                .enumerate()
                .filter_map(|(idx, col)| {
                    match col.data_type.can_be_autogen() && !columns.contains(&col.name) {
                        true => Some((idx, col.name.clone(), col.data_type)),
                        false => None,
                    }
                })
                .collect();

            for (idx, name, _) in &auto_inserts {
                columns.insert(*idx, name.clone());
            }

            // insert generated values into EACH row of values
            for row in values.iter_mut() {
                for (idx, name, r#type) in &auto_inserts {
                    let expr = match r#type {
                        Type::Uuid => Expression::Value(Value::Uuid(Uuid::new_v4())),
                        _ => Expression::Value(Value::Number(
                            metadata.next_val(into.as_ref(), name)?.into(),
                        )),
                    };

                    row.insert(*idx, expr);
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
