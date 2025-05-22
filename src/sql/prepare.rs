use crate::db::{Ctx, DatabaseError, ROW_COL_ID};

use super::statement::{Expression, Statement, Value};

pub(crate) fn prepare(statement: &mut Statement, ctx: &mut impl Ctx) -> Result<(), DatabaseError> {
    match statement {
        Statement::Select { columns, from, .. }
            if columns.iter().any(|expr| expr.eq(&Expression::Wildcard)) =>
        {
            println!("preparing select ... {columns:#?} from {from}");
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
        Statement::Insert {
            into,
            columns,
            values,
        } => {
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

            values.iter_mut().for_each(|values| {
                (0..metadata.schema.len()).for_each(|idx| {
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
