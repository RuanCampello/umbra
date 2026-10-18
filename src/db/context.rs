use crate::collections::hash::HashMap;
use crate::db::DatabaseError;
use crate::db::{Ctx, SqlError, TableMetadata};

/// The tables a session has resolved since the catalogue last changed
#[derive(Debug, Default)]
pub(crate) struct Context(HashMap<String, TableMetadata>);

impl Context {
    pub fn insert(&mut self, metadata: TableMetadata) {
        self.0.insert(metadata.name.clone(), metadata);
    }

    pub fn contains(&self, table: &str) -> bool {
        self.0.contains_key(table)
    }

    pub fn clear(&mut self) {
        self.0.clear();
    }
}

#[cfg(test)]
impl TryFrom<&[&str]> for Context {
    type Error = DatabaseError;

    fn try_from(statements: &[&str]) -> Result<Self, Self::Error> {
        use crate::db::Schema;
        use crate::sql::{
            parser::Parser,
            statement::{Create, Statement},
        };

        let mut context = Self::default();

        for sql in statements {
            match Parser::new(sql).parse_statement()? {
                Statement::Create(Create::Table { name, columns }) => {
                    let mut schema = Schema::from_definition(&columns);
                    schema.prepend_id();
                    context.insert(TableMetadata { name, schema });
                },
                stmt => {
                    let msg = format!("a test context takes CREATE TABLE only, found {stmt:#?}");
                    return Err(SqlError::Other(msg).into());
                },
            }
        }

        Ok(context)
    }
}

impl Ctx for Context {
    fn next_serial(&mut self, _table: &str, _column: usize) -> Result<i64, DatabaseError> {
        Ok(1)
    }

    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata, DatabaseError> {
        self.0
            .get_mut(table)
            .ok_or_else(|| SqlError::InvalidTable(table.to_string()).into())
    }
}
