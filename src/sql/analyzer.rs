//! This module is responsible for doing the semantic and context aware analysis over the
//! parsed `SQL` [statements](crate::sql::statement::Statement).
//!
//! After this analysis, we should be able to handle the execution without almost no runtime error
//! besides from edge cases like [division by zero](crate::vm::expression::VmError), overflow, et cetera.

use super::statement::{Delete, Insert, Select, Update};
use crate::core::date::interval::Interval;
use crate::core::date::{NaiveDate, NaiveDateTime, NaiveTime, Parse};
use crate::core::uuid::Uuid;
use crate::db::{Ctx, DatabaseError, Schema, SqlError, TableMetadata, DB_METADATA, ROW_COL_ID};
use crate::sql::statement::{
    BinaryOperator, Constraint, Create, Drop, Expression, Function, Statement, Type, UnaryOperator,
    Value,
};
use crate::vm::expression::{TypeError, VmType};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::str::FromStr;

struct AliasCtx<'s> {
    schema: &'s Schema,
    aliases: &'s HashMap<String, &'s Expression>,
}

struct JoinCtx<'s> {
    schema: &'s Schema,
    tables: &'s HashMap<&'s str, Schema>,
}

#[derive(Debug, PartialEq)]
pub enum AnalyzerError {
    MissingCols,
    DuplicateCols(String),
    MultiplePrimaryKeys,
    AlreadyExists(AlreadyExists),
    /// Attempt to assign Row Id special column manually.
    RowIdAssignment,
    MetadataAssignment,
    Overflow(Type, usize),
}

#[derive(Debug, PartialEq)]
pub enum AlreadyExists {
    Table(String),
    Index(String),
}

type AnalyzerResult<'exp, T> = Result<T, DatabaseError>;

pub(crate) trait AnalyzeCtx {
    fn resolve_identifier(&self, ident: &str) -> Option<(usize, &Type)>;

    /// Resolves a qualified identifier `table.column`.
    /// Default implementation ignores the table.
    fn resolve_qualified_identifier(&self, table: &str, column: &str) -> Option<(usize, &Type)>;
}

pub(in crate::sql) fn analyze<'s>(
    statement: &'s Statement,
    ctx: &'s mut impl Ctx,
) -> AnalyzerResult<'s, ()> {
    match statement {
        Statement::Create(Create::Table { name, columns }) => {
            match ctx.metadata(name) {
                Err(DatabaseError::Sql(SqlError::InvalidTable(_))) => {
                    // we'll create the table if it does not exist
                }
                Ok(_) => {
                    return Err(SqlError::Analyzer(AnalyzerError::AlreadyExists(
                        AlreadyExists::Table(name.into()),
                    ))
                    .into());
                }
                Err(e) => {
                    return Err(e);
                }
            };

            let mut primary_key = false;
            let mut duplicates = HashSet::new();

            for col in columns {
                if !duplicates.insert(&col.name) {
                    return Err(AnalyzerError::DuplicateCols(col.name.to_string()).into());
                }

                if col.name.eq(ROW_COL_ID) {
                    return Err(AnalyzerError::RowIdAssignment.into());
                }

                if col.constraints.contains(&Constraint::PrimaryKey) {
                    if primary_key {
                        return Err(AnalyzerError::MultiplePrimaryKeys.into());
                    }

                    primary_key = true;
                }
            }
        }

        Statement::Create(Create::Index {
            table,
            unique,
            name,
            ..
        }) => {
            if !unique {
                return Err(SqlError::Other("Non-unique index is not yet supported".into()).into());
            }

            let metadata = ctx.metadata(table)?;

            if metadata.indexes.iter().any(|idx| idx.name.eq(name)) {
                return Err(AnalyzerError::AlreadyExists(AlreadyExists::Index(name.into())).into());
            }
        }

        Statement::Insert(Insert {
            into,
            values: rows,
            columns,
        }) => {
            let metadata = ctx.metadata(into)?;
            if into.eq(DB_METADATA) {
                return Err(AnalyzerError::MetadataAssignment.into());
            }

            // PERFORMANCE: there's a lot of things here, maybe check the performance
            let schema = metadata.key_only_schema();
            let columns_provided_by_user = !columns.is_empty();

            let columns: Vec<&str> = match !columns_provided_by_user {
                true => schema
                    .columns
                    .iter()
                    .filter(|col| col.name.ne(ROW_COL_ID) && !col.is_nullable())
                    .map(|col| col.name.as_str())
                    .collect(),
                false => columns.iter().map(String::as_str).collect(),
            };

            let column_set: HashSet<&str> = columns.iter().copied().collect();

            for row in rows {
                if columns_provided_by_user && row.len().ne(&columns.len()) {
                    return Err(AnalyzerError::MissingCols.into());
                }

                let mut seen = HashSet::new();
                for &col in &columns {
                    if col.eq(ROW_COL_ID) {
                        return Err(AnalyzerError::MetadataAssignment.into());
                    }
                    if metadata.schema.index_of(col).is_none() {
                        return Err(SqlError::InvalidColumn(col.into()).into());
                    }
                    if !seen.insert(col) {
                        return Err(AnalyzerError::DuplicateCols(col.into()).into());
                    }
                }

                for col in &schema.columns {
                    if col.name == ROW_COL_ID {
                        continue;
                    }

                    if !col.is_nullable() && !col.data_type.can_be_autogen() {
                        if !column_set.contains(col.name.as_str()) {
                            return Err(AnalyzerError::MissingCols.into());
                        }
                    }
                }

                for (expr, col) in row.iter().zip(&columns) {
                    analyze_assignment(metadata, col, expr, false)?;
                }
            }
        }

        Statement::Select(Select {
            columns,
            from,
            joins,
            order_by,
            group_by,
            r#where,
            limit: _,
            offset: _,
        }) => {
            // TODO: analyze correcly the join clauses
            let metadata = ctx.metadata(&from.name)?;
            let mut schema = metadata.schema.clone();

            let mut tables = HashMap::new();
            tables.insert(from.key(), metadata.schema.clone());

            for join in joins {
                let join_metadata = ctx.metadata(&join.table.name)?;
                tables.insert(join.table.key(), join_metadata.schema.clone());
                schema.extend(join_metadata.schema.columns.clone());
            }

            let aliases: HashMap<String, &Expression> = columns
                .iter()
                .filter_map(|column_expr| match column_expr {
                    Expression::Alias { expr, alias } => Some((alias.clone(), expr.as_ref())),
                    _ => None,
                })
                .collect();

            for expr in columns {
                let expr = expr.unwrap_alias();

                if expr.eq(&Expression::Wildcard) {
                    continue;
                }

                let is_aggr = contains_aggregate(expr);
                let in_group_by = group_by.iter().any(|group| {
                    // check if the expression match directly the group by expression
                    if group.eq(expr) {
                        return true;
                    }

                    group
                        .as_identifier()
                        .and_then(|alias| aliases.get(alias))
                        .map_or(false, |alias| alias.eq(&expr))
                });

                if !group_by.is_empty() && !is_aggr && !in_group_by {
                    return Err(SqlError::InvalidGroupBy(expr.to_string()).into());
                }

                match joins.is_empty() {
                    true => analyze_expression(&schema, None, expr)?,
                    _ => analyze_expression(&JoinCtx::new(&schema, &tables), None, expr)?,
                };
            }

            analyze_where_with_join(&schema, &tables, r#where, joins.is_empty())?;

            // FIXME: we probably can do this in parallel
            for order in order_by {
                match joins.is_empty() {
                    true => analyze_expression_with_aliases(&schema, &aliases, None, &order.expr)?,
                    _ => analyze_expression_with_aliases_and_joins(
                        &JoinCtx::new(&schema, &tables),
                        &aliases,
                        None,
                        &order.expr,
                    )?,
                };
            }

            for expr in group_by {
                match joins.is_empty() {
                    true => analyze_expression_with_aliases(&schema, &aliases, None, expr)?,
                    _ => analyze_expression_with_aliases_and_joins(
                        &JoinCtx::new(&schema, &tables),
                        &aliases,
                        None,
                        expr,
                    )?,
                };
            }
        }

        Statement::Delete(Delete { from, r#where }) => {
            let metadata = ctx.metadata(from)?;

            if from.eq(DB_METADATA) {
                return Err(AnalyzerError::MetadataAssignment.into());
            }

            analyze_where(&metadata.schema, r#where)?;
        }

        Statement::Update(Update {
            r#where,
            table,
            columns,
        }) => {
            let metadata = ctx.metadata(table)?;
            if table.eq(DB_METADATA) {
                return Err(AnalyzerError::MetadataAssignment.into());
            }

            for col in columns {
                analyze_assignment(metadata, &col.identifier, &col.value, true)?;
            }
            analyze_where(&metadata.schema, r#where)?;
        }

        Statement::Drop(Drop::Table(name)) => {
            ctx.metadata(name)?;
        }

        Statement::Explain(statement) => analyze(statement, ctx)?,
        _ => {}
    }

    Ok(())
}

fn analyze_assignment<'exp, 'id>(
    table: &'exp TableMetadata,
    column: &str,
    value: &'exp Expression,
    allow_id: bool,
) -> Result<(), SqlError> {
    if column.eq(ROW_COL_ID) {
        return Err(AnalyzerError::RowIdAssignment.into());
    }

    let idx = table
        .schema
        .index_of(column)
        .ok_or(SqlError::InvalidColumn(column.into()))?;
    let column = &table.schema.columns[idx];
    let data_type = column.data_type;
    let expect_type = VmType::from(&data_type);

    let evaluate_type = match allow_id {
        true => analyze_expression(&table.schema, Some(&data_type), value)?,
        false => analyze_expression(&Schema::empty(), Some(&data_type), value)?,
    };

    if let Expression::Value(Value::Null) = value {
        if !column.is_nullable() {
            return Err(SqlError::Type(TypeError::ExpectedType {
                expected: expect_type,
                found: Expression::Value(Value::Null),
            }));
        }
    } else if expect_type.ne(&evaluate_type) {
        return Err(SqlError::Type(TypeError::ExpectedType {
            expected: expect_type,
            found: value.clone(),
        }));
    }

    if let Type::Varchar(max) = data_type {
        if let Expression::Value(Value::String(str)) = value {
            if str.chars().count() > max {
                return Err(AnalyzerError::Overflow(data_type.to_owned(), max).into());
            }
        }
    }

    Ok(())
}

/// Check if a given expression contains aggregate functions (recursively)
pub(in crate::sql) fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::Function { func, args } => {
            // if this is an aggregate function, return true
            if func.is_aggr() {
                return true;
            }
            args.iter().any(contains_aggregate)
        }
        Expression::BinaryOperation { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOperation { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => contains_aggregate(expr),
        _ => false,
    }
}

impl AnalyzeCtx for Schema {
    fn resolve_identifier(&self, ident: &str) -> Option<(usize, &Type)> {
        self.index_of(ident)
            .map(|idx| (idx, &self.columns[idx].data_type))
    }

    fn resolve_qualified_identifier(&self, _table: &str, column: &str) -> Option<(usize, &Type)> {
        self.last_index_of(column)
            .map(|idx| (idx, &self.columns[idx].data_type))
    }
}

impl<'s> AnalyzeCtx for AliasCtx<'s> {
    fn resolve_identifier(&self, ident: &str) -> Option<(usize, &Type)> {
        if let Some(_) = self.aliases.get(ident) {
            return None;
        }

        self.schema
            .index_of(ident)
            .map(|idx| (idx, &self.schema.columns[idx].data_type))
    }

    fn resolve_qualified_identifier(&self, _table: &str, column: &str) -> Option<(usize, &Type)> {
        self.schema
            .last_index_of(column)
            .map(|idx| (idx, &self.schema.columns[idx].data_type))
    }
}

impl<'s> AnalyzeCtx for JoinCtx<'s> {
    fn resolve_identifier(&self, ident: &str) -> Option<(usize, &Type)> {
        self.schema
            .index_of(ident)
            .map(|idx| (idx, &self.schema.columns[idx].data_type))
    }

    fn resolve_qualified_identifier(&self, table: &str, column: &str) -> Option<(usize, &Type)> {
        if let Some(schema) = self.tables.get(table) {
            if let Some(_) = schema.index_of(column) {
                return self
                    .schema
                    .last_index_of(column)
                    .map(|idx| (idx, &self.schema.columns[idx].data_type));
            }
        }

        None
    }
}

impl<'s> JoinCtx<'s> {
    pub fn new(schema: &'s Schema, tables: &'s HashMap<&'s str, Schema>) -> Self {
        Self { schema, tables }
    }
}

pub(crate) fn analyze_expression<'exp, Ctx: AnalyzeCtx>(
    ctx: &Ctx,
    data_type: Option<&Type>,
    expr: &'exp Expression,
) -> Result<VmType, SqlError> {
    Ok(match expr {
        Expression::Value(value) => analyze_value(value, data_type)?,
        Expression::Identifier(column) => {
            let data_type = ctx
                .resolve_identifier(column)
                .map(|tuple| tuple.1)
                .ok_or(SqlError::InvalidColumn(column.into()))?;

            // this is an expection because when dealing with outside input, UUID's treated as a
            // String, but inside the engine, we treat it as a Number.
            match data_type {
                Type::Uuid => VmType::String,
                r#type => r#type.into(),
            }
        }
        Expression::QualifiedIdentifier { table, column } => {
            let data_type = ctx
                .resolve_qualified_identifier(table, column)
                .map(|tuple| tuple.1)
                .ok_or(SqlError::InvalidQualifiedColumn {
                    table: table.clone(),
                    column: column.clone(),
                })?;

            match data_type {
                Type::Uuid => VmType::String,
                r#type => r#type.into(),
            }
        }

        Expression::BinaryOperation {
            operator,
            left,
            right,
        } => {
            fn get_hint<'e, C: AnalyzeCtx>(ctx: &'e C, expr: &'e Expression) -> Option<&'e Type> {
                match expr {
                    Expression::Identifier(column)
                    | Expression::QualifiedIdentifier { column, .. } => {
                        ctx.resolve_identifier(column).map(|(_, ty)| ty)
                    }
                    _ => None,
                }
            }

            let left_null = matches!(left.as_ref(), Expression::Value(Value::Null));
            let right_null = matches!(right.as_ref(), Expression::Value(Value::Null));

            let (left_type, right_type) = match (left_null, right_null) {
                // null on left: get the right type, then analyze the left with that info
                (true, false) => {
                    let right_type = analyze_expression(ctx, data_type, right)?;
                    let left_type = analyze_expression(ctx, get_hint(ctx, &right), left)?;

                    (left_type, right_type)
                }
                // null on right: get the left type, then analyze the right with that info
                (false, true) => {
                    let left_type = analyze_expression(ctx, data_type, left)?;
                    let right_type = analyze_expression(ctx, get_hint(ctx, &left), right)?;

                    (left_type, right_type)
                }
                // both are null or non-null: analyze normally
                _ => {
                    let left_type = analyze_expression(ctx, data_type, left)?;
                    let right_type = analyze_expression(ctx, data_type, right)?;
                    (left_type, right_type)
                }
            };

            if !are_types_compatible(operator, &left_type, &right_type) {
                return Err(SqlError::Type(TypeError::CannotApplyBinary {
                    left: *left.clone(),
                    right: *right.clone(),
                    operator: *operator,
                }));
            }

            match operator {
                BinaryOperator::Eq
                | BinaryOperator::Neq
                | BinaryOperator::Lt
                | BinaryOperator::LtEq
                | BinaryOperator::Gt
                | BinaryOperator::GtEq => VmType::Bool,
                BinaryOperator::Like if left_type.eq(&VmType::String) => VmType::Bool,
                BinaryOperator::And | BinaryOperator::Or if left_type.eq(&VmType::Bool) => {
                    VmType::Bool
                }
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Div
                | BinaryOperator::Mul
                    if matches!(left_type, VmType::Number | VmType::Float | VmType::Date) =>
                {
                    left_type
                }
                _ => unreachable!("unexpected operation state"),
            }
        }

        Expression::UnaryOperation { operator, expr } => {
            if let (Some(data_type), UnaryOperator::Minus, Expression::Value(value)) =
                (data_type, operator, &**expr)
            {
                match value {
                    Value::Number(n) => {
                        analyze_number(&-n, data_type)?;
                        return Ok(VmType::Number);
                    }
                    Value::Float(f) => {
                        analyze_float(&-f, data_type)?;
                        return Ok(VmType::Float);
                    }
                    _ => unreachable!(),
                }
            }

            match analyze_expression(ctx, data_type, expr)? {
                VmType::Number | VmType::Float => VmType::Number,
                _ => Err(SqlError::Type(TypeError::ExpectedType {
                    expected: VmType::Number,
                    found: *expr.clone(),
                }))?,
            }
        }

        Expression::Function { func, args } => {
            if let Some((min, max)) = func.size_of_args() {
                if args.len() < min || args.len() > max {
                    return Err(SqlError::InvalidFuncArgs(min, args.len()));
                }
            }

            // this will be naturally very small so we can just allocate at once
            let mut arg_types = Vec::with_capacity(args.len());
            for arg in args {
                if arg.ne(&Expression::Wildcard) {
                    arg_types.push(analyze_expression(ctx, data_type, arg)?);
                }
            }

            match func {
                // TODO: make a variant check function for variadic return types
                Function::Min | Function::Max | Function::Coalesce => match arg_types.first() {
                    Some(first) => return Ok(*first),
                    None => func.return_type(),
                },
                _ => func.return_type(),
            }
        }

        Expression::Nested(expr) | Expression::Alias { expr, .. } => {
            analyze_expression(ctx, data_type, expr)?
        }
        Expression::IsNull { expr, .. } => {
            analyze_expression(ctx, data_type, expr)?;
            VmType::Bool
        }
        Expression::Wildcard => {
            return Err(SqlError::Other("Unexpected wildcard expression (*)".into()))
        }
    })
}

fn analyze_where_with_join<'exp>(
    schema: &'exp Schema,
    tables: &'exp HashMap<&'exp str, Schema>,
    r#where: &'exp Option<Expression>,
    has_join: bool,
) -> Result<(), SqlError> {
    let Some(expr) = r#where else { return Ok(()) };

    let result = match has_join {
        true => analyze_expression(schema, None, expr)?,
        _ => analyze_expression(&JoinCtx::new(schema, tables), None, expr)?,
    };

    if let VmType::Bool = result {
        return Ok(());
    }

    Err(TypeError::ExpectedType {
        expected: VmType::Bool,
        found: expr.clone(),
    }
    .into())
}

fn analyze_expression_with_aliases_and_joins<'exp>(
    join_ctx: &JoinCtx,
    aliases: &HashMap<String, &Expression>,
    col_type: Option<&Type>,
    expr: &'exp Expression,
) -> Result<VmType, SqlError> {
    if let Expression::Identifier(ident) = expr {
        if let Some(alias) = aliases.get(ident) {
            return analyze_expression_with_aliases_and_joins(join_ctx, aliases, col_type, &alias);
        }
    }

    analyze_expression(join_ctx, col_type, expr)
}

fn analyze_expression_with_aliases<'exp>(
    schema: &Schema,
    aliases: &HashMap<String, &Expression>,
    col_type: Option<&Type>,
    expr: &'exp Expression,
) -> Result<VmType, SqlError> {
    if let Expression::Identifier(ident) = expr {
        if let Some(aliases_expr) = aliases.get(ident) {
            return analyze_expression_with_aliases(schema, aliases, col_type, &aliases_expr);
        }
    }

    let ctx = AliasCtx { schema, aliases };
    analyze_expression(&ctx, col_type, expr)
}

fn analyze_value<'exp>(value: &Value, col_type: Option<&Type>) -> Result<VmType, SqlError> {
    let result: Result<VmType, SqlError> = match value {
        Value::Boolean(_) => Ok(VmType::Bool),
        Value::Number(n) => {
            if let Some(col_type) = col_type {
                analyze_number(n, col_type)?;
            }
            Ok(VmType::Number)
        }
        Value::Float(f) => {
            if let Some(col_type) = col_type {
                analyze_float(f, col_type)?;
            }
            Ok(VmType::Float)
        }
        Value::String(s) => match col_type {
            // if the column has a type, delegate to analyze_string,
            // which will return VmType::Date for valid dates,
            // or VmType::String otherwise.
            Some(ty) => analyze_string(s, ty),

            // if we don’t know the target type, we can only
            // treat it as a plain string.
            None => Ok(VmType::String),
        },
        Value::Null => match col_type {
            Some(ty) => Ok(ty.into()),
            None => unreachable!("NULL should always be type aware"),
        },
        _ => Ok(VmType::Date),
    };

    result
}

fn analyze_where<'exp>(
    schema: &'exp Schema,
    r#where: &'exp Option<Expression>,
) -> Result<(), DatabaseError> {
    let Some(expr) = r#where else { return Ok(()) };

    if let VmType::Bool = analyze_expression(schema, None, expr)? {
        return Ok(());
    }

    Err(TypeError::ExpectedType {
        expected: VmType::Bool,
        found: expr.clone(),
    }
    .into())
}

fn analyze_string<'exp>(s: &str, expected_type: &Type) -> Result<VmType, SqlError> {
    match expected_type {
        Type::Date => {
            NaiveDate::parse_str(s)?;
            Ok(VmType::Date)
        }
        Type::DateTime => {
            NaiveDateTime::parse_str(s)?;
            Ok(VmType::Date)
        }
        Type::Time => {
            NaiveTime::parse_str(s)?;
            Ok(VmType::Date)
        }
        Type::Uuid => {
            Uuid::from_str(s)?;
            Ok(VmType::Number)
        }
        Type::Interval => {
            Interval::from_str(s).map_err(|e| TypeError::InvalidInterval(e))?;
            Ok(VmType::Interval)
        }
        _ => Ok(VmType::String),
    }
}

fn analyze_number(integer: &i128, data_type: &Type) -> Result<(), AnalyzerError> {
    if data_type.is_integer() && !data_type.is_integer_in_bounds(integer) {
        return Err(AnalyzerError::Overflow(*data_type, *integer as usize));
    }

    Ok(())
}

fn analyze_float(float: &f64, data_type: &Type) -> Result<(), AnalyzerError> {
    if data_type.is_float() && !data_type.is_float_in_bounds(float) {
        return Err(AnalyzerError::Overflow(*data_type, *float as usize));
    }

    Ok(())
}

// TODO: this is very uhhhhhhhhhmmmmmmm
// I wanna remove this somehow later
fn are_types_compatible(
    operator: &BinaryOperator,
    left_type: &VmType,
    right_type: &VmType,
) -> bool {
    if left_type == right_type {
        return true;
    }

    matches!(
        (operator, left_type, right_type),
        (
            BinaryOperator::Plus | BinaryOperator::Minus,
            VmType::Date,
            VmType::Interval
        ) | (
            BinaryOperator::Plus | BinaryOperator::Minus,
            VmType::Interval,
            VmType::Date
        )
    )
}

impl Display for AnalyzerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AnalyzerError::MissingCols => write!(f, "all columns must be specified"),
            AnalyzerError::DuplicateCols(col) => write!(f, "column {col} was already declared"),
            AnalyzerError::AlreadyExists(name) => f.write_str(&name.to_string()),
            AnalyzerError::MultiplePrimaryKeys => {
                write!(f, "a table can only have one primary key")
            }
            AnalyzerError::RowIdAssignment => {
                write!(f, "row id column cannot be manually assigned")
            }
            AnalyzerError::MetadataAssignment => {
                write!(
                    f,
                    "table {DB_METADATA} is reserved for internal use, it cannot be manually assigned"
                )
            }
            AnalyzerError::Overflow(data_type, max) => {
                write!(f, "value of type {data_type} is larger than max size {max}")
            }
        }
    }
}
impl Display for AlreadyExists {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlreadyExists::Table(table_name) => write!(f, "table {table_name} already exists"),
            AlreadyExists::Index(index_name) => write!(f, "index {index_name} already exists"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::date::DateParseError;
    use crate::core::uuid::UuidError;
    use crate::db::*;
    use crate::sql::parser::*;

    struct Analyze<'sql> {
        sql: &'sql str,
        ctx: &'sql [&'sql str],
        expected: Result<(), DatabaseError>,
    }

    impl<'sql> Analyze<'sql> {
        fn assert(&self) -> Result<(), DatabaseError> {
            let statement = Parser::new(self.sql).parse_statement()?;
            let mut ctx = Context::try_from(self.ctx)?;

            assert_eq!(analyze(&statement, &mut ctx), self.expected);
            Ok(())
        }
    }

    type AnalyzerResult = Result<(), DatabaseError>;
    #[test]
    fn select_from_invalid_table() -> AnalyzerResult {
        // we cannot select from a table not created yet
        Analyze {
            sql: "SELECT * FROM users;",
            ctx: &[],
            expected: Err(SqlError::InvalidTable("users".into()).into()),
        }
        .assert()
    }

    #[test]
    fn select_from_invalid_column() -> AnalyzerResult {
        Analyze {
            sql: "SELECT last_name FROM customer;",
            ctx: &["CREATE TABLE customer (id INT PRIMARY KEY, first_name VARCHAR(69));"],
            expected: Err(SqlError::InvalidColumn("last_name".into()).into()),
        }
        .assert()
    }

    #[test]
    fn select_from_valid_table() -> AnalyzerResult {
        Analyze {
            sql: "SELECT * FROM users;",
            ctx: &["CREATE TABLE users (id INT PRIMARY KEY);"],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn select_from_valid_column() -> AnalyzerResult {
        Analyze {
            sql: "SELECT last_name FROM customer;",
            ctx: &["CREATE TABLE customer (id INT PRIMARY KEY, first_name VARCHAR(69), last_name VARCHAR(69));"],
            expected: Ok(()),
        }
            .assert()
    }

    #[test]
    #[ignore]
    // FIXME: make this be parsable
    fn insert_without_columns_targeting() -> AnalyzerResult {
        Analyze {
            sql: "INSERT INTO users VALUES ('string', 5, 6);",
            ctx: &["CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255) UNIQUE);"],
            expected: Err(TypeError::ExpectedType {
                expected: VmType::Number,
                found: Expression::Value(Value::String("1".into())),
            }
            .into()),
        }
        .assert()
    }

    #[test]
    fn column_values_mismatch() -> AnalyzerResult {
        Analyze {
            sql: "INSERT INTO employees (id, name, birth_date) VALUES (24, 'Mary Dove');",
            ctx: &[
                "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(255), birth_date DATE);",
            ],
            expected: Err(AnalyzerError::MissingCols.into()),
        }
        .assert()
    }

    #[test]
    fn column_type_mismatch() -> AnalyzerResult {
        const CTX: &[&str; 2] = &[
            "CREATE TABLE products (id INT, name VARCHAR(255), price INT);",
            "CREATE TABLE users (id INT, username VARCHAR(255));",
        ];

        Analyze {
            sql: "INSERT INTO users (id, username) VALUES (1, 123);",
            expected: Err(TypeError::ExpectedType {
                expected: VmType::String,
                found: Expression::Value(Value::Number(123)),
            }
            .into()),
            ctx: CTX,
        }
        .assert()?;

        Analyze {
            sql: "INSERT INTO products (id, name, price) VALUES (1, 123, 69);",
            expected: Err(TypeError::ExpectedType {
                expected: VmType::String,
                found: Expression::Value(Value::Number(123)),
            }
            .into()),
            ctx: CTX,
        }
        .assert()?;

        Ok(())
    }

    #[test]
    fn insert_date() -> AnalyzerResult {
        Analyze {
            ctx: &["CREATE TABLE employees (name VARCHAR(255), birth_date DATE);"],
            sql: "INSERT INTO employees (name, birth_date) VALUES ('John Doe', '2004-06-27');",
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_date_time() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE events (
                event_id    INTEGER PRIMARY KEY,
                title       VARCHAR(200),
                occurred_at TIMESTAMP
            );
        "#;

        let sql = r#"
            INSERT INTO events (event_id, title, occurred_at)
            VALUES (420, 'Meeting', '2021-01-01 12:00:00'), (69, 'Data backup', '2025-05-07 23:59:59');
        "#;

        let analyze = Analyze {
            sql,
            ctx: &[CTX],
            expected: Ok(()),
        };

        analyze.assert()
    }

    #[test]
    fn insert_invalid_time_format() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE logs (
                log_id INT PRIMARY KEY,
                logged_at TIME
            );
        "#;
        let ctx = &[CTX];

        #[rustfmt::skip]
        let cases = [
            ("INSERT INTO logs (log_id, logged_at) VALUES (1, '25:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidHour)),
            ("INSERT INTO logs (log_id, logged_at) VALUES (1, '12:60:00');",
             TypeError::InvalidDate(DateParseError::InvalidMinute)),
            ("INSERT INTO logs (log_id, logged_at) VALUES (1, '12:00:60');",
             TypeError::InvalidDate(DateParseError::InvalidSecond)),
        ];

        for (sql, err) in cases {
            let analyze = Analyze {
                sql,
                ctx,
                expected: Err(SqlError::Type(err).into()),
            };

            analyze.assert()?;
        }

        Ok(())
    }

    #[test]
    fn insert_serial() -> AnalyzerResult {
        const CTX: &str = "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(120));";

        Analyze {
            sql: "INSERT INTO users (name) VALUES ('John Doe');",
            ctx: &[CTX],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_float() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE weather_data (
                reading_id SERIAL PRIMARY KEY,
                temperature REAL,
                humidity REAL
            );
        "#;

        Analyze {
            sql: r#"
            INSERT INTO weather_data (temperature, humidity)
            VALUES
                (23.5, 60.2),
                (20.1, 65.3),
                (22.8, 58.1);
            "#,
            ctx: &[CTX],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_double_precision() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE scientific_data (
                measurement_id SERIAL PRIMARY KEY,
                precise_temperature DOUBLE PRECISION,
                co2_levels DOUBLE PRECISION,
                measurement_time TIMESTAMP
            );
        "#;

        Analyze {
            sql: r#"
            INSERT INTO scientific_data (precise_temperature, co2_levels, measurement_time)
            VALUES
                (23.456789, 415.123456789, '2024-02-03 10:00:00'),
                (20.123456, 417.123789012, '2024-02-03 11:00:00'),
                (22.789012, 418.456123789, '2024-02-03 12:00:00');
            "#,
            ctx: &[CTX],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_invalid_datetime_format() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE events (
                id INT,
                happened_at TIMESTAMP
            );
        "#;
        let ctx = &[CTX];

        #[rustfmt::skip]
        let cases = [
            ("INSERT INTO events (id, happened_at) VALUES (1, '2020-13-01 12:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidMonth)),
            ("INSERT INTO events (id, happened_at) VALUES (2, '2020-00-01 12:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidMonth)),
            ("INSERT INTO events (id, happened_at) VALUES (3, '2020-12-32 12:00:00');",
             TypeError::InvalidDate(DateParseError::InvalidDay)),
            ("INSERT INTO events (id, happened_at) VALUES (4, '2023-04-31 10:00:00');",
            TypeError::InvalidDate(DateParseError::InvalidMonthDay)),
            ("INSERT INTO events (id, happened_at) VALUES (5, 'not-a-date');",
            TypeError::InvalidDate(DateParseError::InvalidDateTime)),
        ];

        for (sql, err) in cases {
            let analyze = Analyze {
                sql,
                ctx,
                expected: Err(SqlError::Type(err).into()),
            };

            analyze.assert()?;
        }

        Ok(())
    }

    #[test]
    fn insert_smallint() -> AnalyzerResult {
        const CTX: &str = "CREATE TABLE hamlets (id INT PRIMARY KEY, population SMALLINT);";

        Analyze {
            sql: "INSERT INTO hamlets (id, population) VALUES (1, 4302);",
            ctx: &[CTX],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn multiple_primary_keys() -> AnalyzerResult {
        let analyze = Analyze {
            sql: "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255) PRIMARY KEY);",
            ctx: &[],
            expected: Err(AnalyzerError::MultiplePrimaryKeys.into()),
        };

        analyze.assert()
    }

    #[test]
    fn non_boolean_where() -> AnalyzerResult {
        const CTX: &str = "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));";
        let found = Expression::BinaryOperation {
            operator: BinaryOperator::Plus,
            left: Box::new(Expression::Value(Value::Number(1))),
            right: Box::new(Expression::Value(Value::Number(1))),
        };

        Analyze {
            ctx: &[CTX],
            sql: "SELECT * FROM users WHERE 1 + 1;",
            expected: Err(TypeError::ExpectedType {
                expected: VmType::Bool,
                found,
            }
            .into()),
        }
        .assert()
    }

    #[test]
    fn minus_uniary_operation() -> AnalyzerResult {
        const CTX: &str =
            "CREATE TABLE measurements (id INT PRIMARY KEY, temperature REAL, altitude INT);";

        let sql = r#"
            INSERT INTO measurements (id, temperature, altitude) VALUES
                (1, -12.5, -100),
                (2, 0.0, 0),
                (3, 23.7, 500),
                (4, -22.906847, -43),
                (5, 10.5, -250);
            "#;

        Analyze {
            ctx: &[CTX],
            sql,
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn between_operation() -> AnalyzerResult {
        const CTX: &str = "CREATE TABLE payments (payment_id SERIAL PRIMARY KEY, amount REAL);";

        Analyze {
            ctx: &[CTX],
            sql: "SELECT amount FROM payments WHERE payment_id BETWEEN 24 AND 420;",
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn ordering_operation_on_temporal() -> AnalyzerResult {
        const CTX: &str = r#"
            CREATE TABLE temporal_data (
                id SERIAL PRIMARY KEY,
                event_name VARCHAR(100),
                event_datetime TIMESTAMP,
                event_date DATE,
                event_time TIME
            );
        "#;

        let sql = "SELECT * FROM temporal_data WHERE event_date < '1970-01-01';";

        Analyze {
            ctx: &[CTX],
            sql,
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn uuid_column() -> AnalyzerResult {
        let create_stmt = "CREATE TABLE contracts (id UUID, name VARCHAR(30));";

        Analyze {
            sql: create_stmt,
            ctx: &[],
            expected: Ok(()),
        }
        .assert()?;

        Analyze {
            sql: "INSERT INTO contracts (id, name) VALUES ('not-a-uuid', 'really-good-name');",
            ctx: &[create_stmt],
            expected: Err(TypeError::UuidError(UuidError::InvalidLength).into()),
        }
        .assert()?;

        Analyze {
            sql: "INSERT INTO contracts (id, name) VALUES (1234, 'other-really-good-name');",
            ctx: &[create_stmt],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn substring_function() -> AnalyzerResult {
        let table = "CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(70));";

        Analyze {
            sql: "SELECT name, SUBSTRING(name FROM 1 FOR 1) FROM customers;",
            ctx: &[table],
            expected: Ok(()),
        }
        .assert()?;

        Analyze {
            sql: "SELECT SUBSTRING(name FROM 1) FROM customers;",
            ctx: &[table],
            expected: Ok(()),
        }
        .assert()?;

        Analyze {
            sql: "SELECT SUBSTRING(name FOR 8) FROM customers;",
            ctx: &[table],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn ascii_function() -> AnalyzerResult {
        Analyze {
            sql: "SELECT ASCII(name) FROM users ORDER BY name;",
            ctx: &["CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50));"],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn concat_function() -> AnalyzerResult {
        let table =
            "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(100));";

        Analyze {
            sql: "SELECT CONCAT(name, last_name) FROM users;",
            ctx: &[table],
            expected: Ok(()),
        }
        .assert()?;

        Analyze {
            sql: "SELECT CONCAT(name) FROM users;",
            ctx: &[table],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn position_function() -> AnalyzerResult {
        Analyze {
            sql: "SELECT POSITION('ab' IN last_name) FROM users;",
            ctx: &["CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(100));"],
            expected: Ok(())
        }.assert()
    }

    #[test]
    fn count_function() -> AnalyzerResult {
        Analyze {
            sql: "SELECT COUNT(amount) FROM payments;",
            ctx: &["CREATE TABLE payments (id SERIAL PRIMARY KEY, amount REAL);"],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn group_by() -> AnalyzerResult {
        let context = "CREATE TABLE payment (id SERIAL PRIMARY KEY, customer_id INT, staff_id INT, amount INT);";

        Analyze {
            sql: "SELECT customer_id, SUM(amount) FROM payment GROUP BY amount;",
            ctx: &[context],
            expected: Err(SqlError::InvalidGroupBy("customer_id".into()).into()),
        }
        .assert()?;

        Analyze {
            sql: "SELECT customer_id, SUM(amount) FROM payment GROUP BY customer_id;",
            ctx: &[context],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn alias() -> AnalyzerResult {
        Analyze {
            ctx: &[
                "CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(30), salary REAL, bonus REAL);",
            ],
            sql: "SELECT salary + bonus AS total, name AS employee_name FROM employees;",
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_empty_text() -> AnalyzerResult {
        Analyze {
            sql: "INSERT INTO users (id, notes) VALUES (1, '');",
            ctx: &["CREATE TABLE users (id INT PRIMARY KEY, notes TEXT);"],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn insert_large_text() -> AnalyzerResult {
        Analyze {
            sql: &format!(
                "INSERT INTO t (id, notes) VALUES (1, '{}');",
                "x".repeat(10000)
            ),
            ctx: &["CREATE TABLE t (id INT PRIMARY KEY, notes TEXT);"],
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn test_where_between_text_varchar() -> AnalyzerResult {
        let ctx = &["CREATE TABLE documents (
            id INT PRIMARY KEY,
            short_desc VARCHAR(100),
            full_text TEXT
        );"];

        Analyze {
            sql: "SELECT * FROM documents WHERE short_desc = 'Quick brown fox';",
            ctx,
            expected: Ok(()),
        }
        .assert()?;

        Analyze {
            sql: "SELECT * FROM documents WHERE full_text = 'Lorem ipsum dolor sit amet';",
            ctx,
            expected: Ok(()),
        }
        .assert()?;

        Analyze {
            sql: "SELECT * FROM documents WHERE full_text LIKE '%lorem%';",
            ctx,
            expected: Ok(()),
        }
        .assert()
    }

    #[test]
    fn test_extract_function() -> AnalyzerResult {
        let ctx = &["CREATE TABLE users (name VARCHAR(100), birth_date DATE);"];
        Analyze {
            sql: "SELECT EXTRACT(YEAR FROM birth_date) FROM users;",
            ctx,
            expected: Ok(()),
        }
        .assert()
    }
}
