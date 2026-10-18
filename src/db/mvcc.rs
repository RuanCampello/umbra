//! MVCC-backed database session

use crate::db::{
    Budget, Context, Ctx, DatabaseError, QuerySet, Result, Schema, SqlError, TableMetadata,
};
use crate::executor::{Executor, dispatch::ExecResult};
use crate::sql::{
    self,
    analyzer::AnalyzerError,
    parser::Parser,
    statement::{BinaryOperator, Expression, Function, Select, SetOperator, Statement, Value},
};
use crate::storage::engine::{Config, Engine};
use std::path::Path;

pub struct MvccDatabase {
    executor: Executor,
    /// Caches [TableMetadata] synthesised from the engine's schemas for the
    /// analyzer/prepare pipeline
    context: Context,
    /// Engine schema epoch this session's [Context] was last synced against
    /// a bump means another connection changed the catalogue and the cache is stale
    epoch: u64,
    /// Whether this session owns the engine's lifecycle
    /// Sessions spawned per connection over a shared engine leave closing to the owner
    owns_engine: bool,
}

impl MvccDatabase {
    /// opens (or creates) a durable database rooted at `dir`, recovering state from snapshots and the WAL
    pub fn init(dir: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::with_engine(Self::open_engine(dir, None)?, true))
    }

    /// [MvccDatabase::init] keeping table data in memory within `budget` (by default half the
    /// physical memory): colder settled chunks are evicted to disk and reloaded on demand
    pub fn init_with_budget(dir: impl AsRef<Path>, budget: Budget) -> Result<Self> {
        Ok(Self::with_engine(Self::open_engine(dir, Some(budget))?, true))
    }

    pub fn in_memory() -> Result<Self> {
        let engine = Engine::in_memory();
        engine.start_maintenance(Config::default().maintenance_interval);

        Ok(Self::with_engine(engine, true))
    }

    /// opens and recovers a durable engine, returning the shared handle so
    /// several sessions can be spawned over it with [MvccDatabase::connect]
    pub(crate) fn open_engine(dir: impl AsRef<Path>, budget: Option<Budget>) -> Result<Engine> {
        let default = Config::default();
        let config = Config { budget: budget.or(default.budget), ..default };
        let engine = Engine::open(dir, config.clone())?;
        engine.start_maintenance(config.maintenance_interval);

        Ok(engine)
    }

    /// spawns a session over an already-open shared engine
    /// the session does not own the engine's lifecycle, so dropping it only rolls back its own
    /// transaction
    pub(crate) fn connect(engine: Engine) -> Self {
        Self::with_engine(engine, false)
    }

    /// opens another session over this session's engine, for concurrent use
    /// from another thread
    /// it shares all committed state, keeps its own transaction context, and does not own the engine's lifecycle
    pub fn session(&self) -> Self {
        Self::with_engine(self.executor.engine().clone(), false)
    }

    /// the shared engine handle, for spawning further sessions over it
    #[cfg(test)]
    pub(crate) fn engine(&self) -> Engine {
        self.executor.engine().clone()
    }

    fn with_engine(engine: Engine, owns_engine: bool) -> Self {
        let epoch = engine.epoch();

        Self {
            executor: Executor::new(engine),
            context: Context::default(),
            epoch,
            owns_engine,
        }
    }

    /// drops cached metadata when another connection has changed the
    /// catalogue since this session last looked
    fn sync_schema_epoch(&mut self) {
        let epoch = self.executor.engine().epoch();
        if epoch != self.epoch {
            self.context.clear();
            self.epoch = epoch;
        }
    }

    /// Executes every statement of a `.sql` file
    pub fn load(&mut self, path: impl AsRef<std::path::Path>) -> Result<()> {
        if path.as_ref().extension().and_then(|s| s.to_str()) != Some("sql") {
            return Err(DatabaseError::Other("File must have a .sql extension".to_string()));
        }

        let content = std::fs::read_to_string(path).map_err(DatabaseError::Io)?;
        let statements = Parser::new(&content).try_parse()?;

        self.sync_schema_epoch();
        for statement in statements {
            let statement = sql::process_statement(statement, self)?;
            self.execute(statement)?;
            self.sync_schema_epoch();
        }

        Ok(())
    }

    /// runs `statement`, dropping cached metadata whenever it may change the catalogue this session
    /// sees: its own DDL, and the end of a transaction that may hold some (on success or failure)
    fn execute(&mut self, statement: Statement) -> Result<ExecResult> {
        let catalogue = matches!(
            statement,
            Statement::Create(_)
                | Statement::Drop(_)
                | Statement::Commit
                | Statement::Rollback
                | Statement::RollbackTo(_)
        );
        let result = self.executor.execute(statement);
        if catalogue {
            self.context.clear();
        }

        result
    }

    pub fn exec(&mut self, input: &str) -> Result<QuerySet> {
        self.sync_schema_epoch();
        let mut statement = Parser::new(input).parse_statement()?;

        // its subqueries and union members read the snapshot of the statement around them
        let union = matches!(&statement, Statement::Select(select) if !select.combined.is_empty());
        let implicit = !self.active_transaction()
            && (union || statement.expressions_mut().iter().any(|expr| expr.contains_subquery()));
        if !implicit {
            return self.run(statement);
        }

        self.execute(Statement::StartTransaction(None))?;
        match self.run(statement) {
            Ok(result) => self.execute(Statement::Commit).map(|_| result),
            Err(error) => {
                let _ = self.rollback();
                Err(error)
            },
        }
    }

    fn run(&mut self, mut statement: Statement) -> Result<QuerySet> {
        let result = match statement {
            Statement::Select(select) if !select.combined.is_empty() => self.union(select)?,
            _ => {
                let outer = outer_tables(&statement);
                for expr in statement.expressions_mut() {
                    self.materialise(expr, &outer)?;
                }
                let statement = sql::process_statement(statement, self)?;

                if let Statement::Source(path) = statement {
                    self.load(path)?;
                    return Ok(QuerySet::empty());
                }

                self.execute(statement)?
            },
        };
        self.sync_schema_epoch();

        Ok(match result {
            ExecResult::Rows(schema, tuples) => {
                // an estimate is enough for the guard, exact sizes are for the wire protocol
                let mut total_size = 0;
                for tuple in &tuples {
                    total_size += tuple.iter().map(sql::Value::serialised_size_hint).sum::<usize>();
                    if total_size > 1 << 30 {
                        if self.executor.has_active_transaction() {
                            let _ = self.executor.rollback();
                        }
                        return Err(DatabaseError::NoMemory);
                    }
                }

                let mut set = QuerySet::new(schema, tuples);
                set.display_enums();
                set
            },
            // the wire protocol reads the affected-row count from a schema-less single value
            ExecResult::Affected(count) => {
                QuerySet::new(Schema::empty(), vec![vec![Value::Number(count as i64)]])
            },
            ExecResult::Ok => QuerySet::empty(),
        })
    }

    /// runs the uncorrelated subqueries in `expr` and puts their results in their place: a value
    /// list for `IN`, a boolean for `EXISTS`, the single value of a scalar subquery
    /// an `EXISTS` correlated with `outer` by one equality is first rewritten as a semi-join, see [semi_join]
    fn materialise(&mut self, expr: &mut Expression, outer: &[String]) -> Result<()> {
        let semi = match &*expr {
            Expression::Function { func: Function::Exists, args } => match args.first() {
                Some(Expression::Subquery(select)) => semi_join(select, outer),
                _ => None,
            },
            _ => None,
        };
        if let Some(semi) = semi {
            *expr = semi;
        }

        match expr {
            Expression::Function { func: Function::In, args }
                if matches!(args.get(1), Some(Expression::Subquery(_))) =>
            {
                self.materialise(&mut args[0], outer)?;
                let Some(Expression::Subquery(select)) = args.pop() else {
                    unreachable!("matched above")
                };

                let rows = self.subquery(*select)?;
                match rows.is_empty() {
                    // nothing is in an empty set, not even NULL
                    true => *expr = Expression::Value(Value::Boolean(false)),
                    _ => args.extend(
                        rows.into_iter().map(|mut row| Expression::Value(row.swap_remove(0))),
                    ),
                }
            },
            Expression::Function { func: Function::Exists, args } => {
                let Some(Expression::Subquery(mut select)) = args.pop() else {
                    unreachable!("EXISTS is parsed over a subquery")
                };

                select.limit = Some(1);
                let found = !self.subquery_rows(*select)?.is_empty();
                *expr = Expression::Value(Value::Boolean(found));
            },
            Expression::Subquery(_) => {
                let Expression::Subquery(select) = std::mem::take(expr) else {
                    unreachable!("matched above")
                };
                let mut rows = self.subquery(*select)?;
                *expr = match rows.len() {
                    0 => Expression::Value(Value::Null),
                    1 => Expression::Value(rows.swap_remove(0).swap_remove(0)),
                    _ => {
                        let msg = "more than one row returned by a subquery used as an expression";
                        return Err(DatabaseError::Sql(SqlError::Other(msg.into())));
                    },
                };
            },
            Expression::Function { args, .. } => {
                args.iter_mut().try_for_each(|arg| self.materialise(arg, outer))?
            },
            Expression::BinaryOperation { left, right, .. } => {
                self.materialise(left, outer)?;
                self.materialise(right, outer)?;
            },
            Expression::UnaryOperation { expr, .. }
            | Expression::Nested(expr)
            | Expression::Alias { expr, .. }
            | Expression::IsNull { expr, .. } => self.materialise(expr, outer)?,
            _ => {},
        }

        Ok(())
    }

    /// the rows of a subquery that must return one column
    fn subquery(&mut self, select: Select) -> Result<Vec<Vec<Value>>> {
        let rows = self.subquery_rows(select)?;
        match rows.first().is_some_and(|row| row.len() != 1) {
            true => Err(DatabaseError::Sql(SqlError::Other(
                "subquery must return only one column".into(),
            ))),
            false => Ok(rows),
        }
    }

    fn subquery_rows(&mut self, select: Select) -> Result<Vec<Vec<Value>>> {
        self.select_rows(select).map(|(_, rows)| rows)
    }

    /// a select through the whole pipeline, its subqueries and unions included
    fn select_rows(&mut self, select: Select) -> Result<(Schema, Vec<Vec<Value>>)> {
        if !select.combined.is_empty() {
            return match self.union(select)? {
                ExecResult::Rows(schema, rows) => Ok((schema, rows)),
                _ => unreachable!("a union returns rows"),
            };
        }

        let mut statement = Statement::Select(select);
        let outer = outer_tables(&statement);
        for expr in statement.expressions_mut() {
            self.materialise(expr, &outer)?;
        }

        let statement = sql::process_statement(statement, self)?;
        match self.execute(statement)? {
            ExecResult::Rows(schema, rows) => Ok((schema, rows)),
            _ => unreachable!("a select returns rows"),
        }
    }

    /// runs each member of a set operation as its own select and combines them: `INTERSECT` runs
    /// first, binding tighter, then `UNION` and `EXCEPT` left to right
    /// without `ALL` the result so far loses its duplicates
    /// `ORDER BY` names output columns, by name or position
    fn union(&mut self, mut head: Select) -> Result<ExecResult> {
        let members = std::mem::take(&mut head.combined);
        let order_by = std::mem::take(&mut head.order_by);
        let (limit, offset) = (head.limit.take(), head.offset.take());

        let (schema, rows) = self.select_rows(head)?;
        let mut terms = vec![(SetOperator::Union, true, rows)];
        for (operator, all, member) in members {
            let (columns, member_rows) = self.select_rows(member)?;
            if columns.len() != schema.len() {
                let message = format!("each {operator} query must have the same number of columns");
                return Err(DatabaseError::Sql(SqlError::Other(message)));
            }

            match operator {
                SetOperator::Intersect => {
                    let (_, _, term) = terms.last_mut().expect("the head is a term");
                    *term = combine(std::mem::take(term), member_rows, operator, all);
                },
                _ => terms.push((operator, all, member_rows)),
            }
        }

        let mut terms = terms.into_iter();
        let (_, _, mut rows) = terms.next().expect("the head is a term");
        for (operator, all, term) in terms {
            rows = combine(rows, term, operator, all);
        }

        let keys = order_by
            .iter()
            .map(|order| {
                let position = match &order.expr {
                    Expression::Identifier(name) => schema.index_of(name),
                    Expression::Value(Value::Number(n)) => {
                        usize::try_from(*n - 1).ok().filter(|&p| p < schema.len())
                    },
                    _ => None,
                };
                let descending = order.direction == crate::sql::statement::OrderDirection::Desc;
                position.map(|position| (position, descending)).ok_or_else(|| {
                    let message =
                        format!("ORDER BY {} must name an output column of the UNION", order.expr);
                    DatabaseError::Sql(SqlError::Other(message))
                })
            })
            .collect::<Result<Vec<_>>>()?;

        rows.sort_by(|a, b| {
            keys.iter()
                .map(|&(position, descending)| match descending {
                    true => b[position].cmp(&a[position]),
                    false => a[position].cmp(&b[position]),
                })
                .find(|order| order.is_ne())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let rows = rows
            .into_iter()
            .skip(offset.unwrap_or(0))
            .take(limit.unwrap_or(usize::MAX))
            .collect();
        Ok(ExecResult::Rows(schema, rows))
    }

    pub fn active_transaction(&self) -> bool {
        self.executor.has_active_transaction()
    }

    pub fn rollback(&mut self) -> Result<()> {
        self.executor.rollback()?;
        Ok(())
    }

    /// rolls back any open transaction and, when this session owns the engine,
    /// shuts it down cleanly
    /// shared sessions leave the engine to its owner
    pub fn close(&mut self) -> Result<()> {
        if self.executor.has_active_transaction() {
            self.executor.rollback()?;
        }

        if self.owns_engine {
            self.executor.engine().close()?;
        }
        Ok(())
    }
}

impl Drop for MvccDatabase {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

impl Ctx for MvccDatabase {
    fn metadata(&mut self, table: &str) -> Result<&mut TableMetadata> {
        if !self.context.contains(table) {
            let handle = self
                .executor
                .session_table(table)
                .map_err(|_| DatabaseError::Sql(SqlError::InvalidTable(table.into())))?;

            self.context.insert(TableMetadata {
                name: handle.name().to_string(),
                schema: handle.schema.clone(),
            });
        }

        Ctx::metadata(&mut self.context, table)
    }

    fn next_serial(&mut self, table: &str, column: usize) -> Result<i64> {
        let handle = self
            .executor
            .session_table(table)
            .map_err(|_| DatabaseError::Sql(SqlError::InvalidTable(table.into())))?;
        let value = handle.next_serial(column) as i64;
        let data_type = handle.schema.columns[column].column_type();
        let max = data_type.max();

        match value > max as i64 {
            true => Err(AnalyzerError::Overflow(data_type, max).into()),
            false => Ok(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Value;

    #[test]
    fn sessions_observe_each_others_schema_changes() {
        let mut owner = MvccDatabase::in_memory().unwrap();
        let mut session = MvccDatabase::connect(owner.engine());

        owner.exec("CREATE TABLE t (id SERIAL PRIMARY KEY, v INT);").unwrap();

        // the session predates the table, the schema epoch bump makes it visible
        session.exec("INSERT INTO t (v) VALUES (10);").unwrap();
        let rows = owner.exec("SELECT v FROM t;").unwrap();
        assert_eq!(rows.tuples.len(), 1);
        assert_eq!(rows.tuples[0][0], Value::Number(10));

        owner.exec("DROP TABLE t;").unwrap();
        assert!(
            session.exec("INSERT INTO t (v) VALUES (20);").is_err(),
            "a dropped table is gone for the other session too"
        );
    }

    #[test]
    fn concurrent_sessions_never_collide_on_serials() {
        let mut owner = MvccDatabase::in_memory().unwrap();
        owner.exec("CREATE TABLE t (id SERIAL PRIMARY KEY, who INT);").unwrap();
        let engine = owner.engine();

        let handles: Vec<_> = (0..4)
            .map(|who| {
                let mut session = MvccDatabase::connect(engine.clone());
                std::thread::spawn(move || {
                    for _ in 0..50 {
                        session.exec(&format!("INSERT INTO t (who) VALUES ({who});")).unwrap();
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let rows = owner.exec("SELECT id FROM t ORDER BY id;").unwrap();
        assert_eq!(rows.tuples.len(), 200, "every insert committed");

        let mut ids: Vec<_> = rows.tuples.iter().map(|row| row[0].clone()).collect();
        ids.dedup();
        assert_eq!(ids.len(), 200, "no serial value handed out twice");
    }
}

/// the names the tables of `statement` go by, which a subquery inside it may refer to
fn outer_tables(statement: &Statement) -> Vec<String> {
    match statement {
        Statement::Select(select) => std::iter::once(&select.from)
            .chain(select.joins.iter().map(|join| &join.table))
            .map(|table| table.key().to_string())
            .collect(),
        Statement::Update(update) => vec![update.table.clone()],
        Statement::Delete(delete) => vec![delete.from.clone()],
        Statement::Explain(inner) => outer_tables(inner),
        _ => Vec::new(),
    }
}

/// `EXISTS (SELECT .. FROM inner WHERE o.c = e AND rest)`, the subquery correlated only by that
/// one equality (`o` an outer table no inner one shadows, `e` reading the inner tables), as the
/// uncorrelated `o.c IS NOT NULL AND o.c IN (SELECT DISTINCT e .. WHERE rest AND e IS NOT NULL)`
/// it is never NULL, so `NOT EXISTS` stays exact
/// a subquery that groups, aggregates, limits or combines always has a row or decides which, and is left alone
fn semi_join(select: &Select, outer: &[String]) -> Option<Expression> {
    let simple = select.group_by.is_empty()
        && select.having.is_none()
        && select.limit.is_none()
        && select.offset.is_none()
        && select.combined.is_empty()
        && !select.columns.iter().any(sql::analyzer::contains_aggregate);
    if !simple {
        return None;
    }

    let inner: Vec<_> = std::iter::once(&select.from)
        .chain(select.joins.iter().map(|join| &join.table))
        .map(|table| table.key())
        .collect();
    let correlated: Vec<_> =
        outer.iter().map(String::as_str).filter(|name| !inner.contains(name)).collect();
    let mentions = |expr: &Expression| refers_to(expr, &correlated);
    if select.joins.iter().any(|join| mentions(&join.on)) {
        return None;
    }

    let mut conjuncts = Vec::new();
    let mut pending = select.r#where.iter().collect::<Vec<_>>();
    while let Some(conjunct) = pending.pop() {
        match conjunct {
            Expression::BinaryOperation { left, operator: BinaryOperator::And, right } => {
                pending.extend([right.as_ref(), left.as_ref()])
            },
            Expression::Nested(inner) => pending.push(inner),
            other => conjuncts.push(other),
        }
    }

    let mut link = None;
    let mut rest = Vec::new();
    for conjunct in conjuncts {
        let pair = match conjunct {
            Expression::BinaryOperation { left, operator: BinaryOperator::Eq, right } => {
                Some((left.as_ref(), right.as_ref()))
            },
            _ => None,
        };
        let outer_side = |side: &Expression| matches!(side, Expression::QualifiedIdentifier { table, .. } if correlated.contains(&table.as_str()));
        match pair {
            Some((left, right)) if link.is_none() && outer_side(left) && !mentions(right) => {
                link = Some((left.clone(), right.clone()))
            },
            Some((left, right)) if link.is_none() && outer_side(right) && !mentions(left) => {
                link = Some((right.clone(), left.clone()))
            },
            _ if mentions(conjunct) => return None,
            _ => rest.push(conjunct.clone()),
        }
    }
    let (outer_column, inner_value) = link?;

    let present = Expression::IsNull { expr: Box::new(inner_value.clone()), negated: true };
    let r#where = rest.into_iter().fold(present, |all, conjunct| Expression::BinaryOperation {
        left: Box::new(all),
        operator: BinaryOperator::And,
        right: Box::new(conjunct),
    });
    let mut subquery = select.clone();

    subquery.distinct = true;
    subquery.columns = vec![inner_value];
    subquery.r#where = Some(r#where);
    subquery.order_by.clear();

    let within = Expression::Function {
        func: Function::In,
        args: vec![outer_column.clone(), Expression::Subquery(Box::new(subquery))],
    };
    Some(Expression::BinaryOperation {
        left: Box::new(Expression::IsNull { expr: Box::new(outer_column), negated: true }),
        operator: BinaryOperator::And,
        right: Box::new(within),
    })
}

/// whether `expr` names a column of one of `tables`, a nested subquery counts as naming them
fn refers_to(expr: &Expression, tables: &[&str]) -> bool {
    match expr {
        Expression::QualifiedIdentifier { table, .. } => tables.contains(&table.as_str()),
        Expression::Path { head, .. } => tables.contains(&head.as_str()),
        Expression::Subquery(_) => true,
        Expression::UnaryOperation { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Nested(expr)
        | Expression::Alias { expr, .. } => refers_to(expr, tables),
        Expression::BinaryOperation { left, right, .. } => {
            refers_to(left, tables) || refers_to(right, tables)
        },
        Expression::Function { args, .. } => args.iter().any(|arg| refers_to(arg, tables)),
        Expression::Identifier(_)
        | Expression::Value(_)
        | Expression::Wildcard
        | Expression::Column(_)
        | Expression::JsonPath { .. } => false,
    }
}

/// `left operator right`, multiplicities as SQL defines them: without `ALL` the result is a set,
/// with it `INTERSECT ALL` keeps the lesser count of a row and `EXCEPT ALL` the difference
fn combine(
    mut left: Vec<Vec<Value>>,
    right: Vec<Vec<Value>>,
    operator: SetOperator,
    all: bool,
) -> Vec<Vec<Value>> {
    use crate::collections::hash::{HashMap, HashSet};
    match operator {
        SetOperator::Union => left.extend(right),
        SetOperator::Intersect | SetOperator::Except => {
            let mut counts: HashMap<Vec<Value>, usize> = HashMap::default();
            for row in right {
                *counts.entry(row).or_default() += 1;
            }
            left.retain(|row| match (operator, counts.get_mut(row)) {
                (SetOperator::Intersect, Some(count)) if *count > 0 => {
                    *count -= all as usize;
                    true
                },
                (SetOperator::Intersect, _) => false,
                (_, Some(count)) if *count > 0 => {
                    *count -= all as usize;
                    false
                },
                (_, _) => true,
            });
        },
    }

    if !all {
        let mut seen = HashSet::default();
        left.retain(|row| seen.insert(row.clone()));
    }
    left
}
