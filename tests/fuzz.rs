//! Metamorphic SQL fuzzing: random tables and predicates checked by two oracles
//! that need no second engine
//!
//! - **NoREC** (Rigger & Su, ESEC/FSE 2020): the rows `WHERE p` returns are exactly the rows whose projected `(p)` is true
//!   `WHERE` runs through pushdown, index cursors, bound fast paths and literal folding; the projection evaluates `p` row by row
//! - **TLP** (Rigger & Su, OOPSLA 2020): `p`, `(p) = false` and `(p) IS NULL` partition the table,
//!   so the partitions' rows, counts, sums, extremes and groups recombine into the unfiltered ones,
//!   over a single table and over joins
//! - **Limits**: `ORDER BY ... LIMIT k OFFSET o` is the slice of the unlimited order (top-N and
//!   index-order paths against the full sort)
//! - **Mutations**: inside a transaction, `UPDATE ... WHERE p` and `DELETE ... WHERE p` touch
//!   exactly the rows `WHERE p` selects, and `ROLLBACK` restores the table
//!
//! `UMBRA_FUZZ_SEEDS` raises the number of seeds, a failure prints the seed and the SQL

use std::collections::{BTreeMap, BTreeSet};
use umbra::db::MvccDatabase;
use umbra::sql::statement::Value;

const NUMBERS: [&str; 7] = ["-2", "-1", "0", "1", "2", "1.5", "2.25"];
const TEXTS: [&str; 4] = ["'a'", "'ab'", "'b'", "''"];

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D) % n
    }

    fn pick<'a>(&mut self, options: &[&'a str]) -> &'a str {
        options[self.below(options.len() as u64) as usize]
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// a table of random rows (with nulls) and random indexes, so each seed takes other access paths
fn table(rng: &mut Rng) -> MvccDatabase {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec(
        "CREATE TABLE t (id INT PRIMARY KEY, a INT NULLABLE, r REAL NULLABLE, s TEXT NULLABLE, \
         b BOOLEAN NULLABLE, n NUMERIC(10, 2) NULLABLE);",
    )
    .unwrap();

    for (column, index) in [("a", "idx_a"), ("s", "idx_s"), ("n", "idx_n"), ("r", "idx_r")] {
        if rng.chance(50) {
            db.exec(&format!("CREATE INDEX {index} ON t({column});")).unwrap();
        }
    }

    let rows: Vec<_> = (0..20 + rng.below(40))
        .map(|id| {
            let mut value = |options: &[&str]| match rng.chance(20) {
                true => "NULL".to_string(),
                _ => rng.pick(options).to_string(),
            };
            format!(
                "({id}, {}, {}, {}, {}, {})",
                value(&["-2", "-1", "0", "1", "2", "3"]),
                value(&["-1.5", "0", "1", "2.5"]),
                value(&TEXTS),
                value(&["true", "false"]),
                value(&["0.5", "1", "2.25", "-1"]),
            )
        })
        .collect();
    db.exec(&format!("INSERT INTO t (id, a, r, s, b, n) VALUES {};", rows.join(", ")))
        .unwrap();

    db.exec("CREATE TABLE u (uid INT PRIMARY KEY, ua INT NULLABLE, us TEXT NULLABLE);")
        .unwrap();
    if rng.chance(50) {
        db.exec("CREATE INDEX idx_ua ON u(ua);").unwrap();
    }
    let rows: Vec<_> = (0..5 + rng.below(15))
        .map(|uid| {
            let ua = match rng.chance(20) {
                true => "NULL",
                _ => rng.pick(&["-1", "0", "1", "2", "3"]),
            };
            let us = match rng.chance(20) {
                true => "NULL",
                _ => rng.pick(&TEXTS),
            };
            format!("({uid}, {ua}, {us})")
        })
        .collect();
    db.exec(&format!("INSERT INTO u (uid, ua, us) VALUES {};", rows.join(", ")))
        .unwrap();
    db
}

/// a numeric term: a column, a literal, or arithmetic over terms
fn number(rng: &mut Rng, depth: u32) -> String {
    match rng.below(match depth == 0 {
        true => 2,
        _ => 6,
    }) {
        0 => rng.pick(&["a", "r", "n"]).to_string(),
        1 => rng.pick(&NUMBERS).to_string(),
        2 => format!(
            "({} {} {})",
            number(rng, depth - 1),
            rng.pick(&["+", "-", "*", "/"]),
            number(rng, depth - 1)
        ),
        3 => format!("-{}", number(rng, depth - 1)),
        4 => format!("ABS({})", number(rng, depth - 1)),
        _ => format!("COALESCE({}, {})", number(rng, depth - 1), rng.pick(&NUMBERS)),
    }
}

/// a boolean predicate over the table, type-correct by construction
fn predicate(rng: &mut Rng, depth: u32) -> String {
    let comparison = || ["=", "!=", "<", "<=", ">", ">="];
    match rng.below(match depth == 0 {
        true => 7,
        _ => 10,
    }) {
        0 => format!("{} {} {}", number(rng, depth), rng.pick(&comparison()), number(rng, depth)),
        1 => format!("s {} {}", rng.pick(&comparison()), rng.pick(&TEXTS)),
        2 => format!("{} IS {}NULL", rng.pick(&["a", "r", "s", "b", "n"]), rng.pick(&["", "NOT "])),
        3 => format!(
            "{} BETWEEN {} AND {}",
            rng.pick(&["a", "r", "n"]),
            rng.pick(&NUMBERS),
            rng.pick(&NUMBERS)
        ),
        4 => format!(
            "{} IN ({}, {}, {})",
            rng.pick(&["a", "n"]),
            rng.pick(&NUMBERS),
            rng.pick(&NUMBERS),
            rng.pick(&NUMBERS)
        ),
        5 => format!("s LIKE {}", rng.pick(&["'a%'", "'%b'", "'_'", "'%'"])),
        6 => rng.pick(&["b", "b = true", "b = false"]).to_string(),
        7 => format!("({}) AND ({})", predicate(rng, depth - 1), predicate(rng, depth - 1)),
        8 => format!("({}) OR ({})", predicate(rng, depth - 1), predicate(rng, depth - 1)),
        _ => format!("({}) = {}", predicate(rng, depth - 1), rng.pick(&["true", "false"])),
    }
}

fn rows(db: &mut MvccDatabase, sql: &str) -> Result<Vec<Vec<Value>>, String> {
    db.exec(sql)
        .map(|result| result.tuples)
        .map_err(|error| format!("{sql}: {error}"))
}

fn affected(db: &mut MvccDatabase, sql: &str) -> Result<usize, String> {
    match rows(db, sql)?.as_slice() {
        [row] => match row.as_slice() {
            [Value::Number(count)] => Ok(*count as usize),
            other => panic!("affected {other:?}"),
        },
        other => panic!("affected {other:?}"),
    }
}

fn ids(db: &mut MvccDatabase, sql: &str) -> Result<BTreeSet<i128>, String> {
    Ok(rows(db, sql)?
        .iter()
        .map(|row| match row[0] {
            Value::Number(id) => id,
            ref other => panic!("id {other:?}"),
        })
        .collect())
}

/// the rows of `select` under `p`, `(p) = false` and `(p) IS NULL`, sorted, with the unfiltered rows
/// `select` ends where a `WHERE` can follow
fn partitions(
    db: &mut MvccDatabase,
    select: &str,
    p: &str,
    tail: &str,
) -> Result<[Vec<Vec<Value>>; 4], String> {
    let mut sorted = |filter: &str| {
        rows(db, &format!("{select}{filter}{tail};")).map(|mut rows| {
            rows.sort();
            rows
        })
    };
    Ok([
        sorted(&format!(" WHERE {p}"))?,
        sorted(&format!(" WHERE ({p}) = false"))?,
        sorted(&format!(" WHERE ({p}) IS NULL"))?,
        sorted("")?,
    ])
}

/// checks every oracle on `p`; `Err` describes the mismatch
fn check(db: &mut MvccDatabase, rng: &mut Rng, p: &str) -> Result<(), String> {
    let filtered = ids(db, &format!("SELECT id FROM t WHERE {p};"));
    let projected = rows(db, &format!("SELECT id, ({p}) FROM t;")).map(|rows| {
        rows.iter()
            .filter(|row| row[1] == Value::Boolean(true))
            .map(|row| match row[0] {
                Value::Number(id) => id,
                ref other => panic!("id {other:?}"),
            })
            .collect::<BTreeSet<_>>()
    });

    let filtered = match (filtered, projected) {
        (Ok(filtered), Ok(projected)) if filtered == projected => filtered,
        (Ok(filtered), Ok(projected)) => {
            return Err(format!(
                "NoREC: WHERE returned {filtered:?}, projection says {projected:?}"
            ));
        },
        // an expression both paths reject (e.g. a type error) is not a mismatch
        (Err(_), Err(_)) => return Ok(()),
        (filtered, projected) => {
            return Err(format!(
                "NoREC: one path failed: WHERE {filtered:?}, projection {projected:?}"
            ));
        },
    };

    let all = ids(db, "SELECT id FROM t;")?;
    let negated = ids(db, &format!("SELECT id FROM t WHERE ({p}) = false;"))?;
    let unknown = ids(db, &format!("SELECT id FROM t WHERE ({p}) IS NULL;"))?;
    let disjoint = filtered.is_disjoint(&negated)
        && filtered.is_disjoint(&unknown)
        && negated.is_disjoint(&unknown);
    let union: BTreeSet<_> = filtered.iter().chain(&negated).chain(&unknown).copied().collect();
    if !disjoint || union != all {
        let msg = format!(
            "TLP: true {filtered:?}, false {negated:?}, null {unknown:?} do not partition {all:?}"
        );
        return Err(msg);
    }

    aggregates(db, p)?;
    groups(db, p)?;
    joins(db, rng, p)?;
    limits(db, rng, p)?;

    let mutated = mutations(db, p, &filtered, &all);
    if db.active_transaction() {
        db.rollback().map_err(|error| error.to_string())?;
    }
    mutated?;
    match ids(db, "SELECT id FROM t;")? == all {
        true => Ok(()),
        _ => Err("ROLLBACK did not restore the table".into()),
    }
}

/// DML oracle, the caller rolls back whatever this leaves open
fn mutations(
    db: &mut MvccDatabase,
    p: &str,
    filtered: &BTreeSet<i128>,
    all: &BTreeSet<i128>,
) -> Result<(), String> {
    rows(db, "START TRANSACTION;")?;
    let updated = affected(db, &format!("UPDATE t SET a = a WHERE {p};"))?;
    let deleted = affected(db, &format!("DELETE FROM t WHERE {p};"))?;
    let left = ids(db, "SELECT id FROM t;")?;

    let expected: BTreeSet<_> = all.difference(filtered).copied().collect();
    match (updated, deleted, &left) {
        (updated, deleted, left)
            if updated == filtered.len() && deleted == filtered.len() && *left == expected =>
        {
            Ok(())
        },
        _ => Err(format!(
            "mutations: WHERE selects {filtered:?}; UPDATE touched {updated}, DELETE {deleted}, left {left:?}"
        )),
    }
}

/// TLP over aggregates: counts and integer sums add up, extremes are the extreme of the parts
fn aggregates(db: &mut MvccDatabase, p: &str) -> Result<(), String> {
    let [t, f, n, all] = partitions(db, "SELECT COUNT(*), SUM(a), MIN(a), MAX(s) FROM t", p, "")?;
    let parts = [&t[0], &f[0], &n[0]];

    let count = |row: &Vec<Value>| match row[0] {
        Value::Number(count) => count,
        ref other => panic!("count {other:?}"),
    };
    let sum = |row: &Vec<Value>| match row[1] {
        Value::Number(sum) => sum,
        Value::Null => 0,
        ref other => panic!("sum {other:?}"),
    };
    let extreme = |column: usize, max: bool| {
        let values = parts.iter().map(|row| &row[column]).filter(|value| **value != Value::Null);
        match max {
            true => values.max(),
            false => values.min(),
        }
        .cloned()
        .unwrap_or(Value::Null)
    };

    let recombined = (
        parts.iter().map(|row| count(row)).sum::<i128>(),
        parts.iter().map(|row| sum(row)).sum::<i128>(),
        extreme(2, false),
        extreme(3, true),
    );
    let whole = (count(&all[0]), sum(&all[0]), all[0][2].clone(), all[0][3].clone());
    match recombined == whole {
        true => Ok(()),
        _ => Err(format!(
            "TLP aggregates: parts {parts:?} recombine to {recombined:?}, whole {whole:?}"
        )),
    }
}

/// TLP over `GROUP BY`: per group, the partitions' counts add up to the unfiltered count
fn groups(db: &mut MvccDatabase, p: &str) -> Result<(), String> {
    let [t, f, n, all] = partitions(db, "SELECT s, COUNT(*) FROM t", p, " GROUP BY s")?;
    let mut recombined = BTreeMap::new();
    for row in t.iter().chain(&f).chain(&n) {
        let Value::Number(count) = row[1] else {
            panic!("count {:?}", row[1])
        };
        *recombined.entry(row[0].clone()).or_insert(0) += count;
    }
    let whole: BTreeMap<_, _> = all
        .iter()
        .map(|row| match row[1] {
            Value::Number(count) => (row[0].clone(), count),
            ref other => panic!("count {other:?}"),
        })
        .collect();

    match recombined == whole {
        true => Ok(()),
        _ => Err(format!("TLP groups: parts recombine to {recombined:?}, whole {whole:?}")),
    }
}

/// TLP and NoREC over a join of `t` with `u`, `p` filtering after the join
fn joins(db: &mut MvccDatabase, rng: &mut Rng, p: &str) -> Result<(), String> {
    let join = rng.pick(&["JOIN", "LEFT JOIN"]);
    let on = rng.pick(&["t.a = u.ua", "t.s = u.us", "t.a < u.ua", "t.a = u.ua AND t.s = u.us"]);
    let select = format!("SELECT t.id, u.uid FROM t {join} u ON {on}");

    let [t, f, n, all] = partitions(db, &select, p, "")?;
    let mut union: Vec<_> = t.iter().chain(&f).chain(&n).cloned().collect();
    union.sort();
    if union != all {
        return Err(format!("TLP join `{select}`: parts {t:?} | {f:?} | {n:?}, whole {all:?}"));
    }

    let mut projected: Vec<_> =
        rows(db, &format!("SELECT t.id, u.uid, ({p}) FROM t {join} u ON {on};"))?
            .into_iter()
            .filter(|row| row[2] == Value::Boolean(true))
            .map(|mut row| {
                row.pop();
                row
            })
            .collect();
    projected.sort();

    match projected == t {
        true => Ok(()),
        _ => Err(format!("NoREC join `{select}`: WHERE {t:?}, projection {projected:?}")),
    }
}

/// `LIMIT`/`OFFSET` slice the unlimited order; the order is total thanks to the `id` tie-break
fn limits(db: &mut MvccDatabase, rng: &mut Rng, p: &str) -> Result<(), String> {
    let order = format!(
        "{} {}, id {}",
        rng.pick(&["a", "r", "s", "n", "id"]),
        rng.pick(&["ASC", "DESC"]),
        rng.pick(&["ASC", "DESC"])
    );
    let (limit, offset) = (rng.below(8) as usize, rng.below(5) as usize);
    let full = rows(db, &format!("SELECT id FROM t WHERE {p} ORDER BY {order};"))?;
    let sliced = rows(
        db,
        &format!("SELECT id FROM t WHERE {p} ORDER BY {order} LIMIT {limit} OFFSET {offset};"),
    )?;

    let expected: Vec<_> = full.iter().skip(offset).take(limit).cloned().collect();
    match sliced == expected {
        true => Ok(()),
        _ => {
            let msg = format!(
                "LIMIT: ORDER BY {order} LIMIT {limit} OFFSET {offset} gave {sliced:?}, slice {expected:?}"
            );
            Err(msg)
        },
    }
}

#[test]
fn random_predicates_agree_across_execution_paths() {
    let seeds: u64 = std::env::var("UMBRA_FUZZ_SEEDS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(40);

    for seed in 0..seeds {
        let mut rng = Rng::new(seed);
        let mut db = table(&mut rng);
        for _ in 0..40 {
            let p = predicate(&mut rng, 3);
            // a panic inside the engine is a finding too, reported with its seed
            let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                check(&mut db, &mut rng, &p)
            }));
            match checked {
                Ok(Ok(())) => {},
                // whether a runtime error is raised may depend on the path: `WHERE` can skip `x`
                // in `NULL AND x`, which the projection must evaluate
                Ok(Err(mismatch)) if mismatch.to_lowercase().contains("division by zero") => {},
                Ok(Err(mismatch)) => panic!("seed {seed}: {mismatch}\n  predicate: {p}"),
                Err(_) => panic!("seed {seed}: the engine panicked\n  predicate: {p}"),
            }
        }
    }
}

/// seed 10: the simplifier rewrote `x * 0` to `0`, so `NULL * 0 < 1.5` became true
#[test]
fn multiplying_null_by_zero_stays_null() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(10, 2) NULLABLE);")
        .unwrap();
    db.exec("INSERT INTO t (id, n) VALUES (1, 2.25), (2, NULL);").unwrap();

    assert_eq!(
        ids(&mut db, "SELECT id FROM t WHERE (n * -(0 * 2)) < 1.5;").unwrap(),
        [1].into()
    );
    assert_eq!(ids(&mut db, "SELECT id FROM t WHERE n * 0 IS NULL;").unwrap(), [2].into());
    assert!(db.exec("SELECT id FROM t WHERE 0 / (n - n) = 0;").is_err(), "0 / 0 is an error");
}

/// seed 41: `ABS(NULL)` was a type error, hidden in `WHERE` when `OR` short-circuited
#[test]
fn scalar_functions_of_null_are_null() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(10, 2) NULLABLE, s TEXT NULLABLE);")
        .unwrap();
    db.exec("INSERT INTO t (id, n, s) VALUES (1, NULL, NULL);").unwrap();

    for call in [
        "ABS(n)",
        "SIGN(n)",
        "SQRT(n)",
        "POWER(n, 2)",
        "POWER(2, n)",
        "TRUNC(n)",
        "TRUNC(1.25, n)",
        "ASCII(s)",
        "POSITION('a' IN s)",
        "POSITION(s IN 'a')",
        "SUBSTRING(s FROM 1 FOR 2)",
    ] {
        let rows = rows(&mut db, &format!("SELECT {call} FROM t;"));
        assert_eq!(rows, Ok(vec![vec![Value::Null]]), "{call}");
    }
}

/// seed 85: `WHERE` short-circuited `OR`, the projection evaluated both sides and divided by zero
#[test]
fn and_or_short_circuit_everywhere() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, a INT);").unwrap();
    db.exec("INSERT INTO t (id, a) VALUES (1, 1);").unwrap();

    let decided = [("a = 1 OR 1 / (a - 1) = 0", true), ("a != 1 AND 1 / (a - 1) = 0", false)];
    for (p, value) in decided {
        let result = Ok(vec![vec![Value::Boolean(value)]]);
        assert_eq!(rows(&mut db, &format!("SELECT ({p}) FROM t;")), result, "{p}");
    }
    assert!(
        db.exec("SELECT (a != 1 OR 1 / (a - 1) = 0) FROM t;").is_err(),
        "undecided: evaluates the right"
    );
}

/// seed 353: long division by a multi-digit NUMERIC produced negative digits, and dividing by
/// such a quotient divided by zero inside the engine
#[test]
fn numeric_division_by_long_divisors() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, n NUMERIC(10, 2), m NUMERIC(10, 4));")
        .unwrap();
    db.exec("INSERT INTO t (id, n, m) VALUES (1, 0.5, 1.5), (2, 1, 0.0625), (3, 2.25, 1.5), (4, -1, 12.5);")
        .unwrap();

    assert_eq!(rows(&mut db, "SELECT id, 2.25 / ((-2 / 2.25) / n) FROM t;").unwrap().len(), 4);
    for (p, expected) in [
        ("n / m = 1.5", [3].into()),
        ("n / m = 16", [2].into()),
        ("n / m = -0.08", [4].into()),
        ("(n / m) * m - n < 0.0001 AND (n / m) * m - n > -0.0001", [1, 2, 3, 4].into()),
    ] {
        assert_eq!(ids(&mut db, &format!("SELECT id FROM t WHERE {p};")), Ok(expected), "{p}");
    }
}
