//! Isolation levels through SQL: syntax, conflicts and the anomalies each level forbids

use std::sync::Barrier;
use std::thread;
use umbra::db::{DatabaseError, MvccDatabase};
use umbra::sql::statement::Value;

/// the hermitage fixture: `test (id, value)` holding `(1, 10), (2, 20)`
fn hermitage() -> MvccDatabase {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE test (id INT PRIMARY KEY, value INT);").unwrap();
    db.exec("INSERT INTO test (id, value) VALUES (1, 10), (2, 20);").unwrap();
    db
}

fn values(db: &mut MvccDatabase, query: &str) -> Vec<i128> {
    db.exec(query)
        .unwrap()
        .tuples
        .iter()
        .map(|row| match row[0] {
            Value::Number(n) => n,
            ref other => panic!("expected a number, got {other:?}"),
        })
        .collect()
}

fn value_of(db: &mut MvccDatabase, id: i128) -> i128 {
    values(db, &format!("SELECT value FROM test WHERE id = {id};"))[0]
}

fn is_conflict<T: std::fmt::Debug>(result: Result<T, DatabaseError>) -> bool {
    matches!(result, Err(DatabaseError::Conflict(_)))
}

#[test]
fn every_isolation_syntax_form_parses() {
    let mut db = hermitage();

    for statement in [
        "BEGIN;",
        "BEGIN TRANSACTION;",
        "START TRANSACTION;",
        "BEGIN ISOLATION LEVEL READ COMMITTED;",
        "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;",
        "START TRANSACTION ISOLATION LEVEL SNAPSHOT;",
        "begin transaction isolation level serializable;",
    ] {
        db.exec(statement).unwrap();
        db.exec("SELECT * FROM test;").unwrap();
        db.exec("COMMIT;").unwrap();
    }

    db.exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").unwrap();
    db.exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
        .unwrap();
    db.exec("CHECKPOINT;").unwrap();
    assert!(db.exec("BEGIN ISOLATION LEVEL CHAOS;").is_err());
}

#[test]
fn set_transaction_after_the_first_statement_is_an_error() {
    let mut db = hermitage();

    db.exec("BEGIN;").unwrap();
    db.exec("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").unwrap();
    db.exec("SELECT * FROM test;").unwrap();
    assert!(db.exec("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;").is_err());
    db.exec("ROLLBACK;").unwrap();
}

#[test]
fn isolation_words_remain_usable_as_names() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE session (level INT PRIMARY KEY, read INT, snapshot INT);")
        .unwrap();
    db.exec("INSERT INTO session (level, read, snapshot) VALUES (1, 2, 3);")
        .unwrap();

    assert_eq!(values(&mut db, "SELECT snapshot FROM session WHERE level = 1;"), [3]);
}

#[test]
fn concurrent_writers_of_one_row_conflict() {
    let mut first = hermitage();
    let mut second = first.session();

    first.exec("BEGIN;").unwrap();
    second.exec("BEGIN;").unwrap();
    first.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();

    assert!(is_conflict(second.exec("UPDATE test SET value = 12 WHERE id = 1;")));
    second.exec("ROLLBACK;").unwrap();
    first.exec("COMMIT;").unwrap();

    assert_eq!(value_of(&mut first, 1), 11);
}

#[test]
fn read_committed_increments_from_many_sessions_are_never_lost() {
    let db = hermitage();
    let sessions = 8;
    let increments = 100;
    let barrier = Barrier::new(sessions);

    thread::scope(|scope| {
        for _ in 0..sessions {
            let mut session = db.session();
            let barrier = &barrier;
            scope.spawn(move || {
                session
                    .exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
                    .unwrap();
                barrier.wait();

                let mut done = 0;
                while done < increments {
                    match session.exec("UPDATE test SET value = value + 1 WHERE id = 1;") {
                        Ok(_) => done += 1,
                        Err(DatabaseError::Conflict(_)) => {}
                        Err(error) => panic!("{error}"),
                    }
                }
            });
        }
    });

    let mut db = db;
    assert_eq!(value_of(&mut db, 1), 10 + (sessions * increments) as i128);
}

#[test]
fn snapshot_rejects_a_lost_update_that_read_committed_resolves() {
    for (level, expected) in [("SNAPSHOT", None), ("READ COMMITTED", Some(12))] {
        let mut first = hermitage();
        let mut second = first.session();

        first.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        second.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        assert_eq!(value_of(&mut first, 1), 10);
        assert_eq!(value_of(&mut second, 1), 10);

        first.exec("UPDATE test SET value = value + 1 WHERE id = 1;").unwrap();
        first.exec("COMMIT;").unwrap();

        let second_update = second.exec("UPDATE test SET value = value + 1 WHERE id = 1;");
        match expected {
            None => {
                assert!(is_conflict(second_update), "{level}");
                second.exec("ROLLBACK;").unwrap();
                assert_eq!(value_of(&mut first, 1), 11);
            },
            Some(final_value) => {
                second_update.unwrap();
                second.exec("COMMIT;").unwrap();
                assert_eq!(value_of(&mut first, 1), final_value);
            },
        }
    }
}

#[test]
fn write_skew_is_allowed_at_snapshot_and_rejected_at_serializable() {
    for (level, skew) in [("SNAPSHOT", true), ("SERIALIZABLE", false)] {
        let mut first = hermitage();
        let mut second = first.session();

        first.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        second.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        assert_eq!(values(&mut first, "SELECT value FROM test;").len(), 2);
        assert_eq!(values(&mut second, "SELECT value FROM test;").len(), 2);

        first.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();
        second.exec("UPDATE test SET value = 21 WHERE id = 2;").unwrap();
        first.exec("COMMIT;").unwrap();

        let committed = second.exec("COMMIT;");
        match skew {
            true => assert!(committed.is_ok(), "{level}"),
            false => assert!(is_conflict(committed), "{level}"),
        }
    }
}

const LEVELS: [&str; 3] = ["READ COMMITTED", "SNAPSHOT", "SERIALIZABLE"];

/// two sessions over one Hermitage database, both inside a transaction at `level`
fn pair(level: &str) -> (MvccDatabase, MvccDatabase) {
    let mut first = hermitage();
    let mut second = first.session();
    first.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
    second.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
    (first, second)
}

fn all_values(db: &mut MvccDatabase) -> Vec<i128> {
    values(db, "SELECT value FROM test ORDER BY id;")
}

/// G0: two writers of the same rows; the second is refused instead of interleaving
#[test]
fn g0_dirty_writes_never_interleave() {
    for level in LEVELS {
        let (mut t1, mut t2) = pair(level);

        t1.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();
        assert!(is_conflict(t2.exec("UPDATE test SET value = 12 WHERE id = 1;")), "{level}");
        t1.exec("UPDATE test SET value = 21 WHERE id = 2;").unwrap();
        t1.exec("COMMIT;").unwrap();
        t2.exec("ROLLBACK;").unwrap();

        assert_eq!(all_values(&mut t1), [11, 21], "{level}");
    }
}

/// G1a: a rolled-back write is never read
#[test]
fn g1a_aborted_reads_are_impossible() {
    for level in LEVELS {
        let (mut t1, mut t2) = pair(level);

        t1.exec("UPDATE test SET value = 101 WHERE id = 1;").unwrap();
        assert_eq!(value_of(&mut t2, 1), 10, "{level}");
        t1.exec("ROLLBACK;").unwrap();
        assert_eq!(value_of(&mut t2, 1), 10, "{level}");
        t2.exec("COMMIT;").unwrap();
    }
}

/// G1b: an intermediate (later overwritten) write is never read
#[test]
fn g1b_intermediate_reads_are_impossible() {
    for (level, after_commit) in LEVELS.into_iter().zip([11, 10, 10]) {
        let (mut t1, mut t2) = pair(level);

        t1.exec("UPDATE test SET value = 101 WHERE id = 1;").unwrap();
        assert_eq!(value_of(&mut t2, 1), 10, "{level}");
        t1.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();
        t1.exec("COMMIT;").unwrap();
        assert_eq!(value_of(&mut t2, 1), after_commit, "{level}");
        t2.exec("COMMIT;").unwrap();
    }
}

/// G1c: neither transaction sees the other's uncommitted write
#[test]
fn g1c_circular_information_flow_is_impossible() {
    for level in LEVELS {
        let (mut t1, mut t2) = pair(level);

        t1.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();
        t2.exec("UPDATE test SET value = 22 WHERE id = 2;").unwrap();
        assert_eq!(value_of(&mut t1, 2), 20, "{level}");
        assert_eq!(value_of(&mut t2, 1), 10, "{level}");
        t1.exec("COMMIT;").unwrap();

        // each read the row the other wrote: an anti-dependency cycle SERIALIZABLE must refuse
        let second = t2.exec("COMMIT;");
        match level {
            "SERIALIZABLE" => assert!(is_conflict(second)),
            _ => assert!(second.is_ok(), "{level}"),
        }
    }
}

/// OTV: once a reader sees one write of a transaction it sees all of them, later transactions'
/// writes never pair with half of an earlier one's
#[test]
fn otv_observed_transactions_never_vanish() {
    for level in LEVELS {
        let mut t1 = hermitage();
        let mut t2 = t1.session();
        let mut t3 = t1.session();

        t1.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        t1.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();
        t1.exec("UPDATE test SET value = 19 WHERE id = 2;").unwrap();
        t1.exec("COMMIT;").unwrap();

        t3.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        assert_eq!(value_of(&mut t3, 1), 11, "{level}");

        t2.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
        t2.exec("UPDATE test SET value = 12 WHERE id = 1;").unwrap();
        t2.exec("UPDATE test SET value = 18 WHERE id = 2;").unwrap();
        assert_eq!(value_of(&mut t3, 2), 19, "{level}");
        t2.exec("COMMIT;").unwrap();

        let (id1, id2) = (value_of(&mut t3, 1), value_of(&mut t3, 2));
        match level {
            "READ COMMITTED" => assert_eq!((id1, id2), (12, 18)),
            _ => assert_eq!((id1, id2), (11, 19), "{level}"),
        }
        t3.exec("COMMIT;").unwrap();
    }
}

/// PMP: a predicate read is repeatable from SNAPSHOT up, READ COMMITTED sees the new row
/// (negative control)
#[test]
fn pmp_predicate_reads_are_repeatable_from_snapshot_up() {
    for (level, phantom) in LEVELS.into_iter().zip([true, false, false]) {
        let (mut t1, mut t2) = pair(level);

        assert!(values(&mut t1, "SELECT value FROM test WHERE value > 25;").is_empty());
        t2.exec("INSERT INTO test (id, value) VALUES (3, 30);").unwrap();
        t2.exec("COMMIT;").unwrap();

        let seen = values(&mut t1, "SELECT value FROM test WHERE value > 25;");
        assert_eq!(!seen.is_empty(), phantom, "{level}");
        t1.exec("COMMIT;").unwrap();
    }
}

/// P4: a read-modify-write never loses a concurrent update
/// READ COMMITTED re-applies the second update on the newest version, SNAPSHOT and SERIALIZABLE refuse it
#[test]
fn p4_lost_updates_are_impossible() {
    for level in LEVELS {
        let (mut t1, mut t2) = pair(level);

        assert_eq!(value_of(&mut t1, 1), 10);
        assert_eq!(value_of(&mut t2, 1), 10);
        t1.exec("UPDATE test SET value = value + 1 WHERE id = 1;").unwrap();
        t1.exec("COMMIT;").unwrap();

        let second = t2.exec("UPDATE test SET value = value + 1 WHERE id = 1;");
        match level {
            "READ COMMITTED" => {
                second.unwrap();
                t2.exec("COMMIT;").unwrap();
                assert_eq!(value_of(&mut t1, 1), 12);
            },
            _ => {
                assert!(is_conflict(second), "{level}");
                t2.exec("ROLLBACK;").unwrap();
                assert_eq!(value_of(&mut t1, 1), 11, "{level}");
            },
        }
    }
}

/// G-single: no read skew from SNAPSHOT up, READ COMMITTED shows it (negative control)
#[test]
fn g_single_read_skew_is_impossible_from_snapshot_up() {
    for (level, skew) in LEVELS.into_iter().zip([true, false, false]) {
        let (mut t1, mut t2) = pair(level);

        assert_eq!(value_of(&mut t1, 1), 10);
        assert_eq!(all_values(&mut t2), [10, 20]);
        t2.exec("UPDATE test SET value = 12 WHERE id = 1;").unwrap();
        t2.exec("UPDATE test SET value = 18 WHERE id = 2;").unwrap();
        t2.exec("COMMIT;").unwrap();

        let id2 = value_of(&mut t1, 2);
        assert_eq!(id2 == 18, skew, "{level}");
        t1.exec("COMMIT;").unwrap();
    }
}

/// G2-item: write skew on items commits below SERIALIZABLE (negative controls) and is refused at it
#[test]
fn g2_item_write_skew_is_refused_only_at_serializable() {
    for (level, skew) in LEVELS.into_iter().zip([true, true, false]) {
        let (mut t1, mut t2) = pair(level);

        assert_eq!(values(&mut t1, "SELECT value FROM test WHERE id = 1 OR id = 2;").len(), 2);
        assert_eq!(values(&mut t2, "SELECT value FROM test WHERE id = 1 OR id = 2;").len(), 2);
        t1.exec("UPDATE test SET value = 11 WHERE id = 1;").unwrap();
        t2.exec("UPDATE test SET value = 21 WHERE id = 2;").unwrap();
        t1.exec("COMMIT;").unwrap();

        let committed = t2.exec("COMMIT;");
        match skew {
            true => assert!(committed.is_ok(), "{level}"),
            false => assert!(is_conflict(committed), "{level}"),
        }
    }
}

/// G2: write skew through a predicate (each inserts what the other's read would have seen)
#[test]
fn g2_predicate_write_skew_is_refused_only_at_serializable() {
    for (level, skew) in LEVELS.into_iter().zip([true, true, false]) {
        let (mut t1, mut t2) = pair(level);

        assert!(values(&mut t1, "SELECT value FROM test WHERE value > 25;").is_empty());
        assert!(values(&mut t2, "SELECT value FROM test WHERE value > 25;").is_empty());
        t1.exec("INSERT INTO test (id, value) VALUES (3, 30);").unwrap();
        t2.exec("INSERT INTO test (id, value) VALUES (4, 42);").unwrap();
        t1.exec("COMMIT;").unwrap();

        let committed = t2.exec("COMMIT;");
        match skew {
            true => assert!(committed.is_ok(), "{level}"),
            false => assert!(is_conflict(committed), "{level}"),
        }
    }
}

/// xorshift64*: seeded, reproducible randomness for the load tests
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
}

fn try_values(db: &mut MvccDatabase, query: &str) -> Result<Vec<i128>, DatabaseError> {
    Ok(db
        .exec(query)?
        .tuples
        .iter()
        .map(|row| match row[0] {
            Value::Number(n) => n,
            ref other => panic!("expected a number, got {other:?}"),
        })
        .collect())
}

/// ends a failed explicit transaction, if the failure left one open
fn abandon(db: &mut MvccDatabase) {
    if db.active_transaction() {
        db.exec("ROLLBACK;").unwrap();
    }
}

#[test]
fn bank_transfers_conserve_the_total_in_every_snapshot() {
    const ACCOUNTS: u64 = 16;
    const START: i128 = 1_000;

    for (level, seed) in [("SNAPSHOT", 7), ("SERIALIZABLE", 11)] {
        let mut db = MvccDatabase::in_memory().unwrap();
        db.exec("CREATE TABLE accounts (id INT PRIMARY KEY, balance INT);").unwrap();
        let rows: Vec<_> = (0..ACCOUNTS).map(|id| format!("({id}, {START})")).collect();
        let sql = format!("INSERT INTO accounts (id, balance) VALUES {};", rows.join(", "));
        db.exec(&sql).unwrap();

        let barrier = Barrier::new(8);
        thread::scope(|scope| {
            for worker in 0..6 {
                let (mut session, barrier) = (db.session(), &barrier);
                scope.spawn(move || {
                    let mut rng = Rng::new(seed * 100 + worker);
                    barrier.wait();

                    for _ in 0..300 {
                        let (from, to) = (rng.below(ACCOUNTS), rng.below(ACCOUNTS));
                        let amount = rng.below(50) as i128 + 1;

                        let transfer = |db: &mut MvccDatabase| -> Result<(), DatabaseError> {
                            db.exec(&format!("BEGIN ISOLATION LEVEL {level};"))?;
                            let balance = |db: &mut MvccDatabase, id| {
                                try_values(
                                    db,
                                    &format!("SELECT balance FROM accounts WHERE id = {id};"),
                                )
                                .map(|values| values[0])
                            };
                            let (a, b) = (balance(db, from)?, balance(db, to)?);
                            if from != to && a >= amount {
                                db.exec(&format!(
                                    "UPDATE accounts SET balance = {} WHERE id = {from};",
                                    a - amount
                                ))?;
                                db.exec(&format!(
                                    "UPDATE accounts SET balance = {} WHERE id = {to};",
                                    b + amount
                                ))?;
                            }
                            db.exec("COMMIT;").map(|_| ())
                        };

                        if transfer(&mut session).is_err() {
                            abandon(&mut session);
                        }
                    }
                });
            }

            for _ in 0..2 {
                let (mut session, barrier) = (db.session(), &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..200 {
                        session.exec(&format!("BEGIN ISOLATION LEVEL {level};")).unwrap();
                        let low =
                            values(&mut session, "SELECT balance FROM accounts WHERE id < 8;");
                        let high =
                            values(&mut session, "SELECT balance FROM accounts WHERE id >= 8;");
                        let audited = session.exec("COMMIT;");

                        let total: i128 = low.iter().chain(&high).sum();
                        assert_eq!(total, ACCOUNTS as i128 * START, "{level}, seed {seed}");
                        assert!(low.iter().chain(&high).all(|&b| b >= 0), "{level}");
                        assert!(audited.is_ok(), "read-only transactions never abort");
                    }
                });
            }
        });

        let total: i128 = values(&mut db, "SELECT balance FROM accounts;").iter().sum();
        assert_eq!(total, ACCOUNTS as i128 * START, "{level}");
    }
}

#[test]
fn concurrent_write_skew_never_breaks_the_invariant_at_serializable() {
    for round in 0..50 {
        let mut db = MvccDatabase::in_memory().unwrap();
        db.exec("CREATE TABLE doctors (id INT PRIMARY KEY, on_call INT);").unwrap();
        db.exec("INSERT INTO doctors (id, on_call) VALUES (1, 1), (2, 1);").unwrap();

        let barrier = Barrier::new(2);
        thread::scope(|scope| {
            for id in [1, 2] {
                let (mut session, barrier) = (db.session(), &barrier);
                scope.spawn(move || {
                    session.exec("BEGIN ISOLATION LEVEL SERIALIZABLE;").unwrap();
                    let on_call: i128 =
                        values(&mut session, "SELECT on_call FROM doctors;").iter().sum();
                    barrier.wait();
                    if on_call >= 2 {
                        let off = session
                            .exec(&format!("UPDATE doctors SET on_call = 0 WHERE id = {id};"));
                        if off.and_then(|_| session.exec("COMMIT;")).is_err() {
                            abandon(&mut session);
                        }
                    }
                });
            }
        });

        let on_call: i128 = values(&mut db, "SELECT on_call FROM doctors;").iter().sum();
        assert!(on_call >= 1, "round {round}: nobody left on call");
    }
}

#[derive(Debug)]
enum Op {
    Read(i64, Vec<i64>),
    Append(i64, i64),
}

struct Recorded {
    ops: Vec<Op>,
    committed: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Edge {
    Write,
    Read,
    Anti,
}

/// transactions per worker in the list-append runs: `UMBRA_HISTORY_TXNS` raises it for long runs
fn history_length() -> u64 {
    std::env::var("UMBRA_HISTORY_TXNS")
        .ok()
        .and_then(|n| n.parse().ok())
        .unwrap_or(150)
}

/// runs random list-append transactions from several threads and returns the history plus the
/// final list of every key
fn list_append_history(level: &str, seed: u64) -> (Vec<Recorded>, Vec<Vec<i64>>) {
    const KEYS: u64 = 4;
    let transactions = history_length();

    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE lists (id BIGINT PRIMARY KEY, k BIGINT, pos BIGINT, v BIGINT);")
        .unwrap();

    let history = std::sync::Mutex::new(Vec::new());
    let barrier = Barrier::new(6);

    thread::scope(|scope| {
        for worker in 0..6u64 {
            let (mut session, history, barrier) = (db.session(), &history, &barrier);
            scope.spawn(move || {
                let mut rng = Rng::new(seed * 1_000 + worker);
                barrier.wait();

                for txn in 0..transactions {
                    let mut ops = Vec::new();
                    let mut run = |db: &mut MvccDatabase, ops: &mut Vec<Op>| -> Result<(), DatabaseError> {
                        db.exec(&format!("BEGIN ISOLATION LEVEL {level};"))?;
                        for op in 0..=rng.below(3) {
                            let key = rng.below(KEYS) as i64;
                            let list: Vec<i64> = try_values(db, &format!("SELECT v FROM lists WHERE k = {key} ORDER BY pos;"))?
                                .into_iter()
                                .map(|v| v as i64)
                                .collect();
                            let len = list.len() as i64;
                            ops.push(Op::Read(key, list));

                            if rng.below(2) == 0 {
                                let value = ((worker * 1_000_000 + txn) * 10 + op) as i64;
                                db.exec(&format!(
                                    "INSERT INTO lists (id, k, pos, v) VALUES ({}, {key}, {len}, {value});",
                                    key * 1_000_000 + len
                                ))?;
                                ops.push(Op::Append(key, value));
                            }
                        }
                        db.exec("COMMIT;").map(|_| ())
                    };

                    let committed = run(&mut session, &mut ops).is_ok();
                    if !committed {
                        abandon(&mut session);
                    }
                    history.lock().unwrap().push(Recorded { ops, committed });
                }
            });
        }
    });

    let finals = (0..KEYS as i64)
        .map(|key| {
            values(&mut db, &format!("SELECT v FROM lists WHERE k = {key} ORDER BY pos;"))
                .into_iter()
                .map(|v| v as i64)
                .collect()
        })
        .collect();

    (history.into_inner().unwrap(), finals)
}

/// elle's list-append inference: version order from the final lists, `ww` between consecutive
/// appends, `wr` from the writer of a read's last element, `rw` to the writer of the element right
/// after a read's end
///
/// fails with the anomaly when a committed read is not a prefix of its key's order, sees an aborted
/// append (G1a), or sees an append its writer followed with another to the same key (G1b)
fn dependency_graph(
    history: &[Recorded],
    finals: &[Vec<i64>],
) -> Result<Vec<Vec<(usize, Edge)>>, String> {
    let mut writer = std::collections::HashMap::new();
    let mut aborted = std::collections::HashSet::new();
    let mut last_append = std::collections::HashMap::new();

    for (txn, recorded) in history.iter().enumerate() {
        for op in &recorded.ops {
            if let Op::Append(key, value) = op {
                match recorded.committed {
                    true => writer.insert(*value, txn),
                    false => aborted.insert(*value).then_some(txn),
                };
                last_append.insert((txn, *key), *value);
            }
        }
    }

    let mut graph = vec![Vec::new(); history.len()];
    let mut edge = |from: usize, to: usize, kind| {
        if from != to {
            graph[from].push((to, kind));
        }
    };

    for list in finals {
        for pair in list.windows(2) {
            edge(writer[&pair[0]], writer[&pair[1]], Edge::Write);
        }
    }

    for (txn, recorded) in history.iter().enumerate().filter(|(_, r)| r.committed) {
        for op in &recorded.ops {
            let Op::Read(key, list) = op else {
                continue;
            };
            let full = &finals[*key as usize];
            if list.iter().any(|v| aborted.contains(v)) {
                return Err(format!("G1a: read {list:?} holds an aborted append"));
            }
            if !full.starts_with(list) {
                return Err(format!("incompatible order: read {list:?} of final {full:?}"));
            }

            if let Some(last) = list.last() {
                let source = writer[last];
                if source != txn && last_append.get(&(source, *key)) != Some(last) {
                    return Err(format!("G1b: read {list:?} ends on an intermediate append"));
                }
                edge(source, txn, Edge::Read);
            }
            if let Some(next) = full.get(list.len()) {
                edge(txn, writer[next], Edge::Anti);
            }
        }
    }

    Ok(graph)
}

/// whether `to` reaches `from` over edges accepted by `allowed`
fn reaches(
    graph: &[Vec<(usize, Edge)>],
    from: usize,
    to: usize,
    allowed: impl Fn(Edge) -> bool,
) -> bool {
    let mut seen = vec![false; graph.len()];
    let mut stack = vec![from];

    while let Some(node) = stack.pop() {
        if node == to {
            return true;
        }
        if std::mem::replace(&mut seen[node], true) {
            continue;
        }
        stack.extend(graph[node].iter().filter(|(_, e)| allowed(*e)).map(|(n, _)| *n));
    }

    false
}

/// a cycle exists through `edge` (from -> to) when `to` leads back to `from`
fn cycle_through(
    graph: &[Vec<(usize, Edge)>],
    allowed: impl Fn(Edge) -> bool + Copy,
    kind: Edge,
) -> bool {
    graph.iter().enumerate().any(|(from, edges)| {
        edges
            .iter()
            .filter(|(_, e)| *e == kind)
            .any(|&(to, _)| reaches(graph, to, from, allowed))
    })
}

#[test]
fn list_append_histories_contain_no_forbidden_cycles() {
    // `UMBRA_HISTORY_SEED` replays or varies the schedules of a long run
    let base = std::env::var("UMBRA_HISTORY_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    for (level, seed) in [("READ COMMITTED", 3), ("SNAPSHOT", 5), ("SERIALIZABLE", 9)] {
        let seed = seed + base;
        let (history, finals) = list_append_history(level, seed);
        let committed = history.iter().filter(|r| r.committed).count();
        assert!(committed > history.len() / 4, "{level}: too few commits to say anything");
        assert!(committed < history.len(), "{level}: no contention, nothing was tested");

        let graph = dependency_graph(&history, &finals)
            .unwrap_or_else(|anomaly| panic!("{level} seed {seed}: {anomaly}"));
        let no_anti = |e: Edge| e != Edge::Anti;

        assert!(!cycle_through(&graph, no_anti, Edge::Write), "{level} seed {seed}: G0/G1c");
        assert!(!cycle_through(&graph, no_anti, Edge::Read), "{level} seed {seed}: G1c");

        if level != "READ COMMITTED" {
            assert!(!cycle_through(&graph, no_anti, Edge::Anti), "{level} seed {seed}: G-single");
        }
        if level == "SERIALIZABLE" {
            assert!(!cycle_through(&graph, |_| true, Edge::Anti), "{level} seed {seed}: G2");
        }
    }
}

#[test]
fn the_history_checker_detects_known_anomalies() {
    let committed = |ops| Recorded { ops, committed: true };
    let no_anti = |e: Edge| e != Edge::Anti;

    // read skew: T1 appends to both lists, T2 sees its x but not its y
    let skew = [
        committed(vec![Op::Append(0, 1), Op::Append(1, 2)]),
        committed(vec![Op::Read(0, vec![1]), Op::Read(1, vec![])]),
    ];
    let graph = dependency_graph(&skew, &[vec![1], vec![2]]).unwrap();
    assert!(cycle_through(&graph, no_anti, Edge::Anti), "G-single missed");

    // write skew: each reads the list the other appends to, empty
    let write_skew = [
        committed(vec![Op::Read(0, vec![]), Op::Append(1, 2)]),
        committed(vec![Op::Read(1, vec![]), Op::Append(0, 1)]),
    ];
    let graph = dependency_graph(&write_skew, &[vec![1], vec![2]]).unwrap();
    assert!(!cycle_through(&graph, no_anti, Edge::Anti), "G2 is not G-single");
    assert!(cycle_through(&graph, |_| true, Edge::Anti), "G2 missed");

    // serial: T1 appends, T2 reads it, nothing to report
    let serial = [committed(vec![Op::Append(0, 1)]), committed(vec![Op::Read(0, vec![1])])];
    let graph = dependency_graph(&serial, &[vec![1]]).unwrap();
    assert!(!cycle_through(&graph, |_| true, Edge::Anti));
    assert!(!cycle_through(&graph, |_| true, Edge::Read));

    // G0: two transactions append to both lists in opposite orders
    let g0 = [
        committed(vec![Op::Append(0, 1), Op::Append(1, 4)]),
        committed(vec![Op::Append(0, 2), Op::Append(1, 3)]),
    ];
    let graph = dependency_graph(&g0, &[vec![1, 2], vec![3, 4]]).unwrap();
    assert!(cycle_through(&graph, |e| e == Edge::Write, Edge::Write), "G0 missed");

    // G1c: each reads the other's append, a cycle of wr edges
    let g1c = [
        committed(vec![Op::Append(0, 1), Op::Read(1, vec![2])]),
        committed(vec![Op::Append(1, 2), Op::Read(0, vec![1])]),
    ];
    let graph = dependency_graph(&g1c, &[vec![1], vec![2]]).unwrap();
    assert!(cycle_through(&graph, no_anti, Edge::Read), "G1c missed");

    // G1a: a committed read sees an aborted append
    let g1a = [
        Recorded { ops: vec![Op::Append(0, 1)], committed: false },
        committed(vec![Op::Read(0, vec![1])]),
    ];
    assert!(dependency_graph(&g1a, &[vec![]]).is_err_and(|e| e.starts_with("G1a")));

    // G1b: a read ends on an append its writer followed with another to the same list
    let g1b = [
        committed(vec![Op::Append(0, 1), Op::Append(0, 2)]),
        committed(vec![Op::Read(0, vec![1])]),
    ];
    assert!(dependency_graph(&g1b, &[vec![1, 2]]).is_err_and(|e| e.starts_with("G1b")));
}
