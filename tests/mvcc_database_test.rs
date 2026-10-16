//! End-to-end tests for the MVCC-backed database session:
//! SQL strings through the full pipeline against the engine, including durability across reopen

use std::path::PathBuf;
use umbra::db::MvccDatabase;
use umbra::sql::statement::Value;

fn scratch_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock must be past the epoch")
        .as_nanos();
    let dir =
        std::env::temp_dir().join(format!("umbra-mvccdb-{tag}-{}-{nanos}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir must be creatable");

    dir
}

fn memory_db_with_users() -> MvccDatabase {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64));").unwrap();
    db.exec(
        r#"
        INSERT INTO users (id, name)
        VALUES (1, 'alice'), (2, 'bob'), (3, 'carol');"#,
    )
    .unwrap();

    db
}

#[test]
fn create_insert_select() {
    let mut db = memory_db_with_users();

    let result = db.exec("SELECT * FROM users;").unwrap();
    assert_eq!(result.tuples.len(), 3);
    assert_eq!(result.schema.columns.len(), 2);
    assert_eq!(result.tuples[0][0], Value::Number(1));
    assert_eq!(result.tuples[0][1], Value::String("alice".into()));
}

#[test]
fn select_with_where_projection_order_and_limit() {
    let mut db = memory_db_with_users();

    let result = db
        .exec("SELECT name FROM users WHERE id > 1 ORDER BY id DESC LIMIT 1;")
        .unwrap();

    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0].len(), 1);
    assert_eq!(result.tuples[0][0], Value::String("carol".into()));
}

#[test]
fn update_and_delete_with_where() {
    let mut db = memory_db_with_users();

    db.exec("UPDATE users SET name = 'updated' WHERE id = 2;").unwrap();
    db.exec("DELETE FROM users WHERE id = 1;").unwrap();

    let result = db.exec("SELECT id, name FROM users ORDER BY id;").unwrap();
    assert_eq!(result.tuples.len(), 2);
    assert_eq!(result.tuples[0][0], Value::Number(2));
    assert_eq!(result.tuples[0][1], Value::String("updated".into()));
    assert_eq!(result.tuples[1][0], Value::Number(3));
    assert_eq!(result.tuples[1][1], Value::String("carol".into()));
}

#[test]
fn explicit_transaction_commit_and_rollback() {
    let mut db = memory_db_with_users();

    db.exec("BEGIN TRANSACTION;").unwrap();
    assert!(db.active_transaction());
    db.exec("INSERT INTO users (id, name) VALUES (4, 'dave');").unwrap();
    db.exec("COMMIT;").unwrap();
    assert!(!db.active_transaction());

    let result = db.exec("SELECT * FROM users;").unwrap();
    assert_eq!(result.tuples.len(), 4);

    db.exec("BEGIN TRANSACTION;").unwrap();
    db.exec("DELETE FROM users;").unwrap();
    assert_eq!(db.exec("SELECT * FROM users;").unwrap().tuples.len(), 0);
    db.exec("ROLLBACK;").unwrap();

    let result = db.exec("SELECT * FROM users;").unwrap();
    assert_eq!(result.tuples.len(), 4);
}

#[test]
fn analyser_error_does_not_abort_transaction_block() {
    // mirrors the legacy behaviour: only execution errors abort the block,
    // parse/analyse failures leave it usable
    let mut db = memory_db_with_users();

    db.exec("BEGIN TRANSACTION;").unwrap();
    assert!(db.exec("SELECT * FROM missing_table;").is_err());

    assert_eq!(db.exec("SELECT * FROM users;").unwrap().tuples.len(), 3);

    db.exec("ROLLBACK;").unwrap();
    assert!(!db.active_transaction());
}

#[test]
fn unknown_table_and_column_errors() {
    let mut db = memory_db_with_users();

    assert!(db.exec("SELECT * FROM nope;").is_err());
    assert!(db.exec("SELECT nope FROM users;").is_err());
    assert!(db.exec("INSERT INTO users (id, nope) VALUES (9, 'x');").is_err());
}

fn db_with_orders() -> MvccDatabase {
    let mut db = memory_db_with_users();
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);")
        .unwrap();

    for (id, user_id, total) in [(1, 1, 100), (2, 1, 50), (3, 2, 70)] {
        db.exec(&format!(
            "INSERT INTO orders (id, user_id, total) VALUES ({id}, {user_id}, {total});"
        ))
        .unwrap();
    }

    db
}

#[test]
fn inner_join_matches_rows() {
    let mut db = db_with_orders();

    let result = db
        .exec(
            "SELECT users.name, orders.total FROM users \
             JOIN orders ON users.id = orders.user_id \
             ORDER BY orders.total;",
        )
        .unwrap();

    assert_eq!(result.tuples.len(), 3);
    assert_eq!(result.tuples[0], vec![Value::String("alice".into()), Value::Number(50)]);
    assert_eq!(result.tuples[1], vec![Value::String("bob".into()), Value::Number(70)]);
    assert_eq!(result.tuples[2], vec![Value::String("alice".into()), Value::Number(100)]);
}

#[test]
fn left_join_pads_unmatched_rows() {
    let mut db = db_with_orders();

    let result = db
        .exec(
            "SELECT users.name, orders.total FROM users \
             LEFT JOIN orders ON users.id = orders.user_id \
             ORDER BY users.name;",
        )
        .unwrap();

    // carol has no orders and must still appear with NULL total
    assert_eq!(result.tuples.len(), 4);
    let carol = result
        .tuples
        .iter()
        .find(|row| row[0] == Value::String("carol".into()))
        .expect("carol must survive the left join");
    assert_eq!(carol[1], Value::Null);
}

#[test]
fn aggregate_over_join() {
    let mut db = db_with_orders();

    let result = db
        .exec(
            "SELECT users.name, SUM(orders.total) FROM users \
             JOIN orders ON users.id = orders.user_id \
             GROUP BY users.name ORDER BY users.name;",
        )
        .unwrap();

    assert_eq!(result.tuples.len(), 2);
    assert_eq!(result.tuples[0], vec![Value::String("alice".into()), Value::Number(150)]);
    assert_eq!(result.tuples[1], vec![Value::String("bob".into()), Value::Number(70)]);
}

#[test]
fn source_loads_sql_fixture() {
    let dir = scratch_dir("source");
    let fixture = dir.join("seed.sql");
    std::fs::write(
        &fixture,
        "CREATE TABLE seeded (id INT PRIMARY KEY, tag VARCHAR(16));\n\
         INSERT INTO seeded (id, tag) VALUES (1, 'one');\n\
         INSERT INTO seeded (id, tag) VALUES (2, 'two');\n",
    )
    .unwrap();

    let mut db = MvccDatabase::in_memory().unwrap();
    db.load(&fixture).unwrap();

    let result = db.exec("SELECT tag FROM seeded ORDER BY id;").unwrap();
    assert_eq!(result.tuples.len(), 2);
    assert_eq!(result.tuples[0][0], Value::String("one".into()));

    assert!(db.load(dir.join("nope.txt")).is_err());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn serial_primary_key_autogenerates_and_survives_reopen() {
    let dir = scratch_dir("serial");

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("CREATE TABLE logs (id SERIAL PRIMARY KEY, msg VARCHAR(64));").unwrap();

        db.exec("INSERT INTO logs (msg) VALUES ('first');").unwrap();
        db.exec("INSERT INTO logs (msg) VALUES ('second');").unwrap();

        let result = db.exec("SELECT id, msg FROM logs ORDER BY id;").unwrap();
        assert_eq!(result.tuples.len(), 2);
        assert_eq!(result.tuples[0][0], Value::Number(1));
        assert_eq!(result.tuples[1][0], Value::Number(2));

        db.close().unwrap();
    }

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("INSERT INTO logs (msg) VALUES ('third');").unwrap();

        let result = db.exec("SELECT id FROM logs ORDER BY id;").unwrap();
        assert_eq!(result.tuples.len(), 3);
        assert_eq!(
            result.tuples[2][0],
            Value::Number(3),
            "sequence must continue after the committed max across restarts"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn serial_does_not_reuse_a_deleted_value_across_restart() {
    let dir = scratch_dir("serial-delete");

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("CREATE TABLE logs (id SERIAL PRIMARY KEY, msg VARCHAR(64));").unwrap();

        for msg in ["a", "b", "c"] {
            db.exec(&format!("INSERT INTO logs (msg) VALUES ('{msg}');")).unwrap();
        }
        db.exec("DELETE FROM logs WHERE id = 3;").unwrap();
        db.close().unwrap();
    }

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("INSERT INTO logs (msg) VALUES ('d');").unwrap();

        let result = db.exec("SELECT id FROM logs ORDER BY id;").unwrap();
        let ids: Vec<_> = result.tuples.iter().map(|row| row[0].clone()).collect();
        assert_eq!(
            ids,
            vec![Value::Number(1), Value::Number(2), Value::Number(4)],
            "the deleted id 3 must not be re-issued after restart"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn aggregates_without_group_by() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE sales (id INT PRIMARY KEY, amount INT);").unwrap();

    let result = db.exec("SELECT COUNT(*) FROM sales;").unwrap();
    assert_eq!(result.tuples, vec![vec![Value::Number(0)]]);

    for (id, amount) in [(1, 10), (2, 30), (3, 20)] {
        db.exec(&format!("INSERT INTO sales (id, amount) VALUES ({id}, {amount});"))
            .unwrap();
    }

    let result = db
        .exec("SELECT COUNT(*), SUM(amount), MIN(amount), MAX(amount) FROM sales;")
        .unwrap();
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0][0], Value::Number(3));
    assert_eq!(result.tuples[0][1], Value::Number(60));
    assert_eq!(result.tuples[0][2], Value::Number(10));
    assert_eq!(result.tuples[0][3], Value::Number(30));
}

#[test]
fn aggregates_with_group_by() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE sales (id INT PRIMARY KEY, region VARCHAR(16), amount INT);")
        .unwrap();

    for (id, region, amount) in [
        (1, "north", 10),
        (2, "south", 5),
        (3, "north", 20),
        (4, "south", 15),
        (5, "west", 1),
    ] {
        db.exec(&format!(
            "INSERT INTO sales (id, region, amount) VALUES ({id}, '{region}', {amount});"
        ))
        .unwrap();
    }

    let result = db
        .exec("SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region;")
        .unwrap();

    assert_eq!(result.tuples.len(), 3);
    assert_eq!(result.tuples[0], vec![Value::String("north".into()), Value::Number(30)]);
    assert_eq!(result.tuples[1], vec![Value::String("south".into()), Value::Number(20)]);
    assert_eq!(result.tuples[2], vec![Value::String("west".into()), Value::Number(1)]);

    // WHERE before grouping and COUNT per group
    let result = db
        .exec(
            "SELECT region, COUNT(*) FROM sales WHERE amount > 4 GROUP BY region ORDER BY region;",
        )
        .unwrap();
    assert_eq!(result.tuples.len(), 2);
    assert_eq!(result.tuples[0], vec![Value::String("north".into()), Value::Number(2)]);
    assert_eq!(result.tuples[1], vec![Value::String("south".into()), Value::Number(2)]);
}

#[test]
fn point_lookup_by_primary_key() {
    let mut db = memory_db_with_users();

    let result = db.exec("SELECT name FROM users WHERE id = 2;").unwrap();
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0][0], Value::String("bob".into()));

    assert_eq!(db.exec("SELECT * FROM users WHERE id = 99;").unwrap().tuples.len(), 0);
}

#[test]
fn point_lookup_sees_own_uncommitted_writes() {
    let mut db = memory_db_with_users();

    db.exec("BEGIN TRANSACTION;").unwrap();
    db.exec("INSERT INTO users (id, name) VALUES (4, 'dave');").unwrap();
    db.exec("UPDATE users SET name = 'changed' WHERE id = 1;").unwrap();

    // the index bypass must not hide the transaction's local writes
    let result = db.exec("SELECT name FROM users WHERE id = 4;").unwrap();
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0][0], Value::String("dave".into()));

    let result = db.exec("SELECT name FROM users WHERE id = 1;").unwrap();
    assert_eq!(result.tuples[0][0], Value::String("changed".into()));

    db.exec("ROLLBACK;").unwrap();

    let result = db.exec("SELECT name FROM users WHERE id = 1;").unwrap();
    assert_eq!(result.tuples[0][0], Value::String("alice".into()));
}

#[test]
fn duplicate_primary_key_is_rejected() {
    let mut db = memory_db_with_users();

    let result = db.exec("INSERT INTO users (id, name) VALUES (1, 'clone');");
    assert!(result.is_err(), "duplicate primary key must be rejected");

    assert_eq!(db.exec("SELECT * FROM users;").unwrap().tuples.len(), 3);
}

#[test]
fn unique_index_via_sql() {
    let mut db = memory_db_with_users();

    db.exec("CREATE UNIQUE INDEX users_name_idx ON users(name);").unwrap();

    assert!(db.exec("INSERT INTO users (id, name) VALUES (9, 'alice');").is_err());
    assert!(db.exec("INSERT INTO users (id, name) VALUES (9, 'dora');").is_ok());
}

#[test]
fn drop_table_via_sql() {
    let mut db = memory_db_with_users();

    db.exec("DROP TABLE users;").unwrap();
    assert!(db.exec("SELECT * FROM users;").is_err());

    // recreating under the same name starts empty
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64));").unwrap();
    assert_eq!(db.exec("SELECT * FROM users;").unwrap().tuples.len(), 0);

    assert!(db.exec("DROP TABLE missing;").is_err());
}

#[test]
fn dropped_table_stays_dropped_after_reopen() {
    let dir = scratch_dir("drop-reopen");

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("CREATE TABLE gone (id INT PRIMARY KEY);").unwrap();
        db.exec("INSERT INTO gone (id) VALUES (1);").unwrap();
        db.exec("DROP TABLE gone;").unwrap();
        db.close().unwrap();
    }

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        assert!(db.exec("SELECT * FROM gone;").is_err());
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_database_survives_reopen() {
    let dir = scratch_dir("reopen");

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(32));").unwrap();
        db.exec("INSERT INTO items (id, label) VALUES (1, 'kept');").unwrap();

        db.exec("BEGIN TRANSACTION;").unwrap();
        db.exec("INSERT INTO items (id, label) VALUES (2, 'lost');").unwrap();
        // dropped without commit: must not survive
        db.close().unwrap();
    }

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        let result = db.exec("SELECT id, label FROM items;").unwrap();

        assert_eq!(result.tuples.len(), 1);
        assert_eq!(result.tuples[0][0], Value::Number(1));
        assert_eq!(result.tuples[0][1], Value::String("kept".into()));

        db.exec("INSERT INTO items (id, label) VALUES (3, 'after');").unwrap();
        assert_eq!(db.exec("SELECT * FROM items;").unwrap().tuples.len(), 2);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn enum_column_end_to_end() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec(
        r#"CREATE TABLE tasks (
            id INT PRIMARY KEY,
            name VARCHAR(64),
            urgency 'low' | 'medium' | 'high'
        );"#,
    )
    .unwrap();

    db.exec("INSERT INTO tasks (id, name, urgency) VALUES (1, 'a', 'high');")
        .unwrap();
    db.exec("INSERT INTO tasks (id, name, urgency) VALUES (2, 'b', 'low');")
        .unwrap();
    db.exec("INSERT INTO tasks (id, name, urgency) VALUES (3, 'c', 'medium');")
        .unwrap();

    // variants display as their string form
    let result = db.exec("SELECT urgency FROM tasks WHERE id = 1;").unwrap();
    assert_eq!(result.tuples[0][0], Value::String("high".into()));

    // ordering follows variant declaration order, not lexicographic
    let result = db.exec("SELECT name FROM tasks ORDER BY urgency;").unwrap();
    assert_eq!(
        result.tuples,
        vec![
            vec![Value::String("b".into())],
            vec![Value::String("c".into())],
            vec![Value::String("a".into())],
        ]
    );

    // comparisons against variant literals
    let result = db
        .exec("SELECT name FROM tasks WHERE urgency > 'low' ORDER BY urgency;")
        .unwrap();
    assert_eq!(
        result.tuples,
        vec![vec![Value::String("c".into())], vec![Value::String("a".into())],]
    );

    // unknown variants are rejected on write and in predicates
    assert!(
        db.exec("INSERT INTO tasks (id, name, urgency) VALUES (4, 'd', 'urgent');")
            .is_err()
    );
    assert!(db.exec("SELECT * FROM tasks WHERE urgency = 'HIGH';").is_err());

    // updates store the new variant index
    db.exec("UPDATE tasks SET urgency = 'high' WHERE id = 2;").unwrap();
    let result = db.exec("SELECT urgency FROM tasks WHERE id = 2;").unwrap();
    assert_eq!(result.tuples[0][0], Value::String("high".into()));
}

#[test]
fn enum_group_by_displays_variants() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE tickets (id INT PRIMARY KEY, state 'open' | 'closed');")
        .unwrap();
    db.exec("INSERT INTO tickets (id, state) VALUES (1, 'open');").unwrap();
    db.exec("INSERT INTO tickets (id, state) VALUES (2, 'closed');").unwrap();
    db.exec("INSERT INTO tickets (id, state) VALUES (3, 'open');").unwrap();

    let result = db
        .exec("SELECT state, COUNT(*) FROM tickets GROUP BY state ORDER BY state;")
        .unwrap();

    assert_eq!(
        result.tuples,
        vec![
            vec![Value::String("open".into()), Value::Number(2)],
            vec![Value::String("closed".into()), Value::Number(1)],
        ]
    );
}

#[test]
fn enum_survives_reopen() {
    let dir = scratch_dir("enum");

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        db.exec("CREATE TABLE tickets (id INT PRIMARY KEY, state 'open' | 'closed');")
            .unwrap();
        db.exec("INSERT INTO tickets (id, state) VALUES (1, 'closed');").unwrap();
        db.close().unwrap();
    }

    {
        let mut db = MvccDatabase::init(&dir).unwrap();

        let result = db.exec("SELECT state FROM tickets;").unwrap();
        assert_eq!(result.tuples[0][0], Value::String("closed".into()));

        // variant table must survive the schema round-trip for new writes
        db.exec("INSERT INTO tickets (id, state) VALUES (2, 'open');").unwrap();
        assert!(db.exec("INSERT INTO tickets (id, state) VALUES (3, 'other');").is_err());

        let result = db.exec("SELECT id FROM tickets WHERE state = 'open';").unwrap();
        assert_eq!(result.tuples, vec![vec![Value::Number(2)]]);
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn range_scan_on_indexed_column() {
    let mut db = memory_db_with_users();

    let result = db.exec("SELECT name FROM users WHERE id > 1 AND id <= 3 ORDER BY id;").unwrap();
    assert_eq!(
        result.tuples,
        vec![vec![Value::String("bob".into())], vec![Value::String("carol".into())],]
    );

    let result = db.exec("SELECT name FROM users WHERE id >= 3;").unwrap();
    assert_eq!(result.tuples, vec![vec![Value::String("carol".into())]]);

    let result = db.exec("SELECT name FROM users WHERE 2 > id;").unwrap();
    assert_eq!(result.tuples, vec![vec![Value::String("alice".into())]]);
}

#[test]
fn range_scan_sees_own_uncommitted_writes() {
    let mut db = memory_db_with_users();

    db.exec("BEGIN TRANSACTION;").unwrap();
    db.exec("INSERT INTO users (id, name) VALUES (4, 'dan');").unwrap();

    let result = db.exec("SELECT name FROM users WHERE id > 2 ORDER BY id;").unwrap();
    assert_eq!(
        result.tuples,
        vec![vec![Value::String("carol".into())], vec![Value::String("dan".into())],]
    );

    db.exec("ROLLBACK;").unwrap();
}

#[test]
fn update_and_delete_through_index_bounds() {
    let mut db = memory_db_with_users();

    db.exec("UPDATE users SET name = 'upd' WHERE id >= 2 AND id < 3;").unwrap();
    let result = db.exec("SELECT name FROM users WHERE id = 2;").unwrap();
    assert_eq!(result.tuples, vec![vec![Value::String("upd".into())]]);

    db.exec("DELETE FROM users WHERE id > 1;").unwrap();
    let result = db.exec("SELECT id FROM users;").unwrap();
    assert_eq!(result.tuples, vec![vec![Value::Number(1)]]);
}

#[test]
fn explain_reports_access_paths() {
    let mut db = memory_db_with_users();

    let plan = db.exec("EXPLAIN SELECT name FROM users WHERE id = 2;").unwrap();
    assert!(matches!(&plan.tuples[0][0], Value::String(s) if s.starts_with("IndexScan on users")));

    let plan = db
        .exec("EXPLAIN SELECT name FROM users WHERE id > 1 ORDER BY name LIMIT 2;")
        .unwrap();
    assert!(
        matches!(&plan.tuples[0][0], Value::String(s) if s.starts_with("IndexRangeScan on users"))
    );
    let lines: Vec<String> = plan
        .tuples
        .iter()
        .map(|t| match &t[0] {
            Value::String(s) => s.to_string(),
            other => panic!("plan lines must be strings, got {other:?}"),
        })
        .collect();
    assert!(lines.iter().any(|l| l.starts_with("Filter")));
    assert!(lines.iter().any(|l| l.starts_with("Sort")));
    assert!(lines.iter().any(|l| l.starts_with("Limit")));

    let plan = db.exec("EXPLAIN SELECT id FROM users WHERE name = 'bob';").unwrap();
    assert!(matches!(&plan.tuples[0][0], Value::String(s) if s.starts_with("SeqScan on users")));

    assert!(db.exec("EXPLAIN CREATE TABLE t (id INT PRIMARY KEY);").is_err());
}

#[test]
fn hash_join_and_nested_loop_fallback() {
    let mut db = memory_db_with_users();
    db.exec("CREATE TABLE pets (id INT PRIMARY KEY, owner_id INT, name VARCHAR(64));")
        .unwrap();
    db.exec(
        r#"
        INSERT INTO pets (id, owner_id, name)
        VALUES (1, 1, 'rex'), (2, 1, 'milo'), (3, 3, 'luna'), (4, 9, 'ghost');"#,
    )
    .unwrap();

    let set = db
        .exec(
            r#"
            SELECT u.name, p.name FROM users AS u
            JOIN pets AS p ON u.id = p.owner_id
            ORDER BY p.id;"#,
        )
        .unwrap();
    assert_eq!(
        set.tuples,
        vec![
            vec!["alice".into(), "rex".into()],
            vec!["alice".into(), "milo".into()],
            vec!["carol".into(), "luna".into()],
        ]
    );

    let set = db
        .exec(
            r#"
            SELECT u.name, p.name FROM users AS u
            LEFT JOIN pets AS p ON u.id = p.owner_id
            ORDER BY u.id, p.id;"#,
        )
        .unwrap();
    assert_eq!(set.tuples.len(), 4);
    assert_eq!(set.tuples[2], vec!["bob".into(), Value::Null]);

    // non-equi ON falls back to the nested loop
    let set = db
        .exec(
            r#"
            SELECT u.name, p.name FROM users AS u
            JOIN pets AS p ON u.id < p.owner_id
            ORDER BY u.id, p.id;"#,
        )
        .unwrap();
    assert_eq!(
        set.tuples,
        vec![
            vec!["alice".into(), "luna".into()],
            vec!["alice".into(), "ghost".into()],
            vec!["bob".into(), "luna".into()],
            vec!["bob".into(), "ghost".into()],
            vec!["carol".into(), "ghost".into()],
        ]
    );

    let plan = db
        .exec("EXPLAIN SELECT u.name FROM users AS u JOIN pets AS p ON u.id = p.owner_id;")
        .unwrap();
    let lines: Vec<String> = plan
        .tuples
        .iter()
        .map(|t| match &t[0] {
            Value::String(s) => s.to_string(),
            other => panic!("plan lines must be strings, got {other:?}"),
        })
        .collect();
    assert!(lines.iter().any(|l| l.starts_with("HashJoin")));

    db.exec("CREATE INDEX idx_pets_owner ON pets(owner_id);").unwrap();
    let plan = db
        .exec("EXPLAIN SELECT u.name FROM users AS u JOIN pets AS p ON u.id = p.owner_id;")
        .unwrap();
    assert!(
        plan.tuples
            .iter()
            .any(|t| matches!(&t[0], Value::String(s) if s.starts_with("IndexJoin")))
    );

    let plan = db
        .exec("EXPLAIN SELECT u.name FROM users AS u JOIN pets AS p ON u.id < p.owner_id;")
        .unwrap();
    assert!(
        plan.tuples
            .iter()
            .any(|t| matches!(&t[0], Value::String(s) if s.starts_with("NestedLoopJoin")))
    );
}

#[test]
fn non_unique_index_serves_exact_and_range_lookups() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE people (id INT PRIMARY KEY, age INT);").unwrap();
    db.exec(
        r#"
        INSERT INTO people (id, age)
        VALUES (1, 30), (2, 40), (3, 30), (4, 50), (5, 40);"#,
    )
    .unwrap();

    db.exec("CREATE INDEX people_age_idx ON people(age);").unwrap();
    db.exec("INSERT INTO people (id, age) VALUES (6, 30);").unwrap();

    let plan = db.exec("EXPLAIN SELECT id FROM people WHERE age = 30;").unwrap();
    assert!(matches!(&plan.tuples[0][0], Value::String(s) if s.starts_with("IndexScan on people")));

    let exact = db.exec("SELECT id FROM people WHERE age = 30 ORDER BY id;").unwrap();
    assert_eq!(exact.tuples, vec![vec![1.into()], vec![3.into()], vec![6.into()],]);

    let range = db.exec("SELECT id FROM people WHERE age >= 40 ORDER BY id;").unwrap();
    assert_eq!(range.tuples, vec![vec![2.into()], vec![4.into()], vec![5.into()],]);
}

#[test]
fn in_and_or_are_served_by_index_point_probes() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE people (id INT PRIMARY KEY, age INT);").unwrap();
    db.exec(
        r#"
        INSERT INTO people (id, age)
        VALUES (1, 30), (2, 40), (3, 50), (4, 40), (5, 60);"#,
    )
    .unwrap();
    db.exec("CREATE INDEX people_age_idx ON people(age);").unwrap();

    let plan = db.exec("EXPLAIN SELECT id FROM people WHERE age IN (30, 50);").unwrap();
    assert!(matches!(&plan.tuples[0][0], Value::String(s) if s.starts_with("IndexScan on people")));

    let in_list = db.exec("SELECT id FROM people WHERE age IN (30, 50) ORDER BY id;").unwrap();
    assert_eq!(in_list.tuples, vec![vec![1.into()], vec![3.into()]]);

    let disjunction = db
        .exec(
            r#"
            SELECT id FROM people
            WHERE age = 40 OR age = 60
            ORDER BY id;
        "#,
        )
        .unwrap();
    assert_eq!(disjunction.tuples, vec![vec![2.into()], vec![4.into()], vec![5.into()],]);

    let repeated = db
        .exec(
            r#"
            SELECT id FROM people
            WHERE age = 40 OR age = 40
            ORDER BY id;
        "#,
        )
        .unwrap();
    assert_eq!(repeated.tuples, vec![vec![Value::Number(2)], vec![Value::Number(4)]]);
}

#[test]
fn inner_join_pushes_single_table_filter_into_the_scan() {
    let mut db = db_with_orders();
    db.exec("CREATE INDEX orders_total_idx ON orders(total);").unwrap();

    // alias-qualified filter on the joined table is pushed into its scan
    let pushed = db
        .exec(
            "SELECT u.name, o.total FROM users AS u \
             JOIN orders AS o ON u.id = o.user_id \
             WHERE o.total = 100;",
        )
        .unwrap();
    assert_eq!(pushed.tuples, vec![vec![Value::String("alice".into()), Value::Number(100)]]);

    // a two-table predicate cannot be pushed, but still filters correctly
    let residual = db
        .exec(
            "SELECT u.name, o.total FROM users AS u \
             JOIN orders AS o ON u.id = o.user_id \
             WHERE o.total > u.id + 60 ORDER BY o.total;",
        )
        .unwrap();
    assert_eq!(
        residual.tuples,
        vec![
            vec![Value::String("bob".into()), Value::Number(70)],
            vec![Value::String("alice".into()), Value::Number(100)],
        ]
    );

    // a LEFT join never pushes (not all-inner), WHERE still drops null-padded rows
    let outer = db
        .exec(
            "SELECT u.name, o.total FROM users AS u \
             LEFT JOIN orders AS o ON u.id = o.user_id \
             WHERE o.total = 100;",
        )
        .unwrap();
    assert_eq!(outer.tuples, vec![vec![Value::String("alice".into()), Value::Number(100)]]);
}

#[test]
fn full_order_by_spills_to_disk_and_stays_sorted() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE wide (id INT PRIMARY KEY, sort_key INT, payload VARCHAR(4096));")
        .unwrap();

    // > 8 MiB of tuples in scrambled key order, so the sort must spill runs to
    // disk and k-way merge them rather than sort in memory
    let payload = "x".repeat(2000);
    let total = 6000i64;
    let mut expected = Vec::with_capacity(total as usize);
    let mut id = 0;

    while id < total {
        let mut statement = String::from("INSERT INTO wide (id, sort_key, payload) VALUES ");
        for row in 0..500 {
            let key = id.wrapping_mul(2_654_435_761) % total;
            expected.push(key);
            if row > 0 {
                statement.push(',');
            }
            statement.push_str(&format!("({id}, {key}, '{payload}')"));
            id += 1;
        }

        statement.push(';');
        db.exec(&statement).unwrap();
    }
    expected.sort();

    let result = db.exec("SELECT sort_key FROM wide ORDER BY sort_key;").unwrap();
    assert_eq!(result.tuples.len(), total as usize);

    let sorted: Vec<_> = result
        .tuples
        .iter()
        .map(|tuple| match &tuple[0] {
            Value::Number(n) => *n as i64,
            other => panic!("expected a number, got {other:?}"),
        })
        .collect();
    assert_eq!(sorted, expected);
}

#[test]
fn order_by_indexed_column_is_served_by_the_index() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, v INT);").unwrap();
    db.exec(
        "INSERT INTO t (id, v) VALUES \
         (5,50),(2,20),(8,80),(1,10),(9,90),(3,30),(7,70),(4,40),(6,60);",
    )
    .unwrap();

    let plan = db.exec("EXPLAIN SELECT id FROM t ORDER BY id;").unwrap();
    let lines: Vec<String> = plan
        .tuples
        .iter()
        .map(|t| match &t[0] {
            Value::String(s) => s.to_string(),
            other => panic!("plan lines are strings, got {other:?}"),
        })
        .collect();
    assert!(lines.iter().any(|l| l.starts_with("Index order")), "{lines:?}");
    assert!(!lines.iter().any(|l| l.starts_with("Sort")), "{lines:?}");

    let asc = db.exec("SELECT id FROM t ORDER BY id;").unwrap();
    assert_eq!(
        asc.tuples.iter().map(|t| t[0].clone()).collect::<Vec<_>>(),
        (1..=9i128).map(Value::Number).collect::<Vec<_>>(),
    );

    let desc = db.exec("SELECT id FROM t ORDER BY id DESC;").unwrap();
    assert_eq!(
        desc.tuples.iter().map(|t| t[0].clone()).collect::<Vec<_>>(),
        (1..=9i128).rev().map(Value::Number).collect::<Vec<_>>(),
    );

    let page = db.exec("SELECT id FROM t ORDER BY id LIMIT 3 OFFSET 2;").unwrap();
    assert_eq!(
        page.tuples.iter().map(|t| t[0].clone()).collect::<Vec<_>>(),
        vec![Value::Number(3), Value::Number(4), Value::Number(5)],
    );
}

#[test]
fn order_by_secondary_index_respects_mvcc_visibility() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, score INT);").unwrap();
    db.exec("CREATE INDEX idx_score ON t(score);").unwrap();
    db.exec("INSERT INTO t (id, score) VALUES (1,30),(2,10),(3,20),(4,50),(5,40);")
        .unwrap();
    db.exec("DELETE FROM t WHERE id = 4;").unwrap();

    let plan = db.exec("EXPLAIN SELECT score FROM t ORDER BY score;").unwrap();
    let lines: Vec<String> = plan
        .tuples
        .iter()
        .map(|t| match &t[0] {
            Value::String(s) => s.to_string(),
            other => panic!("plan lines are strings, got {other:?}"),
        })
        .collect();
    assert!(lines.iter().any(|l| l.starts_with("Index order")), "{lines:?}");

    // the deleted score=50 row is filtered by visibility, order stays correct
    let ordered = db.exec("SELECT score FROM t ORDER BY score;").unwrap();
    assert_eq!(
        ordered.tuples.iter().map(|t| t[0].clone()).collect::<Vec<_>>(),
        vec![Value::Number(10), Value::Number(20), Value::Number(30), Value::Number(40),],
    );
}

#[test]
fn projection_pushdown_with_where_and_order_on_unselected_columns() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT, label VARCHAR(16));")
        .unwrap();
    db.exec(
        "INSERT INTO t (id, a, b, c, label) VALUES \
         (1, 10, 100, 1000, 'x'), (2, 20, 200, 2000, 'y'), (3, 30, 300, 3000, 'z');",
    )
    .unwrap();

    let rows = db.exec("SELECT b, label FROM t WHERE c > 1000 ORDER BY a DESC;").unwrap();
    assert_eq!(
        rows.tuples,
        vec![
            vec![Value::Number(300), Value::String("z".into())],
            vec![Value::Number(200), Value::String("y".into())],
        ],
    );
}

#[test]
fn group_by_over_more_than_the_sort_budget_spills_and_aggregates_exactly() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE wide (id INT PRIMARY KEY, grp INT, payload VARCHAR(4096));")
        .unwrap();

    // > 8 MiB of rows in scrambled group order, so grouping must spill sorted runs
    let payload = "x".repeat(2000);
    let (total, groups) = (6000i64, 37i64);
    let mut id = 0;
    while id < total {
        let rows: Vec<_> = (0..500)
            .map(|offset| {
                let row = id + offset;
                format!("({row}, {}, '{payload}')", row.wrapping_mul(2_654_435_761) % groups)
            })
            .collect();
        db.exec(&format!("INSERT INTO wide (id, grp, payload) VALUES {};", rows.join(",")))
            .unwrap();
        id += 500;
    }

    let result = db
        .exec("SELECT grp, COUNT(*), SUM(id), MIN(id), MAX(id) FROM wide GROUP BY grp;")
        .unwrap();
    assert_eq!(result.tuples.len(), groups as usize);

    for (position, row) in result.tuples.iter().enumerate() {
        let members: Vec<i64> = (0..total)
            .filter(|row| row.wrapping_mul(2_654_435_761) % groups == position as i64)
            .collect();

        assert_eq!(row[0], Value::Number(position as i128), "groups come out in key order");
        assert_eq!(row[1], Value::Number(members.len() as i128));
        assert_eq!(row[2], Value::Number(members.iter().map(|&m| m as i128).sum()));
        assert_eq!(row[3], Value::Number(members[0] as i128));
        assert_eq!(row[4], Value::Number(*members.last().unwrap() as i128));
    }

    let totals = db.exec("SELECT COUNT(*), AVG(grp) FROM wide WHERE id < 0;").unwrap();
    assert_eq!(totals.tuples, vec![vec![Value::Number(0), Value::Null]]);
}

#[test]
fn joins_whose_build_side_exceeds_the_budget_spill_and_match_exactly() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE wide (id INT PRIMARY KEY, payload VARCHAR(4096));")
        .unwrap();
    db.exec("CREATE TABLE refs (id INT PRIMARY KEY, wide_id INT);").unwrap();

    // > 8 MiB on the build side forces partitioning: wide ids 0..6000, refs point at 3000..9000
    let payload = "y".repeat(2000);
    for start in (0..6000).step_by(500) {
        let rows: Vec<_> = (start..start + 500).map(|id| format!("({id}, '{payload}')")).collect();
        db.exec(&format!("INSERT INTO wide (id, payload) VALUES {};", rows.join(",")))
            .unwrap();

        let refs: Vec<_> =
            (start..start + 500).map(|id| format!("({id}, {})", id + 3000)).collect();
        db.exec(&format!("INSERT INTO refs (id, wide_id) VALUES {};", refs.join(",")))
            .unwrap();
    }

    let count = |db: &mut MvccDatabase, kind: &str| {
        let result = db
            .exec(&format!(
                "SELECT COUNT(*), SUM(wide.id), SUM(refs.id) FROM refs {kind} JOIN wide ON refs.wide_id = wide.id;"
            ))
            .unwrap();
        result.tuples[0].clone()
    };

    let matched_wide: i128 = (3000..6000).sum();
    let matched_refs: i128 = (0..3000).sum();
    let all_wide: i128 = (0..6000).sum();
    let all_refs: i128 = (0..6000).sum();

    assert_eq!(
        count(&mut db, "INNER"),
        vec![Value::Number(3000), Value::Number(matched_wide), Value::Number(matched_refs)]
    );
    assert_eq!(
        count(&mut db, "LEFT"),
        vec![Value::Number(6000), Value::Number(matched_wide), Value::Number(all_refs)]
    );
    assert_eq!(
        count(&mut db, "RIGHT"),
        vec![Value::Number(6000), Value::Number(all_wide), Value::Number(matched_refs)]
    );
    assert_eq!(
        count(&mut db, "FULL"),
        vec![Value::Number(9000), Value::Number(all_wide), Value::Number(all_refs)]
    );
}

#[test]
fn numeric_columns_compare_with_integer_and_float_literals() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE records (id INTEGER PRIMARY KEY, amount NUMERIC(12,2), weight REAL);")
        .unwrap();
    db.exec("CREATE INDEX records_weight ON records(weight);").unwrap();
    db.exec("INSERT INTO records (id, amount, weight) VALUES (1, 60000.50, 2), (2, 100.25, 2.5), (3, 50000, 1);")
        .unwrap();

    let ids = |db: &mut MvccDatabase, sql: &str| -> Vec<Value> {
        db.exec(sql).unwrap().tuples.into_iter().map(|row| row[0].clone()).collect()
    };

    assert_eq!(
        ids(&mut db, "SELECT id FROM records WHERE amount > 50000 LIMIT 100;"),
        vec![Value::Number(1)]
    );
    assert_eq!(
        ids(&mut db, "SELECT id FROM records WHERE amount >= 100.25 ORDER BY amount;"),
        vec![Value::Number(2), Value::Number(3), Value::Number(1)]
    );
    // integer literals stored in a REAL column sit in the same index as floats
    assert_eq!(
        ids(&mut db, "SELECT id FROM records WHERE weight > 1.5 ORDER BY weight;"),
        vec![Value::Number(1), Value::Number(2)]
    );
}

#[test]
fn grouping_by_the_primary_key_streams_and_matches_sorted_grouping() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(16));").unwrap();
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT);")
        .unwrap();
    db.exec("CREATE TABLE items (id INT PRIMARY KEY, order_id INT);").unwrap();

    let users: Vec<_> = (0..300).map(|id| format!("({id}, 'u{}')", id % 7)).collect();
    db.exec(&format!("INSERT INTO users (id, name) VALUES {};", users.join(",")))
        .unwrap();
    let orders: Vec<_> =
        (0..900).map(|id| format!("({id}, {}, {})", (id * 7) % 250, id % 13)).collect();
    db.exec(&format!("INSERT INTO orders (id, user_id, total) VALUES {};", orders.join(",")))
        .unwrap();
    let items: Vec<_> = (0..1800).map(|id| format!("({id}, {})", id % 900)).collect();
    db.exec(&format!("INSERT INTO items (id, order_id) VALUES {};", items.join(",")))
        .unwrap();

    let grouped = |db: &mut MvccDatabase, key: &str, joins: &str| {
        db.exec(&format!(
            "SELECT {key}, users.name, COUNT(orders.id), SUM(orders.total), MAX(orders.total) + 1 \
             FROM users {joins} GROUP BY {key}, users.name ORDER BY {key};"
        ))
        .unwrap()
        .tuples
    };

    let left = "LEFT JOIN orders ON users.id = orders.user_id";
    let inner = "JOIN orders ON users.id = orders.user_id";
    let both = "LEFT JOIN orders ON users.id = orders.user_id \
                LEFT JOIN items ON orders.id = items.order_id";

    for joins in [left, inner, both] {
        let streamed = grouped(&mut db, "users.id", joins);
        assert_eq!(streamed, grouped(&mut db, "users.id + 0", joins), "{joins}");
    }

    db.exec("CREATE INDEX idx_orders_user_id ON orders(user_id);").unwrap();
    for joins in [left, inner, both] {
        let streamed = grouped(&mut db, "users.id", joins);
        assert_eq!(streamed, grouped(&mut db, "users.id + 0", joins), "{joins}");
    }

    let padded = grouped(&mut db, "users.id", left);
    assert_eq!(padded.len(), 300);
    assert_eq!(padded[299][2], Value::Number(0), "users past 249 have no orders");

    let single = |db: &mut MvccDatabase, key: &str| {
        db.exec(&format!(
            "SELECT {key}, COUNT(*), MIN(name) FROM users WHERE id > 10 GROUP BY {key} ORDER BY {key};"
        ))
        .unwrap()
        .tuples
    };
    assert_eq!(single(&mut db, "id"), single(&mut db, "id + 0"));
    let first = db
        .exec("SELECT id, COUNT(*) FROM users GROUP BY id LIMIT 3 OFFSET 2;")
        .unwrap()
        .tuples;
    assert_eq!(first.len(), 3);
    assert!(first.iter().all(|row| row[1] == Value::Number(1)));
    let count = db.exec("SELECT COUNT(*) FROM users;").unwrap().tuples;
    assert_eq!(count, vec![vec![Value::Number(300)]]);
    assert_eq!(db.exec("SELECT 7 FROM users WHERE id < 4;").unwrap().tuples.len(), 4);

    let limited = db
        .exec(
            "SELECT users.id, COUNT(orders.id) FROM users \
             LEFT JOIN orders ON users.id = orders.user_id GROUP BY users.id LIMIT 5 OFFSET 3;",
        )
        .unwrap()
        .tuples;
    assert_eq!(limited.len(), 5);
    for row in &limited {
        assert!(padded.iter().any(|full| full[0] == row[0] && full[2] == row[1]));
    }
}

#[test]
fn hash_grouping_past_its_budget_sorts_the_new_groups_and_stays_exact() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE wide (id INT PRIMARY KEY, grp INT, payload VARCHAR(4096));")
        .unwrap();

    let payload = "x".repeat(2000);
    let (total, groups) = (12_000i64, 6_000i64);
    for start in (0..total).step_by(500) {
        let rows: Vec<_> = (start..start + 500)
            .map(|id| format!("({id}, {}, '{payload}')", id.wrapping_mul(2_654_435_761) % groups))
            .collect();
        db.exec(&format!("INSERT INTO wide (id, grp, payload) VALUES {};", rows.join(",")))
            .unwrap();
    }

    let result = db
        .exec("SELECT grp, COUNT(*), SUM(id), MIN(id) FROM wide GROUP BY grp ORDER BY grp;")
        .unwrap();
    assert_eq!(result.tuples.len(), groups as usize);

    let mut expected = vec![(0i128, 0i128, i128::MAX); groups as usize];
    for id in 0..total {
        let entry = &mut expected[(id.wrapping_mul(2_654_435_761) % groups) as usize];
        entry.0 += 1;
        entry.1 += id as i128;
        entry.2 = entry.2.min(id as i128);
    }
    for (grp, (row, (count, sum, min))) in result.tuples.iter().zip(expected).enumerate() {
        assert_eq!(
            row,
            &vec![
                Value::Number(grp as i128),
                Value::Number(count),
                Value::Number(sum),
                Value::Number(min)
            ]
        );
    }
}

#[test]
fn hash_grouping_merges_representations_of_equal_values() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE readings (id INT PRIMARY KEY, level REAL NULLABLE, label VARCHAR(8) NULLABLE);")
        .unwrap();
    db.exec(
        "INSERT INTO readings (id, level, label) VALUES \
         (1, 2, 'a'), (2, 2.0, 'b'), (3, 2.5, NULL), (4, NULL, NULL), (5, NULL, 'a');",
    )
    .unwrap();

    let levels = db.exec("SELECT level, COUNT(*) FROM readings GROUP BY level;").unwrap().tuples;
    assert_eq!(levels.len(), 3, "{levels:?}");
    assert_eq!(levels[0][1], Value::Number(2));
    assert_eq!(levels[2], vec![Value::Null, Value::Number(2)]);

    let labels = db.exec("SELECT label, COUNT(*) FROM readings GROUP BY label;").unwrap().tuples;
    assert_eq!(
        labels,
        vec![
            vec![Value::String("a".into()), Value::Number(2)],
            vec![Value::String("b".into()), Value::Number(1)],
            vec![Value::Null, Value::Number(2)],
        ]
    );
}

#[test]
fn string_literals_compare_with_uuid_and_temporal_columns() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec(
        "CREATE TABLE events (id INT PRIMARY KEY, ext UUID, day DATE, at TIME, stamp TIMESTAMP);",
    )
    .unwrap();
    db.exec(
        "INSERT INTO events (id, ext, day, at, stamp) VALUES \
         (1, '0b7e5c8e-3f1a-4c6e-9d2b-1a2b3c4d5e6f', '2024-01-10', '08:30:00', '2024-01-10 08:30:00'), \
         (2, '6f5e4d3c-2b1a-4d9e-8c7f-0e1d2c3b4a59', '2024-02-20', '17:45:00', '2024-02-20 17:45:00'), \
         (3, 'a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d', '2024-03-30', '23:59:59', '2024-03-30 23:59:59');",
    )
    .unwrap();

    let ids = |db: &mut MvccDatabase, predicate: &str| -> Vec<Value> {
        db.exec(&format!("SELECT id FROM events WHERE {predicate} ORDER BY id;"))
            .unwrap()
            .tuples
            .into_iter()
            .map(|mut row| row.remove(0))
            .collect()
    };
    let numbers = |ids: &[i128]| ids.iter().map(|&id| Value::Number(id)).collect::<Vec<_>>();

    for (predicate, expected) in [
        ("ext = '6f5e4d3c-2b1a-4d9e-8c7f-0e1d2c3b4a59'", &[2][..]),
        ("'6f5e4d3c-2b1a-4d9e-8c7f-0e1d2c3b4a59' = ext", &[2]),
        ("ext != '6f5e4d3c-2b1a-4d9e-8c7f-0e1d2c3b4a59'", &[1, 3]),
        ("day = '2024-02-20'", &[2]),
        ("day > '2024-01-31' AND day < '2024-03-01'", &[2]),
        ("day = '2024-02-20 10:00:00'", &[2]),
        ("at >= '17:45:00'", &[2, 3]),
        ("at < '2024-05-05 09:00:00'", &[1]),
        ("stamp > '2024-02-20 17:44:59'", &[2, 3]),
        ("stamp = '2024-03-30'", &[3]),
        ("(day = '2024-01-10' OR id = 3)", &[1, 3]),
    ] {
        assert_eq!(ids(&mut db, predicate), numbers(expected), "{predicate}");
    }
}

#[test]
fn incremental_sort_over_an_index_matches_a_full_sort() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec(
        "CREATE TABLE people (id INT PRIMARY KEY, age INT NULLABLE, balance INT, name VARCHAR(16));",
    )
    .unwrap();
    let rows: Vec<_> = (0..2_000)
        .map(|id| {
            let age = match id % 37 {
                0 => "NULL".to_string(),
                _ => (id * 7 % 23).to_string(),
            };
            format!("({id}, {age}, {}, 'n{}')", id * 13 % 101, id % 5)
        })
        .collect();
    db.exec(&format!(
        "INSERT INTO people (id, age, balance, name) VALUES {};",
        rows.join(",")
    ))
    .unwrap();

    let queries = [
        "SELECT * FROM people ORDER BY age DESC, balance, name, id LIMIT 100;",
        "SELECT * FROM people ORDER BY age, balance DESC, id LIMIT 50 OFFSET 120;",
        "SELECT id, name FROM people ORDER BY age, balance, id;",
        "SELECT name, id FROM people ORDER BY age DESC, name DESC, id LIMIT 7;",
    ];
    let sorted: Vec<_> = queries.iter().map(|sql| db.exec(sql).unwrap().tuples).collect();

    db.exec("CREATE INDEX idx_people_age ON people(age);").unwrap();
    let plan = db
        .exec("EXPLAIN SELECT * FROM people ORDER BY age DESC, balance LIMIT 3;")
        .unwrap()
        .tuples;
    assert!(
        plan.iter().any(|line| format!("{line:?}").contains("Incremental sort")),
        "{plan:?}"
    );

    for (sql, expected) in queries.iter().zip(sorted) {
        assert_eq!(db.exec(sql).unwrap().tuples, expected, "{sql}");
    }
}

#[test]
fn index_lookups_stream_and_match_a_scan() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE people (id INT PRIMARY KEY, age INT, name VARCHAR(16));")
        .unwrap();
    let rows: Vec<_> = (0..600).map(|id| format!("({id}, {}, 'p{id}')", id % 40)).collect();
    db.exec(&format!("INSERT INTO people (id, age, name) VALUES {};", rows.join(",")))
        .unwrap();
    db.exec("CREATE INDEX idx_people_age ON people(age);").unwrap();

    let ids = |db: &mut MvccDatabase, predicate: &str| {
        let mut ids: Vec<_> = db
            .exec(&format!("SELECT id FROM people WHERE {predicate};"))
            .unwrap()
            .tuples
            .into_iter()
            .map(|row| row[0].clone())
            .collect();
        ids.sort();
        ids
    };
    let same = |db: &mut MvccDatabase, indexed: &str, scanned: &str| {
        let expected = ids(db, scanned);
        assert!(!expected.is_empty(), "{scanned} selects something");
        assert_eq!(ids(db, indexed), expected, "{indexed}");
    };

    same(&mut db, "age IN (3, 3, 5, 39)", "age + 0 = 3 OR age + 0 = 5 OR age + 0 = 39");
    same(&mut db, "age = 7 OR age = 2", "age + 0 = 7 OR age + 0 = 2");
    same(&mut db, "age >= 10 AND age < 13", "age + 0 >= 10 AND age + 0 < 13");
    assert!(ids(&mut db, "age > 30 AND age < 5").is_empty());
    assert!(ids(&mut db, "age > 7 AND age < 7").is_empty());

    db.exec("BEGIN;").unwrap();
    db.exec("UPDATE people SET age = 5 WHERE id < 40 AND age = 3;").unwrap();
    same(&mut db, "age IN (3, 5)", "age + 0 = 3 OR age + 0 = 5");
    same(&mut db, "age >= 3 AND age <= 5", "age + 0 >= 3 AND age + 0 <= 5");
    db.exec("COMMIT;").unwrap();
    same(&mut db, "age IN (3, 5)", "age + 0 = 3 OR age + 0 = 5");

    let page = db
        .exec("SELECT id FROM people WHERE age IN (1, 2, 4) LIMIT 4 OFFSET 13;")
        .unwrap()
        .tuples;
    assert_eq!(page.len(), 4);
    let all = ids(&mut db, "age IN (1, 2, 4)");
    assert!(page.iter().all(|row| all.contains(&row[0])));
}

#[test]
fn joins_read_only_the_columns_they_need_and_stay_exact() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec(
        "CREATE TABLE staff (id INT PRIMARY KEY, name VARCHAR(16), boss INT NULLABLE, pay INT);",
    )
    .unwrap();
    db.exec("CREATE TABLE desks (id INT PRIMARY KEY, staff_id INT, floor INT);")
        .unwrap();
    db.exec(
        "INSERT INTO staff (id, name, boss, pay) VALUES \
         (1, 'ana', NULL, 90), (2, 'bo', 1, 50), (3, 'cy', 1, 60), (4, 'di', 2, 40);",
    )
    .unwrap();
    db.exec("INSERT INTO desks (id, staff_id, floor) VALUES (10, 2, 3), (11, 3, 1), (12, 9, 2);")
        .unwrap();

    let rows = |db: &mut MvccDatabase, sql: &str| db.exec(sql).unwrap().tuples;
    let s = |text: &str| Value::String(text.into());
    let n = |number: i128| Value::Number(number);

    assert_eq!(
        rows(
            &mut db,
            "SELECT e.name, b.name FROM staff AS e JOIN staff AS b ON e.boss = b.id ORDER BY e.pay DESC;"
        ),
        vec![vec![s("cy"), s("ana")], vec![s("bo"), s("ana")], vec![s("di"), s("bo")]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT staff.name FROM staff JOIN desks ON staff.id = desks.staff_id ORDER BY desks.floor;"
        ),
        vec![vec![s("cy")], vec![s("bo")]]
    );
    assert_eq!(
        rows(
            &mut db,
            "SELECT staff.name, desks.id FROM staff RIGHT JOIN desks ON staff.id = desks.staff_id ORDER BY desks.id;"
        ),
        vec![vec![s("bo"), n(10)], vec![s("cy"), n(11)], vec![Value::Null, n(12)]]
    );

    let everything = db
        .exec("SELECT * FROM staff JOIN desks ON staff.id = desks.staff_id ORDER BY desks.id;")
        .unwrap();
    assert_eq!(everything.schema.len(), 7);
    assert_eq!(everything.tuples[0], vec![n(2), s("bo"), n(1), n(50), n(10), n(2), n(3)]);
}

#[test]
fn numeric_columns_with_number_literals_compare_and_compute_exactly() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE ledger (id INT PRIMARY KEY, amount NUMERIC(16, 2) NULLABLE);")
        .unwrap();
    db.exec(
        "INSERT INTO ledger (id, amount) VALUES (1, 10.25), (2, 50000), (3, 50000.01), (4, NULL), (5, 7);",
    )
    .unwrap();

    let column = |db: &mut MvccDatabase, sql: &str| -> Vec<String> {
        db.exec(sql)
            .unwrap()
            .tuples
            .into_iter()
            .map(|row| row.iter().map(|value| value.to_string()).collect::<Vec<_>>().join(","))
            .collect()
    };

    assert_eq!(
        column(&mut db, "SELECT id FROM ledger WHERE amount > 50000 ORDER BY id;"),
        ["3"]
    );
    assert_eq!(
        column(&mut db, "SELECT id FROM ledger WHERE 50000 <= amount ORDER BY id;"),
        ["2", "3"]
    );
    assert_eq!(column(&mut db, "SELECT id FROM ledger WHERE amount = 7 ORDER BY id;"), ["5"]);
    assert_eq!(
        column(&mut db, "SELECT id FROM ledger WHERE amount < 10.3 ORDER BY id;"),
        ["1", "5"]
    );
    assert_eq!(
        column(
            &mut db,
            "SELECT id, amount * 2, amount + 0.5, 100 - amount FROM ledger ORDER BY id;"
        ),
        column(
            &mut db,
            "SELECT id, amount * 2.0, amount + 0.50, 100.0 - amount FROM ledger ORDER BY id;"
        )
    );
    let doubled = column(&mut db, "SELECT amount * 1.085 FROM ledger WHERE id = 1;");
    assert!(doubled[0].starts_with("11.12"), "{doubled:?}");
    assert!(db.exec("SELECT amount / 0 FROM ledger WHERE id = 1;").is_err());
}

#[test]
fn offset_over_index_order_skips_exactly_the_visible_rows() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE t (id INT PRIMARY KEY, v INT);").unwrap();
    let rows: Vec<_> = (0..1_000).map(|id| format!("({id}, {})", (id % 7 == 3) as i32)).collect();
    db.exec(&format!("INSERT INTO t (id, v) VALUES {};", rows.join(","))).unwrap();
    db.exec("DELETE FROM t WHERE v = 1;").unwrap();

    let page = |db: &mut MvccDatabase, offset: usize| -> Vec<i128> {
        db.exec(&format!("SELECT id FROM t ORDER BY id LIMIT 10 OFFSET {offset};"))
            .unwrap()
            .tuples
            .into_iter()
            .map(|row| match row[0] {
                Value::Number(id) => id,
                ref other => panic!("{other:?}"),
            })
            .collect()
    };

    let mut live: Vec<i128> = (0..1_000).filter(|id| id % 7 != 3).collect();
    for offset in [0, 1, 219, 220, 255, 256, 257, 511, 600, live.len() - 3, live.len() + 5] {
        let expected: Vec<i128> = live.iter().skip(offset).take(10).copied().collect();
        assert_eq!(page(&mut db, offset), expected, "offset {offset}");
    }

    db.exec("BEGIN;").unwrap();
    db.exec("DELETE FROM t WHERE id < 300;").unwrap();
    live.retain(|&id| id >= 300);
    for offset in [0, 250, 256, 400] {
        let expected: Vec<i128> = live.iter().skip(offset).take(10).copied().collect();
        assert_eq!(page(&mut db, offset), expected, "own deletes, offset {offset}");
    }
    db.exec("ROLLBACK;").unwrap();

    let descending =
        db.exec("SELECT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 300;").unwrap().tuples;
    let all: Vec<i128> = (0..1_000).filter(|id| id % 7 != 3).rev().collect();
    assert_eq!(descending, vec![vec![Value::Number(all[300])], vec![Value::Number(all[301])]]);
}

#[test]
fn a_filtered_join_on_the_primary_key_drives_from_the_filtered_side() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(16));").unwrap();
    db.exec(
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, status VARCHAR(16), amount INT);",
    )
    .unwrap();
    db.exec("CREATE INDEX idx_orders_status ON orders(status);").unwrap();
    db.exec("CREATE INDEX idx_orders_user ON orders(user_id);").unwrap();

    let users: Vec<_> = (0..50).map(|id| format!("({id}, 'u{id}')")).collect();
    db.exec(&format!("INSERT INTO users (id, name) VALUES {};", users.join(",")))
        .unwrap();
    let status = |id: i32| ["new", "paid", "sent"][(id % 3) as usize];
    let orders: Vec<_> = (0..300)
        .map(|id| format!("({id}, {}, '{}', {id})", id * 7 % 60, status(id)))
        .collect();
    db.exec(&format!(
        "INSERT INTO orders (id, user_id, status, amount) VALUES {};",
        orders.join(",")
    ))
    .unwrap();

    let sql = "SELECT u.name, o.amount FROM users AS u JOIN orders AS o ON u.id = o.user_id \
               WHERE o.status = 'paid'";
    let plan = db.exec(&format!("EXPLAIN {sql};")).unwrap().tuples;
    assert!(format!("{:?}", plan[0]).contains("orders"), "driven from orders: {plan:?}");

    let mut got: Vec<(String, i128)> = db
        .exec(&format!("{sql};"))
        .unwrap()
        .tuples
        .into_iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::String(name), Value::Number(amount)) => (name.to_string(), *amount),
            other => panic!("{other:?}"),
        })
        .collect();
    got.sort();

    let mut expected: Vec<(String, i128)> = (0..300)
        .filter(|&id| status(id) == "paid" && id * 7 % 60 < 50)
        .map(|id| (format!("u{}", id * 7 % 60), id as i128))
        .collect();
    expected.sort();
    assert_eq!(got, expected);

    let limited = db.exec(&format!("{sql} LIMIT 5;")).unwrap().tuples;
    assert_eq!(limited.len(), 5);

    let not_on_key = db
        .exec("EXPLAIN SELECT u.name FROM users AS u JOIN orders AS o ON u.name = o.status WHERE o.status = 'paid';")
        .unwrap()
        .tuples;
    assert!(format!("{:?}", not_on_key[0]).contains("users"), "{not_on_key:?}");
}
