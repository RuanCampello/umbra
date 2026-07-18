//! End-to-end tests for the MVCC-backed database session:
//! SQL strings through the full pipeline (parse → analyse → optimise →
//! execute) against the engine, including durability across reopen.

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
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64));")
        .unwrap();
    db.exec("INSERT INTO users (id, name) VALUES (1, 'alice');")
        .unwrap();
    db.exec("INSERT INTO users (id, name) VALUES (2, 'bob');")
        .unwrap();
    db.exec("INSERT INTO users (id, name) VALUES (3, 'carol');")
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

    db.exec("UPDATE users SET name = 'updated' WHERE id = 2;")
        .unwrap();
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
    db.exec("INSERT INTO users (id, name) VALUES (4, 'dave');")
        .unwrap();
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
    assert!(db
        .exec("INSERT INTO users (id, nope) VALUES (9, 'x');")
        .is_err());
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
    assert_eq!(
        result.tuples[0],
        vec![Value::String("alice".into()), Value::Number(50)]
    );
    assert_eq!(
        result.tuples[1],
        vec![Value::String("bob".into()), Value::Number(70)]
    );
    assert_eq!(
        result.tuples[2],
        vec![Value::String("alice".into()), Value::Number(100)]
    );
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
    assert_eq!(
        result.tuples[0],
        vec![Value::String("alice".into()), Value::Number(150)]
    );
    assert_eq!(
        result.tuples[1],
        vec![Value::String("bob".into()), Value::Number(70)]
    );
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
        db.exec("CREATE TABLE logs (id SERIAL PRIMARY KEY, msg VARCHAR(64));")
            .unwrap();

        db.exec("INSERT INTO logs (msg) VALUES ('first');").unwrap();
        db.exec("INSERT INTO logs (msg) VALUES ('second');")
            .unwrap();

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
fn aggregates_without_group_by() {
    let mut db = MvccDatabase::in_memory().unwrap();
    db.exec("CREATE TABLE sales (id INT PRIMARY KEY, amount INT);")
        .unwrap();

    let result = db.exec("SELECT COUNT(*) FROM sales;").unwrap();
    assert_eq!(result.tuples, vec![vec![Value::Number(0)]]);

    for (id, amount) in [(1, 10), (2, 30), (3, 20)] {
        db.exec(&format!(
            "INSERT INTO sales (id, amount) VALUES ({id}, {amount});"
        ))
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
    assert_eq!(
        result.tuples[0],
        vec![Value::String("north".into()), Value::Number(30)]
    );
    assert_eq!(
        result.tuples[1],
        vec![Value::String("south".into()), Value::Number(20)]
    );
    assert_eq!(
        result.tuples[2],
        vec![Value::String("west".into()), Value::Number(1)]
    );

    // WHERE before grouping and COUNT per group
    let result = db
        .exec(
            "SELECT region, COUNT(*) FROM sales WHERE amount > 4 GROUP BY region ORDER BY region;",
        )
        .unwrap();
    assert_eq!(result.tuples.len(), 2);
    assert_eq!(
        result.tuples[0],
        vec![Value::String("north".into()), Value::Number(2)]
    );
    assert_eq!(
        result.tuples[1],
        vec![Value::String("south".into()), Value::Number(2)]
    );
}

#[test]
fn point_lookup_by_primary_key() {
    let mut db = memory_db_with_users();

    let result = db.exec("SELECT name FROM users WHERE id = 2;").unwrap();
    assert_eq!(result.tuples.len(), 1);
    assert_eq!(result.tuples[0][0], Value::String("bob".into()));

    assert_eq!(
        db.exec("SELECT * FROM users WHERE id = 99;")
            .unwrap()
            .tuples
            .len(),
        0
    );
}

#[test]
fn point_lookup_sees_own_uncommitted_writes() {
    let mut db = memory_db_with_users();

    db.exec("BEGIN TRANSACTION;").unwrap();
    db.exec("INSERT INTO users (id, name) VALUES (4, 'dave');")
        .unwrap();
    db.exec("UPDATE users SET name = 'changed' WHERE id = 1;")
        .unwrap();

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

    db.exec("CREATE UNIQUE INDEX users_name_idx ON users(name);")
        .unwrap();

    assert!(db
        .exec("INSERT INTO users (id, name) VALUES (9, 'alice');")
        .is_err());
    assert!(db
        .exec("INSERT INTO users (id, name) VALUES (9, 'dora');")
        .is_ok());
}

#[test]
fn drop_table_via_sql() {
    let mut db = memory_db_with_users();

    db.exec("DROP TABLE users;").unwrap();
    assert!(db.exec("SELECT * FROM users;").is_err());

    // recreating under the same name starts empty
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64));")
        .unwrap();
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
        db.exec("CREATE TABLE items (id INT PRIMARY KEY, label VARCHAR(32));")
            .unwrap();
        db.exec("INSERT INTO items (id, label) VALUES (1, 'kept');")
            .unwrap();

        db.exec("BEGIN TRANSACTION;").unwrap();
        db.exec("INSERT INTO items (id, label) VALUES (2, 'lost');")
            .unwrap();
        // dropped without commit: must not survive
        db.close().unwrap();
    }

    {
        let mut db = MvccDatabase::init(&dir).unwrap();
        let result = db.exec("SELECT id, label FROM items;").unwrap();

        assert_eq!(result.tuples.len(), 1);
        assert_eq!(result.tuples[0][0], Value::Number(1));
        assert_eq!(result.tuples[0][1], Value::String("kept".into()));

        db.exec("INSERT INTO items (id, label) VALUES (3, 'after');")
            .unwrap();
        assert_eq!(db.exec("SELECT * FROM items;").unwrap().tuples.len(), 2);
    }

    let _ = std::fs::remove_dir_all(&dir);
}
