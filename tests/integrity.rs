use std::path::{Path, PathBuf};
use umbra::db::{DatabaseError, MvccDatabase, QuerySet};
use umbra::sql::statement::Value;

type Result<T> = std::result::Result<T, DatabaseError>;

struct State {
    db: MvccDatabase,
    path: PathBuf,
}

impl State {
    fn exec(&mut self, sql: &str) -> Result<QuerySet> {
        self.db.exec(sql)
    }

    fn reopen(&mut self) -> Result<()> {
        self.db.close()?;
        self.db = MvccDatabase::init(&self.path)?;
        Ok(())
    }
}

impl Default for State {
    #[track_caller]
    fn default() -> Self {
        let caller = std::panic::Location::caller();
        let stem = Path::new(caller.file()).file_stem().unwrap().to_str().unwrap();
        let path = PathBuf::from(format!("{}-{}.db", stem, caller.line()));
        let _ = std::fs::remove_dir_all(&path);

        Self { db: MvccDatabase::init(&path).unwrap(), path }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let _ = self.db.close();
        std::fs::remove_dir_all(&self.path).expect("Failed to drop State")
    }
}

#[track_caller]
fn accounts() -> Result<State> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                owner VARCHAR(30),
                balance INTEGER,
                iban VARCHAR(34) UNIQUE NULLABLE
            );"#,
    )?;
    db.exec("INSERT INTO accounts (id, owner, balance) VALUES (1, 'Ada', 100);")?;

    Ok(db)
}

#[track_caller]
fn shop(on_delete: &str) -> Result<State> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE customers (
                id INTEGER PRIMARY KEY,
                email VARCHAR(50) UNIQUE
            );"#,
    )?;
    db.exec(&format!(
        r#"
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer INTEGER NULLABLE REFERENCES customers(id) {on_delete},
                contact VARCHAR(50) NULLABLE,
                FOREIGN KEY (contact) REFERENCES customers(email)
            );"#
    ))?;
    db.exec(
        r#"
            INSERT INTO customers (id, email) VALUES
                (1, 'ada@example.com'),
                (2, 'grace@example.com');
        "#,
    )?;

    Ok(db)
}

#[test]
fn rollback_to_savepoint() -> Result<()> {
    let mut db = accounts()?;

    db.exec("BEGIN;")?;
    db.exec("INSERT INTO accounts (id, owner, balance) VALUES (2, 'Grace', 50);")?;
    db.exec("SAVEPOINT transfer;")?;
    db.exec("INSERT INTO accounts (id, owner, balance) VALUES (3, 'Alan', 10);")?;
    db.exec("UPDATE accounts SET balance = 0 WHERE id = 2;")?;
    db.exec("DELETE FROM accounts WHERE id = 1;")?;
    db.exec("ROLLBACK TO SAVEPOINT transfer;")?;

    let query = db.exec("SELECT id, balance FROM accounts ORDER BY id;")?;
    assert_eq!(query.tuples, vec![vec![1.into(), 100.into()], vec![2.into(), 50.into()]]);

    db.exec("INSERT INTO accounts (id, owner, balance) VALUES (4, 'Edsger', 70);")?;
    db.exec("COMMIT;")?;

    let query = db.exec("SELECT id FROM accounts ORDER BY id;")?;
    assert_eq!(query.tuples, vec![vec![1.into()], vec![2.into()], vec![4.into()]]);

    Ok(())
}

#[test]
fn nested_savepoints() -> Result<()> {
    let mut db = accounts()?;

    db.exec("BEGIN;")?;
    db.exec("UPDATE accounts SET balance = 1 WHERE id = 1;")?;
    db.exec("SAVEPOINT first;")?;
    db.exec("UPDATE accounts SET balance = 2 WHERE id = 1;")?;
    db.exec("SAVEPOINT second;")?;
    db.exec("UPDATE accounts SET balance = 3 WHERE id = 1;")?;

    db.exec("ROLLBACK TO SAVEPOINT second;")?;
    assert_eq!(db.exec("SELECT balance FROM accounts;")?.tuples, vec![vec![2.into()]]);

    db.exec("ROLLBACK TO SAVEPOINT first;")?;
    assert_eq!(db.exec("SELECT balance FROM accounts;")?.tuples, vec![vec![1.into()]]);
    assert!(db.exec("ROLLBACK TO SAVEPOINT second;").is_err());

    db.exec("ROLLBACK TO SAVEPOINT first;")?;
    db.exec("UPDATE accounts SET balance = 5 WHERE id = 1;")?;
    db.exec("RELEASE SAVEPOINT first;")?;
    assert!(db.exec("ROLLBACK TO SAVEPOINT first;").is_err());

    db.exec("ROLLBACK;")?;
    assert_eq!(db.exec("SELECT balance FROM accounts;")?.tuples, vec![vec![100.into()]]);

    Ok(())
}

#[test]
fn savepoint_revives_failed_transaction() -> Result<()> {
    let mut db = accounts()?;

    db.exec("BEGIN;")?;
    db.exec("INSERT INTO accounts (id, owner, balance, iban) VALUES (2, 'Grace', 50, 'DE01');")?;
    db.exec("SAVEPOINT before_duplicate;")?;
    db.exec("INSERT INTO accounts (id, owner, balance, iban) VALUES (3, 'Alan', 10, 'DE02');")?;
    assert!(
        db.exec("INSERT INTO accounts (id, owner, balance) VALUES (1, 'Ada', 0);")
            .is_err()
    );
    assert!(db.exec("SELECT * FROM accounts;").is_err());

    db.exec("ROLLBACK TO SAVEPOINT before_duplicate;")?;
    db.exec("INSERT INTO accounts (id, owner, balance, iban) VALUES (4, 'Edsger', 70, 'DE02');")?;
    db.exec("COMMIT;")?;

    let query = db.exec("SELECT id FROM accounts ORDER BY id;")?;
    assert_eq!(query.tuples, vec![vec![1.into()], vec![2.into()], vec![4.into()]]);

    Ok(())
}

#[test]
fn savepoint_undoes_unique_keys_and_ddl() -> Result<()> {
    let mut db = accounts()?;

    db.exec("BEGIN;")?;
    db.exec("SAVEPOINT setup;")?;
    db.exec("INSERT INTO accounts (id, owner, balance, iban) VALUES (2, 'Grace', 50, 'DE01');")?;
    db.exec("CREATE TABLE audit (id INTEGER PRIMARY KEY);")?;
    db.exec("ROLLBACK TO SAVEPOINT setup;")?;

    assert!(db.exec("SELECT * FROM audit;").is_err());
    db.exec("INSERT INTO accounts (id, owner, balance, iban) VALUES (3, 'Alan', 10, 'DE01');")?;
    db.exec("COMMIT;")?;

    let query = db.exec("SELECT id FROM accounts ORDER BY id;")?;
    assert_eq!(query.tuples, vec![vec![1.into()], vec![3.into()]]);

    Ok(())
}

#[test]
fn savepoints_outside_transaction() -> Result<()> {
    let mut db = accounts()?;

    assert!(db.exec("SAVEPOINT transfer;").is_err());
    assert!(db.exec("ROLLBACK TO SAVEPOINT transfer;").is_err());
    assert!(db.exec("RELEASE transfer;").is_err());

    Ok(())
}

#[test]
fn defaults() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                stock INTEGER DEFAULT 2 + 3,
                weight REAL DEFAULT 1,
                description TEXT NULL DEFAULT 'none',
                active BOOLEAN DEFAULT TRUE,
                supplier INTEGER NULL
            );"#,
    )?;
    db.exec("INSERT INTO products (id) VALUES (1);")?;
    db.exec("INSERT INTO products (id, stock, description, supplier) VALUES (2, 7, NULL, 9);")?;
    db.exec("INSERT INTO products (supplier, id) VALUES (4, 3);")?;

    let query = db.exec("SELECT * FROM products ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![
                1.into(),
                5.into(),
                1f64.into(),
                "none".into(),
                Value::Boolean(true),
                Value::Null
            ],
            vec![2.into(), 7.into(), 1f64.into(), Value::Null, Value::Boolean(true), 9.into()],
            vec![3.into(), 5.into(), 1f64.into(), "none".into(), Value::Boolean(true), 4.into()],
        ]
    );

    assert!(db.exec("CREATE TABLE broken (id INTEGER PRIMARY KEY, n INTEGER DEFAULT id);").is_err());
    assert!(
        db.exec("CREATE TABLE broken (id INTEGER PRIMARY KEY, n INTEGER DEFAULT 'text');")
            .is_err()
    );

    Ok(())
}

#[test]
fn defaults_after_restart() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE invoices (
                id INTEGER PRIMARY KEY,
                currency VARCHAR(3) DEFAULT 'EUR',
                rate NUMERIC(6, 2) DEFAULT 1.5
            );"#,
    )?;
    db.exec("CHECKPOINT;")?;
    db.reopen()?;

    db.exec("INSERT INTO invoices (id) VALUES (1);")?;
    db.exec("INSERT INTO invoices (id, currency, rate) VALUES (2, 'EUR', 1.5);")?;

    let query = db.exec("SELECT currency, rate FROM invoices ORDER BY id;")?;
    assert_eq!(query.tuples[0][0], "EUR".into());
    assert_eq!(query.tuples[0], query.tuples[1]);

    Ok(())
}

#[test]
fn nullability() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE contacts (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                phone TEXT NULL,
                email TEXT NULLABLE,
                city TEXT
            );"#,
    )?;
    db.exec("INSERT INTO contacts (id, name, phone, email, city) VALUES (1, 'Ada', NULL, NULL, 'London');")?;
    assert!(db.exec("INSERT INTO contacts (id, name, phone, email, city) VALUES (2, NULL, '1', 'a', 'Paris');").is_err());
    assert!(db.exec("INSERT INTO contacts (id, name, phone, email, city) VALUES (3, 'Alan', '1', 'a', NULL);").is_err());

    Ok(())
}

#[test]
fn omitted_nullable_columns() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE notes (id INTEGER PRIMARY KEY, author INTEGER NULLABLE, body TEXT NULLABLE);")?;
    db.exec("INSERT INTO notes (id, body) VALUES (1, 'draft');")?;
    db.exec("INSERT INTO notes (id) VALUES (2);")?;

    let query = db.exec("SELECT id, author, body FROM notes ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![1.into(), Value::Null, "draft".into()],
            vec![2.into(), Value::Null, Value::Null]
        ]
    );

    Ok(())
}

#[test]
fn check_constraints() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                price INTEGER CHECK (price > 0),
                discount INTEGER NULL,
                name TEXT,
                CHECK (discount IS NULL OR discount < price),
                CHECK (name != '')
            );"#,
    )?;
    db.exec("INSERT INTO products (id, price, discount, name) VALUES (1, 10, 5, 'Lamp');")?;
    db.exec("INSERT INTO products (id, price, name) VALUES (2, 10, 'Desk');")?;

    assert!(
        db.exec("INSERT INTO products (id, price, name) VALUES (3, 0, 'Chair');")
            .is_err()
    );
    assert!(
        db.exec("INSERT INTO products (id, price, discount, name) VALUES (4, 10, 10, 'Sofa');")
            .is_err()
    );
    assert!(db.exec("INSERT INTO products (id, price, name) VALUES (5, 10, '');").is_err());
    assert!(db.exec("UPDATE products SET price = -1 WHERE id = 1;").is_err());
    assert!(db.exec("UPDATE products SET discount = 20 WHERE id = 2;").is_err());

    db.exec("UPDATE products SET price = 20, discount = 15 WHERE id = 1;")?;

    let query = db.exec("SELECT id, price, discount FROM products ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![vec![1.into(), 20.into(), 15.into()], vec![2.into(), 10.into(), Value::Null]]
    );

    Ok(())
}

#[test]
fn check_constraints_after_restart() -> Result<()> {
    let mut db = State::default();

    assert!(
        db.exec("CREATE TABLE readings (id INTEGER PRIMARY KEY, value INTEGER CHECK (value + 1));")
            .is_err()
    );
    assert!(
        db.exec("CREATE TABLE readings (id INTEGER PRIMARY KEY, value INTEGER CHECK (other > 0));")
            .is_err()
    );

    db.exec(
        r#"
            CREATE TABLE readings (
                id INTEGER PRIMARY KEY,
                label TEXT CHECK (label LIKE 'it''s%' OR label = 'q'),
                value INTEGER CHECK (ABS(value) < 10 AND value != -3)
            );"#,
    )?;
    db.exec("CHECKPOINT;")?;
    db.reopen()?;

    db.exec(r#"INSERT INTO readings (id, label, value) VALUES (1, 'it''s fine', 2);"#)?;
    db.exec("INSERT INTO readings (id, label, value) VALUES (2, 'q', 9);")?;
    assert!(db.exec("INSERT INTO readings (id, label, value) VALUES (3, 'x', 1);").is_err());
    assert!(db.exec("INSERT INTO readings (id, label, value) VALUES (4, 'q', -3);").is_err());
    assert!(db.exec("INSERT INTO readings (id, label, value) VALUES (5, 'q', 12);").is_err());

    Ok(())
}

#[test]
fn unique_columns() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(20) UNIQUE,
                badge INTEGER UNIQUE NULLABLE
            );"#,
    )?;
    db.exec(
        "INSERT INTO users (id, username, badge) VALUES (1, 'ada', NULL), (2, 'grace', NULL);",
    )?;
    assert!(db.exec("INSERT INTO users (id, username) VALUES (3, 'ada');").is_err());
    assert!(db.exec("UPDATE users SET username = 'grace' WHERE id = 1;").is_err());

    db.exec("INSERT INTO users (id, username, badge) VALUES (4, 'alan', 7);")?;
    assert!(
        db.exec("INSERT INTO users (id, username, badge) VALUES (5, 'edsger', 7);")
            .is_err()
    );

    let query = db.exec("SELECT username FROM users ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![vec!["ada".into()], vec!["grace".into()], vec!["alan".into()]]
    );

    Ok(())
}

#[test]
fn foreign_keys() -> Result<()> {
    let mut db = shop("")?;

    db.exec("INSERT INTO orders (id, customer, contact) VALUES (1, 1, 'grace@example.com'), (2, NULL, NULL);")?;
    assert!(db.exec("INSERT INTO orders (id, customer) VALUES (3, 9);").is_err());
    assert!(
        db.exec("INSERT INTO orders (id, contact) VALUES (4, 'nobody@example.com');")
            .is_err()
    );
    assert!(db.exec("UPDATE orders SET customer = 9 WHERE id = 1;").is_err());
    db.exec("UPDATE orders SET customer = 2 WHERE id = 1;")?;

    assert!(db.exec("DELETE FROM customers WHERE id = 2;").is_err());
    assert!(db.exec("UPDATE customers SET email = 'g@example.com' WHERE id = 2;").is_err());
    db.exec("UPDATE customers SET email = 'a@example.com' WHERE id = 1;")?;
    db.exec("DELETE FROM customers WHERE id = 1;")?;

    db.exec("BEGIN;")?;
    db.exec("INSERT INTO customers (id, email) VALUES (3, 'alan@example.com');")?;
    db.exec("INSERT INTO orders (id, customer) VALUES (5, 3);")?;
    db.exec("DELETE FROM orders WHERE id = 1;")?;
    db.exec("DELETE FROM customers WHERE id = 2;")?;
    db.exec("COMMIT;")?;

    let query = db.exec("SELECT id FROM customers;")?;
    assert_eq!(query.tuples, vec![vec![3.into()]]);

    Ok(())
}

#[test]
fn on_delete_cascade() -> Result<()> {
    let mut db = shop("ON DELETE CASCADE")?;

    db.exec("CREATE TABLE shipments (id INTEGER PRIMARY KEY, purchase INTEGER REFERENCES orders(id) ON DELETE CASCADE);")?;
    db.exec("INSERT INTO orders (id, customer) VALUES (1, 1), (2, 1), (3, 2);")?;
    db.exec("INSERT INTO shipments (id, purchase) VALUES (1, 1), (2, 3);")?;
    db.exec("DELETE FROM customers WHERE id = 1;")?;

    assert_eq!(db.exec("SELECT id FROM orders;")?.tuples, vec![vec![3.into()]]);
    assert_eq!(db.exec("SELECT id FROM shipments;")?.tuples, vec![vec![2.into()]]);

    db.exec(
        r#"
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                manager INTEGER NULL REFERENCES employees(id) ON DELETE CASCADE
            );"#,
    )?;
    db.exec(
        "INSERT INTO employees (id, manager) VALUES (1, NULL), (2, 1), (3, 2), (4, 4), (5, NULL);",
    )?;
    db.exec("DELETE FROM employees WHERE id = 1;")?;

    let query = db.exec("SELECT id FROM employees ORDER BY id;")?;
    assert_eq!(query.tuples, vec![vec![4.into()], vec![5.into()]]);

    Ok(())
}

#[test]
fn foreign_keys_after_restart() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE teams (id INTEGER PRIMARY KEY, size INTEGER, name TEXT UNIQUE);")?;
    assert!(
        db.exec("CREATE TABLE players (id INTEGER PRIMARY KEY, team INTEGER REFERENCES teams(size));")
            .is_err()
    );
    assert!(
        db.exec("CREATE TABLE players (id INTEGER PRIMARY KEY, team INTEGER REFERENCES teams(name));")
            .is_err()
    );
    assert!(
        db.exec("CREATE TABLE players (id INTEGER PRIMARY KEY, team INTEGER REFERENCES leagues(id));")
            .is_err()
    );
    assert!(
        db.exec(
            "CREATE TABLE players (id INTEGER PRIMARY KEY, FOREIGN KEY (team) REFERENCES teams(id));"
        )
        .is_err()
    );

    db.exec("CREATE TABLE players (id INTEGER PRIMARY KEY, team INTEGER REFERENCES teams(id) ON DELETE CASCADE);")?;
    db.exec("INSERT INTO teams (id, size, name) VALUES (1, 11, 'Benfica');")?;
    db.exec("INSERT INTO players (id, team) VALUES (1, 1);")?;
    db.exec("CHECKPOINT;")?;
    db.reopen()?;

    assert!(db.exec("INSERT INTO players (id, team) VALUES (2, 2);").is_err());
    db.exec("DELETE FROM teams WHERE id = 1;")?;
    assert!(db.exec("SELECT id FROM players;")?.tuples.is_empty());

    Ok(())
}

#[test]
fn concurrent_customer_delete_and_order() -> Result<()> {
    for level in ["READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"] {
        for order_first in [true, false] {
            let mut db = shop("")?;
            let mut other = db.db.session();
            let begin = format!("BEGIN ISOLATION LEVEL {level};");

            db.exec(&begin)?;
            other.exec(&begin)?;
            db.exec("INSERT INTO orders (id, customer) VALUES (1, 1);")?;
            other.exec("DELETE FROM customers WHERE id = 1;")?;

            match order_first {
                true => {
                    db.exec("COMMIT;")?;
                    assert!(other.exec("COMMIT;").is_err());
                },
                false => {
                    other.exec("COMMIT;")?;
                    assert!(db.exec("COMMIT;").is_err());
                },
            }

            let orphans = db.exec(
                r#"
                    SELECT orders.id FROM orders
                    LEFT JOIN customers ON orders.customer = customers.id
                    WHERE customers.id IS NULL;
                "#,
            )?;
            assert!(orphans.tuples.is_empty());
        }
    }

    Ok(())
}
