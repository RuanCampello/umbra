use umbra::db::{Database, DatabaseError, QuerySet};
use umbra::sql::statement::Value;

type Result<T> = std::result::Result<T, DatabaseError>;

struct State {
    db: Database<std::fs::File>,
    path: std::path::PathBuf,
}

impl State {
    fn new(path: impl AsRef<std::path::Path>) -> Self {
        Self {
            db: Database::init(&path).unwrap(),
            path: path.as_ref().to_path_buf(),
        }
    }

    fn exec(&mut self, sql: &str) -> Result<QuerySet> {
        self.db.exec(sql)
    }
}

impl Drop for State {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).expect("Failed to drop State")
    }
}

#[test]
fn serialisation_and_deserialisation() -> Result<()> {
    let mut db = State::new("test.db");

    db.exec(
        r#"
            CREATE TABLE employees (
                id SERIAL PRIMARY KEY,
                name VARCHAR(30),
                department VARCHAR(20),
                salary DOUBLE PRECISION, 
                hire_date DATE,
                performance_rating INT
            );"#,
    )?;

    db.exec(
        r#"
            INSERT INTO employees (name, department, salary, hire_date, performance_rating) VALUES
                ('John Smith', 'Engineering', 85000.00, '2020-05-15', 8),
                ('Jane Doe', 'Marketing', 72000.00, '2019-11-20', 7),
                ('Mike Johnson', 'Engineering', 92000.00, '2018-03-10', 9),
                ('Sarah Williams', 'HR', 68000.00, '2021-01-05', 6),
                ('David Brown', 'Marketing', 78000.00, '2020-07-22', 8),
                ('Emily Davis', 'Engineering', 88000.00, '2019-09-14', 7);
        "#,
    )?;

    let query = db.exec(
        r#"
            SELECT 
                department AS dept,
                COUNT(*) AS employee_count,
                TRUNC(AVG(salary), 1) AS avg_salary,
                MAX(performance_rating) AS max_rating,
                MIN(hire_date) AS earliest_hire
            FROM employees
            GROUP BY dept
            ORDER BY avg_salary DESC;
        "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                "Engineering".into(),
                3.into(),
                88333.3f64.into(),
                9.into(),
                Value::Temporal(("2018-03-10").try_into()?),
            ],
            vec![
                "Marketing".into(),
                2.into(),
                75000f64.into(),
                8.into(),
                Value::Temporal(("2019-11-20").try_into()?),
            ],
            vec![
                "HR".into(),
                1.into(),
                68000f64.into(),
                6.into(),
                Value::Temporal(("2021-01-05").try_into()?),
            ]
        ]
    );

    Ok(())
}

#[test]
fn unique_column_with_text() -> Result<()> {
    let mut db = State::new("test_unique_text.db");

    // Create table with TEXT UNIQUE column
    db.exec(
        r#"
            CREATE TABLE users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email TEXT UNIQUE
            );
        "#,
    )?;

    // Insert first record - should succeed
    db.exec("INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com');")?;

    // Insert record with different email - should succeed
    db.exec("INSERT INTO users (name, email) VALUES ('Jane Smith', 'jane@example.com');")?;

    // Try to insert duplicate email - should fail
    let result = db.exec("INSERT INTO users (name, email) VALUES ('Bob Johnson', 'john@example.com');");
    assert!(result.is_err(), "Expected unique constraint violation");

    // Test with empty string
    db.exec("INSERT INTO users (name, email) VALUES ('Empty User', '');")?;

    // Try to insert another empty string - should fail (empty string should also be unique)
    let result = db.exec("INSERT INTO users (name, email) VALUES ('Another Empty', '');");
    assert!(result.is_err(), "Expected unique constraint violation for empty string");

    // Test with long TEXT content
    let long_email = format!("{}@example.com", "a".repeat(1000));
    db.exec(&format!("INSERT INTO users (name, email) VALUES ('Long Email User', '{}');", long_email))?;

    // Try to insert the same long email - should fail
    let result = db.exec(&format!("INSERT INTO users (name, email) VALUES ('Another Long User', '{}');", long_email));
    assert!(result.is_err(), "Expected unique constraint violation for long text");

    // Verify we have the correct number of records
    let query = db.exec("SELECT COUNT(*) FROM users;")?;
    assert_eq!(query.tuples.len(), 1);
    assert_eq!(query.tuples[0][0], Value::Number(4)); // 3 successful inserts + 1 long email

    Ok(())
}