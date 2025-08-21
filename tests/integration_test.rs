use std::path::Path;

use umbra::db::{Database, DatabaseError, QuerySet};
use umbra::sql::statement::Value;

type Result<T> = std::result::Result<T, DatabaseError>;

struct State {
    db: Database<std::fs::File>,
    path: std::path::PathBuf,
}

impl State {
    fn new(path: impl AsRef<Path>) -> Self {
        Self {
            db: Database::init(&path).unwrap(),
            path: path.as_ref().to_path_buf(),
        }
    }

    fn exec(&mut self, sql: &str) -> Result<QuerySet> {
        self.db.exec(sql)
    }
}

impl Default for State {
    #[track_caller]
    fn default() -> Self {
        let caller = std::panic::Location::caller();
        let path = format!(
            "{}-{}.db",
            Path::new(caller.file())
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap(),
            caller.line()
        );
        let path = Path::new(path.as_str());

        Self::new(path)
    }
}

impl Drop for State {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).expect("Failed to drop State")
    }
}

#[test]
fn serialisation_and_deserialisation() -> Result<()> {
    let mut db = State::default();

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
    let mut db = State::default();
    db.exec("CREATE TABLE logs (id SERIAL PRIMARY KEY, log TEXT UNIQUE);")?;

    let mut keys = ["zebra", "banana", "fig", "cherry", "apple"];
    for key in keys {
        let text = format!(
            r#"The system logs are processed nightly to ensure data integrity and performance.
            This process involves parsing, cleaning, and aggregating terabytes of data.
            The critical keyword for this specific entry is: {key}"#
        );
        db.exec(format!("INSERT INTO logs (log) VALUES ('{text}');").as_str())?;
    }

    let query = db.exec("SELECT SUBSTRING(log FROM 230) FROM logs ORDER BY log;")?;
    keys.sort_unstable();
    for (idx, key) in keys.iter().enumerate() {
        assert_eq!(query.tuples[idx][0], Value::String(key.to_string()))
    }

    let query = db.exec("SELECT SUBSTRING(log FROM 230) FROM logs ORDER BY log DESC;")?;
    keys.reverse();
    for (idx, key) in keys.iter().enumerate() {
        assert_eq!(query.tuples[idx][0], Value::String(key.to_string()))
    }

    Ok(())
}
