use std::path::Path;
use umbra::db::{Database, DatabaseError, QuerySet};
use umbra::sql::statement::{Type, Value};
use umbra::{interval, temporal};

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

fn assert_values(left: &[Vec<Value>], right: &[Vec<Value>]) {
    for (left_row, right_row) in left.iter().zip(right.iter()) {
        for (left_val, right_val) in left_row.iter().zip(right_row.iter()) {
            match (left_val, right_val) {
                (Value::Null, Value::Null) => {}
                _ => assert_eq!(left_val, right_val),
            }
        }
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
fn test_comparison_on_floats() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
            CREATE TABLE temperature_readings (
                reading_id SERIAL PRIMARY KEY,
                sensor_a REAL,
                sensor_b DOUBLE PRECISION
            );"#,
    )?;

    db.exec(
        r#"
            INSERT INTO temperature_readings(sensor_a, sensor_b) VALUES
                (25.5, 25.5000001),
                (25.5, 25.5),
                (0.3, 0.1 + 0.2),
                (100.0, 99.9999999);
        "#,
    )?;

    let query = db.exec(
        r#"
            SELECT 
                sensor_a = sensor_b,
                sensor_a > sensor_b,
                sensor_a < sensor_b
                FROM temperature_readings;
            "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                Value::Boolean(false),
                Value::Boolean(false),
                Value::Boolean(true)
            ],
            vec![
                Value::Boolean(true),
                Value::Boolean(false),
                Value::Boolean(false)
            ],
            vec![
                Value::Boolean(false),
                Value::Boolean(true),
                Value::Boolean(false)
            ],
            vec![
                Value::Boolean(false),
                Value::Boolean(true),
                Value::Boolean(false)
            ]
        ]
    );

    Ok(())
}

#[test]
fn insert_negative_floats() -> Result<()> {
    let mut db = State::default();
    db.exec(
        "CREATE TABLE location (id BIGSERIAL PRIMARY KEY, name VARCHAR(255), lat REAL, lon REAL);",
    )?;

    db.exec(
        r#"
            INSERT INTO location (name, lat, lon) VALUES
            ('Rio de Janeiro, Brazil', -22.906847, -43.172897),
            ('London, United Kingdom', 51.507351, -0.127758);
        "#,
    )?;

    Ok(())
}

#[test]
fn date_ordering() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
            CREATE TABLE temporal_data (
                id SERIAL PRIMARY KEY,
                event_name VARCHAR(100),
                event_datetime TIMESTAMP,
                event_date DATE,
                event_time TIME
            );
        "#,
    )?;
    db.exec(
        r#"
            INSERT INTO temporal_data (event_name, event_datetime, event_date, event_time) 
            VALUES
                ('Unix Epoch', '1970-01-01 00:00:00', '1970-01-01', '00:00:00'),
                ('Future Event', '2025-04-20 10:01:23', '2025-04-20', '10:01:23'),
                ('WWI Start', '1914-06-28 00:00:00', '1914-06-28', '00:00:00'),
                ('Midday', '2023-01-01 12:00:00', '2023-01-01', '12:00:00'),
                ('Midnight', '2023-01-01 00:00:00', '2023-01-01', '00:00:00');
            "#,
    )?;

    let query = db.exec("SELECT event_name FROM temporal_data WHERE event_time > '00:00:00';")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("Future Event".into())],
            vec![Value::String("Midday".into())]
        ]
    );

    Ok(())
}

#[test]
#[ignore = "what the hell is happening here?"]
fn select_with_between() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
            CREATE TABLE invoice (
                invoice_id     SERIAL PRIMARY KEY,
                client_id      INTEGER,
                total          REAL,
                invoice_date   TIMESTAMP
            );
            "#,
    )?;
    db.exec(
        r#"
            INSERT INTO invoice (client_id, total, invoice_date) VALUES
                (101, 55.20, '2023-08-01 09:15:00'),
                (102, 12.75, '2023-08-02 11:30:45'),
                (103, 89.90, '2023-08-03 14:05:10'),
                (104, 10.00, '2023-08-04 10:20:00'),
                (105, 33.30, '2023-08-05 16:45:30'),
                (106, 75.00, '2023-08-06 13:55:15'),
                (107, 9.99,  '2023-08-07 08:00:00');
            "#,
    )?;

    let query = db.exec(
        r#"
            SELECT
                client_id, invoice_id, total, invoice_date
            FROM invoice
            WHERE
                invoice_date BETWEEN '2023-08-02' AND '2023-08-06'
            ORDER BY invoice_date;
            "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                Value::Number(103),
                Value::Number(3),
                Value::Float(89.90),
                temporal!("2023-08-03 14:05:10")?
            ],
            vec![
                Value::Number(105),
                Value::Number(5),
                Value::Float(33.30),
                temporal!("2023-08-05 16:45:30")?
            ],
            vec![
                Value::Number(106),
                Value::Number(6),
                Value::Float(75.00),
                temporal!("2023-08-06 13:55:15")?
            ]
        ]
    );

    Ok(())
}

#[test]
fn select_with_in() -> Result<()> {
    let mut db = State::default();

    db.exec(
        "CREATE TABLE actors (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(50));",
    )?;
    let names = [
        ("Meryl", "Allen"),
        ("Cuba", "Allen"),
        ("Kim", "Allen"),
        ("Jon", "Chase"),
        ("Ed", "Chase"),
        ("Susan", "Davis"),
        ("Jennifer", "Davis"),
        ("Susan", "Davis"),
        ("Alex", "Johnson"),
    ];

    for (name, last_name) in names {
        db.exec(
            format!("INSERT INTO actors (name, last_name) VALUES ('{name}', '{last_name}');")
                .as_str(),
        )?;
    }

    let query = db.exec(
        "SELECT name, last_name FROM actors WHERE last_name IN ('Allen', 'Chase', 'Davis');",
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("Meryl".into()), Value::String("Allen".into())],
            vec![Value::String("Cuba".into()), Value::String("Allen".into())],
            vec![Value::String("Kim".into()), Value::String("Allen".into())],
            vec![Value::String("Jon".into()), Value::String("Chase".into())],
            vec![Value::String("Ed".into()), Value::String("Chase".into())],
            vec![Value::String("Susan".into()), Value::String("Davis".into())],
            vec![
                Value::String("Jennifer".into()),
                Value::String("Davis".into())
            ],
            vec![Value::String("Susan".into()), Value::String("Davis".into())],
        ]
    );

    Ok(())
}

#[test]
fn select_with_like() -> Result<()> {
    let mut db = State::default();
    db.exec(
        "CREATE TABLE customer (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(50));",
    )?;
    db.exec(
        r#"
        INSERT INTO customer (name, last_name) VALUES 
            ('Jennifer', 'Smith'),
            ('Jenny', 'Johnson'),
            ('Benjamin', 'Brown'),
            ('Jessica', 'Jones'),
            ('Jenifer', 'Miller'),
            ('Michael', 'Davis');
            "#,
    )?;
    let query = db.exec("SELECT name, last_name FROM customer WHERE name LIKE 'Jen%';")?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                Value::String("Jennifer".into()),
                Value::String("Smith".into()),
            ],
            vec![
                Value::String("Jenny".into()),
                Value::String("Johnson".into())
            ],
            vec![
                Value::String("Jenifer".into()),
                Value::String("Miller".into())
            ]
        ],
    );

    Ok(())
}

#[test]
fn substring_function() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(70));")?;
    db.exec(
        r#"
            INSERT INTO customers (name) VALUES
            ('Jared'), ('Mary'), ('Patricia'), ('Linda');
            "#,
    )?;

    let query = db.exec("SELECT SUBSTRING(name FROM 1 FOR 1) FROM customers;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("J".into())],
            vec![Value::String("M".into())],
            vec![Value::String("P".into())],
            vec![Value::String("L".into())],
        ]
    );

    let query = db.exec("SELECT SUBSTRING(name FROM 100 FOR 2) FROM customers;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("".into())],
            vec![Value::String("".into())],
            vec![Value::String("".into())],
            vec![Value::String("".into())],
        ]
    );

    let query = db.exec("SELECT SUBSTRING(name FROM 2 FOR 0) FROM customers;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("".into())],
            vec![Value::String("".into())],
            vec![Value::String("".into())],
            vec![Value::String("".into())],
        ]
    );

    let query = db.exec("SELECT SUBSTRING(name FROM 3) FROM customers;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("red".into())],
            vec![Value::String("ry".into())],
            vec![Value::String("tricia".into())],
            vec![Value::String("nda".into())],
        ]
    );

    Ok(())
}

#[test]
fn ascii_function() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(30));")?;
    db.exec(
        r#"
            INSERT INTO users (name) VALUES
            ('Alice'), ('Αλέξανδρος'), ('Zoe'), ('Émile'), ('Chloé');
            "#,
    )?;

    let query = db.exec("SELECT ASCII(name) FROM users ORDER BY name;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(65)],
            vec![Value::Number(67)],
            vec![Value::Number(90)],
            vec![Value::Number(201)],
            vec![Value::Number(913)],
        ]
    );

    Ok(())
}

#[test]
fn concat_function() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
            CREATE TABLE contacts (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(15)
            );
        "#,
    )?;
    db.exec(
        r#"
            INSERT INTO contacts (name, email, phone)
            VALUES
                ('John Doe', 'john.doe@example.com', '123-456-7890'),
                ('Jane Smith', 'jane.smith@example.com', '987-654-3210'),
                ('Bob Johnson', 'bob.johnson@example.com', '555-1234'),
                ('Alice Brown', 'alice.brown@example.com', '555-1235'),
                ('Charlie Davis', 'charlie.davis@example.com', '987-654-3210');
        "#,
    )?;

    let query = db.exec("SELECT CONCAT(name, ' ', '(', email, ')', ' ', phone) FROM contacts;")?;

    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String(
                "John Doe (john.doe@example.com) 123-456-7890".into()
            )],
            vec![Value::String(
                "Jane Smith (jane.smith@example.com) 987-654-3210".into()
            )],
            vec![Value::String(
                "Bob Johnson (bob.johnson@example.com) 555-1234".into()
            )],
            vec![Value::String(
                "Alice Brown (alice.brown@example.com) 555-1235".into()
            )],
            vec![Value::String(
                "Charlie Davis (charlie.davis@example.com) 987-654-3210".into()
            )]
        ]
    );

    Ok(())
}

#[test]
fn position_function() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
            CREATE TABLE films (
                id SERIAL PRIMARY KEY,
                title VARCHAR(100),
                description VARCHAR(255)
            );
        "#,
    )?;

    let inserts = [
            "INSERT INTO films (title, description) VALUES ('The Matrix', 'A computer hacker learns about the true nature of reality.');",
            "INSERT INTO films (title, description) VALUES ('Inception', 'A thief who steals corporate secrets through dream-sharing technology.');",
            "INSERT INTO films (title, description) VALUES ('Interstellar', 'A team of explorers travel through a wormhole in space.');",
            "INSERT INTO films (title, description) VALUES ('The Prestige', 'Two stage magicians engage in a battle to create the ultimate illusion.');",
            "INSERT INTO films (title, description) VALUES ('Memento', 'A man with short-term memory loss attempts to track down his wifes murderer.');"
        ];

    for insert in inserts {
        db.exec(insert)?;
    }

    let query = db.exec("SELECT title, POSITION('the' IN description) FROM films;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("The Matrix".into()), Value::Number(32)],
            vec![Value::String("Inception".into()), Value::Number(0)],
            vec![Value::String("Interstellar".into()), Value::Number(0)],
            vec![Value::String("The Prestige".into()), Value::Number(50)],
            vec![Value::String("Memento".into()), Value::Number(0)],
        ]
    );

    Ok(())
}

#[test]
fn basic_math_functions() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
            CREATE TABLE employees (
                employee_id SERIAL PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                salary DOUBLE PRECISION,
                bonus_percentage REAL,
                tax_deduction DOUBLE PRECISION
            );"#,
    )?;

    db.exec(
            r#"
            INSERT INTO employees (employee_id, first_name, last_name, salary, bonus_percentage, tax_deduction)
            VALUES
            (101, 'John', 'Smith', 75000.00, 15.00, -12500.00),
            (102, 'Sarah', 'Johnson', 68000.50, 12.50, -10200.75),
            (103, 'Michael', 'Williams', 92000.00, 20.00, -18400.00),
            (104, 'Emily', 'Brown', 55000.25, 10.25, -8250.38),
            (105, 'David', 'Jones', 110000.00, 25.00, -27500.00);
        "#,
        )?;

    let query = db.exec("SELECT first_name, salary, TRUNC(SQRT(salary), 2) FROM employees;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec!["John".into(), 75000f64.into(), 273.86f64.into()],
            vec!["Sarah".into(), 68000.50f64.into(), 260.76f64.into()],
            vec!["Michael".into(), 92000f64.into(), 303.31.into()],
            vec!["Emily".into(), 55000.25f64.into(), 234.52f64.into()],
            vec!["David".into(), 110000.00f64.into(), 331.66f64.into()],
        ]
    );

    let query = db.exec("SELECT SIGN(tax_deduction) FROM employees;")?;
    for row in query.tuples {
        assert!(row.iter().all(|i| i.eq(&Value::Number(-1))));
    }

    let query = db.exec("SELECT salary, TRUNC(salary/10000) FROM employees;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![75000f64.into(), 7f64.into()],
            vec![68000.5f64.into(), 6f64.into()],
            vec![92000f64.into(), 9f64.into()],
            vec![55000.25f64.into(), 5f64.into()],
            vec![110000f64.into(), 11f64.into()]
        ]
    );

    let query = db.exec(
        "SELECT TRUNC(POWER(bonus_percentage, 2), 2) FROM employees ORDER BY bonus_percentage;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![105.06f64.into()],
            vec![156.25f64.into()],
            vec![225f64.into()],
            vec![400f64.into()],
            vec![625f64.into()]
        ]
    );

    let query = db.exec("SELECT ABS(SIGN(tax_deduction)) FROM employees;")?;
    for row in query.tuples {
        assert!(row.iter().all(|i| i.eq(&Value::Number(1))));
    }

    Ok(())
}

#[test]
fn count_function() -> Result<()> {
    let mut db = State::default();
    db.exec(
        "CREATE TABLE customer (id SERIAL PRIMARY KEY, name VARCHAR(50), last_name VARCHAR(50));",
    )?;
    db.exec(
        r#"
        INSERT INTO customer (name, last_name) VALUES 
            ('Jennifer', 'Smith'),
            ('Jenny', 'Johnson'),
            ('Benjamin', 'Brown'),
            ('Jessica', 'Jones'),
            ('Jenifer', 'Miller'),
            ('Michael', 'Davis');
            "#,
    )?;
    let query = db.exec("SELECT COUNT(*) FROM customer;")?;
    assert_eq!(query.tuples, vec![vec![Value::Number(6)]]);
    assert_eq!(
        db.exec("SELECT COUNT(name) FROM customer;"),
        db.exec("SELECT COUNT(last_name) FROM customer;")
    );

    Ok(())
}

#[test]
fn count_function_on_empty() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE product (id INT PRIMARY KEY, name VARCHAR(50));")?;

    let query = db.exec("SELECT COUNT(*) FROM product;")?;
    assert_eq!(query.tuples, vec![vec![Value::Number(0)]]);

    assert_eq!(
        db.exec("SELECT COUNT(id) FROM product;")?.tuples,
        vec![vec![Value::Number(0)]]
    );
    Ok(())
}

#[test]
fn aggregate_functions() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE sales (id SERIAL PRIMARY KEY, price DOUBLE PRECISION, quantity INT, category VARCHAR(30));")?;

    db.exec(
        "INSERT INTO sales (id, price, quantity, category) VALUES
            (1, 10.0, 2, 'books'),
            (2, 20.5, 1, 'books'),
            (3, 5.0, 5, 'stationery'),
            (4, 8.2, 3, 'stationery'),
            (5, 100.0, 1, 'electronics');",
    )?;

    assert_eq!(
        db.exec("SELECT SUM(price) FROM sales;")?.tuples,
        vec![vec![Value::Float(143.7)]]
    );
    assert_eq!(
        db.exec("SELECT AVG(price) FROM sales;")?.tuples,
        vec![vec![Value::Float(28.74)]]
    );
    assert_eq!(
        db.exec("SELECT MIN(price) FROM sales;")?.tuples,
        vec![vec![Value::Float(5.0)]]
    );
    assert_eq!(
        db.exec("SELECT MAX(price) FROM sales;")?.tuples,
        vec![vec![Value::Float(100.00)]]
    );

    Ok(())
}

#[test]
fn typeof_function() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(25), active BOOLEAN, salary REAL, hire_date DATE);")?;
    db.exec("INSERT INTO employees (id, name, active, salary, hire_date) VALUES (1, 'Alice', TRUE, 1000.0, '2020-01-01');")?;

    let cases = vec![
        ("id", Type::Integer),
        ("name", Type::Varchar(25)),
        ("active", Type::Boolean),
        ("salary", Type::Real),
        ("hire_date", Type::Date),
    ];

    for (column, r#type) in cases {
        let query = db.exec(&format!("SELECT typeof({column}) FROM employees;"))?;
        assert_eq!(query.tuples, vec![vec![Value::String(r#type.to_string())]])
    }

    Ok(())
}

#[test]
fn group_by() -> Result<()> {
    let mut db = State::default();
    db.exec(
            "CREATE TABLE sales (id SERIAL PRIMARY KEY, region VARCHAR(2), price INT, qty INT UNSIGNED);",
        )?;

    db.exec("INSERT INTO sales (region, price, qty) VALUES ('N', 10, 1);")?;
    db.exec("INSERT INTO sales (region, price, qty) VALUES ('S', 20, 2);")?;
    db.exec("INSERT INTO sales (region, price, qty) VALUES ('N', 15, 3);")?;
    db.exec("INSERT INTO sales (region, price, qty) VALUES ('S', 30, 4);")?;
    db.exec("INSERT INTO sales (region, price, qty) VALUES ('X', 40, 5);")?;

    let query = db.exec("SELECT region, SUM(price) FROM sales GROUP BY region ORDER BY region;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("N".into()), Value::Float(25.0)],
            vec![Value::String("S".into()), Value::Float(50.0)],
            vec![Value::String("X".into()), Value::Float(40.0)],
        ]
    );

    let query = db.exec("SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY region;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("N".into()), Value::Number(2)],
            vec![Value::String("S".into()), Value::Number(2)],
            vec![Value::String("X".into()), Value::Number(1)],
        ]
    );

    let empty: Vec<Vec<Value>> = Vec::new();
    let query = db.exec(
        "SELECT region, SUM(price) FROM sales WHERE price > 100 GROUP BY region ORDER BY region;",
    )?;
    assert_eq!(query.tuples, empty);

    Ok(())
}

#[test]
fn group_by_distinct_behaviour() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE payment (id SERIAL PRIMARY KEY, customer_id INT);")?;
    db.exec("INSERT INTO payment (customer_id) VALUES (1), (2), (1), (3), (2), (2);")?;

    let query =
        db.exec("SELECT customer_id FROM payment GROUP BY customer_id ORDER BY customer_id;")?;

    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1)],
            vec![Value::Number(2)],
            vec![Value::Number(3)]
        ]
    );

    Ok(())
}

#[test]
fn group_by_with_multiple_cols() -> Result<()> {
    let mut db = State::default();
    db.exec(
        "CREATE TABLE payment (id SERIAL PRIMARY KEY, customer_id INT, staff_id INT, amount INT);",
    )?;
    db.exec(
        r#"
        INSERT INTO payment (customer_id, staff_id, amount) VALUES
            (1, 2, 100),
            (2, 2, 200),
            (1, 2, 150),
            (1, 3, 80),
            (2, 2, 120),
            (3, 3, 50),
            (2, 3, 90),
            (1, 2, 60);
        "#,
    )?;

    let query = db.exec(
        r#"
        SELECT customer_id, staff_id, SUM(amount), AVG(amount)
        FROM payment 
        GROUP BY staff_id, customer_id
        ORDER BY customer_id, staff_id;
        "#,
    )?;

    #[rustfmt::skip]
        assert_eq!(
            query.tuples,
            vec![
                vec![Value::Number(1), Value::Number(2), Value::Float(310.0), Value::Float(103.33333333333333)],
                vec![Value::Number(1), Value::Number(3), Value::Float(80.0), Value::Float(80.0)],
                vec![Value::Number(2), Value::Number(2), Value::Float(320.0), Value::Float(160.0)],
                vec![Value::Number(2), Value::Number(3), Value::Float(90.0), Value::Float(90.0)],
                vec![Value::Number(3), Value::Number(3), Value::Float(50.0), Value::Float(50.0)],
            ]
        );
    Ok(())
}

#[test]
fn order_by_desc() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE items (id SERIAL PRIMARY KEY, price INT, name VARCHAR(20));")?;
    db.exec(
        r#"
            INSERT INTO items (price, name)
            VALUES (50, 'banana'), (10, 'apple'), (30, 'cherry');
        "#,
    )?;

    let query = db.exec("SELECT name FROM items ORDER BY name DESC;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("cherry".into())],
            vec![Value::String("banana".into())],
            vec![Value::String("apple".into())]
        ]
    );

    Ok(())
}

#[test]
fn order_by_desc_multiple_columns() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE people (id SERIAL PRIMARY KEY, first_name VARCHAR(20), last_name VARCHAR(20));")?;
    db.exec(
        r#"
        INSERT INTO people (first_name, last_name)
        VALUES
            ('John', 'Smith'),
            ('Alice', 'Smith'),
            ('Bob', 'Adams'),
            ('Mary', 'Baker'),
            ('Jane', 'Adams');
        "#,
    )?;

    let query = db.exec(
        "SELECT first_name, last_name FROM people ORDER BY last_name DESC, first_name DESC;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("John".into()), Value::String("Smith".into())],
            vec![Value::String("Alice".into()), Value::String("Smith".into())],
            vec![Value::String("Mary".into()), Value::String("Baker".into())],
            vec![Value::String("Jane".into()), Value::String("Adams".into())],
            vec![Value::String("Bob".into()), Value::String("Adams".into())],
        ]
    );

    let query = db.exec(
        "SELECT first_name, last_name FROM people ORDER BY last_name ASC, first_name DESC;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("Jane".into()), Value::String("Adams".into())],
            vec![Value::String("Bob".into()), Value::String("Adams".into())],
            vec![Value::String("Mary".into()), Value::String("Baker".into())],
            vec![Value::String("John".into()), Value::String("Smith".into())],
            vec![Value::String("Alice".into()), Value::String("Smith".into())],
        ]
    );

    // assert `ASC` and `DESC` parity
    let query =
        db.exec("SELECT first_name, last_name FROM people ORDER BY last_name, first_name;")?;
    let mut reverse_query = db.exec(
        "SELECT first_name, last_name FROM people ORDER BY last_name DESC, first_name DESC;",
    )?;
    reverse_query.tuples.reverse();

    assert_eq!(query.tuples, reverse_query.tuples);

    Ok(())
}

#[test]
fn select_with_aliases() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(100), salary REAL, bonus REAL);")?;
    db.exec(
        r#"
            INSERT INTO employees (name, salary, bonus)
            VALUES ('Alice', 3000.0, 500.0), ('Bob', 2500.0, 300.0);
        "#,
    )?;

    let query = db.exec("SELECT salary + bonus AS total, name AS employee_name FROM employees;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Float(3500.0), Value::String("Alice".into())],
            vec![Value::Float(2800.0), Value::String("Bob".into())]
        ]
    );

    Ok(())
}

#[test]
fn select_alias_with_group_by() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
            CREATE TABLE sales (
                id SERIAL PRIMARY KEY,
                region VARCHAR(10),
                price INT,
                bonus INT,
                discount INT,
                salesperson VARCHAR(20)
            );
        "#,
    )?;

    db.exec(
        r#"
            INSERT INTO sales (region, price, bonus, discount, salesperson) VALUES
                ('N', 10, 2, 1, 'Alice'),
                ('N', 15, 3, 2, 'Bob'),
                ('S', 20, 5, 0, 'Carol'),
                ('S', 30, 7, 3, 'Dave'),
                ('S', 25, 0, 2, 'Alice'),
                ('N', 8, 1, 1, 'Eve');
        "#,
    )?;

    let query = db.exec(
            "SELECT region, SUM(price + bonus) AS sum_total FROM sales GROUP BY region ORDER BY region;",
        )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("N".to_string()), Value::Float(39.0)],
            vec![Value::String("S".to_string()), Value::Float(87.0)],
        ]
    );

    let query = db.exec(
        r#"
            SELECT region, SUM(price) AS total_price, AVG(bonus) AS avg_bonus, MAX(discount) AS max_discount 
            FROM sales 
            GROUP BY region 
            ORDER BY region;
        "#
        )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                Value::String("N".to_string()),
                Value::Float(33.0),
                Value::Float(2.0),
                Value::Float(2.0),
            ],
            vec![
                Value::String("S".to_string()),
                Value::Float(75.0),
                Value::Float(4.0),
                Value::Float(3.0),
            ],
        ]
    );

    let query = db.exec(
        r#"
            SELECT salesperson, COUNT(*) AS num_sales, SUM(price + bonus - discount) AS net_total 
            FROM sales 
            GROUP BY salesperson 
            ORDER BY salesperson;
        "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                Value::String("Alice".to_string()),
                Value::Number(2),
                Value::Float(34.0)
            ],
            vec![
                Value::String("Bob".to_string()),
                Value::Number(1),
                Value::Float(16.0)
            ],
            vec![
                Value::String("Carol".to_string()),
                Value::Number(1),
                Value::Float(25.0)
            ],
            vec![
                Value::String("Dave".to_string()),
                Value::Number(1),
                Value::Float(34.0)
            ],
            vec![
                Value::String("Eve".to_string()),
                Value::Number(1),
                Value::Float(8.0)
            ],
        ]
    );

    Ok(())
}

#[test]
fn alias_with_order_by() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(30), age INT UNSIGNED);")?;
    db.exec("INSERT INTO users (age, name) VALUES (19, 'John Doe'), (23, 'Mary Dove');")?;

    let query = db.exec("SELECT name as user_name, age FROM users ORDER BY user_name;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::String("John Doe".into()), Value::Number(19)],
            vec![Value::String("Mary Dove".into()), Value::Number(23)]
        ]
    );

    Ok(())
}

#[test]
fn complete_alias_with_group_and_order() -> Result<()> {
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
                temporal!("2018-03-10")?,
            ],
            vec![
                "Marketing".into(),
                2.into(),
                75000f64.into(),
                8.into(),
                temporal!("2019-11-20")?,
            ],
            vec![
                "HR".into(),
                1.into(),
                68000f64.into(),
                6.into(),
                temporal!("2021-01-05")?,
            ]
        ]
    );

    Ok(())
}

#[test]
fn text_column() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE notes (id SERIAL PRIMARY KEY, title VARCHAR(100), content TEXT);")?;
    db.exec(r#"
        INSERT INTO notes (title, content) VALUES
            ('Meeting Notes', 'Discussed project timeline and deliverables. Team agreed on Q2 launch.'),
            ('Ideas', 'Consider adding dark mode to the application. Users have requested this feature.'),
            ('Reminder', 'Call dentist on Monday to schedule annual checkup.');
        "#)?;

    let query = db.exec("SELECT title, content FROM notes WHERE content LIKE '%project%';")?;
    assert_eq!(
        query.tuples,
        vec![vec![
            "Meeting Notes".into(),
            "Discussed project timeline and deliverables. Team agreed on Q2 launch.".into()
        ]]
    );

    Ok(())
}

#[test]
fn text_column_ordering() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);")?;
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Carol');")?;

    let query = db.exec("SELECT name FROM users ORDER BY name DESC;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec!["Carol".into()],
            vec!["Bob".into()],
            vec!["Alice".into()]
        ]
    );

    Ok(())
}

#[test]
fn text_string_functions() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name TEXT);")?;
    db.exec(
        "INSERT INTO users (id, name) VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave');",
    )?;

    let query = db.exec("SELECT SUBSTRING(name FROM 2 FOR 2) FROM users ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec!["li".into()],
            vec!["ob".into()],
            vec!["ar".into()],
            vec!["av".into()]
        ]
    );

    let query = db.exec("SELECT CONCAT(name, '_user') FROM users ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec!["alice_user".into()],
            vec!["bob_user".into()],
            vec!["carol_user".into()],
            vec!["dave_user".into()]
        ]
    );

    let query = db.exec("SELECT ASCII(name) FROM users ORDER BY id;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![('a' as i128).into()],
            vec![('b' as i128).into()],
            vec![('c' as i128).into()],
            vec![('d' as i128).into()],
        ]
    );

    Ok(())
}

#[test]
fn text_column_filtering() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);")?;
    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Carol');")?;

    let query = db.exec("SELECT name FROM users WHERE name LIKE 'A%';")?;
    assert_eq!(query.tuples, vec![vec!["Alice".into()]]);

    Ok(())
}

#[test]
fn nullable_column() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            email VARCHAR(255) UNIQUE,
            phone VARCHAR(15) NULLABLE UNIQUE,
            age SMALLINT UNSIGNED NULLABLE
        );
        "#,
    )?;

    db.exec(
        r#"
        INSERT INTO users (name, email, phone, age)
        VALUES
            ('Alice Smith',  'alice@example.com',   '+15551234567', 33),
            ('Bob Johnson',  'bob@example.com',     '+15559876543', 27),
            ('Carol Perez',  'carol@example.com',   NULL, NULL),
            ('Daniel Silva', 'daniel@example.com',  '+15557654321', NULL);
        "#,
    )?;

    let query = db.exec("SELECT name, phone, age FROM users;")?;
    assert_values(
        &query.tuples,
        &vec![
            vec!["Alice Smith".into(), "+15551234567".into(), 33.into()],
            vec!["Bob Johnson".into(), "+15559876543".into(), 27.into()],
            vec!["Carol Perez".into(), Value::Null, Value::Null],
            vec!["Daniel Silva".into(), "+15557654321".into(), Value::Null],
        ],
    );
    Ok(())
}

#[test]
fn nullable_conditions() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            customer_name VARCHAR(255),
            shipping_notes TEXT NULLABLE,
            priority INTEGER NULLABLE
        );
        "#,
    )?;

    db.exec(
        r#"
        INSERT INTO orders (customer_name, shipping_notes, priority)
        VALUES
            ('Alice', 'Fragile package', 1),
            ('Bob', NULL, NULL),
            ('Carol', 'Leave at door', 2),
            ('Dave', NULL, 1),
            ('Eve', 'Signature required', NULL);
        "#,
    )?;

    let query =
        db.exec("SELECT customer_name, priority FROM orders WHERE shipping_notes IS NOT NULL;")?;
    assert_values(
        &query.tuples,
        &vec![
            vec!["Alice".into(), 1.into()],
            vec!["Carol".into(), 2.into()],
            vec!["Eve".into(), Value::Null],
        ],
    );

    let query = db.exec(
        "SELECT priority FROM orders WHERE priority IS NOT NULL AND shipping_notes IS NOT NULL;",
    )?;
    assert_values(&query.tuples, &vec![vec![1.into()], vec![2.into()]]);

    Ok(())
}

#[test]
fn coalesce_function() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
        CREATE TABLE items (
            id SERIAL PRIMARY KEY,
            product VARCHAR (100),
            price INT,
            discount INT NULLABLE
        );
        "#,
    )?;
    db.exec(
        r#"
    INSERT INTO items (product, price, discount)
    VALUES
        ('A', 1000, 10),
        ('B', 1500, 20),
        ('C', 800, 5),
        ('D', 500, NULL);
    "#,
    )?;

    let query = db.exec("SELECT product, (price - discount) AS net_price FROM items;")?;
    assert_values(
        &query.tuples,
        &vec![
            vec!["A".into(), 990.into()],
            vec!["B".into(), 1480.into()],
            vec!["C".into(), 795.into()],
            vec!["D".into(), Value::Null],
        ],
    );

    let query =
        db.exec("SELECT product, (price - COALESCE(discount, 0)) AS net_price FROM items;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec!["A".into(), 990.into()],
            vec!["B".into(), 1480.into()],
            vec!["C".into(), 795.into()],
            vec!["D".into(), 500.into()],
        ],
    );

    Ok(())
}

#[test]
fn nullable_aggregation() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
    CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NULLABLE,
        department VARCHAR(50) NULLABLE,
        salary DOUBLE PRECISION NULLABLE, 
        bonus DOUBLE PRECISION NULLABLE,
        join_date DATE NULLABLE,
        performance_rating INTEGER NULLABLE
    );
    "#,
    )?;

    db.exec(
        r#"
    INSERT INTO employees (name, department, salary, bonus, join_date, performance_rating) VALUES
        ('Alice Johnson', 'Engineering', 75000.00, 10000.00, '2020-03-15', 5),
        ('Bob Smith', 'Engineering', 68000.00, NULL, '2021-06-20', 4),
        ('Carol Davis', 'Marketing', 62000.00, 5000.00, '2019-11-10', NULL),
        ('David Wilson', 'Sales', 55000.00, 8000.00, '2022-01-05', 3),
        ('Eva Brown', NULL, 72000.00, NULL, '2020-08-30', 5),
        ('Frank Miller', 'Sales', NULL, 3000.00, '2021-09-12', 2),
        ('Grace Lee', 'Engineering', 81000.00, 12000.00, '2018-05-22', 5),
        (NULL, 'Marketing', 58000.00, 4000.00, '2022-03-18', 4),
        ('Henry Taylor', 'HR', 65000.00, NULL, '2019-07-25', NULL),
        ('Ivy Chen', NULL, NULL, NULL, '2023-02-14', NULL);
    "#,
    )?;

    let query = db.exec(
        r#"
        SELECT 
            COUNT(*) AS total_rows,
            COUNT(name) AS non_null_names,
            COUNT(department) AS non_null_departments,
            COUNT(salary) AS non_null_salaries,
            COUNT(bonus) AS non_null_bonuses
        FROM employees;
    "#,
    )?;
    assert_eq!(
        &query.tuples,
        &vec![vec![10.into(), 9.into(), 8.into(), 8.into(), 6.into()]],
    );

    let query = db.exec(
        r#"
        SELECT 
            SUM(salary) AS total_salary,
            AVG(salary) AS avg_salary,
            SUM(COALESCE(salary, 0)) AS total_salary_with_nulls_as_zero,
            AVG(COALESCE(salary, 0)) AS avg_salary_with_nulls_as_zero,
            SUM(bonus) AS total_bonus,
            AVG(bonus) AS avg_bonus_non_null,
            AVG(COALESCE(bonus, 0)) AS avg_bonus_all_rows
        FROM employees;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![vec![
            536000.0.into(),
            67000.0.into(),
            536000.0.into(),
            53600.0.into(),
            42000.0.into(),
            7000.0.into(),
            4200.0.into()
        ]]
    );

    let query = db.exec(
        r#"
    SELECT 
        department,
        COUNT(*) AS employee_count,
        COUNT(salary) AS employees_with_salary,
        TRUNC(AVG(COALESCE(salary, 0)), 1) AS avg_salary,
        SUM(COALESCE(bonus, 0)) AS total_bonus,
        TRUNC(AVG(COALESCE(performance_rating, 0)), 1) AS avg_rating
    FROM employees
    GROUP BY department
    ORDER BY department;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                "Engineering".into(),
                3.into(),
                3.into(),
                74666.6.into(),
                22000.0.into(),
                4.6.into()
            ],
            vec![
                "HR".into(),
                1.into(),
                1.into(),
                65000.0.into(),
                0.0.into(),
                0.0.into()
            ],
            vec![
                "Marketing".into(),
                2.into(),
                2.into(),
                60000.0.into(),
                9000.0.into(),
                2.0.into()
            ],
            vec![
                "Sales".into(),
                2.into(),
                1.into(),
                27500.0.into(),
                11000.0.into(),
                2.5.into()
            ],
            vec![
                Value::Null,
                2.into(),
                1.into(),
                36000.0.into(),
                0.0.into(),
                2.5.into()
            ]
        ]
    );

    let query = db.exec("SELECT MIN(salary), MAX(salary) FROM employees;")?;
    assert_eq!(query.tuples, vec![vec![55000.into(), 81000.into()]]);

    let query = db.exec("SELECT name FROM employees WHERE department = NULL;")?;
    assert!(query.tuples.is_empty());

    let query = db.exec("SELECT name FROM employees WHERE department IS NULL;")?;
    assert_eq!(
        query.tuples,
        vec![vec!["Eva Brown".into()], vec!["Ivy Chen".into()]]
    );

    Ok(())
}

#[test]
fn extract_function() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
    CREATE TABLE events (
        event_id SERIAL PRIMARY KEY,
        event_name VARCHAR(100),
        event_timestamp TIMESTAMP
    );"#,
    )?;

    db.exec(
        r#"
    INSERT INTO events (event_name, event_timestamp) VALUES
        ('Conference A', '2024-03-15 09:00:00'),
        ('Workshop B', '2024-06-22 14:30:00'),
        ('Seminar C', '2024-09-10 11:15:00'),
        ('Conference D', '2024-12-05 10:00:00'),
        ('Workshop E', '2025-02-18 13:45:00');
    "#,
    )?;

    let query = db.exec(
        r#"
    SELECT
        event_name,
        EXTRACT(YEAR FROM event_timestamp) AS year,
        EXTRACT(MONTH FROM event_timestamp) AS month
    FROM events ORDER BY event_timestamp;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Conference A".into(), 2024.into(), 3.into()],
            vec!["Workshop B".into(), 2024.into(), 6.into()],
            vec!["Seminar C".into(), 2024.into(), 9.into()],
            vec!["Conference D".into(), 2024.into(), 12.into()],
            vec!["Workshop E".into(), 2025.into(), 2.into()],
        ]
    );

    let query = db.exec(
        r#"
    SELECT
        EXTRACT(YEAR FROM event_timestamp) AS year,
        EXTRACT(QUARTER FROM event_timestamp) AS quarter,
        COUNT(*) AS event_count
    FROM events
    GROUP BY year, quarter
    ORDER BY year, quarter;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![2024.into(), 1.into(), 1.into()],
            vec![2024.into(), 2.into(), 1.into()],
            vec![2024.into(), 3.into(), 1.into()],
            vec![2024.into(), 4.into(), 1.into()],
            vec![2025.into(), 1.into(), 1.into()],
        ]
    );

    Ok(())
}

#[test]
fn interval_operations() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
    CREATE TABLE events (
        event_id SERIAL PRIMARY KEY,
        event_date DATE,
        event_datetime TIMESTAMP,
        event_time TIME
    );"#,
    )?;

    db.exec(
        r#"
    INSERT INTO events (event_date, event_datetime, event_time) VALUES
        ('2024-01-31', '2024-01-31 23:59:59', '23:59:59'),
        ('2024-02-29', '2024-02-29 00:00:00', '00:00:00'),
        ('2024-12-31', '2024-12-31 12:30:45', '12:30:45'),
        ('2024-03-15', '2024-03-15 18:45:30', '18:45:30'),
        ('2024-06-30', '2024-06-30 13:15:20', '13:15:20');
    "#,
    )?;

    let query = db.exec(
        "SELECT event_id, event_date + INTERVAL '1 month' AS next_month FROM events ORDER BY event_id;",
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1), temporal!("2024-02-29").unwrap()], // jan 31 + 1 month = feb 29
            vec![Value::Number(2), temporal!("2024-03-29").unwrap()], // feb 29 + 1 month = mar 29
            vec![Value::Number(3), temporal!("2025-01-31").unwrap()], // dec 31 + 1 month = jan 31 next year
            vec![Value::Number(4), temporal!("2024-04-15").unwrap()],
            vec![Value::Number(5), temporal!("2024-07-30").unwrap()], // jun 30 + 1 month = jul 30
        ]
    );

    let query = db.exec(
        "SELECT event_id, event_time + INTERVAL '25 hours 75 minutes 100 seconds' AS broken_time FROM events ORDER BY event_id;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1), temporal!("02:16:39").unwrap()],
            vec![Value::Number(2), temporal!("02:16:40").unwrap()], // 00:00:00 + 26h15m40s = 02:16:40
            vec![Value::Number(3), temporal!("14:47:25").unwrap()], // 12:30:45 + 26h15m40s = next day 14:47:25
            vec![Value::Number(4), temporal!("21:02:10").unwrap()], // 18:45:30 + 26h15m40s = next day 21:02:10
            vec![Value::Number(5), temporal!("15:32:00").unwrap()], // 13:15:20 + 26h15m40s = next day 15:32:00
        ]
    );

    let query = db.exec(
        "SELECT event_id, event_date - INTERVAL '35 days' AS past_date FROM events ORDER BY event_id;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1), temporal!("2023-12-27").unwrap()], // jan 31 - 35 days = dec 27 previous year
            vec![Value::Number(2), temporal!("2024-01-25").unwrap()], // feb 29 - 35 days = jan 25
            vec![Value::Number(3), temporal!("2024-11-26").unwrap()], // dec 31 - 35 days = nov 26
            vec![Value::Number(4), temporal!("2024-02-09").unwrap()], // mar 15 - 35 days = feb 9
            vec![Value::Number(5), temporal!("2024-05-26").unwrap()], // jun 30 - 35 days = may 26
        ]
    );

    let query = db.exec(
        "SELECT event_id, event_datetime + INTERVAL '2 months 15 days 27 hours 90 minutes' AS complex_future FROM events ORDER BY event_id;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1), temporal!("2024-04-17 04:29:59").unwrap()],
            vec![Value::Number(2), temporal!("2024-05-15 04:30:00").unwrap()],
            vec![Value::Number(3), temporal!("2025-03-16 17:00:45").unwrap()],
            vec![Value::Number(4), temporal!("2024-05-31 23:15:30").unwrap()],
            vec![Value::Number(5), temporal!("2024-09-15 17:45:20").unwrap()],
        ]
    );

    let query = db.exec(
        "SELECT event_id, event_datetime + INTERVAL '-1 month 15 days -2 hours 30 minutes' AS mixed_interval FROM events ORDER BY event_id;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1), temporal!("2024-01-15 22:29:59").unwrap()],
            vec![Value::Number(2), temporal!("2024-02-12 22:30:00").unwrap()],
            vec![Value::Number(3), temporal!("2024-12-15 11:00:45").unwrap()],
            vec![Value::Number(4), temporal!("2024-03-01 17:15:30").unwrap()],
            vec![Value::Number(5), temporal!("2024-06-14 11:45:20").unwrap()],
        ]
    );

    // let query = db.exec(
    //     "SELECT event_id, event_time + INTERVAL '0.5 seconds' AS micro_time FROM events ORDER BY event_id;",
    // )?;
    // assert_eq!(
    //     query.tuples,
    //     vec![
    //         vec![Value::Number(1), temporal!("23:59:59.5").unwrap()], // 23:59:59 + 0.5s
    //         vec![Value::Number(2), temporal!("00:00:00.5").unwrap()], // 00:00:00 + 0.5s
    //         vec![Value::Number(3), temporal!("12:30:45.5").unwrap()], // 12:30:45 + 0.5s
    //         vec![Value::Number(4), temporal!("18:45:30.5").unwrap()], // 18:45:30 + 0.5s
    //         vec![Value::Number(5), temporal!("13:15:20.5").unwrap()], // 13:15:20 + 0.5s
    //     ]
    // );

    let query = db.exec(
        "SELECT event_id, event_date + INTERVAL '13 months' AS next_year FROM events ORDER BY event_id;",
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1), temporal!("2025-02-28").unwrap()], // jan 31 + 13 months
            vec![Value::Number(2), temporal!("2025-03-29").unwrap()], // feb 29 + 13 months
            vec![Value::Number(3), temporal!("2026-01-31").unwrap()], // dec 31 + 13 months
            vec![Value::Number(4), temporal!("2025-04-15").unwrap()], // mar 15 + 13 months
            vec![Value::Number(5), temporal!("2025-07-30").unwrap()], // jun 30 + 13 months
        ]
    );

    Ok(())
}

#[test]
fn extraction_on_intervals() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
    CREATE TABLE event_intervals (
        id SERIAL PRIMARY KEY,
        event_name VARCHAR(100),
        created_at TIMESTAMP,
        duration INTERVAL,
        time_until_next_event INTERVAL,
        total_processing_time INTERVAL
    );
    "#,
    )?;

    db.exec(r#"
        INSERT INTO event_intervals (event_name, created_at, duration, time_until_next_event, total_processing_time) VALUES
            ('Data Backup', '2024-01-15 08:00:00', '2 hours 30 minutes', '1 day', '15 days'),
            ('System Update', '2024-02-20 14:30:00', '45 minutes', '7 days', '3 months'),
            ('Report Generation', '2024-03-10 09:15:00', '5 minutes', '1 hour', '2 weeks'),
            ('Database Maintenance', '2024-04-05 22:00:00', '6 hours', '1 month', '1 year'),
            ('Quick Sync', '2024-05-12 16:45:30', '30 seconds', '10 minutes', '1 day');
    "#)?;

    let query = db.exec(
        r#"
    SELECT 
        duration,
        EXTRACT(DAY FROM duration) as days,
        EXTRACT(HOUR FROM duration) as hours,
        EXTRACT(MINUTE FROM duration) as minutes,
        EXTRACT(SECOND FROM duration) as seconds,
        EXTRACT(EPOCH FROM duration) as duration_total_seconds
    FROM event_intervals;
    "#,
    )?;

    #[rustfmt::skip]
    assert_eq!(
        query.tuples,
        vec![
            vec![interval!("2 hours 30 minutes"), 0.into(), 2.into(), 30.into(), 0.into(), 9000.into()],
            vec![interval!("45 minutes"), 0.into(), 0.into(), 45.into(), 0.into(), 2700.into()],
            vec![interval!("5 minutes"), 0.into(), 0.into(), 5.into(), 0.into(), 300.into()],
            vec![interval!("6 hours"), 0.into(), 6.into(), 0.into(), 0.into(), 21600.into()],
            vec![interval!("30 seconds"), 0.into(), 0.into(), 0.into(), 30.into(), 30.into()],
        ],
    );

    let query = db.exec(
        r#"
    SELECT 
        time_until_next_event,
        EXTRACT(DAY FROM time_until_next_event) as days_until_next,
        EXTRACT(HOUR FROM time_until_next_event) as hours_until_next,
        total_processing_time,
        EXTRACT(MONTH FROM total_processing_time) as processing_months,
        EXTRACT(DAY FROM total_processing_time) as processing_days
    FROM event_intervals;
    "#,
    )?;

    #[rustfmt::skip]
    assert_eq!(
        query.tuples,
        vec![
            vec![interval!("1 day"), 1.into(), 0.into(), interval!("15 days"), 0.into(), 15.into()],
            vec![interval!("7 days"), 7.into(), 0.into(), interval!("3 months"), 3.into(), 0.into()],
            vec![interval!("1 hour"), 0.into(), 1.into(), interval!("2 weeks"), 0.into(), 14.into()],
            vec![interval!("1 month"), 0.into(), 0.into(), interval!("1 year"), 12.into(), 0.into()],
            vec![interval!("10 minutes"), 0.into(), 0.into(), interval!("1 day"), 0.into(), 1.into()],
        ],
    );

    let query = db.exec(
        r#"
    SELECT 
        duration,
        (created_at + duration) as estimated_end_time,
        EXTRACT(HOUR FROM (created_at + duration)) as end_hour,
        EXTRACT(DAY FROM (created_at + duration)) as end_day
    FROM event_intervals;
    "#,
    )?;

    #[rustfmt::skip]
    assert_eq!(
        query.tuples,
        vec![
            vec![interval!("2 hours 30 minutes"), temporal!("2024-01-15 10:30:00")?, 10.into(), 15.into()],
            vec![interval!("45 minutes"), temporal!("2024-02-20 15:15:00")?, 15.into(), 20.into()],
            vec![interval!("5 minutes"), temporal!("2024-03-10 09:20:00")?, 9.into(), 10.into()],
            vec![interval!("6 hours"), temporal!("2024-04-06 04:00:00")?, 4.into(), 6.into()],
            vec![interval!("30 seconds"), temporal!("2024-05-12 16:46:00")?, 16.into(), 12.into()],
        ],
    );

    Ok(())
}

#[test]
fn simple_join() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE basket_a(a SERIAL PRIMARY KEY, fruit_a VARCHAR (100));")?;
    db.exec("CREATE TABLE basket_b(b SERIAL PRIMARY KEY, fruit_b VARCHAR (100));")?;

    db.exec(
        r#"
    INSERT INTO basket_a (a, fruit_a)
    VALUES
        (1, 'Apple'),
        (2, 'Orange'),
        (3, 'Banana'),
        (4, 'Cucumber');
    "#,
    )?;

    db.exec(
        r#"
    INSERT INTO basket_b (b, fruit_b)
    VALUES
        (1, 'Orange'),
        (2, 'Apple'),
        (3, 'Watermelon'),
        (4, 'Pear');
    "#,
    )?;

    let query = db.exec(
        r#"
    SELECT fruit_a, fruit_b
    FROM basket_a
    INNER JOIN basket_b ON fruit_a = fruit_b;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Apple".into(), "Apple".into()],
            vec!["Orange".into(), "Orange".into()]
        ]
    );

    let query = db.exec(
        r#"
    SELECT fruit_a, fruit_b
    FROM basket_a
    LEFT JOIN basket_b
    ON fruit_a = fruit_b
    ORDER BY fruit_a;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Apple".into(), "Apple".into()],
            vec!["Banana".into(), Value::Null],
            vec!["Cucumber".into(), Value::Null],
            vec!["Orange".into(), "Orange".into()],
        ]
    );

    let query = db.exec(
        r#"
    SELECT fruit_a, fruit_b
    FROM basket_a
    RIGHT JOIN basket_b ON fruit_a = fruit_b
    ORDER BY fruit_b;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Apple".into(), "Apple".into()],
            vec!["Orange".into(), "Orange".into()],
            vec![Value::Null, "Pear".into()],
            vec![Value::Null, "Watermelon".into()],
        ]
    );

    let query = db.exec(
        r#"
    SELECT fruit_a, fruit_b FROM basket_a
    FULL JOIN basket_b ON fruit_a = fruit_b
    ORDER BY fruit_a, fruit_b;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Apple".into(), "Apple".into()],
            vec!["Banana".into(), Value::Null],
            vec!["Cucumber".into(), Value::Null],
            vec!["Orange".into(), "Orange".into()],
            vec![Value::Null, "Pear".into()],
            vec![Value::Null, "Watermelon".into()],
        ]
    );

    Ok(())
}

#[test]
fn multiple_join() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);")?;
    db.exec("CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT, title TEXT);")?;
    db.exec("CREATE TABLE comments (id SERIAL PRIMARY KEY, post_id INT, content TEXT);")?;

    db.exec("INSERT INTO users (name) VALUES ('Alice'), ('Bob');")?;
    db.exec(
        r#"
    INSERT INTO posts (user_id, title) VALUES
        (1, 'Alice Post 1'),
        (1, 'Alice Post 2'),
        (2, 'Bob Post 1');"#,
    )?;
    db.exec(
        r#"
    INSERT INTO comments (post_id, content) VALUES
        (1, 'Nice post!'),
        (1, 'I agree'),
        (3, 'Cool');"#,
    )?;

    let query = db.exec(
        r#"
    SELECT u.name, p.title, c.content FROM users AS u
    JOIN posts AS p ON u.id = p.user_id
    LEFT JOIN comments AS c ON p.id = c.post_id
    ORDER BY u.name;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Alice".into(), "Alice Post 1".into(), "Nice post!".into()],
            vec!["Alice".into(), "Alice Post 1".into(), "I agree".into()],
            vec!["Alice".into(), "Alice Post 2".into(), Value::Null],
            vec!["Bob".into(), "Bob Post 1".into(), "Cool".into()],
        ]
    );

    Ok(())
}

#[test]
fn more_complex_join() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
    CREATE TABLE customers (
        customer_id SERIAL PRIMARY KEY,
        first_name VARCHAR(50),
        last_name VARCHAR(50),
        email VARCHAR(100) UNIQUE
    );
    "#,
    )?;
    db.exec(
        r#"
    CREATE TABLE orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        order_date DATE,
        total_amount REAL,
        status VARCHAR(20)
    );
    "#,
    )?;

    db.exec(
        r#"
    INSERT INTO customers (first_name, last_name, email) VALUES
        ('John', 'Doe', 'john.doe@email.com'),
        ('Jane', 'Smith', 'jane.smith@email.com'),
        ('Bob', 'Johnson', 'bob.johnson@email.com'),
        ('Alice', 'Brown', 'alice.brown@email.com');
    "#,
    )?;
    db.exec(
        r#"
    INSERT INTO orders (customer_id, order_date, total_amount, status) VALUES
        (1, '2024-01-15', 99.99, 'completed'),
        (1, '2024-02-20', 149.50, 'pending'),
        (2, '2024-01-20', 75.25, 'completed'),
        (3, '2024-02-10', 200.00, 'shipped'),
        (2, '2024-02-25', 45.99, 'pending'),
        (1, '2024-03-01', 299.99, 'completed'),
        (4, '2024-02-28', 89.75, 'pending');"#,
    )?;

    let query = db.exec(
        r#"
    SELECT 
        c.first_name,
        c.last_name,
        COUNT(o.order_id) as total_orders,
        TRUNC(SUM(o.total_amount), 2) as total_spent
    FROM customers AS c
    LEFT JOIN orders AS o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
    ORDER BY total_spent DESC;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["John".into(), "Doe".into(), 3.into(), 549.47.into()],
            vec!["Bob".into(), "Johnson".into(), 1.into(), 200.into()],
            vec!["Jane".into(), "Smith".into(), 2.into(), 121.24.into()],
            vec!["Alice".into(), "Brown".into(), 1.into(), 89.75.into()]
        ],
    );

    Ok(())
}

#[test]
fn join_type_coercion() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));")?;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, user_id BIGINT, amount INT);")?;

    db.exec("INSERT INTO users (id, name) VALUES (1, 'Alice');")?;
    db.exec("INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 50);")?;

    let query =
        db.exec("SELECT users.name FROM users JOIN orders ON users.id = orders.user_id;")?;

    assert_eq!(query.tuples, vec![vec![Value::String("Alice".into())]]);

    Ok(())
}

#[test]
#[should_panic]
fn non_equi_join_panic() {
    let mut db = State::default();

    db.exec("CREATE TABLE t1 (val INT PRIMARY KEY);").unwrap();
    db.exec("CREATE TABLE t2 (val INT PRIMARY KEY);").unwrap();

    db.exec("INSERT INTO t1 VALUES (10);").unwrap();
    db.exec("INSERT INTO t2 VALUES (5);").unwrap();

    let _ = db.exec("SELECT t1.val FROM t1 JOIN t2 ON t1.val > t2.val;");
}

#[test]
fn self_join() -> Result<()> {
    let mut db = State::default();

    db.exec(
        "CREATE TABLE employees (id INT PRIMARY KEY, name VARCHAR(50), manager_id INT NULLABLE);",
    )?;
    db.exec(
        r#"
    INSERT INTO employees (id, name, manager_id) VALUES 
        (1, 'The Boss', NULL),
        (2, 'Alice', 1),
        (3, 'Bob', 1),
        (4, 'Charlie', 2);
    "#,
    )?;

    let query = db.exec(
        r#"
    SELECT e.name, m.name 
    FROM employees AS e 
    JOIN employees AS m ON e.manager_id = m.id
    ORDER BY e.name;
    "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                Value::String("Alice".into()),
                Value::String("The Boss".into())
            ],
            vec![
                Value::String("Bob".into()),
                Value::String("The Boss".into())
            ],
            vec![
                Value::String("Charlie".into()),
                Value::String("Alice".into())
            ],
        ]
    );

    db.exec(
        r#"
    CREATE TABLE employee (
        employee_id INT PRIMARY KEY,
        first_name VARCHAR (255),
        last_name VARCHAR (255),
        manager_id INT NULLABLE
    );"#,
    )?;
    db.exec(
        r#"
    INSERT INTO employee (employee_id, first_name, last_name, manager_id)
        VALUES
        (1, 'Windy', 'Hays', NULL),
        (2, 'Ava', 'Christensen', 1),
        (3, 'Hassan', 'Conner', 1),
        (4, 'Anna', 'Reeves', 2),
        (5, 'Sau', 'Norman', 2),
        (6, 'Kelsie', 'Hays', 3),
        (7, 'Tory', 'Goff', 3),
        (8, 'Salley', 'Lester', 3);"#,
    )?;

    let query = db.exec(
        r#"
    SELECT
        CONCAT(e.first_name, ' ', e.last_name) AS employee,
        CONCAT(m.first_name, ' ', m.last_name) AS manager
    FROM employee AS e
    INNER JOIN employee AS m ON m.employee_id = e.manager_id
    ORDER BY manager, employee;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Anna Reeves".into(), "Ava Christensen".into()],
            vec!["Sau Norman".into(), "Ava Christensen".into()],
            vec!["Kelsie Hays".into(), "Hassan Conner".into()],
            vec!["Salley Lester".into(), "Hassan Conner".into()],
            vec!["Tory Goff".into(), "Hassan Conner".into()],
            vec!["Ava Christensen".into(), "Windy Hays".into()],
            vec!["Hassan Conner".into(), "Windy Hays".into()],
        ]
    );

    Ok(())
}

#[test]
fn limit() -> Result<()> {
    let mut db = State::default();

    db.exec(
        r#"
    CREATE TABLE employees (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        department VARCHAR(50),
        salary REAL
    );"#,
    )?;

    db.exec(
        r#"
    INSERT INTO employees (name, department, salary) VALUES
        ('Alice', 'Engineering', 70000),
        ('Bob', 'HR', 50000),
        ('Carol', 'Engineering', 75000),
        ('Dave', 'Marketing', 60000),
        ('Eve', 'Engineering', 72000),
        ('Frank', 'HR', 52000),
        ('Grace', 'Marketing', 58000),
        ('Hank', 'Engineering', 69000),
        ('Ivy', 'HR', 51000),
        ('Jack', 'Marketing', 62000);
        "#,
    )?;

    let query =
        db.exec("SELECT name, department, salary FROM employees ORDER BY salary DESC LIMIT 3;")?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Carol".into(), "Engineering".into(), 75000.into()],
            vec!["Eve".into(), "Engineering".into(), 72000.into()],
            vec!["Alice".into(), "Engineering".into(), 70000.into()],
        ]
    );

    Ok(())
}

#[test]
fn limit_zero() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE items (id SERIAL PRIMARY KEY, name VARCHAR(50));")?;
    db.exec("INSERT INTO items (name) VALUES ('item1'), ('item2'), ('item3');")?;

    let query = db.exec("SELECT name FROM items LIMIT 0;")?;
    assert!(query.tuples.is_empty());

    Ok(())
}

#[test]
fn limit_with_join() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE departments (id SERIAL PRIMARY KEY, name VARCHAR(50));")?;
    db.exec("CREATE TABLE employees (id SERIAL PRIMARY KEY, name VARCHAR(50), dept_id INT);")?;

    db.exec(
        r#"
        INSERT INTO departments (name)
        VALUES ('Engineering'), ('Sales'), ('HR');"#,
    )?;

    db.exec(
        r#"
        INSERT INTO employees (name, dept_id)
        VALUES ('Alice', 1), ('Bob', 1), ('Charlie', 2), ('David', 2), ('Eve', 3);"#,
    )?;

    let query = db.exec(
        r#"
        SELECT employees.name, departments.name
        FROM employees
        JOIN departments ON employees.dept_id = departments.id
        ORDER BY employees.name
        LIMIT 2;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["Alice".into(), "Engineering".into()],
            vec!["Bob".into(), "Engineering".into()]
        ]
    );

    Ok(())
}

#[test]
fn offset() -> Result<()> {
    let mut db = State::default();
    db.exec(
        r#"
    CREATE TABLE weather_data (
        id SERIAL PRIMARY KEY,
        device_id VARCHAR(50),
        temperature DOUBLE PRECISION,
        humidity DOUBLE PRECISION,
        wind_speed DOUBLE PRECISION,
        recorded_at TIMESTAMP
    );"#,
    )?;

    db.exec(
        r#"
    INSERT INTO weather_data (device_id, temperature, humidity, wind_speed, recorded_at) VALUES
        ('device_1', 22.5, 55.0, 12.5, '2023-01-15 08:00:00'),
        ('device_2', 21.0, 60.0, 10.0, '2023-01-15 08:05:00'),
        ('device_1', 23.0, 57.0, 11.0, '2023-01-15 08:10:00'),
        ('device_2', 19.5, 62.0, 8.0, '2023-01-15 08:15:00'),
        ('device_1', 20.0, 59.0, 9.5, '2023-01-15 08:20:00'),
        ('device_3', 24.0, 54.0, 13.0, '2023-01-15 08:25:00'),
        ('device_2', 18.5, 63.0, 7.5, '2023-01-15 08:30:00'),
        ('device_3', 22.0, 55.5, 12.0, '2023-01-15 08:35:00'),
        ('device_1', 21.5, 58.0, 10.5, '2023-01-15 08:40:00'),
        ('device_3', 23.5, 53.0, 13.5, '2023-01-15 08:45:00');"#,
    )?;

    let query = db.exec(
        r#"
        SELECT temperature, humidity, wind_speed, recorded_at
        FROM weather_data OFFSET 5;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![
                24.into(),
                54.into(),
                13.into(),
                temporal!("2023-01-15 08:25:00")?,
            ],
            vec![
                18.5.into(),
                63.into(),
                7.5.into(),
                temporal!("2023-01-15 08:30:00")?
            ],
            vec![
                22.into(),
                55.5.into(),
                12.into(),
                temporal!("2023-01-15 08:35:00")?
            ],
            vec![
                21.5.into(),
                58.into(),
                10.5.into(),
                temporal!("2023-01-15 08:40:00")?
            ],
            vec![
                23.5.into(),
                53.into(),
                13.5.into(),
                temporal!("2023-01-15 08:45:00")?
            ]
        ]
    );

    let query = db.exec(
        r#"
    SELECT temperature, humidity, wind_speed
    FROM weather_data ORDER BY recorded_at DESC LIMIT 5 OFFSET 5;"#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec![20.into(), 59.into(), 9.5.into()],
            vec![19.5.into(), 62.into(), 8.into()],
            vec![23.into(), 57.into(), 11.into()],
            vec![21.into(), 60.into(), 10.into()],
            vec![22.5.into(), 55.into(), 12.5.into()]
        ]
    );

    Ok(())
}

#[test]
fn pagination_pattern() -> Result<()> {
    let mut db = State::default();
    db.exec("CREATE TABLE data (id SERIAL PRIMARY KEY, value INT);")?;
    db.exec(
        r#"
        INSERT INTO data (value)
        VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
        "#,
    )?;

    let query = db.exec("SELECT value FROM data LIMIT 3 OFFSET 0;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(1)],
            vec![Value::Number(2)],
            vec![Value::Number(3)]
        ]
    );

    let query = db.exec("SELECT value FROM data LIMIT 3 OFFSET 3;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(4)],
            vec![Value::Number(5)],
            vec![Value::Number(6)]
        ]
    );

    let query = db.exec("SELECT value FROM data LIMIT 3 OFFSET 6;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(7)],
            vec![Value::Number(8)],
            vec![Value::Number(9)]
        ]
    );

    let query = db.exec("SELECT value FROM data LIMIT 3 OFFSET 9;")?;
    assert_eq!(query.tuples, vec![vec![Value::Number(10)]]);

    let query = db.exec("SELECT value FROM data LIMIT 3 OFFSET 10;")?;
    assert!(query.tuples.is_empty());

    Ok(())
}

#[test]
fn index_nested_loop_join() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50));")?;
    db.exec("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT);")?;
    db.exec("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');")?;
    db.exec(
        "INSERT INTO orders (id, user_id, amount) VALUES (100, 1, 50), (101, 1, 75), (102, 2, 100);",
    )?;

    let query = db.exec(
        r#"
        SELECT orders.amount, users.name 
        FROM orders 
        JOIN users ON orders.user_id = users.id 
        ORDER BY orders.id;"#,
    )?;
    assert_eq!(
        query.tuples,
        vec![
            vec![Value::Number(50), Value::String("Alice".into())],
            vec![Value::Number(75), Value::String("Alice".into())],
            vec![Value::Number(100), Value::String("Bob".into())],
        ]
    );

    db.exec("INSERT INTO orders (id, user_id, amount) VALUES (103, 99, 200);")?;
    let query = db.exec(
        r#"
        SELECT orders.amount, users.name 
        FROM orders 
        LEFT JOIN users ON orders.user_id = users.id 
        WHERE orders.id = 103;"#,
    )?;
    assert_eq!(query.tuples, vec![vec![Value::Number(200), Value::Null]]);

    Ok(())
}

#[test]
fn test_complex_join_integrity_scenario() -> Result<()> {
    let mut db = State::default();

    db.exec("CREATE TABLE authors (id SERIAL PRIMARY KEY, name VARCHAR(100));")?;
    db.exec("CREATE TABLE books (id SERIAL PRIMARY KEY, author_id INT REFERENCES authors(id), title VARCHAR(100), year INT);")?;
    db.exec("INSERT INTO authors (id, name) VALUES (1, 'J.R.R. Tolkien'), (2, 'George R.R. Martin'), (3, 'Patrick Rothfuss');")?;
    db.exec("INSERT INTO books (author_id, title, year) VALUES (1, 'The Hobbit', 1937);")?;
    db.exec("INSERT INTO books (author_id, title, year) VALUES (1, 'The Fellowship of the Ring', 1954);")?;
    db.exec("INSERT INTO books (author_id, title, year) VALUES (2, 'A Game of Thrones', 1996);")?;
    assert!(db
        .exec("INSERT INTO books (author_id, title, year) VALUES (99, 'Unknown', 2000);")
        .is_err());

    let query = db.exec(
        r#"
        SELECT a.name, b.title 
        FROM authors AS a 
        JOIN books AS b ON a.id = b.author_id 
        WHERE a.id = 1
        ORDER BY b.year;
        "#,
    )?;

    assert_eq!(
        query.tuples,
        vec![
            vec!["J.R.R. Tolkien".into(), "The Hobbit".into()],
            vec!["J.R.R. Tolkien".into(), "The Fellowship of the Ring".into()]
        ]
    );

    let query = db.exec(
        r#"
        SELECT a.name, b.title 
        FROM authors AS a 
        LEFT JOIN books AS b ON a.id = b.author_id 
        WHERE a.name = 'Patrick Rothfuss';
        "#,
    )?;
    assert_eq!(
        query.tuples,
        vec![vec!["Patrick Rothfuss".into(), Value::Null]]
    );

    Ok(())
}
