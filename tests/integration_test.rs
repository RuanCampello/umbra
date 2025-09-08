use std::path::Path;
use umbra::db::{Database, DatabaseError, QuerySet};
use umbra::sql::statement::{Type, Value};
use umbra::temporal;

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
            ('Daniel Silva', 'daniel@example.com',  '+15557654321', 'NULL');
        "#,
    )?;

    let query = db.exec("SELECT name, phone, age FROM users;")?;
    assert_eq!(
        query.tuples,
        vec![
            vec!["Alice Smith".into(), "+15551234567".into(), 33.into()],
            vec!["Bob Johnson".into(), "+15559876543".into(), 27.into()],
            vec!["Carol Perez".into(), Value::Null, Value::Null],
            vec!["Daniel Silva".into(), "+15557654321".into(), Value::Null],
        ]
    );
    Ok(())
}
