use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use umbra::{
    db::{Numeric, QuerySet},
    sql::statement::Value,
    tcp::{self, Response},
};

struct State {
    path: PathBuf,
    port: u16,
}

struct Client {
    stream: TcpStream,
}

impl State {
    fn new(path: &str) -> Self {
        let server = Self::default();
        let full_path = Path::new(file!()).parent().unwrap().join(path);
        let full_path = std::fs::canonicalize(&full_path)
            .unwrap_or_else(|_| panic!("Script file not found: {:#?}", full_path));

        let mut client = server.client();

        client.exec(&format!("SOURCE '{}';", full_path.display()));
        server
    }

    fn client(&self) -> Client {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));
        let mut retries = 50;

        loop {
            match TcpStream::connect(addr) {
                Ok(stream) => {
                    stream.set_nodelay(true).expect("Failed to set nodelay");
                    return Client { stream };
                }
                Err(_) if retries > 0 => {
                    retries -= 1;
                    thread::sleep(Duration::from_millis(20));
                }
                Err(e) => panic!("Failed to connect to server at {addr}: {e}"),
            }
        }
    }
}

impl Default for State {
    fn default() -> Self {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };

        let db_path = std::env::temp_dir().join(format!("umbra_test_{port}.db"));

        let _ = std::fs::remove_file(&db_path);
        let server_db_path = db_path.clone();

        thread::spawn(move || {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            umbra::tcp::server::start(addr, server_db_path).unwrap()
        });

        Self {
            path: db_path,
            port,
        }
    }
}

impl Client {
    fn exec(&mut self, query: &str) -> umbra::db::QuerySet {
        self.stream
            .write_all(&(query.len() as u32).to_le_bytes())
            .unwrap();

        self.stream.write_all(query.as_bytes()).unwrap();

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).unwrap();
        let len = u32::from_le_bytes(len) as usize;

        let mut content = vec![0u8; len];
        self.stream.read_exact(&mut content).unwrap();

        match tcp::deserialize(&content).unwrap() {
            Response::QuerySet(tuples) => tuples,
            _ => QuerySet::default(),
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

fn numeric(val: i128, scale: u16) -> Value {
    Value::Numeric(Numeric::from_scaled_i128(val, scale).unwrap())
}

#[test]
fn join_over_tcp() {
    let server = State::new("sql/employees-and-departments.sql");
    let mut db = server.client();

    let query = db.exec(
        r#"
        SELECT 
            CONCAT(e.first_name, ' ', e.last_name) as name,
            e.salary,
            d.department_name,
            d.location
        FROM employees AS e
        JOIN departments AS d ON e.department_id = d.department_id
        ORDER BY e.employee_id
        LIMIT 3;"#,
    );

    assert_eq!(
        query.tuples,
        vec![
            vec![
                "John Doe".into(),
                75000.into(),
                "Engineering".into(),
                "New York".into()
            ],
            vec![
                "Jane Smith".into(),
                65000.into(),
                "Sales".into(),
                "Chicago".into()
            ],
            vec![
                "Bob Johnson".into(),
                80000.into(),
                "Engineering".into(),
                "New York".into()
            ]
        ]
    )
}

/// this should be valid until we make the core engine non-blocking with a MVCC
#[test]
fn transaction_blocks_concurrent_reads() {
    let server = State::new("sql/employees-and-departments.sql");
    let mut client_a = server.client();

    let (tx, rx) = std::sync::mpsc::channel();

    let handler = std::thread::spawn(move || {
        client_a.exec("BEGIN TRANSACTION;");
        tx.send(()).unwrap();

        client_a.exec(
            r#"
            INSERT INTO departments (department_id, department_name, location)
            VALUES (5, 'Accounting', 'Trenton');
            "#,
        );

        std::thread::sleep(std::time::Duration::from_millis(500));
        client_a.exec("COMMIT;");
    });
    rx.recv().expect("Client A failed to start transaction");

    let mut client_b = server.client();
    let start = std::time::Instant::now();

    let query = client_b.exec("SELECT * FROM departments WHERE location = 'Trenton';");
    let duration = start.elapsed();

    assert!(duration >= std::time::Duration::from_millis(500));
    assert_eq!(query.tuples[0][1], "Accounting".into());

    handler.join().unwrap();
}

#[test]
fn concurrent_inserts_are_atomic() {
    let server = State::default();
    let mut setup_client = server.client();
    setup_client.exec("CREATE TABLE traffic (id SERIAL PRIMARY KEY, thread_id INT);");

    let mut handles = vec![];
    for i in 0..10 {
        let mut client = server.client();

        handles.push(thread::spawn(move || {
            for _ in 0..50 {
                client.exec(&format!("INSERT INTO traffic (thread_id) VALUES ({});", i));
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    let mut verifier = server.client();

    let query = verifier.exec("SELECT count(*) FROM traffic;");
    assert_eq!(query.tuples[0][0], 500.into());

    let query = verifier.exec("SELECT id FROM traffic;");

    let original_tuples = query.tuples;
    let mut sorted_tuples = original_tuples.clone();
    sorted_tuples.sort_unstable();

    assert_eq!(original_tuples, sorted_tuples)
}

#[test]
fn numeric_joins_aggregation() {
    let server = State::new("sql/orders-and-users.sql");
    let mut db = server.client();

    let query = db.exec(
        r#"
        SELECT 
            p.category, 
            SUM(o.total_price) as category_total 
        FROM orders AS o 
        JOIN products AS p ON o.product_id = p.id 
        GROUP BY p.category 
        ORDER BY category_total DESC;
        "#,
    );

    let categories: [(&str, i128); 8] = [
        ("Automotive", 2350722),
        ("Beauty", 2108030),
        ("Sports", 2103120),
        ("Garden", 2083935),
        ("Clothing", 1931961),
        ("Electronics", 1886387),
        ("Home", 1808070),
        ("Grocery", 1783215),
    ];

    for (idx, (category, total)) in categories.iter().enumerate() {
        assert_eq!(
            query.tuples[idx],
            vec![Value::from(*category), numeric(*total, 2)]
        )
    }
}

#[test]
fn analytics_reporting() {
    let server = State::new("sql/orders-and-users.sql");
    let mut db = server.client();

    let query = db.exec(
        r#"
        SELECT 
            p.category, 
            SUM(o.total_price) as total_revenue,
            AVG(o.quantity) as avg_quantity,
            COUNT(o.id) as order_count
        FROM orders AS o
        JOIN users AS u ON o.user_id = u.id
        JOIN products AS p ON o.product_id = p.id
        WHERE u.country = 'USA' OR u.country = 'UK' AND o.order_date >= '2023-01-01'
        GROUP BY p.category
        ORDER BY total_revenue DESC;
        "#,
    );

    let results: [(&str, i128, i128, i128); 8] = [
        ("Beauty", 757640, 41666666666666667, 6),
        ("Automotive", 730903, 31111111111111111, 9),
        ("Sports", 729063, 42000000000000000, 5),
        ("Grocery", 489123, 43333333333333333, 3),
        ("Electronics", 309454, 30000000000000000, 4),
        ("Home", 280108, 27500000000000000, 4),
        ("Clothing", 173060, 23333333333333333, 3),
        ("Garden", 130526, 20000000000000000, 2),
    ];

    for (idx, (category, revenue, avg, count)) in results.iter().enumerate() {
        assert_eq!(
            query.tuples[idx],
            vec![
                Value::from(*category),
                numeric(*revenue, 2),
                numeric(*avg, 16),
                Value::from(*count)
            ]
        );
    }
}

#[test]
fn users_metadata() {
    let server = State::new("sql/users-json.sql");
    let mut db = server.client();

    let query = db.exec(
        r#"
        SELECT name, metadata.age AS age, metadata.city AS city
        FROM users
        WHERE metadata.active = true
        ORDER BY name DESC
        LIMIT 5;"#,
    );
    assert!(!query.tuples.is_empty());
    assert_eq!(
        query.tuples,
        vec![
            vec!["Sven".into(), 49.into(), "Amsterdam".into()],
            vec!["Rafael".into(), 37.into(), "São Paulo".into()],
            vec!["Paulo".into(), 50.into(), "São Paulo".into()],
            vec!["Olivia".into(), 24.into(), "London".into()],
            vec!["Oliver".into(), 44.into(), "London".into()],
        ]
    );

    let query = db.exec(
        r#"
        SELECT name, metadata.age AS age, metadata.city AS city
        FROM users ORDER BY age DESC LIMIT 3;
        "#,
    );
    assert!(!query.tuples.is_empty());
    assert_eq!(query.tuples.len(), 3);
    assert_eq!(
        query.tuples,
        vec![
            vec!["Paulo".into(), 50.into(), "São Paulo".into()],
            vec!["Sven".into(), 49.into(), "Amsterdam".into()],
            vec!["Nathan".into(), 48.into(), "Toronto".into()],
        ]
    );

    let query = db.exec(
        r#"
        SELECT name, metadata.city AS city
        FROM users
        WHERE metadata.profile.lang = 'pt' AND metadata.age >= 18;"#,
    );
    assert!(!query.tuples.is_empty());
    assert_eq!(
        query.tuples,
        vec![
            vec!["Ivan".into(), "São Paulo".into()],
            vec!["Mateus".into(), "São Paulo".into()],
            vec!["Rafael".into(), "São Paulo".into()],
        ]
    );

    let query = db.exec(
        r#"
        SELECT name, metadata.profile.lang as lang
        FROM users
        WHERE metadata.profile.lang IS NOT NULL
        ORDER BY name;"#,
    );
    assert_eq!(
        query.tuples,
        vec![
            vec!["Alice".into(), "en".into()],
            vec!["Bob".into(), "en".into()],
            vec!["Daniel".into(), "en".into()],
            vec!["Dave".into(), "en".into()],
            vec!["Frank".into(), "jp".into()],
            vec!["Ivan".into(), "pt".into()],
            vec!["Liam".into(), "de".into()],
            vec!["Mateus".into(), "pt".into()],
            vec!["Rafael".into(), "pt".into()],
        ]
    );

    let query = db.exec("SELECT metadata.non_existent_field FROM users LIMIT 1;");
    assert_eq!(query.tuples[0][0], Value::Null);
}

#[test]
fn project_requests() {
    let server = State::new("sql/user-requests.sql");
    let mut db = server.client();

    let query = db.exec("SELECT * FROM requests WHERE priority < 'critical';");
    println!("{query}")
}
