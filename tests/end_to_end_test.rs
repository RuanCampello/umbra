use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use umbra::{
    db::QuerySet,
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

        let server = Self {
            path: db_path,
            port,
        };

        let mut client = server.client();
        let full_path = Path::new(file!()).parent().unwrap().join(path);
        let full_path = std::fs::canonicalize(full_path)
            .unwrap_or_else(|_| panic!("Script file not found: {:#?}", path));

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
