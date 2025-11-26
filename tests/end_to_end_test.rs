use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use umbra::tcp::{self, Response};

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

        let response = client.exec(&format!("SOURCE '{}';", full_path.display()));

        if let Response::Err(e) = response {
            panic!("Failed to source script '{}': {}", path, e);
        }

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
    fn exec(&mut self, query: &str) -> Response {
        self.stream
            .write_all(&(query.len() as u32).to_le_bytes())
            .unwrap();

        self.stream.write_all(query.as_bytes()).unwrap();

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).unwrap();
        let len = u32::from_le_bytes(len) as usize;

        let mut content = vec![0u8; len];
        self.stream.read_exact(&mut content).unwrap();

        tcp::deserialize(&content).unwrap()
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

    let response = db.exec(
        r#"
        SELECT 
            e.employee_id,
            e.first_name,
            e.last_name,
            e.salary,
            d.department_name,
            d.location
        FROM employees AS e
        JOIN departments AS d ON e.department_id = d.department_id
        ORDER BY e.employee_id;"#,
    );

    assert!(matches!(response, Response::QuerySet(_)));
}
