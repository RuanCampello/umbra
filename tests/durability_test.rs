//! Durability of the real server binary: it is killed with SIGKILL under load and restarted

use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::Path,
    thread,
    time::Duration,
};
use umbra::{
    sql::statement::Value,
    tcp::{self, Response},
};

struct Client {
    stream: TcpStream,
}

/// The real server binary on its own port and directory
struct Process {
    child: std::process::Child,
    port: u16,
}

impl Process {
    fn start(dir: &Path, port: u16, budget: Option<usize>) -> Self {
        let child = std::process::Command::new(env!("CARGO_BIN_EXE_umbra"))
            .arg(dir)
            .arg(port.to_string())
            .args(budget.map(|chunks| chunks.to_string()))
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("server binary starts");

        Self { child, port }
    }

    fn client(&self) -> Client {
        connect(self.port)
    }
}

fn connect(port: u16) -> Client {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let mut retries = 250;

    loop {
        match TcpStream::connect(addr) {
            Ok(stream) => {
                stream.set_nodelay(true).expect("Failed to set nodelay");
                return Client { stream };
            },
            _ if retries > 0 => {
                retries -= 1;
                thread::sleep(Duration::from_millis(20));
            },
            Err(e) => panic!("Failed to connect to server at {addr}: {e}"),
        }
    }
}

impl Client {
    fn exec(&mut self, query: &str) -> Vec<Vec<Value>> {
        match self.try_exec(query).expect("server is up") {
            Ok(Response::QuerySet(set)) => set.tuples,
            Ok(_) => Vec::new(),
            Err(message) => panic!("{query}: {message}"),
        }
    }

    /// `None` once the server is gone
    /// `Some(Err)` for a statement the server refused
    fn try_exec(&mut self, query: &str) -> Option<Result<Response, String>> {
        self.stream.write_all(&(query.len() as u32).to_le_bytes()).ok()?;
        self.stream.write_all(query.as_bytes()).ok()?;

        let mut len = [0u8; 4];
        self.stream.read_exact(&mut len).ok()?;
        let mut content = vec![0u8; u32::from_le_bytes(len) as usize];
        self.stream.read_exact(&mut content).ok()?;

        Some(match tcp::deserialize(&content).unwrap() {
            Response::Err(message) => Err(message),
            response => Ok(response),
        })
    }
}

#[test]
fn killed_server_keeps_every_acknowledged_commit_and_no_partial_one() {
    // one test, run in turn: parallel tests would race each other's port probes across forks
    kill_under_load(None, Duration::from_millis(500));
    // a 2-chunk memory budget, long enough for maintenance to evict under load
    kill_under_load(Some(2), Duration::from_millis(2500));
}

fn kill_under_load(budget: Option<usize>, load: Duration) {
    // below the kernel's ephemeral range, away from ports other programs are handed
    let port = (0..10_000u16)
        .map(|n| 20_000 + (std::process::id() as u16 % 10_000 + n) % 10_000)
        .find(|&port| std::net::TcpListener::bind(("127.0.0.1", port)).is_ok())
        .expect("a free port below the ephemeral range");
    let dir = std::env::temp_dir().join(format!("umbra_kill_{port}.db"));
    let _ = std::fs::remove_dir_all(&dir);

    let mut server = Process::start(&dir, port, budget);
    server.client().exec("CREATE TABLE pairs (id BIGINT PRIMARY KEY, txn BIGINT);");

    let acknowledged = std::sync::Mutex::new(Vec::new());
    thread::scope(|scope| {
        for worker in 0..4i64 {
            let (mut client, acknowledged) = (server.client(), &acknowledged);
            scope.spawn(move || {
                for n in 0.. {
                    let txn = worker * 1_000_000 + n;
                    let statements = [
                        "BEGIN;".to_string(),
                        format!("INSERT INTO pairs (id, txn) VALUES ({}, {txn});", 2 * txn),
                        format!("INSERT INTO pairs (id, txn) VALUES ({}, {txn});", 2 * txn + 1),
                        "COMMIT;".to_string(),
                    ];

                    for statement in &statements {
                        match client.try_exec(statement) {
                            Some(Ok(_)) => {},
                            _ => return,
                        }
                    }
                    acknowledged.lock().unwrap().push(txn);
                }
            });
        }

        thread::sleep(load);
        server.child.kill().unwrap();
        server.child.wait().unwrap();
    });

    if budget.is_some() {
        let evicted = std::fs::read_dir(dir.join("cold"))
            .into_iter()
            .flatten()
            .flatten()
            .flat_map(|table| std::fs::read_dir(table.path()).into_iter().flatten())
            .count();
        assert!(evicted > 0, "the budget run never evicted a chunk");
    }

    let acknowledged = acknowledged.into_inner().unwrap();
    assert!(!acknowledged.is_empty(), "no commit before the kill");

    let mut restarted = Process::start(&dir, port, budget);
    let rows = restarted.client().exec("SELECT txn FROM pairs;");
    restarted.child.kill().unwrap();
    restarted.child.wait().unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    let mut counts = std::collections::HashMap::new();
    for row in rows {
        let Value::Number(txn) = row[0] else {
            panic!("txn is a number")
        };
        *counts.entry(txn as i64).or_insert(0) += 1;
    }

    assert!(counts.values().all(|&count| count == 2), "a transaction survived partially");
    assert!(
        acknowledged.iter().all(|txn| counts.contains_key(txn)),
        "an acknowledged commit was lost"
    );
}
