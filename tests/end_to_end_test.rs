use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    thread,
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
    fn new() -> Self {
        let port = {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.local_addr().unwrap().port()
        };

        let path = std::env::temp_dir().join(format!("umbra_test_{}.db", port));
        let _ = std::fs::remove_file(&path);

        let db_path = path.clone();
        thread::spawn(move || {
            let addr = SocketAddr::from(([127, 0, 0, 1], port));
            umbra::tcp::server::start(addr, db_path).unwrap()
        });

        Self { path, port }
    }

    fn client(&self) -> Client {
        let addr = SocketAddr::from(([127, 0, 0, 1], self.port));

        loop {
            match TcpStream::connect(addr) {
                Ok(stream) => {
                    stream.set_nodelay(true).expect("Failed to set nodelay");
                    return Client { stream };
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
