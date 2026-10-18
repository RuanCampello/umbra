use std::{
    io::{Read, Write},
    mem,
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
};

use crate::{
    db::{Budget, DatabaseError, MvccDatabase},
    storage::engine::Engine,
    tcp::protocol::{self, Response},
};

/// Connections served at once, PostgreSQL's default `max_connections`: past it a client is told so and disconnected, instead of waiting unanswered
const MAX_CONNECTIONS: usize = 100;

/// starts the database rooted at the given directory and listens on the given address
pub fn start<Dir: AsRef<Path>>(address: SocketAddr, dir: Dir) -> Result<(), DatabaseError> {
    start_with_budget(address, dir, None)
}

/// [start] keeping table data in memory within `budget` (by default half the physical memory)
pub fn start_with_budget<Dir: AsRef<Path>>(
    address: SocketAddr,
    dir: Dir,
    budget: Option<Budget>,
) -> Result<(), DatabaseError> {
    // the engine is opened once and shared: every connection runs its own
    // session over it, so transactions no longer serialise on a global lock
    // a static lifetime is fine, the engine lives until the process exits
    let engine: &'static Engine = &*Box::leak(Box::new(MvccDatabase::open_engine(&dir, budget)?));
    println!("Database initialised on {}", dir.as_ref().display());

    let listener = TcpListener::bind(address)?;
    println!("Listening on {address}");

    static CONNECTED: AtomicUsize = AtomicUsize::new(0);
    for stream in listener.incoming() {
        let Ok(mut stream) = stream else {
            continue;
        };

        if CONNECTED.fetch_add(1, Ordering::AcqRel) >= MAX_CONNECTIONS {
            CONNECTED.fetch_sub(1, Ordering::AcqRel);
            let refusal = format!("too many connections (at most {MAX_CONNECTIONS})");
            let _ = stream.write_all(
                &protocol::serialize(&Response::Err(refusal)).expect("an error encodes"),
            );
            continue;
        }

        thread::spawn(move || {
            let served = stream
                .set_nodelay(true)
                .map_err(DatabaseError::Io)
                .and_then(|_| handle(&mut stream, engine.clone()));
            if let Err(err) = served {
                eprintln!(
                    "Error on thread {:?} while processing connection: {err:#?}",
                    thread::current().id()
                )
            }
            CONNECTED.fetch_sub(1, Ordering::AcqRel);
        });
    }

    Ok(())
}

fn handle(stream: &mut TcpStream, engine: Engine) -> Result<(), DatabaseError> {
    let connection = stream.peer_addr().unwrap().to_string();
    println!("Connection from: {connection}");

    let mut db = MvccDatabase::connect(engine);
    let mut content_buff_len = [0; mem::size_of::<u32>()];

    loop {
        let mut content_buff = Vec::new();

        let result = stream.read_exact(&mut content_buff_len).and_then(|_| {
            let content_len = u32::from_le_bytes(content_buff_len);
            content_buff.resize(content_len as usize, 0);
            stream.read_exact(&mut content_buff)
        });

        if result.is_err() {
            break;
        }

        let statement = match String::from_utf8(content_buff) {
            Ok(string) => string,
            Err(err) => {
                let packet =
                    protocol::serialize(&Response::Err(format!("UTF-8 decode error: {err}")));
                stream.write_all(&packet.unwrap())?;
                continue;
            },
        };

        let result = db.exec(&statement);

        match protocol::serialize(&Response::from(result)) {
            Ok(packet) => stream.write_all(&packet)?,
            Err(err) => {
                let packet = protocol::serialize(&Response::Err(format!(
                    "Could not encode response: {err}"
                )));
                stream.write_all(&packet.unwrap())?;

                if db.active_transaction() {
                    db.rollback()?;
                }
            },
        };
    }

    println!("Close {connection} connection");
    if db.active_transaction() {
        println!(
            "Connection {connection} closed in the middle of a transaction. Running rollback."
        );
        db.rollback()?;
    }

    Ok(())
}
