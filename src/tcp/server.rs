use std::{
    io::{Read, Write},
    mem,
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    sync::Arc,
    thread,
};

use crate::{
    db::{DatabaseError, MvccDatabase},
    storage::mvcc::engine::Engine,
    tcp::{
        pool::ThreadPool,
        protocol::{self, Response},
    },
};

/// Starts the database rooted at the given directory and listens on the given address.
pub fn start<Dir: AsRef<Path>>(address: SocketAddr, dir: Dir) -> Result<(), DatabaseError> {
    // the engine is opened once and shared: every connection runs its own
    // session over it, so transactions no longer serialise on a global lock.
    // a static lifetime is fine, the engine lives until the process exits
    let engine: &'static Arc<Engine> = &*Box::leak(Box::new(MvccDatabase::open_engine(&dir)?));
    println!("Database initialised on {}", dir.as_ref().display());

    let pool = ThreadPool::new(8);
    let listener = TcpListener::bind(address)?;
    println!("Listening on {address}");

    listener.incoming().for_each(|stream| {
        pool.execute(|| {
            let stream = &mut stream.unwrap();
            stream.set_nodelay(true).unwrap();

            if let Err(err) = handle(stream, Arc::clone(engine)) {
                eprintln!(
                    "Error on thread {:?} while processing connection: {err:#?}",
                    thread::current().id()
                )
            }
        })
    });

    Ok(())
}

fn handle(stream: &mut TcpStream, engine: Arc<Engine>) -> Result<(), DatabaseError> {
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
            }
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
            }
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
