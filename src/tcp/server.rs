use std::{
    fs::File,
    io::{Read, Write},
    mem,
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    sync::{Mutex, MutexGuard},
    thread,
};

use crate::{
    db::{Database, DatabaseError},
    tcp::{
        pool::ThreadPool,
        protocol::{self, Response},
    },
};

/// Starts the database at the given file path and listens on the given address.
pub fn start<File: AsRef<Path>>(address: SocketAddr, file: File) -> Result<(), DatabaseError> {
    // it's alright have a static lifetime here because... well, the database will live forever
    // until the program exit :)
    let mut db = &*Box::leak(Box::new(Mutex::new(Database::init(&file)?)));
    println!("Database file initialised on {}", file.as_ref().display());

    let pool = ThreadPool::new(8);
    let listener = TcpListener::bind(address)?;
    println!("Listening on {address}");

    listener.incoming().for_each(|stream| {
        pool.execute(|| {
            if let Err(err) = handle(&mut stream.unwrap(), db) {
                eprintln!(
                    "Error on thread {:?} while processing connection: {err:#?}",
                    thread::current().id()
                )
            }
        })
    });

    Ok(())
}

fn handle(stream: &mut TcpStream, db: &'static Mutex<Database<File>>) -> Result<(), DatabaseError> {
    let connection = stream.peer_addr().unwrap().to_string();
    println!("Connection from: {connection}");

    let mut content_buff_len = [0; mem::size_of::<u32>()];
    let mut guard: Option<MutexGuard<'_, Database<File>>> = None;

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

        if guard.is_none() {
            guard = match db.try_lock() {
                Ok(guard) => Some(guard),
                Err(_) => {
                    println!("Connection {} locked on mutex", connection);
                    Some(db.lock().unwrap())
                }
            }
        }

        let db = guard.as_mut().unwrap();
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

        if !db.active_transaction() {
            drop(guard.take());
        }
    }

    println!("Close {connection} connection");
    if let Some(mut db) = guard {
        if db.active_transaction() {
            println!(
                "Connection {connection} closed in the middle of a transaction. Running rollback."
            );
            db.rollback()?;
        }
    }

    Ok(())
}
