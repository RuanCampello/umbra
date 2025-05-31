use std::{
    fs::File,
    net::{SocketAddr, TcpListener, TcpStream},
    path::Path,
    sync::Mutex,
    thread,
};

use crate::{
    db::{Database, DatabaseError},
    tcp::pool::ThreadPool,
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

fn handle(
    stream: &mut TcpStream,
    db_mutex: &'static Mutex<Database<File>>,
) -> Result<(), DatabaseError> {
    todo!()
}
