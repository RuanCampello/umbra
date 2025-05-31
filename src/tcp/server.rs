use std::{net::SocketAddr, path::Path, sync::Mutex};

use crate::db::{Database, DatabaseError};

/// Starts the database at the given file path and listens on the given address.
pub fn start<File: AsRef<Path>>(address: SocketAddr, file: File) -> Result<(), DatabaseError> {
    let mut db = &*Box::leak(Box::new(Mutex::new(Database::init(&file)?)));
    println!("Database file initialised on {}", file.as_ref().display());

    todo!()
}
