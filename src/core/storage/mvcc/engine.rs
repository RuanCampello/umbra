use std::{
    fs::File,
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::core::storage::mvcc::MvccError;

/// Good ol' "styx"
const SNAPSHOT_MAGIC: u32 = 0x73747978;
const SNAPSHOT_VERSION: u32 = 1;

/// magic (4) + version(4) + lsn(8) + time(8) + hash(4)
const SNAPSHOT_HEADER_SIZE: usize = 28;

fn serialise_snapshot_header(path: &Path, lsn: u64) -> Result<(), MvccError> {
    let mut buff = Vec::with_capacity(SNAPSHOT_HEADER_SIZE);

    buff.extend_from_slice(&SNAPSHOT_MAGIC.to_le_bytes());
    buff.extend_from_slice(&SNAPSHOT_VERSION.to_le_bytes());
    buff.extend_from_slice(&lsn.to_le_bytes());

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);

    buff.extend_from_slice(&timestamp.to_le_bytes());

    let hash = fnv1a(&buff);
    buff.extend_from_slice(&hash.to_le_bytes());

    let temp = path.with_extension("temp");
    let mut file = File::create(&temp)?;

    file.write_all(&buff)?;
    file.sync_all()?;
    std::fs::rename(&temp, path)?;

    if let Some(parent) = path.parent() {
        if let Ok(file) = File::open(parent) {
            let _ = file.sync_all();
        }
    }

    Ok(())
}

/// Returns 0 if **invalid** or not found.
fn deserialise_snapshot_header(path: &Path) -> u64 {
    let data = match std::fs::read(path) {
        Ok(data) => data,
        _ => return 0,
    };

    if data.len() < SNAPSHOT_HEADER_SIZE {
        return 0;
    }

    let magic = u32::from_le_bytes(data[0..4].try_into().unwrap());
    if magic != SNAPSHOT_MAGIC {
        return 0;
    }

    let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
    if version != SNAPSHOT_VERSION {
        eprintln!("Snapshot version {version} not supported supported: {SNAPSHOT_VERSION}");
        return 0;
    }

    let hash = u32::from_le_bytes(data[24..28].try_into().unwrap());
    let computed_hash = fnv1a(&data[0..24]);
    if hash != computed_hash {
        eprintln!("Snapshot header checksum does not match");
        return 0;
    }

    u64::from_le_bytes(data[8..16].try_into().unwrap())
}

#[inline(always)]
/// Basic hashing function, used for simplicity.
/// We could probably make it faster using `crc32` or `crc32c`, but that's a much more complex
/// algorithm.
/// That can be done in the future thou :D
///
/// For the interesed ones: [https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function].
const fn fnv1a(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    let mut idx = 0;

    while idx < bytes.len() {
        hash ^= bytes[idx] as u32;
        hash = hash.wrapping_mul(0x01000193);

        idx += 1;
    }

    hash
}
