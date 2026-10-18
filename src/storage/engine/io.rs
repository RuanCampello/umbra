//! The storage seam under the commit log and checkpoints
//!
//! [OsStorage] is the real file system
//! [memory::MemoryStorage] models what a crash may keep: file bytes become durable
//! on [StorageFile::sync], directory entries (creations, renames, removals) on [Storage::sync_dir] of their parent
//! It also counts mutating operations so tests can inject a failure or a crash after any of them

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

pub(crate) struct OsStorage;

/// Exclusive OS lock on a database directory, held for the engine's lifetime
pub(crate) struct DirectoryLock {
    _file: File,
}

pub(crate) struct OsFile {
    file: File,
    len: u64,
    /// Bytes the file holds, reserved space past `len` included
    size: u64,
}

pub(crate) trait Storage: Send + Sync {
    /// Creates `path` empty, truncating any previous content
    fn create(&self, path: &Path) -> io::Result<Box<dyn StorageFile>>;
    /// Opens an existing file for appending
    fn open(&self, path: &Path) -> io::Result<Box<dyn StorageFile>>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    /// Streams a file, for contents too large to hold at once
    fn reader(&self, path: &Path) -> io::Result<Box<dyn Read + Send>>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// Removes a directory and everything under it
    fn remove_dir(&self, path: &Path) -> io::Result<()>;
    /// Direct children of `dir`, sorted
    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>>;
    /// Creates `path` and its missing parents
    fn create_dir(&self, path: &Path) -> io::Result<()>;
    /// Makes the creations, renames and removals inside `dir` durable
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
    fn exists(&self, path: &Path) -> bool;
}

pub(crate) trait StorageFile: Send {
    /// writes at [StorageFile::len], over reserved space when there is some
    fn append(&mut self, bytes: &[u8]) -> io::Result<()>;
    /// Makes every appended byte durable (`fdatasync`)
    fn sync(&mut self) -> io::Result<()>;
    /// cuts the file at `len`, reserved space included
    fn truncate(&mut self, len: u64) -> io::Result<()>;
    /// zero-fills the file up to `size` bytes and syncs it, leaving [StorageFile::len] alone, so
    /// later appends overwrite allocated blocks and their syncs carry no change of file size
    fn reserve(&mut self, size: u64) -> io::Result<()>;
    /// bytes appended (or the file size, for a file just opened)
    fn len(&self) -> u64;
}

impl DirectoryLock {
    pub fn acquire(dir: &Path) -> io::Result<Self> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(dir.join("db.lock"))?;

        file.try_lock().map_err(|_| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("database {} is open in another process", dir.display()),
            )
        })?;

        file.set_len(0)?;
        write!(file, "{}", std::process::id())?;
        Ok(Self { _file: file })
    }
}

impl Storage for OsStorage {
    fn create(&self, path: &Path) -> io::Result<Box<dyn StorageFile>> {
        let file = OpenOptions::new().create(true).write(true).truncate(true).open(path)?;
        Ok(Box::new(OsFile { file, len: 0, size: 0 }))
    }

    fn open(&self, path: &Path) -> io::Result<Box<dyn StorageFile>> {
        let file = OpenOptions::new().write(true).open(path)?;
        let len = file.metadata()?.len();
        Ok(Box::new(OsFile { file, len, size: len }))
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        File::open(path)?.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    fn reader(&self, path: &Path) -> io::Result<Box<dyn Read + Send>> {
        Ok(Box::new(io::BufReader::with_capacity(1 << 20, File::open(path)?)))
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
        let mut paths = fs::read_dir(dir)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<io::Result<Vec<_>>>()?;

        paths.sort_unstable();
        Ok(paths)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        File::open(dir)?.sync_all()
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }
}

impl StorageFile for OsFile {
    fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
        // positioned: a short write leaves `len` where it was, so the next one overwrites it
        self.file.write_all_at(bytes, self.len)?;
        self.len += bytes.len() as u64;
        self.size = self.size.max(self.len);
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }

    fn truncate(&mut self, len: u64) -> io::Result<()> {
        self.file.set_len(len)?;
        self.len = len;
        Ok(self.size = len)
    }

    fn reserve(&mut self, size: u64) -> io::Result<()> {
        const ZEROS: [u8; 64 << 10] = [0; 64 << 10];

        if self.size >= size {
            return Ok(());
        }

        let mut at = self.size;
        while at < size {
            let chunk = ZEROS.len().min((size - at) as usize);
            self.file.write_all_at(&ZEROS[..chunk], at)?;
            at += chunk as u64;
        }

        self.file.sync_data()?;
        Ok(self.size = size)
    }

    #[inline(always)]
    fn len(&self) -> u64 {
        self.len
    }
}

#[cfg(test)]
pub(crate) mod memory {
    use super::{Storage, StorageFile};
    use crate::collections::hash::HashMap;
    use std::collections::BTreeMap;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Condvar, Mutex, MutexGuard};

    #[derive(Clone, Default)]
    pub(crate) struct MemoryStorage {
        state: Arc<Mutex<MemoryState>>,
        /// Wakes operations parked by [MemoryStorage::hold]
        gate: Arc<Condvar>,
    }

    struct MemoryFile {
        state: Arc<Mutex<MemoryState>>,
        gate: Arc<Condvar>,
        inode: u64,
        len: u64,
    }

    #[derive(Default)]
    struct MemoryState {
        current: BTreeMap<PathBuf, Entry>,
        durable: BTreeMap<PathBuf, Entry>,
        inodes: HashMap<u64, Inode>,
        next_inode: u64,
        operations: u64,
        fail_at: Option<u64>,
        /// Every operation from this one on fails, until the next reboot
        fail_from: Option<u64>,
        crash_at: Option<u64>,
        crashed: bool,
        /// Operations of this kind wait while set, so a test can stop a writer mid-commit
        held: Option<Op>,
        waiting: usize,
        /// Kinds of the operations failed on purpose, in order
        failed: Vec<Op>,
    }

    #[derive(Default)]
    struct Inode {
        current: Vec<u8>,
        durable: Vec<u8>,
    }

    /// What a crash does to the bytes written since a file's last sync
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub(crate) enum Loss {
        /// All of them are lost
        Clean,
        /// The first half survives, the rest is lost
        Torn,
        /// They survive with every seventh byte flipped
        Garbled,
        /// The first half is lost, the second half survives: the disk persisted later
        /// sectors of an unsynced write before earlier ones
        Reordered,
    }

    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    pub(crate) enum Op {
        Create,
        Rename,
        Remove,
        RemoveDir,
        CreateDir,
        SyncDir,
        Append,
        Sync,
        Truncate,
        Reserve,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Entry {
        File(u64),
        Dir,
    }

    impl MemoryStorage {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn operations(&self) -> u64 {
            self.lock().operations
        }

        pub fn fail_at(&self, n: u64) {
            let mut state = self.lock();
            state.fail_at = Some(state.operations + n);
        }

        /// fails the `n`-th operation from now and every one after it, until the next reboot
        pub fn fail_from(&self, n: u64) {
            let mut state = self.lock();
            state.fail_from = Some(state.operations + n);
        }

        pub fn crash_at(&self, n: u64) {
            let mut state = self.lock();
            state.crash_at = Some(state.operations + n);
        }

        /// parks every file operation of kind `op` until [MemoryStorage::release]
        pub fn hold(&self, op: Op) {
            self.lock().held = Some(op);
        }

        pub fn release(&self) {
            self.lock().held = None;
            self.gate.notify_all();
        }

        /// operations parked by [MemoryStorage::hold]
        pub fn waiting(&self) -> usize {
            self.lock().waiting
        }

        /// kinds of the operations failed by [MemoryStorage::fail_at] or [MemoryStorage::fail_from]
        pub fn failed(&self) -> Vec<Op> {
            self.lock().failed.clone()
        }

        pub fn reboot(&self, torn: bool) {
            self.reboot_with(match torn {
                true => Loss::Torn,
                false => Loss::Clean,
            });
        }

        /// restarts the machine: everything not synced is lost or damaged as `loss` says
        pub fn reboot_with(&self, loss: Loss) {
            let mut state = self.lock();
            let state = &mut *state;

            state.current = state.durable.clone();
            state.crashed = false;
            state.crash_at = None;
            state.fail_at = None;
            state.fail_from = None;

            let live: Vec<_> = state
                .durable
                .values()
                .filter_map(|entry| match entry {
                    Entry::File(inode) => Some(*inode),
                    Entry::Dir => None,
                })
                .collect();
            state.inodes.retain(|inode, _| live.contains(inode));

            for inode in state.inodes.values_mut() {
                // the unsynced bytes: from the first that differs from the durable copy to the last, appended or written over reserved space
                let (current, durable) = (&inode.current, &inode.durable);
                let shared = current.len().min(durable.len());
                let start = (0..shared).find(|&i| current[i] != durable[i]).unwrap_or(shared);
                let end = match current.len() > durable.len() {
                    true => current.len(),
                    _ => (start..shared)
                        .rev()
                        .find(|&i| current[i] != durable[i])
                        .map_or(start, |i| i + 1),
                };
                let half = start + (end - start) / 2;

                let kept = match (loss, current.len() < durable.len()) {
                    (Loss::Clean, _) | (_, true) => durable.clone(),
                    (Loss::Torn, _) => overlay(durable, current, start..half),
                    (Loss::Reordered, _) => overlay(durable, current, half..end),
                    (Loss::Garbled, _) => {
                        let mut garbled = current.clone();
                        garbled[start..end].iter_mut().step_by(7).for_each(|byte| *byte ^= 0xA5);
                        garbled
                    },
                };

                inode.current.clone_from(&kept);
                inode.durable = kept;
            }
        }

        fn lock(&self) -> MutexGuard<'_, MemoryState> {
            self.state.lock().expect("memory storage poisoned")
        }
    }

    /// `durable` with `current`'s bytes over `range`, zero-filled where neither reaches
    fn overlay(durable: &[u8], current: &[u8], range: std::ops::Range<usize>) -> Vec<u8> {
        let mut bytes = durable.to_vec();
        if bytes.len() < range.end {
            bytes.resize(range.end, 0);
        }

        bytes[range.clone()].copy_from_slice(&current[range]);
        bytes
    }

    impl MemoryState {
        fn operation(&mut self, op: Op) -> io::Result<()> {
            if self.crashed {
                return Err(io::Error::other("storage crashed"));
            }

            self.operations += 1;

            if self.crash_at == Some(self.operations) {
                self.crashed = true;
                return Err(io::Error::other("storage crashed"));
            }

            let failing = self.fail_at == Some(self.operations)
                || self.fail_from.is_some_and(|from| self.operations >= from);

            match failing {
                true => {
                    self.failed.push(op);
                    Err(io::Error::other(format!("injected {op:?} failure")))
                },
                _ => Ok(()),
            }
        }

        fn inode(&self, path: &Path) -> io::Result<u64> {
            match self.current.get(path) {
                Some(Entry::File(inode)) => Ok(*inode),
                _ => Err(io::Error::new(io::ErrorKind::NotFound, path.display().to_string())),
            }
        }

        fn require_parent(&self, path: &Path) -> io::Result<()> {
            match path.parent() {
                Some(parent) if !parent.as_os_str().is_empty() => {
                    match self.current.get(parent) == Some(&Entry::Dir) {
                        true => Ok(()),
                        false => Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            parent.display().to_string(),
                        )),
                    }
                },
                _ => Ok(()),
            }
        }
    }

    impl Storage for MemoryStorage {
        fn create(&self, path: &Path) -> io::Result<Box<dyn StorageFile>> {
            let mut state = self.lock();
            state.operation(Op::Create)?;
            state.require_parent(path)?;

            let inode = state.next_inode;
            state.next_inode += 1;
            state.inodes.insert(inode, Inode::default());
            state.current.insert(path.to_path_buf(), Entry::File(inode));

            Ok(Box::new(MemoryFile {
                state: Arc::clone(&self.state),
                gate: Arc::clone(&self.gate),
                inode,
                len: 0,
            }))
        }

        fn open(&self, path: &Path) -> io::Result<Box<dyn StorageFile>> {
            let state = self.lock();
            let inode = state.inode(path)?;
            let len = state.inodes[&inode].current.len() as u64;

            Ok(Box::new(MemoryFile {
                state: Arc::clone(&self.state),
                gate: Arc::clone(&self.gate),
                inode,
                len,
            }))
        }

        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            let state = self.lock();
            let inode = state.inode(path)?;
            Ok(state.inodes[&inode].current.clone())
        }

        fn reader(&self, path: &Path) -> io::Result<Box<dyn std::io::Read + Send>> {
            Ok(Box::new(io::Cursor::new(self.read(path)?)))
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let mut state = self.lock();
            state.operation(Op::Rename)?;
            state.require_parent(to)?;

            let entry = state.current.remove(from).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, from.display().to_string())
            })?;
            state.current.insert(to.to_path_buf(), entry);

            Ok(())
        }

        fn remove(&self, path: &Path) -> io::Result<()> {
            let mut state = self.lock();
            state.operation(Op::Remove)?;
            state.inode(path)?;
            state.current.remove(path);

            Ok(())
        }

        fn remove_dir(&self, path: &Path) -> io::Result<()> {
            let mut state = self.lock();
            state.operation(Op::RemoveDir)?;

            if state.current.get(path) != Some(&Entry::Dir) {
                return Err(io::Error::new(io::ErrorKind::NotFound, path.display().to_string()));
            }
            state.current.retain(|entry, _| !entry.starts_with(path));

            Ok(())
        }

        fn list(&self, dir: &Path) -> io::Result<Vec<PathBuf>> {
            let state = self.lock();
            if state.current.get(dir) != Some(&Entry::Dir) {
                return Err(io::Error::new(io::ErrorKind::NotFound, dir.display().to_string()));
            }

            Ok(state
                .current
                .keys()
                .filter(|path| path.parent() == Some(dir))
                .cloned()
                .collect())
        }

        fn create_dir(&self, path: &Path) -> io::Result<()> {
            let mut state = self.lock();
            state.operation(Op::CreateDir)?;

            for ancestor in path.ancestors().filter(|p| !p.as_os_str().is_empty()) {
                match state.current.get(ancestor) {
                    Some(Entry::Dir) => break,
                    Some(Entry::File(_)) => {
                        return Err(io::Error::new(
                            io::ErrorKind::AlreadyExists,
                            ancestor.display().to_string(),
                        ));
                    },
                    _ => {
                        state.current.insert(ancestor.to_path_buf(), Entry::Dir);
                    },
                }
            }

            Ok(())
        }

        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            let mut state = self.lock();
            state.operation(Op::SyncDir)?;
            let state = &mut *state;

            state.durable.retain(|path, _| path.parent() != Some(dir));
            for (path, entry) in &state.current {
                if path.parent() == Some(dir) {
                    state.durable.insert(path.clone(), *entry);
                }
            }

            Ok(())
        }

        fn exists(&self, path: &Path) -> bool {
            self.lock().current.contains_key(path)
        }
    }

    impl MemoryFile {
        fn lock(&self) -> MutexGuard<'_, MemoryState> {
            self.state.lock().expect("memory storage poisoned")
        }

        /// the state, once operations of kind `op` are no longer held
        fn park(&self, op: Op) -> MutexGuard<'_, MemoryState> {
            let mut state = self.lock();
            if state.held == Some(op) {
                state.waiting += 1;
                state = self
                    .gate
                    .wait_while(state, |state| state.held == Some(op))
                    .expect("memory storage poisoned");
                state.waiting -= 1;
            }
            state
        }
    }

    impl StorageFile for MemoryFile {
        fn append(&mut self, bytes: &[u8]) -> io::Result<()> {
            let mut state = self.park(Op::Append);
            let outcome = state.operation(Op::Append);

            // a failed write may still have written a prefix (a short write before ENOSPC)
            let written = match (&outcome, state.crashed) {
                (Ok(()), _) => bytes,
                (Err(_), false) => &bytes[..bytes.len() / 2],
                (Err(_), _) => &[],
            };
            let inode = state.inodes.get_mut(&self.inode).expect("open file has an inode");
            let at = self.len as usize;
            if inode.current.len() < at + written.len() {
                inode.current.resize(at + written.len(), 0);
            }
            inode.current[at..at + written.len()].copy_from_slice(written);
            drop(state);

            if outcome.is_ok() {
                self.len += bytes.len() as u64;
            }
            outcome
        }

        fn sync(&mut self) -> io::Result<()> {
            let mut state = self.park(Op::Sync);
            state.operation(Op::Sync)?;

            let inode = state.inodes.get_mut(&self.inode).expect("open file has an inode");
            Ok(inode.durable.clone_from(&inode.current))
        }

        fn truncate(&mut self, len: u64) -> io::Result<()> {
            let mut state = self.lock();
            state.operation(Op::Truncate)?;

            let inode = state.inodes.get_mut(&self.inode).expect("open file has an inode");
            inode.current.truncate(len as usize);
            drop(state);

            Ok(self.len = len)
        }

        fn reserve(&mut self, size: u64) -> io::Result<()> {
            let mut state = self.park(Op::Reserve);
            if state.inodes[&self.inode].current.len() as u64 >= size {
                return Ok(());
            }
            state.operation(Op::Reserve)?;

            let inode = state.inodes.get_mut(&self.inode).expect("open file has an inode");
            inode.current.resize(size as usize, 0);
            Ok(inode.durable.clone_from(&inode.current))
        }

        #[inline(always)]
        fn len(&self) -> u64 {
            self.len
        }
    }
}

#[cfg(test)]
mod tests {
    use super::memory::{MemoryStorage, Op};
    use super::*;

    fn durable_root() -> MemoryStorage {
        let storage = MemoryStorage::new();
        storage.create_dir(Path::new("db")).unwrap();
        storage.sync_dir(Path::new("")).unwrap();
        storage
    }

    fn write(storage: &dyn Storage, path: &str, bytes: &[u8], sync: bool) {
        let mut file = storage.create(Path::new(path)).unwrap();
        file.append(bytes).unwrap();
        if sync {
            file.sync().unwrap();
        }
    }

    #[test]
    fn crash_keeps_only_synced_data_in_synced_entries() {
        let storage = durable_root();

        write(&storage, "db/a", b"durable", true);
        write(&storage, "db/b", b"unsynced", false);
        storage.sync_dir(Path::new("db")).unwrap();
        write(&storage, "db/c", b"no entry", true);

        storage.reboot(false);

        assert_eq!(storage.read(Path::new("db/a")).unwrap(), b"durable");
        assert_eq!(storage.read(Path::new("db/b")).unwrap(), b"");
        assert!(!storage.exists(Path::new("db/c")));
    }

    #[test]
    fn rename_is_durable_only_after_syncing_the_directory() {
        let storage = durable_root();
        write(&storage, "db/tmp", b"manifest", true);
        storage.sync_dir(Path::new("db")).unwrap();

        storage.rename(Path::new("db/tmp"), Path::new("db/MANIFEST")).unwrap();
        storage.reboot(false);
        assert!(storage.exists(Path::new("db/tmp")));
        assert!(!storage.exists(Path::new("db/MANIFEST")));

        storage.rename(Path::new("db/tmp"), Path::new("db/MANIFEST")).unwrap();
        storage.sync_dir(Path::new("db")).unwrap();
        storage.reboot(false);
        assert_eq!(storage.read(Path::new("db/MANIFEST")).unwrap(), b"manifest");
    }

    #[test]
    fn torn_reboot_keeps_part_of_the_unsynced_tail() {
        let storage = durable_root();

        let mut file = storage.create(Path::new("db/log")).unwrap();
        storage.sync_dir(Path::new("db")).unwrap();
        file.append(b"0123").unwrap();
        file.sync().unwrap();
        file.append(b"456789").unwrap();

        storage.reboot(true);
        assert_eq!(storage.read(Path::new("db/log")).unwrap(), b"0123456");
    }

    #[test]
    fn crash_tears_an_overwrite_of_reserved_space() {
        let storage = durable_root();

        let mut file = storage.create(Path::new("db/log")).unwrap();
        storage.sync_dir(Path::new("db")).unwrap();
        file.reserve(16).unwrap();
        file.append(b"0123").unwrap();
        file.sync().unwrap();
        file.append(b"456789").unwrap();
        assert_eq!(file.len(), 10);

        storage.reboot(true);
        assert_eq!(storage.read(Path::new("db/log")).unwrap(), b"0123456\0\0\0\0\0\0\0\0\0");

        let mut file = storage.open(Path::new("db/log")).unwrap();
        file.truncate(4).unwrap();
        file.append(b"45").unwrap();
        file.sync().unwrap();
        storage.reboot_with(memory::Loss::Reordered);
        assert_eq!(storage.read(Path::new("db/log")).unwrap(), b"012345");
    }

    #[test]
    fn injected_failure_and_crash_stop_operations() {
        let storage = MemoryStorage::new();
        storage.create_dir(Path::new("db")).unwrap();

        storage.fail_at(2);
        let mut file = storage.create(Path::new("db/log")).unwrap();
        assert!(file.append(b"lost").is_err());
        assert_eq!(storage.read(Path::new("db/log")).unwrap(), b"lo");
        file.append(b"kept").unwrap();
        assert_eq!(storage.read(Path::new("db/log")).unwrap(), b"kept");
        assert_eq!(storage.failed(), [Op::Append]);

        storage.crash_at(1);
        assert!(file.sync().is_err());
        assert!(file.append(b"x").is_err());
        assert!(storage.sync_dir(Path::new("db")).is_err());

        storage.reboot(false);
        assert!(!storage.exists(Path::new("db/log")));
        storage.create_dir(Path::new("db")).unwrap();
    }

    #[test]
    fn os_storage_round_trip() {
        let dir = std::env::temp_dir().join(format!("umbra-io-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let storage = OsStorage;

        storage.create_dir(&dir).unwrap();
        let path = dir.join("log");
        let mut file = storage.create(&path).unwrap();
        file.append(b"hello world").unwrap();
        file.sync().unwrap();
        file.truncate(5).unwrap();
        drop(file);

        let mut file = storage.open(&path).unwrap();
        assert_eq!(file.len(), 5);
        file.append(b"!").unwrap();
        file.reserve(8).unwrap();
        file.append(b"?").unwrap();
        assert_eq!(file.len(), 7);
        storage.sync_dir(&dir).unwrap();

        assert_eq!(storage.read(&path).unwrap(), b"hello!?\0");
        assert_eq!(storage.list(&dir).unwrap(), vec![path]);
        storage.remove_dir(&dir).unwrap();
    }
}
