use std::{
    fs::{self, File},
    path::Path,
};

#[derive(Debug)]
pub(crate) struct Options {
    inner: fs::OpenOptions,
    bypass_cache: bool,
    sync_on_write: bool,
    lock: bool,
}

pub(crate) struct Fs;

pub(crate) trait FileSystemBlockSize {
    fn block_size(path: impl AsRef<Path>) -> std::io::Result<usize>;
}

pub(crate) trait Open {
    fn open(self, path: impl AsRef<Path>) -> std::io::Result<File>;
}

macro_rules! generate_methods {
    ($($name:ident: $field:ident),+) => {
        $(
            pub fn $name(mut self, $field: bool) -> Self {
                self.$field = $field;
                self
            }
        )+
    };
}

macro_rules! generate_inner_methods {
    ($($name:ident),+) => {
        $(
            pub fn $name(mut self, $name: bool) -> Self {
                self.inner.$name($name);
                self
            }
        )+
    };
}

impl Options {
    generate_methods! {
        bypass_cache: bypass_cache,
        sync_on_write: sync_on_write,
        lock: lock
    }

    generate_inner_methods! {
        create,
        read,
        write,
        truncate
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            inner: File::options(),
            bypass_cache: false,
            sync_on_write: false,
            lock: false,
        }
    }
}

impl Fs {
    pub fn options() -> Options {
        Options::default()
    }
}

#[cfg(unix)]
mod unix {
    use std::{
        fs::File,
        os::{
            fd::AsRawFd,
            unix::{fs::OpenOptionsExt, prelude::MetadataExt},
        },
    };

    use super::{FileSystemBlockSize, Fs, Open, Options};

    impl FileSystemBlockSize for Fs {
        fn block_size(path: impl AsRef<std::path::Path>) -> std::io::Result<usize> {
            Ok(File::open(&path)?.metadata()?.blksize() as usize)
        }
    }

    impl Open for Options {
        fn open(mut self, path: impl AsRef<std::path::Path>) -> std::io::Result<File> {
            let mut flags = 0;

            #[cfg(target_os = "linux")]
            if self.bypass_cache {
                flags |= libc::O_DIRECT;
            }

            if self.sync_on_write {
                flags |= libc::O_DSYNC;
            }

            if flags.ne(&0) {
                self.inner.custom_flags(flags);
            }

            let file = self.inner.open(&path)?;
            if self.lock {
                let lock = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };

                if lock.ne(&0) {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Couldn't lock file: {}", path.as_ref().display()),
                    ));
                }
            }

            Ok(file)
        }
    }
}
