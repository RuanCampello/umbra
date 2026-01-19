//! Memory barriers optimised for RCU.
//!
//! Inspired by <https://github.com/ibraheemdev/seize> and <https://github.com/jeehoonkang/membarrier-rs>.
//!
//! # Semantics
//!
//! There is a total order over all memory barriers provided by this module:
//! - Light store barriers, created by [`light_store`] ordering.
//! - Light barriers, created by [`light_barrier`].
//! - Heavy barriers, created by [`heavy`].
//!
//! If thread A issues barrier X and thread B issues barrier Y and X occurs
//! before Y in the total order, X is ordered before Y with respect to coherence
//! only if either X or Y is a heavy barrier.

use std::sync::atomic::{self, AtomicU8, Ordering};

/// The ordering for a store operation that synchronizes with heavy barriers.
///
/// Must be followed by a light barrier.
#[inline]
pub fn light_store() -> Ordering {
    match STRATEGY.load(Ordering::Relaxed) {
        FALLBACK => Ordering::SeqCst,
        _ => Ordering::Relaxed,
    }
}

/// Issues a light memory barrier for a preceding store or subsequent load.
#[inline]
pub fn light_barrier() {
    atomic::compiler_fence(Ordering::SeqCst)
}

/// The ordering for a load operation that synchronizes with heavy barriers.
#[inline]
pub fn light_load() -> Ordering {
    Ordering::SeqCst
}

/// Issues a heavy memory barrier for slow path.
#[inline]
pub fn heavy() {
    match STRATEGY.load(Ordering::Relaxed) {
        MEMBARRIER => membarrier::barrier(),
        MPROTECT => mprotect::barrier(),
        _ => atomic::fence(Ordering::SeqCst),
    }
}

/// Use the `membarrier` system call.
const MEMBARRIER: u8 = 0;

/// Use the `mprotect`-based trick.
const MPROTECT: u8 = 1;

/// Use `SeqCst` fences.
const FALLBACK: u8 = 2;

/// The right strategy to use on the current machine.
static STRATEGY: AtomicU8 = AtomicU8::new(FALLBACK);

/// Perform runtime detection for a membarrier strategy.
pub fn detect() {
    if membarrier::is_supported() {
        STRATEGY.store(MEMBARRIER, Ordering::Relaxed);
    } else if mprotect::is_supported() {
        STRATEGY.store(MPROTECT, Ordering::Relaxed);
    }
}

#[cfg(target_os = "linux")]
mod membarrier {
    /// Commands for the membarrier system call.
    #[repr(i32)]
    #[allow(dead_code, non_camel_case_types)]
    enum membarrier_cmd {
        MEMBARRIER_CMD_QUERY = 0,
        MEMBARRIER_CMD_GLOBAL = (1 << 0),
        MEMBARRIER_CMD_GLOBAL_EXPEDITED = (1 << 1),
        MEMBARRIER_CMD_REGISTER_GLOBAL_EXPEDITED = (1 << 2),
        MEMBARRIER_CMD_PRIVATE_EXPEDITED = (1 << 3),
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED = (1 << 4),
        MEMBARRIER_CMD_PRIVATE_EXPEDITED_SYNC_CORE = (1 << 5),
        MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED_SYNC_CORE = (1 << 6),
    }

    #[inline]
    fn sys_membarrier(cmd: membarrier_cmd) -> libc::c_long {
        unsafe { libc::syscall(libc::SYS_membarrier, cmd as libc::c_int, 0 as libc::c_int) }
    }

    pub fn is_supported() -> bool {
        let ret = sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_QUERY);
        if ret < 0
            || ret & membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED as libc::c_long == 0
            || ret & membarrier_cmd::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED as libc::c_long == 0
        {
            return false;
        }

        if sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_REGISTER_PRIVATE_EXPEDITED) < 0 {
            return false;
        }

        true
    }

    #[inline]
    pub fn barrier() {
        if sys_membarrier(membarrier_cmd::MEMBARRIER_CMD_PRIVATE_EXPEDITED) < 0 {
            unsafe { libc::abort() }
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod membarrier {
    pub fn is_supported() -> bool {
        false
    }

    #[inline]
    pub fn barrier() {
        unreachable!()
    }
}

#[cfg(all(unix, any(target_arch = "x86", target_arch = "x86_64")))]
mod mprotect {
    use std::cell::UnsafeCell;
    use std::mem::MaybeUninit;
    use std::ptr;
    use std::sync::{atomic, OnceLock};

    struct Barrier {
        lock: UnsafeCell<libc::pthread_mutex_t>,
        page: u64,
        page_size: libc::size_t,
    }

    unsafe impl Sync for Barrier {}

    impl Barrier {
        #[inline]
        fn barrier(&self) {
            let page = self.page as *mut libc::c_void;

            unsafe {
                if libc::pthread_mutex_lock(self.lock.get()) != 0 {
                    libc::abort();
                }

                if libc::mprotect(page, self.page_size, libc::PROT_READ | libc::PROT_WRITE) != 0 {
                    libc::abort();
                }

                let atomic_usize = &*(page as *const atomic::AtomicUsize);
                atomic_usize.fetch_add(1, atomic::Ordering::SeqCst);

                if libc::mprotect(page, self.page_size, libc::PROT_NONE) != 0 {
                    libc::abort();
                }

                if libc::pthread_mutex_unlock(self.lock.get()) != 0 {
                    libc::abort();
                }
            }
        }
    }

    static BARRIER: OnceLock<Barrier> = OnceLock::new();

    pub fn is_supported() -> bool {
        true
    }

    #[inline]
    pub fn barrier() {
        let barrier = BARRIER.get_or_init(|| unsafe {
            let page_size = libc::sysconf(libc::_SC_PAGESIZE);
            if page_size <= 0 {
                libc::abort();
            }
            let page_size = page_size as libc::size_t;

            let page = libc::mmap(
                ptr::null_mut(),
                page_size,
                libc::PROT_NONE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1 as libc::c_int,
                0 as libc::off_t,
            );
            if page == libc::MAP_FAILED {
                libc::abort();
            }
            if page as libc::size_t % page_size != 0 {
                libc::abort();
            }

            libc::mlock(page, page_size);

            let lock = UnsafeCell::new(libc::PTHREAD_MUTEX_INITIALIZER);
            let mut attr = MaybeUninit::<libc::pthread_mutexattr_t>::uninit();
            if libc::pthread_mutexattr_init(attr.as_mut_ptr()) != 0 {
                libc::abort();
            }
            let mut attr = attr.assume_init();
            if libc::pthread_mutexattr_settype(&mut attr, libc::PTHREAD_MUTEX_NORMAL) != 0 {
                libc::abort();
            }
            if libc::pthread_mutex_init(lock.get(), &attr) != 0 {
                libc::abort();
            }
            if libc::pthread_mutexattr_destroy(&mut attr) != 0 {
                libc::abort();
            }

            Barrier {
                lock,
                page: page as u64,
                page_size,
            }
        });

        barrier.barrier();
    }
}

#[cfg(not(all(unix, any(target_arch = "x86", target_arch = "x86_64"))))]
mod mprotect {
    pub fn is_supported() -> bool {
        false
    }

    #[inline]
    pub fn barrier() {
        unreachable!()
    }
}
