//! Node header for memory reclamation.
//!
//! Every memory block managed by Crystalline must include a [`Node`] header.
//! The header uses a compact 3-word layout with field reuse across different
//! lifecycle stages.

use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

/// Header prepended to every memory block managed by Crystalline.
///
/// # Memory Layout (24 bytes on 64-bit systems)
///
/// The header uses a union-like design where fields are reused at different
/// stages of a node's lifecycle:
///
/// - **Before retirement**: `birth_or_next` holds birth era, `blink` is null
/// - **REFS node**: `refc_or_bnext` holds ref count, `blink` points to first SLOT (low bit = 1)
/// - **SLOT node**: `refc_or_bnext` holds bnext, `blink` points to REFS (low bit = 0)
#[repr(C)]
pub struct Node {
    pub refc_or_bnext: AtomicU64,
    pub birth_or_next: AtomicU64,
    pub blink: AtomicPtr<Node>,
}

/// Sentinel value indicating an inactive reservation or tainted pointer.
pub const INVPTR: *mut Node = usize::MAX as *mut Node;

impl Node {
    #[inline]
    pub const fn new(birth_era: u64) -> Self {
        Self {
            refc_or_bnext: AtomicU64::new(0),
            birth_or_next: AtomicU64::new(birth_era),
            blink: AtomicPtr::new(ptr::null_mut()),
        }
    }

    #[inline]
    pub fn birth(&self) -> u64 {
        self.birth_or_next.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_birth(&self, era: u64) {
        self.birth_or_next.store(era, Ordering::Relaxed);
    }

    #[inline]
    pub fn is_retired(&self) -> bool {
        !self.blink.load(Ordering::Acquire).is_null()
    }

    #[inline]
    pub fn is_refs(&self) -> bool {
        is_refs_link(self.blink.load(Ordering::Relaxed))
    }

    #[inline]
    pub fn refc(&self) -> u64 {
        self.refc_or_bnext.load(Ordering::Acquire)
    }

    #[inline]
    pub fn set_refc(&self, count: u64) {
        self.refc_or_bnext.store(count, Ordering::Release);
    }

    #[inline]
    pub fn fetch_add_refc(&self, delta: i64) -> u64 {
        match delta >= 0 {
            true => self.refc_or_bnext.fetch_add(delta as u64, Ordering::AcqRel),
            _ => self
                .refc_or_bnext
                .fetch_sub((-delta) as u64, Ordering::AcqRel),
        }
    }

    #[inline]
    pub fn bnext(&self) -> *mut Node {
        self.refc_or_bnext.load(Ordering::Acquire) as *mut Node
    }

    #[inline]
    pub fn set_bnext(&self, next: *mut Node) {
        self.refc_or_bnext.store(next as u64, Ordering::Release);
    }

    #[inline]
    pub fn next(&self) -> *mut Node {
        self.birth_or_next.load(Ordering::Acquire) as *mut Node
    }

    #[inline]
    pub fn set_next(&self, next: *mut Node) {
        self.birth_or_next.store(next as u64, Ordering::Release);
    }

    #[inline]
    pub fn swap_next(&self, new: *mut Node) -> *mut Node {
        self.birth_or_next.swap(new as u64, Ordering::AcqRel) as *mut Node
    }

    #[inline]
    pub fn refs_node(&self) -> *mut Node {
        debug_assert!(!self.is_refs(), "called refs_node on a REFS node");
        self.blink.load(Ordering::Acquire)
    }

    #[inline]
    pub fn first_slot(&self) -> *mut Node {
        debug_assert!(self.is_refs(), "called first_slot on a SLOT node");
        decode_refs_link(self.blink.load(Ordering::Acquire))
    }

    #[inline]
    pub fn set_as_refs(&self, first_slot: *mut Node) {
        self.blink
            .store(encode_refs_link(first_slot), Ordering::Release);
    }

    #[inline]
    pub fn set_as_slot(&self, refs: *mut Node) {
        self.blink.store(refs, Ordering::Release);
    }
}

#[inline]
pub fn is_refs_link(ptr: *mut Node) -> bool {
    (ptr as usize) & 1 == 1
}

#[inline]
pub fn encode_refs_link(ptr: *mut Node) -> *mut Node {
    ((ptr as usize) | 1) as *mut Node
}

#[inline]
pub fn decode_refs_link(ptr: *mut Node) -> *mut Node {
    ((ptr as usize) & !1) as *mut Node
}

#[inline]
pub fn is_invptr(ptr: *mut Node) -> bool {
    ptr::eq(ptr, INVPTR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_new() {
        let node = Node::new(42);
        assert_eq!(node.birth(), 42);
        assert!(!node.is_retired());
        assert!(!node.is_refs());
    }

    #[test]
    fn test_refs_encoding() {
        let ptr = 0x1000 as *mut Node;
        let encoded = encode_refs_link(ptr);
        assert!(is_refs_link(encoded));
        assert_eq!(decode_refs_link(encoded), ptr);

        let null_encoded = encode_refs_link(ptr::null_mut());
        assert!(is_refs_link(null_encoded));
        assert_eq!(decode_refs_link(null_encoded), ptr::null_mut());
    }

    #[test]
    fn test_refs_slot_distinction() {
        let refs_node = Node::new(1);
        let slot_node = Node::new(2);

        assert!(!refs_node.is_refs());
        assert!(!slot_node.is_refs());

        refs_node.set_as_refs(&slot_node as *const _ as *mut _);
        assert!(refs_node.is_refs());
        assert!(refs_node.is_retired());

        slot_node.set_as_slot(&refs_node as *const _ as *mut _);
        assert!(!slot_node.is_refs());
        assert!(slot_node.is_retired());
    }

    #[test]
    fn test_invptr() {
        assert!(is_invptr(INVPTR));
        assert!(!is_invptr(ptr::null_mut()));
        assert!(!is_invptr(0x1000 as *mut Node));
    }
}
