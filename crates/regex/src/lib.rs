//! Regex implementation for Umbra database.
//! This is mostly a "lite" version with the things I need from the [Andrew Gallant](https://github.com/rust-lang/regex/tree/master)
//! implementation, one of the goats of rust community.

mod dfa;
mod fsm;
mod look;
mod nfa;
mod search;
mod string;

pub(self) struct DebugByte(pub u8);
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct SmallIdx(u32);

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
#[repr(transparent)]
pub(crate) struct StateId(SmallIdx);

pub(crate) trait U32 {
    fn as_usize(self) -> usize;
}

impl U32 for StateId {
    fn as_usize(self) -> usize {
        self.0.0 as usize
    }
}

impl SmallIdx {
    pub const ZERO: Self = SmallIdx::new_unchecked(0);

    const fn new_unchecked(index: usize) -> Self {
        Self(index as u32)
    }

    #[inline]
    const fn as_usize(self) -> usize {
        self.0 as usize
    }
}

impl<T> core::ops::Index<SmallIdx> for [T] {
    type Output = T;

    fn index(&self, index: SmallIdx) -> &Self::Output {
        &self[index.as_usize()]
    }
}

impl<T> core::ops::IndexMut<SmallIdx> for [T] {
    #[inline]
    fn index_mut(&mut self, index: SmallIdx) -> &mut T {
        &mut self[index.as_usize()]
    }
}

impl<T> core::ops::Index<StateId> for [T] {
    type Output = T;

    fn index(&self, index: StateId) -> &Self::Output {
        &self[index.as_usize()]
    }
}

impl core::fmt::Debug for DebugByte {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        if self.0 == b' ' {
            return write!(f, "' '");
        }

        let mut bytes = [0u8; 10];
        let mut len = 0;

        for (idx, mut byte) in core::ascii::escape_default(self.0).enumerate() {
            if idx >= 2 && b'a' <= byte && byte <= b'f' {
                byte -= 32;
            }

            bytes[len] = byte;
            len += 1;
        }

        write!(f, "{}", core::str::from_utf8(&bytes[..len]).unwrap())
    }
}
