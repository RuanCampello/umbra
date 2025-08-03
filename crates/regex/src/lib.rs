//! Regex implementation for Umbra database.
//! This is mostly a "lite" version with the things I need from the [Andrew Gallant](https://github.com/rust-lang/regex/tree/master)
//! implementation, one of the goats of rust community.

mod dfa;
mod fsm;
mod nfa;
mod search;
mod string;

pub(self) struct DebugByte(pub u8);

pub(crate) type StateId = u32;

pub(crate) trait U32 {
    fn as_usize(self) -> usize;
}

impl U32 for u32 {
    fn as_usize(self) -> usize {
        self as usize
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
