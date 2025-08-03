//! Provides non-deterministic finite automata (NFA) and the engines that use them.

use std::{fmt::Debug, sync::Arc};

use crate::{StateId, U32, fsm::PatternId, look::Look};

pub(crate) struct Nfa(Arc<Inner>);

pub(crate) struct Inner {
    states: Vec<State>,
    start_anchored: StateId,
    start_unanchored: StateId,
}

#[derive(PartialEq, Eq, Hash, Clone, Copy)]
pub(crate) struct Transition {
    pub start: u8,
    pub end: u8,
    pub next: StateId,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct SparseTransition {
    pub transitions: Box<[Transition]>,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub(crate) struct DenseTransition {
    pub transitions: Box<[StateId]>,
}

pub(crate) enum State {
    ByteRange {
        transition: Transition,
    },
    Sparse(SparseTransition),
    Dense(DenseTransition),
    Look {
        look: Look,
        next: StateId,
    },
    Union {
        alternates: Box<[StateId]>,
    },
    Capture {
        next: StateId,
        pattern_id: PatternId,
        group_idx: u32,
        slot: u32,
    },
    BinaryUnion {
        alt1: StateId,
        alt2: StateId,
    },
    Fail,
    Match {
        pattern_id: PatternId,
    },
}

impl State {
    #[inline]
    pub const fn is_epsilon(&self) -> bool {
        match *self {
            State::ByteRange { .. }
            | State::Sparse { .. }
            | State::Dense { .. }
            | State::Fail
            | State::Match { .. } => false,
            State::Union { .. }
            | State::Look { .. }
            | State::BinaryUnion { .. }
            | State::Capture { .. } => true,
        }
    }
}

impl Transition {
    fn matches(&self, haystack: &[u8], at: usize) -> bool {
        haystack
            .get(at)
            .map_or(false, |&byte| self.match_byte(byte))
    }

    fn match_byte(&self, byte: u8) -> bool {
        self.start <= byte && self.end >= byte
    }
}

impl SparseTransition {
    #[inline]
    fn matches(&self, haystack: &[u8], at: usize) -> Option<StateId> {
        haystack.get(at).and_then(|&byte| self.match_byte(byte))
    }

    #[inline]
    fn match_byte(&self, byte: u8) -> Option<StateId> {
        for transition in self.transitions.iter() {
            if transition.start > byte {
                break;
            } else if transition.match_byte(byte) {
                return Some(transition.next);
            }
        }

        None
    }
}

impl DenseTransition {
    #[inline]
    fn matches(&self, haystack: &[u8], at: usize) -> Option<StateId> {
        haystack.get(at).and_then(|&byte| self.match_byte(byte))
    }

    fn match_byte(&self, byte: u8) -> Option<StateId> {
        let next = self.transitions[usize::from(byte)];

        match next == 0 {
            true => None,
            false => Some(next),
        }
    }
}

impl Debug for Transition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use crate::DebugByte;

        let Transition { start, end, next } = *self;
        match start == end {
            true => write!(f, "{:?} => {:?}", DebugByte(start), next.as_usize()),
            false => write!(
                f,
                "{:?}-{:?} => {:?}",
                DebugByte(start),
                DebugByte(end),
                next.as_usize()
            ),
        }
    }
}
