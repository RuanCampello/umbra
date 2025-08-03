//! Provides non-deterministic finite automata (NFA) and the engines that use them.

use std::{fmt::Debug, sync::Arc};

use crate::{SmallIdx, StateId, U32, fsm::PatternId, look::Look};

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
        group_idx: SmallIdx,
        slot: SmallIdx,
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

    const fn memory_usage(&self) -> usize {
        match *self {
            State::ByteRange { .. }
            | State::BinaryUnion { .. }
            | State::Capture { .. }
            | State::Look { .. }
            | State::Match { .. }
            | State::Fail => 0,
            State::Dense { .. } => 256 * core::mem::size_of::<StateId>(),
            State::Sparse(SparseTransition { ref transitions }) => {
                transitions.len() * core::mem::size_of::<Transition>()
            }
            State::Union { ref alternates } => alternates.len() * core::mem::size_of::<StateId>(),
        }
    }

    fn remap(&mut self, remap: &[StateId]) {
        match *self {
            State::ByteRange { ref mut transition } => transition.next = remap[transition.next],
            State::Sparse(SparseTransition {
                ref mut transitions,
            }) => {
                for t in transitions.iter_mut() {
                    t.next = remap[t.next];
                }
            }
            State::Dense(DenseTransition {
                ref mut transitions,
            }) => {
                for sid in transitions.iter_mut() {
                    *sid = remap[*sid];
                }
            }
            State::Look { ref mut next, .. } => *next = remap[*next],
            State::Union { ref mut alternates } => {
                for alt in alternates.iter_mut() {
                    *alt = remap[*alt];
                }
            }
            State::BinaryUnion {
                ref mut alt1,
                ref mut alt2,
            } => {
                *alt1 = remap[*alt1];
                *alt2 = remap[*alt2];
            }
            State::Capture { ref mut next, .. } => *next = remap[*next],
            State::Fail => {}
            State::Match { .. } => {}
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

        match next == StateId(SmallIdx::ZERO) {
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
