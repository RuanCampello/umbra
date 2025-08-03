//! Look around assertion.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum Look {
    Start = 1 << 0,
    End = 1 << 1,
    StartLf = 1 << 2,
    EndLf = 1 << 3,
    StartCrLf = 1 << 4,
    EndCrLf = 1 << 5,
    WordAscii = 1 << 6,
    WordAsciiNeg = 1 << 7,
    WordUnicode = 1 << 8,
    WordUnicodeNeg = 1 << 9,
    WordStartAscii = 1 << 10,
    WordEndAscii = 1 << 11,
    WordStartUnicode = 1 << 12,
    WordEndUnicode = 1 << 13,
    WordStartHalfAscii = 1 << 14,
    WordEndHalfAscii = 1 << 15,
    WordStartHalfUnicode = 1 << 16,
    WordEndHalfUnicode = 1 << 17,
}

impl Look {
    #[inline]
    pub const fn reversed(self) -> Look {
        match self {
            Look::Start => Look::End,
            Look::End => Look::Start,
            Look::StartLf => Look::EndLf,
            Look::EndLf => Look::StartLf,
            Look::StartCrLf => Look::EndCrLf,
            Look::EndCrLf => Look::StartCrLf,
            Look::WordAscii => Look::WordAscii,
            Look::WordAsciiNeg => Look::WordAsciiNeg,
            Look::WordUnicode => Look::WordUnicode,
            Look::WordUnicodeNeg => Look::WordUnicodeNeg,
            Look::WordStartAscii => Look::WordEndAscii,
            Look::WordEndAscii => Look::WordStartAscii,
            Look::WordStartUnicode => Look::WordEndUnicode,
            Look::WordEndUnicode => Look::WordStartUnicode,
            Look::WordStartHalfAscii => Look::WordEndHalfAscii,
            Look::WordEndHalfAscii => Look::WordStartHalfAscii,
            Look::WordStartHalfUnicode => Look::WordEndHalfUnicode,
            Look::WordEndHalfUnicode => Look::WordStartHalfUnicode,
        }
    }

    #[inline]
    pub const fn repr(self) -> u32 {
        self as u32
    }
}
