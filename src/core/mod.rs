use crate::sql::statements::Type;

pub mod btree;
pub mod io;
mod pager;

type PageNumber = u16;

/// Returns the byte length of a given SQL [`Type`] for integers.
fn byte_len_of_int_type(data_type: &Type) -> usize {
    match data_type {
        Type::Integer | Type::UnsignedInteger => 4,
        Type::BigInteger | Type::UnsignedBigInteger => 8,
        _ => unreachable!("This must only be used for integers."),
    }
}

/// Returns the length of bytes needed to storage a given `Varchar` [`Type`].
fn utf_8_length_bytes(max_size: usize) -> usize {
    match max_size {
        0..64 => 1,
        64..16384 => 2,
        _ => 4,
    }
}
