use crate::sql::statement::Type;

pub(in crate::core) mod btree;
pub mod date;
mod page;
mod pagination;
pub(in crate::core) mod random;

type PageNumber = u32;

/// Returns the byte length of a given SQL [`Type`].
fn byte_len_of_type(data_type: &Type) -> usize {
    match data_type {
        Type::BigInteger | Type::UnsignedBigInteger | Type::DateTime => 8,
        Type::Integer | Type::UnsignedInteger | Type::Date => 4,
        Type::Time => 3, // TODO: this can be 4, but let it happen
        Type::Boolean => 1,
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
