// ! SQL statements and types declaration.

/// SQL data types.
#[derive(Debug, PartialEq)]
pub(crate) enum Type {
    Integer,
    UnsignedInteger,
    BigInteger,
    UnsignedBigInteger,
    Boolean,
    Varchar(usize)
}