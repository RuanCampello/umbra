//! Little-endian encoding helpers shared by log records and checkpoint images

use crate::core::crc32c::crc32c_append;
use crate::sql::Value;
use crate::storage::engine::error::{EngineError, Result};
use crate::storage::engine::table::Row;

/// Sequential reader over a byte stream that checksums everything it consumes, every short read is
/// reported as corruption of `what`
pub(crate) struct Stream<R> {
    reader: R,
    crc: u32,
    what: &'static str,
    position: u64,
}

/// Bounds-checked reader, every short read is reported as corruption of `what`
pub(crate) struct Cursor<'b> {
    bytes: &'b [u8],
    position: usize,
    what: &'static str,
}

impl<'b> Cursor<'b> {
    pub const fn new(bytes: &'b [u8], what: &'static str) -> Self {
        Self { bytes, position: 0, what }
    }

    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.position == self.bytes.len()
    }

    pub fn bytes(&mut self, len: usize) -> Result<&'b [u8]> {
        let end = self
            .position
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| self.corrupt("truncated"))?;

        let bytes = &self.bytes[self.position..end];
        self.position = end;
        Ok(bytes)
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    #[inline]
    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.bytes(2)?.try_into().unwrap()))
    }

    #[inline]
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    #[inline]
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    #[inline]
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    pub fn string(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        String::from_utf8(self.bytes(len)?.to_vec()).map_err(|_| self.corrupt("invalid UTF-8"))
    }

    /// a length-prefixed blob (`u32` length)
    pub fn blob(&mut self) -> Result<&'b [u8]> {
        let len = self.u32()? as usize;
        self.bytes(len)
    }

    pub fn row(&mut self) -> Result<Row> {
        let count = self.u16()? as usize;
        let mut values = Vec::with_capacity(count);

        for _ in 0..count {
            let (value, consumed) = Value::deserialise(&self.bytes[self.position..])
                .map_err(|_| self.corrupt("invalid value"))?;
            self.position += consumed;
            values.push(value);
        }

        Ok(values.into())
    }

    pub fn corrupt(&self, why: &str) -> EngineError {
        EngineError::Corrupt(format!("{} at byte {}: {why}", self.what, self.position))
    }
}

impl<R: std::io::Read> Stream<R> {
    pub const fn new(reader: R, what: &'static str) -> Self {
        Self { reader, crc: 0, what, position: 0 }
    }

    pub fn exact<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0; N];
        self.fill(&mut bytes)?;
        Ok(bytes)
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.exact::<1>()?[0])
    }

    #[inline]
    pub fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.exact()?))
    }

    #[inline]
    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.exact()?))
    }

    #[inline]
    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.exact()?))
    }

    #[inline]
    pub fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_le_bytes(self.exact()?))
    }

    /// a length-prefixed blob (`u32` length, at most 1 GiB so a corrupt length cannot exhaust
    /// memory before the checksum is reached)
    pub fn blob(&mut self) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        if len > 1 << 30 {
            return Err(self.corrupt("implausible length"));
        }

        let mut bytes = vec![0; len];
        self.fill(&mut bytes)?;
        Ok(bytes)
    }

    pub fn string(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        let mut bytes = vec![0; len];
        self.fill(&mut bytes)?;
        String::from_utf8(bytes).map_err(|_| self.corrupt("invalid UTF-8"))
    }

    /// checks the CRC32C footer against everything read so far and that the stream ends there
    pub fn finish(mut self) -> Result<()> {
        let expected = self.crc;
        let mut footer = [0; 4];
        self.reader
            .read_exact(&mut footer)
            .map_err(|_| self.corrupt("missing checksum"))?;

        if u32::from_le_bytes(footer) != expected {
            return Err(self.corrupt("checksum mismatch"));
        }

        match self.reader.read(&mut [0]) {
            Ok(0) => Ok(()),
            _ => Err(self.corrupt("trailing bytes")),
        }
    }

    pub fn corrupt(&self, why: &str) -> EngineError {
        EngineError::Corrupt(format!("{} at byte {}: {why}", self.what, self.position))
    }

    fn fill(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.reader.read_exact(bytes).map_err(|_| self.corrupt("truncated"))?;
        self.crc = crc32c_append(self.crc, bytes);
        self.position += bytes.len() as u64;
        Ok(())
    }
}

#[inline]
pub(crate) fn put_string(buf: &mut Vec<u8>, text: &str) {
    buf.extend_from_slice(&(text.len() as u16).to_le_bytes());
    buf.extend_from_slice(text.as_bytes());
}

#[inline]
pub(crate) fn put_blob(buf: &mut Vec<u8>, bytes: &[u8]) {
    buf.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(bytes);
}

/// column count, then each value's self-delimiting encoding
pub(crate) fn put_row(buf: &mut Vec<u8>, row: &[Value]) {
    buf.extend_from_slice(&(row.len() as u16).to_le_bytes());
    for value in row {
        value.serialise_into(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn rows_round_trip_and_truncation_is_corruption() {
        let row: Row = Arc::from([
            Value::Number(-7),
            Value::String("umbra".into()),
            Value::Null,
            Value::Float(1.5),
        ]);

        let mut buf = Vec::new();
        put_row(&mut buf, &row);
        put_string(&mut buf, "name");

        let mut cursor = Cursor::new(&buf, "test");
        assert_eq!(cursor.row().unwrap(), row);
        assert_eq!(cursor.string().unwrap(), "name");
        assert!(cursor.is_empty());

        let mut short = Cursor::new(&buf[..buf.len() - 1], "test");
        short.row().unwrap();
        assert!(matches!(short.string(), Err(EngineError::Corrupt(_))));
    }
}
