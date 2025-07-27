# UMBRA BINARY PROTOCOL v0.2

**The Shadow Whisper**


## Message Format:

Each message is structured as follows:

[ 4 bytes ] - SQL Content length (u32, little-endian)

[ 1 byte  ] - Message type (ASCII character)

[ n bytes ] - Payload (depends on message type)

## Message Types:

(+) -> (0x2B) - `QuerySet` Response
(!) -> (0x21) - Empty / OK Response
(-) -> (0x2D) - Error Response

## Payload Details:

### (+) `QuerySet` Response:

[ 2 bytes ] - Column count (u16, little-endian)

For each column:
  [ 2 bytes ] - Column name length (u16, LE)

  [ n bytes ] - Column name (UTF-8)

  [ 1 byte  ] - Type code (see below)
      If `VARCHAR`:
        [ 4 bytes ] - Max length (u32, LE)

[ 4 bytes ] - Row count (u32, LE)

For each row:
  [ n bytes ] - Serialized row data (encoded per column type)

### (!) Empty / OK Response:

[ 4 bytes ] - Affected rows count (u32, LE)

### (-) Error Response:

[ n bytes ] - UTF-8 encoded error message


## Type Encoding:

Each column type is encoded as a single byte:

Boolean Types:
 - 0x00 - `BOOLEAN`

Integer Types:
 - 0x10 - `SMALLINT`
 - 0x11 - `UNSIGNED SMALLINT`
 - 0x12 - `INTEGER`
 - 0x13 - `UNSIGNED INTEGER`
 - 0x14 - `BIGINT`
 - 0x15 - `UNSIGNED BIGINT`
 - 0x16 - `SMALLSERIAL`
 - 0x17 - `SERIAL`
 - 0x18 - `BIGSERIAL`

String Types:
 - 0x40 - `VARCHAR` (requires additional 4-byte max length)
 - 0x41 - `TEXT` (no max length field)

Temporal Types:
 - 0x50 - `DATE`
 - 0x51 - `TIME`
 - 0x52 - `TIMESTAMP`


## Connection Flow:

1. Client connects via TCP
2. Messages are exchanged using the format above

## Notes:

- All numbers are little-endian
- Strings are UTF-8 encoded

*"The protocol speaks in shadows — implement it faithfully."* 🦇

