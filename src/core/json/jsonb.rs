#![allow(unused)]

use super::JsonError;
use crate::parse_error;
use std::{borrow::Cow, cmp::Ordering, hint::unreachable_unchecked, str::FromStr};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Jsonb {
    data: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct JsonHeader(pub ElementType, pub usize);

pub(crate) enum Header {
    Inline([u8; 1]),
    OneByte([u8; 2]),
    TwoBytes([u8; 3]),
    FourBytes([u8; 5]),
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonIdentation<'i> {
    Identation(Cow<'i, str>),
    None,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[allow(unused)]
pub enum ElementType {
    NULL = 0,
    TRUE,
    FALSE,
    INT,
    INT5,
    FLOAT,
    FLOAT5,
    TEXT,
    TEXTJ,
    TEXT5,
    TEXTRAW,
    ARRAY,
    OBJECT,
    RESERVED1,
    RESERVED2,
    RESERVED3,
}

type Table = [u8; 256];

const MARKER_8BIT: u8 = 12;
const MARKER_16BIT: u8 = 13;
const MARKER_32BIT: u8 = 14;
const INFINITY_CHAR: u8 = 5;
const MAX_DEPTH: usize = 1000;

static WS_TABLE: Table = whitespace_table();
static CHARACTER_TYPE: Table = char_type_table();
static CHARACTER_OK_TYPE: Table = char_type_ok_table();

impl Jsonb {
    pub fn new(capacity: usize, data: Option<&[u8]>) -> Self {
        if let Some(data) = data {
            return Self {
                data: data.to_vec(),
            };
        }

        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub const fn len(&self) -> usize {
        self.data.len()
    }

    pub fn data(self) -> Vec<u8> {
        self.data
    }

    pub fn element_type(&self) -> super::Result<ElementType> {
        match self.read_header(0) {
            Ok((header, offset)) => match self.data.get(offset..offset + header.1).is_some() {
                true => Ok(header.0),
                _ => parse_error!("malformed JSON"),
            },

            _ => parse_error!("malformed JSON"),
        }
    }

    fn deserialize_value(
        &mut self,
        input: &[u8],
        mut position: usize,
        depth: usize,
    ) -> super::Result<usize> {
        if depth > MAX_DEPTH {
            parse_error!("Too deep");
        }

        position = Self::skip_whitespace(input, position);
        if position >= input.len() {
            parse_error!("Unexpected end of input");
        }

        match input[position] {
            _ => parse_error!("Unexpected character at {position}"),
        }
    }

    fn deserialize_obj(
        &mut self,
        input: &[u8],
        mut pos: usize,
        depth: usize,
    ) -> super::Result<usize> {
        if depth > MAX_DEPTH {
            parse_error!("Too deep");
        }

        if self.data.capacity() - self.data.len() < 50 {
            self.data.reserve(self.data.capacity());
        }
        if pos >= input.len() {
            parse_error!("Unexpected end of input");
        }

        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::OBJECT, 0, false)
            .map_err(|_| parse_error!("Failed to write to header"))?;

        let obj_start = self.len();
        let mut first = true;

        loop {
            pos = Self::skip_whitespace(input, pos);
            if pos >= input.len() {
                parse_error!("Unexpected end of input");
            }

            match input[pos] {
                b'}' => {
                    pos += 1;
                    if first {
                        return Ok(pos);
                    } else {
                        let obj_size = self.len() - obj_start;
                        self.write_element_header(header_pos, ElementType::OBJECT, obj_size, false)
                            .map_err(|_| parse_error!("Failed to write to header"))?;

                        return Ok(pos);
                    }
                }

                b',' if !first => {
                    pos += 1;
                    pos = Self::skip_whitespace(input, pos);
                    if input[pos] == b',' || input[pos] == b'{' {
                        parse_error!("Two commas in a row");
                    }
                }
                _ => {
                    pos = self.deserialize_string(input, pos)?;

                    pos = Self::skip_whitespace(input, pos);
                    if pos >= input.len() || input[pos] != b':' {
                        parse_error!("Expected : after object key");
                    }

                    pos += 1;
                    pos = Self::skip_whitespace(input, pos);

                    pos = self.deserialize_value(input, pos, depth + 1)?;
                    pos = Self::skip_whitespace(input, pos);
                    if pos < input.len() && !matches!(input[pos], b',' | b'}') {
                        parse_error!("Should be , or }}");
                    }
                    first = false;
                }
            }
        }
    }

    fn deserialize_string(&mut self, input: &[u8], mut pos: usize) -> super::Result<usize> {
        if pos >= input.len() {
            parse_error!("Unexpected end of input");
        }

        let string_start = self.len();
        let quote = input[pos];
        pos += 1;

        let quoted = quote == b'"' || quote == b'\'';
        let mut len = 0;

        if quoted {
            let mut end_pos = pos;
            let is_simple = true;

            while end_pos < input.len() {
                let c = input[end_pos];
                if c == quote {
                    if is_simple {
                        let len = end_pos - pos;
                        let header_pos = self.data.len();

                        if len <= 11 {
                            self.data
                                .push((ElementType::TEXT as u8) | ((len as u8) << 4));
                        } else {
                            self.write_element_header(header_pos, ElementType::TEXT, len, false)
                                .map_err(|_| parse_error!("Failed to write header"))?;
                        }

                        self.data.extend_from_slice(&input[pos..end_pos]);
                        return Ok(end_pos + 1);
                    }
                    break;
                } else if c == b'\\' || c < 32 {
                    break;
                }
                end_pos += 1;
            }
        }

        self.write_element_header(string_start, ElementType::TEXT, 0, false)
            .map_err(|_| parse_error!("Failed to write header"))?;

        if pos >= input.len() {
            parse_error!("Unexpected end of input");
        }

        let mut element_type = ElementType::TEXT;

        if !quoted {
            self.data.push(quote);
            len += 1;

            if pos < input.len() && input[pos] == b':' {
                return Ok(pos);
            }
        }

        let mut escape_buffer = [0u8; 6];

        while pos < input.len() {
            let c = input[pos];
            pos += 1;

            if quoted && c == quote {
                break;
            } else if !quoted && (c == b'"' || c == b'\'') {
                parse_error!("Unexpected input");
            } else if c == b'\\' {
                if pos >= input.len() {
                    parse_error!("Unexpected end of input");
                }

                let esc = input[pos];
                pos += 1;

                match esc {
                    b'b' => {
                        self.data.extend_from_slice(b"\\b");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'f' => {
                        self.data.extend_from_slice(b"\\f");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'n' => {
                        self.data.extend_from_slice(b"\\n");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'r' => {
                        self.data.extend_from_slice(b"\\r");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b't' => {
                        self.data.extend_from_slice(b"\\t");
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'\\' | b'"' | b'/' => {
                        self.data.push(b'\\');
                        self.data.push(esc);
                        len += 2;
                        element_type = ElementType::TEXTJ;
                    }
                    b'u' => {
                        if pos + 4 > input.len() {
                            parse_error!("Incomplete unicode escape sequence")
                        }

                        escape_buffer[0] = b'\\';
                        escape_buffer[1] = b'u';

                        for i in 0..4 {
                            let h = input[pos + i];
                            escape_buffer[2 + i] = h;
                        }

                        self.data.extend_from_slice(&escape_buffer[0..6]);
                        len += 6;
                        pos += 4;
                        element_type = ElementType::TEXTJ;
                    }
                    b'\n' => {
                        self.data.extend_from_slice(b"\\\n");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'\'' => {
                        self.data.extend_from_slice(b"\\\'");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'0' => {
                        self.data.extend_from_slice(b"\\0");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'v' => {
                        self.data.extend_from_slice(b"\\v");
                        len += 2;
                        element_type = ElementType::TEXT5;
                    }
                    b'x' => {
                        if pos + 2 > input.len() {
                            parse_error!("Incomplete hex escape sequence")
                        }

                        escape_buffer[0] = b'\\';
                        escape_buffer[1] = b'x';

                        for i in 0..2 {
                            let h = input[pos + i];
                            escape_buffer[2 + i] = h;
                        }

                        self.data.extend_from_slice(&escape_buffer[0..4]);
                        len += 4;
                        pos += 2;
                        element_type = ElementType::TEXT5;
                    }

                    _ => parse_error!("Invalid escape sequence"),
                }
            } else if !quoted && (c == b':' || c.is_ascii_whitespace()) {
                pos -= 1;
                break;
            } else if c <= 0x1F {
                element_type = ElementType::TEXT5;
                self.data.push(c);
                len += 1;
            } else {
                self.data.push(c);
                len += 1;
            }
        }

        self.write_element_header(string_start, element_type, len, false)
            .map_err(|_| parse_error!("Failed to write header"))?;

        Ok(pos)
    }

    fn write_element_header(
        &mut self,
        cursor: usize,
        element_type: ElementType,
        payload_size: usize,
        size_might_change: bool,
    ) -> super::Result<usize> {
        if payload_size <= 11 && !size_might_change {
            let header_byte = (element_type as u8) | ((payload_size as u8) << 4);
            match cursor == self.len() {
                true => self.data.push(header_byte),
                _ => self.data[cursor] = header_byte,
            }
            return Ok(1);
        }

        let header = JsonHeader::new(element_type, payload_size).into_bytes();
        let header_bytes = header.as_bytes();
        let header_len = header_bytes.len();

        if cursor == self.len() {
            self.data.extend_from_slice(header_bytes);
            return Ok(header_len);
        }

        let old_len = match size_might_change {
            false => 1,
            _ => {
                let (_, offset) = self.read_header(cursor)?;
                offset
            }
        };

        let new_len = header_bytes.len();

        match new_len.cmp(&old_len) {
            Ordering::Greater => {
                self.data.splice(
                    cursor + old_len..cursor + old_len,
                    std::iter::repeat_n(0, new_len - old_len),
                );
            }
            Ordering::Less => {
                self.data.drain(cursor + new_len..cursor + old_len);
            }
            Ordering::Equal => {}
        }

        for (i, &byte) in header_bytes.iter().enumerate() {
            self.data[cursor + i] = byte;
        }

        Ok(new_len)
    }

    fn read_header(&self, cursor: usize) -> super::Result<(JsonHeader, usize)> {
        JsonHeader::from_slice(cursor, &self.data)
    }

    pub fn skip_whitespace(input: &[u8], mut pos: usize) -> usize {
        while pos < input.len() && WS_TABLE[input[pos] as usize] == 1 {
            pos += 1;
        }

        pos
    }
}

impl FromStr for Jsonb {
    type Err = super::JsonError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut result = Self::new(s.len(), None);
        let input = s.as_bytes();
        if input.is_empty() {
            parse_error!("Unexpected input after JSON");
        }

        let mut pos = 0;
        pos = result.deserialize_value(input, pos, 0)?;
        pos = Self::skip_whitespace(input, pos);

        if pos < input.len() {
            parse_error!("Unexpected input after JSON");
        }

        Ok(result)
    }
}

impl JsonHeader {
    fn new(element_type: ElementType, payload_size: usize) -> Self {
        Self(element_type, payload_size)
    }

    pub fn from_slice(cursor: usize, slice: &[u8]) -> super::Result<(Self, usize)> {
        match slice.get(cursor) {
            Some(header) => {
                let element_type = header & 15;

                if element_type > 15 {
                    parse_error!("Invalid element type: {element_type}");
                }

                let header_size = header >> 4;
                let offset: usize;

                let total_size = match header_size {
                    size if size <= 11 => {
                        offset = 1;
                        size as usize
                    }

                    12 => match slice.get(cursor + 1) {
                        Some(value) => {
                            offset = 2;
                            *value as usize
                        }
                        None => parse_error!("Failed to read 1-byte size"),
                    },

                    13 => match slice.get(cursor + 1..cursor + 3) {
                        Some(bytes) => {
                            offset = 3;
                            u16::from_be_bytes([bytes[0], bytes[1]]) as usize
                        }
                        None => parse_error!("Failed to read 2-byte size"),
                    },

                    14 => match slice.get(cursor + 1..cursor + 5) {
                        Some(bytes) => {
                            offset = 5;
                            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
                        }
                        None => parse_error!("Failed to read 4-byte size"),
                    },

                    _ => unreachable!(),
                };

                Ok((Self(element_type.try_into()?, total_size), offset))
            }
            None => parse_error!("Failed to read header byte"),
        }
    }

    pub fn into_bytes(self) -> Header {
        let (element_type, payload_size) = (self.0, self.1);
        match payload_size {
            size if size <= 11 => Header::Inline([(element_type as u8) | ((size as u8) << 4)]),

            size if size <= 0xFF => {
                Header::OneByte([(element_type as u8) | (MARKER_8BIT << 4), size as u8])
            }

            size if size <= 0xFFFF => {
                let size_bytes = (size as u16).to_be_bytes();
                Header::TwoBytes([
                    (element_type as u8) | (MARKER_16BIT << 4),
                    size_bytes[0],
                    size_bytes[1],
                ])
            }

            size if size <= 0xFFFFFFFF => {
                let size_bytes = (size as u32).to_be_bytes();
                Header::FourBytes([
                    (element_type as u8) | (MARKER_32BIT << 4),
                    size_bytes[0],
                    size_bytes[1],
                    size_bytes[2],
                    size_bytes[3],
                ])
            }
            _ => panic!("Payload size too large for encoding"),
        }
    }

    pub fn null() -> Self {
        Self(ElementType::NULL, 0)
    }

    pub fn object() -> Self {
        Self(ElementType::OBJECT, 1)
    }

    pub fn element_type(&self) -> ElementType {
        self.0
    }

    pub fn payload_size(&self) -> usize {
        self.1
    }
}

impl Header {
    pub const fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Inline(bytes) => bytes,
            Self::OneByte(bytes) => bytes,
            Self::TwoBytes(bytes) => bytes,
            Self::FourBytes(bytes) => bytes,
        }
    }
}

impl<'i> JsonIdentation<'i> {
    pub const fn is_pretty(&self) -> bool {
        matches!(self, Self::Identation(_))
    }
}

impl ElementType {
    pub const fn is_valid_key(&self) -> bool {
        matches!(self, Self::TEXT | Self::TEXT5 | Self::TEXTJ | Self::TEXTRAW)
    }
}

impl From<ElementType> for String {
    fn from(value: ElementType) -> Self {
        match value {
            ElementType::ARRAY => "array".to_string(),
            ElementType::OBJECT => "object".to_string(),
            ElementType::NULL => "null".to_string(),
            ElementType::TRUE => "true".to_string(),
            ElementType::FALSE => "false".to_string(),
            ElementType::FLOAT | ElementType::FLOAT5 => "real".to_string(),
            ElementType::INT | ElementType::INT5 => "integer".to_string(),
            ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
                "text".to_string()
            }

            _ => unsafe { unreachable_unchecked() },
        }
    }
}

impl TryFrom<u8> for ElementType {
    type Error = JsonError;
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::NULL),
            1 => Ok(Self::TRUE),
            2 => Ok(Self::FALSE),
            3 => Ok(Self::INT),
            4 => Ok(Self::INT5),
            5 => Ok(Self::FLOAT),
            6 => Ok(Self::FLOAT5),
            7 => Ok(Self::TEXT),
            8 => Ok(Self::TEXTJ),
            9 => Ok(Self::TEXT5),
            10 => Ok(Self::TEXTRAW),
            11 => Ok(Self::ARRAY),
            12 => Ok(Self::OBJECT),
            13 => Ok(Self::RESERVED1),
            14 => Ok(Self::RESERVED2),
            15 => Ok(Self::RESERVED3),
            _ => parse_error!("Failed to recognize jsonvalue type"),
        }
    }
}

const fn whitespace_table() -> Table {
    let mut table = [0u8; 256];

    table[0x09] = 1;
    table[0x0A] = 1;
    table[0x0D] = 1;
    table[0x20] = 1;

    table
}

const fn char_type_table() -> Table {
    let mut table = [0u8; 256];

    table[0x09] = 1;
    table[0x0A] = 1;
    table[0x0D] = 1;
    table[0x20] = 1;
    table[0x30] = 2;
    table[0x31] = 2;
    table[0x32] = 2;
    table[0x33] = 2;
    table[0x34] = 2;
    table[0x35] = 2;
    table[0x36] = 2;
    table[0x37] = 2;
    table[0x38] = 2;
    table[0x39] = 2;
    table[0x41] = 3;
    table[0x42] = 3;
    table[0x43] = 3;
    table[0x44] = 3;
    table[0x45] = 3;
    table[0x46] = 3;
    table[0x61] = 3;
    table[0x62] = 3;
    table[0x63] = 3;
    table[0x64] = 3;
    table[0x65] = 3;
    table[0x66] = 3;

    table
}

const fn char_type_ok_table() -> Table {
    let mut table = [0u8; 256];

    let mut i = 0x20;
    while i <= 0x7E {
        if i != 0x22 && i != 0x5C {
            table[i] |= 4;
        }
        i += 1;
    }
    table
}
