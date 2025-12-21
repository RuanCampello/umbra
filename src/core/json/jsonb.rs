#![allow(unused)]

use super::JsonError;
use crate::{
    core::json::{
        path::{JsonPath, PathElement},
        Conv,
    },
    parse_error,
};
use std::{
    borrow::Cow, cmp::Ordering, collections::HashMap, fmt::Display, hint::unreachable_unchecked,
    str::FromStr,
};

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct Jsonb {
    data: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct JsonHeader(pub ElementType, pub usize);

pub(crate) struct TransversalResult {
    key_index: LocationKind,
    value_index: usize,
    delta: isize,
    array_position: Option<ArrayPosition>,
}

pub(crate) enum Header {
    Inline([u8; 1]),
    OneByte([u8; 2]),
    TwoBytes([u8; 3]),
    FourBytes([u8; 5]),
}

#[derive(Debug, PartialEq, Clone)]
pub enum JsonIndentation<'i> {
    Indentation(Cow<'i, str>),
    None,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PathOperation {
    ReplaceExisting,
    Upsert,
    Insert,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum LocationKind {
    Object(usize),
    Root,
    ArrayEntry,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum ArrayPosition {
    Index(usize),
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum SegmentVariant<'s> {
    Single(&'s PathElement<'s>),
    KeyWithArrayIndex(&'s PathElement<'s>, &'s PathElement<'s>),
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

    pub fn from_data(data: &[u8]) -> Self {
        Self::new(data.len(), Some(data))
    }

    pub fn from_str_with_mode(input: &str, conversion: Conv) -> super::Result<Self> {
        match matches!(conversion, Conv::ToString) {
            true => {
                let mut str = input.replace('\\', "\\\\").replace('"', "\\\"");
                str.insert(0, '"');
                str.push('"');

                Jsonb::from_str(&str)
            }
            _ => Jsonb::from_str(input),
        }
    }

    pub fn to_string_pretty(&self, indentation: Option<&str>) -> super::Result<String> {
        let mut result = String::with_capacity(self.data.len() * 2);
        let indent = JsonIndentation::Indentation(match indentation {
            Some(indent) => Cow::Borrowed(indent),
            _ => Cow::Borrowed("    "),
        });

        self.write_string(&mut result, indent);
        Ok(result)
    }

    pub fn is_valid(&self) -> bool {
        self.validate(0, self.data.len(), 0).is_ok()
    }

    pub fn navigate_path(
        &mut self,
        path: &JsonPath,
        mode: PathOperation,
    ) -> super::Result<Vec<TransversalResult>> {
        let mut iter = path.elements.iter().peekable();
        let mut cursor = 0;
        let mut stack: Vec<TransversalResult> = Vec::with_capacity(path.elements.len());

        while let Some(current) = iter.next() {
            let next_is_array = matches!(iter.peek(), Some(PathElement::ArrayLocator(_)))
                && !matches!(current, PathElement::ArrayLocator(_));

            let is_intermediate = match next_is_array {
                false => iter.peek().is_some(),
                _ => {
                    let mut temp = iter.clone();
                    temp.next();
                    temp.peek().is_some()
                }
            };

            let mode = match is_intermediate {
                true => PathOperation::Upsert,
                _ => mode,
            };

            let result = match next_is_array {
                false => self.navigate_segment(SegmentVariant::Single(current), cursor, mode)?,
                _ => {
                    let locator = iter.next().ok_or_else(|| {
                        parse_error("Array locator must exist after peek", Some(cursor))
                    })?;

                    self.navigate_segment(
                        SegmentVariant::KeyWithArrayIndex(current, locator),
                        cursor,
                        mode,
                    )?
                }
            };

            cursor = match &result.array_position {
                Some(ArrayPosition::Index(index)) => *index,
                _ => result.value_index,
            };

            stack.push(result);
        }

        Ok(stack)
    }

    fn navigate_segment(
        &mut self,
        segment: SegmentVariant,
        mut pos: usize,
        mode: PathOperation,
    ) -> super::Result<TransversalResult> {
        let (JsonHeader(element_type, element_size), header_size) = self.read_header(pos)?;

        match segment {
            SegmentVariant::Single(PathElement::Root) => {
                return Ok(TransversalResult::new(pos, LocationKind::Root, 0))
            }
            SegmentVariant::Single(PathElement::ArrayLocator(index)) => {
                let (JsonHeader(root_type, root_size), root_header_size) = self.read_header(pos)?;

                match root_type {
                    ElementType::ARRAY => {
                        let end_position = pos + root_header_size + root_size;

                        match index {
                            Some(index) if *index >= 0 => {
                                let mut count = 0;
                                let mut array_position = pos + root_header_size;

                                while array_position < end_position && count != *index as usize {
                                    array_position = self.skip_element(array_position)?;
                                    count += 1;
                                }

                                if mode.allows_insert() && array_position == end_position {
                                    let placeholder =
                                        JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                                    let placeholder_bytes = placeholder.as_bytes();

                                    self.data.splice(
                                        array_position..array_position,
                                        placeholder_bytes.iter().copied(),
                                    );

                                    return Ok(TransversalResult::with_array_index(
                                        pos + root_header_size,
                                        LocationKind::ArrayEntry,
                                        placeholder_bytes.len() as isize,
                                        array_position,
                                    ));
                                }

                                if array_position != end_position && mode.allows_replace() {
                                    return Ok(TransversalResult::with_array_index(
                                        pos,
                                        LocationKind::ArrayEntry,
                                        0,
                                        array_position,
                                    ));
                                }

                                parse_error!("Not found");
                            }

                            Some(index) if *index < 0 => {
                                let mut index_map: HashMap<i32, usize> =
                                    HashMap::with_capacity(100);
                                let mut element_index = 0;
                                let mut array_position = pos + root_header_size;

                                while array_position < end_position {
                                    index_map.insert(element_index, array_position);
                                    array_position = self.skip_element(array_position)?;
                                    element_index += 1;
                                }

                                let index = element_index + index;
                                match index_map.get(&index) {
                                    Some(index) => {
                                        return Ok(TransversalResult::with_array_index(
                                            pos,
                                            LocationKind::ArrayEntry,
                                            0,
                                            *index,
                                        ))
                                    }
                                    _ => parse_error!("Element with negative index not foun"),
                                };
                            }

                            _ => unreachable!(),
                        }
                    }
                    ElementType::OBJECT
                        if root_size == 0
                            && (*index == Some(0) || index.is_none())
                            && mode.allows_insert() =>
                    {
                        let array = JsonHeader::new(ElementType::ARRAY, 0).into_bytes();
                        let array_bytes = array.as_bytes();
                        let placeholder = JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                        let placeholder_bytes = placeholder.as_bytes();

                        self.data.splice(
                            pos..pos + root_header_size,
                            array_bytes
                                .iter()
                                .copied()
                                .chain(placeholder_bytes.iter().copied()),
                        );

                        return Ok(TransversalResult::with_array_index(
                            pos,
                            LocationKind::ArrayEntry,
                            placeholder_bytes.len() as isize,
                            pos + array_bytes.len(),
                        ));
                    }
                    _ => parse_error!("Root is not an array"),
                }
            }
            SegmentVariant::Single(PathElement::Key(path_key, is_raw)) => match element_type {
                ElementType::OBJECT => {
                    let end_position = pos + element_size + header_size;
                    pos + header_size;

                    while pos < end_position {
                        let (JsonHeader(key_type, key_len), key_header_len) =
                            self.read_header(pos)?;

                        if !key_type.is_valid_key() {
                            parse_error!("Key should be string");
                        }

                        let key_start = pos + key_header_len;
                        let json_key = unsafe {
                            std::str::from_utf8_unchecked(
                                &self.data[key_start..key_start + key_len],
                            )
                        };

                        if compare((json_key, key_type), (path_key, *is_raw)) {
                            match mode.allows_replace() {
                                true => {
                                    let value_position = pos + key_header_len + key_len;
                                    let key_position = pos;

                                    return Ok(TransversalResult::new(
                                        value_position,
                                        LocationKind::Object(key_position),
                                        0,
                                    ));
                                }
                                _ => parse_error!("Can't replace"),
                            }
                        } else {
                            pos += key_header_len + key_len;
                            pos = self.skip_element(pos)?;
                        }
                    }

                    match mode.allows_insert() {
                        true => {
                            let key_type = match *is_raw {
                                true => ElementType::TEXTRAW,
                                _ => ElementType::TEXT,
                            };

                            let key_header = JsonHeader::new(key_type, path_key.len()).into_bytes();
                            let key_header_bytes = key_header.as_bytes();
                            let key_bytes = path_key.as_bytes();
                            let value_header = JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                            let value_header_bytes = value_header.as_bytes();

                            self.data.splice(
                                pos..pos,
                                key_header_bytes
                                    .iter()
                                    .copied()
                                    .chain(key_bytes.iter().copied())
                                    .chain(value_header_bytes.iter().copied()),
                            );

                            let key_index = pos;
                            let value_index = pos + key_header_bytes.len() + key_bytes.len();
                            let delta =
                                key_header_bytes.len() + key_bytes.len() + value_header_bytes.len();

                            return Ok(TransversalResult::new(
                                value_index,
                                LocationKind::Object(key_index),
                                delta as isize,
                            ));
                        }
                        _ => parse_error!("Mode doesn't allow insert cannot create another key"),
                    }
                }
                _ => parse_error!("Bah"),
            },
            SegmentVariant::KeyWithArrayIndex(
                PathElement::Root,
                PathElement::ArrayLocator(index),
            ) => {
                let (JsonHeader(root_type, root_size), root_header_size) = self.read_header(pos)?;

                match root_type {
                    ElementType::ARRAY => {
                        let end_position = pos + root_header_size + root_size;

                        match index {
                            Some(index) if *index >= 0 => {
                                let mut count = 0;
                                let mut array_position = pos + root_header_size;

                                while array_position <= end_position && count != *index as usize {
                                    array_position = self.skip_element(array_position)?;
                                    count += 1;
                                }

                                if mode.allows_insert()
                                    && array_position == end_position
                                    && count == *index as usize
                                {
                                    let placeholder =
                                        JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                                    let placeholder_bytes = placeholder.as_bytes();

                                    self.data.splice(
                                        array_position..array_position,
                                        placeholder_bytes.iter().copied(),
                                    );

                                    return Ok(TransversalResult::with_array_index(
                                        pos,
                                        LocationKind::Root,
                                        placeholder_bytes.len() as isize,
                                        array_position,
                                    ));
                                }

                                if array_position != end_position && mode.allows_replace() {
                                    return Ok(TransversalResult::with_array_index(
                                        pos,
                                        LocationKind::Root,
                                        0,
                                        array_position,
                                    ));
                                }

                                parse_error!("Not found")
                            }

                            Some(index) if *index < 0 => {
                                let mut index_map = HashMap::with_capacity(100);
                                let mut element_index = 0;
                                let mut array_position = pos + root_header_size;

                                while array_position < end_position {
                                    index_map.insert(element_index, array_position);
                                    array_position = self.skip_element(array_position)?;
                                    element_index += 1;
                                }

                                let index = element_index + index;

                                match index_map.get(&index) {
                                    Some(index) => {
                                        return Ok(TransversalResult::with_array_index(
                                            pos,
                                            LocationKind::Root,
                                            0,
                                            *index,
                                        ))
                                    }
                                    _ => parse_error!("Element with negative index was not found"),
                                }
                            }
                            _ => parse_error!("Root isn't an array"),
                        }
                    }
                    _ => parse_error!("Root isn't an array"),
                }
            }

            SegmentVariant::KeyWithArrayIndex(
                PathElement::Key(path_key, is_raw),
                PathElement::ArrayLocator(index),
            ) => {
                if element_type.ne(&ElementType::OBJECT) {
                    parse_error!("Element type isn't an object")
                }

                let end_position = pos + header_size + element_size;
                let mut current = pos + header_size;

                while current < end_position {
                    let (JsonHeader(key_type, key_size), key_header_size) =
                        self.read_header(current)?;

                    if !key_type.is_valid_key() {
                        parse_error!("Key must be string")
                    }

                    let object_key = unsafe {
                        std::str::from_utf8_unchecked(
                            &self.data
                                [current + key_header_size..current + key_header_size + key_size],
                        )
                    };

                    match compare((object_key, key_type), (path_key, *is_raw)) {
                        true => break,
                        _ => current = self.skip_element(current + key_size + key_header_size)?,
                    }
                }

                if current == end_position && mode.allows_insert() {
                    if let Some(index) = index {
                        if *index == 0 {
                            parse_error!("Cannot create an new array with index")
                        }
                    }

                    let key_header_type = match is_raw {
                        true => ElementType::TEXTRAW,
                        _ => ElementType::TEXT,
                    };

                    let key_header = JsonHeader::new(key_header_type, path_key.len()).into_bytes();
                    let key_header_bytes = key_header.as_bytes();
                    let key_bytes = path_key.as_bytes();
                    let array_header = JsonHeader::new(ElementType::ARRAY, 1).into_bytes();
                    let array_header_bytes = array_header.as_bytes();
                    let array_value_header = JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                    let array_value_header_bytes = array_value_header.as_bytes();

                    let delta = (key_header_bytes.len()
                        + key_bytes.len()
                        + array_header_bytes.len()
                        + array_value_header_bytes.len()) as isize;

                    self.data.splice(
                        current..current,
                        key_header_bytes
                            .iter()
                            .copied()
                            .chain(key_bytes.iter().copied())
                            .chain(array_header_bytes.iter().copied())
                            .chain(array_value_header_bytes.iter().copied()),
                    );

                    let key_index = current;
                    let value_index = current + key_header_bytes.len() + key_bytes.len();
                    let array_index = value_index + array_header_bytes.len();

                    return Ok(TransversalResult::with_array_index(
                        value_index,
                        LocationKind::Object(key_index),
                        delta,
                        array_index,
                    ));
                }

                if current != end_position && mode.allows_replace() {
                    let key_index = current;

                    current = self.skip_element(current)?;
                    let value_index = current;

                    let (JsonHeader(value_type, value_size), value_header_size) =
                        self.read_header(value_index)?;
                    if value_type.ne(&ElementType::ARRAY) {
                        parse_error!("ElementType should be an array")
                    }

                    let end_position = current + value_header_size + value_size;
                    match index {
                        Some(index) if *index >= 0 => {
                            let mut count = 0;
                            let mut array_position = value_index + value_header_size;

                            while array_position < end_position && count != *index as usize {
                                array_position = self.skip_element(array_position)?;
                                count += 1;
                            }

                            if mode.allows_insert()
                                && array_position == end_position
                                && count == *index as usize
                            {
                                let placeholder =
                                    JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();

                                self.data.splice(
                                    array_position..array_position,
                                    placeholder_bytes.iter().copied(),
                                );

                                self.write_element_header(
                                    value_index,
                                    ElementType::ARRAY,
                                    value_size + placeholder_bytes.len(),
                                    true,
                                )?;

                                return Ok(TransversalResult::with_array_index(
                                    value_index,
                                    LocationKind::Object(key_index),
                                    placeholder_bytes.len() as isize,
                                    array_position,
                                ));
                            }

                            if array_position != end_position && mode.allows_replace() {
                                return Ok(TransversalResult::with_array_index(
                                    value_index,
                                    LocationKind::Object(key_index),
                                    0,
                                    array_position,
                                ));
                            }

                            parse_error!("Not found")
                        }

                        Some(index) if *index < 0 => {
                            let mut index_map = HashMap::with_capacity(100);
                            let mut element_index = 0;
                            let mut array_position = value_index + value_header_size;

                            while array_position < end_position {
                                index_map.insert(element_index, array_position);
                                array_position = self.skip_element(array_position)?;
                                element_index += 1;
                            }

                            let index = element_index + index;

                            match index_map.get(&index) {
                                Some(index) => {
                                    return Ok(TransversalResult::with_array_index(
                                        value_index,
                                        LocationKind::Object(key_index),
                                        0,
                                        *index,
                                    ))
                                }
                                _ => parse_error!("Element at negative index {index} not found"),
                            }
                        }

                        Some(_) => unreachable!(),

                        None => match mode.allows_insert() {
                            false => parse_error!("Cannot insert item"),
                            _ => {
                                let placeholder =
                                    JsonHeader::new(ElementType::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();
                                let insert = value_index + value_size + value_header_size;

                                self.data.insert(insert, placeholder_bytes[0]);
                            }
                        },
                    }
                }
            }
            _ => unreachable!(),
        }

        Err(parse_error!("Not found"))
    }

    fn skip_element(&self, mut pos: usize) -> super::Result<usize> {
        let (header, skip_header) = self.read_header(pos)?;
        pos += skip_header + header.1;

        Ok(pos)
    }

    fn validate(&self, start: usize, end: usize, depth: usize) -> super::Result<()> {
        if depth > MAX_DEPTH {
            parse_error!("Too deep");
        }

        if start >= end {
            parse_error!("Empty element");
        }

        let (header, offset) = self.read_header(start)?;
        let start = start + offset;
        let payload_size = header.payload_size();
        let payload_end = start + payload_size;

        if payload_end != end {
            parse_error!("Size mismatch");
        }

        match header.element_type() {
            ElementType::NULL | ElementType::TRUE | ElementType::FALSE => match payload_size > 0 {
                true => Ok(()),
                _ => parse_error!("Invalid payload for primitive value"),
            },
            ElementType::INT | ElementType::INT5 | ElementType::FLOAT | ElementType::FLOAT5 => {
                match payload_size > 0 {
                    true => Ok(()),
                    _ => parse_error!("Empty number payload"),
                }
            }
            ElementType::TEXT | ElementType::TEXTJ | ElementType::TEXT5 | ElementType::TEXTRAW => {
                let payload = &self.data[start..payload_end];
                std::str::from_utf8(payload)
                    .map_err(|_| parse_error("Invalid UTF-8 in text payload", None))?;

                Ok(())
            }
            ElementType::ARRAY => {
                let mut position = start;

                while position < payload_end {
                    if position >= self.data.len() {
                        parse_error!("Array element out of bounds");
                    }

                    let (header, size) = self.read_header(position)?;
                    let end = position + size + header.payload_size();
                    if end > payload_end {
                        parse_error!("Array element exceeds bounds");
                    }

                    self.validate(position, end, depth + 1)?;
                    position = end;
                }

                Ok(())
            }
            ElementType::OBJECT => {
                let mut position = start;
                let mut count = 0;

                while position < payload_end {
                    if position >= self.data.len() {
                        parse_error!("Object element out of bounds");
                    }

                    let (header, size) = self.read_header(position)?;
                    if count % 2 == 0 && !header.element_type().is_valid_key() {
                        parse_error!("Object key must be text");
                    }

                    let end = position + size + header.payload_size();
                    if end > payload_end {
                        parse_error!("Object element exceeds bounds");
                    }

                    self.validate(position, end, depth + 1)?;
                    position = end;
                    count += 1;
                }

                if count % 2 != 0 {
                    parse_error!("Object must have an even number of elements");
                }

                Ok(())
            }

            _ => parse_error!("Invalid element type"),
        }
    }

    fn deserialize_value(
        &mut self,
        input: &[u8],
        mut position: usize,
        depth: usize,
    ) -> super::Result<usize> {
        const TRUE: &[u8; 4] = b"true";
        const FALSE: &[u8; 5] = b"false";

        if depth > MAX_DEPTH {
            parse_error!("Too deep");
        }

        position = Self::skip_whitespace(input, position);
        if position >= input.len() {
            parse_error!("Unexpected end of input");
        }

        match input[position] {
            b'{' => {
                position += 1;
                position = self.deserialize_obj(input, position, depth + 1)?;
            }
            b'[' => {
                position += 1;
                position = self.deserialize_array(input, position, depth + 1)?;
            }
            #[rustfmt::skip]
            b't' => position = self.deserialize_literal(input, position, TRUE, ElementType::TRUE)?,
            #[rustfmt::skip]
            b'f' => position = self.deserialize_literal(input, position, FALSE, ElementType::FALSE)?,
            b'n' | b'N' => position = self.deserialize_null_or_nan(input, position)?,
            b'"' | b'\'' => position = self.deserialize_string(input, position)?,
            c if c.is_ascii_digit()
                || c.eq_ignore_ascii_case(&b'i')
                || c == b'+'
                || c == b'-'
                || c == b'.' =>
            {
                position = self.deserialize_number(input, position)?;
            }
            _ => return Err(parse_error("Unexpected character", Some(position))),
        };

        Ok(position)
    }

    fn deserialize_literal<const N: usize>(
        &mut self,
        input: &[u8],
        mut pos: usize,
        literal: &[u8; N],
        elem: ElementType,
    ) -> super::Result<usize> {
        if input.len().saturating_sub(pos) < N || input[pos..pos + N] != literal[..] {
            return Err(parse_error("Invalid literal", Some(pos)));
        }

        pos += N;
        self.data.push(elem as u8);

        Ok(pos)
    }

    fn deserialize_null_or_nan(&mut self, input: &[u8], mut pos: usize) -> super::Result<usize> {
        if pos + 3 >= input.len() {
            return Err(parse_error("Unexpected end of input", Some(pos)));
        }

        if pos + 4 <= input.len()
            && input[pos] == b'n'
            && input[pos + 1] == b'u'
            && input[pos + 2] == b'l'
            && input[pos + 3] == b'l'
        {
            pos += 4;
            self.data.push(ElementType::NULL as u8);

            return Ok(pos);
        }

        if pos + 3 <= input.len()
            && (input[pos] == b'n' || input[pos] == b'N')
            && (input[pos + 1] == b'a' || input[pos + 1] == b'A')
            && (input[pos + 2] == b'n' || input[pos + 2] == b'N')
        {
            pos += 3;
            self.data.push(ElementType::NULL as u8);

            return Ok(pos);
        }

        Err(parse_error("Expected null or nan", Some(pos)))
    }

    fn deserialize_number(&mut self, input: &[u8], mut pos: usize) -> super::Result<usize> {
        let start = self.len();
        let mut len = 0;
        let mut is_float = false;
        let mut is_v5 = false;

        macro_rules! push_current {
            () => {
                self.data.push(input[pos]);
                pos += 1;
                len += 1;
            };
        }

        self.write_element_header(start, ElementType::INT, 0, false)
            .map_err(|_| parse_error("Failed to write to header", Some(pos)))?;

        match pos < input.len() && (input[pos] == b'-' || input[pos] == b'+') {
            true if input[pos] == b'+' => {
                is_v5 = true;
                pos += 1;
            }
            _ => {
                push_current!();
            }
        }

        if pos < input.len() && input[pos] == b'.' {
            is_v5 = true;
            is_float = true;
        }

        if pos < input.len() && input[pos] == b'0' && pos + 1 < input.len() {
            push_current!();

            // hexdecimal
            if pos < input.len() && (input[pos] == b'x' || input[pos] == b'X') {
                push_current!();

                let mut has_digit = false;
                while pos < input.len() && is_hex(input[pos]) {
                    push_current!();

                    has_digit = true;
                }

                if !has_digit {
                    return Err(parse_error("Invalid hex digit", Some(pos)));
                }

                self.write_element_header(start, ElementType::INT5, len, false)
                    .map_err(|_| parse_error("Unexpected input after JSON", Some(pos)))?;

                return Ok(pos);
            } else if pos < input.len() && input[pos].is_ascii_digit() {
                return Err(parse_error("Leading zero is not allowed", Some(pos)));
            }
        }

        if pos < input.len() && (input[pos] == b'I' || input[pos] == b'i') {
            let infinity = b"infinity";
            let mut idx = 0;

            while idx < infinity.len() && pos + idx < input.len() {
                if input[pos + idx].to_ascii_lowercase() != infinity[idx] {
                    return Err(parse_error("Invalid number", Some(pos)));
                }

                idx += 1;
            }

            if idx < infinity.len() {
                return Err(parse_error("Incomplete infinity", Some(pos)));
            }

            pos += infinity.len();

            self.data.extend_from_slice(b"9.0e+999");
            self.write_element_header(
                start,
                ElementType::FLOAT5,
                len + INFINITY_CHAR as usize,
                false,
            )
            .map_err(|_| parse_error("Failed to write to header", Some(pos)))?;

            return Ok(pos);
        }

        while pos < input.len() {
            match input[pos] {
                b'0'..b'9' => {
                    push_current!();
                }
                b'.' => {
                    push_current!();
                    is_float = true;

                    if pos >= input.len() || !input[pos].is_ascii_digit() {
                        is_v5 = true;
                    }
                }
                b'e' | b'E' => {
                    push_current!();
                    is_float = true;

                    if pos < input.len() && (input[pos] == b'+' || input[pos] == b'-') {
                        push_current!();
                    }
                }
                _ => break,
            };
        }

        if len == 0 && (!is_v5 || !is_float) {
            return Err(parse_error("Not a digit", Some(pos)));
        }

        let element = match (is_float, is_v5) {
            (true, true) => ElementType::FLOAT5,
            (false, true) => ElementType::INT5,
            _ => ElementType::INT,
        };

        self.write_element_header(start, element, len, false)
            .map_err(|_| parse_error("Unexpected input after JSON", Some(pos)))?;

        Ok(pos)
    }

    fn deserialize_obj(
        &mut self,
        input: &[u8],
        mut pos: usize,
        depth: usize,
    ) -> super::Result<usize> {
        if depth > MAX_DEPTH {
            parse_error("Too deep", Some(pos));
        }

        if self.data.capacity() - self.data.len() < 50 {
            self.data.reserve(self.data.capacity());
        }
        if pos >= input.len() {
            parse_error("Unexpected end of input", Some(pos));
        }

        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::OBJECT, 0, false)
            .map_err(|_| parse_error("Failed to write to header", Some(pos)))?;
        let obj_start = self.len();
        let mut first = true;

        loop {
            pos = Self::skip_whitespace(input, pos);
            if pos >= input.len() {
                parse_error("Unexpected end of input", Some(pos));
            }

            match input[pos] {
                b'}' => {
                    pos += 1;

                    if !first {
                        let obj_size = self.len() - obj_start;
                        self.write_element_header(header_pos, ElementType::OBJECT, obj_size, false)
                            .map_err(|_| parse_error("Failed to write to header", Some(pos)))?;
                    }

                    return Ok(pos);
                }

                b',' => {
                    if first {
                        parse_error("Unexpected comma", Some(pos));
                    }

                    pos += 1;
                    pos = Self::skip_whitespace(input, pos);
                    if input[pos] == b',' || input[pos] == b'{' {
                        parse_error!("Two commas in a row");
                    }

                    continue;
                }

                _ => {}
            }

            pos = self.deserialize_obj_entry(input, pos, depth)?;
            first = false;
        }
    }

    fn deserialize_array(
        &mut self,
        input: &[u8],
        mut pos: usize,
        depth: usize,
    ) -> super::Result<usize> {
        if depth > MAX_DEPTH {
            return Err(parse_error("Too deep", Some(pos)));
        }

        let header_pos = self.len();
        self.write_element_header(header_pos, ElementType::ARRAY, 0, false)?;
        let array_start = self.len();
        let mut first = true;

        loop {
            pos = Self::skip_whitespace(input, pos);
            if pos >= input.len() {
                return Err(parse_error("Unexpected end of input", Some(pos)));
            }

            match input[pos] {
                b']' => {
                    pos += 1;

                    match first {
                        true => return Ok(pos),
                        _ => {
                            let array_len = self.len() - array_start;
                            self.write_element_header(
                                header_pos,
                                ElementType::ARRAY,
                                array_len,
                                false,
                            )?;

                            return Ok(pos);
                        }
                    }
                }

                b',' if !first => {
                    pos += 1;
                    pos = Self::skip_whitespace(input, pos);

                    if input[pos] == b',' {
                        return Err(parse_error("Two commas in a row", Some(pos)));
                    }
                }

                _ => {
                    pos = Self::skip_whitespace(input, pos);
                    pos = self.deserialize_value(input, pos, depth + 1)?;

                    first = false;
                }
            }
        }
    }

    fn deserialize_obj_entry(
        &mut self,
        input: &[u8],
        mut pos: usize,
        depth: usize,
    ) -> super::Result<usize> {
        pos = self.deserialize_string(input, pos)?;
        pos = Self::skip_whitespace(input, pos);

        if pos >= input.len() || input[pos] != b':' {
            return Err(parse_error("Expected : after object key", Some(pos)));
        }

        pos += 1;
        pos = Self::skip_whitespace(input, pos);
        pos = self.deserialize_value(input, pos, depth + 1)?;
        pos = Self::skip_whitespace(input, pos);

        if pos < input.len() && !matches!(input[pos], b',' | b'}') {
            return Err(parse_error("Should be , or }}", Some(pos)));
        }

        Ok(pos)
    }

    fn deserialize_string(&mut self, input: &[u8], mut pos: usize) -> super::Result<usize> {
        if pos >= input.len() {
            parse_error("Unexpected end of input", Some(pos));
        }

        let string_start = self.len();
        let quote = input[pos];
        pos += 1;

        let quoted = quote == b'"' || quote == b'\'';

        if quoted {
            if let Some(simple_end) = self.try_deserialize_simple_string(input, pos, quote)? {
                return Ok(simple_end);
            }
        }

        self.write_element_header(string_start, ElementType::TEXT, 0, false)?;

        if pos >= input.len() {
            parse_error("Unexpected end of input", Some(pos));
        }

        let mut element_type = ElementType::TEXT;
        let mut len = 0;

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
            }

            if !quoted && (c == b'"' || c == b'\'') {
                parse_error("Unexpected input", Some(pos));
            }

            if c == b'\\' {
                let (new_pos, new_len, esc_type) =
                    self.handle_escape_sequence(input, pos, len, &mut escape_buffer)?;
                pos = new_pos;
                len = new_len;
                element_type = esc_type;

                continue;
            }

            if !quoted && (c == b':' || c.is_ascii_whitespace()) {
                pos -= 1;
                break;
            }

            if c <= 0x1F {
                element_type = ElementType::TEXT5;
            }

            self.data.push(c);
            len += 1;
        }

        self.write_element_header(string_start, element_type, len, false)?;
        Ok(pos)
    }

    fn handle_escape_sequence(
        &mut self,
        input: &[u8],
        mut pos: usize,
        mut len: usize,
        escape_buffer: &mut [u8; 6],
    ) -> super::Result<(usize, usize, ElementType)> {
        const ESCAPE_PREFIX: usize = 2;
        const HEX_ESCAPE_DIGITS: usize = 2;
        const UNICODE_HEX_DIGITS: usize = 4;

        if pos >= input.len() {
            parse_error("Unexpected end of input", Some(pos));
        }

        let esc = input[pos];
        pos += 1;

        let (bytes, element_type) = match esc {
            b'b' => (&b"\\b"[..], ElementType::TEXTJ),
            b'f' => (&b"\\f"[..], ElementType::TEXTJ),
            b'n' => (&b"\\n"[..], ElementType::TEXTJ),
            b'r' => (&b"\\r"[..], ElementType::TEXTJ),
            b't' => (&b"\\t"[..], ElementType::TEXTJ),

            b'\\' | b'"' | b'/' => {
                self.data.push(b'\\');
                self.data.push(esc);
                len += 2;
                return Ok((pos, len, ElementType::TEXTJ));
            }

            b'u' => {
                if pos + UNICODE_HEX_DIGITS > input.len() {
                    parse_error!("Incomplete unicode escape sequence");
                }
                escape_buffer[0] = b'\\';
                escape_buffer[1] = b'u';
                let hex_start = ESCAPE_PREFIX;
                let hex_end = ESCAPE_PREFIX + UNICODE_HEX_DIGITS;
                escape_buffer[hex_start..hex_end]
                    .copy_from_slice(&input[pos..pos + UNICODE_HEX_DIGITS]);
                pos += UNICODE_HEX_DIGITS;
                len += 6;
                self.data.extend_from_slice(&escape_buffer[..6]);
                return Ok((pos, len, ElementType::TEXTJ));
            }

            b'\n' => (&b"\\\n"[..], ElementType::TEXT5),
            b'\'' => (&b"\\\'"[..], ElementType::TEXT5),
            b'0' => (&b"\\0"[..], ElementType::TEXT5),
            b'v' => (&b"\\v"[..], ElementType::TEXT5),

            b'x' => {
                if pos + HEX_ESCAPE_DIGITS > input.len() {
                    parse_error!("Incomplete hex escape sequence");
                }
                escape_buffer[0] = b'\\';
                escape_buffer[1] = b'x';
                let hex_start = ESCAPE_PREFIX;
                let hex_end = ESCAPE_PREFIX + HEX_ESCAPE_DIGITS;
                escape_buffer[hex_start..hex_end]
                    .copy_from_slice(&input[pos..pos + HEX_ESCAPE_DIGITS]);
                pos += HEX_ESCAPE_DIGITS;
                len += ESCAPE_PREFIX + HEX_ESCAPE_DIGITS;
                self.data
                    .extend_from_slice(&escape_buffer[..ESCAPE_PREFIX + HEX_ESCAPE_DIGITS]);
                return Ok((pos, len, ElementType::TEXT5));
            }
            _ => parse_error!("Invalid escape sequence"),
        };

        self.data.extend_from_slice(bytes);
        len += bytes.len();

        Ok((pos, len, element_type))
    }

    fn try_deserialize_simple_string(
        &mut self,
        input: &[u8],
        start_pos: usize,
        quote: u8,
    ) -> super::Result<Option<usize>> {
        let mut end_pos = start_pos;

        while end_pos < input.len() {
            let c = input[end_pos];

            if c == quote {
                let len = end_pos - start_pos;
                let header_pos = self.data.len();

                match len <= 11 {
                    true => self
                        .data
                        .push((ElementType::TEXT as u8) | ((len as u8) << 4)),
                    _ => {
                        self.write_element_header(header_pos, ElementType::TEXT, len, false)?;
                    }
                };

                self.data.extend_from_slice(&input[start_pos..end_pos]);
                return Ok(Some(end_pos + 1));
            }

            if c == b'\\' || c < 32 {
                return Ok(None);
            }

            end_pos += 1;
        }

        Ok(None)
    }

    fn write_string(&self, string: &mut String, indentation: JsonIndentation) {
        let cursor = 0;

        let _ = self.serialize_value(string, cursor, 0, &indentation);
    }

    fn serialize_value(
        &self,
        string: &mut String,
        cursor: usize,
        depth: usize,
        delimiter: &JsonIndentation,
    ) -> super::Result<usize> {
        let (header, offset) = self.read_header(cursor)?;

        let cursor = cursor + offset;

        let current = match header {
            #[rustfmt::skip]
            JsonHeader(ElementType::OBJECT, len) => self.serialize_object(string, cursor, len, depth, delimiter)?,
            #[rustfmt::skip]
            JsonHeader(ElementType::ARRAY, len) => self.serialize_array(string, cursor, len, depth, delimiter)?,

            JsonHeader(ElementType::TEXT, len)
            | JsonHeader(ElementType::TEXTJ, len)
            | JsonHeader(ElementType::TEXTRAW, len)
            | JsonHeader(ElementType::TEXT5, len) => {
                self.serialize_string(string, cursor, len, &header.0, true)?
            }

            JsonHeader(ElementType::INT, len)
            | JsonHeader(ElementType::INT5, len)
            | JsonHeader(ElementType::FLOAT, len)
            | JsonHeader(ElementType::FLOAT5, len) => {
                self.serialize_number(string, cursor, len, &header.0)?
            }

            JsonHeader(ElementType::TRUE, _) => self.serialize_literal(string, cursor, "true"),
            JsonHeader(ElementType::FALSE, _) => self.serialize_literal(string, cursor, "false"),
            JsonHeader(ElementType::NULL, _) => self.serialize_literal(string, cursor, "null"),

            JsonHeader(_, _) => unsafe { unreachable_unchecked() },
        };

        Ok(current)
    }

    fn serialize_literal(&self, string: &mut String, cursor: usize, literal: &str) -> usize {
        string.push_str(literal);
        cursor
    }

    fn serialize_number(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
    ) -> super::Result<usize> {
        let current = cursor + len;
        let slice = std::str::from_utf8(&self.data[cursor..current])
            .map_err(|_| JsonError::Internal("Failed to parse integer".into()))?;

        match kind {
            ElementType::INT | ElementType::FLOAT => string.push_str(slice),
            ElementType::INT5 => unimplemented!(),
            ElementType::FLOAT5 => unimplemented!(),
            _ => unsafe { unreachable_unchecked() },
        }

        Ok(current)
    }

    fn serialize_object(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        mut depth: usize,
        indent: &JsonIndentation,
    ) -> super::Result<usize> {
        let end_cursor = cursor + len;
        let mut current_cursor = cursor;
        depth += 1;
        string.push('{');
        if indent.is_pretty() {
            string.push('\n');
        };

        while current_cursor < end_cursor {
            let (key_header, key_header_offset) = self.read_header(current_cursor)?;
            current_cursor += key_header_offset;
            let JsonHeader(element_type, len) = key_header;
            if let JsonIndentation::Indentation(value) = indent {
                for _ in 0..depth {
                    string.push_str(value);
                }
            };

            match element_type {
                ElementType::TEXT
                | ElementType::TEXTRAW
                | ElementType::TEXTJ
                | ElementType::TEXT5 => {
                    current_cursor =
                        self.serialize_string(string, current_cursor, len, &element_type, true)?;
                }
                _ => parse_error!("malformed JSON"),
            }

            string.push(':');
            if indent.is_pretty() {
                string.push(' ');
            }
            current_cursor = self.serialize_value(string, current_cursor, depth, indent)?;
            if current_cursor < end_cursor {
                string.push(',');
            }

            if indent.is_pretty() {
                string.push('\n');
            };
        }

        if let JsonIndentation::Indentation(value) = indent {
            for _ in 0..depth - 1 {
                string.push_str(value);
            }
        };

        string.push('}');

        Ok(current_cursor)
    }

    fn serialize_array(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        mut depth: usize,
        indent: &JsonIndentation,
    ) -> super::Result<usize> {
        let end_cursor = cursor + len;
        let mut current_cursor = cursor;

        depth += 1;
        string.push('[');
        if indent.is_pretty() {
            string.push('\n');
        };

        while current_cursor < end_cursor {
            if let JsonIndentation::Indentation(value) = indent {
                for _ in 0..depth {
                    string.push_str(value);
                }
            };

            current_cursor = self.serialize_value(string, current_cursor, depth, indent)?;

            if current_cursor < end_cursor {
                string.push(',');
            }
            if indent.is_pretty() {
                string.push('\n');
            };
        }

        if let JsonIndentation::Indentation(value) = indent {
            for _ in 0..depth - 1 {
                string.push_str(value);
            }
        };

        string.push(']');

        Ok(current_cursor)
    }

    fn serialize_string(
        &self,
        string: &mut String,
        cursor: usize,
        len: usize,
        kind: &ElementType,
        quote: bool,
    ) -> super::Result<usize> {
        let word = &self.data[cursor..cursor + len];
        if quote {
            string.push('"');
        }

        match kind {
            ElementType::TEXT | ElementType::TEXTJ => {
                let word = std::str::from_utf8(word)
                    .map_err(|_| parse_error("Failed to serialize string", Some(cursor)))?;
                string.push_str(word);
            }

            ElementType::TEXT5 => {
                let mut idx = 0;

                while idx < word.len() {
                    let c = word[idx];

                    if c == b'\'' {
                        string.push(c as char);
                        idx += 1;

                        continue;
                    }

                    match c {
                        b'"' => {
                            string.push_str("\\\"");
                            idx += 1;
                        }

                        c if c <= 0x1F => {
                            match c {
                                0x08 => string.push_str("\\b"),
                                b'\t' => string.push_str("\\t"),
                                b'\n' => string.push_str("\\n"),
                                0x0C => string.push_str("\\f"),
                                b'\r' => string.push_str("\\r"),
                                _ => {
                                    let hex = format!("\\u{c:04x}");
                                    string.push_str(&hex);
                                }
                            }

                            idx += 1;
                        }

                        b'\\' if idx + 1 < word.len() => {
                            let next = word[idx + 1];

                            match next {
                                b'\'' => {
                                    string.push('\'');
                                    idx += 2;
                                }
                                b'v' => {
                                    string.push_str("\\u0009");
                                    idx += 2;
                                }
                                b'x' if idx + 3 < word.len() => {
                                    string.push_str("\\u00");
                                    string.push(word[idx + 2] as char);
                                    string.push(word[idx + 3] as char);

                                    idx += 4;
                                }
                                b'0' => {
                                    string.push_str("\\u0000");
                                    idx += 2;
                                }
                                b'\r' => match idx + 2 < word.len() && word[idx + 2] == b'\n' {
                                    true => idx += 3,
                                    _ => idx += 2,
                                },
                                b'\n' => idx += 2,
                                0xe2 if idx + 3 < word.len()
                                    && word[idx + 2] == 0x80
                                    && (word[idx + 3] == 0xa8 || word[idx + 3] == 0xa9) =>
                                {
                                    idx += 4;
                                }

                                _ => {
                                    string.push('\\');
                                    string.push(next as char);

                                    idx += 2;
                                }
                            }
                        }

                        _ => {
                            string.push(c as char);
                            idx += 1;
                        }
                    }
                }
            }

            ElementType::TEXTRAW => {
                let word = std::str::from_utf8(word)
                    .map_err(|_| parse_error("Failed to serialize string", Some(cursor)))?;

                for c in word.chars() {
                    match c {
                        '"' => string.push_str("\\\""),
                        '\\' => string.push_str("\\\\"),
                        '\x08' => string.push_str("\\b"),
                        '\x0C' => string.push_str("\\f"),
                        '\n' => string.push_str("\\n"),
                        '\r' => string.push_str("\\r"),
                        '\t' => string.push_str("\\t"),
                        c if c <= '\u{001F}' => string.push_str(&format!("\\u{:04x}", c as u32)),
                        _ => string.push(c),
                    }
                }
            }

            _ => unsafe { unreachable_unchecked() },
        }

        if quote {
            string.push('"');
        }

        Ok(cursor + len)
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

    #[inline]
    pub fn skip_whitespace(input: &[u8], mut pos: usize) -> usize {
        while let Some(&ch) = input.get(pos) {
            match ch {
                _ if (WS_TABLE[ch as usize] & 1) != 0 => {
                    pos += 1;
                }
                b'/' => match input.get(pos + 1) {
                    Some(b'/') => {
                        pos += 2;
                        while pos < input.len() && input[pos] != b'\n' {
                            pos += 1;
                        }
                        if pos < input.len() {
                            pos += 1;
                        }
                    }
                    Some(b'*') => {
                        pos += 2;
                        while let Some(window) = input.get(pos..pos + 2) {
                            if window == b"*/" {
                                pos += 2;
                                break;
                            }
                            pos += 1;
                        }
                        if pos < input.len() && pos + 1 >= input.len() {
                            pos = input.len();
                        }
                    }
                    _ => break,
                },
                _ => break,
            }
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

impl Display for Jsonb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut result = String::with_capacity(self.len() * 2);

        self.write_string(&mut result, JsonIndentation::None);
        write!(f, "{result}")
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
                let (offset, total_size) = match header_size {
                    size if size <= 11 => (1, size as usize),

                    12 => {
                        let value = slice
                            .get(cursor + 1)
                            .ok_or(parse_error("Failed to read 1-byte size", None))?;
                        (2, *value as usize)
                    }

                    13 => {
                        let bytes = slice
                            .get(cursor + 1..cursor + 3)
                            .ok_or(parse_error("Failed to read 2-byte size", None))?;
                        (3, u16::from_be_bytes([bytes[0], bytes[1]]) as usize)
                    }

                    14 => {
                        let bytes = slice
                            .get(cursor + 1..cursor + 5)
                            .ok_or(parse_error("Failed to read 4-byte size", None))?;
                        (
                            5,
                            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize,
                        )
                    }
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

impl TransversalResult {
    pub fn new(value_index: usize, key_index: LocationKind, delta: isize) -> Self {
        Self {
            key_index,
            value_index,
            delta,
            array_position: None,
        }
    }

    pub fn with_array_index(
        value_index: usize,
        key_index: LocationKind,
        delta: isize,
        index: usize,
    ) -> Self {
        Self {
            key_index,
            value_index,
            delta,
            array_position: Some(ArrayPosition::Index(index)),
        }
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

impl<'i> JsonIndentation<'i> {
    pub const fn is_pretty(&self) -> bool {
        matches!(self, Self::Indentation(_))
    }
}

impl PathOperation {
    pub const fn allows_replace(&self) -> bool {
        matches!(self, Self::ReplaceExisting | Self::Upsert)
    }

    pub const fn allows_insert(&self) -> bool {
        matches!(self, Self::Insert | Self::Upsert)
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

#[inline]
const fn is_hex(c: u8) -> bool {
    (CHARACTER_TYPE[c as usize] & 3) == 2 || (CHARACTER_TYPE[c as usize] & 3) == 3
}

#[inline]
fn compare(key: (&str, ElementType), path_key: (&str, bool)) -> bool {
    let (key, element_type) = key;
    let (path_key, is_raw) = path_key;

    if !is_raw && element_type == ElementType::TEXT {
        return match key.len() == path_key.len() {
            true => key == path_key,
            _ => false,
        };
    }

    if !is_raw {
        return unescape_string(key) == path_key;
    }

    match element_type {
        ElementType::TEXTJ | ElementType::TEXT5 | ElementType::TEXTRAW | ElementType::TEXT => {
            return unescape_string(key) == unescape_string(path_key)
        }
        _ => {}
    }

    false
}

#[inline]
fn unescape_string(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut code_point = String::with_capacity(5);

    while let Some(c) = chars.next() {
        match c == '\\' {
            true => match chars.next() {
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('/') => result.push('/'),
                Some('"') => result.push('"'),
                Some('b') => result.push('\u{0008}'),
                Some('f') => result.push('\u{000C}'),

                Some('x') => {
                    code_point.clear();

                    for _ in 0..2 {
                        match chars.next() {
                            Some(hex) => code_point.push(hex),
                            #[rustfmt::skip]
                            _ => { break; }
                        }
                    }

                    if let Ok(code) = u16::from_str_radix(&code_point, 16) {
                        if let Some(c) = char::from_u32(code as u32) {
                            result.push(c)
                        }
                    }
                }

                Some('u') => {
                    code_point.clear();

                    for _ in 0..4 {
                        match chars.next() {
                            Some(hex) => code_point.push(hex),
                            #[rustfmt::skip]
                            _ => { break; }
                        }
                    }

                    if let Ok(code) = u16::from_str_radix(&code_point, 16) {
                        if matches!(code, 0xD800..=0xDBFF) {
                            match chars.next() == Some('\\') && chars.next() == Some('u') {
                                true => {
                                    code_point.clear();

                                    for _ in 0..4 {
                                        match chars.next() {
                                            Some(hex) => code_point.push(hex),
                                            #[rustfmt::skip]
                                            _ => { break; }
                                        }
                                    }

                                    if let Ok(low) = u16::from_str_radix(&code_point, 16) {
                                        match (0xDC00..=0xDFFF).contains(&low) {
                                            false => {
                                                if let Some(c1) = char::from_u32(code as u32) {
                                                    result.push(c1);
                                                }

                                                if let Some(c2) = char::from_u32(low as u32) {
                                                    result.push(c2)
                                                }
                                            }
                                            _ => {
                                                let high_bits = (code - 0xD800) as u32;
                                                let low_bits = (low - 0xDC00) as u32;
                                                let code_point = (high_bits << 10) | low_bits;
                                                let unicode_value = code_point + 0x10000;

                                                if let Some(unicode_char) =
                                                    char::from_u32(unicode_value)
                                                {
                                                    result.push(unicode_char);
                                                }
                                            }
                                        }
                                    }
                                }

                                #[rustfmt::skip]
                                _ => if let Some(unicode_char) = char::from_u32(code as u32) {
                                    result.push(unicode_char);
                                },
                            }
                        }
                    }
                }

                Some(c) => {
                    result.push('\\');
                    result.push(c)
                }
                None => result.push('\\'),
            },
            _ => result.push(c),
        }
    }

    result
}

pub(crate) fn parse_error(msg: &str, location: Option<usize>) -> super::JsonError {
    super::JsonError::Message {
        msg: msg.to_string(),
        location: location,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_serialisation() {
        let mut json = Jsonb::new(10, None);
        json.data.push(ElementType::NULL as u8);

        let json_str = json.to_string();
        assert_eq!(json_str, "null");

        let json = Jsonb::from_str("null").unwrap();
        assert_eq!(json.data[0], ElementType::NULL as u8);
    }

    #[test]
    fn binary_conversion() {
        let json = r#"{"key":"value","array":[1,2,3]}"#;
        let result = Jsonb::from_str(json).unwrap();
        let binary = result.data.clone();

        let result = Jsonb::new(0, Some(&binary));
        assert_eq!(result.to_string(), json)
    }

    #[test]
    fn comments() {
        let result = Jsonb::from_str(
            r#"{
            // a line comment
            "key": "value" }"#,
        )
        .unwrap();
        assert_eq!(result.to_string(), r#"{"key":"value"}"#);

        let result = Jsonb::from_str(
            r#"{
            /* a block
                comment */
            "key": "value"
            }"#,
        )
        .unwrap();
        assert_eq!(result.to_string(), r#"{"key":"value"}"#);

        let parsed = Jsonb::from_str(
            r#"[1, // Comment
                                       2, /* Another comment */ 3]"#,
        )
        .unwrap();
        assert_eq!(parsed.to_string(), "[1,2,3]");
    }

    #[test]
    fn whitespace() {
        let json = r#"
            {
                "key":              "value",
                    "key2": [1,             2,3]      ,
                "key3"  : {
                            "nested"        :       true
                }
            }"#;

        let result = Jsonb::from_str(json).unwrap();
        assert_eq!(
            result.to_string(),
            r#"{"key":"value","key2":[1,2,3],"key3":{"nested":true}}"#
        );
    }

    #[test]
    fn string_json_5() {
        let json = r#"{
        "multi_line": "Line one \
line two \
line three",
        "emoji_soup": "👨‍👩‍👧‍👦 🚀 \u2728",
        "nested_json_string": "{\"key\": \"value with \\\"quotes\\\"\"}",
        "mixed_scripts": "Greetings! 你好, שָׁלוֹם, 👋",
        "pathological": " \t\r\n\\\/\"' ",
        "unicode_max": "\uDBFF\uDFFF"
    }"#;
        let result = Jsonb::from_str(json)
            .expect("Failed to parse complex JSON")
            .to_string();

        assert!(result.contains(r#""multi_line":"Line one line two line three""#));
        assert!(result.contains("👨‍👩‍👧‍👦"));
        assert!(
            result.contains(r#""nested_json_string":"{\"key\": \"value with \\\"quotes\\\"\"}""#)
        );
        assert!(result.contains("你好"));
        assert!(result.contains(r#""pathological":" \t\r\n\\\/\"' ""#));
    }

    #[test]
    fn array_serialisation() {
        let result = Jsonb::from_str("[]").unwrap();
        assert_eq!(result.to_string(), "[]");

        let result = Jsonb::from_str("[1,2,3]").unwrap();
        assert_eq!(result.to_string(), "[1,2,3]");

        let result = Jsonb::from_str("[[1,2],[3,4]]").unwrap();
        assert_eq!(result.to_string(), "[[1,2],[3,4]]");

        let result = Jsonb::from_str(r#"[1,"text",true,null,{"key":"value"}]"#).unwrap();
        assert_eq!(
            result.to_string(),
            r#"[1,"text",true,null,{"key":"value"}]"#
        );

        let header = JsonHeader::from_slice(0, &result.data).unwrap().0;
        assert!(matches!(header.0, ElementType::ARRAY));
    }
}
