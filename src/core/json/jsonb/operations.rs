//! Writes at a JSON path: insert, replace and delete, and building documents piece by piece
#![cfg_attr(not(test), expect(dead_code, reason = "kept for the native JSON syntax"))]

use crate::collections::hash::HashMap;
use crate::core::json::{
    ElementType, JsonError, JsonHeader, Jsonb, Result,
    jsonb::{compare, parse_error},
    path::{JsonPath, PathElement},
};

pub(crate) struct TransversalResult {
    key_index: LocationKind,
    value_index: usize,
    delta: isize,
    array_position: Option<ArrayPosition>,
}

pub(crate) struct InsertOperation {
    mode: PathOperationMode,
    value: Jsonb,
}

pub(crate) struct DeleteOperation {
    mode: PathOperationMode,
}

pub(crate) struct ReplaceOperation {
    mode: PathOperationMode,
    value: Jsonb,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PathOperationMode {
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

pub trait PathOperation {
    fn mode(&self) -> PathOperationMode;
    fn execute(&mut self, json: &mut Jsonb, stack: Vec<TransversalResult>) -> Result<()>;
}

impl Jsonb {
    pub fn navigate_path(
        &mut self,
        path: &JsonPath,
        mode: PathOperationMode,
    ) -> Result<Vec<TransversalResult>> {
        let mut iter = path.elements.iter().peekable();
        let mut cursor = 0;
        let mut stack: Vec<_> = Vec::with_capacity(path.elements.len());

        while let Some(current) = iter.next() {
            let next_is_array = matches!(iter.peek(), Some(PathElement::ArrayLocator(_)))
                && !matches!(current, PathElement::ArrayLocator(_));

            let is_intermediate = match next_is_array {
                false => iter.peek().is_some(),
                _ => {
                    let mut temp = iter.clone();
                    temp.next();
                    temp.peek().is_some()
                },
            };

            let mode = match is_intermediate {
                true => PathOperationMode::Upsert,
                _ => mode,
            };

            let result = match next_is_array {
                false => self.navigate_segment(SegmentVariant::Single(current), cursor, mode)?,
                _ => {
                    let locator = iter.next().ok_or_else(|| {
                        parse_error("Array locator must exist after peek", Some(cursor))
                    })?;

                    let seg = SegmentVariant::KeyWithArrayIndex(current, locator);
                    self.navigate_segment(seg, cursor, mode)?
                },
            };

            cursor = match &result.array_position {
                Some(ArrayPosition::Index(index)) => *index,
                _ => result.value_index,
            };

            stack.push(result);
        }

        Ok(stack)
    }

    pub fn operate_on_path<O: PathOperation>(
        &mut self,
        path: &JsonPath,
        operation: &mut O,
    ) -> Result<()> {
        let mode = operation.mode();
        let stack = self.navigate_path(path, mode)?;

        Ok(operation.execute(self, stack)?)
    }

    pub fn array_length(&self) -> Result<usize> {
        let (header, header_skip) = self.read_header(0)?;

        if header.0 != ElementType::ARRAY {
            return Ok(0);
        }

        let (mut count, mut position) = (0, header_skip);
        while position < header_skip + header.1 {
            position = self.skip_element(position)?;
            count += 1;
        }

        Ok(count)
    }

    pub fn empty_object(size: usize) -> Self {
        let mut json = Self::new(size, None);
        json.write_element_header(0, ElementType::OBJECT, 0, false)
            .expect("Header writing should not fail");

        json
    }

    pub fn empty_array(size: usize) -> Self {
        let mut json = Self::new(size, None);
        json.write_element_header(0, ElementType::ARRAY, 0, false)
            .expect("Header writing should not fail");

        json
    }

    pub fn append(&mut self, mut data: Vec<u8>) {
        self.data.append(&mut data);
    }

    pub fn finalise(&mut self, element_type: ElementType) -> Result<()> {
        self.write_element_header(0, element_type, self.len() - 1, false)?;
        Ok(())
    }

    fn navigate_segment(
        &mut self,
        segment: SegmentVariant,
        mut pos: usize,
        mode: PathOperationMode,
    ) -> Result<TransversalResult> {
        use ElementType as E;
        use PathElement as P;
        use SegmentVariant::*;

        let (header, header_size) = self.read_header(pos)?;
        let (element_type, element_size) = header.into();

        match segment {
            Single(P::Root) => return Ok(TransversalResult::new(pos, LocationKind::Root, 0)),
            Single(P::ArrayLocator(index)) => {
                let (root_header, root_header_size) = self.read_header(pos)?;
                let (root_type, root_size) = root_header.into();

                match root_type {
                    E::ARRAY => {
                        let end_position = pos + root_header_size + root_size;

                        match index {
                            Some(index) if *index >= 0 => {
                                let (mut count, mut array_position) = (0, pos + root_header_size);
                                while array_position < end_position && count != *index as usize {
                                    array_position = self.skip_element(array_position)?;
                                    count += 1;
                                }

                                if mode.allows_insert() && array_position == end_position {
                                    let placeholder = JsonHeader::new(E::OBJECT, 0).into_bytes();
                                    let placeholder_bytes = placeholder.as_bytes();
                                    let range = array_position..array_position;
                                    self.data.splice(range, placeholder_bytes.iter().copied());

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
                            },

                            Some(index) if *index < 0 => {
                                let mut index_map: HashMap<i32, usize> =
                                    HashMap::with_capacity_and_hasher(100, Default::default());
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
                                        ));
                                    },
                                    _ => parse_error!("Element with negative index not foun"),
                                };
                            },

                            _ => unreachable!(),
                        }
                    },
                    E::OBJECT
                        if root_size == 0
                            && (*index == Some(0) || index.is_none())
                            && mode.allows_insert() =>
                    {
                        let array = JsonHeader::new(E::ARRAY, 0).into_bytes();
                        let array_bytes = array.as_bytes();
                        let placeholder = JsonHeader::new(E::OBJECT, 0).into_bytes();
                        let placeholder_bytes = placeholder.as_bytes();

                        self.data.splice(
                            pos..pos + root_header_size,
                            array_bytes.iter().copied().chain(placeholder_bytes.iter().copied()),
                        );

                        return Ok(TransversalResult::with_array_index(
                            pos,
                            LocationKind::ArrayEntry,
                            placeholder_bytes.len() as isize,
                            pos + array_bytes.len(),
                        ));
                    },
                    _ => parse_error!("Root is not an array"),
                }
            },

            Single(P::Key(path_key, is_raw)) => match element_type {
                E::OBJECT => {
                    let end_position = pos + element_size + header_size;
                    pos += header_size;

                    while pos < end_position {
                        let (key_header, key_header_len) = self.read_header(pos)?;
                        let (key_type, key_len) = key_header.into();

                        if !key_type.is_valid_key() {
                            parse_error!("Key should be string");
                        }

                        let key_start = pos + key_header_len;
                        let range = key_start..key_start + key_len;
                        // a stored blob is user data: its keys are checked, not assumed, UTF-8
                        let json_key = std::str::from_utf8(&self.data[range])
                            .map_err(|_| parse_error("object key is not UTF-8", Some(key_start)))?;

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
                                },
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
                                true => E::TEXTRAW,
                                _ => E::TEXT,
                            };

                            let key_header = JsonHeader::new(key_type, path_key.len()).into_bytes();
                            let key_header_bytes = key_header.as_bytes();
                            let key_bytes = path_key.as_bytes();
                            let value_header = JsonHeader::new(E::OBJECT, 0).into_bytes();
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
                        },
                        _ => parse_error!("Mode doesn't allow insert cannot create another key"),
                    }
                },
                _ => parse_error!("Bah"),
            },

            KeyWithArrayIndex(P::Root, P::ArrayLocator(index)) => {
                let (root_header, root_header_size) = self.read_header(pos)?;
                let (root_type, root_size) = root_header.into();

                match root_type {
                    E::ARRAY => {
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
                                    let placeholder = JsonHeader::new(E::OBJECT, 0).into_bytes();
                                    let placeholder_bytes = placeholder.as_bytes();
                                    let range = array_position..array_position;

                                    self.data.splice(range, placeholder_bytes.iter().copied());

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
                            },

                            Some(index) if *index < 0 => {
                                let mut index_map =
                                    HashMap::with_capacity_and_hasher(100, Default::default());
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
                                        ));
                                    },
                                    _ => parse_error!("Element with negative index was not found"),
                                }
                            },
                            _ => parse_error!("Root isn't an array"),
                        }
                    },
                    _ => parse_error!("Root isn't an array"),
                }
            },

            KeyWithArrayIndex(P::Key(path_key, is_raw), P::ArrayLocator(index)) => {
                if element_type != E::OBJECT {
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

                    let start = current + key_header_size;
                    let object_key = std::str::from_utf8(&self.data[start..start + key_size])
                        .map_err(|_| parse_error("object key is not UTF-8", Some(start)))?;

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
                        true => E::TEXTRAW,
                        _ => E::TEXT,
                    };

                    let key_header = JsonHeader::new(key_header_type, path_key.len()).into_bytes();
                    let key_header_bytes = key_header.as_bytes();
                    let key_bytes = path_key.as_bytes();
                    let array_header = JsonHeader::new(E::ARRAY, 1).into_bytes();
                    let array_header_bytes = array_header.as_bytes();
                    let array_value_header = JsonHeader::new(E::OBJECT, 0).into_bytes();
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

                    let (value_header, value_header_size) = self.read_header(value_index)?;
                    let (value_type, value_size) = value_header.into();

                    if value_type != E::ARRAY {
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
                                let placeholder = JsonHeader::new(E::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();
                                let range = array_position..array_position;
                                let size = value_size + placeholder_bytes.len();

                                self.data.splice(range, placeholder_bytes.iter().copied());
                                self.write_element_header(value_index, E::ARRAY, size, true)?;

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
                        },

                        Some(index) if *index < 0 => {
                            let mut index_map =
                                HashMap::with_capacity_and_hasher(100, Default::default());
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
                                    ));
                                },
                                _ => parse_error!("Element at negative index {index} not found"),
                            }
                        },
                        Some(_) => unreachable!(),
                        _ => match mode.allows_insert() {
                            false => parse_error!("Cannot insert item"),
                            _ => {
                                let placeholder = JsonHeader::new(E::OBJECT, 0).into_bytes();
                                let placeholder_bytes = placeholder.as_bytes();
                                let insert = value_index + value_size + value_header_size;

                                self.data.insert(insert, placeholder_bytes[0]);
                            },
                        },
                    }
                }
            },

            Single(P::Wildcard) => {
                if matches!(element_type, E::ARRAY) {
                    return Ok(TransversalResult::new(pos, LocationKind::Root, 0));
                }

                return Err(parse_error("Wildcard can only be applied to arrays", Some(pos)));
            },

            _ => {},
        }

        parse_error!("Not found")
    }

    fn update_parent(&mut self, stack: Vec<TransversalResult>, delta: isize) -> Result<()> {
        let mut delta = delta;
        let mut is_prev_array = false;

        for parent in stack.iter().rev() {
            let (JsonHeader(element_type, element_size), element_header_len) =
                self.read_header(parent.value_index)?;

            match element_type == ElementType::ARRAY && !is_prev_array {
                true => {
                    is_prev_array = true;

                    let element_index = parent.get_array_index().ok_or_else(|| {
                        JsonError::Internal("Array length must have an index".into())
                    })?;

                    let (JsonHeader(element_type, element_size), element_header_len) =
                        self.read_header(element_index)?;

                    let payload_size = (element_size as isize + delta) as usize;
                    let new_array_header_len =
                        self.write_element_header(element_index, element_type, payload_size, true)?;

                    delta += (new_array_header_len - element_header_len) as isize;
                },
                _ => is_prev_array = false,
            }

            let size = element_size as isize + delta;
            let header_size =
                self.write_element_header(parent.value_index, element_type, size as usize, true)?;

            let header_diff = header_size as isize - element_header_len as isize;

            delta += parent.delta;
            delta += header_diff;
        }

        Ok(())
    }
}

impl TransversalResult {
    pub fn new(value_index: usize, key_index: LocationKind, delta: isize) -> Self {
        Self { key_index, value_index, delta, array_position: None }
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

    pub fn get_array_index(&self) -> Option<usize> {
        match self.array_position {
            Some(ArrayPosition::Index(index)) => Some(index),
            _ => None,
        }
    }

    pub fn has_specific_index(&self) -> bool {
        matches!(self.array_position, Some(ArrayPosition::Index(_)))
    }
}

impl InsertOperation {
    pub fn new(value: Jsonb) -> Self {
        Self { value, mode: PathOperationMode::Insert }
    }
}

impl PathOperation for InsertOperation {
    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<TransversalResult>) -> Result<()> {
        if stack.is_empty() {
            parse_error!("Nothing to operate on");
        }

        let value = &self.value.data;
        let target = stack
            .pop()
            .ok_or_else(|| JsonError::Internal("Stack should be not empty after peek".into()))?;

        if target.has_specific_index() {
            let array_value_index = target
                .get_array_index()
                .ok_or(JsonError::Internal("Target should have an array index".into()))?;

            let object_index = target.value_index;

            let (JsonHeader(_, object_value_size), object_value_header_size) =
                json.read_header(object_index)?;
            let (JsonHeader(_, array_value_size), array_value_header_size) =
                json.read_header(array_value_index)?;

            let delta =
                value.len() as isize - (array_value_size + array_value_header_size) as isize;
            let end_position = array_value_index + array_value_size + array_value_header_size;
            json.data.splice(array_value_index..end_position, value.iter().copied());

            let h_delta =
                match matches!(target.key_index, LocationKind::Object(_) | LocationKind::Root) {
                    false => 0,
                    _ => {
                        let (size, typ) =
                            ((object_value_size as isize + delta) as _, ElementType::ARRAY);
                        let delta = json.write_element_header(object_index, typ, size, true)?;
                        (delta - object_value_header_size) as isize
                    },
                };

            json.update_parent(stack, target.delta + delta + h_delta)?;
        } else {
            let old_value_index = target.value_index;
            let (JsonHeader(_, old_value_size), old_value_header_size) =
                json.read_header(old_value_index)?;
            let delta = value.len() as isize - (old_value_header_size + old_value_size) as isize;
            let end_position = old_value_index + old_value_header_size + old_value_size;

            json.data.splice(old_value_index..end_position, value.iter().copied());
            json.update_parent(stack, delta + target.delta)?;
        }

        Ok(())
    }

    fn mode(&self) -> PathOperationMode {
        self.mode
    }
}

impl DeleteOperation {
    pub fn new() -> Self {
        Self { mode: PathOperationMode::ReplaceExisting }
    }
}

impl PathOperation for DeleteOperation {
    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<TransversalResult>) -> Result<()> {
        let target = stack
            .pop()
            .ok_or_else(|| JsonError::Internal("Stack must be not empty after peek".into()))?;

        if target.has_specific_index() {
            let array_value_index = target
                .get_array_index()
                .ok_or_else(|| JsonError::Internal("Target should have an index".into()))?;
            let index = target.value_index;

            let (JsonHeader(_, object_size), object_header_size) = json.read_header(index)?;
            let (JsonHeader(_, array_size), array_header_size) =
                json.read_header(array_value_index)?;

            let delta = 0 - (array_size + array_header_size) as isize;
            let end_position = array_value_index + array_size + array_header_size;
            json.data.drain(array_value_index..end_position);

            let h_delta =
                match matches!(target.key_index, LocationKind::Object(_) | LocationKind::Root) {
                    true => {
                        let size = (object_size as isize + delta) as usize;
                        let h_delta =
                            json.write_element_header(index, ElementType::ARRAY, size, true)?;
                        h_delta as isize - object_header_size as isize
                    },
                    _ => 0,
                };

            json.update_parent(stack, delta + target.delta + h_delta)?;
        } else if let LocationKind::Object(key_index) = target.key_index {
            let value_index = target.value_index;

            let (JsonHeader(_, value_size), value_header_size) = json.read_header(value_index)?;
            let (JsonHeader(_, key_size), key_header_size) = json.read_header(key_index)?;

            let delta = 0 - (value_header_size + value_size + key_size + key_header_size) as isize;
            let end_position =
                key_index + value_header_size + value_size + key_size + key_header_size;

            json.data.drain(key_index..end_position);
            json.update_parent(stack, delta + target.delta)?;
        } else {
            let null = JsonHeader::null().into_bytes();
            let null_bytes = null.as_bytes();

            json.data.clear();
            json.data.extend_from_slice(null_bytes);
        }

        Ok(())
    }

    fn mode(&self) -> PathOperationMode {
        self.mode
    }
}

impl ReplaceOperation {
    pub fn new(value: Jsonb) -> Self {
        Self { mode: PathOperationMode::ReplaceExisting, value }
    }
}

impl PathOperation for ReplaceOperation {
    fn execute(&mut self, json: &mut Jsonb, mut stack: Vec<TransversalResult>) -> Result<()> {
        let target = stack
            .pop()
            .ok_or_else(|| JsonError::Internal("Stack should not be empty after peek".into()))?;
        let value = &self.value.data;

        if target.has_specific_index() {
            let array_index = target
                .get_array_index()
                .ok_or(JsonError::Internal("Target must have an index".into()))?;

            let object_index = target.value_index;
            let (object_value_header, object_value_header_size) = json.read_header(object_index)?;
            let (_, object_value_size) = object_value_header.into();
            let (array_value_header, array_value_header_size) = json.read_header(array_index)?;
            let (_, array_value_size) = array_value_header.into();

            let delta =
                value.len() as isize - (array_value_size + array_value_header_size) as isize;
            let end_position = array_index + array_value_size + array_value_header_size;

            json.data.splice(array_index..end_position, value.iter().copied());

            let h_delta =
                match matches!(target.key_index, LocationKind::Object(_) | LocationKind::Root) {
                    false => 0,
                    _ => {
                        let size = (object_value_size as isize + delta) as usize;
                        let h_delta = json.write_element_header(
                            object_index,
                            ElementType::ARRAY,
                            size,
                            true,
                        )?;
                        (h_delta - object_value_header_size) as isize
                    },
                };

            json.update_parent(stack, target.delta + delta + h_delta)?;
        } else {
            let value_index = target.value_index;
            let (value_header, value_header_size) = json.read_header(value_index)?;
            let (_, value_size) = value_header.into();

            let delta = value.len() as isize - (value_header_size + value_size) as isize;
            let end_position = value_index + value_header_size + value_size;

            json.data.splice(value_index..end_position, value.iter().copied());
            json.update_parent(stack, delta + target.delta)?;
        }

        Ok(())
    }

    fn mode(&self) -> PathOperationMode {
        self.mode
    }
}

impl PathOperationMode {
    pub const fn allows_replace(&self) -> bool {
        matches!(self, Self::ReplaceExisting | Self::Upsert)
    }

    pub const fn allows_insert(&self) -> bool {
        matches!(self, Self::Insert | Self::Upsert)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;
    use std::str::FromStr;

    #[test]
    fn documents_are_built_piece_by_piece() {
        assert_eq!(Jsonb::empty_object(8).to_string(), "{}");

        let mut array = Jsonb::empty_array(8);
        for element in ["1", r#""two""#, "[3]"] {
            array.append(Jsonb::from_str(element).unwrap().data());
        }
        array.finalise(ElementType::ARRAY).unwrap();
        assert_eq!(array.to_string(), r#"[1,"two",[3]]"#);
        assert_eq!(array.array_length().unwrap(), 3);
        assert_eq!(Jsonb::from_str(r#"{"a": 1}"#).unwrap().array_length().unwrap(), 0);
    }

    #[test]
    fn insert_operation() {
        let json = r#"{"name": "John"}"#;
        let mut json = Jsonb::from_str(json).unwrap();

        let value = Jsonb::from_str("30").unwrap();
        let mut operation = InsertOperation::new(value);

        let path = JsonPath {
            elements: vec![PathElement::Root, PathElement::Key(Cow::Borrowed("age"), false)],
        };

        let result = json.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        assert_eq!(json.to_string(), r#"{"name":"John","age":30}"#)
    }

    #[test]
    fn delete_operation() {
        let json = r#"{"name": "John", "age": 20}"#;
        let mut json = Jsonb::from_str(json).unwrap();

        let mut operation = DeleteOperation::new();
        let path = JsonPath {
            elements: vec![PathElement::Root, PathElement::Key(Cow::Borrowed("age"), false)],
        };

        let result = json.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        assert_eq!(json.to_string(), r#"{"name":"John"}"#);
    }

    #[test]
    fn replace_operation() {
        let json = r#"{"items": [10, 20, 30]}"#;
        let mut json = Jsonb::from_str(json).unwrap();

        let value = Jsonb::from_str("50").unwrap();
        let mut operation = ReplaceOperation::new(value);

        let path = JsonPath {
            elements: vec![
                PathElement::Root,
                PathElement::Key(Cow::Borrowed("items"), false),
                PathElement::ArrayLocator(Some(1)),
            ],
        };

        let result = json.operate_on_path(&path, &mut operation);
        assert!(result.is_ok());

        assert_eq!(json.to_string(), r#"{"items":[10,50,30]}"#);
    }
}
