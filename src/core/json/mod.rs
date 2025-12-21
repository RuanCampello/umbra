use crate::{
    core::json::{
        cache::JsonCacheCell,
        jsonb::{parse_error, ElementType, JsonHeader, SearchOperation},
        path::{json_path, JsonPath, PathElement},
    },
    parse_error,
    sql::statement::Value,
};
use error::Error as JsonError;
use jsonb::Jsonb;
use std::{borrow::Cow, hint::unreachable_unchecked, str::FromStr};

mod cache;
pub mod error;
mod jsonb;
mod ops;
mod path;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Conv {
    Strict,
    NotStrict,
    ToString,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum OutputFlag {
    String,
    ElementType,
    Binary,
}

pub type Result<T> = std::result::Result<T, JsonError>;

pub fn jsonb(value: &Value, cache: &JsonCacheCell) -> Result<Value> {
    let json_fn = fn_to_json(Conv::Strict);
    let json = cache.get_or_insert(value, json_fn);

    match json {
        Ok(json) => Ok(Value::Blob(json.data())),
        _ => parse_error!("Malformed JSON"),
    }
}

pub fn get(value: &Value, indent: Option<&str>) -> Result<Value> {
    match value {
        Value::String(_) => {
            let value = from_value_to_jsonb(value, Conv::Strict)?;
            let json = match indent {
                Some(indent) => value.to_string_pretty(Some(indent))?,
                _ => value.to_string(),
            };

            Ok(Value::String(json))
        }
        Value::Blob(blob) => {
            let json = Jsonb::new(blob.len(), Some(blob));
            json.element_type()?;

            Ok(Value::String(json.to_string()))
        }
        Value::Null => Ok(Value::Null),
        _ => {
            let value = from_value_to_jsonb(value, Conv::Strict)?;
            let json = match indent {
                Some(indent) => Value::String(value.to_string_pretty(Some(indent))?),
                _ => {
                    let element_type = value.element_type()?;
                    from_json_to_value(value, element_type, OutputFlag::ElementType)?
                }
            };

            Ok(json)
        }
    }
}

pub fn object<'v, I, E>(values: I) -> Result<Value>
where
    E: ExactSizeIterator<Item = &'v Value>,
    I: IntoIterator<IntoIter = E, Item = &'v Value>,
{
    let mut values = values.into_iter();
    if values.len() % 2 != 0 {
        return Err(JsonError::Internal(format!(
            "json::object requires an even number of arguments"
        )));
    }

    let mut json = Jsonb::empty_object(values.len() * 50);

    while values.len() > 1 {
        let first = values.next().ok_or_else(|| {
            JsonError::Internal("Values must have at least 2 elements".to_string())
        })?;

        if !matches!(first, &Value::String(_)) {
            return Err(JsonError::Internal(
                "json::object labels must be text type".to_string(),
            ));
        }

        let key = from_value_to_jsonb(first, Conv::ToString)?;
        json.append(key.data());

        let second = values.next().ok_or_else(|| {
            JsonError::Internal("Values must have at least 2 elements".to_string())
        })?;
        let value = from_value_to_jsonb(second, Conv::NotStrict)?;
        json.append(value.data());
    }

    json.finalise(ElementType::OBJECT)?;
    from_json_to_value(json, ElementType::OBJECT, OutputFlag::String)
}

pub fn from_value_to_jsonb(value: &Value, strict: Conv) -> Result<Jsonb> {
    match value {
        Value::String(string) => {
            let result = match strict {
                Conv::Strict => Jsonb::from_str_with_mode(&string, strict),
                Conv::NotStrict => Jsonb::from_str_with_mode(&string, strict)
                    .or(Jsonb::from_str(&escape_string(&string))),
                _ => Jsonb::from_str(&escape_string(&string)),
            };

            result.map_err(|_| parse_error("Malformed JSON", None))
        }
        Value::Null => Ok(Jsonb::from_data(JsonHeader::null().into_bytes().as_bytes())),
        #[rustfmt::skip]
        Value::Number(int) => Jsonb::from_str(&int.to_string()).map_err(|_| parse_error("Malformed JSON", None)),
        Value::Float(float) => match float.is_infinite() {
            true => {
                let json = match float.is_sign_negative() {
                    true => "-9.0e+999",
                    _ => "9.0e+999",
                };

                Jsonb::from_str(json).map_err(|_| parse_error("Malformed JSON", None))
            }
            _ => Jsonb::from_str(float.to_string().as_str())
                .map_err(|_| parse_error("Malformed JSON", None)),
        },
        Value::Blob(blob) => {
            let index = blob
                .iter()
                .position(|&b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
                .unwrap_or(blob.len());
            let slice = &blob[index..];

            let json = match slice {
                [b'"', ..] | [b'-', ..] | [b'0'..=b'2', ..] => slice_to_json(slice, strict)?,
                _ => match JsonHeader::from_slice(0, slice) {
                    Ok((header, offset)) => {
                        let size = header.payload_size();
                        let expected = size + offset;

                        match expected != slice.len() {
                            true => slice_to_json(slice, strict)?,
                            _ => {
                                let json = Jsonb::from_data(slice);
                                let is_valid = match expected <= 7 {
                                    true => json.is_valid(),
                                    _ => json.element_type().is_ok(),
                                };

                                match is_valid {
                                    true => json,
                                    _ => slice_to_json(slice, strict)?,
                                }
                            }
                        }
                    }
                    Err(_) => slice_to_json(slice, strict)?,
                },
            };

            json.element_type()?;
            Ok(json)
        }
        _ => unsafe { unreachable_unchecked() },
    }
}

fn json_array_length(value: &Value, path: Option<&Value>, cache: &JsonCacheCell) -> Result<Value> {
    let json_fn = fn_to_json(Conv::Strict);
    let mut json = cache.get_or_insert(value, json_fn)?;

    if path.is_none() {
        let length = json.array_length()?;
        return Ok(Value::Number(length as i128));
    }

    let path = from_value_to_path(path.unwrap(), true)?;

    if let Some(path) = path {
        let mut operation = SearchOperation::new(json.len() / 2);
        json.operate_on_path(&path, &mut operation)?;

        if let Ok(len) = operation.result().array_length() {
            return Ok(Value::Number(len as i128));
        }
    }

    Ok(Value::Null)
}

fn from_value_to_path<'v>(path: &'v Value, strict: bool) -> Result<Option<JsonPath<'v>>> {
    let path = match strict {
        true => match path {
            Value::String(string) => json_path(string.as_str())?,
            Value::Null => return Ok(None),
            _ => {
                return Err(JsonError::Internal(format!(
                    "JSON path error near: {:?}",
                    path.to_string()
                )))
            }
        },
        _ => match path {
            Value::String(string) => match string.starts_with("$") {
                true => json_path(string.as_str())?,
                _ => JsonPath {
                    elements: vec![
                        PathElement::Root,
                        PathElement::Key(Cow::Borrowed(string.as_str()), false),
                    ],
                },
            },
            Value::Number(int) => JsonPath {
                elements: vec![
                    PathElement::Root,
                    PathElement::ArrayLocator(Some(*int as i32)),
                ],
            },
            Value::Float(float) => JsonPath {
                elements: vec![
                    PathElement::Root,
                    PathElement::Key(Cow::Owned(float.to_string()), false),
                ],
            },
            Value::Null => return Ok(None),
            _ => {
                return Err(JsonError::Internal(format!(
                    "JSON path error near: {:?}",
                    path.to_string()
                )))
            }
        },
    };

    Ok(Some(path))
}

fn fn_to_json(conversion: Conv) -> impl FnOnce(&Value) -> Result<Jsonb> {
    move |value| from_value_to_jsonb(value, conversion)
}

fn slice_to_json(slice: &[u8], conversion: Conv) -> Result<Jsonb> {
    let zero = slice.iter().position(|&b| b == 0).unwrap_or(slice.len());
    let truncate = &slice[..zero];
    let str = std::str::from_utf8(truncate).map_err(|_| parse_error("Malformed JSON", None))?;

    Jsonb::from_str_with_mode(str, conversion).map_err(Into::into)
}

fn from_json_to_value(json: Jsonb, element_type: ElementType, flag: OutputFlag) -> Result<Value> {
    let mut string = json.to_string();

    if matches!(flag, OutputFlag::Binary) {
        return Ok(Value::Blob(json.data()));
    }

    Ok(match element_type {
        ElementType::ARRAY | ElementType::OBJECT => Value::String(string),
        ElementType::TEXT | ElementType::TEXT5 | ElementType::TEXTJ | ElementType::TEXTRAW => {
            match matches!(flag, OutputFlag::ElementType) {
                false => Value::String(string),
                _ => {
                    string.remove(string.len() - 1);
                    string.remove(0);

                    Value::String(string)
                }
            }
        }
        ElementType::INT | ElementType::INT5 => {
            let result = i64::from_str(&string);

            match result {
                Ok(int) => Value::Number(int as i128),
                _ => {
                    let result = f64::from_str(&string);
                    match result {
                        Ok(float) => Value::Float(float),
                        _ => Value::Null,
                    }
                }
            }
        }
        ElementType::NULL => Value::Null,
        _ => unreachable!(),
    })
}

fn escape_string(str: &str) -> String {
    let mut str = str.replace('\\', "\\\\").replace('"', "\\\"");
    str.insert(0, '"');
    str.push('"');
    str
}

#[cfg(test)]
mod tests {
    use crate::core::json::jsonb::{DeleteOperation, InsertOperation, ReplaceOperation};

    use super::*;

    #[test]
    fn blob_jsonb() {
        let binary = vec![124, 55, 104, 101, 121, 39, 121, 111];
        let input = Value::Blob(binary);
        let json = get(&input, None).unwrap();

        match json {
            Value::String(string) => {
                assert!(string.as_str().contains(r#"{"hey":"yo"}"#));
            }
            _ => panic!("Expected Value::String"),
        }
    }

    #[test]
    fn array_length() {
        let input = Value::String("[1,2,3,4]".into());
        let mut cache = JsonCacheCell::new();
        let result = json_array_length(&input, Some(&Value::String("$".into())), &cache).unwrap();

        match result {
            Value::Number(size) => assert_eq!(size, 4),
            _ => panic!("Expected Value::Number"),
        };

        let input = Value::String("{key: [1,2,3,4]}".into());
        cache.clear();
        let result = json_array_length(&input, None, &cache).unwrap();

        match result {
            Value::Number(size) => assert_eq!(size, 0),
            _ => panic!("Expected Value::Number"),
        };

        cache.clear();
        let result =
            json_array_length(&input, Some(&Value::String("$.key".into())), &cache).unwrap();

        match result {
            Value::Number(size) => assert_eq!(size, 4),
            _ => panic!("Expected Value::Number"),
        };
    }

    #[test]
    fn array_length_indexing() {
        let input = Value::String("[[1,2,3,4]]".into());
        let cache = JsonCacheCell::new();
        let result =
            json_array_length(&input, Some(&Value::String("$[0]".into())), &cache).unwrap();

        match result {
            Value::Number(size) => assert_eq!(size, 4),
            _ => panic!("Expected Value::Number"),
        };
    }

    #[test]
    fn json_object() {
        let key = Value::String("key".into());
        let value = Value::String("value".into());
        let input = [key, value];

        let parent_key = Value::String("parent_key".into());
        let parent_value = object(&input).unwrap();
        let parent_input = [parent_key, parent_value];

        let result = object(&parent_input).unwrap();

        match result {
            Value::String(json) => assert_eq!(json.as_str(), r#"{"parent_key":{"key":"value"}}"#),
            _ => panic!("Expected Value::Text"),
        }
    }
}
