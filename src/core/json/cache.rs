use crate::{core::json::jsonb::Jsonb, sql::statement::Value};
use std::cell::{Cell, UnsafeCell};

const CACHE_SIZE: usize = 4;

#[derive(Debug)]
pub(crate) struct JsonCache {
    entries: [Option<(Value, Jsonb)>; CACHE_SIZE],
    age: [usize; CACHE_SIZE],
    used: usize,
    counter: usize,
}

#[derive(Debug)]
pub(crate) struct JsonCacheCell {
    inner: UnsafeCell<Option<JsonCache>>,
    accessed: Cell<bool>,
}

impl JsonCache {
    pub fn new() -> Self {
        Self {
            entries: [None, None, None, None],
            age: [0, 0, 0, 0],
            used: 0,
            counter: 0,
        }
    }

    pub fn insert(&mut self, key: &Value, value: &Jsonb) {
        match self.used < CACHE_SIZE {
            true => {
                self.entries[self.used] = Some((key.clone(), value.clone()));
                self.age[self.used] = self.counter;
                self.counter += 1;
                self.used += 1;
            }
            false => {
                let idx = self.find_oldest();

                self.entries[idx] = Some((key.clone(), value.clone()));
                self.age[idx] = self.counter;
                self.counter += 1;
            }
        }
    }

    pub fn lookup(&mut self, key: &Value) -> Option<Jsonb> {
        for idx in (0..self.used).rev() {
            if let Some((stored_key, value)) = &self.entries[idx] {
                if key.clone() == *stored_key {
                    self.age[idx] = self.counter;
                    self.counter += 1;
                    let json = value.clone();

                    return Some(json);
                }
            }
        }

        None
    }

    pub fn find_oldest(&self) -> usize {
        let mut idx = 0;
        let mut age = self.age[0];

        (1..self.used).for_each(|i| {
            if self.age[i] < age {
                idx = i;
                age = self.age[i];
            }
        });

        idx
    }

    pub fn clear(&mut self) {
        self.counter = 0;
        self.used = 0;
    }
}

impl JsonCacheCell {
    pub fn new() -> Self {
        Self {
            inner: UnsafeCell::new(None),
            accessed: Cell::new(false),
        }
    }

    pub fn get_or_insert(
        &self,
        key: &Value,
        value: impl FnOnce(&Value) -> super::Result<Jsonb>,
    ) -> super::Result<Jsonb> {
        assert!(!self.accessed.get());

        self.accessed.set(true);
        let result = unsafe {
            let ptr = self.inner.get();
            if (*ptr).is_none() {
                *ptr = Some(JsonCache::new());
            }

            if let Some(cache) = &mut (*ptr) {
                match cache.lookup(&key) {
                    Some(jsonb) => Ok(jsonb),
                    _ => {
                        let result = value(&key);
                        match result {
                            Ok(json) => {
                                cache.insert(&key, &json);
                                Ok(json)
                            }
                            _ => result,
                        }
                    }
                }
            } else {
                value(&key)
            }
        };

        self.accessed.set(false);
        result
    }

    pub(in crate::core::json) fn clear(&mut self) {
        self.accessed.set(false);
        self.inner = UnsafeCell::new(None);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn pair(json: &str) -> (Value, Jsonb) {
        let key = Value::from(json);
        let value = Jsonb::from_str(json).unwrap();

        (key, value)
    }

    #[test]
    fn json_cache() {
        let cache = JsonCache::new();

        assert_eq!(cache.used, 0);
        assert_eq!(cache.counter, 0);
        assert_eq!(cache.age, [0, 0, 0, 0]);

        assert!(cache.entries.iter().all(|entry| entry.is_none()));
    }

    #[test]
    fn json_insert_and_lookup() {
        let mut cache = JsonCache::new();
        let json_str = "{\"test\": \"value\"}";
        let (key, value) = pair(json_str);

        cache.insert(&key, &value);
        assert_eq!(cache.used, 1);
        assert_eq!(cache.counter, 1);

        let result = cache.lookup(&key);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), value);
        assert_eq!(cache.counter, 2);
    }

    #[test]
    fn multiple_cache_entries() {
        let mut cache = JsonCache::new();

        let (key1, value1) = pair("{\"id\": 1}");
        let (key2, value2) = pair("{\"id\": 2}");
        let (key3, value3) = pair("{\"id\": 3}");

        cache.insert(&key1, &value1);
        cache.insert(&key2, &value2);
        cache.insert(&key3, &value3);

        assert_eq!(cache.used, 3);
        assert_eq!(cache.counter, 3);

        let result3 = cache.lookup(&key3);
        let result2 = cache.lookup(&key2);
        let result1 = cache.lookup(&key1);

        assert_eq!(result3.unwrap(), value3);
        assert_eq!(result2.unwrap(), value2);
        assert_eq!(result1.unwrap(), value1);
        assert_eq!(cache.counter, 6);
    }
}
