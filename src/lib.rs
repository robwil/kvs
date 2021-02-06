#![deny(missing_docs)]

//! The kvs crate library implements a KvStore type, which is a basic key-value store.
//! Currently, it stores values in memory, but future work will store to disk.

use std::collections::HashMap;

/// A basic String key-value store, which will store its keys and values in memory.
pub struct KvStore {
    map: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    ///
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }

    /// Set a `value` for `key`. If `key` was already present, the new `value` will override it.
    pub fn set(&mut self, key: String, value: String) {
        self.map.insert(key, value);
    }

    /// Get Some(value) from the KvStore, searching by `key`. If the `key` is not present, None will be returned.
    pub fn get(&self, key: String) -> Option<String> {
        match self.map.get(&key) {
            Some(val) => Some(val.clone()),
            None => None,
        }
    }

    /// Removes `key` from the KvStore. This will succeed whether the `key` is present or not.
    pub fn remove(&mut self, key: String) {
        self.map.remove(&key);
    }
}
