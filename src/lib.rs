#![deny(missing_docs)]

//! The kvs crate library implements a KvStore type, which is a basic key-value store.
//! Currently, it stores values in memory, but future work will store to disk.

use std::path::PathBuf;
use std::collections::HashMap;
pub use anyhow::Result;

/// A basic String key-value store, which will store its keys and values in memory.
/// 
/// ```rust
/// # use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key".to_owned(), "value".to_owned());
/// let val = store.get("key".to_owned());
/// assert_eq!(val, Some("value".to_owned()));
/// ```
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

    /// Open the KvStore at a given path. Return the KvStore.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            map: HashMap::new(),
        })
    }

    /// Set a `value` for `key`. If `key` was already present, the new `value` will override it.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key, value);
        Ok(())
    }

    /// Get Some(value) from the KvStore, searching by `key`. If the `key` is not present, None will be returned.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    /// Removes `key` from the KvStore. This will succeed whether the `key` is present or not.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(&key);
        Ok(())
    }
}