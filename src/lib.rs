#![deny(missing_docs)]

//! The kvs crate library implements a KvStore type, which is a basic key-value store.
//! Currently, it stores values in memory, but future work will store to disk.

mod command;

use std::path::Path;
pub use anyhow::Result;
use anyhow::{bail, Context};
use command::Command;
use std::collections::HashMap;
use std::fs;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;

/// A basic String key-value store, which will store its keys and values in memory.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    // directory for the log and other data.
    path: PathBuf,
    // internal map used to handle the in-memory storing of the keys
    map: InternalMap,
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path).context("Creating directory for log files")?;

        let mut internal_map = InternalMap::new();
        // TODO: don't hard-code gen
        if let Ok(mut file) = get_read_handle(&path, 1) {
            let mut current_pos = file.seek(SeekFrom::Current(0))?;
            while let Ok(cmd) = Command::from_reader(&mut file) {
                match cmd {
                    Command::Set { key, value: _ } => {
                        internal_map.set(key, current_pos)?;
                    }
                    Command::Remove { key } => {
                        internal_map.remove(key)?;
                    }
                }
                current_pos = file.seek(SeekFrom::Current(0))?;
            }
        } else {
            // TODO: only skip file read if it's File Not Found error, as opposed to others
        }

        Ok(Self {
            path,
            map: internal_map,
        })
    }

    /// Set a `value` for `key`. If `key` was already present, the new `value` will override it.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // TODO: don't open file handle on every command
        // TODO: don't hard-code gen
        let mut file = get_write_handle(&self.path, 1)
            .context("Opening file for writing during set command")?;

        // Store current end position to map
        let current_pos = file.seek(SeekFrom::End(0))?;
        // TODO: remove these clones. actually should probably just make the input a &str
        self.map.set(key.clone(), current_pos)?;

        // Actually write to file
        let cmd = Command::Set { key, value };
        cmd.to_writer(&mut file)?;

        Ok(())
    }

    /// Get Some(value) from the KvStore, searching by `key`. If the `key` is not present, None will be returned.
    pub fn get(&self, key: String) -> Result<Option<String>> {
        let value = self.map.get(key)?.and_then(|file_pos| {
            // TODO: don't open file handle on every command
            // TODO: don't hard-code gen
            // TODO: these Results -> Option conversion are bad. How do we propagate Result out?
            let mut file = get_read_handle(&self.path, 1).ok()?;
            file.seek(SeekFrom::Start(file_pos)).ok()?;
            if let Ok(cmd) = Command::from_reader(&mut file) {
                match cmd {
                    Command::Set { key: _, value } => Some(value),
                    Command::Remove { key: _ } => None,
                }
            } else {
                None
            }
        });
        Ok(value)
    }

    /// Removes `key` from the KvStore. This will succeed whether the `key` is present or not.
    pub fn remove(&mut self, key: String) -> Result<()> {
        // TODO: remove these clones. actually should probably just make the input a &str
        self.map.remove(key.clone())?;

        // TODO: don't open file handle on every command
        // TODO: don't hard-code gen
        let file = get_write_handle(&self.path, 1)
            .context("Opening file for writing during remove command")?;
        let cmd = Command::Remove { key };
        cmd.to_writer(file)?;
        Ok(())
    }
}

/// InternalMap is the in-memory mapping of keys used to save trips to disk.
/// The values in the map are file offsets used to seek to the true values on disk.
struct InternalMap {
    map: HashMap<String, u64>,
}

impl InternalMap {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    fn set(&mut self, key: String, file_pos: u64) -> Result<()> {
        self.map.insert(key, file_pos);
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<u64>> {
        Ok(self.map.get(&key).cloned())
    }
    fn remove(&mut self, key: String) -> Result<()> {
        if self.map.remove(&key).is_none() {
            bail!("Key not found");
        }
        Ok(())
    }
}

fn get_write_handle(path: &Path, gen: u64) -> Result<BufWriter<fs::File>> {
    let file_path = log_path(path, gen);
    let context = format!("Opening file {:?} for writing", file_path.to_str());
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_path)
        .context(context)?;
    Ok(BufWriter::new(file))
}

fn get_read_handle(path: &Path, gen: u64) -> Result<BufReader<fs::File>> {
    let file_path = log_path(path, gen);
    let context = format!("Opening file {:?} for reading", file_path.to_str());
    let file = fs::OpenOptions::new()
        .read(true)
        .open(file_path)
        .context(context)?;
    Ok(BufReader::new(file))
}

fn log_path(dir: &Path, gen: u64) -> PathBuf {
    dir.join(format!("{}.log", gen))
}
