#![deny(missing_docs)]

//! The kvs crate library implements a KvStore type, which is a basic key-value store.
//! Currently, it stores values in memory, but future work will store to disk.

mod command;

pub use anyhow::Result;
use anyhow::{anyhow, bail, Context};
use command::Command;
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
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
#[derive(Debug)]
pub struct KvStore {
    // directory for the log and other data.
    path: PathBuf,
    // internal map used to handle the in-memory storing of the keys
    map: InternalMap,
    // current generation
    current_generation: u64,
    // current write handle (to current generation)
    writer: BufWriter<fs::File>,
    // all generations reader handles
    readers: HashMap<u64, BufReader<fs::File>>,
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

        let internal_map = InternalMap::new();
        let mut readers = HashMap::new();
        let gen_list = sorted_gen_list(&path)?;
        let current_generation;
        let writer;
        if gen_list.is_empty() {
            // Brand new database, so start with current_generation = 1
            current_generation = 1;
            // need to create writer & reader handles since we won't be going through usual load() path
            writer = get_write_handle(&path, 1)
                .context("Opening file for writing during initialization")?;
            readers.insert(1, get_read_handle(&path, 1)?);
        } else {
            current_generation = gen_list.last().copied().unwrap();
            writer = get_write_handle(&path, current_generation)
                .context("Opening file for writing during initialization")?;
        }

        let mut kvs = Self {
            path,
            map: internal_map,
            current_generation,
            writer,
            readers,
        };

        for generation in gen_list {
            kvs.load(generation)?;
        }

        Ok(kvs)
    }

    fn load(&mut self, generation: u64) -> Result<()> {
        let mut reader = get_read_handle(&self.path, generation)
            .context("Opening file for reading during load")?;
        let mut current_pos = reader.seek(SeekFrom::Current(0))?;
        while let Ok(cmd) = Command::from_reader(&mut reader) {
            match cmd {
                Command::Set { key, value: _ } => {
                    self.map.set(&key, current_pos)?;
                }
                Command::Remove { key } => {
                    self.map.remove(&key)?;
                }
            }
            current_pos = reader.seek(SeekFrom::Current(0))?;
        }
        self.readers.insert(generation, reader);
        Ok(())
    }

    /// Set a `value` for `key`. If `key` was already present, the new `value` will override it.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // Store current end position to map
        let current_pos = self.writer.seek(SeekFrom::End(0))?;
        self.map.set(&key, current_pos)?;

        // Actually write to file
        let cmd = Command::Set { key, value };
        cmd.to_writer(&mut self.writer)?;
        self.writer.flush()?;

        Ok(())
    }

    /// Get Some(value) from the KvStore, searching by `key`. If the `key` is not present, None will be returned.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let maybe_file_pos = self.map.get(&key)?;
        if maybe_file_pos.is_none() {
            return Ok(None);
        }
        let file_pos = maybe_file_pos.unwrap();
        // TODO: don't assume all values are in current gen
        let mut reader = self
            .readers
            .get_mut(&self.current_generation)
            .ok_or(anyhow!("Unable to open reader for current generation"))?;
        reader.seek(SeekFrom::Start(file_pos))?;
        Command::from_reader(&mut reader).map(|cmd| match cmd {
            Command::Set { key: _, value } => Some(value),
            Command::Remove { key: _ } => None,
        })
    }

    /// Removes `key` from the KvStore. This will succeed whether the `key` is present or not.
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.map.remove(&key)?;

        let cmd = Command::Remove { key };
        cmd.to_writer(&mut self.writer)?;
        Ok(())
    }
}

/// InternalMap is the in-memory mapping of keys used to save trips to disk.
/// The values in the map are file offsets used to seek to the true values on disk.
#[derive(Debug)]
struct InternalMap {
    map: HashMap<String, u64>,
}

impl InternalMap {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    fn set(&mut self, key: &str, file_pos: u64) -> Result<()> {
        self.map.insert(key.to_owned(), file_pos);
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Option<u64>> {
        Ok(self.map.get(key).cloned())
    }
    fn remove(&mut self, key: &str) -> Result<()> {
        if self.map.remove(key).is_none() {
            bail!("Key not found");
        }
        Ok(())
    }
}

/// Returns sorted generation numbers in the given directory.
fn sorted_gen_list(path: &Path) -> Result<Vec<u64>> {
    let mut gen_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    gen_list.sort_unstable();
    Ok(gen_list)
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
