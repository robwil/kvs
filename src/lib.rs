#![deny(missing_docs)]
#![warn(rust_2018_idioms)]

//! The kvs crate library implements a KvStore type, which is a basic key-value store.
//! Currently, it stores values in memory, but future work will store to disk.

mod command;

pub use anyhow::Result;
use anyhow::{anyhow, bail, Context};
use command::Command;
use std::collections::HashMap;
use std::collections::HashSet;
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
    // keep track of wasted bytes (eligible for compaction)
    wasted_bytes: usize,
}

const COMPACTION_BYTES_THRESHOLD: usize = 1024 * 1024; // 1MB wasted space (very eager compaction)

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
            // need to create writer & reader handles since we won't be going through usual load() path below
            writer = get_write_handle(&path, 1, LogFileType::Blessed)
                .context("Opening file for writing during initialization")?;
            readers.insert(1, get_read_handle(&path, 1, LogFileType::Blessed)?);
        } else {
            current_generation = gen_list.last().copied().unwrap();
            writer = get_write_handle(&path, current_generation, LogFileType::Blessed)
                .context("Opening file for writing during initialization")?;
        }

        let mut kvs = Self {
            path,
            map: internal_map,
            current_generation,
            writer,
            readers,
            wasted_bytes: 0,
        };

        for generation in gen_list {
            kvs.load(generation)?;
        }

        Ok(kvs)
    }

    /// load will read a generation's log file from disk, modifying the in-memory map with the proper file offsets
    fn load(&mut self, generation: u64) -> Result<()> {
        let mut reader = get_read_handle(&self.path, generation, LogFileType::Blessed)
            .context("Opening file for reading during load")?;
        let mut current_pos = reader.seek(SeekFrom::Current(0))?;
        while let Ok(cmd) = Command::from_reader(&mut reader) {
            match cmd {
                Command::Set { key, value } => {
                    let estimated_bytes = key.len() + value.len();
                    self.wasted_bytes +=
                        self.map
                            .set(&key, generation, current_pos, estimated_bytes)?;
                }
                Command::Remove { key } => {
                    self.wasted_bytes += self.map.remove(&key)?;
                }
            }
            current_pos = reader.seek(SeekFrom::Current(0))?;
        }
        self.readers.insert(generation, reader);
        Ok(())
    }

    /// Set a `value` for `key`. If `key` was already present, the new `value` will override it.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let current_pos = self.writer.seek(SeekFrom::End(0))?;
        let estimated_bytes = key.len() + value.len();
        let cmd = Command::Set {
            key: key.clone(),
            value,
        };
        cmd.to_writer(&mut self.writer)?;
        self.writer.flush()?;
        // internal book-keeping performed after successful disk write
        self.wasted_bytes +=
            self.map
                .set(&key, self.current_generation, current_pos, estimated_bytes)?;
        self.maybe_run_compaction()?;
        Ok(())
    }

    /// Get Some(value) from the KvStore, searching by `key`. If the `key` is not present, None will be returned.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key)? {
            None => Ok(None),
            Some(LogEntry {
                generation,
                file_pos,
                estimated_bytes: _,
            }) => {
                let mut reader = self.readers.get_mut(&generation).ok_or_else(|| anyhow!(
                    "Unable to open reader for generation {} during get",
                    generation
                ))?;
                reader.seek(SeekFrom::Start(file_pos))?;
                Command::from_reader(&mut reader).map(|cmd| match cmd {
                    Command::Set { key: _, value } => Some(value),
                    Command::Remove { key: _ } => None,
                })
            }
        }
    }

    /// Removes `key` from the KvStore. This will throw an error if the `key` does not already exist.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let cmd = Command::Remove { key: key.clone() };
        cmd.to_writer(&mut self.writer)?;
        self.writer.flush()?;
        // internal book-keeping performed after successful disk write
        self.wasted_bytes += self.map.remove(&key)?;
        self.maybe_run_compaction()?;
        Ok(())
    }

    /// Checks if compaction is desired, and if so run the compaction now.
    fn maybe_run_compaction(&mut self) -> Result<()> {
        if self.wasted_bytes < COMPACTION_BYTES_THRESHOLD {
            return Ok(());
        }

        // Step 1) Create two new log files, one for compaction and one for new writes.
        let gen_list = sorted_gen_list(&self.path)?;
        let compaction_target_generation = self.current_generation + 1;
        let new_writes_generation = self.current_generation + 2;
        let mut compaction_writer = get_write_handle(
            &self.path,
            compaction_target_generation,
            LogFileType::Temporary,
        )?;
        let new_writer = get_write_handle(&self.path, new_writes_generation, LogFileType::Blessed)?;
        // Note: Most of this Step 1 implementation is meant to be future-proof for multi-threading,
        //       but this transition to a new write generation probably requires a critical section (i.e. Mutex)
        self.writer = new_writer;
        self.readers.insert(
            compaction_target_generation,
            get_read_handle(
                &self.path,
                compaction_target_generation,
                LogFileType::Temporary,
            )?,
        );
        self.readers.insert(
            new_writes_generation,
            get_read_handle(&self.path, new_writes_generation, LogFileType::Blessed)?,
        );
        self.current_generation = new_writes_generation;

        // Step 2) Read previous log files, writing the latest values of any keys encountered
        // to the new compaction target file.
        let mut already_handled = HashSet::new();
        for generation in gen_list.clone() {
            let mut reader = get_read_handle(&self.path, generation, LogFileType::Blessed)?;
            reader.seek(SeekFrom::Start(0))?;
            while let Ok(cmd) = Command::from_reader(&mut reader) {
                match cmd {
                    Command::Set { key, value: _ } => {
                        if already_handled.contains(&key) {
                            // no work to be done, we already handled (wrote) latest value of key to compaction target
                            continue;
                        }
                        // look up latest value for key and write to compaction target
                        if let Some(value) = self.get(key.clone())? {
                            let current_pos = compaction_writer.seek(SeekFrom::End(0))?;
                            let estimated_bytes = key.len() + value.len();
                            Command::Set {
                                key: key.clone(),
                                value,
                            }
                            .to_writer(&mut compaction_writer)?;
                            self.writer.flush()?;
                            // now must update in-memory map to allow future reads to get this value
                            // (and not try to read from old files which we're about to delete in step 4)
                            self.map.set(
                                &key,
                                compaction_target_generation,
                                current_pos,
                                estimated_bytes,
                            )?;
                        }
                        already_handled.insert(key);
                    }
                    Command::Remove { key } => {
                        already_handled.insert(key);
                    }
                }
            }
        }

        // Step 3) now all previous logs are compacted into compaction_target_generation, so bless that file by renaming it.
        // NOTE: multi-threading will require a critical section here too
        drop(compaction_writer);
        fs::rename(
            log_path(
                &self.path,
                compaction_target_generation,
                LogFileType::Temporary,
            ),
            log_path(
                &self.path,
                compaction_target_generation,
                LogFileType::Blessed,
            ),
        )?;
        self.readers.insert(
            compaction_target_generation,
            get_read_handle(
                &self.path,
                compaction_target_generation,
                LogFileType::Blessed,
            )?,
        );
        self.wasted_bytes = 0;

        // Step 4) Previous logs are now obsolete, so remove them.
        for generation in gen_list {
            fs::remove_file(log_path(&self.path, generation, LogFileType::Blessed))?;
        }

        Ok(())
    }
}

/// InternalMap is the in-memory mapping of keys used to save trips to disk.
/// The values in the map are file offsets used to seek to the true values on disk.
#[derive(Debug)]
struct InternalMap {
    map: HashMap<String, LogEntry>,
}

#[derive(Debug, Clone)]
struct LogEntry {
    // track which generation log file the value was written to
    generation: u64,
    // track file offset within that file where we can read the value
    file_pos: u64,
    // estimate the total bytes necessary to store the key and value to disk
    // this is used to estimate wasted space eligible for compaction
    estimated_bytes: usize,
}

impl InternalMap {
    fn new() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
    /// Create entry in InternalMap that tracks the LogEntry for this key.
    /// Returns estimate of wasted bytes detected (if we just overwrote an existing key).
    fn set(
        &mut self,
        key: &str,
        generation: u64,
        file_pos: u64,
        estimated_bytes: usize,
    ) -> Result<usize> {
        let mut wasted_bytes = 0;
        if let Some(entry_that_will_be_overwritten) = self.map.get(key) {
            if entry_that_will_be_overwritten.generation > generation {
                // if incoming write is from a previous generation as the one it is replacing
                // then exit early (i.e. block the update). This should only ever happen
                // during compaction.
                return Ok(0);
            }
            wasted_bytes = entry_that_will_be_overwritten.estimated_bytes;
        }
        self.map.insert(
            key.to_owned(),
            LogEntry {
                generation,
                file_pos,
                estimated_bytes,
            },
        );
        Ok(wasted_bytes)
    }
    fn get(&self, key: &str) -> Result<Option<LogEntry>> {
        Ok(self.map.get(key).cloned())
    }
    /// Remove entry in InternalMap, signifying deletion on disk.
    /// Returns estimate of wasted bytes detected (if we just removed an existing key).
    fn remove(&mut self, key: &str) -> Result<usize> {
        let mut wasted_bytes = 0;
        if let Some(entry_that_will_be_overwritten) = self.map.get(key) {
            wasted_bytes = entry_that_will_be_overwritten.estimated_bytes;
        }
        if self.map.remove(key).is_none() {
            bail!("Key not found");
        }
        Ok(wasted_bytes)
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

enum LogFileType {
    Temporary, // temporary log file, used during compaction, should not receive active reads or writes
    Blessed,   // blessed log file, ready for active reads and write
}

fn get_write_handle(path: &Path, gen: u64, temporary: LogFileType) -> Result<BufWriter<fs::File>> {
    let file_path = log_path(path, gen, temporary);
    let context = format!("Opening file {:?} for writing", file_path.to_str());
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(file_path)
        .context(context)?;
    Ok(BufWriter::new(file))
}

fn get_read_handle(
    path: &Path,
    gen: u64,
    log_file_type: LogFileType,
) -> Result<BufReader<fs::File>> {
    let file_path = log_path(path, gen, log_file_type);
    let context = format!("Opening file {:?} for reading", file_path.to_str());
    let file = fs::OpenOptions::new()
        .read(true)
        .open(file_path)
        .context(context)?;
    Ok(BufReader::new(file))
}

fn log_path(dir: &Path, gen: u64, log_file_type: LogFileType) -> PathBuf {
    let file_extension = match log_file_type {
        LogFileType::Temporary => "tmp",
        LogFileType::Blessed => "log",
    };
    dir.join(format!("{}.{}", gen, file_extension))
}
