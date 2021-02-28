# kvs

`kvs` is a key-value store implemented in Rust.

It is being built for learning purposes, as part of the [PingCap Practical Networked Applications in Rust course](https://github.com/pingcap/talent-plan/tree/master/courses/rust).

## Implementation Notes

### Project 1

Project 1 was all about creating the basic `struct` API interface for the KvStore library, and then making a companion CLI tool. The key decision here was the usage of the `clap` crate for handling CLI argument parsing.

### Project 2

Project 2 is where the bulk of the key-value store logic was built, using a log structured storage approach similar to BitCask.

The most interesting part of this algorithm is how to implement compaction. That is, given incoming commands which invalidate previous log entries, how do we ensure disk space doesn't grow infinitely for stale entries?

| idx | command |
|:---:|:--------|
| 0 | ~Command::Set("key-1", "value-1a")~ |
| 20 | Command::Set("key-2", "value-2") |
| | ... |
| 100 | Command::Set("key-1", "value-1b") |

The above example shows what this might look like. `key-1` has a new value, so the original value stored at the beginning of the log is wasted space.

The way I implemented compaction is like this:
1) All incoming writes keep track of which file they are writing to (`current`). At the beginning, this is only a single log file.

2) For each `Set` which overwrites a previous value, and for each `Remove`, increment a running tally of wasted bytes.

3) When the tally of wasted bytes exceeds some configurable threshold (`COMPACTION_BYTES_THRESHOLD`), trigger the compaction process.

    1) Create two new log files. `current+1.tmp` will be used to compact all previous logs (`N <= current`>), while `current+2.log` will receive any new writes. Note that in the current implementation, everything is single-threaded so there won't be any new writes during compaction, but this choice will future proof us for Project 4 when we introduce multi threading.

    2) Read all previous log files, in sequential order (e.g. 1, 2, 3, ...). For each key encountered, use the already-in-memory index to read its latest value from disk. If the current value does not exist (i.e. the key has been removed), skip this key and continue. Otherwise, write the key and value to disk. Mark this key as "handled" in a temporary new map, so we don't have to keep searching and writing such entries.

    3) When all previous keys have been handled, we can "bless" the new compacted file by renaming it from `current+1.tmp` to `current+1.log`.

    4) Previous logs (`N <= current`>) can now be removed.

If anything goes wrong during steps 1 - 3, there will just be an orphaned `current+1.tmp` file (which could be cleaned up during next compaction). If something goes wrong during step 4, then that means some or all of the previous log files will still be around. However, in this case, the newly blessed `current+1.log` will contain all the latest data, so a partial deletion of old logs will have no impact on data correctness. A subsequent compaction step will correct any of the redundancy present.