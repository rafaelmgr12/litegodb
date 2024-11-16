# Chapter 1 

This chapter introduces the challenges of building a database from scratch and the foundational concepts that will be explored in the subsequent chapters. The goal is to understand the transition from file management to database systems and the key considerations for building a database.

## From file to database

The first thing we will see is files and the challenges of using files.

### 1.1 Updating files in-place

```go
func SaveData1(path string, data []byte) error {
    fp, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
    if err != nil {
        return err
    }

    defer fp.Close()

    _, err = fp.Write(data)
    if err !=  nil{
        return err
    }

    return fp.Sync() // fsync
}

```

This codes creates the file if does not exist, or truncates the exisiting one before writing the content. And most importantly the data is not persistent unless you call `fsync` (as done with `fp.Sync()` in Go). However, this method has significant limitations when considering database requirements:

1. **Whole Content Update**:
    - This approach updates the entire file content at once, which is not efficiente for large datasets. Databases often require fine -grained updates, not wholesale replacements.

2. **Crash Risks**:
    - If the application crashes during the write operation, the file may be corrupted. Databases must ensure that data is written atomically to prevent corruption.

3. **Concurrency Issues**:
    - If multiple processes or threads write to the file simultaneously, the data may be corrupted. Databases must provide mechanisms to ensure that concurrent writes are safe.
        - Readers may access partially updated data.
        - Writers may overwrite each other’s changes without proper synchronization.
    - This is why most databases use a client-server model or a coordination layer.

### 1.2 Atomic renaming

To avoid the risks of in-place updates, a safer approach involves writing changes to a temporary file and then atomically renaming it to replace the old file:

```go
func SaveData2(path string, data []byte) error {
    tmp := fmt.Sprintf("%s.tmp.%d", path, randomInt())
    fp, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0664)
    if err != nil {
        return err
    }

    defer func() {
        fp.Close()
        if err != nil {
            os.Remove(tmp)
        }
    }()

    _, err = fp.Write(data)
    if err != nil {
        return err
    }

    err = fp.Sync() // fsync
    if err != nil {
        return err
    }

    return os.Rename(tmp, path)
}
```

#### Advantages of Atomic Renaming

1. **Crash Safety**:
    - If a crash occurs during the update, the original file remains intact.
2. **Concurrency-Friendly**:
    - Readers will always see either the old file or the complete new file, never an intermediate state.

#### Remaining Challenges

1. **Directory Syncing**:
    - `os.Rename` does not guarantee durability if a crash occurs before the metadata (like file references) is flushed to disk. To ensure durability, a `fsync` must also be called on the parent directory.
2. **Concurrent Writers**:
    - Multiple writers attempting simultaneous updates still require coordination to avoid conflicts.

#### Why does renaming *work*?

Filesystems maintain a mapping from file names to file data. When a file is replaced via renaming, the file name simply points to the new data without modifying the old data. This enables atomic renaming in filesystems, with the operation cost remaining constant regardless of the data size.

On Linux, the old file may still persist if it's being accessed by a reader, although it's no longer accessible via the file name. Readers can safely continue working with their version of the data, and writers are not blocked by readers. However, there must be a mechanism to prevent concurrent writers. The concurrency model we aim to implement is multi-reader-single-writer.

### 1.3 Append-only logs

Using an append-only log is another method to manage updates safely without overwriting existing data in-place. Here's how it works:

#### How Append-only Logs Work

1. **Log-Based Updates**:
    - Instead of updating the data directly, each modification is recorded as a new entry in a log file.
    - The log accumulates all updates sequentially.

Example of a log:

```css
0 | set a=1
1 | set b=2
2 | set a=3
3 | del b
```

- **The current state** of the is derived by replaying the log:
  - `a=3`
  - `b` is deleted

2. **Crash Safety**:
    - Because logs are append-only, no existing dasta is overwritten during an update. If a crash occurs, the old log entries remain intact.
    - Only the last entry might be incomplete, which can be detected and ignored during recovery.

3. **Concurrency Access**:
    - Multiple processes can read the log concurrently without risk of accessing partially updated data, as earlier entries remain unchanged.

#### Advantages of Append-Only Logs

1. **No Overwrite Risks**:
    - Each update appends to the end of the log, preserving all previous data.
2. **Crash Recovery**:
    - After a crash, replaying the log rebuilds the data's most recent state.
3. **Incremental Updates:**:
    - 2Each update is independent, making it easy to track changes over time.

#### Challenges of Append-Only Logs

1. **No Indexing**:
    - Logs are not designed for efficient lookups; finding the current value of a key requires replaying the entire log.
2. **Space Reclamation**:
    - Deleted or outdated entries remain in the log, consuming disk space. A separate process (like **compaction**) is needed to clean up old data.

#### Example: Atomic Log Updates with Checksums

Adding a checksum to each log entry ensures atomicity even if a crash occurs during an update. If the checksum is invalid, the entry is ignored:

```go
type LogEntry struct {
    Checksum uint32
    Data     []byte
}

func AppendLog(file *os.File, data []byte) error {
    checksum := crc32.ChecksumIEEE(data)
    entry := LogEntry{Checksum: checksum, Data: data}

    _, err := file.Write(serialize(entry)) // Append the new entry
    if err != nil {
        return err
    }

    return file.Sync() // fsync ensures durability
}

```

### How Append-Only Logs Improve Safety

1. **Atomicity**:
    - Appending ensures that previous log entries remain unaffected.
2. **Durability**:
    - Data is flushed to disk (`fsync`) after appending to ensure the update is not lost.
3. **Easy Recovery**:
    - During recovery, replay the log and discard any corrupted entries (e.g., with invalid checksums).

#### Comparison to In-Place Updates and Atomic Renaming

| Feature | In-Place Updates | Atomic Renaming | Append-Only Logs |
| --- | --- | --- | --- |
| **Crash Safety** | Risk of partial/corrupt updates | Old file remains intact | All previous entries intact |
| **Concurrency** | Readers may see mixed states | Readers see old/new state | Readers see consistent logs |
| **Performance** | Minimal overhead | Moderate (temporary file needed) | Moderate (log replay needed) |
| **Space Efficiency** | Overwrites old data | Temporary file usage | Requires periodic compaction |

#### **Why Use Append-Only Logs?**

Append-only logs are ideal for systems where:

- Crash recovery is critical.
- The focus is on ensuring data consistency rather than immediate space optimization.
- Incremental updates must be preserved for auditing or historical tracking.

The next step is to address the **lack of indexing** and **space reuse** by combining logs with indexing data structures.

### 1.4 `fsync` Gotchas

The fsync system call ensures that file data and metadata are written to the storage device, making the updates durable. However, there are specific challenges and nuances to its use:

Directory Syncing:

After renaming or creating files, it's necessary to call fsync on the parent directory to ensure that the directory mapping (name-to-file mapping) is also written to disk.
Without this step, a crash might result in a missing or incomplete directory entry, even if the file data itself is durable.
Example:

```go
dir, err := os.Open("/path/to/directory")
if err != nil {
    return err
}
defer dir.Close()

if err := dir.Sync(); err != nil {
    return err
}

```

**Error Handling**:

- If `fsync` fails (e.g., due to a disk error), the database update should be considered unsuccessful.
- However, in some filesystems, **page caching** can create inconsistency:
    - You might read the new data (from the cache) even though `fsync` failed, giving a false sense of success.
- This behavior is filesystem-dependent, requiring careful error checking and additional measures for reliability.

### 1.5 Summary of Database Challenges*

From these lessons, we can summarize key challenges and their solutions when transitioning from simple file management to building a database:

1. **Problems with In-Place Updates**:
    - **Solution**: Avoid in-place updates by using safer techniques:
        - **Renaming files**: Replace old files atomically with new versions.
        - **Append-only logs**: Record updates incrementally to ensure old data is preserved.
2. **Append-Only Logs**:
    - **Advantages**:
        - Allows incremental updates.
        - Preserves old data during crashes.
    - **Limitations**:
        - Does not handle indexing or reclaim unused space effectively.
3. **`fsync` Usage**:
    - Essential for ensuring durability of both file data and directory mappings.
    - Requires careful handling of failures to maintain database consistency.

### Unanswered Questions

The foundational techniques discussed leave several open challenges that must be addressed in subsequent steps:

1. **Indexing Data Structures**:
    - How to enable fast lookups and efficient range queries in a database?
2. **Space Reclamation**:
    - How to reclaim disk space in append-only logs while ensuring consistency?
3. **Combining Logs with Indexing**:
    - How can logs and indexing work together to provide both durability and performance?
4. **Concurrency**:
    - How to manage concurrent readers and writers in a database system?

References:

- [1] [USENIX OSDI 2014 Slides on `fsync`](https://www.usenix.org/sites/default/files/conference/protected-files/osdi14_slides_pillai.pdf#page=31)
- [2] [USENIX ATC 2020 on Filesystem Consistency](https://www.usenix.org/conference/atc20/presentation/rebello)