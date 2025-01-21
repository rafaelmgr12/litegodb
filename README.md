# LiteGoDB

LiteGoDB is a lightweight key-value store implemented in Go. Inspired by the book "Building a Database from Scratch in Go" by James Smith, the project aims to provide a practical, from-scratch implementation of a key-value database featuring B-Trees and LSM Trees, coupled with concepts like Write-Ahead Logging (WAL) and disk-based storage management.

## Table of Contents
- [LiteGoDB](#litegodb)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Features](#features)
  - [Installation](#installation)
  - [Usage](#usage)
  - [Testing](#testing)
  - [Notes](#notes)
    - [Appendix Notes](#appendix-notes)
      - [Key Concepts](#key-concepts)
  - [Contributing](#contributing)
  - [License](#license)

## Introduction
LiteGoDB is designed to demonstrate the fundamentals of database architecture, focusing on the implementation of B-Trees and LSM Trees. It includes disk persistence, write-ahead logging, and the ability to recover from crashes. The project is ideal for learning purposes and is built with simplicity and clarity in mind.

## Features
- **B-Tree-based key-value store**: Efficient data insertion, search, and deletion operations.
- **Write-Ahead Logs (WAL)**: Ensures durability and consistency.
- **Disk-based storage management**: Manages pages on disk, supports periodic flushing for persistence.
- **Log-Structured Merge Trees (LSM Trees)**: Provides efficient writes and reads via an in-memory tree merged with disk-based storage.
- **Crash Recovery**: Recovers from crashes using WAL.

## Installation
To install LiteGoDB, you need to have Go installed on your machine. Clone the project repository and build the project using the following steps:

```sh
git clone https://github.com/rafaelmgr12/litegodb.git
cd litegodb
go build
```

## Usage
Here is a simple example of how to use LiteGoDB:

1. **Initialize the key-value store**

```go
package main

import (
    "log"
    "time"
    "github.com/rafaelmgr12/litegodb/internal/storage/disk"
    "github.com/rafaelmgr12/litegodb/internal/storage/kvstore"
)

func main() {
    // Create a new DiskManager
    diskManager, err := disk.NewFileDiskManager("data.db")
    if err != nil {
        log.Fatalf("Failed to create disk manager: %v", err)
    }
    defer diskManager.Close()

    // Initialize the key-value store with a degree of 2 for the B-Tree
    kvStore, err := kvstore.NewBTreeKVStore(2, diskManager, "log.db")
    if err != nil {
        log.Fatalf("Failed to create the key-value store: %v", err)
    }
    defer kvStore.Close()

    // Start periodic flushing to disk every 10 seconds
    kvStore.StartPeriodicFlush(10 * time.Second)
    
    // Put some key-value pairs
    if err := kvStore.Put(1, "value1"); err != nil {
        log.Fatalf("Failed to put key-value pair: %v", err)
    }
    if err := kvStore.Put(2, "value2"); err != nil {
        log.Fatalf("Failed to put key-value pair: %v", err)
    }

    // Get key-value pairs
    value, found, err := kvStore.Get(1)
    if err != nil {
        log.Fatalf("Failed to get key-value pair: %v", err)
    }
    if found {
        log.Printf("Key 1: %s\n", value)
    } else {
        log.Println("Key 1 not found")
    }

    // Delete a key-value pair
    if err := kvStore.Delete(2); err != nil {
        log.Fatalf("Failed to delete key-value pair: %v", err)
    }
}
```

## Testing
LiteGoDB includes several tests to ensure correctness. To run the tests, use:

```sh
go test ./...
```

This will run all integration and unit tests to verify that different components of LiteGoDB work as expected.

## Notes

### Appendix Notes
When working with databases or file systems, data is read from and written to disk in **pages**, which are fixed-sized chunks of data. This concept is crucial for understanding data structures like B-Trees or LSM Trees, which are optimized for disk I/O.

#### Key Concepts
1. **What is a Page?**
   - A **page** is the smallest unit of data that a database or file system reads from or writes to disk.
   - Common page sizes include 4 KB and 8 KB.

2. **Why Pages?**
   - **Disk I/O Cost**: Disk operations are slow compared to memory operations. By reading or writing in fixed-size chunks (pages), the system reduces the number of I/O operations.
   - **Alignment with Storage Devices**: Storage devices like HDDs and SSDs are optimized for reading and writing blocks of data, which correspond to page sizes.

3. **B-Trees and Pages**
   - **Node Size Matches Page Size**: Each node in a B-Tree fits within a single page. When accessing a node, the database reads the entire page containing that node into memory in one I/O operation.
   - **Minimizing I/O**: By maximizing the number of keys stored in each node (based on the page size), the B-Tree reduces its height.

## Contributing
Contributions are welcome! If you find a bug or want to add a new feature, feel free to fork the repository, make your changes, and open a pull request.

## License
This project is licensed under the MIT License. See the LICENSE file for details.

---