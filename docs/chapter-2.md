# Chapter 2

<!-- Here will be the introduction fo this chapter -->

## 2.1 Types of queries

A database query is a request to retrieve or manipulate data within a database. Understanding the types of queries is essential for optimizing query performance and designing efficient query interfaces.

### Types of Queries:

1. **Scan the Whole Dataset**: The entire dataset is scanned to retrieve the required data. This kind of query is O(n) in terms of time complexity, where n is the number of records in the dataset. 
    - Example: `SELECT * FROM table_name;`
    - Time Complexity: O(n)
    - **Optimization**: *Column-based storage* can be used to optimize this kind of query.
2. **Point Query**: Retrives data using a specifig key index. This kind of query is O(log n) in terms of time complexity.
    - Example: `SELECT * FROM table_name WHERE id = 123;`
    - Time Complexity: O(log n)
    - Only involves the Seek phase (finding the key).
3. **Range Query**: Retrieves data for a range of values in the index. This query type benefits from a sorted index to allow efficient traversal.
    - **Example**: `SELECT * FROM table_name WHERE id BETWEEN 100 AND 200;`
    - **Time Complexity**: **O(log N)** for the seek phase, followed by traversal.
    - **Phases**:
        1. **Seek**: Find the starting key in the sorted index.
        2. **Iterate**: Traverse the index to find the next or previous keys in sorted order.

## 2.2 Hashtables

Hashtables are suitable for handling **point queries** like `get`, `set`, and `delete` efficiently but lack the ability to handle **range queries** because they do not maintain key ordering.

### Key Characteristics of Hashtables

- **Point Query Efficiency**: Provides `O(1)` average time complexity for operations like `get` and `set`.
- **No Ordering**: Unsuitable for sorted data queries, as keys are hashed into buckets with no inherent order.
- **Growing the Hashtable**: When the load factor becomes high, keys must be rehashed into a larger hashtable. Rehashing all keys at once (e.g., during resizing) is expensive (`O(n)`), so progressive rehashing is often used in practice.

### Value as an Exercise

Even though hashtables are not directly applicable to database indexing (due to the lack of ordering), implementing one is still a valuable exercise:

- Provides foundational knowledge about managing in-memory data structures.
- Highlights challenges such as in-place updates and space management.

The focus in the next sections will shift toward ordered data structures, which are essential for efficient range queries and space reuse.

## 2.3 Sorted Arrays

Sorted arrays are one of the simplest data structures for maintaining ordered data, making them useful for efficient point queries and range queries.

### Key Characteristics

1. **Binary Search for Queries**:
    - Binary search enables `O(log N)` lookups for specific keys.
2. **Update Cost**:
    - Inserting or deleting elements in a sorted array requires `O(N)` time due to the need to shift elements to maintain order.
3. **Handling Variable-Length Data**:
    - For variable-length keys (e.g., strings), an array of pointers can be used for efficient binary searches.

### Limitations

- **Impractical Updates**:
  - The high cost of updates (`O(N)`) makes sorted arrays unsuitable for frequent modifications.
- **Memory Fragmentation**:
  - Using pointers can lead to inefficient memory usage when handling large datasets.

### Extensions to Sorted Arrays

To reduce update costs, sorted arrays can be divided into multiple smaller, non-overlapping arrays:

1. **Nested Sorted Arrays**:
    - Splitting a large sorted array into smaller arrays reduces the update cost but introduces the challenge of maintaining these smaller arrays.
    - This extension serves as a foundation for data structures like **B+ Trees**.
2. **Log-Structured Merge Trees (LSM Trees)**:
    - Updates are first buffered in a smaller sorted array and merged into the main sorted array when full.
    - This approach amortizes update costs by gradually propagating changes.

Sorted arrays, while simple and efficient for read-heavy scenarios, are rarely used alone for database indexing due to their limitations with updates and space reuse. The next sections explore advanced data structures like B+ Trees that overcome these drawbacks.

## 2.4 B-Trees

A B-tree is a balanced n-ary-tree, comparable to balanced binary trees. Each nodes stores varible number of keys (and branches) up to $n$ and $n>2$

B-Trees are balanced n-ary trees commonly used for database indexing because of their efficiency in reducing random disk access.

### Key Characteristics

1. **Structure**:
    - Each node can store a variblae number of keys and branaches, up to maximum n, where 