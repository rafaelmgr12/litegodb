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

Hashtables are viables if you 
