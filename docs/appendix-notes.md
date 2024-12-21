# Appendix Notes


## Understanding I/O in Units of Pages

When working with database or file systems, data is typically read from and written to disk in **pages**, whcih are fixed-sized chuncks of data. This concept is crucial for understanding the efficiency of data structures like B-Trees, which are optimized for disk I/O.

### Key Concepts

#### 1. What is a Page?

- A **page** is the smallest unit of data that a database or file system reads from or writes to disk.
- Common page sizes: 4 KB, 8 KB, etc.
- When a database retrieves data, it fetches an entire page into memory, even if only a small portion of that page is needed.

#### 2. Why Pages ?

- **Disk I/O Cost**: Disk opereations are slow compared to memory operations are slow compared to memory operations. By reading or writing in fixed-size chunks (pages), the system reduces the number of I/O operations.
- **Aligment with Storage Devices**: Storage devices like HDDs and SSDs are optimized for reading and writing blocks of data, which correspond to page sizes.

#### 3. B-Trees and Pages

- **Node Size Matches Page Size**:
  - Each node in a B-tree is designed to fit within a single page.
  - When accessing a node, the database reads the entire page containing that node into memory in one I/O operation.
- **Minimizing I/O**:
  - By maximizing the number of keys stored in each node (based on the page size), the B-tree reduces its height.
  - Fewer levels mean fewer page reads to find a key or update a value.
