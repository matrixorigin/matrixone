- Feature Name: Analytic Optimized Engine Overall Design
- Status: In Progress
- Start Date: 2021-05-10
- Authors: [Xu Peng](https://github.com/XuPeng-SH)
- Implementation PR:
- Issue for this RFC:

# Summary

**AOE** (Analytic Optimized Engine) is designed for analytical query workloads, which can be used as the underlying storage engine of database management system (DBMS) for online analytical processing of queries (OLAP).
- Cloud native
- Efficient processing small and big inserts
- State-of-art data layout for vectorized execution engines
- Fine-grained, efficient and convenient memory management
- Conveniently support new index types and provide dedicated index cache management
- Serious data management for concurent queries and mutations
- Avoid external dependencies

# Guilde-level design

## Terms
### Layout
- **Block**: Piece of a segment which is the minimum part of table data. The maximum number of rows of a block is fixed.
- **Segment**: Piece of a table which is composed of blocks. The maximum number of blocks of a segment is fixed.
- **Table**: Piece of a database which is composed of segments
- **Database**: A combination of tables, which shares the same log space

### State
- **Transient Block**: Block where the number of rows does not reach the upper limit and the blocks queued to be sorted and flushed
- **Persisted Block**: Sorted block
- **Unclosed Segment**: Segment that not merge sorted
- **Closed Segment**: Segment that merge sorted

### Container
- **Vector**: Data fragment of a column in memory
- **Batch**: A combination of vectors, and the number of rows in each vector is aligned

### Misc
- **Log space**: A raft group can be considered as a log space

## Data storage
### Table
**AOE** stores data represented as tables. Each table is bound to a schema consisting of numbers of column definitions. A table data is organized as a log-structured merge-tree (LSM tree).

Currently, **AOE** is a three-level LSM tree, called L0, L1 and L2. L0 is small and can be entirely resident in memory, whereas L1 and L2 are both definitely resident on disk. In **AOE**, L0 consists of transient blocks and L1 consists of sorted blocks. The incoming new data is always inserted into the latest transient block. If the insertion causes the block to exceed the maximum row count of a block, the block will be sorted by primary key and flushed into L1 as sorted block. If the number of sorted blocks exceed the maximum number of a segment, the segment will be sorted by primary key using merge sort.

L1 and L2 are organized into sorted runs of data. Each run contains data sorted by the primary key, which can be represented on disk as a single file. There will be overlapping primary key ranges between sort runs. The difference of L1 and L2 is that a run in L1 is a **block** while a run in L2 is a **segment**.

As described above, transient blocks can be entirely resident in memory, but not necessarily so. Because there will be many tables, each table has transient blocks. If they are always resident in memory, it will cause a huge waste. In **AOE**, transient blocks from all tables share a dedicated fixed-size LRU cache. A evicted transient block will be unloaded from memory and flushed as a transient block file. In practice, the transient blocks are constantly flowing to the L1 and the number of transient blocks per table at a certain time is very small, those active transient blocks will likly reside in memory even with a small-sized cache.

### Indexes
There's no table-level index in **AOE**, only segment and block-level indexes are available. **AOE** supports dynamic index creation and deletion, index creation is an asynchronous process. Indexes will only be created on blocks and segments at L1 and L2, that is, transient blocks don't have any index.

In **AOE**, there is a dedicated fixed-size LRU cache for all indexes. Compared with the original data, the index occupies a limited space, but the acceleration of the query is very obvious, and the index will be called very frequently. A dedicated cache can avoid a memory copy when being called.

Currently, **AOE** uses two index types:
- **Zonemap**: It is automatically created for all columns. Persisted.
- **BSI**: It should be explicitly defined for a column. Persisted.

It is very easy to add a new index to **AOE**.

### Compression
**AOE** is a column-oriented data store, very friendly to data compression. It supports per-column compression codecs and now only **LZ4** is used. You can easily obtain the meta information of compressed blocks. In **AOE**, the compression unit is a column of a block.

### Layout
#### Block
   ![image](https://user-images.githubusercontent.com/39627130/145402878-72f9aa0a-65f5-494a-96ff-c075065c1f01.png)

#### Segment
   ![image](https://user-images.githubusercontent.com/39627130/145402537-6500bcf4-5897-4dfa-b3fc-196d0c5835df.png)

## Buffer manager
Buffer manager is responsible for the allocation of buffer space. It handles all requests for data pages and temporary blocks of the **AOE**.
1. Each page is bound to a buffer node with a unique node ID
2. A buffer node has two states:
   1) Loaded
   2) Unloaded
3. When a requestor **Pin** a node:
   1) If the node is in **Loaded** state, it will increase the node reference count by 1 and wrap a node handle with the page address in memory
   2) If the node is in **Unloaded** state, it will read the page from disk|remote first, increase the node reference count by 1 and wrap a node handle with the page address in memory. When there is no left room in the buffer, some victim node will be unloaded to make room. The current replacement strategy is **LRU**
4. When a requestor **Unpin** a node, just call **Close** of the node handle. It will decrease the node reference count by 1. If the reference count is 0, the node will be a candidate for eviction. Node with reference count greater than 0 never be evicted.

There are currently three buffer managers for different purposes in **AOE**
1. Mutation buffer manager: A dedicated fixed-size buffer used by L0 transient blocks. Each block corresponds to a node in the buffer
2. SST buffer manager: A dedicated fixed-size buffer used by L1 and L2 blocks. Each column within a block corresponds to a node in the buffer
3. Index buffer manager: A dedicated fixed-size buffer used by indexes. Each block or a segment index corresponds to a node in the buffer

## WAL
**Write-ahead logging** (WAL) is the key for providing **atomicity** and **durability**. All modifications should be written to a log before applied. **AOE** depends a abstract **WAL** layer on top of a more concrete **WAL** backend. There are two **WAL** roles in **AOE**:
1. HolderRole: The backend **WAL** is used as a **AOE** embedded **WAL**
2. BrokerRole: The incoming modification requests were already written to external log and now applied to **AOE**. The backend **WAL** is used to share with external **WAL**

### Share with external WAL
When a storage engine is used as a state machine of a raft group, **WAL** in the storage engine is unnecessary and would only add overhead. **AOE** is currently used as the underlying state machine of **MatrixOne**, which uses **Raft** consensus for replication. To share with external **Raft** log, **AOE** can use a default **WAL** backend of role **BrokerRole**

## Catalog
**Catalog** is **AOE**'s in-memory metadata manager that manages all states of the engine, and the underlying driver is an embedded **LogStore**. **Catalog** implements a simple memory transaction database, retains a complete version chain in memory, and is compacted when it is not referenced. **Catalog** can be fully replayed from the underlying **LogStore**.
1. DDL operation infos
2. Table Schema infos
3. Layout infos

### Example
![image](https://user-images.githubusercontent.com/39627130/139570327-f484858c-347c-4100-b0cc-afd03c5e6e8d.png)
- There are 5 tables (id: 1,3,5,6,7) with the same table name "m" and only table 7 is active now.
- Table 1 is created @ timestamp 1, soft-deleted @ timestamp 2, hard-deleted @ timestamp 9
- Table 3 is created @ timestamp 3, soft-deleted @ timestamp 4, hard-deleted @ timestamp 5
- Table 5 is created @ timestamp 6, soft-deleted @ timestamp 7, hard-deleted @ timestamp 8
- Table 6 is created @ timestamp 10, **replaced** @ timestamp 11, hard-deleted @ timestamp 12
- Table 7 is created @ timestamp 11

## LogStore
An embedded log-structured data store. It is used as the underlying driver of **Catalog** and **WAL**.

## Multi-Version Concurrency Control (MVCC)
For any update, **AOE** create a new version of data object instead of in-place update. The concurrent read operations started at an older timestamp could still see the old version.

### Example
![image](https://user-images.githubusercontent.com/39627130/145431744-e3f52d23-7ae0-4356-801e-29807e9fc325.png)

## Database (Column Families)
In **AOE**, a **Table** is a **Column Family** while a **Database** is **Column Families**. The main idea behind **Column Families** is that they share the write-ahead log (Share **Log Space**), so that we can implement **Database-level** atomic writes. The old **WAL** cannot be compacted when the mutable buffer of a **Table** flushed since it may contains live data from other **Tables**. It can only be compacted when all related **Tables** mutable buffer are flushed.

**AOE** supports multiple **Databases**, that is, one **AOE** instance can work with multiple **Log Spaces**. Our **MatrixOne** DBMS is built upon multi-raft and each node only needs one **AOE** engine, and each raft group corresponds to a **Database**. It is complicated and what makes it more complicated is the engine shares the external **WAL** with **Raft** log.

## Snapshot
**AOE** can create a snapshot of **Database** at a certain time or LSN. As described in **MVCC**, a snapshoter can fetch a database snapshot and dump all related data and metadata to a specified path. **AOE** also can install snapshot files to restore to the same state as when the snapshot was created.

## Split
**AOE** can split a **Database** into the specified number of **Databases**. Currently, the segment is not splitable. Splitting corresponds to the data layer, just a reorganization of segments.

## GC
1. Metadata compaction
   1) In-memory version chain compaction
   2) In-memory hard deleted metadata entry compaction
   3) Persisted data compaction
2. Stale data deletion
   1) Table data with reference count equal 0
   2) Log compaction

# Feature works
1. More index types
2. Per-column compress codec
3. More LSM tree levels
4. Integrate some scan and filter operators
5. Support deletion
