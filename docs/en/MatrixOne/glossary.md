# **Glossary**

### **Terms**

It's helpful to understand a few terms before reading our architecture documentation.

|  Term   | Definition   |
|  ----  | ----  |
| A  |  |
| AST (Abstract syntax tree)  | Abstract Syntax Trees or ASTs are tree representations of code. They are a fundamental part of the way a compiler works. |
| C  |  |
| Cluster  | A distributed MatrixOne deployment, which acts as a single logical application.|
| D  |  |
 | Data Storage  | A DataStorage is an interface for implementing distributed storage service. <!--It must be defined in prior to using MatrixCube.--> DataStorage needs to be implemented based on the characteristics of storage engine.|
| E  |  |
  | Event Notify | The machanism of synchronizing heartbeat information to all nodes is called an event notify. |
  | Explicit Transactions| Explicit Transaction has the beginning, ending and rollback of transactions with the command Begin Transaction, Commit Transaction and Rollback Transaction. |
  | F  |  |
  | Factorization | The factorization method uses basic factorization formula to reduce any algebraic or quadratic equation into its simpler form. MatrixOne uses compact factorized representations at the physical layer to reduce data redundancy and boost query performance. |
  | H  |  |
  | Heartbeat | Every node in a MatrixOne cluster will periodically sends its status information, this information is called a heartbeat. |
  |I|  |
  | Implicit Transactions| Implicit Transaction is the auto commit. There is no beginning or ending of the transaction. |
 | M  |  |
 | P  |  |
  | Prophet | Prophet is a scheduling module<!-- in MatrixCube-->. It takes charge of Auto-Rebalance, which keeps the system storage level and read/write throughput level balanced across Stores. The initial 3 Stores of a MatrixCube cluster are all Prophet Nodes. |
   | Pure Storage | In contrast to Prophet, pure storage is another type of node, which doesn't handle any scheduling job and works as simple storage. |
| R  |  |
  | Replica | To provide reliable service, each shard is stored not only once, it will have several copy stored in different stores. Every copy of a shard is called a Replica. |
  | S  |  |
  | Snapshot Isolation (SI) | Snapshot Isolation is a multi-version concurrency control approach that is widely used in practice. MatrixOne supports distributed transaction of snapshot isolation level. |
 | Store | A <!--MatrixCube-->distributed system consists of several physical servers, our data are stored across these physical server. We call each server inside this cluster a Store. |
  | Shard | In MatrixOne, the data are split into different partitions to store in order to get better scalability. Each partition is called a Shard. In our design, a new created table is initially a Shard. When the size of the table exceeds the Shard size limit, the Shard will split. |
 | Shard Splitting | There is a certain limit to a Shard size. Whenever a Shard exceeds its storage limit,<!--MatrixCube-->Distributed system splits a Shard into two Shards and keep each Shard with the same storage level. |
  | Shard Proxy | The Shard Proxy is a central module to accept all user read/write requests and route requests to corresponding nodes.|

### **Concepts**

MatrixOne relies heavily on the following concepts. Being familiar with them will help you understand what our architecture achieves.

|  Term   | Definition   |
|  ----  | ----  |
| A  |  |
| Auto-Rebalance  | A modern distributed database should do more than just split data amongst a number of servers. The automatic process of storage and workload distribution among servers is called an Auto-Rebalance. |
| C  |  |
| Consistency  | MatrixOne supports a strong consistency. It is guaranted that after any successful data write, the reading afterwards will get the latest value, no matter from which store. |
| E  |  |
| Execution Plan  | An execution plan in a database is a simple graphical representation of the operations that the query optimizer generates to calculate the most efficient way to return a set of results.  |
| F  |  |
| Fault-Tolerance  | Fault-Tolerance simply means a system's ability to continue operating uninterrupted despite the failure of one or more of its components.  |
| J  |  |
| JIT Compilation  | Turns SQL plan tree or Intermediate Representation code into a native program using LLVM at runtime.  |
| M  |  |
| Monolithic Engine  | A monolithic database engine is designed to support hybrid workloads: transactional, analytical, streaming, time-series, machine learning, etc.  |
| Materialized View  | A materialized view is a pre-computed data set derived from a query specification (the SELECT in the view definition) and stored for later use. Materialized view is usually used for increasing performance and efficiency. |
| Metadata  | Metadata is the data that describes the structure and creation method of data in a database. |
| P  |  |
| Paxos  | Paxos is an algorithm that is used to achieve consensus among a distributed set of computers that communicate via an asynchronous network. |
| R  |  |
| Raft  | Raft is a consensus algorithm that is designed to be easy to understand. It's equivalent to Paxos in fault-tolerance and performance. The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems. |
| Raft Group and Leader | Raft defines a strong, single leader and number of followers in a group of peers. The group represents a replicated state machine. Only the leader may service client requests. The leader replicates actions to the followers. |
 | S  |  |
  | SIMD instruction | SIMD is short for Single Instruction/Multiple Data, while the term SIMD operations refers to a computing method that enables processing of multiple data with a single instruction. |
| T  |  |
| Transaction | A set of operations performed on your database that satisfy the requirements of ACID semantics. |
| V  |  |
| Vectorized Execution  | Vectorized data processing helps with developing faster analytical query engines by making efficient utilization of CPU cache. Arrow's columnar format allows to use lightweight schemes like dictionary encoding, bit packing, and run length encoding, which favor query performance over compression ratio. |
