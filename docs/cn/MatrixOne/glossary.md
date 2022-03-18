# **术语表** 

### **术语**

阅读以下对相关词汇的概念解释或许有助于你理解我们的整体架构。

|  术语   | 定义   |
|  ----  | ----  |
| A  |  |
| AST (抽象语法树)  | 抽象语法树是代码的树结构表示形式，是组成编译器工作模式的基本部分|
| C  |  |
| Cluster  | MatrixOne的分布式部署形式，由多台主机组成，在逻辑上构成一个整体。|
| D  |  |
 | Data Storage  | DataStorage接口实现了分布式存储服务，必须在使用MatrixCube之前就预先定义。并且，DataStorage的实现依赖于存储引擎的具体特性 |
| E  |  |
  | Event Notify | 将心跳(Hearbeat)信息同步到所有节点的机制称为Event Notify。|
  | F  |  |
  | Factorization | 因子化方法使用了基本的因式分解公式把代数式或二次方程简化为更简洁的形式。**MatrixOne在物理层使用紧凑的分解表示，以减少数据冗余并提高查询性能**  |
  | H  |  |
  | Heartbeat | 在MatrixOne集群中的每个节点都将周期性地发送自身的状态信息，而这些信息被称作heartbeat。 |
 | M  |  |
  | MatrixCube | MatrixCube是一个构建分布式系统的框架，保证了集群的可用性、一致性以及可扩展性。MatrixCube的设计目标是让开发人员只需要关注一个节点上的业务逻辑但却能够轻松实现各种强一致的分布式存储服务。| 
 | P  |  |
  | Prophet | Prophet是MatrixCube中的调度模块，执行Auto-Rebalance操作来维持集群中各个节点的存储量、读写负载均衡。集群中最初的三个节点将作为Prophet节点。|
   | Pure Storage | 与Prophet相对, pure storage是另一种类型的节点，并不执行调度工作，只进行普通的存储工作。|
| R  |  |
  | Replica | 为了保证存储服务的高可用性，每部分数据需要存储多份，并且分布在不同的节点上，因此将一个数据副本称之为一个Replica。同一部分的数据会包含多个Replica，并且每个Replica中的数据都是相同的。 |
  | S  |  |
  | Snapshot Isolation (SI) | Snapshot Isolation是一种在实践中广泛应用的多版本并发控制技术，MatrixOne支持Snapshot隔离级别的分布式事务。|
 | Store |MatrixCube分布式系统包括一定数量的主机，数据正是存放在这些机器中，而我们把集群中的每台主机称之为Store。|
  | Shard | 数据库中的数据按逻辑组织成表，而数据又可以按照不同的分区进行存储，如此可以得到更好的扩展性。因此，数据在MatrixCube集群中分片存储，每个数据分片我们称之为一个Shard，而一个Store中可以管理多个Shard；当Shard的存储容量超过限制时，会进行分裂(Split)。 |
 | Shard Splitting | 当一个Shard超过了规定的存储容量限制时，MatrixCube将会把该Shard分裂（Split）为两个存储量相当的Shard。 |
  | Shard Proxy | Shard Proxy是接受用户读写请求的中心模块，在收到请求后将其发送到相应的节点以做出回应|


### **重要概念**



|  Term   | Definition   |
|  ----  | ----  |
| A  |  |
| Auto-Rebalance  | 在分布式系统中，多个服务器的存储量、读写负载的自动平衡过程称之为Auto-Rebalance。|
| C  |  |
| Consistency  | MatrixOne支持强一致性，保证了在成功写入数据后，无论在哪个Store(节点)上都能读取到最新的数据。|
| E  |  |
| Execution Plan  |  数据库中的执行计划是查询优化器生成的查询操作的图形表示，可以得到执行该操作的最高效方法 |
| F  |  |
| Fault-Tolerance  | Fault-Tolerance（容错性）意味着系统在其中一个或多个组件发生故障后仍然可以继续运行的能力。|
| J  |  |
| JIT Compilation  |使用运行时的LLVM将SQL计划树或中间代码转化为本地程序。|
| M  |  |
| Monolitic Engine  | Monolitic Engine即超融合引擎，可支持TP、AP、时序、机器学习等混合工作负载。  |
| Materialized View  | A materialized view is a pre-computed data set derived from a query specification (the SELECT in the view definition) and stored for later use. Materialized view is usually used for increasing performance and efficiency. |
| Metadata  | Metadata即元数据，是用于描述数据库中数据的结构信息的数据。|
| P  |  |
| Paxos  | Paxos是一种一致性算法，保持一组异步网络通信的分布式计算机之间的一致性。 |
| R  |  |
| Raft  | Raft是一种易于理解的一致性协议算法，在容错性与性能上与Paxos相当，它们的不同之处在于，不同之处在于：The difference is that it's decomposed into relatively independent subproblems, and it cleanly addresses all major pieces needed for practical systems. |
| Raft Group and Leader | Raft在一组中定义了一个leader以及许多followers。一个组代表一个复制状态机，只有leader才可以响应客户端请求，然后将传达给followers。 |
| S  |  |
| SIMD instruction | SIMD是Single Instruction/Multiple Data的简写，即单指令多数据流，SIMD操作一般指一种使用一条指令即可处理多条数据的计算方法。 |
| T  |  |
| Transaction |  在数据库中执行的一系列满足ACID基本要求的操作。| 
| V  |  |
| Vectorized Execution  |Vectorized data processing helps with developing faster analytical query engines by making efficient utilization of CPU cache. Arrow's columnar format allows to use lightweight schemes like dictionary encoding, bit packing, and run length encoding, which favor query performance over compression ratio.|