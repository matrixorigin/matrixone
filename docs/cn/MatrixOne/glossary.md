# **术语表**

### **术语**

阅读以下对相关词汇的概念解释或许有助于你理解我们的整体架构。

|  术语   | 定义   |
|  ----  | ----  |
| A  |  |
| AST  | AST即抽象语法树，是代码的树结构表示形式，是组成编译器工作模式的基本部分|
| C  |  |
| Cluster  | MatrixOne的分布式部署形式，由多台主机组成，在逻辑上构成一个整体。|
| E  |  |
  | Explicit Transactions| 显式事务，即是一种指定的事务，这种事务需要由你自己决定哪批工作必须成功完成，否则所有部分都不完成。可以使用 `BEGIN TRANSACTION` 和 `ROLLBACK TRANSACTION` 或 `COMMIT TRANSACTION` 关键字进行控制。|
  |I|  |
  | Implicit transactions| 隐式事务，即自动提交事务。 |
  | S  |  |
  | Snapshot Isolation (SI) | Snapshot Isolation是一种在实践中广泛应用的多版本并发控制技术，MatrixOne支持Snapshot隔离级别的分布式事务。|

### **重要概念**

|  概念   |定义   |
|  ----  | ----  |
| A  |  |
| Auto-Rebalance  | 在分布式系统中，多个服务器的存储量、读写负载的自动平衡过程称之为Auto-Rebalance。|
| C  |  |
| Consistency  | MatrixOne支持强一致性，保证了在成功写入数据后，无论在哪个Store(节点)上都能读取到最新的数据。|
| E  |  |
| Execution Plan  |  数据库中的执行计划是查询优化器生成的查询操作的图形表示，可以得到执行该操作的最高效方法 |
| F  |  |
| Fault-Tolerance  | Fault-Tolerance（容错性）意味着系统在其中一个或多个组件发生故障后仍然可以继续运行的能力。|
| M  |  |
| Monolitic Engine  | Monolitic Engine即超融合引擎，可支持TP、AP、时序、机器学习等混合工作负载。  |
| Materialized View  |Materialized View即物化视图，是预先被计算好的数据集，存储下来以便后续使用，通常可以提升查询的运行效率。 |
| Metadata  | Metadata即元数据，是用于描述数据库中数据的结构信息的数据。|
| P  |  |
| Paxos  | Paxos是一种一致性算法，保持一组异步网络通信的分布式计算机之间的一致性。 |
| R  |  |
| Raft  | Raft是一种易于理解的一致性协议算法，在容错性与性能上与Paxos相当。 |
| Raft Group and Leader | Raft在一组中定义了一个leader以及许多followers。一个组代表一个复制状态机，只有leader才可以响应客户端请求，然后将传达给followers。 |
| S  |  |
| SIMD instruction | SIMD是Single Instruction/Multiple Data的简写，即单指令多数据流，SIMD操作一般指一种使用一条指令即可处理多条数据的计算方法。 |
| T  |  |
| Transaction |  在数据库中执行的一系列满足ACID基本要求的操作。|
| V  |  |
| Vectorized Execution  |通过有效利用CPU的缓存，向量化执行提高了分析查询引擎的速度。Arrow 的列式格式可以使用轻量级的架构，如dictionary encoding，bit packing以及run length encoding,这都进一步了提升了查询效率.|
