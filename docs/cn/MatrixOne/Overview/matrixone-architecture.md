# **MatrixOne架构概述**

## **整体架构**

MatrixOne是一个从0开始打造的全新数据库，其整体架构如下图所示：
![MatrixOne Architecture](https://github.com/matrixorigin/artwork/blob/main/docs/overview/overall-architecture.png?raw=true)



## **查询解析层(Query Parser Layer)**
* **解析器(Parser):** 将SQL、流处理或Python语言解析为抽象语法树，以便进一步处理。
* **计划器(Planner):** 通过一系列基于规则、基于成本的优化算法找到最佳的执行计划，并将抽象语法树转换为计划树。
* **IR生成器(IR Generator):** 将Python代码转变为LLVM IR的中间码。

## **计算层(Computation Layer)**
* **即时编译(JIT Compilation):** 在运行时使用LLVM将SQL计划树或IR代码转换为本地程序。
* **向量化执行(Vectorized Execution):** MatrixOne利用SIMD指令构造向量化执行通道。
* **缓存(Cache):** 用于查询的数据、索引和元数据的多版本的缓存。  

## **集群管理层(MatrixCube)**
MatrixCube是构建分布式系统的基础库，它保证数据库集群的了可靠性、强一致性和可扩展性。它完成了分布式的、有状态应用程序的构建，允许开发人员只需要关注单个节点上的业务逻辑。MatrixCube目前搭建在`multi-raft`之上并实现复制状态机，之后将迁移到`Paxos`，便于适应跨多个数据中心的应用场景。


* **调度器(Prophet):** MatrixCube中用于管理、调度MatrixOne集群的工具。
* **事务管理器(Transaction Manager):** MatrixOne支持快照隔离级别(snapshot isolation)的分布式事务。
* **复制状态机(Replicated State Machine):** MatrixOne使用基于`raft`的共识协议和超逻辑时钟来实现集群的强一致性。

## **存储引擎层(Replicated Storage Layer)**

* **行存(Row Storage):** 存储交易类负载数据、元数据和Catalog
* **列存(Column Storage):** 存储分析负载类数据与物化视图

## **Storage Provision Layer**

MatrixOne是“对基础设施无感知”的数据库，可以支持共享存储S3/HDFS，或者本地磁盘、私有数据中心、混合云，甚至是任何智能设备中。

## **相关信息**
本节介绍了MatrixOne的大致架构，若您想了解更详细的技术问题，可阅读：
[MatrixOne技术架构详解](matrxione-modules.zh.md)  

其他信息可参见：  

* [安装MatrixOne](../Get-Started/install-standalone-matrixone.zh.md)
* [MySQL兼容性](mysql-compatibility.zh.md)
* [最新发布信息](what's-new.zh.md)