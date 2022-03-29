# **Auto-Rebalance & Scheduling**

MatrixCube是一个实现分布式系统的框架。对于分布式系统来说，数据存储在多台机器上，而当集群中的计算机的数量发生变化时，例如集群扩展或计算机崩溃，都需要跨主机进行数据转移。  

`Prophet`是MatrixCube的自平衡和调度的关键组件。`Prophet`中嵌入了`Etcd Server`组件来存储集群的元数据。
其主要实现了三个目标：  
* 保持每个`Store`存储量平衡。
* 保持读写请求的负载平衡。
* 保持逻辑表分布平衡。 

为此，我们设计了`Heartbeat`和`Event Notify`机制来实现以上目标。每个`Store`和`Leader Replica`将向`Prophet`发送`heartbeat`信息，`Prophet`将根据该信息做出调度决策。因此，我们首先需要配置某些`Store`来承担`Prophet`的职责。

## **Store Hearbeat**

每个`Store`周期性地向`Prophet`发送`Heartbeat`，其中包括以下信息：

* 当前该`Store`存储的`Replica`的数量。
* 当前该`Store`的存储空间，包括已使用空间与剩余空间。

`Prophet`收集所有的`Heartbeat`，然后可以得到全局的`Replica`视图以及每个节点的存储空间信息。根据这些信息，`Prophet`将发送调度命令，将部分`Replica`转移到合适的`Store`上，以便保证每个`Store`上的`Replica`数目相近。由于每份`Replica`的数据量相同，因此`Shard`之间也能达到存储量的平衡。

## **Replica Hearbeat**
分布式体系下每一个`Shard`在各个`Store`上有相应的`Replicas`（副本），这些`Replica`形成了一个`Raft-Group`，并且将在组内选举Leader。Leader周期性地向`Prophet`发送`Heartbeat`，将包括一下信息：

* 当前`Shard`所拥有的`Replica`数量，以及每个`Replica`的最新活跃时间。
* 当前`Leader Replica`所处的`Store`。 

`Prophet`收集所有`Hearbeat`然后构建一个全局的`Shard Replica`与`Replica Leader`视图。根据这些信息，`Prophet`会发出调度命令，如下：

* `Add Replica`命令：如果`Shard Replica`的数目不足，将寻找合适的`Stores`并向其中添加`Replica`。
* `Remove Replica`命令：如果`Shard Replica`的数目超过限制，则在合适的`Stores`上删除`Replica`。
* `Move Replica`命令：如果`Shard Replica`数目分布不均匀，那么某些`Replica`将被转移到相映`Store`上来保持平衡。
* `Transfer Leader`命令: 如果`Leader`在集群中数目不平衡，那么某些`Leader`将被转移。

## **Event Notify**

收集到的`Heartbeat`信息将被同步到所有`Store`，从而每个`Store`中都能在本地形成全局路由信息。

