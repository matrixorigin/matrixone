# **MatrixCube简介**


`MatrixCube`是一个基于`Multi-Raft`实现的自带调度能力的分布式存储框架，实现了集群的高可用性、强一致性与可扩展性。`MatrixCube`的设计目标是让开发人员只需要关注一个节点上的业务逻辑但却能够轻松实现各种强一致的分布式存储服务。我们可以使用`MatrixCube`来构建一些常见的分布式存储服务，比如：`分布式Redis`、`分布式KV`等等。

## **MatrixCube框架**

![MatrixCube](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-architecture.svg?raw=true) 

## **核心概念**

我们首先需要阐明一些MatrixCube的核心概念。

### **Store**
MatrixCube分布式系统包括一定数量的主机，数据正是存放在这些机器中——我们把集群中的每台计算机称之为`Store`。

### **Shard**
数据库中的数据是按逻辑组织成表，而数据又可以分成不同的分区进行存储，如此可以得到更好的扩展性。因此，数据在`Matrixcube`集群中分片存储，每个数据分片我们称之为一个`Shard`，而一个`Store`中可以管理多个`Shard`；当`Shard`的存储容量超过限制时，会进行分裂(Split)。

### **Replica**

为了保证存储服务的高可用性，每个`Shard`的数据需要存储多份，并且分布在不同的`Store`上，`Shard`的一个数据副本我们称之为一个`Replica`。所以一个`Shard`会包含多个`Replica`，并且每个`Replica`中的数据都是相同的。


### **Raft-group与Leader**

多个数据副本存储在不同的`Store`上，一旦其中的一个副本更新，其他的副本也必须与之保持一致。无论客户端向哪个副本发出查询请求，都将得到相同的结果。为了保证数据的一致性，我们采用`Raft`协议来实现数据共识，一个`Shard`的多个`Replica`会组成一个`Raft-Group`。   
在每个`Raft-group`中，将会选出一个`Leader`来代表整个组，所有的读写请求仅仅由这个`Leader`处理。  

更多相关信息：[Raft中如何选举出一个Leader?](https://raft.github.io/)

### **Data Storage**

`DataStorage`是实现分布式存储服务的接口，使用MatrixCube就必须要预先定义`DataStorage`，用来存储单机数据。基于`DataStorage`可以轻松构建一些常见的分布式存储服务。MatrixCube默认提供了一个完整的基于`KV`的DataStorage，因为基于`KV`的存储可以满足大部分的场景。


### **Prophet**

`Prophet`是一个调度模块，主要职责是实现`Auto-Rebalance`，从而维持每个`Shard`的`Replica`数目均衡（本质上是为了保持`Store`之间的存储量与吞吐量均衡）。集群中初始的三个`Store`便是全部的`Prophet Nodes`。  

更多相关信息：[Prophet如何实现调度功能？](matrixcube-auto-rebalance-scheduling.md)

### **Raftstore**

`Raftstore` 是MatrixCube的核心组件，实现了MatrixCube中大多数重要功能：

* Metadata storage：存储`Store`，`Shard`，`Raft-log`的元数据信息。

* Multi-Raft management：管理了`Store`，`Shard`，`Replica`，`Raft-Group`之间的相关关系，实现多个`Raft-Group`，Leader选举和再选举。

* Global Routing：`Prophet`的`Event Notify`机制构建了一个全局路由表；读写路由将也基于此路由表。

* Shard Proxy：`Shard Proxy`用来代理`Shard`的读写请求。为了对`MatrixCube`的使用者屏蔽`Multi-Raft`的实现细节，`MatrixCube`集群中的任意节点对于发起读写请求的客户端都是等价的，用户可以在任意节点发起读写操作，这些读写请求会被`Shard Proxy`代理，并根据全局路由将请求转发到正确的节点。

更多相关信息：[Shard Proxy与Global Routing的工作机理](matrixcube-proxy-routing.md)






## **主要特性**

### *强一致性** 


MatrixCube实现了强大的一致性，它保证在任何数据成功写入之后，之后的读取将获得最新的值，而无论来自哪个节点。

### **容错性**

MatrixCube实现的分布式存储服务具有容错性、高可用性。若一个`Shard`拥有`2*N+1`个`Replicas`，那么只要失效的`Replicas`不超过`N`个，系统都将正常运转。  
例如，有3个`Stores`的集群可以在1个`Store`下线时继续运转；有5个`Stores`的集群可以在2个 `Stores`下线的情况运转。

### **Shard Splitting**

`Shard`容量大小有一定限制。每当`Shard`超过其存储容量限制时，MatrixCube将`Shard`分裂为两个，并保持分裂后的每个`Shard`具有相同的存储量。  
你可以在此处了解有关此过程的详细叙述：[Shard Splitting的工作机制](matrixcube-shard-splitting.md)。

### **Auto-Rebalance**


分布式系统应该充分利用所有节点的计算能力和存储能力。对于MatrixCube集群，当增加或减少`Stores`时，将发生`Auto-Rebalance`，它将在`Store`之间转移数据，以达到每个`Store`的负载平衡。


详情请见[Auto-Rebalance的工作机制]：(matrixcube-auto-rebalance-scheduling.md)。

### **Scale-out**

通过`shard splitting`与` auto-rebalance`，MatrixCube分布式系统能够横向扩展，集群存储和吞吐量能力与`Stores`的数量成比例。

### **用户自定义存储引擎**

MatrixCube对集群中每个数据存储引擎没有限制。换言之，任何存储引擎只要实现了MatrixCube定义的`DataStorage`接口，都可以搭建起基于MatrixCube的分布式系统。默认情况下，MatrixCube提供了一个基于`Pebble`的Key-Value存储引擎(`Pebble`是`RocksDB`的Go语言版本)。


### **用户自定义读写**

MatrixCube作为一种通用分布式存储框架，可以构建不同的分布式存储系统。用户也可以自定义读写命令。只要它能够在单机版本中工作，MatrixCube就可以将其升级到分布式版本。




