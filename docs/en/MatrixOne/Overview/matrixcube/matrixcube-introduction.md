# **MatrixCube Introduction**

MatrixCube is a fundamental library for building distributed systems, which offers guarantees about reliability, consistency, and scalability. It is designed to facilitate distributed, stateful application building to allow developers only need to focus on the business logic on a single node. MatrixCube is currently built upon multi-raft to provide replicated state machine and will migrate to Paxos families to increase friendliness to scenarios spanning multiple data centers.

Unlike many other distributed systems, MatrixCube is designed as part of the storage nodes. A matrixone distributed deployment doesn't not have dedicated scheduling nodes. MatrixCube cannot work as a standalone module. 


## **MatrixCube Architecture**


![Matrix Cube](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-architecture.svg?raw=true) 



## **Key Concepts**

There are several key concepts for understanding how MatrixCube works. 

### **Store**

A MatrixCube distributed system consists of several physical computers, our data are stored across these physical computers. We call each computer inside this cluster a `Store`.

### **Shard**

Our data in database are organized in tables logically. But for physical storage, the data are split into different partitions to store in order to get better scalability. Each partition is called a `Shard`. In our design, a new created table is initially a `Shard`. When the size of the table exceeds the `Shard` size limit, the `Shard` will split. 

### **Replica**

To provide reliable service, each `Shard` is stored not only once, it will have several copy stored in different `Stores`. We call each copy a `Replica`. A `Shard` can have multiple `Replica`, the data in each `Replica` are the same. 

### **Raft-group and Leader**

Since multiple `Replicas` are located in different `Stores`, once a `Replica` is updated, the other `Replicas` must be updated to keep data consistency. When a client makes query to no matter which `Replica`, it always gets the same result. We deploy Raft protocol to implement the concensus process. The `Replicas` of a particular `Shard` group into a `Raft-group`. 

In each `Raft-group`, a `Leader` is elected to be the representative of this group. All consistent read and write requests are handled only by the leader.

Learn more about: [How does a `Leader` get elected in Raft?](https://raft.github.io/)

### **Data Storage**

A `DataStorage` is an interface for implementing distributed storage service. It must be defined in prior to using MatrixCube. `DataStorage` needs to be implemented based on the characteristics of storage engine. Some common distributed storage service can be easily constructed based on `DataStorage`, such as `Distributed Redis`, `Distributed Key-Value`, `Distributed File System` etc.  A default Key-Value based `DataStorage` is provided to meet the requirements of most scenarios.  

### **Prophet**

`Prophet` is a scheduling module. It takes charge of `Auto-Rebalance`, which keeps the system storage level and read/write throughput level balanced across `Stores`. The inital 3 `Stores` of a MatrixCube cluster are all `Prophet Nodes`. 

Learn more about [How does Prophet handle the scheduling?](matrixcube-auto-rebalance-scheduling.md)

### **Raftstore**

`Raftstore` is the core component of MatrixCube, it implements the most important features of MatrixCube:

* Metadata storage: including the metadata of `Store`, `Shard`, `Raft-log`. 

* Multi-Raft management: the relationship between  `Store`, `Shard`, `Replica`, `Raft-Group`, the communication between multiple `Raft-Group`s, Leader election and re-election.

* Global Routing: a global routing table will be constructed with the Event Notify mechanism of `Prophet`. The read/write routing will be based on this routing table. 

* Shard Proxy: a proxy for read/write request for `Shard`. With the proxy, the detailed implementation of `Multi-Raft` is senseless and all `Store`s are equal for users. User can make request to any `Store`, all requests will be routed to the right `Store` by `Shard Proxy`. 

Learn more about [How do the `Shard Proxy` and `Global Routing` work?](matrixcube-proxy-routing.md)






## **Key Features**

### **Strong Consistency** 

MatrixCube provides a strong consistency. It is guaranted that after any successful data write, the reading afterwards will get the latest value, no matter from which store.

### **Fault Tolerance**

The distributed storage service implemented by MatrixCube is a fault tolerant and high available service. When a `Shard` has `2*N+1` `Replicas`, the system can still work until `N+1` `Replicas` fail.

For example, a cluster with 3 `Stores` can survive with 1 `Store` failure; a cluster with 5 `Stores` can survive with 2 `Stores` failure.


### **Shard Splitting**

There is a certain limit to a `Shard` size. Whenever a `Shard` exceeds its storage limit, MatrixCube splits a `Shard` into two `Shards` and keep each `Shard` with the same storage level. 

You can checkout a more detailed descripition about this process with [How does the Shard Splitting work?](matrixcube-shard-splitting.md). 

### **Auto-Rebalance**

A distributed system should leverage all the computation power and storage of all nodes. For a MatrixCube cluster, when there is an increase or decrease of `Stores`, an `Auto-Rebalance` will occur, which moves data across `Stores` to reach balance for each single `Store`. 

Learn more about: [How does the Auto-Rebalance work?](matrixcube-auto-rebalance-scheduling.md).

### **Scale-out**

With shard splitting and auto-rebalance, a MatrixCube distributed system is capable of scaling out. The cluster storage and throughput capability are proportional to the number of `Stores`.

### **User-defined storage engine**

MatrixCube has no limit to standalone data storage engine. Any storage engine implementing `DataStorage` interface defined by MatrixCube could construct a MatrixCube-based distributed system. By default, MatrixCube provides a `Pebble`-based Key-Value storage engine. (`Pebble` is a Go version of `RocksDB`).

### **User-defined Read/Write**

As a general distributed framework, different distributed storage system could be build based on MatrixCube. User can also customize their read/write commands. As long as it works in a standalone version, MatrixCube can help you upgrading it to a distributed version. 





