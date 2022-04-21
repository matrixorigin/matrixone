# **部署分布式MatrixOne集群**


MatrixOne支持分布式部署，需要至少三个节点来完成分布式配置。

## **软硬件环境要求**
服务器建议配置：x86 CPU，16核，64GB内存。

操作系统版本要求：CentOS 7.x版本。

## **两类节点**

MatrixOne集群中有两种类型的节点(store)：调度节点(prophet stores)和纯存储节点(pure storage stores)。集群中前三个节点为调度节点，其他的都是纯存储节点。

例如，如果集群中有三个节点，那么所有节点都是调度节点。 如果集群中有5个节点，那么其中3个是调度节点，2个是纯存储节点。


## **部署流程**
此处使用五个节点来演示部署教程。
### **步骤1** 


准备3台服务器作为调度节点。
每个节点必须安装单机版本的MatrixOne。安装步骤与[单机版安装教程](install-standalone-matrixone.zh.md)一致。
 

### **步骤2** 

创建store1，称之为`Prophet Genesis Store`（种子调度节点）

**1.** 设置种子调度节点的`id`
```
nodeID = 1
```
**2.** 设置`addr-raft` 与 `addr-client`参数，分别用于接收集群中的Raft组与`Shard Proxy`的信息：

```
addr-raft = "your_ip_address:10000"
addr-client = "your_ip_address:20000"
```

**3.** 设置`Shard`的容量限制，超过这个容量`Shard`将会自动分裂：
```
shard-capacity-bytes = "4096MB"
```

**4.** 设置`rpc-addr`来接收集群中的心跳信息：
```
rpc-addr = "your_ip_address:30000"
```

**5.** 设置`join`， `client-urls`和`peer-urls`参数，组成一个内嵌的`Etcd-server`集群。当`Prophet Genesis Store`为首个节点时，`join`参数可以为空：

```
join = ""
client-urls = "your_ip_address:40000"
peer-urls = "your_ip_address:50000"
```
**6.** 将`storage-node`设置为`TRUE`，调度节点将存储集群的元数据信息：
```
storage-node = true
```
**7.** 设置`max-replicas`参数来指定每个`Shard`需要多少个副本。注意该参数取值只能是`2*N+1`：
```
max-replicas = 3
```

### **步骤3** 
将store2与store3设置为调度节点，并重复步骤1与步骤2。唯一的区别在于需要设置独特的`nodeID`与`join`地址：

``` 
nodeID = 2 or 3; 
join = "ip_address_of_store1"
```

### **步骤4** 
设置其他两个节点为纯存储节点（pure storage store），并重复步骤1与步骤2。有三个额外的参数需要指定：

* Set a unique number for nodeID. 按照nodeID设置相应的number
* 将`storage-node`设置为TRUE`  
* 使用三个调度节点的三个`client-urls`参数来当作`external-etcd`参数：
```
nodeID = 4/5;
storage-node = false;
storage-node = ["ip_address_of_prophet_store1","ip_address_of_prophet_store2","ip_address_of_prophet_store3"]
```

## **相关信息**
本节介绍了分布式MatrixOne集群的安装部署流程。若想了解更多关于分布式架构配置参数的信息，可以查阅[分布式配置参数手册](../Reference/System-Parameters/distributed-settings.md). 
