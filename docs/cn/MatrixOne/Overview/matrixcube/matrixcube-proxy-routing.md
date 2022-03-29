# **Shard Proxy与Global Routing**

分布式系统由多台服务器组成。为了使存储在所有服务器上的数据均衡，从而使服务规模保持均衡，系统中实现了许多复杂的管理和协调机制。但用户往往不关心分布式系统的细节，因此，我们设计了`Shard Proxy`板块来给用户提供简易的使用体验，就仿佛只在使用一个单机的数据库一般。  
简单来说，`Shard Proxy`就是一个接受所有用户读写请求的中心模块。

例如，当用户从数据库查询某个表时。对于一个分布式数据库系统，这个请求实际上是在一个特定的`Store`中寻找一个`Shard`。  
用户可以将该请求发送到系统里任意`Store`中，其中的`Shard Proxy`将处理该请求，它将通过全路由表`Global Routing` 来寻找到正确的`Store`。  

正如我们对`Raft Group`与`Leader`的解释，`Leader` 代表了一个`Raft Group`，所有的读写操作只由它处理。因此，当我们执行对某些数据行的查询请求时： 
* 首先，需要定位这些数据行所在的`Shard`。
* 其次，找到这个`Shard`所对应的`Raft Group`中的`Leader Replica`。 
* 接着，将请求路由到`Leader Replica`所在的`Store`。
* 最后，`Leader Replica`执行请求并作出响应。 


## **示例**
如果我们有一个由三个`Store`组成的集群，它们的状态如下：

||Range|Store1|Store2|Store3|
|-|-|-|-|-|
|Shard1|[key1-key10)|Leader|Follower|Follower|
|Shard2|[key10-key20)|Follower|Leader|Follower|
|Shard3|[key20-key30)|Follower|Follower|Leader|


用户分别向key1，key10和key20数据发送请求，下图说明了请求如何通过`Shard Proxy`路由组件并被转发。


![User Request Routing Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-requests.svg?raw=true)

