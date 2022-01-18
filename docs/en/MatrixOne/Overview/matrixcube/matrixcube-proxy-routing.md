# **Shard Proxy and Global Routing**

A distributed system consists of multiple servers. A lot of complicated traffic and coordination mechanisms are implemented to make the data stored in all servers balanced and the service equally scaled. But users usually don't care and have little intention to understand the distributed detail. Therefore, we designed a `Shard Proxy` to get a simple user experience just like working with a standalone database.The `Shard Proxy` is a central module to accept all user read/write requests. 

For example, when a user is quering for a certain table from a database. For a distributed database system, this request is actually looking for a `Shard` in a ceratin `Store`. 

A user can send this request to any `Store` of the system. The `Shard Proxy` of the `Store` will take that request, it will look up for the `Global Routing` table to find the correct `Store`. 

As we have explained in the `Raft Group and Leader`,`Leader` is the representative of a `Raft Group`, all read and write requests are handled only by the leader. 

Therefore, when a request for certain rows are executed. 
* Firstly, we need to locate the `Shard` where these rows are stored. 
* Secondly, locate the `Leader Replica` of this `Shard` group. 
* Thirdly, route the request to the `Store` where the `Leader Replica` is located. 
* Finally, `Leader Replica` executes the request and returns response. 


## **Example**

We have a cluster of 3 `Stores`, and their status are as below: 

||Range|Store1|Store2|Store3|
|-|-|-|-|-|
|Shard1|[key1-key10)|Leader|Follower|Follower|
|Shard2|[key10-key20)|Follower|Leader|Follower|
|Shard3|[key20-key30)|Follower|Follower|Leader|

A user sends requests on key1, key10 and key20, the following diagram illustrates how the requests gets through `Shard Proxy` and being routed. 

![User Request Routing Diagram](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-requests.svg?raw=true)

