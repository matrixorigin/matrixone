# **Auto-Rebalance & Scheduling**

MatrixCube is a framework to implement distributed system. For a distributed system, data are stored across many machines. When the number of machines changes, with cluster scaling out or machine crash for example, data need to be moved across machines. 

 `Prophet` is the key component of MatrixCube for Auto-Rebalance and Scheduling. An `Etcd Server` is embedded inside `Prophet` for storing the metadata of the cluster.

It has three main objectives to reach:

* Keep the storage level of each `Store` balanced.
* Keep the write/read requests balanced. 
* Keep the logical table distribution balanced. 

We designed a mechanism of `Heartbeat` and `Event Notify` to achieve these objectives. Each `Store` and `Leader Replica` will send `Hearbeat` information to `Prophet`, `Prophet` will make scheduling decision based on the information. We need to configure certain `Store`s to take the duty of `Prophet`.

## **Store Hearbeat**

Each `Store` sends `Heartbeat` periodically to `Prophet`, the `Heartbeat` includes: 

* At the moment, how many `Replicas` are in this `Store`.
* At the moment, how much storage space does this `Store` have, how much space is already used, how much space remaining. 

`Prophet` collects all `Heartbeats`, and `Prophet` will understand a global `Replica` mapping and the storage space of each `Store`. Based on this information, `Prophet` sends scheduling orders, moving some `Replica`s to proper `Store`s, in order to balance the `Replica` numbers for each `Store`. Since each `Replica` of a `Shard` is the same size, the storage space is thus equalized.

## **Replica Hearbeat**

For each `Shard`, it has several `Replicas` distributed in several `Stores`. These `Replica`s form a `Raft-Group` and a `Leader` is elected. This `Leader` sends periodically `Heartbeats` to `Prophet`. This `Heartbeat` has information as:

* At the moment, how many `Replicas` a `Shard` has, and the latest active time of each `Replica`.
* At the moment, who is the `Leader Replica`. 

`Prophet` collects all `Hearbeat`s and constructs a global `Shard Replica` and `Replica Leader` mapping. With this information, `Prophet` launches scheduling orders under the following circumstances:

* `Add Replica` order: if the number of `Shard Replica` is not enough, look for appropriate `Stores` to add `Replicas`. 
* `Remove Replica` order: if the number of `Shard Replica` exceeds the limit, delete `Replicas` in appropriate `Stores`.
* `Move Replica` order: if the number of `Shard Replica` is not balanced, some `Replicas` will be moved to achieve the balance. 
* `Transfer Leader` order: if the `Leader` number is not balanced in the cluster, some `Leaders` will be transferred. 

## **Event Notify**

The `Heartbeat` information collected will be synchronized to all MatrixCube `Stores`. Each `Store` will form the global routing table. 

