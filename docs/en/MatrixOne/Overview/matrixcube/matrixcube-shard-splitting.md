# **Shard Splitting**

A `Shard` is a data partition, or a data slice. A distributed system ususally cuts a big data trunk to pieces, and places these pieces into different machines to increase the overall storage capability and handle greater workloads. A `Shard` will also have several `Replicas` to maintain a high availability. All these `Replicas` form a `Raft-Group`.  

In our design, a created new table is a `Shard`. A `Shard` has a certain size limit based on a user-defined parameter. 

As size of a table increases, it will exceed the size limit of a `Shard`. At that moment, a `Shard Splitting` will occur. The original `Shard` will be cut in into two `Shards` with each `Shard` having relatively equal storage level. In the meantime, all other `Replicas` of this `Raft-Group` will be splitted with the same behavior. The original `Raft-Group` will be removed, and two new `Raft-Groups` will be created . 

Once two new `Raft-Groups` are created, a re-election will immediately be held. In most cases, the original `Replica` leader will still be elected as `leaders`.  

The belowing diagram illustrates how the shard splitting works:

![Shard Splitting](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-shard-splitting.svg?raw=true)
