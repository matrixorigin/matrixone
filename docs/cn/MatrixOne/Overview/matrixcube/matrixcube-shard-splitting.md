# **Shard Splitting**


`Shard`是数据的一个分区或分片。分布式系统通常将庞大的数据切割为碎片并将之存储集群内不同的机器上，以此增强整体的存储能力并承担更大的工作负载。每个`Shard`都会有几个`Replica`作为副本，来维持高可用性，而一组`Replica`便组成了`Raft-Group`。  

在MatrixCube的设计框架中，创建的新表是一个`Shard`。根据用户定义的容量限制参数，`Shard`有相应的大小限制。  
随着表中数据量不断增加，它将超过`Shard`的容量限制。此时，将发生`Shard Splitting`，原始的`Shard`将被切割成两个`Shard`，每个`Shard`有相对相等的存储量。与此同时，该`Raft-Group`中其他所有`Replica`将以相同的行为分裂。原来的`Raft-Group`将被移除，代之以两个新的`Raft-Group`。  
一旦两个新的`Raft-Groups`成立，将立即举行`re-election`操作。在大多数情况下，原始的`Replica leader`仍然会被选为leader。  
下面的图表可以形象地解释这一机制：
![Shard Splitting](https://github.com/matrixorigin/artwork/blob/main/docs/overview/matrixcube-shard-splitting.svg?raw=true)

