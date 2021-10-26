package shard

type Aware interface {
	ShardCreated(id uint64)
	ShardDeleted(id uint64)
	OnStats(stats interface{})
}

type Node interface {
	GetId() uint64
}

type NodeAware interface {
	Aware
	ShardNodeCreated(shardId, nodeId uint64)
	ShardNodeDeleted(shardId, nodeId uint64)
}
