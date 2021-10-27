package flusher

import "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"

type NodeDriver interface {
	GetId() uint64
	FlushNode(id uint64) error
}

type DriverFactory = func(id uint64) NodeDriver

type Driver interface {
	shard.NodeAware
	Start()
	Stop()
	String() string
}
