package flusher

import "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/shard"

type FlushDriver interface {
	GetId() uint64
	FlushNode(id uint64) error
}

type DriverFactory = func(id uint64) FlushDriver

type Flusher interface {
	shard.NodeAware
	Start()
	Stop()
	String() string
}
