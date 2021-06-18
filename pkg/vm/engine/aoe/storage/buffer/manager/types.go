package manager

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	iw "matrixone/pkg/vm/engine/aoe/storage/worker/base"
	"sync"
)

type EvictNode struct {
	Handle iface.INodeHandle
	Iter   uint64
}

type IEvictHolder interface {
	sync.Locker
	Enqueue(n *EvictNode)
	Dequeue() *EvictNode
}

type BufferManager struct {
	buf.IMemoryPool
	sync.RWMutex
	Nodes           map[uint64]iface.INodeHandle // Manager is not responsible to Close handle
	EvictHolder     IEvictHolder
	Flusher         iw.IOpWorker
	NextID          uint64
	NextTransientID uint64
	EvictTimes      int64
	LoadTimes       int64
}
