package manager

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"sync"
)

var (
	MockVFile = mockVFile{}
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
	NextID          uint64
	NextTransientID uint64
	EvictTimes      int64
	LoadTimes       int64
	UnregisterTimes int64
	Dir             []byte
}

type mockVFile struct{}

func (vf *mockVFile) Ref()   {}
func (vf *mockVFile) Unref() {}
func (vf *mockVFile) Read(p []byte) (n int, err error) {
	return n, err
}
