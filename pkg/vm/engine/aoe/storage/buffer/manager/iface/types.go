package iface

import (
	"io"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"sync"
)

type IBufferManager interface {
	sync.Locker
	RLock()
	RUnlock()
	buf.IMemoryPool

	String() string
	NodeCount() int
	GetNextID() uint64
	GetNextTransientID() uint64

	RegisterMemory(capacity uint64, spillable bool, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	RegisterSpillableNode(capacity uint64, node_id uint64, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	RegisterNode(capacity uint64, node_id uint64, reader io.Reader, constructor buf.MemoryNodeConstructor) nif.INodeHandle
	UnregisterNode(nif.INodeHandle)

	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)
}
