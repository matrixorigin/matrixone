package iface

import (
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"sync"
)

type IBufferManager interface {
	sync.Locker
	RLock()
	RUnlock()
	buf.IMemoryPool

	String() string
	NodeCount() int

	RegisterMemory(capacity uint64, spillable bool) nif.INodeHandle
	RegisterSpillableNode(capacity uint64, node_id layout.ID) nif.INodeHandle
	RegisterNode(capacity uint64, node_id layout.ID, segFile interface{}) nif.INodeHandle
	UnregisterNode(nif.INodeHandle)

	// // Allocate(size uint64) buf.IBufferH

	Pin(h nif.INodeHandle) nif.IBufferHandle
	Unpin(h nif.INodeHandle)
}
