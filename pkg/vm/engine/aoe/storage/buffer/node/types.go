package node

import (
	"io"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"sync"
)

type NodeBuffer struct {
	buf.IBuffer
	ID uint64
	// Type nif.BufferType
}

type NodeHandleCtx struct {
	ID          uint64
	Buff        buf.IBuffer
	Spillable   bool
	Manager     mgrif.IBufferManager
	Size        uint64
	Reader      io.Reader
	Constructor buf.MemoryNodeConstructor
	Dir         []byte
}

type NodeHandle struct {
	sync.Mutex
	State       nif.NodeState
	ID          uint64
	Buff        buf.IBuffer
	Spillable   bool
	Capacity    uint64
	RTState     nif.NodeRTState
	Refs        uint64
	Manager     mgrif.IBufferManager
	Iter        uint64
	IO          ioif.IO
	Constructor buf.MemoryNodeConstructor
}

// BufferHandle is created from IBufferManager::Pin, which will set the INodeHandle reference to 1
// The following IBufferManager::Pin will call INodeHandle::Ref to increment the reference count
// BufferHandle should alway be closed manually when it is not needed, which will call IBufferManager::Unpin
type BufferHandle struct {
	Handle  nif.INodeHandle
	Manager mgrif.IBufferManager
}
