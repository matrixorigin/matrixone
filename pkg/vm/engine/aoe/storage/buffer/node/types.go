package node

import (
	"io"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	ioif "matrixone/pkg/vm/engine/aoe/storage/dataio/iface"
	"sync"
)

type NodeBuffer struct {
	buf.IBuffer
	ID common.ID
	// Type nif.BufferType
}

type NodeHandleCtx struct {
	ID        common.ID
	Buff      buf.IBuffer
	Spillable bool
	Manager   mgrif.IBufferManager
	Size      uint64
	Reader    io.Reader
}

type NodeHandle struct {
	sync.Mutex
	State     nif.NodeState
	ID        common.ID
	Buff      buf.IBuffer
	Spillable bool
	Capacity  uint64
	RTState   nif.NodeRTState
	Refs      uint64
	Manager   mgrif.IBufferManager
	Iter      uint64
	IO        ioif.IO
}

// BufferHandle is created from IBufferManager::Pin, which will set the INodeHandle reference to 1
// The following IBufferManager::Pin will call INodeHandle::Ref to increment the reference count
// BufferHandle should alway be closed manually when it is not needed, which will call IBufferManager::Unpin
type BufferHandle struct {
	Handle  nif.INodeHandle
	Manager mgrif.IBufferManager
}
