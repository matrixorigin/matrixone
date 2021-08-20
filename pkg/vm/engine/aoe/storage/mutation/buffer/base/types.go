package base

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync"
)

type INodeHandle interface {
	io.Closer
	GetID() common.ID
	GetNode() INode
}

type INode interface {
	sync.Locker
	io.Closer
	common.IRef
	RLock()
	RUnlock()
	GetID() common.ID
	Unload()
	Unloadable() bool
	IsLoaded() bool
	Load()
	MakeHandle() INodeHandle
	Destroy()
	Size() uint64
	Iteration() uint64
	IncIteration() uint64
	IsClosed() bool
	GetState() iface.NodeState
}

type INodeManager interface {
	ISizeLimiter
	sync.Locker
	RLock()
	RUnlock()
	String() string
	Count() int
	RegisterNode(INode)
	UnregisterNode(INode)
	Pin(INode) INodeHandle
	Unpin(INode)
}

type ISizeLimiter interface {
	Total() uint64
	ApplyQuota(uint64) bool
	RetuernQuota(uint64) uint64
}
