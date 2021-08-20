package base

import (
	"io"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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

type IMemTable interface {
	common.IRef
	Append(bat *batch.Batch, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Unpin()
	GetMeta() *md.Block
	GetID() common.ID
	String() string
}

type ICollection interface {
	common.IRef
	Append(bat *batch.Batch, index *md.LogIndex) (err error)
	String() string
}

type ISizeLimiter interface {
	ActiveSize() uint64
	ApplySizeQuota(uint64) bool
	RetuernQuota(uint64) uint64
}

type IManager interface {
	WeakRefCollection(id uint64) ICollection
	StrongRefCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
	String() string
}
