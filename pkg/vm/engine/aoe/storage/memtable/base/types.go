package base

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
)

type IMemTable interface {
	common.IRef
	Append(bat *batch.Batch, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	Commit() error
	Unpin()
	GetMeta() *md.Block
	GetID() common.ID
	String() string
}

type ICollection interface {
	common.IRef
	Append(bat *batch.Batch, index *md.LogIndex) (err error)
	FetchImmuTable() IMemTable
	String() string
}

type IManager interface {
	WeakRefCollection(id uint64) ICollection
	StrongRefCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
	String() string
}
