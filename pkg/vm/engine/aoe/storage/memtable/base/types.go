package base

import (
	"matrixone/pkg/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

type IMemTable interface {
	Append(bat *batch.Batch, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	Close() error
	Unpin()
	GetMeta() *md.Block
	GetID() common.ID
}

type ICollection interface {
	Append(bat *batch.Batch, index *md.LogIndex) (err error)
	FetchImmuTable() IMemTable
}

type IManager interface {
	GetCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
}
