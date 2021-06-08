package base

import (
	// "matrixone/pkg/vm/engine/aoe/storage/common/table"
	// "matrixone/pkg/vm/engine/aoe/storage/common/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
)

type IMemTable interface {
	Append(c *chunk.Chunk, offset uint64, index *md.LogIndex) (n uint64, err error)
	IsFull() bool
	Flush() error
	Close() error
	GetMeta() *md.Block
	GetID() common.ID
	InitScanCursors(cursors []interface{}) error
}

type ICollection interface {
	Append(ck *chunk.Chunk, index *md.LogIndex) (err error)
	FetchImmuTable() IMemTable
}

type IManager interface {
	GetCollection(id uint64) ICollection
	RegisterCollection(interface{}) (c ICollection, err error)
	UnregisterCollection(id uint64) (c ICollection, err error)
	CollectionIDs() map[uint64]uint64
}
