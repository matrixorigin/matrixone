package dbi

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
)

type ISnapshot interface {
	io.Closer
	SegmentIds() []uint64
	NewIt() ISegmentIt
	GetSegment(id uint64) ISegment
}

type IBlockHandle interface {
	io.Closer
	GetVector(int) *vector.StdVector
}

type IBlock interface {
	GetID() uint64
	GetSegmentID() uint64
	GetTableID() uint64
	Prefetch() batch.IBatchReader
}

type ISegment interface {
	NewIt() IBlockIt
	GetID() uint64
	GetTableID() uint64
	BlockIds() []uint64
	GetBlock(id uint64) IBlock
}

type Iterator interface {
	io.Closer
	Next()
	Valid() bool
}

type IBlockIt interface {
	Iterator
	GetHandle() IBlock
}

type ISegmentIt interface {
	Iterator
	GetHandle() ISegment
}
