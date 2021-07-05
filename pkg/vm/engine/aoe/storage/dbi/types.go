package dbi

import (
	"io"
)

type ISnapshot interface {
	io.Closer
	SegmentIds() []uint64
	NewIt() ISegmentIt
	GetSegment(id uint64) ISegment
}

type IBlock interface {
	GetID() uint64
	GetSegmentID() uint64
	GetTableID() uint64
	Prefetch() IBatchReader
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
