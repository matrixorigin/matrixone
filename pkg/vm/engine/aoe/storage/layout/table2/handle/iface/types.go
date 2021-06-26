package iface

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
)

type IBlock interface {
	io.Closer
	GetID() uint64
	GetSegmentID() uint64
	GetTableID() uint64
	Prefetch() iface.IBlockHandle
}

type ISegment interface {
	io.Closer
	NewIt() IBlockIt
	GetID() uint64
	GetTableID() uint64
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
