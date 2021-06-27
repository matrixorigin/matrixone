package base

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
)

type IBlockHandle interface {
	io.Closer
	GetID() *common.ID
	GetColumn(int) col.IColumnBlock
	InitScanCursor() []col.ScanCursor
	GetIndexHolder() *index.BlockHolder
}

type ISegmentHandle interface {
	io.Closer
	NewIterator() IBlockIterator
}

type Iterator interface {
	io.Closer
	Next()
	Valid() bool
}

type IBlockIterator interface {
	Iterator
	GetBlockHandle() IBlockHandle
}

type ISegmentIterator interface {
	Iterator
	GetSegmentHandle() ISegmentHandle
}
