package col

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
	"sync/atomic"
)

type IColumnBlock interface {
	io.Closer
	GetNext() IColumnBlock
	SetNext(next IColumnBlock)
	GetID() common.ID
	GetRowCount() uint64
	InitScanCursor(cusor *ScanCursor) error
	Append(part IColumnPart)
	GetPartRoot() IColumnPart
	GetBlockType() base.BlockType
	GetIndexHolder() *index.BlockHolder
	GetColIdx() int
	CloneWithUpgrade(IColumnSegment, *md.Block) IColumnBlock
	String() string
	Ref() IColumnBlock
	UnRef()
	GetRefs() int64
}

type ColumnBlock struct {
	sync.RWMutex
	ID          common.ID
	Next        IColumnBlock
	Type        base.BlockType
	ColIdx      int
	Refs        int64
	Meta        *md.Block
	File        ldio.ISegmentFile
	IndexHolder *index.BlockHolder
}

func (blk *ColumnBlock) GetRefs() int64 {
	return atomic.LoadInt64(&blk.Refs)
}

func (blk *ColumnBlock) GetIndexHolder() *index.BlockHolder {
	return blk.IndexHolder
}

func (blk *ColumnBlock) GetColIdx() int {
	return blk.ColIdx
}

func (blk *ColumnBlock) GetBlockType() base.BlockType {
	blk.RLock()
	defer blk.RUnlock()
	return blk.Type
}

func (blk *ColumnBlock) GetRowCount() uint64 {
	return atomic.LoadUint64(&blk.Meta.Count)
}

func (blk *ColumnBlock) SetNext(next IColumnBlock) {
	blk.Lock()
	defer blk.Unlock()
	if blk.Next != nil {
		blk.Next.UnRef()
	}
	blk.Next = next
}

func (blk *ColumnBlock) GetNext() IColumnBlock {
	blk.RLock()
	if blk.Next != nil {
		blk.Next.Ref()
	}
	r := blk.Next
	blk.RUnlock()
	return r
}

func (blk *ColumnBlock) GetID() common.ID {
	return blk.ID
}
