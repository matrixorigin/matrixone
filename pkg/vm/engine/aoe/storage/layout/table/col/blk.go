package col

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"sync"
)

type BlockType uint8

const (
	TRANSIENT_BLK BlockType = iota
	PERSISTENT_BLK
	PERSISTENT_SORTED_BLK
	MOCK_BLK
)

type IColumnBlock interface {
	io.Closer
	GetNext() IColumnBlock
	SetNext(next IColumnBlock)
	GetID() layout.ID
	GetRowCount() uint64
	InitScanCursor(cusor *ScanCursor) error
	Append(part IColumnPart)
	GetPartRoot() IColumnPart
	GetBlockType() BlockType
	GetColIdx() int
	CloneWithUpgrade(IColumnSegment) IColumnBlock
	String() string
}

type ColumnBlock struct {
	sync.RWMutex
	ID       layout.ID
	Next     IColumnBlock
	RowCount uint64
	Type     BlockType
	ColIdx   int
	// Segment  IColumnSegment
}

func (blk *ColumnBlock) GetColIdx() int {
	return blk.ColIdx
}

func (blk *ColumnBlock) GetBlockType() BlockType {
	blk.RLock()
	defer blk.RUnlock()
	return blk.Type
}

func (blk *ColumnBlock) GetRowCount() uint64 {
	return blk.RowCount
}

func (blk *ColumnBlock) SetNext(next IColumnBlock) {
	blk.Lock()
	defer blk.Unlock()
	blk.Next = next
}

func (blk *ColumnBlock) GetNext() IColumnBlock {
	blk.RLock()
	defer blk.RUnlock()
	return blk.Next
}

func (blk *ColumnBlock) GetID() layout.ID {
	return blk.ID
}
