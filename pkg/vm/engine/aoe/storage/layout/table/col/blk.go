package col

import (
	"io"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"sync"
	"sync/atomic"
)

type BlockType uint8

const (
	TRANSIENT_BLK BlockType = iota
	PERSISTENT_BLK
	PERSISTENT_SORTED_BLK
	MOCK_BLK
	MOCK_PERSISTENT_BLK
	MOCK_PERSISTENT_SORTED_BLK
)

func (t BlockType) String() string {
	switch t {
	case TRANSIENT_BLK:
		return "TB"
	case PERSISTENT_BLK:
		return "PB"
	case PERSISTENT_SORTED_BLK:
		return "PSB"
	case MOCK_BLK:
		return "MB"
	case MOCK_PERSISTENT_BLK:
		return "MPB"
	case MOCK_PERSISTENT_SORTED_BLK:
		return "MPSB"
	}
	panic("unspported")
}

type IColumnBlock interface {
	io.Closer
	GetNext() IColumnBlock
	SetNext(next IColumnBlock)
	GetID() common.ID
	GetRowCount() uint64
	InitScanCursor(cusor *ScanCursor) error
	Append(part IColumnPart)
	GetPartRoot() IColumnPart
	GetBlockType() BlockType
	GetColIdx() int
	CloneWithUpgrade(IColumnSegment, *md.Block) IColumnBlock
	String() string
	Ref() IColumnBlock
	UnRef()
	GetRefs() int64
}

type ColumnBlock struct {
	sync.RWMutex
	ID     common.ID
	Next   IColumnBlock
	Type   BlockType
	ColIdx int
	Refs   int64
	Meta   *md.Block
	// Segment  IColumnSegment
}

func (blk *ColumnBlock) GetRefs() int64 {
	return atomic.LoadInt64(&blk.Refs)
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
