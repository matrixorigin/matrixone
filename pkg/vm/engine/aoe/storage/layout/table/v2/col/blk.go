package col

import (
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/process"
	"sync"
	"sync/atomic"
	// log "github.com/sirupsen/logrus"
)

type IColumnBlock interface {
	common.IRef
	GetID() uint64
	GetMeta() *md.Block
	GetRowCount() uint64
	RegisterPart(part IColumnPart)
	GetType() base.BlockType
	GetColType() types.Type
	GetIndexHolder() *index.BlockHolder
	GetColIdx() int
	GetSegmentFile() base.ISegmentFile
	CloneWithUpgrade(iface.IBlock) IColumnBlock
	// EvalFilter(*index.FilterCtx) error
	String() string
	Size() uint64
	GetVector() vector.IVector
	ForceLoad(ref uint64, proc *process.Process) (*ro.Vector, error)
	GetVectorReader() dbi.IVectorReader
}

type ColumnBlock struct {
	sync.RWMutex
	common.RefHelper
	// Next        IColumnBlock
	ColIdx      int
	Meta        *md.Block
	SegmentFile base.ISegmentFile
	IndexHolder *index.BlockHolder
	Type        base.BlockType
}

// func (blk *ColumnBlock) EvalFilter(ctx *index.FilterCtx) error {
// 	return blk.IndexHolder.EvalFilter(blk.ColIdx, ctx)
// }

func (blk *ColumnBlock) GetSegmentFile() base.ISegmentFile {
	return blk.SegmentFile
}

func (blk *ColumnBlock) GetIndexHolder() *index.BlockHolder {
	return blk.IndexHolder
}

func (blk *ColumnBlock) GetColIdx() int {
	return blk.ColIdx
}

func (blk *ColumnBlock) GetColType() types.Type {
	return blk.Meta.Segment.Schema.ColDefs[blk.ColIdx].Type
}

func (blk *ColumnBlock) GetMeta() *md.Block {
	return blk.Meta
}

func (blk *ColumnBlock) GetType() base.BlockType {
	return blk.Type
}

func (blk *ColumnBlock) GetRowCount() uint64 {
	return atomic.LoadUint64(&blk.Meta.Count)
}

// func (blk *ColumnBlock) SetNext(next IColumnBlock) {
// 	blk.Lock()
// 	defer blk.Unlock()
// 	if blk.Next != nil {
// 		blk.Next.UnRef()
// 	}
// 	blk.Next = next
// }

// func (blk *ColumnBlock) GetNext() IColumnBlock {
// 	blk.RLock()
// 	if blk.Next != nil {
// 		blk.Next.Ref()
// 	}
// 	r := blk.Next
// 	blk.RUnlock()
// 	return r
// }

func (blk *ColumnBlock) GetID() uint64 {
	return blk.Meta.ID
}
