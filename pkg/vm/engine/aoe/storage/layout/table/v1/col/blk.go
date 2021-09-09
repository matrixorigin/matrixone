package col

import (
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/process"
	"sync"
	"sync/atomic"
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
	String() string
	Size() uint64
	GetVector() vector.IVector
	LoadVectorWrapper() (*vector.VectorWrapper, error)
	ForceLoad(ref uint64, proc *process.Process) (*ro.Vector, error)
	Prefetch() error
	GetVectorReader() dbi.IVectorReader
}

type columnBlock struct {
	sync.RWMutex
	common.RefHelper
	colIdx      int
	meta        *md.Block
	segFile     base.ISegmentFile
	indexHolder *index.BlockHolder
	typ         base.BlockType
}

func (blk *columnBlock) GetSegmentFile() base.ISegmentFile {
	return blk.segFile
}

func (blk *columnBlock) GetIndexHolder() *index.BlockHolder {
	return blk.indexHolder
}

func (blk *columnBlock) GetColIdx() int {
	return blk.colIdx
}

func (blk *columnBlock) GetColType() types.Type {
	return blk.meta.Segment.Table.Schema.ColDefs[blk.colIdx].Type
}

func (blk *columnBlock) GetMeta() *md.Block {
	return blk.meta
}

func (blk *columnBlock) GetType() base.BlockType {
	return blk.typ
}

func (blk *columnBlock) GetRowCount() uint64 {
	return atomic.LoadUint64(&blk.meta.Count)
}

func (blk *columnBlock) GetID() uint64 {
	return blk.meta.ID
}
