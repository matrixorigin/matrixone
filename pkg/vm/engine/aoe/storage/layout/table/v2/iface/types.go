package iface

import (
	"io"
	"matrixone/pkg/container/types"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

type ITableData interface {
	common.IRef
	GetID() uint64
	GetName() string
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetSegmentCount() uint32
	GetIndexHolder() *index.TableHolder
	RegisterSegment(meta *md.Segment) (seg ISegment, err error)
	RegisterBlock(meta *md.Block) (blk IBlock, err error)
	StrongRefSegment(id uint64) ISegment
	WeakRefSegment(id uint64) ISegment
	StrongRefBlock(segId, blkId uint64) IBlock
	WeakRefBlock(segId, blkId uint64) IBlock
	String() string
	UpgradeSegment(id uint64) (ISegment, error)
	UpgradeBlock(*md.Block) (IBlock, error)
	SegmentIds() []uint64
	StongRefRoot() ISegment
	WeakRefRoot() ISegment
}

type ISegment interface {
	common.IRef
	GetMeta() *md.Segment
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.SegmentHolder
	GetSegmentFile() base.ISegmentFile
	GetType() base.SegmentType
	RegisterBlock(*md.Block) (blk IBlock, err error)
	StrongRefBlock(id uint64) IBlock
	WeakRefBlock(id uint64) IBlock
	GetNext() ISegment
	SetNext(ISegment)
	String() string
	CloneWithUpgrade(ITableData, *md.Segment) (ISegment, error)
	UpgradeBlock(*md.Block) (IBlock, error)
	BlockIds() []uint64
}

type IBlock interface {
	common.IRef
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.BlockHolder
	GetMeta() *md.Block
	GetType() base.BlockType
	CloneWithUpgrade(ISegment, *md.Block) (IBlock, error)
	GetSegmentFile() base.ISegmentFile
	String() string
	GetBlockHandle() IBlockHandle
	StrongWrappedBlock(colIdx []int) IBlockHandle
	GetNext() IBlock
	SetNext(next IBlock)
}

type IColBlockHandle interface {
	io.Closer
	GetPageNode(int) bmgrif.MangaedNode
}

type IBlockHandle interface {
	io.Closer
	GetHost() IBlock
	GetPageNode(colIdx, pos int) bmgrif.MangaedNode
	GetVector(int) *vector.StdVector
	Cols() int
	ColType(idx int) types.Type
}
