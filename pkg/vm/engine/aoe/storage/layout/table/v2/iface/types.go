package iface

import (
	"io"
	"matrixone/pkg/container/vector"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/process"
)

type ITableData interface {
	common.IRef
	GetID() uint64
	GetName() string
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetSegmentCount() uint32
	GetSegmentedIndex() (uint64, bool)
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
	GetRowCount() uint64
	AddRows(uint64) uint64
	GetMeta() *md.Table
	Size(string) uint64
}

type ISegment interface {
	common.IRef
	GetReplayIndex() *md.LogIndex
	GetMeta() *md.Segment
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.SegmentHolder
	GetSegmentFile() base.ISegmentFile
	GetSegmentedIndex() (uint64, bool)
	GetType() base.SegmentType
	RegisterBlock(*md.Block) (blk IBlock, err error)
	StrongRefBlock(id uint64) IBlock
	WeakRefBlock(id uint64) IBlock
	GetNext() ISegment
	SetNext(ISegment)
	String() string
	GetRowCount() uint64
	Size(string) uint64
	CloneWithUpgrade(ITableData, *md.Segment) (ISegment, error)
	UpgradeBlock(*md.Block) (IBlock, error)
	BlockIds() []uint64
}

type IBlock interface {
	common.MVCC
	common.IRef
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.BlockHolder
	GetSegmentedIndex() (uint64, bool)
	GetMeta() *md.Block
	GetType() base.BlockType
	CloneWithUpgrade(ISegment, *md.Block) (IBlock, error)
	GetSegmentFile() base.ISegmentFile
	String() string
	GetFullBatch() batch.IBatch
	GetBatch(attrs []int) dbi.IBatchReader
	GetVectorCopy(attr string, ref uint64, proc *process.Process) (*vector.Vector, error)
	GetRowCount() uint64
	GetNext() IBlock
	SetNext(next IBlock)
	Size(string) uint64
}

type IColBlockHandle interface {
	io.Closer
	GetPageNode(int) bmgrif.MangaedNode
}
