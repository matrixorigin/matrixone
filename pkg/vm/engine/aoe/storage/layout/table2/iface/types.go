package iface

import (
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

type ITableData interface {
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
	String() string
	CloneWithUpgrade(ITableData, *md.Segment) (ISegment, error)
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
}
