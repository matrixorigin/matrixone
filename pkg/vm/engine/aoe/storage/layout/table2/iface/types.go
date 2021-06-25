package iface

import (
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/index"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
)

type ISegment interface {
	common.IRef
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
}

type IBlock interface {
	common.IRef
	GetMTBufMgr() bmgrif.IBufferManager
	GetSSTBufMgr() bmgrif.IBufferManager
	GetFsManager() base.IManager
	GetIndexHolder() *index.BlockHolder
	GetMeta() *md.Block
	GetType() base.BlockType
}
