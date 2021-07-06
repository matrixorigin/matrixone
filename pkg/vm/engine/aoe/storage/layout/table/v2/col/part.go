package col

import (
	"matrixone/pkg/container/types"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/wrapper"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type IColumnPart interface {
	bmgrif.INode
	common.IRef
	GetNext() IColumnPart
	SetNext(IColumnPart)
	GetID() uint64
	GetColIdx() int
	CloneWithUpgrade(IColumnBlock, bmgrif.IBufferManager) IColumnPart
	GetVector() vector.IVector
}

type ColumnPart struct {
	sync.RWMutex
	common.RefHelper
	*bmgr.Node
	Block IColumnBlock
	Next  IColumnPart
}

func NewColumnPart(host iface.IBlock, blk IColumnBlock, capacity uint64) IColumnPart {
	defer host.Unref()
	defer blk.Unref()
	var bufMgr bmgrif.IBufferManager
	part := &ColumnPart{Block: blk}
	blkId := blk.GetMeta().AsCommonID().AsBlockID()
	blkId.Idx = uint16(blk.GetColIdx())
	var vf bmgrif.IVFile
	switch blk.GetType() {
	case base.TRANSIENT_BLK:
		bufMgr = host.GetMTBufMgr()
	case base.PERSISTENT_BLK:
		bufMgr = host.GetSSTBufMgr()
		vf = blk.GetSegmentFile().MakeVirtualPartFile(&blkId)
	case base.PERSISTENT_SORTED_BLK:
		bufMgr = host.GetSSTBufMgr()
		vf = blk.GetSegmentFile().MakeVirtualPartFile(&blkId)
	default:
		panic("not support")
	}
	if vf != nil {
		vvf := vf.(base.IVirtaulFile)
		// Only in mock case, the stat is nil
		if stat := vvf.Stat(); stat != nil {
			capacity = uint64(stat.Size())
		}
	}
	var node bmgrif.INode
	switch blk.GetColType().Oid {
	case types.T_char, types.T_varchar, types.T_json:
		node = bufMgr.CreateNode(vf, vector.StrVectorConstructor, capacity)
	default:
		node = bufMgr.CreateNode(vf, vector.StdVectorConstructor, capacity)
	}
	if node == nil {
		return nil
	}
	part.Node = node.(*bmgr.Node)

	blk.RegisterPart(part)
	part.Ref()
	return part
}

func (part *ColumnPart) CloneWithUpgrade(blk IColumnBlock, sstBufMgr bmgrif.IBufferManager) IColumnPart {
	defer blk.Unref()
	cloned := &ColumnPart{Block: blk}
	blkId := blk.GetMeta().AsCommonID().AsBlockID()
	blkId.Idx = uint16(blk.GetColIdx())
	var vf bmgrif.IVFile
	switch blk.GetType() {
	case base.TRANSIENT_BLK:
		panic("logic error")
	case base.PERSISTENT_BLK:
		vf = blk.GetSegmentFile().MakeVirtualPartFile(&blkId)
	case base.PERSISTENT_SORTED_BLK:
		vf = blk.GetSegmentFile().MakeVirtualPartFile(&blkId)
	default:
		panic("not supported")
	}
	if vf != nil {
		vvf := vf.(base.IVirtaulFile)
		if stat := vvf.Stat(); stat != nil {
			part.Capacity = uint64(vvf.Stat().Size())
		}
	}
	switch blk.GetColType().Oid {
	case types.T_char, types.T_varchar, types.T_json:
		cloned.Node = sstBufMgr.CreateNode(vf, vector.StrVectorConstructor, part.Capacity).(*bmgr.Node)
	default:
		cloned.Node = sstBufMgr.CreateNode(vf, vector.StdVectorConstructor, part.Capacity).(*bmgr.Node)
	}

	return cloned
}

func (part *ColumnPart) GetVector() vector.IVector {
	handle := part.GetBufferHandle()
	vec := wrapper.NewVector(handle)
	return vec
}

func (part *ColumnPart) GetColIdx() int {
	return part.Block.GetColIdx()
}

func (part *ColumnPart) GetID() uint64 {
	return part.Block.GetMeta().ID
}

func (part *ColumnPart) SetNext(next IColumnPart) {
	part.Lock()
	defer part.Unlock()
	part.Next = next
}

func (part *ColumnPart) GetNext() IColumnPart {
	part.RLock()
	defer part.RUnlock()
	return part.Next
}
