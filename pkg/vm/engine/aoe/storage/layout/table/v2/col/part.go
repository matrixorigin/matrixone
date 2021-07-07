package col

import (
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/wrapper"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
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
	var constructor buf.MemoryNodeConstructor
	switch blk.GetType() {
	case base.TRANSIENT_BLK:
		bufMgr = host.GetMTBufMgr()
		switch blk.GetColType().Oid {
		case types.T_char, types.T_varchar, types.T_json:
			constructor = vector.StrVectorConstructor
		default:
			constructor = vector.StdVectorConstructor
		}
	case base.PERSISTENT_BLK:
		bufMgr = host.GetSSTBufMgr()
		vf = blk.GetSegmentFile().MakeVirtualPartFile(&blkId)
		constructor = vector.VectorWrapperConstructor
	case base.PERSISTENT_SORTED_BLK:
		bufMgr = host.GetSSTBufMgr()
		vf = blk.GetSegmentFile().MakeVirtualPartFile(&blkId)
		constructor = vector.VectorWrapperConstructor
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
	node := bufMgr.CreateNode(vf, constructor, capacity).(*bmgr.Node)
	if node == nil {
		return nil
	}
	part.Node = node

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
	cloned.Node = sstBufMgr.CreateNode(vf, vector.VectorWrapperConstructor, part.Capacity).(*bmgr.Node)

	return cloned
}

func (part *ColumnPart) GetVector() vector.IVector {
	handle := part.GetBufferHandle()
	vec := wrapper.NewVector(handle)
	return vec
}

func (part *ColumnPart) ForceLoad(ref uint64, proc *process.Process) (*ro.Vector, error) {
	size := int64(part.Capacity)
	data, err := proc.Alloc(size)
	if err != nil {
		return nil, err
	}
	data = data[:mempool.CountSize+size]
	if _, err := part.VFile.Read(data[mempool.CountSize:]); err != nil {
		proc.Free(data)
		return nil, err
	}
	vec := ro.New(part.Block.GetColType())
	if err := vec.Read(data[:len(data)-4]); err != nil {
		proc.Free(data)
		return nil, err
	}
	vec.Data = data
	copy(data, encoding.EncodeUint64(ref))
	return vec, nil
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
