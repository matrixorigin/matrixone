package col

import (
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	"matrixone/pkg/prefetch"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/wrapper"
	"matrixone/pkg/vm/process"
	"sync"
	"syscall"

	// log "github.com/sirupsen/logrus"
)

type IColumnPart interface {
	bmgrif.INode
	common.IRef
	GetNext() IColumnPart
	SetNext(IColumnPart)
	GetID() uint64
	GetColIdx() int
	LoadVectorWrapper() (*vector.VectorWrapper, error)
	ForceLoad(ref uint64, proc *process.Process) (*ro.Vector, error)
	Prefetch() error
	CloneWithUpgrade(IColumnBlock, bmgrif.IBufferManager) IColumnPart
	GetVector() vector.IVector
	Size() uint64
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
	var vf common.IVFile
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
		vf = common.NewMemFile(int64(capacity))
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

	var node bmgrif.INode
	node = bufMgr.CreateNode(vf, false, constructor)
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
	var vf common.IVFile
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
	cloned.Node = sstBufMgr.CreateNode(vf, false, vector.VectorWrapperConstructor).(*bmgr.Node)

	return cloned
}

func (part *ColumnPart) GetVector() vector.IVector {
	handle := part.GetBufferHandle()
	vec := wrapper.NewVector(handle)
	return vec
}

func (part *ColumnPart) LoadVectorWrapper() (*vector.VectorWrapper, error) {
	if part.VFile.GetFileType() == common.MemFile {
		panic("logic error")
	}
	wrapper := vector.NewEmptyWrapper(part.Block.GetColType())
	wrapper.File = part.VFile
	_, err := wrapper.ReadFrom(part.VFile)
	if err != nil {
		return nil, err
	}
	return wrapper, nil
}

func (part *ColumnPart) ForceLoad(ref uint64, proc *process.Process) (*ro.Vector, error) {
	if part.VFile.GetFileType() == common.MemFile {
		var ret *ro.Vector
		vec := part.GetVector()
		if !vec.IsReadonly() {
			if vec.Length() == 0 {
				vec.Close()
				return ro.New(part.Block.GetColType()), nil
			}
			vec = vec.GetLatestView()
		}
		ret, err := vec.CopyToVectorWithProc(ref, proc)
		vec.Close()
		return ret, err
	}
	wrapper := vector.NewEmptyWrapper(part.Block.GetColType())
	wrapper.File = part.VFile
	_, err := wrapper.ReadWithProc(part.VFile, ref, proc)
	if err != nil {
		return nil, err
	}
	return &wrapper.Vector, nil
}

func (part *ColumnPart) Prefetch() error {
	if part.VFile.GetFileType() == common.MemFile {
		return nil
	}
	fd, err := syscall.Open(part.VFile.Stat().Name(), syscall.O_RDONLY, 0)
	if err != nil {
		return err
	}
	err = prefetch.Prefetch(uintptr(fd), 0, uintptr(part.Size()))
	if err != nil {
		return err
	}
	err = syscall.Close(fd)
	return err
}

func (part *ColumnPart) Size() uint64 {
	if part.VFile.GetFileType() == common.DiskFile {
		return part.BufNode.GetCapacity()
	}
	vec := part.GetVector()
	defer vec.Close()
	return vec.GetMemorySize()
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
