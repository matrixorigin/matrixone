// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package col

import (
	"bytes"
	"matrixone/pkg/container/types"
	ro "matrixone/pkg/container/vector"
	buf "matrixone/pkg/vm/engine/aoe/storage/buffer"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/wrapper"
	"sync"
)

type sllnode = common.SLLNode

type loadFunc = func(*bytes.Buffer, *bytes.Buffer) (*ro.Vector, error)
type partLoadFunc = func(*columnPart) loadFunc

var (
	defalutPartLoadFunc partLoadFunc
)

func init() {
	defalutPartLoadFunc = func(part *columnPart) loadFunc {
		return part.loadFromDisk
	}
	// defalutPartLoadFunc = func(part *columnPart) loadFunc {
	// 	return part.loadFromBuf
	// }
}

type IColumnPart interface {
	bmgrif.INode
	common.ISLLNode
	GetNext() IColumnPart
	SetNext(IColumnPart)
	GetID() uint64
	GetColIdx() int
	LoadVectorWrapper() (*vector.VectorWrapper, error)
	ForceLoad(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*ro.Vector, error)
	Prefetch() error
	CloneWithUpgrade(IColumnBlock, bmgrif.IBufferManager) IColumnPart
	GetVector() vector.IVector
	Size() uint64
}

type columnPart struct {
	sllnode
	*bmgr.Node
	host IColumnBlock
}

func NewColumnPart(host iface.IBlock, blk IColumnBlock, capacity uint64) IColumnPart {
	defer host.Unref()
	defer blk.Unref()
	var bufMgr bmgrif.IBufferManager
	part := &columnPart{
		host:    blk,
		sllnode: *common.NewSLLNode(new(sync.RWMutex)),
	}
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

func (part *columnPart) CloneWithUpgrade(blk IColumnBlock, sstBufMgr bmgrif.IBufferManager) IColumnPart {
	defer blk.Unref()
	cloned := &columnPart{host: blk}
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

func (part *columnPart) GetVector() vector.IVector {
	handle := part.GetBufferHandle()
	vec := wrapper.NewVector(handle)
	return vec
}

func (part *columnPart) LoadVectorWrapper() (*vector.VectorWrapper, error) {
	if part.VFile.GetFileType() == common.MemFile {
		panic("logic error")
	}
	wrapper := vector.NewEmptyWrapper(part.host.GetColType())
	wrapper.File = part.VFile
	_, err := wrapper.ReadFrom(part.VFile)
	if err != nil {
		return nil, err
	}
	return wrapper, nil
}

// func (part *columnPart) loadFromBuf(ref uint64, proc *process.Process) (*ro.Vector, error) {
// 	iv := part.GetVector()
// 	v, err := iv.CopyToVectorWithProc(ref, proc)
// 	if err != nil {
// 		return nil, err
// 	}
// 	iv.Close()
// 	return v, nil
// }

func (part *columnPart) loadFromDisk(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	wrapper := vector.NewEmptyWrapper(part.host.GetColType())
	wrapper.File = part.VFile
	_, err := wrapper.ReadWithBuffer(part.VFile, compressed, deCompressed)
	if err != nil {
		return nil, err
	}
	return &wrapper.Vector, nil
}

func (part *columnPart) ForceLoad(compressed *bytes.Buffer, deCompressed *bytes.Buffer) (*ro.Vector, error) {
	if part.VFile.GetFileType() == common.MemFile {
		var ret *ro.Vector
		vec := part.GetVector()
		if !vec.IsReadonly() {
			if vec.Length() == 0 {
				vec.Close()
				return ro.New(part.host.GetColType()), nil
			}
			vec = vec.GetLatestView()
		}
		ret, err := vec.CopyToVectorWithBuffer(compressed, deCompressed)
		vec.Close()
		return ret, err
	}
	return defalutPartLoadFunc(part)(compressed, deCompressed)
}

func (part *columnPart) Prefetch() error {
	if part.VFile.GetFileType() == common.MemFile {
		return nil
	}
	id := *part.host.GetMeta().AsCommonID()
	id.Idx = uint16(part.host.GetColIdx())
	return part.host.GetSegmentFile().PrefetchPart(uint64(part.GetColIdx()), id)
}

func (part *columnPart) Size() uint64 {
	return part.BufNode.GetCapacity()
}

func (part *columnPart) GetColIdx() int {
	return part.host.GetColIdx()
}

func (part *columnPart) GetID() uint64 {
	return part.host.GetMeta().ID
}

func (part *columnPart) SetNext(next IColumnPart) {
	part.sllnode.SetNextNode(next)
}

func (part *columnPart) GetNext() IColumnPart {
	r := part.sllnode.GetNextNode()
	if r == nil {
		return nil
	}
	return r.(IColumnPart)
}
