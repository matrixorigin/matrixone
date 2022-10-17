// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexwrapper

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/evictable"
)

type BfReader struct {
	bfKey     string
	mgr       base.INodeManager
	bfFacotry evictable.EvictableNodeFactory
}

func newBfReader(mgr base.INodeManager, typ types.Type, id common.ID, fs *objectio.ObjectFS, col uint16, metaloc string) *BfReader {
	metaKey := evictable.EncodeColMetaKey(id.Idx, metaloc)
	bfKey := evictable.EncodeColBfKey(id.Idx, metaloc)

	return &BfReader{
		mgr:       mgr,
		bfKey:     bfKey,
		bfFacotry: func() (base.INode, error) { return evictable.NewBfNode(mgr, bfKey, metaKey, fs, col, metaloc, typ) },
	}
}

func (r *BfReader) MayContainsKey(key any) (b bool, err error) {
	h, err := evictable.PinEvictableNode(r.mgr, r.bfKey, r.bfFacotry)
	if err != nil {
		// TODOa: Error Handling?
		return
	}
	defer h.Close()
	bfNode := h.GetNode().(*evictable.BfNode)
	return bfNode.Bf.MayContainsKey(key)
}

func (r *BfReader) MayContainsAnyKeys(keys containers.Vector, visibility *roaring.Bitmap) (b bool, m *roaring.Bitmap, err error) {
	h, err := evictable.PinEvictableNode(r.mgr, r.bfKey, r.bfFacotry)
	if err != nil {
		// TODOa: Error Handling?
		return
	}
	defer h.Close()
	bfNode := h.GetNode().(*evictable.BfNode)
	return bfNode.Bf.MayContainsAnyKeys(keys, visibility)
}

func (r *BfReader) Destroy() error { return nil }

type BFWriter struct {
	cType       common.CompressType
	writer      objectio.Writer
	block       objectio.BlockObject
	impl        index.StaticFilter
	data        containers.Vector
	colIdx      uint16
	internalIdx uint16
}

func NewBFWriter() *BFWriter {
	return &BFWriter{}
}

func (writer *BFWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
	writer.writer = wr
	writer.block = block
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *BFWriter) Finalize() (*IndexMeta, error) {
	if writer.impl != nil {
		panic("formerly finalized filter not cleared yet")
	}
	sf, err := index.NewBinaryFuseFilter(writer.data)
	if err != nil {
		return nil, err
	}
	writer.impl = sf
	writer.data = nil

	appender := writer.writer
	meta := NewEmptyIndexMeta()
	meta.SetIndexType(StaticFilterIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.impl.Marshal()
	if err != nil {
		return nil, err
	}
	bf := objectio.NewBloomFilter(writer.colIdx, uint8(writer.cType), iBuf)
	rawSize := uint32(len(iBuf))
	compressed := common.Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)

	err = appender.WriteIndex(writer.block, bf)
	if err != nil {
		return nil, err
	}
	//meta.SetStartOffset(startOffset)
	writer.impl = nil
	return meta, nil
}

func (writer *BFWriter) AddValues(values containers.Vector) error {
	if writer.data == nil {
		writer.data = values
		return nil
	}
	if writer.data.GetType() != values.GetType() {
		return moerr.NewInternalError("wrong type")
	}
	writer.data.Extend(values)
	return nil
}

// Query is only used for testing or debugging
func (writer *BFWriter) Query(key any) (bool, error) {
	return writer.impl.MayContainsKey(key)
}
