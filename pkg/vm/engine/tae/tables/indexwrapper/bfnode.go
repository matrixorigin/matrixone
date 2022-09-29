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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type bfNode struct {
	*buffer.Node

	bf index.StaticFilter

	metaKey, bfKey string
	typ            types.Type
	mgr            base.INodeManager
	col            file.ColumnBlock
	metaloc        string
}

func newBfNode(mgr base.INodeManager, bfKey, metaKey string, col file.ColumnBlock, metaloc string, typ types.Type) (node *bfNode, err error) {
	node = &bfNode{
		bfKey:   bfKey,
		metaKey: metaKey,
		typ:     typ,
		mgr:     mgr,
		col:     col,
		metaloc: metaloc,
	}

	h, err := node.pinMeta()
	if err != nil {
		return
	}
	defer h.Close()
	meta := h.GetNode().(*columnMetaNode)
	size := meta.GetMeta().GetBloomFilter().OriginSize()
	node.Node = buffer.NewNode(node, mgr, bfKey, uint64(size))
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnload
	node.HardEvictableFunc = func() bool { return true }
	return
}

func (n *bfNode) pinMeta() (base.INodeHandle, error) {
	var h base.INodeHandle
	var err error
	h, err = n.mgr.TryPinByKey(n.metaKey, ConstPinDuration)
	if err == base.ErrNotFound {
		n.mgr.Add(newColumnMetaNode(n.mgr, n.metaKey, n.col, n.metaloc, n.typ))
		h, err = n.mgr.TryPinByKey(n.metaKey, ConstPinDuration)
	}
	return h, err
}

func (n *bfNode) onLoad() {
	var h base.INodeHandle
	var err error
	h, err = n.pinMeta()
	if err != nil {
		panic(err)
	}
	metaNode := h.GetNode().(*columnMetaNode)
	h.Close()

	stat := metaNode.GetMeta()
	compressTyp := stat.GetAlg()
	// Do IO, fetch bloomfilter buf
	fsData, err := metaNode.GetIndex(objectio.BloomFilterType)
	if err != nil {
		panic(err)
	}
	rawSize := stat.GetBloomFilter().OriginSize()
	buf := make([]byte, rawSize)
	data := fsData.(*objectio.BloomFilter).GetData()
	if err = Decompress(data, buf, CompressType(compressTyp)); err != nil {
		panic(err)
	}
	n.bf, err = index.NewBinaryFuseFilterFromSource(buf)
	if err != nil {
		panic(err)
	}
}

func (n *bfNode) onUnload() {
	n.bf = nil
}

type BfReader struct {
	metaKey, bfKey string
	typ            types.Type
	mgr            base.INodeManager
	col            file.ColumnBlock
	metaloc        string
}

func newBfReader(mgr base.INodeManager, typ types.Type, id common.ID, col file.ColumnBlock, metaloc string) *BfReader {
	return &BfReader{
		metaKey: encodeColMetaKey(&id),
		bfKey:   encodeColBfKey(&id),
		mgr:     mgr,
		col:     col,
		metaloc: metaloc,
	}
}

func (r *BfReader) pin() (h base.INodeHandle, err error) {
	h, err = r.mgr.TryPinByKey(r.bfKey, ConstPinDuration)
	if err == base.ErrNotFound {
		bfNode, newerr := newBfNode(r.mgr, r.bfKey, r.metaKey, r.col, r.metaloc, r.typ)
		if newerr != nil {
			return nil, newerr
		}
		r.mgr.Add(bfNode)
		h, err = r.mgr.TryPinByKey(r.bfKey, ConstPinDuration)
	}
	return h, err
}

func (r *BfReader) MayContainsKey(key any) (b bool, err error) {
	h, err := r.pin()
	if err != nil {
		// TODOa: Error Handling?
		return
	}
	defer h.Close()
	bfNode := h.GetNode().(*bfNode)
	return bfNode.bf.MayContainsKey(key)
}

func (r *BfReader) MayContainsAnyKeys(keys containers.Vector, visibility *roaring.Bitmap) (b bool, m *roaring.Bitmap, err error) {
	h, err := r.pin()
	if err != nil {
		// TODOa: Error Handling?
		return
	}
	defer h.Close()
	bfNode := h.GetNode().(*bfNode)
	return bfNode.bf.MayContainsAnyKeys(keys, visibility)
}

func (r *BfReader) Destroy() error { return nil }

type BFWriter struct {
	cType       CompressType
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

func (writer *BFWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType CompressType, colIdx uint16, internalIdx uint16) error {
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
	compressed := Compress(iBuf, writer.cType)
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
