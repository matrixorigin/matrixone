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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type bloomFilterNode struct {
	*buffer.Node
	mgr  base.INodeManager
	file common.IVFile
	impl index.StaticFilter
}

func newBloomFilterNode(mgr base.INodeManager, file common.IVFile, id *common.ID) *bloomFilterNode {
	impl := new(bloomFilterNode)
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(file.Stat().Size()))
	impl.LoadFunc = impl.OnLoad
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestroy
	impl.file = file
	//impl.meta = meta
	impl.mgr = mgr
	mgr.RegisterNode(impl)
	return impl
}

func (n *bloomFilterNode) OnLoad() {
	if n.impl != nil {
		// no-op
		return
	}
	var err error
	//startOffset := n.meta.StartOffset
	stat := n.file.Stat()
	size := stat.Size()
	compressTyp := stat.CompressAlgo()
	data := make([]byte, size)
	if _, err := n.file.Read(data); err != nil {
		panic(err)
	}
	rawSize := stat.OriginSize()
	buf := make([]byte, rawSize)
	if err = Decompress(data, buf, CompressType(compressTyp)); err != nil {
		panic(err)
	}
	n.impl, err = index.NewBinaryFuseFilterFromSource(buf)
	if err != nil {
		panic(err)
	}
}

func (n *bloomFilterNode) OnUnload() {
	if n.impl == nil {
		// no-op
		return
	}
	n.impl = nil
}

func (n *bloomFilterNode) OnDestroy() {
	n.file.Unref()
}

func (n *bloomFilterNode) Close() (err error) {
	if err = n.Node.Close(); err != nil {
		return err
	}
	n.impl = nil
	return nil
}

type BFReader struct {
	node *bloomFilterNode
}

func NewBFReader(mgr base.INodeManager, file common.IVFile, id *common.ID) *BFReader {
	return &BFReader{
		node: newBloomFilterNode(mgr, file, id),
	}
}

func (reader *BFReader) Destroy() (err error) {
	if err = reader.node.Close(); err != nil {
		return err
	}
	return nil
}

func (reader *BFReader) MayContainsKey(key any) (bool, error) {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.impl.MayContainsKey(key)
}

func (reader *BFReader) MayContainsAnyKeys(keys containers.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error) {
	handle := reader.node.mgr.Pin(reader.node)
	defer handle.Close()
	return reader.node.impl.MayContainsAnyKeys(keys, visibility)
}

type BFWriter struct {
	cType       CompressType
	file        common.IRWFile
	impl        index.StaticFilter
	data        containers.Vector
	colIdx      uint16
	internalIdx uint16
}

func NewBFWriter() *BFWriter {
	return &BFWriter{}
}

func (writer *BFWriter) Init(file common.IRWFile, cType CompressType, colIdx uint16, internalIdx uint16) error {
	writer.file = file
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

	appender := writer.file
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
	rawSize := uint32(len(iBuf))
	compressed := Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	_, err = appender.Write(compressed)
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
		return data.ErrWrongType
	}
	writer.data.Extend(values)
	return nil
}

// Query is only used for testing or debugging
func (writer *BFWriter) Query(key any) (bool, error) {
	return writer.impl.MayContainsKey(key)
}
