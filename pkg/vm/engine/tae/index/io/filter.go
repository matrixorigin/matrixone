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

package io

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	gCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/basic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
)

type staticFilterIndexNode struct {
	*buffer.Node
	mgr base.INodeManager
	//host  dataio.IndexFile
	//meta  *common.IndexMeta
	host  gCommon.IVFile
	inner basic.StaticFilter
}

func newStaticFilterIndexNode(mgr base.INodeManager, host gCommon.IVFile, id *gCommon.ID) *staticFilterIndexNode {
	impl := new(staticFilterIndexNode)
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(host.Stat().Size()))
	impl.LoadFunc = impl.OnLoad
	impl.UnloadFunc = impl.OnUnload
	impl.DestroyFunc = impl.OnDestroy
	impl.host = host
	//impl.meta = meta
	impl.mgr = mgr
	mgr.RegisterNode(impl)
	return impl
}

func (n *staticFilterIndexNode) OnLoad() {
	if n.inner != nil {
		// no-op
		return
	}
	var err error
	//startOffset := n.meta.StartOffset
	stat := n.host.Stat()
	size := stat.Size()
	compressTyp := stat.CompressAlgo()
	data := make([]byte, size)
	if _, err := n.host.Read(data); err != nil {
		panic(err)
	}
	rawSize := stat.OriginSize()
	buf := make([]byte, rawSize)
	if err = common.Decompress(data, buf, common.CompressType(compressTyp)); err != nil {
	}
	n.inner, err = basic.NewBinaryFuseFilterFromSource(buf)
	if err != nil {
		panic(err)
	}
	return
}

func (n *staticFilterIndexNode) OnUnload() {
	if n.inner == nil {
		// no-op
		return
	}
	n.inner = nil
}

func (n *staticFilterIndexNode) OnDestroy() {
	n.host.Unref()
}

func (n *staticFilterIndexNode) Close() (err error) {
	if err = n.Node.Close(); err != nil {
		return err
	}
	n.inner = nil
	return nil
}

type StaticFilterIndexReader struct {
	inode *staticFilterIndexNode
}

func NewStaticFilterIndexReader() *StaticFilterIndexReader {
	return &StaticFilterIndexReader{}
}

func (reader *StaticFilterIndexReader) Init(mgr base.INodeManager, host gCommon.IVFile, id *gCommon.ID) error {
	reader.inode = newStaticFilterIndexNode(mgr, host, id)
	return nil
}

func (reader *StaticFilterIndexReader) Destroy() (err error) {
	if err = reader.inode.Close(); err != nil {
		return err
	}
	return nil
}

func (reader *StaticFilterIndexReader) MayContainsKey(key interface{}) (bool, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*staticFilterIndexNode).inner.MayContainsKey(key)
}

func (reader *StaticFilterIndexReader) MayContainsAnyKeys(keys *vector.Vector, visibility *roaring.Bitmap) (bool, *roaring.Bitmap, error) {
	handle := reader.inode.mgr.Pin(reader.inode)
	defer handle.Close()
	return handle.GetNode().(*staticFilterIndexNode).inner.MayContainsAnyKeys(keys, visibility)
}

type StaticFilterIndexWriter struct {
	cType       common.CompressType
	host        gCommon.IRWFile
	inner       basic.StaticFilter
	data        *vector.Vector
	colIdx      uint16
	internalIdx uint16
}

func NewStaticFilterIndexWriter() *StaticFilterIndexWriter {
	return &StaticFilterIndexWriter{}
}

func (writer *StaticFilterIndexWriter) Init(host gCommon.IRWFile, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
	writer.host = host
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *StaticFilterIndexWriter) Finalize() (*common.IndexMeta, error) {
	if writer.inner != nil {
		panic("formerly finalized filter not cleared yet")
	}
	sf, err := basic.NewBinaryFuseFilter(writer.data)
	if err != nil {
		return nil, err
	}
	writer.inner = sf
	writer.data = nil

	appender := writer.host
	meta := common.NewEmptyIndexMeta()
	meta.SetIndexType(common.StaticFilterIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.inner.Marshal()
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(iBuf))
	compressed := common.Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	_, err = appender.Write(compressed)
	if err != nil {
		return nil, err
	}
	//meta.SetStartOffset(startOffset)
	writer.inner = nil
	return meta, nil
}

func (writer *StaticFilterIndexWriter) AddValues(values *vector.Vector) error {
	if writer.data == nil {
		writer.data = values
		return nil
	}
	if writer.data.Typ != values.Typ {
		return errors.ErrTypeMismatch
	}
	if err := vector.Append(writer.data, values.Col); err != nil {
		return err
	}
	return nil
}

// Query is only used for testing or debugging
func (writer *StaticFilterIndexWriter) Query(key interface{}) (bool, error) {
	return writer.inner.MayContainsKey(key)
}
