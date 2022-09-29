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
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

const (
	ConstPinDuration = 5 * time.Second
)

func encodeColMetaKey(id *common.ID) string {
	return fmt.Sprintf("colMeta-%d-%d", id.BlockID, id.Idx)
}

func encodeColBfKey(id *common.ID) string {
	return fmt.Sprintf("colBf-%d-%d", id.BlockID, id.Idx)
}

// func encodeColDataKey(id *common.ID) string {
// 	return fmt.Sprintf("colData-%d-%d", id.BlockID, id.Idx)
// }

type columnMetaNode struct {
	*buffer.Node
	// data
	objectio.ColumnObject
	zonemap *index.ZoneMap
	typ     types.Type
	// used to load data
	col     file.ColumnBlock
	metaloc string
}

func newColumnMetaNode(mgr base.INodeManager, metaKey string, col file.ColumnBlock, metaloc string, typ types.Type) *columnMetaNode {
	node := &columnMetaNode{
		col:     col,
		metaloc: metaloc,
		typ:     typ,
	}
	_, ext, _ := blockio.DecodeMetaLoc(metaloc)
	baseNode := buffer.NewNode(node, mgr, metaKey, uint64(ext.OriginSize()))
	node.Node = baseNode
	node.LoadFunc = node.onLoad
	node.UnloadFunc = node.onUnLoad
	node.HardEvictableFunc = func() bool { return true }
	return node
}

func (n *columnMetaNode) onLoad() {
	if n.ColumnObject != nil {
		return
	}
	// Do IO, fetch columnData
	meta := n.col.GetDataObject(n.metaloc)
	n.ColumnObject = meta

	// deserialize zonemap
	zmData, err := meta.GetIndex(objectio.ZoneMapType)
	// TODOa: Error Handling?
	if err != nil {
		panic(err)
	}
	data := zmData.(*objectio.ZoneMap)
	n.zonemap = index.NewZoneMap(n.typ)
	err = n.zonemap.Unmarshal(data.GetData())
	if err != nil {
		panic(err)
	}
}

func (n *columnMetaNode) onUnLoad() {
	n.zonemap = nil
	n.ColumnObject = nil
}

type ZmReader struct {
	metaKey string
	typ     types.Type
	mgr     base.INodeManager
	col     file.ColumnBlock
	metaloc string
}

func newZmReader(mgr base.INodeManager, typ types.Type, id common.ID, col file.ColumnBlock, metaloc string) *ZmReader {
	return &ZmReader{
		metaKey: encodeColMetaKey(&id),
		typ:     typ,
		mgr:     mgr,
		col:     col,
		metaloc: metaloc,
	}
}

func (r *ZmReader) pin() (h base.INodeHandle, err error) {
	h, err = r.mgr.TryPinByKey(r.metaKey, ConstPinDuration)
	if err == base.ErrNotFound {
		// Ingnore duplicate node error. TODO NoSpaceError
		r.mgr.Add(newColumnMetaNode(r.mgr, r.metaKey, r.col, r.metaloc, r.typ))
		h, err = r.mgr.TryPinByKey(r.metaKey, ConstPinDuration)
	}
	return h, err
}

func (r *ZmReader) Contains(key any) bool {
	h, err := r.pin()
	if err != nil {
		// TODOa: Error Handling?
		return false
	}
	defer h.Close()
	zm := h.GetNode().(*columnMetaNode)
	return zm.zonemap.Contains(key)
}

func (r *ZmReader) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	h, err := r.pin()
	if err != nil {
		return
	}
	defer h.Close()
	zm := h.GetNode().(*columnMetaNode)
	return zm.zonemap.ContainsAny(keys)
}

func (r *ZmReader) Destroy() error { return nil }

type ZMWriter struct {
	cType       CompressType
	writer      objectio.Writer
	block       objectio.BlockObject
	zonemap     *index.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewZMWriter() *ZMWriter {
	return &ZMWriter{}
}

func (writer *ZMWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType CompressType, colIdx uint16, internalIdx uint16) error {
	writer.writer = wr
	writer.block = block
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *ZMWriter) Finalize() (*IndexMeta, error) {
	if writer.zonemap == nil {
		panic("unexpected error")
	}
	appender := writer.writer
	meta := NewEmptyIndexMeta()
	meta.SetIndexType(BlockZoneMapIndex)
	meta.SetCompressType(writer.cType)
	meta.SetIndexedColumn(writer.colIdx)
	meta.SetInternalIndex(writer.internalIdx)

	//var startOffset uint32
	iBuf, err := writer.zonemap.Marshal()
	if err != nil {
		return nil, err
	}
	zonemap, err := objectio.NewZoneMap(writer.colIdx, iBuf)
	if err != nil {
		return nil, err
	}
	rawSize := uint32(len(iBuf))
	compressed := Compress(iBuf, writer.cType)
	exactSize := uint32(len(compressed))
	meta.SetSize(rawSize, exactSize)
	err = appender.WriteIndex(writer.block, zonemap)
	if err != nil {
		return nil, err
	}
	//meta.SetStartOffset(startOffset)
	return meta, nil
}

func (writer *ZMWriter) AddValues(values containers.Vector) (err error) {
	typ := values.GetType()
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = moerr.NewInternalError("wrong type")
			return
		}
	}
	ctx := new(index.KeysCtx)
	ctx.Keys = values
	ctx.Count = values.Length()
	err = writer.zonemap.BatchUpdate(ctx)
	return
}

func (writer *ZMWriter) SetMinMax(min, max any, typ types.Type) (err error) {
	if writer.zonemap == nil {
		writer.zonemap = index.NewZoneMap(typ)
	} else {
		if writer.zonemap.GetType() != typ {
			err = moerr.NewInternalError("wrong type")
			return
		}
	}
	writer.zonemap.SetMin(min)
	writer.zonemap.SetMax(max)
	return
}
