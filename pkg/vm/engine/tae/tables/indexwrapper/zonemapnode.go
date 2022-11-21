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

type ZmReader struct {
	metaKey        string
	mgr            base.INodeManager
	colMetaFactory evictable.EvictableNodeFactory
}

func newZmReader(mgr base.INodeManager, typ types.Type, id common.ID, fs *objectio.ObjectFS, idx uint16, metaloc string) *ZmReader {
	metaKey := evictable.EncodeColMetaKey(id.Idx, metaloc)
	return &ZmReader{
		metaKey: metaKey,
		mgr:     mgr,
		colMetaFactory: func() (base.INode, error) {
			return evictable.NewColumnMetaNode(
				idx,
				typ,
				metaloc,
				metaKey,
				mgr,
				fs), nil
		},
	}
}

func (r *ZmReader) Contains(key any) bool {
	// TODOa: new zmreader if metaloc changed, replayIndex is not right?
	h, err := evictable.PinEvictableNode(r.mgr, r.metaKey, r.colMetaFactory)
	if err != nil {
		// TODOa: Error Handling?
		return false
	}
	defer h.Close()
	node := h.GetNode().(*evictable.ColumnMetaNode)
	return node.Zonemap.Contains(key)
}

func (r *ZmReader) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	h, err := evictable.PinEvictableNode(r.mgr, r.metaKey, r.colMetaFactory)
	if err != nil {
		return
	}
	defer h.Close()
	node := h.GetNode().(*evictable.ColumnMetaNode)
	return node.Zonemap.ContainsAny(keys)
}

func (r *ZmReader) Destroy() error { return nil }

type ZMWriter struct {
	cType       common.CompressType
	writer      objectio.Writer
	block       objectio.BlockObject
	zonemap     *index.ZoneMap
	colIdx      uint16
	internalIdx uint16
}

func NewZMWriter() *ZMWriter {
	return &ZMWriter{}
}

func (writer *ZMWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
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
	compressed := common.Compress(iBuf, writer.cType)
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
