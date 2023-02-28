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

package dataio

import (
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ZmReader struct {
	metaKey string
	idx     uint16
	reader  *blockio.Reader
}

func newZmReader(mgr base.INodeManager, typ types.Type, id common.ID, fs *objectio.ObjectFS, idx uint16, metaloc string) *ZmReader {
	reader, _ := blockio.NewReader(context.Background(), fs, metaloc)
	return &ZmReader{
		metaKey: metaloc,
		idx:     idx,
		reader:  reader,
	}
}

func (r *ZmReader) getZoneMap() (*index.ZoneMap, error) {
	_, extent, _ := blockio.DecodeMetaLoc(r.metaKey)
	zmList, err := r.reader.LoadZoneMapByExtent(context.Background(), []uint16{r.idx}, extent, nil)
	if err != nil {
		// TODOa: Error Handling?
		return nil, err
	}
	return zmList[0], err
}

func (r *ZmReader) Contains(key any) bool {
	zm, err := r.getZoneMap()
	if err != nil {
		// TODOa: Error Handling?
		return false
	}
	return zm.Contains(key)
}

func (r *ZmReader) FastContainsAny(keys containers.Vector) (ok bool) {
	zm, err := r.getZoneMap()
	if err != nil {
		// TODOa: Error Handling?
		return false
	}
	return zm.FastContainsAny(keys)
}

func (r *ZmReader) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	zm, err := r.getZoneMap()
	if err != nil {
		// TODOa: Error Handling?
		return
	}
	return zm.ContainsAny(keys)
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

func (writer *ZMWriter) String() string {
	return fmt.Sprintf("ZmWriter[Cid-%d,%s]", writer.colIdx, writer.zonemap.String())
}

func (writer *ZMWriter) Init(wr objectio.Writer, block objectio.BlockObject, cType common.CompressType, colIdx uint16, internalIdx uint16) error {
	writer.writer = wr
	writer.block = block
	writer.cType = cType
	writer.colIdx = colIdx
	writer.internalIdx = internalIdx
	return nil
}

func (writer *ZMWriter) Finalize() (*indexwrapper.IndexMeta, error) {
	if writer.zonemap == nil {
		panic(any("unexpected error"))
	}
	appender := writer.writer
	meta := indexwrapper.NewEmptyIndexMeta()
	meta.SetIndexType(indexwrapper.BlockZoneMapIndex)
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
			err = moerr.NewInternalErrorNoCtx("wrong type")
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
			err = moerr.NewInternalErrorNoCtx("wrong type")
			return
		}
	}
	writer.zonemap.SetMin(min)
	writer.zonemap.SetMax(max)
	return
}
