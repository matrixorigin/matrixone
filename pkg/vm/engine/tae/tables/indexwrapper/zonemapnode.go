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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"sync"
)

type ZMReader struct {
	sync.Mutex
	file    objectio.ColumnObject
	zonemap *index.ZoneMap
	dataTyp types.Type
}

func NewZMReader(file objectio.ColumnObject, typ types.Type) *ZMReader {
	return &ZMReader{
		file:    file,
		dataTyp: typ,
	}
}

func (reader *ZMReader) Destroy() (err error) {
	reader.Lock()
	defer reader.Unlock()
	reader.zonemap = nil
	return nil
}

func (reader *ZMReader) readIndex() {
	reader.Lock()
	defer reader.Unlock()
	if reader.zonemap != nil {
		// no-op
		return
	}
	fsData, err := reader.file.GetIndex(objectio.ZoneMapType)
	if err != nil {
		panic(err)
	}
	data := fsData.(*objectio.ZoneMap)
	reader.zonemap = index.NewZoneMap(reader.dataTyp)
	err = reader.zonemap.Unmarshal(data.GetData())
	if err != nil {
		panic(err)
	}
}

func (reader *ZMReader) ContainsAny(keys containers.Vector) (visibility *roaring.Bitmap, ok bool) {
	reader.readIndex()
	return reader.zonemap.ContainsAny(keys)
}

func (reader *ZMReader) Contains(key any) bool {
	reader.readIndex()
	return reader.zonemap.Contains(key)
}

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
