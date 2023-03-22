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

package objectio

import (
	"bytes"
	"context"
	"encoding/binary"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// ColumnBlock is the organizational structure of a vector in objectio
// One batch can be written at a time, and a batch can contain multiple vectors.
// It is a child of the block
type ColumnBlock struct {
	// meta is the metadata of the ColumnBlock,
	// such as index, data location, compression algorithm...
	meta *ColumnMeta

	// object is the block.object
	object *Object
}

func NewColumnBlock(idx uint16, object *Object) ColumnObject {
	meta := &ColumnMeta{
		idx:         idx,
		zoneMap:     ZoneMap{},
		bloomFilter: Extent{},
	}
	col := &ColumnBlock{
		object: object,
		meta:   meta,
	}
	return col
}

func (cb *ColumnBlock) GetData(ctx context.Context, m *mpool.MPool) (*fileservice.IOVector, error) {
	var err error
	data := &fileservice.IOVector{
		FilePath: cb.object.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	data.Entries[0] = fileservice.IOEntry{
		Offset: int64(cb.meta.location.Offset()),
		Size:   int64(cb.meta.location.Length()),
	}
	data.Entries[0].ToObject = newDecompressToObject(int64(cb.meta.location.OriginSize()))
	err = cb.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cb *ColumnBlock) GetIndex(ctx context.Context, dataType IndexDataType, readFunc ReadObjectFunc, m *mpool.MPool) (IndexData, error) {
	if dataType == ZoneMapType {
		return &cb.meta.zoneMap, nil
	} else if dataType == BloomFilterType {
		data := &fileservice.IOVector{
			FilePath: cb.object.name,
			Entries:  make([]fileservice.IOEntry, 1),
		}
		data.Entries[0] = fileservice.IOEntry{
			Offset: int64(cb.meta.bloomFilter.Offset()),
			Size:   int64(cb.meta.bloomFilter.Length()),
		}
		var err error
		data.Entries[0].ToObject = readFunc(int64(cb.meta.bloomFilter.OriginSize()))
		err = cb.object.fs.Read(ctx, data)
		if err != nil {
			return nil, err
		}
		return NewBloomFilter(cb.meta.idx, 0, data.Entries[0].Object), nil
	}
	return nil, nil
}

func (cb *ColumnBlock) GetMeta() *ColumnMeta {
	return cb.meta
}

func (cb *ColumnBlock) MarshalMeta() ([]byte, error) {
	var (
		err    error
		buffer bytes.Buffer
	)
	if err = binary.Write(&buffer, endian, cb.meta.typ); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.alg); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.idx); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.location.Offset()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.location.Length()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.location.OriginSize()); err != nil {
		return nil, err
	}
	var buf []byte
	if cb.meta.zoneMap.data == nil {
		buf = make([]byte, ZoneMapMinSize+ZoneMapMaxSize)
		cb.meta.zoneMap.data = buf
	}
	if err = binary.Write(&buffer, endian, cb.meta.zoneMap.data.([]byte)); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.bloomFilter.Offset()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.bloomFilter.Length()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.bloomFilter.OriginSize()); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, cb.meta.dummy); err != nil {
		return nil, err
	}
	if err = binary.Write(&buffer, endian, uint32(0)); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (cb *ColumnBlock) UnMarshalMate(cache *bytes.Buffer) error {
	var err error
	if err = binary.Read(cache, endian, &cb.meta.typ); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.alg); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.idx); err != nil {
		return err
	}
	cb.meta.location = Extent{}
	if err = binary.Read(cache, endian, &cb.meta.location.offset); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.location.length); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.location.originSize); err != nil {
		return err
	}
	buf := make([]byte, ZoneMapMinSize+ZoneMapMaxSize)
	cb.meta.zoneMap.idx = cb.meta.idx
	if err = binary.Read(cache, endian, &buf); err != nil {
		return err
	}
	t := types.T(cb.meta.typ).ToType()
	if err = cb.meta.zoneMap.Unmarshal(buf, t); err != nil {
		return err
	}
	cb.meta.bloomFilter = Extent{}
	if err = binary.Read(cache, endian, &cb.meta.bloomFilter.offset); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.bloomFilter.length); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.bloomFilter.originSize); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.dummy); err != nil {
		return err
	}
	if err = binary.Read(cache, endian, &cb.meta.checksum); err != nil {
		return err
	}
	return err
}
