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
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

// ColumnBlock is the organizational structure of a vector in objectio
// One batch can be written at a time, and a batch can contain multiple vectors.
// It is a child of the block
type ColumnBlock struct {
	// meta is the metadata of the ColumnBlock,
	// such as index, data location, compression algorithm...
	meta ColumnMeta

	// object is the block.object
	object *Object
}

func NewColumnBlock(meta ColumnMeta, object *Object) *ColumnBlock {
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
		Offset: int64(cb.meta.Location().Offset()),
		Size:   int64(cb.meta.Location().Length()),
	}
	data.Entries[0].ToObject = newDecompressToObject(int64(cb.meta.Location().OriginSize()))
	err = cb.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (cb *ColumnBlock) GetIndex(ctx context.Context, dataType IndexDataType, readFunc ReadObjectFunc, m *mpool.MPool) (IndexData, error) {
	if dataType == ZoneMapType {
		return NewZoneMap(cb.meta.ZoneMap().data)
	} else if dataType == BloomFilterType {
		data := &fileservice.IOVector{
			FilePath: cb.object.name,
			Entries:  make([]fileservice.IOEntry, 1),
		}
		data.Entries[0] = fileservice.IOEntry{
			Offset: int64(cb.meta.BloomFilter().Offset()),
			Size:   int64(cb.meta.BloomFilter().Length()),
		}
		var err error
		data.Entries[0].ToObject = readFunc(int64(cb.meta.BloomFilter().OriginSize()))
		err = cb.object.fs.Read(ctx, data)
		if err != nil {
			return nil, err
		}
		return NewBloomFilter(0, data.Entries[0].Object), nil
	}
	return nil, nil
}

func (cb *ColumnBlock) GetMeta() ColumnMeta {
	return cb.meta
}

func (cb *ColumnBlock) MarshalMeta() []byte {
	return cb.meta
}

func (cb *ColumnBlock) UnmarshalMate(data []byte) error {
	return nil
}
