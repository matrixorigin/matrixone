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

type ObjectReader struct {
	object *Object
	name   string
}

func NewObjectReader(name string, fs fileservice.FileService) (Reader, error) {
	reader := &ObjectReader{
		name:   name,
		object: NewObject(name, fs),
	}
	return reader, nil
}

func (r *ObjectReader) ReadMeta(extents []Extent, m *mpool.MPool) ([]BlockObject, error) {
	var err error
	if len(extents) == 0 {
		return nil, nil
	}
	metas := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, len(extents)),
	}
	for i, extent := range extents {
		metas.Entries[i] = fileservice.IOEntry{
			Offset: int64(extent.offset),
			Size:   int64(extent.originSize),
		}
	}
	err = r.allocData(metas.Entries, m)
	if err != nil {
		r.freeData(metas.Entries, m)
	}
	err = r.object.fs.Read(context.Background(), metas)
	if err != nil {
		r.freeData(metas.Entries, m)
		return nil, err
	}
	blocks := make([]BlockObject, len(extents))
	for i := range extents {
		blocks[i] = &Block{
			object: r.object,
			extent: extents[i],
		}
		err = blocks[i].(*Block).UnMarshalMeta(metas.Entries[i].Data)
		if err != nil {
			r.freeData(metas.Entries, m)
			return nil, err
		}
	}
	return blocks, err
}

func (r *ObjectReader) Read(extent Extent, idxs []uint16, m *mpool.MPool) (*fileservice.IOVector, error) {
	var err error
	extents := make([]Extent, 1)
	extents[0] = extent
	blocks, err := r.ReadMeta(extents, m)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, idx := range idxs {
		col := block.(*Block).columns[idx]
		entry := fileservice.IOEntry{
			Offset: int64(col.GetMeta().location.Offset()),
			Size:   int64(col.GetMeta().location.Length()),
		}
		data.Entries = append(data.Entries, entry)
	}
	err = r.allocData(data.Entries, m)
	if err != nil {
		r.freeData(data.Entries, m)
	}
	err = r.object.fs.Read(context.Background(), data)
	if err != nil {
		r.freeData(data.Entries, m)
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadIndex(extent Extent, idxs []uint16, typ IndexDataType, m *mpool.MPool) ([]IndexData, error) {
	var err error
	extents := make([]Extent, 1)
	extents[0] = extent
	blocks, err := r.ReadMeta(extents, m)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	indexes := make([]IndexData, 0)
	for _, idx := range idxs {
		col := block.(*Block).columns[idx]

		index, err := col.GetIndex(typ, m)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func (r *ObjectReader) freeData(Entries []fileservice.IOEntry, m *mpool.MPool) {
	if m != nil {
		for _, entry := range Entries {
			if entry.Data != nil {
				m.Free(entry.Data)
			}
		}
	}
}

func (r *ObjectReader) allocData(Entries []fileservice.IOEntry, m *mpool.MPool) (err error) {
	if m != nil {
		for _, entry := range Entries {
			entry.Data, err = m.Alloc(int(entry.Size))
			if err != nil {
				return
			}
		}
	}
	return nil
}
