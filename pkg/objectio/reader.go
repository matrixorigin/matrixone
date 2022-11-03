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

const ExtentTypeSize = 4 * 3
const ExtentsLength = 20
const FooterSize = 8 + 4

func NewObjectReader(name string, fs fileservice.FileService) (Reader, error) {
	reader := &ObjectReader{
		name:   name,
		object: NewObject(name, fs),
	}
	return reader, nil
}

func (r *ObjectReader) ReadMeta(ctx context.Context, extents []Extent, m *mpool.MPool) ([]BlockObject, error) {
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
	defer r.freeData(metas.Entries, m)
	if err != nil {
		return nil, err
	}
	err = r.object.fs.Read(ctx, metas)
	if err != nil {
		return nil, err
	}
	blocks := make([]BlockObject, len(extents))
	for i := range extents {
		blocks[i] = &Block{
			object: r.object,
			extent: extents[i],
			name:   r.name,
		}
		err = blocks[i].(*Block).UnMarshalMeta(metas.Entries[i].Data)
		if err != nil {
			return nil, err
		}
	}
	return blocks, err
}

func (r *ObjectReader) Read(ctx context.Context, extent Extent, idxs []uint16, m *mpool.MPool) (*fileservice.IOVector, error) {
	var err error
	extents := make([]Extent, 1)
	extents[0] = extent
	blocks, err := r.ReadMeta(ctx, extents, m)
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
		return nil, err
	}
	err = r.object.fs.Read(ctx, data)
	if err != nil {
		r.freeData(data.Entries, m)
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadIndex(ctx context.Context, extent Extent, idxs []uint16, typ IndexDataType, m *mpool.MPool) ([]IndexData, error) {
	var err error
	extents := make([]Extent, 1)
	extents[0] = extent
	blocks, err := r.ReadMeta(ctx, extents, m)
	if err != nil {
		return nil, err
	}
	block := blocks[0]
	indexes := make([]IndexData, 0)
	for _, idx := range idxs {
		col := block.(*Block).columns[idx]

		index, err := col.GetIndex(ctx, typ, m)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, index)
	}
	return indexes, nil
}

func (r *ObjectReader) ReadAllMeta(ctx context.Context, fileSize int64, m *mpool.MPool) ([]BlockObject, error) {
	footer, err := r.readFooter(ctx, fileSize, m)
	if err != nil {
		return nil, err
	}
	return r.ReadMeta(ctx, footer.extents, m)
}

func (r *ObjectReader) readFooter(ctx context.Context, fileSize int64, m *mpool.MPool) (*Footer, error) {
	var err error
	var footer *Footer

	// I don't know how many blocks there are in the object,
	// read "ExtentsLength" blocks by default
	size := int64(FooterSize + ExtentsLength*ExtentTypeSize)
	if size > fileSize {
		size = fileSize
	}
	footer, err = r.readFooterAndUnMarshal(ctx, fileSize, size, m)
	if err != nil {
		return nil, err
	}
	if len(footer.extents) == 0 {
		size = int64(FooterSize + footer.blockCount*ExtentTypeSize)
		footer, err = r.readFooterAndUnMarshal(ctx, fileSize, size, m)
		if err != nil {
			return nil, err
		}
	}

	return footer, nil
}

func (r *ObjectReader) readFooterAndUnMarshal(ctx context.Context, fileSize, size int64, m *mpool.MPool) (*Footer, error) {
	var err error
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	data.Entries[0] = fileservice.IOEntry{
		Offset: fileSize - size,
		Size:   size,
	}
	err = r.allocData(data.Entries, m)
	if err != nil {
		return nil, err
	}
	defer r.freeData(data.Entries, m)
	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	footer := &Footer{}
	err = footer.UnMarshalFooter(data.Entries[0].Data)
	if err != nil {
		return nil, err
	}
	return footer, err
}

func (r *ObjectReader) freeData(Entries []fileservice.IOEntry, m *mpool.MPool) {
	if m != nil {
		for i := range Entries {
			if Entries[i].Data != nil {
				m.Free(Entries[i].Data)
			}
		}
	}
}

func (r *ObjectReader) allocData(Entries []fileservice.IOEntry, m *mpool.MPool) (err error) {
	if m != nil {
		for i := range Entries {
			Entries[i].Data, err = m.Alloc(int(Entries[i].Size))
			if err != nil {
				return
			}
		}
	}
	return nil
}
