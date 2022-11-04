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
	"github.com/matrixorigin/matrixone/pkg/compress"
	"io"

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
		metas.Entries[i].Data, err = allocData(metas.Entries[i].Size, m)
		if err != nil {
			r.freeDataEntries(metas.Entries, m)
			return nil, err
		}
		metas.Entries[i].ToObject = newToObject
	}
	defer r.freeDataEntries(metas.Entries, m)
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
		err = blocks[i].(*Block).UnMarshalMeta(metas.Entries[i].Object.([]byte))
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
	objects := make([][]byte, len(idxs))
	for i, idx := range idxs {
		col := block.(*Block).columns[idx]
		entry := fileservice.IOEntry{
			Offset: int64(col.GetMeta().location.Offset()),
			Size:   int64(col.GetMeta().location.Length()),
		}
		// IOEntry.Data needs to allocate memory in advance
		entry.Data, err = allocData(int64(col.GetMeta().location.Length()), m)
		if err != nil {
			r.freeDataEntries(data.Entries, m)
			r.freeObjects(objects, m)
			return nil, err
		}
		// object needs to allocate memory inside ToObject,
		// because ToObject will not be called if the object is cached
		entry.ToObject = newDecompressToObject(objects[i], int64(col.GetMeta().location.OriginSize()), m)
		data.Entries = append(data.Entries, entry)
	}
	// Temp data needs to be free
	defer r.freeDataEntries(data.Entries, m)
	err = r.object.fs.Read(ctx, data)
	if err != nil {
		// If the read fails, you need to release the memory allocated by ToObject to the object
		r.freeObjects(objects, m)
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
	data.Entries[0].Data, err = allocData(data.Entries[0].Size, m)
	if err != nil {
		return nil, err
	}
	defer r.freeDataEntries(data.Entries, m)
	data.Entries[0].ToObject = newToObject
	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	footer := &Footer{}
	err = footer.UnMarshalFooter(data.Entries[0].Object.([]byte))
	if err != nil {
		return nil, err
	}
	return footer, err
}

type ToObjectFunc = func(r io.Reader, buf []byte) (any, int64, error)

// newDecompressToObject the decompression function passed to fileservice
func newDecompressToObject(object []byte, size int64, m *mpool.MPool) ToObjectFunc {
	return func(read io.Reader, buf []byte) (any, int64, error) {
		var err error
		var data []byte
		data = buf
		if len(buf) == 0 {
			data, err = io.ReadAll(read)
			if err != nil {
				return nil, 0, err
			}
		}
		object, err = allocData(size, m)
		if err != nil {
			return nil, 0, err
		}
		object, err = compress.Decompress(data, object, compress.Lz4)
		if err != nil {
			freeData(buf, m)
			return nil, 0, err
		}
		return object, int64(len(object)), nil
	}
}

// newToObject Because the caller needs to use IOEntry.Object,
// it needs to be passed to the fileservice ToObject function
func newToObject(read io.Reader, buf []byte) (any, int64, error) {
	var err error
	if len(buf) > 0 {
		return buf, int64(len(buf)), err
	}
	data, err := io.ReadAll(read)
	return data, int64(len(data)), err
}

// freeDataEntries free all IOEntry.Data in Entries
func (r *ObjectReader) freeDataEntries(Entries []fileservice.IOEntry, m *mpool.MPool) {
	if m != nil {
		for i := range Entries {
			if len(Entries[i].Data) > 0 {
				m.Free(Entries[i].Data)
			}
		}
	}
}

// freeObjectEntries free all IOEntry.Object in objects
func (r *ObjectReader) freeObjects(objects [][]byte, m *mpool.MPool) {
	if m != nil {
		for i := range objects {
			if len(objects[i]) > 0 {
				m.Free(objects[i])
			}
		}
	}
}

func allocData(size int64, m *mpool.MPool) (data []byte, err error) {
	if m != nil {
		data, err = m.Alloc(int(size))
		if err != nil {
			return
		}
	} else {
		// Because the external caller may not use mpool
		data = make([]byte, size)
	}
	return data, nil
}

func freeData(buf []byte, m *mpool.MPool) {
	if m != nil {
		m.Free(buf)
	}
}
