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
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"

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
	l := len(extents)
	if l == 0 {
		return nil, nil
	}

	metas := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 1, l),
	}

	metas.Entries[0] = fileservice.IOEntry{
		Offset: int64(extents[0].offset),
		Size:   int64(extents[0].originSize),

		ToObject: func(reader io.Reader, data []byte) (any, int64, error) {
			if len(data) == 0 {
				var err error
				data, err = io.ReadAll(reader)
				if err != nil {
					return nil, 0, err
				}
			}
			dataLen := len(data)
			blocks := make([]*Block, 0)
			size := uint32(0)
			i := uint32(0)
			for {
				if size == uint32(dataLen) {
					break
				}
				extent := Extent{
					id:         i,
					offset:     extents[0].offset,
					length:     extents[0].length,
					originSize: extents[0].originSize,
				}
				block := &Block{
					object: r.object,
					extent: extent,
					name:   r.name,
				}
				cache := data[size:dataLen]
				unSize, err := block.UnMarshalMeta(cache)
				if err != nil {
					logutil.Infof("UnMarshalMeta failed: %v, extent %v", err.Error(), extents[0])
					return nil, 0, err
				}
				i++
				size += unSize
				blocks = append(blocks, block)
			}
			return blocks, int64(len(data)), nil
		},
	}

	err := r.object.fs.Read(ctx, metas)
	if err != nil {
		return nil, err
	}

	blocks := make([]BlockObject, 0, l)

	for _, extent := range extents {
		block := metas.Entries[0].Object.([]*Block)[extent.id]
		blocks = append(blocks, block)
	}
	return blocks, err
}

func (r *ObjectReader) Read(ctx context.Context, extent Extent, idxs []uint16, m *mpool.MPool) (*fileservice.IOVector, error) {
	blocks, err := r.ReadMeta(ctx, []Extent{extent}, m)
	if err != nil {
		return nil, err
	}
	block := blocks[0]

	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0, len(idxs)),
	}
	for _, idx := range idxs {
		col := block.(*Block).columns[idx]
		data.Entries = append(data.Entries, fileservice.IOEntry{
			Offset: int64(col.GetMeta().location.Offset()),
			Size:   int64(col.GetMeta().location.Length()),

			ToObject: newDecompressToObject(int64(col.GetMeta().location.OriginSize())),
		})
	}

	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadIndex(ctx context.Context, extent Extent, idxs []uint16, typ IndexDataType, m *mpool.MPool) ([]IndexData, error) {
	blocks, err := r.ReadMeta(ctx, []Extent{extent}, m)
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
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries: []fileservice.IOEntry{
			{
				Offset: fileSize - size,
				Size:   size,

				ToObject: func(reader io.Reader, data []byte) (any, int64, error) {
					// unmarshal
					if len(data) == 0 {
						var err error
						data, err = io.ReadAll(reader)
						if err != nil {
							return nil, 0, err
						}
					}
					footer := &Footer{}
					if err := footer.UnMarshalFooter(data); err != nil {
						return nil, 0, err
					}
					return footer, int64(len(data)), nil
				},
			},
		},
	}
	err := r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}

	return data.Entries[0].Object.(*Footer), nil
}

type ToObjectFunc = func(r io.Reader, buf []byte) (any, int64, error)

// newDecompressToObject the decompression function passed to fileservice
func newDecompressToObject(size int64) ToObjectFunc {
	return func(reader io.Reader, data []byte) (any, int64, error) {
		// decompress
		var err error
		if len(data) == 0 {
			data, err = io.ReadAll(reader)
			if err != nil {
				return nil, 0, err
			}
		}
		decompressed := make([]byte, size)
		decompressed, err = compress.Decompress(data, decompressed, compress.Lz4)
		if err != nil {
			return nil, 0, err
		}
		return decompressed, int64(len(decompressed)), nil
	}
}
