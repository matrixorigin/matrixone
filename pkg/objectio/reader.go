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

func (r *ObjectReader) ReadMeta(ctx context.Context,
	extents []Extent, m *mpool.MPool, ZMUnmarshalFunc ZoneMapUnmarshalFunc) ([]BlockObject, error) {
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
					id:     i,
					object: r.object,
					extent: extent,
					name:   r.name,
				}
				cache := data[size:dataLen]
				unSize, err := block.UnMarshalMeta(cache, ZMUnmarshalFunc)
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

	blockLen := len(metas.Entries[0].Object.([]*Block))
	blocks := make([]BlockObject, blockLen)
	for i := range metas.Entries[0].Object.([]*Block) {
		blocks[i] = metas.Entries[0].Object.([]*Block)[i]
	}
	return blocks, err
}

func (r *ObjectReader) Read(ctx context.Context,
	extent Extent, idxs []uint16, ids []uint32, m *mpool.MPool,
	zoneMapFunc ZoneMapUnmarshalFunc,
	readFunc ReadObjectFunc) (*fileservice.IOVector, error) {
	blocks, err := r.ReadMeta(ctx, []Extent{extent}, m, zoneMapFunc)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		ids = make([]uint32, len(blocks))
		for i := range ids {
			ids[i] = uint32(i)
		}
	}
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0, len(idxs)*len(ids)),
	}
	for _, id := range ids {
		for _, idx := range idxs {
			col := blocks[id].(*Block).columns[idx]
			data.Entries = append(data.Entries, fileservice.IOEntry{
				Offset: int64(col.GetMeta().location.Offset()),
				Size:   int64(col.GetMeta().location.Length()),

				ToObject: readFunc(int64(col.GetMeta().location.OriginSize())),
			})
		}
	}

	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadAllMeta(ctx context.Context,
	fileSize int64, m *mpool.MPool, ZMUnmarshalFunc ZoneMapUnmarshalFunc) ([]BlockObject, error) {
	footer, err := r.readFooter(ctx, fileSize, m)
	if err != nil {
		return nil, err
	}
	return r.ReadMeta(ctx, footer.extents, m, ZMUnmarshalFunc)
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
type ReadObjectFunc = func(size int64) ToObjectFunc

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
