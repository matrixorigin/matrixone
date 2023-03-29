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
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectReader struct {
	object *Object
	name   string
	ReaderOptions
}

func NewObjectReader(name string, fs fileservice.FileService, opts ...ReaderOptionFunc) (Reader, error) {
	reader := &ObjectReader{
		name:   name,
		object: NewObject(name, fs),
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func (r *ObjectReader) ReadMeta(ctx context.Context,
	extents []Extent, m *mpool.MPool, ZMUnmarshalFunc ZoneMapUnmarshalFunc) (*ObjectMeta, error) {
	l := len(extents)
	if l == 0 {
		return nil, nil
	}

	metas := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 1, l),
		NoCache:  r.noCache,
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

			meta := &ObjectMeta{}

			meta.Rows = types.DecodeUint32(data[:4])
			blkMetaSize := types.DecodeUint32(data[4:8])
			data = data[8:]
			blkMeta := data[:blkMetaSize]
			colMeta := data[blkMetaSize:]

			dataLen := len(blkMeta)
			blocks := make([]BlockObject, 0)
			size := 0
			i := 0
			// decode block meta
			for {
				if size == dataLen {
					break
				}
				extent := Extent{
					id:         uint32(i),
					offset:     extents[0].offset,
					length:     extents[0].length,
					originSize: extents[0].originSize,
				}
				block := &Block{
					id:     uint32(i),
					object: r.object,
					extent: extent,
					name:   r.name,
				}
				cache := blkMeta[size:]
				unSize, err := block.UnmarshalMeta(cache, ZMUnmarshalFunc)
				if err != nil {
					logutil.Infof("UnMarshalMeta failed: %v, extent %v", err.Error(), extents[0])
					return nil, 0, err
				}
				i++
				size += int(unSize)
				blocks = append(blocks, block)
			}

			meta.BlkMetas = blocks

			// decode column meta
			cols := make([]ObjectColumnMeta, 0)
			i = 0

			for len(colMeta) != 0 {
				col := ObjectColumnMeta{}
				if err := col.Read(colMeta); err != nil {
					return nil, 0, err
				}
				// zonemap to object
				col.Zonemap.idx = uint16(i)
				col.Zonemap.unmarshalFunc = ZMUnmarshalFunc
				coldef, err := blocks[0].GetColumn(uint16(i))
				if err != nil {
					return nil, 0, err
				}
				if err = col.Zonemap.Unmarshal(col.Zonemap.data.([]byte), types.T(coldef.GetMeta().typ).ToType()); err != nil {
					return nil, 0, err
				}
				i++
				colMeta = colMeta[ObjectColumnMetaSize:]
				cols = append(cols, col)
			}

			meta.ColMetas = cols

			return meta, int64(len(data)), nil
		},
	}

	err := r.object.fs.Read(ctx, metas)
	if err != nil {
		return nil, err
	}

	meta := metas.Entries[0].Object.(*ObjectMeta)
	return meta, err
}

func (r *ObjectReader) Read(ctx context.Context,
	extent Extent, idxs []uint16, ids []uint32, m *mpool.MPool,
	zoneMapFunc ZoneMapUnmarshalFunc,
	readFunc ReadObjectFunc) (*fileservice.IOVector, error) {
	meta, err := r.ReadMeta(ctx, []Extent{extent}, m, zoneMapFunc)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		ids = make([]uint32, len(meta.BlkMetas))
		for i := range ids {
			ids[i] = uint32(i)
		}
	}
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0, len(idxs)*len(ids)),
		NoCache:  r.noCache,
	}
	for _, id := range ids {
		for _, idx := range idxs {
			col := meta.BlkMetas[id].(*Block).columns[idx]
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

func (r *ObjectReader) ReadBlocks(ctx context.Context,
	extent Extent, ids map[uint32]*ReadBlockOptions, m *mpool.MPool,
	zoneMapFunc ZoneMapUnmarshalFunc,
	readFunc ReadObjectFunc) (*fileservice.IOVector, error) {
	meta, err := r.ReadMeta(ctx, []Extent{extent}, m, zoneMapFunc)
	blocks := meta.BlkMetas
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, block := range ids {
		for idx := range block.Idxes {
			col := blocks[block.Id].(*Block).columns[idx]
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
	fileSize int64, m *mpool.MPool, ZMUnmarshalFunc ZoneMapUnmarshalFunc) (*ObjectMeta, error) {
	footer, err := r.readFooter(ctx, fileSize, m)
	if err != nil {
		return nil, err
	}
	extent := []Extent{{offset: footer.metaStart, length: footer.metaLen, originSize: footer.metaLen}}
	return r.ReadMeta(ctx, extent, m, ZMUnmarshalFunc)
}

func (r *ObjectReader) readFooter(ctx context.Context, fileSize int64, m *mpool.MPool) (*Footer, error) {
	return r.readFooterAndUnMarshal(ctx, fileSize, FooterSize, m)
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
					err := footer.Unmarshal(data)
					if err != nil {
						return footer, 0, nil
					}
					return footer, int64(len(data)), nil
				},
			},
		},
		NoCache: r.noCache,
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

type ReaderOptions struct {
	// noCache true means NOT cache IOVector in FileService's cache
	noCache bool
}

type ReaderOptionFunc func(opt *ReaderOptions)

func WithNoCacheReader(noCache bool) ReaderOptionFunc {
	return ReaderOptionFunc(func(opt *ReaderOptions) {
		opt.noCache = noCache
	})
}
