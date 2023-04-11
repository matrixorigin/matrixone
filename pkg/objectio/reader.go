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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectReader struct {
	object  *Object
	nameStr string
	name    ObjectName
	ReaderOptions
}

func NewObjectReaderWithStr(name string, fs fileservice.FileService, opts ...ReaderOptionFunc) (*ObjectReader, error) {
	reader := &ObjectReader{
		nameStr: name,
		object:  NewObject(name, fs),
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func NewObjectReader(name ObjectName, fs fileservice.FileService, opts ...ReaderOptionFunc) (*ObjectReader, error) {
	str := name.String()
	reader := &ObjectReader{
		nameStr: str,
		name:    name,
		object:  NewObject(str, fs),
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func (r *ObjectReader) GetObject() *Object {
	return r.object
}

func (r *ObjectReader) ReadMeta(
	ctx context.Context,
	extent *Extent,
	m *mpool.MPool,
) (ObjectMeta, error) {
	metas := &fileservice.IOVector{
		FilePath: r.nameStr,
		Entries:  make([]fileservice.IOEntry, 1),
		NoCache:  r.noCache,
	}

	metas.Entries[0] = fileservice.IOEntry{
		Offset: int64(extent.offset),
		Size:   int64(extent.originSize),

		ToObject: func(reader io.Reader, data []byte) (any, int64, error) {
			if len(data) == 0 {
				var err error
				data, err = io.ReadAll(reader)
				if err != nil {
					return nil, 0, err
				}
			}
			return data, int64(len(data)), nil
		},
	}

	err := r.object.fs.Read(ctx, metas)
	if err != nil {
		return nil, err
	}

	meta := ObjectMeta(metas.Entries[0].Object.([]byte))
	return meta, err
}

func (r *ObjectReader) Read(
	ctx context.Context,
	extent *Extent,
	idxs []uint16,
	id uint32,
	m *mpool.MPool,
	readFunc ReadObjectFunc,
) (*fileservice.IOVector, error) {
	meta, err := r.ReadMeta(ctx, extent, m)
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.nameStr,
		Entries:  make([]fileservice.IOEntry, 0),
		NoCache:  r.noCache,
	}
	for _, idx := range idxs {
		col := meta.GetColumnMeta(idx, id)
		ext := col.Location()
		data.Entries = append(data.Entries, fileservice.IOEntry{
			Offset: int64(ext.Offset()),
			Size:   int64(ext.Length()),

			ToObject: readFunc(int64(ext.OriginSize())),
		})
	}

	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadAll(
	ctx context.Context,
	extent *Extent,
	idxs []uint16,
	m *mpool.MPool,
	readFunc ReadObjectFunc,
) (*fileservice.IOVector, error) {
	meta, err := r.ReadMeta(ctx, extent, m)
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.nameStr,
		Entries:  make([]fileservice.IOEntry, 0),
		NoCache:  r.noCache,
	}
	ids := make([]uint32, meta.BlockCount())
	for i := range ids {
		ids[i] = uint32(i)
	}
	for _, id := range ids {
		for _, idx := range idxs {
			col := meta.GetColumnMeta(idx, id)
			ext := col.Location()
			data.Entries = append(data.Entries, fileservice.IOEntry{
				Offset: int64(ext.Offset()),
				Size:   int64(ext.Length()),

				ToObject: readFunc(int64(ext.OriginSize())),
			})
		}
	}

	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadBlocks(
	ctx context.Context,
	extent *Extent,
	ids map[uint32]*ReadBlockOptions,
	m *mpool.MPool,
	readFunc ReadObjectFunc,
) (*fileservice.IOVector, error) {
	meta, err := r.ReadMeta(ctx, extent, m)
	if err != nil {
		return nil, err
	}
	data := &fileservice.IOVector{
		FilePath: r.nameStr,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, block := range ids {
		for idx := range block.Idxes {
			col := meta.GetColumnMeta(idx, block.Id)
			data.Entries = append(data.Entries, fileservice.IOEntry{
				Offset: int64(col.Location().Offset()),
				Size:   int64(col.Location().Length()),

				ToObject: readFunc(int64(col.Location().OriginSize())),
			})
		}
	}

	err = r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *ObjectReader) ReadAllMeta(
	ctx context.Context,
	fileSize int64,
	m *mpool.MPool,
) (ObjectMeta, error) {
	footer, err := r.readFooter(ctx, fileSize, m)
	if err != nil {
		return nil, err
	}
	extent := Extent{offset: footer.metaStart, length: footer.metaLen, originSize: footer.metaLen}
	return r.ReadMeta(ctx, &extent, m)
}

func (r *ObjectReader) readFooter(ctx context.Context, fileSize int64, m *mpool.MPool) (*Footer, error) {
	return r.readFooterAndUnMarshal(ctx, fileSize, FooterSize, m)
}

func (r *ObjectReader) readFooterAndUnMarshal(ctx context.Context, fileSize, size int64, m *mpool.MPool) (*Footer, error) {
	data := &fileservice.IOVector{
		FilePath: r.nameStr,
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
