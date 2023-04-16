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

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type ObjectReader struct {
	object *Object
	name   string
	oname  ObjectName
	ReaderOptions
}

func NewObjectReaderWithStr(name string, fs fileservice.FileService, opts ...ReaderOptionFunc) (*ObjectReader, error) {
	reader := &ObjectReader{
		name:   name,
		object: NewObject(name, fs),
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func NewObjectReader(oname ObjectName, fs fileservice.FileService, opts ...ReaderOptionFunc) (*ObjectReader, error) {
	name := oname.String()
	reader := &ObjectReader{
		name:   name,
		oname:  oname,
		object: NewObject(name, fs),
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func (r *ObjectReader) GetObject() *Object {
	return r.object
}

func (r *ObjectReader) ReadZM(
	ctx context.Context,
	blk uint16,
	cols []uint16,
	metaExt Extent,
	m *mpool.MPool,
) (zms []ZoneMap, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, metaExt, m); err != nil {
		return
	}
	blkMeta := meta.GetBlockMeta(uint32(blk))
	zms = blkMeta.ToColumnZoneMaps(cols)
	return
}

func (r *ObjectReader) ReadMeta(
	ctx context.Context,
	extent Extent,
	m *mpool.MPool,
) (meta ObjectMeta, err error) {
	return ReadObjectMeta(ctx, r.name, &extent, r.noCache, r.object.fs)
}

func (r *ObjectReader) ReadOneBlock(
	ctx context.Context,
	metaExt Extent,
	idxs []uint16,
	id uint16,
	m *mpool.MPool,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, metaExt, m); err != nil {
		return
	}
	return ReadOneBlockWithMeta(ctx, &meta, r.name, id, idxs, m, r.object.fs, factory)
}

func (r *ObjectReader) ReadAll(
	ctx context.Context,
	metaExt Extent,
	idxs []uint16,
	m *mpool.MPool,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, metaExt, m); err != nil {
		return
	}
	return ReadAllBlocksWithMeta(ctx, &meta, r.name, idxs, r.noCache, m, r.object.fs, factory)
}

func (r *ObjectReader) ReadOneBF(
	ctx context.Context,
	metaExt Extent,
	blk uint16,
	m *mpool.MPool,
) (bf StaticFilter, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, metaExt, m); err != nil {
		return
	}
	extent := meta.BlockHeader().BFExtent()
	bfs, err := ReadBloomFilter(ctx, r.name, &extent, r.noCache, r.object.fs)
	if err != nil {
		return
	}
	bf = bfs[blk]
	return
}

func (r *ObjectReader) ReadExtent(
	ctx context.Context,
	extent Extent,
) ([]byte, error) {
	v, err := ReadExtent(
		ctx,
		r.name,
		&extent,
		r.noCache,
		r.object.fs,
		defaultConstructorFactory)
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (r *ObjectReader) ReadMultiBlocks(
	ctx context.Context,
	metaExt Extent,
	opts map[uint16]*ReadBlockOptions,
	m *mpool.MPool,
	constructor CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, metaExt, m); err != nil {
		return
	}
	return ReadMultiBlocksWithMeta(
		ctx,
		r.name,
		&meta,
		opts,
		false,
		m,
		r.object.fs,
		constructor)
}

func (r *ObjectReader) ReadAllMeta(
	ctx context.Context,
	m *mpool.MPool,
) (ObjectMeta, error) {
	header, err := r.readHeader(ctx, m)
	if err != nil {
		return nil, err
	}
	return r.ReadMeta(ctx, header.Extent(), m)
}

func (r *ObjectReader) readHeader(ctx context.Context, m *mpool.MPool) (Header, error) {
	return r.readHeaderAndUnMarshal(ctx, HeaderSize, m)
}

func (r *ObjectReader) readHeaderAndUnMarshal(ctx context.Context, size int64, m *mpool.MPool) (Header, error) {
	data := &fileservice.IOVector{
		FilePath: r.name,
		Entries: []fileservice.IOEntry{
			{
				Offset: 0,
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
					return data, int64(len(data)), nil
				},
			},
		},
		NoCache: r.noCache,
	}
	err := r.object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}

	return data.Entries[0].Object.([]byte), nil
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
