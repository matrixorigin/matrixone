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
	Object
	ReaderOptions
	oname   *ObjectName
	metaExt *Extent
}

func NewObjectReaderWithStr(name string, fs fileservice.FileService, opts ...ReaderOptionFunc) (*ObjectReader, error) {
	reader := &ObjectReader{
		Object: Object{
			name: name,
			fs:   fs,
		},
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func NewObjectReader(
	oname *ObjectName,
	metaExt *Extent,
	fs fileservice.FileService,
	opts ...ReaderOptionFunc,
) (*ObjectReader, error) {
	name := oname.String()
	reader := &ObjectReader{
		Object: Object{
			name: name,
			fs:   fs,
		},
		oname:   oname,
		metaExt: metaExt,
	}
	for _, f := range opts {
		f(&reader.ReaderOptions)
	}
	return reader, nil
}

func (r *ObjectReader) GetObject() *Object {
	return &r.Object
}

func (r *ObjectReader) GetMetaExtent() *Extent {
	return r.metaExt
}

func (r *ObjectReader) GetObjectName() *ObjectName {
	return r.oname
}

func (r *ObjectReader) GetName() string {
	return r.name
}

func (r *ObjectReader) CacheMetaExtent(ext *Extent) {
	r.metaExt = ext
}

func (r *ObjectReader) ReadZM(
	ctx context.Context,
	blk uint16,
	cols []uint16,
	m *mpool.MPool,
) (zms []ZoneMap, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	blkMeta := meta.GetBlockMeta(uint32(blk))
	zms = blkMeta.ToColumnZoneMaps(cols)
	return
}

func (r *ObjectReader) ReadMeta(
	ctx context.Context,
	m *mpool.MPool,
) (meta ObjectMeta, err error) {
	return ReadObjectMeta(ctx, r.name, r.metaExt, r.noCache, r.fs)
}

func (r *ObjectReader) ReadOneBlock(
	ctx context.Context,
	idxs []uint16,
	blk uint16,
	m *mpool.MPool,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	return ReadOneBlockWithMeta(ctx, &meta, r.name, blk, idxs, m, r.fs, factory)
}

func (r *ObjectReader) ReadAll(
	ctx context.Context,
	idxs []uint16,
	m *mpool.MPool,
	factory CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	return ReadAllBlocksWithMeta(ctx, &meta, r.name, idxs, r.noCache, m, r.fs, factory)
}

func (r *ObjectReader) ReadOneBF(
	ctx context.Context,
	blk uint16,
) (bf StaticFilter, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, nil); err != nil {
		return
	}
	extent := meta.BlockHeader().BFExtent()
	bfs, err := ReadBloomFilter(ctx, r.name, &extent, r.noCache, r.fs)
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
		r.fs,
		decompressConstructorFactory)
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (r *ObjectReader) ReadMultiBlocks(
	ctx context.Context,
	opts map[uint16]*ReadBlockOptions,
	m *mpool.MPool,
	constructor CacheConstructorFactory,
) (ioVec *fileservice.IOVector, err error) {
	var meta ObjectMeta
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	return ReadMultiBlocksWithMeta(
		ctx,
		r.name,
		&meta,
		opts,
		false,
		m,
		r.fs,
		constructor)
}

func (r *ObjectReader) ReadAllMeta(
	ctx context.Context,
	m *mpool.MPool,
) (ObjectMeta, error) {
	if r.metaExt == nil {
		header, err := r.ReadHeader(ctx, m)
		if err != nil {
			return nil, err
		}
		ext := header.Extent()
		r.CacheMetaExtent(&ext)
	}
	return r.ReadMeta(ctx, m)
}

func (r *ObjectReader) ReadHeader(ctx context.Context, m *mpool.MPool) (h Header, err error) {
	ext := NewExtent(0, 0, HeaderSize, HeaderSize)
	v, err := ReadExtent(ctx, r.name, &ext, r.noCache, r.fs, noDecompressConstructorFactory)
	if err != nil {
		return
	}
	h = Header(v.([]byte))
	return
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
