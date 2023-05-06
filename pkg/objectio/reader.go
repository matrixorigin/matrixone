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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type objectReaderV1 struct {
	Object
	ReaderOptions
	oname     *ObjectName
	metaExt   *Extent
	metaCache atomic.Pointer[objectMetaV1]
}

func newObjectReaderWithStrV1(name string, fs fileservice.FileService, opts ...ReaderOptionFunc) (*objectReaderV1, error) {
	reader := &objectReaderV1{
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

func newObjectReaderV1(
	oname *ObjectName,
	metaExt *Extent,
	fs fileservice.FileService,
	opts ...ReaderOptionFunc,
) (*objectReaderV1, error) {
	name := oname.String()
	reader := &objectReaderV1{
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

func (r *objectReaderV1) GetObject() *Object {
	return &r.Object
}

func (r *objectReaderV1) GetMetaExtent() *Extent {
	return r.metaExt
}

func (r *objectReaderV1) GetObjectName() *ObjectName {
	return r.oname
}

func (r *objectReaderV1) GetName() string {
	return r.name
}

func (r *objectReaderV1) CacheMetaExtent(ext *Extent) {
	r.metaExt = ext
}

func (r *objectReaderV1) ReadZM(
	ctx context.Context,
	blk uint16,
	cols []uint16,
	m *mpool.MPool,
) (zms []ZoneMap, err error) {
	var meta objectMetaV1
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	blkMeta := meta.GetBlockMeta(uint32(blk))
	zms = blkMeta.ToColumnZoneMaps(cols)
	return
}

func (r *objectReaderV1) ReadMeta(
	ctx context.Context,
	m *mpool.MPool,
) (meta objectMetaV1, err error) {
	if r.withMetaCache {
		cache := r.metaCache.Load()
		if cache != nil {
			meta = *cache
			return
		}
	}
	if r.oname != nil {
		// read table data block
		if meta, err = LoadObjectMetaByExtent(ctx, r.oname, r.metaExt, r.noLRUCache, r.fs); err != nil {
			return
		}
	} else {
		// read gc/ckp/etl ... data
		if meta, err = ReadObjectMeta(ctx, r.name, r.metaExt, r.noLRUCache, r.fs); err != nil {
			return
		}
	}
	if r.withMetaCache {
		r.metaCache.Store(&meta)
	}
	return
}

func (r *objectReaderV1) ReadOneBlock(
	ctx context.Context,
	idxs []uint16,
	blk uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var meta objectMetaV1
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	return ReadOneBlockWithMeta(ctx, &meta, r.name, blk, idxs, m, r.fs, constructorFactory)
}

func (r *objectReaderV1) ReadAll(
	ctx context.Context,
	idxs []uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var meta objectMetaV1
	if meta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	return ReadAllBlocksWithMeta(ctx, &meta, r.name, idxs, r.noLRUCache, m, r.fs, constructorFactory)
}

// ReadOneBF read one bloom filter
func (r *objectReaderV1) ReadOneBF(
	ctx context.Context,
	blk uint16,
) (bf StaticFilter, size uint32, err error) {
	var meta objectMetaV1
	if meta, err = r.ReadMeta(ctx, nil); err != nil {
		return
	}
	extent := meta.BlockHeader().BFExtent()
	bfs, err := ReadBloomFilter(ctx, r.name, &extent, r.noLRUCache, r.fs)
	if err != nil {
		return
	}
	buf := bfs.GetBloomFilter(uint32(blk))
	bf = index.NewEmptyBinaryFuseFilter()
	err = index.DecodeBloomFilter(bf, buf)
	if err != nil {
		return
	}
	size = uint32(len(buf))
	return bf, size, nil
}

func (r *objectReaderV1) ReadAllBF(
	ctx context.Context,
) (bfs BloomFilter, size uint32, err error) {
	var meta objectMetaV1
	var buf []byte
	if meta, err = r.ReadMeta(ctx, nil); err != nil {
		return
	}
	extent := meta.BlockHeader().BFExtent()
	if buf, err = ReadBloomFilter(ctx, r.name, &extent, r.noLRUCache, r.fs); err != nil {
		return
	}
	return buf, extent.OriginSize(), nil
}

func (r *objectReaderV1) ReadExtent(
	ctx context.Context,
	extent Extent,
	noHeaderHint bool,
) ([]byte, error) {
	v, err := ReadExtent(
		ctx,
		r.name,
		&extent,
		r.noLRUCache,
		noHeaderHint,
		r.fs,
		constructorFactory)
	if err != nil {
		return nil, err
	}
	return v.([]byte), nil
}

func (r *objectReaderV1) ReadMultiBlocks(
	ctx context.Context,
	opts map[uint16]*ReadBlockOptions,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var meta objectMetaV1
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
		constructorFactory)
}

func (r *objectReaderV1) ReadAllMeta(
	ctx context.Context,
	m *mpool.MPool,
) (objectMetaV1, error) {
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

func (r *objectReaderV1) ReadHeader(ctx context.Context, m *mpool.MPool) (h Header, err error) {
	ext := NewExtent(0, 0, HeaderSize, HeaderSize)
	v, err := ReadExtent(ctx, r.name, &ext, r.noLRUCache, true, r.fs, constructorFactory)
	if err != nil {
		return
	}
	h = Header(v.([]byte))
	return
}

type ReaderOptions struct {
	// noLRUCache true means NOT cache IOVector in FileService's cache
	noLRUCache bool
	// withMetaCache true means cache objectMetaV1 in the Reader
	// Note: if withMetaCache is true, cleanup is needed
	withMetaCache bool
}

type ReaderOptionFunc func(opt *ReaderOptions)

func WithNoLRUCacheOption(noLRUCache bool) ReaderOptionFunc {
	return ReaderOptionFunc(func(opt *ReaderOptions) {
		opt.noLRUCache = noLRUCache
	})
}

func WithLocalMetaCacheOption(withMetaCache bool) ReaderOptionFunc {
	return ReaderOptionFunc(func(opt *ReaderOptions) {
		opt.withMetaCache = withMetaCache
	})
}
