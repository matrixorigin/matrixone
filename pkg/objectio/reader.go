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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

type objectReaderV1 struct {
	Object
	ReaderOptions
	oname     *ObjectName
	metaExt   *Extent
	metaCache atomic.Pointer[ObjectMeta]
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
	if len(opts) == 0 {
		reader.metaReadPolicy = fileservice.SkipMemoryCache
		reader.metaReadPolicy |= fileservice.SkipFullFilePreloads
	} else {
		for _, f := range opts {
			f(&reader.ReaderOptions)
		}
	}
	return reader, nil
}

func (r *objectReaderV1) Init(location Location, fs fileservice.FileService) {
	oName := location.Name()
	extent := location.Extent()
	r.name = oName.String()
	r.oname = &oName
	r.metaExt = &extent
	r.fs = fs
	r.metaCache.Store(nil)
}

func (r *objectReaderV1) Reset() {
	r.metaExt = nil
	r.oname = nil
	r.metaCache.Store(nil)
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
	seqnums []uint16,
	m *mpool.MPool,
) (zms []ZoneMap, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta, _ := metaHeader.DataMeta()
	blkMeta := meta.GetBlockMeta(uint32(blk))
	zms = blkMeta.ToColumnZoneMaps(seqnums)
	return
}

func (r *objectReaderV1) ReadMeta(
	ctx context.Context,
	m *mpool.MPool,
) (meta ObjectMeta, err error) {
	if r.withMetaCache {
		cache := r.metaCache.Load()
		if cache != nil {
			meta = *cache
			return
		}
	}
	if r.oname != nil {
		// read table data block
		if meta, err = LoadObjectMetaByExtent(ctx, r.oname, r.metaExt, false, r.metaReadPolicy, r.fs); err != nil {
			return
		}
	} else {
		// read gc/ckp/etl ... data
		if meta, err = ReadObjectMeta(ctx, r.name, r.metaExt, r.metaReadPolicy, r.fs); err != nil {
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
	typs []types.Type,
	blk uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta, _ := metaHeader.DataMeta()
	return ReadOneBlockWithMeta(ctx, &meta, r.name, blk, idxs, typs, m, r.fs, constructorFactory, r.dataReadPolicy)
}

func (r *objectReaderV1) ReadSubBlock(
	ctx context.Context,
	idxs []uint16,
	typs []types.Type,
	blk uint16,
	m *mpool.MPool,
) (ioVecs []*fileservice.IOVector, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta, _ := metaHeader.SubMeta(blk)
	ioVecs = make([]*fileservice.IOVector, 0)
	for i := uint32(0); i < meta.BlockCount(); i++ {
		var ioVec *fileservice.IOVector
		ioVec, err = ReadOneBlockWithMeta(ctx, &meta, r.name, meta.BlockHeader().StartID()+uint16(i), idxs, typs, m, r.fs, constructorFactory, fileservice.Policy(0))
		if err != nil {
			return
		}
		ioVecs = append(ioVecs, ioVec)
	}
	return
}

func (r *objectReaderV1) ReadOneSubBlock(
	ctx context.Context,
	idxs []uint16,
	typs []types.Type,
	dataType uint16,
	blk uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta, _ := metaHeader.SubMeta(dataType)
	ioVec, err = ReadOneBlockWithMeta(ctx, &meta, r.name, blk, idxs, typs, m, r.fs, constructorFactory, fileservice.Policy(0))
	if err != nil {
		return
	}
	return
}

func (r *objectReaderV1) ReadAll(
	ctx context.Context,
	idxs []uint16,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	meta := metaHeader.MustDataMeta()
	return ReadAllBlocksWithMeta(ctx, &meta, r.name, idxs, r.dataReadPolicy, m, r.fs, constructorFactory)
}

// ReadOneBF read one bloom filter
func (r *objectReaderV1) ReadOneBF(
	ctx context.Context,
	blk uint16,
) (bf StaticFilter, size uint32, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, nil); err != nil {
		return
	}
	meta := metaHeader.MustDataMeta()
	extent := meta.BlockHeader().BFExtent()
	bfs, err := ReadBloomFilter(ctx, r.name, &extent, r.dataReadPolicy, r.fs)
	if err != nil {
		return
	}
	buf := bfs.GetBloomFilter(uint32(blk))
	typ := meta.GetBlockMeta(uint32(blk)).BlockHeader().BloomFilterType()
	bf = index.NewEmptyBloomFilterWithType(typ)
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
	var metaHeader ObjectMeta
	var buf []byte
	if metaHeader, err = r.ReadMeta(ctx, nil); err != nil {
		return
	}
	meta := metaHeader.MustDataMeta()
	extent := meta.BlockHeader().BFExtent()
	if buf, err = ReadBloomFilter(ctx, r.name, &extent, r.dataReadPolicy, r.fs); err != nil {
		return
	}
	return buf, extent.OriginSize(), nil
}

func (r *objectReaderV1) ReadExtent(
	ctx context.Context,
	extent Extent,
) ([]byte, error) {
	v, err := ReadExtent(
		ctx,
		r.name,
		&extent,
		r.metaReadPolicy,
		r.fs,
		constructorFactory)
	if err != nil {
		return nil, err
	}

	var obj any
	obj, err = Decode(v)
	if err != nil {
		return nil, err
	}

	return obj.([]byte), nil
}

func (r *objectReaderV1) ReadMultiBlocks(
	ctx context.Context,
	opts map[uint16]*ReadBlockOptions,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var objectMeta ObjectMeta
	if objectMeta, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	return ReadMultiBlocksWithMeta(
		ctx,
		r.name,
		objectMeta,
		opts,
		r.fs,
		constructorFactory)
}

func (r *objectReaderV1) ReadMultiSubBlocks(
	ctx context.Context,
	opts map[uint16]*ReadBlockOptions,
	m *mpool.MPool,
) (ioVec *fileservice.IOVector, err error) {
	var metaHeader ObjectMeta
	if metaHeader, err = r.ReadMeta(ctx, m); err != nil {
		return
	}
	ioVec = &fileservice.IOVector{
		FilePath: r.name,
		Entries:  make([]fileservice.IOEntry, 0),
	}
	for _, opt := range opts {
		meta, _ := metaHeader.SubMeta(opt.DataType)
		for seqnum := range opt.Idxes {
			blkmeta := meta.GetBlockMeta(uint32(opt.Id))
			if seqnum > blkmeta.GetMaxSeqnum() || blkmeta.ColumnMeta(seqnum).DataType() == 0 {
				// prefetch, do not generate
				continue
			}
			col := blkmeta.ColumnMeta(seqnum)
			ioVec.Entries = append(ioVec.Entries, fileservice.IOEntry{
				Offset: int64(col.Location().Offset()),
				Size:   int64(col.Location().Length()),

				ToCacheData: constructorFactory(int64(col.Location().OriginSize()), col.Location().Alg()),
			})
		}
	}

	err = r.fs.Read(ctx, ioVec)
	return
}

func (r *objectReaderV1) ReadAllMeta(
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

func (r *objectReaderV1) ReadHeader(ctx context.Context, m *mpool.MPool) (h Header, err error) {
	ext := NewExtent(0, 0, HeaderSize, HeaderSize)
	v, err := ReadExtent(ctx, r.name, &ext, r.metaReadPolicy, r.fs, constructorFactory)
	if err != nil {
		return
	}
	h = Header(v)
	return
}

type ReaderOptions struct {
	// metaReadPolicy true means NOT cache IOVector in FileService's cache
	metaReadPolicy fileservice.Policy
	dataReadPolicy fileservice.Policy
	// withMetaCache true means cache objectDataMetaV1 in the Reader
	// Note: if withMetaCache is true, cleanup is needed
	withMetaCache bool
}

type ReaderOptionFunc func(opt *ReaderOptions)

func WithDataCachePolicyOption(noLRUCache fileservice.Policy) ReaderOptionFunc {
	return ReaderOptionFunc(func(opt *ReaderOptions) {
		opt.dataReadPolicy = noLRUCache
	})
}

func WithMetaCachePolicyOption(noLRUCache fileservice.Policy) ReaderOptionFunc {
	return ReaderOptionFunc(func(opt *ReaderOptions) {
		opt.metaReadPolicy = noLRUCache
	})
}
