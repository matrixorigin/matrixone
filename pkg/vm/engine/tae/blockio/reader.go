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

package blockio

import (
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BlockReader struct {
	reader  *objectio.ObjectReader
	key     objectio.Location
	name    string
	meta    objectio.Extent
	manager *IoPipeline
}

type fetchParams struct {
	name   string
	meta   objectio.Extent
	idxes  []uint16
	id     uint16
	pool   *mpool.MPool
	reader *objectio.ObjectReader
}

func NewObjectReader(service fileservice.FileService, key objectio.Location) (*BlockReader, error) {
	reader, err := objectio.NewObjectReader(key.Name(), service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader:  reader,
		key:     key,
		meta:    key.Extent(),
		manager: pipeline,
	}, nil
}

func NewFileReader(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReaderWithStr(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader:  reader,
		name:    name,
		manager: pipeline,
	}, nil
}

func NewFileReaderNoCache(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReaderWithStr(name, service, objectio.WithNoCacheReader(true))
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		name:   name,
	}, nil
}

func (r *BlockReader) LoadColumns(ctx context.Context, idxes []uint16,
	id uint16, m *mpool.MPool) (*batch.Batch, error) {
	var bat *batch.Batch
	if r.meta.End() == 0 {
		return bat, nil
	}
	proc := fetchParams{
		name:   r.name,
		meta:   r.meta,
		idxes:  idxes,
		id:     id,
		pool:   m,
		reader: r.reader,
	}
	v, err := r.manager.Fetch(ctx, proc)
	if err != nil {
		return nil, err
	}
	ioVectors := v.(*fileservice.IOVector)
	bat = batch.NewWithSize(len(idxes))
	for i := range idxes {
		bat.Vecs[i] = ioVectors.Entries[i].Object.(*vector.Vector)
	}
	return bat, nil
}

func (r *BlockReader) LoadAllColumns(ctx context.Context, idxs []uint16, m *mpool.MPool) ([]*batch.Batch, error) {
	meta, err := r.reader.ReadAllMeta(ctx, m)
	if err != nil {
		return nil, err
	}
	if meta.BlockHeader().MetaLocation().End() == 0 {
		return nil, nil
	}
	block := meta.GetBlockMeta(0)
	if len(idxs) == 0 {
		idxs = make([]uint16, block.GetColumnCount())
		for i := range idxs {
			idxs[i] = uint16(i)
		}
	}

	bats := make([]*batch.Batch, 0)

	ioVectors, err := r.reader.ReadAll(ctx, meta.BlockHeader().MetaLocation(), idxs, nil, LoadColumnFunc)
	if err != nil {
		return nil, err
	}
	for y := 0; y < int(meta.BlockCount()); y++ {
		bat := batch.NewWithSize(len(idxs))
		for i := range idxs {
			bat.Vecs[i] = ioVectors.Entries[y*len(idxs)+i].Object.(*vector.Vector)
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func (r *BlockReader) LoadZoneMaps(ctx context.Context, idxs []uint16,
	id uint16, m *mpool.MPool) ([]objectio.ZoneMap, error) {
	meta, err := r.reader.ReadMeta(ctx, r.meta, m)
	if err != nil {
		return nil, err
	}

	block := meta.GetBlockMeta(uint32(id))
	blocksZoneMap, err := r.LoadZoneMap(ctx, idxs, block, m)
	if err != nil {
		return nil, err
	}
	return blocksZoneMap, nil
}

func (r *BlockReader) LoadObjectMeta(ctx context.Context, m *mpool.MPool) (objectio.ObjectMeta, error) {
	return r.reader.ReadMeta(ctx, r.meta, m)
}

func (r *BlockReader) LoadAllBlocks(ctx context.Context, m *mpool.MPool) ([]objectio.BlockObject, error) {
	meta, err := r.reader.ReadAllMeta(ctx, m)
	if err != nil {
		return nil, err
	}
	blocks := make([]objectio.BlockObject, meta.BlockCount())
	for i := 0; i < int(meta.BlockCount()); i++ {
		blocks[i] = meta.GetBlockMeta(uint32(i))
	}
	if r.meta == nil && len(blocks) > 0 {
		r.meta = meta.BlockHeader().MetaLocation()
	}
	return blocks, nil
}

func (r *BlockReader) LoadZoneMap(
	ctx context.Context,
	idxs []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]objectio.ZoneMap, error) {
	zoneMapList := make([]objectio.ZoneMap, len(idxs))
	for i, idx := range idxs {
		column, err := block.GetColumn(idx)
		if err != nil {
			return nil, err
		}
		zoneMapList[i] = index.DecodeZM(column.ZoneMap())
	}

	return zoneMapList, nil
}

func (r *BlockReader) LoadBloomFilter(ctx context.Context, idx uint16,
	id uint16, m *mpool.MPool) (objectio.StaticFilter, error) {
	meta, err := r.reader.ReadMeta(ctx, r.meta, m)
	if err != nil {
		return nil, err
	}
	bf, err := r.reader.ReadBloomFilter(ctx, meta.BlockHeader().BloomFilter(), LoadBloomFilterFunc)
	if err != nil {
		return nil, err
	}
	return bf[id], nil
}

func (r *BlockReader) MvccLoadColumns(ctx context.Context, idxs []uint16, info catalog.BlockInfo,
	ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	return bat, nil
}

func (r *BlockReader) GetObjectName() objectio.ObjectName {
	return r.key.Name()
}

func (r *BlockReader) GetName() string {
	return r.name
}

func (r *BlockReader) GetObjectExtent() objectio.Extent {
	return r.meta
}
func (r *BlockReader) GetObjectReader() *objectio.ObjectReader {
	return r.reader
}

func LoadBloomFilterFunc(size int64) objectio.ToObjectFunc {
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
		indexes := make([]objectio.StaticFilter, 0)
		bf := objectio.BloomFilter(decompressed)
		count := bf.BlockCount()
		for i := uint32(0); i < count; i++ {
			buf := bf.GetBloomFilter(i)
			if len(buf) == 0 {
				indexes = append(indexes, nil)
				continue
			}
			index, err := index.DecodeBloomFilter(bf.GetBloomFilter(i))
			if err != nil {
				return nil, 0, err
			}
			indexes = append(indexes, index)
		}
		return indexes, int64(len(decompressed)), nil
	}
}

func LoadColumnFunc(size int64) objectio.ToObjectFunc {
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
		vec := vector.NewVec(types.Type{})
		if err = vec.UnmarshalBinary(decompressed); err != nil {
			return nil, 0, err
		}
		return vec, int64(len(decompressed)), nil
	}
}

// The caller has merged the block information that needs to be prefetched
func PrefetchWithMerged(params prefetchParams) error {
	return pipeline.Prefetch(params)
}

func Prefetch(idxes []uint16, ids []uint16, service fileservice.FileService, key objectio.Location) error {

	params, err := BuildPrefetchParams(service, key)
	if err != nil {
		return err
	}
	params.AddBlock(idxes, ids)
	return pipeline.Prefetch(params)
}

func PrefetchMeta(service fileservice.FileService, key objectio.Location) error {
	params, err := BuildPrefetchParams(service, key)
	if err != nil {
		return err
	}
	return pipeline.Prefetch(params)
}

func PrefetchFile(service fileservice.FileService, name string) error {
	reader, err := NewFileReader(service, name)
	if err != nil {
		return err
	}
	bs, err := reader.LoadAllBlocks(context.Background(), common.DefaultAllocator)
	if err != nil {
		return err
	}
	params := buildPrefetchParams2(reader)
	for i := range bs {
		idxes := make([]uint16, bs[i].GetColumnCount())
		for a := uint16(0); a < bs[i].GetColumnCount(); a++ {
			idxes[a] = a
		}
		params.AddBlock(idxes, []uint16{bs[i].GetID()})
	}
	return PrefetchWithMerged(params)
}
