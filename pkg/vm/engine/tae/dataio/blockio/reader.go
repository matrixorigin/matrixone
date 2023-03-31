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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type BlockReader struct {
	reader  objectio.Reader
	key     string
	name    string
	meta    objectio.Extent
	manager *IoPipeline
}

type fetch struct {
	name   string
	meta   objectio.Extent
	idxes  []uint16
	ids    []uint32
	pool   *mpool.MPool
	reader objectio.Reader
}

func NewObjectReader(service fileservice.FileService, key string) (dataio.Reader, error) {
	name, _, meta, _, err := DecodeLocation(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader:  reader,
		name:    name,
		meta:    meta,
		manager: pipeline,
	}, nil
}

func NewFileReader(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReader(name, service)
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
	reader, err := objectio.NewObjectReader(name, service, objectio.WithNoCacheReader(true))
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		name:   name,
	}, nil
}

func NewCheckPointReader(service fileservice.FileService, key string) (dataio.Reader, error) {
	name, locs, err := DecodeLocationToMetas(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		key:     key,
		reader:  reader,
		name:    name,
		meta:    locs[0],
		manager: pipeline,
	}, nil
}

func (r *BlockReader) LoadColumns(ctx context.Context, idxes []uint16,
	ids []uint32, m *mpool.MPool) ([]*batch.Batch, error) {
	bats := make([]*batch.Batch, 0)
	if r.meta.End() == 0 {
		return bats, nil
	}
	proc := fetch{
		name:   r.name,
		meta:   r.meta,
		idxes:  idxes,
		ids:    ids,
		pool:   m,
		reader: r.reader,
	}
	v, err := r.manager.Fetch(ctx, proc)
	if err != nil {
		return nil, err
	}
	ioVectors := v.(*fileservice.IOVector)
	for y := range ids {
		bat := batch.NewWithSize(len(idxes))
		for i := range idxes {
			bat.Vecs[i] = ioVectors.Entries[y*len(idxes)+i].Object.(*vector.Vector)
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func (r *BlockReader) LoadAllColumns(ctx context.Context, idxs []uint16,
	size int64, m *mpool.MPool) ([]*batch.Batch, error) {
	meta, err := r.reader.ReadAllMeta(ctx, size, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocks := meta.BlkMetas
	if blocks[0].GetExtent().End() == 0 {
		return nil, nil
	}
	if len(idxs) == 0 {
		idxs = make([]uint16, blocks[0].GetColumnCount())
		for i := range idxs {
			idxs[i] = uint16(i)
		}
	}
	bats := make([]*batch.Batch, 0)
	ioVectors, err := r.reader.Read(ctx, blocks[0].GetExtent(), idxs, nil, nil, LoadZoneMapFunc, LoadColumnFunc)
	if err != nil {
		return nil, err
	}
	for y := range blocks {
		bat := batch.NewWithSize(len(idxs))
		for i := range idxs {
			bat.Vecs[i] = ioVectors.Entries[y*len(idxs)+i].Object.(*vector.Vector)
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func (r *BlockReader) LoadZoneMaps(ctx context.Context, idxs []uint16,
	ids []uint32, m *mpool.MPool) ([][]dataio.Index, error) {
	meta, err := r.reader.ReadMeta(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksZoneMap := make([][]dataio.Index, len(ids))
	for i, id := range ids {
		blocksZoneMap[i], err = r.LoadZoneMap(ctx, idxs, meta.BlkMetas[id], m)
		if err != nil {
			return nil, err
		}
	}
	return blocksZoneMap, nil
}

func (r *BlockReader) LoadObjectMeta(ctx context.Context, m *mpool.MPool) (*dataio.ObjectMeta, error) {
	objmeta, err := r.reader.ReadMeta(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	meta := &dataio.ObjectMeta{}
	meta.Rows = objmeta.Rows
	for _, colmeta := range objmeta.ColMetas {
		meta.ColMetas = append(meta.ColMetas, dataio.ColMeta{NullCnt: colmeta.NullCnt, Ndv: colmeta.Ndv, Zm: colmeta.Zonemap.GetData().(*BlockIndex)})
	}
	var idxs []uint16
	for _, blkmeta := range objmeta.BlkMetas {
		if idxs == nil {
			idxs = make([]uint16, blkmeta.GetColumnCount())
			for i := 0; i < len(idxs); i++ {
				idxs[i] = uint16(i)
			}
		}
		zm, err := r.LoadZoneMap(ctx, idxs, blkmeta, m)
		meta.Zms = append(meta.Zms, zm)
		if err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func (r *BlockReader) LoadBlocksMeta(ctx context.Context, m *mpool.MPool) ([]objectio.BlockObject, error) {
	_, locs, err := DecodeLocationToMetas(r.key)
	if err != nil {
		return nil, err
	}
	meta, err := r.reader.ReadMeta(ctx, locs, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	return meta.BlkMetas, nil
}

func (r *BlockReader) LoadAllBlocks(ctx context.Context, size int64, m *mpool.MPool) ([]objectio.BlockObject, error) {
	meta, err := r.reader.ReadAllMeta(ctx, size, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocks := meta.BlkMetas
	if r.meta.End() == 0 && len(blocks) > 0 {
		r.meta = blocks[0].GetExtent()
	}
	return blocks, nil
}

func (r *BlockReader) LoadZoneMap(
	ctx context.Context,
	idxs []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]dataio.Index, error) {
	zoneMapList := make([]dataio.Index, len(idxs))
	for i, idx := range idxs {
		column, err := block.GetColumn(idx)
		if err != nil {
			return nil, err
		}
		zm, err := column.GetIndex(ctx, objectio.ZoneMapType, nil, m)
		if err != nil {
			return nil, err
		}
		data := zm.(*objectio.ZoneMap).GetData()

		zoneMapList[i] = data.(*BlockIndex)
	}

	return zoneMapList, nil
}

func (r *BlockReader) LoadBloomFilter(ctx context.Context, idx uint16,
	ids []uint32, m *mpool.MPool) ([]index.StaticFilter, error) {
	meta, err := r.reader.ReadMeta(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksBloomFilters := make([]index.StaticFilter, len(ids))
	for i, id := range ids {
		column, err := meta.BlkMetas[id].GetColumn(idx)
		if err != nil {
			return nil, err
		}
		bf, err := column.GetIndex(ctx, objectio.BloomFilterType, LoadBloomFilterFunc, m)
		if err != nil {
			return nil, err
		}
		blocksBloomFilters[i] = bf.(*objectio.BloomFilter).GetData().(index.StaticFilter)
	}
	return blocksBloomFilters, nil
}

func (r *BlockReader) MvccLoadColumns(ctx context.Context, idxs []uint16, info catalog.BlockInfo,
	ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	return bat, nil
}

func (r *BlockReader) GetObjectName() string {
	return r.name
}
func (r *BlockReader) GetObjectExtent() objectio.Extent {
	return r.meta
}
func (r *BlockReader) GetObjectReader() objectio.Reader {
	return r.reader
}

func LoadZoneMapFunc(buf []byte, typ types.Type) (any, error) {
	zm := NewZoneMap(typ)
	err := zm.Unmarshal(buf[:])
	if err != nil {
		return nil, err
	}
	return zm, err
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
		bf, err := index.NewBinaryFuseFilterFromSource(decompressed)
		if err != nil {
			return nil, 0, err
		}
		return bf, int64(len(decompressed)), nil
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
func PrefetchWithMerged(pref prefetch) error {
	return pipeline.Prefetch(pref)
}

func Prefetch(idxes []uint16, reader dataio.Reader,
	ids []uint32, m *mpool.MPool) error {
	if reader.GetObjectExtent().End() == 0 {
		return nil
	}

	pref := BuildPrefetch(reader, m)
	pref.AddBlock(idxes, ids)
	return pipeline.Prefetch(pref)
}

func PrefetchBlocksMeta(reader dataio.Reader, m *mpool.MPool) error {
	if reader.GetObjectExtent().End() == 0 {
		return nil
	}
	pref := BuildPrefetch(reader, m)
	return pipeline.Prefetch(pref)
}
