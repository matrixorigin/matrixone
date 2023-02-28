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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/compress"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"io"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type BlockReader struct {
	reader objectio.Reader
	key    string
	name   string
	meta   objectio.Extent
}

func NewBlockReader(cxt context.Context, service fileservice.FileService, key string) (dataio.Reader, error) {
	name, _, meta, _, err := dataio.DecodeLocation(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		name:   name,
		meta:   meta,
	}, nil
}

func (r *BlockReader) LoadColumns(ctx context.Context, idxs []uint16,
	ids []uint32, m *mpool.MPool) ([]*batch.Batch, error) {
	bats := make([]*batch.Batch, 0)
	if r.meta.End() == 0 {
		return bats, nil
	}
	ioVectors, err := r.reader.ReadWithFunc(ctx, r.meta, idxs, ids, nil, LoadColumnFunc)
	if err != nil {
		return nil, err
	}
	for y, _ := range ids {
		bat := batch.NewWithSize(len(idxs))
		for i := range idxs {
			bat.Vecs[i] = ioVectors.Entries[y*len(idxs)+i].Object.(*vector.Vector)
		}
		bats = append(bats, bat)
	}
	return bats, nil
}

func (r *BlockReader) LoadZoneMaps(ctx context.Context, idxs []uint16,
	ids []uint32, m *mpool.MPool) ([][]*index.ZoneMap, error) {
	blocks, err := r.reader.ReadMetaWithFunc(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksZoneMap := make([][]*index.ZoneMap, len(ids))
	for i, id := range ids {
		blocksZoneMap[i], err = r.LoadZoneMap(ctx, idxs, blocks[id], m)
		if err != nil {
			return nil, err
		}
	}
	return blocksZoneMap, nil
}

func (r *BlockReader) LoadZoneMap(
	ctx context.Context,
	idxs []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]*index.ZoneMap, error) {
	zoneMapList := make([]*index.ZoneMap, len(idxs))
	for i, idx := range idxs {
		column, err := block.GetColumn(idx)
		if err != nil {
			return nil, err
		}
		zm, err := column.GetIndex(ctx, objectio.ZoneMapType, m)
		if err != nil {
			return nil, err
		}
		data := zm.(*objectio.ZoneMap).GetData()

		zoneMapList[i] = data.(*index.ZoneMap)
	}

	return zoneMapList, nil
}

func (r *BlockReader) LoadBloomFilter(ctx context.Context, idx uint16,
	ids []uint32, m *mpool.MPool) ([]index.StaticFilter, error) {
	blocks, err := r.reader.ReadMetaWithFunc(ctx, []objectio.Extent{r.meta}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksBloomFilters := make([]index.StaticFilter, len(ids))
	for i, id := range ids {
		column, err := blocks[id].GetColumn(idx)
		if err != nil {
			return nil, err
		}
		bf, err := column.GetIndex(ctx, objectio.BloomFilterType, m)
		if err != nil {
			return nil, err
		}
		blocksBloomFilters[i] = bf.(*objectio.BloomFilter).GetData().(index.StaticFilter)
	}
	return blocksBloomFilters, nil
}

func (r *BlockReader) LoadMeta(ctx context.Context, extent objectio.Extent, m *mpool.MPool) (objectio.BlockObject, error) {
	block, _ := r.reader.ReadMeta(ctx, []objectio.Extent{extent}, m)
	return block[0], nil
}

func (r *BlockReader) LoadAllMetas(ctx context.Context, extent objectio.Extent, m *mpool.MPool) ([]objectio.BlockObject, error) {
	block, _ := r.reader.ReadMeta(ctx, []objectio.Extent{extent}, m)
	return block, nil
}

func (r *BlockReader) LoadColumnsByTS(ctx context.Context, idxs []uint16, info catalog.BlockInfo,
	ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	return bat, nil
}

func NewCheckpointReader(cxt context.Context, fs fileservice.FileService, key string) (*BlockReader, error) {
	name, _, err := DecodeMetaLocToMetas(key)
	if err != nil {
		return nil, err
	}
	reader, err := objectio.NewObjectReader(name, fs)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		key:    key,
		name:   name,
	}, nil
}

func (r *BlockReader) BlkColumnByMetaLoadJob(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject,
) *tasks.Job {
	exec := func(_ context.Context) (result *tasks.JobResult) {
		bat, err := r.LoadBlkColumnsByMeta(colTypes, colNames, nullables, block)
		return &tasks.JobResult{
			Err: err,
			Res: bat,
		}
	}
	return tasks.NewJob(uuid.NewString(), r.readCxt, exec)
}

func (r *BlockReader) BlkColumnsByMetaAndIdxLoadJob(
	colTypes []types.Type,
	colNames []string,
	nullables []bool,
	block objectio.BlockObject,
	idx int,
) *tasks.Job {
	exec := func(_ context.Context) (result *tasks.JobResult) {
		bat, err := r.LoadBlkColumnsByMetaAndIdx(
			colTypes,
			colNames,
			nullables,
			block,
			idx)
		return &tasks.JobResult{
			Err: err,
			Res: bat,
		}
	}
	return tasks.NewJob(uuid.NewString(), r.readCxt, exec)
}

func (r *Reader) LoadZoneMapByExtent(
	ctx context.Context,
	idxs []uint16,
	extent objectio.Extent,
	m *mpool.MPool) ([]*index.ZoneMap, error) {

	obs, err := r.reader.ReadMetaWithFunc(ctx, []objectio.Extent{extent}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	zonemapList, err := r.LoadZoneMap(ctx, idxs, obs[0], m)
	if err != nil {
		return nil, err
	}
	return zonemapList, nil
}

func (r *Reader) LoadBloomFilterByExtent(
	ctx context.Context,
	idx uint16,
	extent objectio.Extent,
	m *mpool.MPool) (index.StaticFilter, error) {

	obs, err := r.reader.ReadMetaWithFunc(ctx, []objectio.Extent{extent}, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	column, err := obs[0].GetColumn(idx)
	if err != nil {
		return nil, err
	}
	bf, err := column.GetIndex(ctx, objectio.BloomFilterType, m)
	if err != nil {
		return nil, err
	}
	data := bf.(*objectio.BloomFilter).GetData()
	return data.(index.StaticFilter), nil
}

func (r *Reader) LoadAllZoneMap(
	ctx context.Context,
	size int64,
	idxs []uint16,
	m *mpool.MPool) ([][]*index.ZoneMap, error) {
	blocks, err := r.reader.ReadAllMetaWithFunc(r.readCxt, size, m, LoadZoneMapFunc)
	if err != nil {
		return nil, err
	}
	blocksZoneMap := make([][]*index.ZoneMap, len(blocks))
	for i, block := range blocks {
		blocksZoneMap[i], err = r.LoadZoneMap(ctx, idxs, block, m)
		if err != nil {
			return nil, err
		}
	}
	return blocksZoneMap, nil
}

func (r *Reader) LoadZoneMap(
	ctx context.Context,
	idxs []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]*index.ZoneMap, error) {
	zoneMapList := make([]*index.ZoneMap, len(idxs))
	for i, idx := range idxs {
		column, err := block.GetColumn(idx)
		if err != nil {
			return nil, err
		}
		zm, err := column.GetIndex(ctx, objectio.ZoneMapType, m)
		if err != nil {
			return nil, err
		}
		data := zm.(*objectio.ZoneMap).GetData()

		zoneMapList[i] = data.(*index.ZoneMap)
	}

	return zoneMapList, nil
}

func LoadZoneMapFunc(buf []byte, typ types.Type) (any, error) {
	zm := index.NewZoneMap(typ)
	err := zm.Unmarshal(buf[:])
	if err != nil {
		return nil, err
	}
	return zm, err
}

func LoadBloomFilterFunc(size int64, vec any) objectio.ToObjectFunc {
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
		vec := vector.New(types.Type{})
		if err = vec.Read(decompressed); err != nil {
			return nil, 0, err
		}
		return vec, int64(len(decompressed)), nil
	}
}

func (r *Reader) LoadBlkColumns(
	ctx context.Context,
	colTypes []types.Type,
	idxs []uint16,
	extent objectio.Extent,
	m *mpool.MPool,
) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(colTypes[i])
	}
	iov, err := r.reader.ReadWithFunc(ctx, extent, idxs, m, LoadColumnFunc, bat.Vecs)
	if err != nil {
		return nil, err
	}
	for i, entry := range iov.Entries {
		bat.Vecs[i] = entry.Object.(*vector.Vector)
	}
	return bat, nil
}

func (r *Reader) LoadAllBlkColumns(
	ctx context.Context,
	size int64,
	colTypes []types.Type,
	idxs []uint16,
	m *mpool.MPool,
) ([]*batch.Batch, error) {
	blocks, err := r.reader.ReadAllMeta(r.readCxt, size, m)
	if err != nil {
		return nil, err
	}
	bats := make([]*batch.Batch, 0)
	for _, block := range blocks {
		batch, err := r.LoadBlkColumns(ctx, colTypes, idxs, block.GetExtent(), m)
		if err != nil {
			return nil, err
		}
		bats = append(bats, batch)
	}
	return bats, nil
}

func (r *Reader) ReaAllMetas(ctx context.Context, size int64, m *mpool.MPool) ([]objectio.BlockObject, error) {
	blocks, err := r.reader.ReadAllMeta(ctx, size, m)
	if err != nil {
		return nil, err
	}
	return blocks, err
}

func (r *Reader) ReadMeta(m *mpool.MPool) (objectio.BlockObject, error) {
	extents := make([]objectio.Extent, 1)
	extents[0] = r.meta.GetLoc()
	block, err := r.reader.ReadMeta(r.readCxt, extents, m)
	if err != nil {
		return nil, err
	}
	return block[0], err
}

func (r *Reader) ReadMetas(m *mpool.MPool) ([]objectio.BlockObject, error) {
	block, err := r.reader.ReadMeta(r.readCxt, r.locs, m)
	return block, err
}

func (r *Reader) GetDataObject(idx uint16, m *mpool.MPool) objectio.ColumnObject {
	block, err := r.ReadMeta(m)
	if err != nil {
		panic(any(err))
	}
	if block == nil {
		return nil
	}
	object, err := block.GetColumn(idx)
	if err != nil {
		panic(any(err))
	}
	return object
}
