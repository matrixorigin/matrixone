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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	AsyncIo = 1
	SyncIo  = 2
)

var IoModel = SyncIo

type BlockReader struct {
	reader *objectio.ObjectReader
	aio    *IoPipeline
}

type fetchParams struct {
	idxes  []uint16
	blk    uint16
	pool   *mpool.MPool
	reader *objectio.ObjectReader
}

func NewObjectReader(
	service fileservice.FileService,
	key objectio.Location,
	opts ...objectio.ReaderOptionFunc,
) (*BlockReader, error) {
	name := key.Name()
	metaExt := key.Extent()
	reader, err := objectio.NewObjectReader(&name, &metaExt, service, opts...)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		aio:    pipeline,
	}, nil
}

func NewFileReader(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReaderWithStr(name, service)
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		aio:    pipeline,
	}, nil
}

func NewFileReaderNoCache(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReaderWithStr(name, service, objectio.WithNoLRUCacheOption(true))
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
	}, nil
}

func (r *BlockReader) LoadColumns(
	ctx context.Context,
	cols []uint16,
	blk uint16,
	m *mpool.MPool,
) (bat *batch.Batch, err error) {
	metaExt := r.reader.GetMetaExtent()
	if metaExt == nil || metaExt.End() == 0 {
		return
	}
	var ioVectors *fileservice.IOVector
	if IoModel == AsyncIo {
		proc := fetchParams{
			idxes:  cols,
			blk:    blk,
			pool:   m,
			reader: r.reader,
		}
		var v any
		if v, err = r.aio.Fetch(ctx, proc); err != nil {
			return
		}
		ioVectors = v.(*fileservice.IOVector)
	} else {
		ioVectors, err = r.reader.ReadOneBlock(ctx, cols, blk, m)
		if err != nil {
			return
		}
	}
	bat = batch.NewWithSize(len(cols))
	for i := range cols {
		bat.Vecs[i] = ioVectors.Entries[i].Object.(*vector.Vector)
	}
	return
}

func (r *BlockReader) LoadAllColumns(
	ctx context.Context,
	idxs []uint16,
	m *mpool.MPool,
) ([]*batch.Batch, error) {
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

	ioVectors, err := r.reader.ReadAll(ctx, idxs, nil)
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

func (r *BlockReader) LoadZoneMaps(
	ctx context.Context,
	idxs []uint16,
	id uint16,
	m *mpool.MPool,
) ([]objectio.ZoneMap, error) {
	return r.reader.ReadZM(ctx, id, idxs, m)
}

func (r *BlockReader) LoadObjectMeta(ctx context.Context, m *mpool.MPool) (objectio.ObjectMeta, error) {
	return r.reader.ReadMeta(ctx, m)
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
	return blocks, nil
}

func (r *BlockReader) LoadZoneMap(
	ctx context.Context,
	idxs []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]objectio.ZoneMap, error) {
	return block.ToColumnZoneMaps(idxs), nil
}

func (r *BlockReader) LoadOneBF(
	ctx context.Context,
	blk uint16,
) (objectio.StaticFilter, error) {
	return r.reader.ReadOneBF(ctx, blk)
}

func (r *BlockReader) LoadAllBF(
	ctx context.Context,
) ([]objectio.StaticFilter, uint32, error) {
	return r.reader.ReadAllBF(ctx)
}

func (r *BlockReader) GetObjectName() *objectio.ObjectName {
	return r.reader.GetObjectName()
}

func (r *BlockReader) GetName() string {
	return r.reader.GetName()
}

func (r *BlockReader) GetObjectReader() *objectio.ObjectReader {
	return r.reader
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
