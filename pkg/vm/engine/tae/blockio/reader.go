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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
	typs   []types.Type
	blk    uint16
	pool   *mpool.MPool
	reader *objectio.ObjectReader
}

func NewObjectReader(
	sid string,
	service fileservice.FileService,
	key objectio.Location,
	opts ...objectio.ReaderOptionFunc,
) (*BlockReader, error) {
	name := key.Name()
	metaExt := key.Extent()
	var reader *objectio.ObjectReader
	var err error
	if opts == nil {
		reader, err = objectio.NewObjectReader(
			&name,
			&metaExt,
			service,
			objectio.WithMetaCachePolicyOption(fileservice.SkipMemoryCache|fileservice.SkipFullFilePreloads))
	} else {
		reader, err = objectio.NewObjectReader(&name, &metaExt, service, opts...)
	}
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		aio:    GetPipeline(sid),
	}, nil
}

func NewFileReader(
	sid string,
	service fileservice.FileService,
	name string,
) (*BlockReader, error) {
	reader, err := objectio.NewObjectReaderWithStr(
		name,
		service,
		objectio.WithMetaCachePolicyOption(fileservice.SkipMemoryCache|fileservice.SkipFullFilePreloads))
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
		aio:    GetPipeline(sid),
	}, nil
}

func NewFileReaderNoCache(service fileservice.FileService, name string) (*BlockReader, error) {
	reader, err := objectio.NewObjectReaderWithStr(
		name,
		service,
		objectio.WithDataCachePolicyOption(fileservice.SkipAllCache),
		objectio.WithMetaCachePolicyOption(fileservice.SkipAllCache))
	if err != nil {
		return nil, err
	}
	return &BlockReader{
		reader: reader,
	}, nil
}

// LoadColumns needs typs to generate columns, if the target table has no schema change, nil can be passed.
func (r *BlockReader) LoadColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	blk uint16,
	m *mpool.MPool,
) (bat *batch.Batch, release func(), err error) {
	metaExt := r.reader.GetMetaExtent()
	if metaExt == nil || metaExt.End() == 0 {
		return
	}
	var ioVectors fileservice.IOVector
	if IoModel == AsyncIo {
		proc := fetchParams{
			idxes:  cols,
			blk:    blk,
			typs:   typs,
			pool:   m,
			reader: r.reader,
		}
		var v any
		if v, err = r.aio.Fetch(ctx, proc); err != nil {
			return
		}
		ioVectors = v.(fileservice.IOVector)
	} else {
		ioVectors, err = r.reader.ReadOneBlock(ctx, cols, typs, blk, m)
		if err != nil {
			return
		}
	}
	release = func() {
		objectio.ReleaseIOVector(&ioVectors)
	}
	defer func() {
		if err != nil {
			release()
		}
	}()
	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		obj, err = objectio.Decode(ioVectors.Entries[i].CachedData.Bytes())
		if err != nil {
			return
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}
	return
}

// LoadColumns needs typs to generate columns, if the target table has no schema change, nil can be passed.
func (r *BlockReader) LoadSubColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	blk uint16,
	m *mpool.MPool,
) (bats []*batch.Batch, releases func(), err error) {
	metaExt := r.reader.GetMetaExtent()
	if metaExt == nil || metaExt.End() == 0 {
		return
	}
	var ioVectors []fileservice.IOVector
	ioVectors, err = r.reader.ReadSubBlock(ctx, cols, typs, blk, m)
	if err != nil {
		return
	}
	releases = func() {
		for _, vec := range ioVectors {
			objectio.ReleaseIOVector(&vec)
		}
	}
	bats = make([]*batch.Batch, 0)
	for idx := range ioVectors {
		bat := batch.NewWithSize(len(cols))
		var obj any
		for i := range cols {
			obj, err = objectio.Decode(ioVectors[idx].Entries[i].CachedData.Bytes())
			if err != nil {
				return
			}
			bat.Vecs[i] = obj.(*vector.Vector)
			bat.SetRowCount(bat.Vecs[i].Length())
		}
		bats = append(bats, bat)
	}
	return
}

// LoadColumns needs typs to generate columns, if the target table has no schema change, nil can be passed.
func (r *BlockReader) LoadOneSubColumns(
	ctx context.Context,
	cols []uint16,
	typs []types.Type,
	dataType uint16,
	blk uint16,
	m *mpool.MPool,
) (bat *batch.Batch, release func(), err error) {
	metaExt := r.reader.GetMetaExtent()
	if metaExt == nil || metaExt.End() == 0 {
		return
	}
	ioVector, err := r.reader.ReadOneSubBlock(ctx, cols, typs, dataType, blk, m)
	release = func() {
		objectio.ReleaseIOVector(&ioVector)
	}
	if err != nil {
		return
	}
	bat = batch.NewWithSize(len(cols))
	var obj any
	for i := range cols {
		obj, err = objectio.Decode(ioVector.Entries[i].CachedData.Bytes())
		if err != nil {
			return
		}
		bat.Vecs[i] = obj.(*vector.Vector)
		bat.SetRowCount(bat.Vecs[i].Length())
	}
	return
}

func (r *BlockReader) LoadAllColumns(
	ctx context.Context,
	idxs []uint16,
	m *mpool.MPool,
) ([]*batch.Batch, func(), error) {
	meta, err := r.reader.ReadAllMeta(ctx, m)
	if err != nil {
		return nil, nil, err
	}
	dataMeta := meta.MustDataMeta()
	if dataMeta.BlockHeader().MetaLocation().End() == 0 {
		return nil, nil, nil
	}
	block := dataMeta.GetBlockMeta(0)
	if len(idxs) == 0 {
		idxs = make([]uint16, block.GetColumnCount())
		for i := range idxs {
			idxs[i] = uint16(i)
		}
	}

	bats := make([]*batch.Batch, 0)

	ioVectors, err := r.reader.ReadAll(ctx, idxs, nil)
	if err != nil {
		return nil, nil, err
	}
	defer func() {
		if err != nil {
			objectio.ReleaseIOVector(&ioVectors)
		}
	}()
	for y := 0; y < int(dataMeta.BlockCount()); y++ {
		bat := batch.NewWithSize(len(idxs))
		var obj any
		for i := range idxs {
			obj, err = objectio.Decode(ioVectors.Entries[y*len(idxs)+i].CachedData.Bytes())
			if err != nil {
				return nil, nil, err
			}
			bat.Vecs[i] = obj.(*vector.Vector)
			bat.SetRowCount(bat.Vecs[i].Length())
		}
		bats = append(bats, bat)
	}
	return bats, func() { objectio.ReleaseIOVector(&ioVectors) }, nil
}

func (r *BlockReader) LoadZoneMaps(
	ctx context.Context,
	seqnums []uint16,
	id uint16,
	m *mpool.MPool,
) ([]objectio.ZoneMap, error) {
	return r.reader.ReadZM(ctx, id, seqnums, m)
}

func (r *BlockReader) LoadObjectMeta(ctx context.Context, m *mpool.MPool) (objectio.ObjectDataMeta, error) {
	meta, err := r.reader.ReadMeta(ctx, m)
	if err != nil {
		return nil, err
	}
	return meta.MustDataMeta(), nil
}

func (r *BlockReader) LoadAllBlocks(ctx context.Context, m *mpool.MPool) ([]objectio.BlockObject, error) {
	meta, err := r.reader.ReadAllMeta(ctx, m)
	if err != nil {
		return nil, err
	}
	dataMeta := meta.MustDataMeta()
	blocks := make([]objectio.BlockObject, dataMeta.BlockCount())
	for i := 0; i < int(dataMeta.BlockCount()); i++ {
		blocks[i] = dataMeta.GetBlockMeta(uint32(i))
	}
	return blocks, nil
}

func (r *BlockReader) LoadZoneMap(
	ctx context.Context,
	seqnums []uint16,
	block objectio.BlockObject,
	m *mpool.MPool) ([]objectio.ZoneMap, error) {
	return block.ToColumnZoneMaps(seqnums), nil
}

func (r *BlockReader) LoadOneBF(
	ctx context.Context,
	blk uint16,
) (objectio.StaticFilter, uint32, error) {
	return r.reader.ReadOneBF(ctx, blk)
}

func (r *BlockReader) LoadAllBF(
	ctx context.Context,
) (objectio.BloomFilter, uint32, error) {
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

func Prefetch(
	sid string,
	service fileservice.FileService,
	key objectio.Location,
) error {
	params, err := BuildPrefetchParams(service, key)
	if err != nil {
		return err
	}
	params.typ = PrefetchFileType
	return MustGetPipeline(sid).Prefetch(params)
}

func PrefetchMeta(
	sid string,
	service fileservice.FileService,
	key objectio.Location,
) error {
	params, err := BuildPrefetchParams(service, key)
	if err != nil {
		return err
	}
	params.typ = PrefetchMetaType
	return MustGetPipeline(sid).Prefetch(params)
}
