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

package desginio

import (
	"context"
	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type DataReader struct {
	reader objectio.Reader
	fs     fileservice.FileService
	name   string
}

func NewDataReader(service fileservice.FileService, name string) (dataio.Reader, error) {
	reader, err := objectio.NewObjectReader(name, service)
	if err != nil {
		return nil, err
	}
	return &DataReader{
		reader: reader,
		fs:     service,
		name:   name,
	}, nil
}

func (r *DataReader) LoadColumns(ctx context.Context, idxs []uint16,
	extent objectio.Extent, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	return bat, nil
}

func (r *DataReader) LoadZoneMaps(ctx context.Context, idxs []uint16,
	extent objectio.Extent, m *mpool.MPool) ([]*index.ZoneMap, error) {
	idx := index.NewZoneMap(types.Type{})
	return []*index.ZoneMap{idx}, nil
}

func (r *DataReader) LoadBloomFilter(ctx context.Context, idx uint16,
	extent objectio.Extent, m *mpool.MPool) (index.StaticFilter, error) {
	bf, _ := index.NewBinaryFuseFilter(nil)
	return bf, nil
}

func (r *DataReader) LoadMeta(ctx context.Context, extent objectio.Extent, m *mpool.MPool) (objectio.BlockObject, error) {
	block, _ := r.reader.ReadMeta(ctx, []objectio.Extent{extent}, m)
	return block[0], nil
}

func (r *DataReader) LoadAllMetas(ctx context.Context, extent objectio.Extent, m *mpool.MPool) ([]objectio.BlockObject, error) {
	block, _ := r.reader.ReadMeta(ctx, []objectio.Extent{extent}, m)
	return block, nil
}

func (r *DataReader) LoadAllData(ctx context.Context, extent objectio.Extent, m *mpool.MPool) ([]*batch.Batch, error) {
	bat := batch.NewWithSize(10)
	return []*batch.Batch{bat}, nil
}

func (r *DataReader) LoadColumnsByTS(ctx context.Context, idxs []uint16, info *pkgcatalog.BlockInfo,
	ts timestamp.Timestamp, m *mpool.MPool) (*batch.Batch, error) {
	bat := batch.NewWithSize(len(idxs))
	return bat, nil
}
