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

package tables

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func LoadPersistedColumnData(
	ctx context.Context,
	rt *dbutils.Runtime,
	id *common.ID,
	def *catalog.ColDef,
	location objectio.Location,
) (vec containers.Vector, err error) {
	if def.IsPhyAddr() {
		return model.PreparePhyAddrData(&id.BlockID, 0, location.Rows(), rt.VectorPool.Transient)
	}
	bat, err := blockio.LoadColumns(
		ctx, []uint16{uint16(def.SeqNum)},
		[]types.Type{def.Type},
		rt.Fs.Service,
		location,
		nil)
	if err != nil {
		return
	}
	return containers.ToDNVector(bat.Vecs[0]), nil
}

func LoadPersistedColumnDatas(
	ctx context.Context,
	schema *catalog.Schema,
	rt *dbutils.Runtime,
	id *common.ID,
	colIdxs []int,
	location objectio.Location,
) ([]containers.Vector, error) {
	cols := make([]uint16, 0)
	typs := make([]types.Type, 0)
	vectors := make([]containers.Vector, len(colIdxs))
	phyAddIdx := -1
	for i, colIdx := range colIdxs {
		def := schema.ColDefs[colIdx]
		if def.IsPhyAddr() {
			vec, err := model.PreparePhyAddrData(&id.BlockID, 0, location.Rows(), rt.VectorPool.Transient)
			if err != nil {
				return nil, err
			}
			phyAddIdx = i
			vectors[phyAddIdx] = vec
			continue
		}
		cols = append(cols, def.SeqNum)
		typs = append(typs, def.Type)
	}
	if len(cols) == 0 {
		return vectors, nil
	}
	bat, err := blockio.LoadColumns(
		ctx, cols,
		typs,
		rt.Fs.Service,
		location,
		nil)
	if err != nil {
		return nil, err
	}
	for i, vec := range bat.Vecs {
		idx := i
		if idx >= phyAddIdx && phyAddIdx > -1 {
			idx++
		}
		vectors[idx] = containers.ToDNVector(vec)
	}
	return vectors, nil
}

func ReadPersistedBlockRow(location objectio.Location) int {
	return int(location.Rows())
}

func LoadPersistedDeletes(
	ctx context.Context,
	pkName string,
	fs *objectio.ObjectFS,
	location objectio.Location) (bat *containers.Batch, err error) {
	movbat, err := blockio.LoadTombstoneColumns(ctx, []uint16{0, 1, 2, 3}, nil, fs.Service, location, nil)
	if err != nil {
		return
	}
	bat = containers.NewBatch()
	colNames := []string{catalog.PhyAddrColumnName, catalog.AttrCommitTs, pkName, catalog.AttrAborted}
	for i := 0; i < 4; i++ {
		bat.AddVector(colNames[i], containers.ToDNVector(movbat.Vecs[i]))
	}
	return
}

// func MakeBFLoader(
// 	meta *catalog.BlockEntry,
// 	bf objectio.BloomFilter,
// 	cache model.LRUCache,
// 	fs fileservice.FileService,
// ) indexwrapper.Loader {
// 	return func(ctx context.Context) ([]byte, error) {
// 		location := meta.GetMetaLoc()
// 		var err error
// 		if len(bf) == 0 {
// 			if bf, err = LoadBF(ctx, location, cache, fs, false); err != nil {
// 				return nil, err
// 			}
// 		}
// 		return bf.GetBloomFilter(uint32(location.ID())), nil
// 	}
// }

func MakeImmuIndex(
	ctx context.Context,
	meta *catalog.BlockEntry,
	bf objectio.BloomFilter,
	rt *dbutils.Runtime,
) (idx indexwrapper.ImmutIndex, err error) {
	pkZM, err := meta.GetPKZoneMap(ctx, rt.Fs.Service)
	if err != nil {
		return
	}
	idx = indexwrapper.NewImmutIndex(
		*pkZM, bf, meta.GetMetaLoc(),
	)
	return
}
