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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

//func LoadPersistedColumnData(
//	ctx context.Context,
//	rt *dbutils.Runtime,
//	id *common.ID,
//	def *catalog.ColDef,
//	location objectio.Location,
//	mp *mpool.MPool,
//) (vec containers.Vector, err error) {
//	if def.IsPhyAddr() {
//		return model.PreparePhyAddrData(&id.BlockID, 0, location.Rows(), rt.VectorPool.Transient)
//	}
//	//Extend lifetime of vectors is without the function.
//	//need to copy. closeFunc will be nil.
//	vectors, _, err := blockio.LoadColumns2(
//		ctx, []uint16{uint16(def.SeqNum)},
//		[]types.Type{def.Type},
//		rt.Fs.Service,
//		location,
//		fileservice.Policy(0),
//		true,
//		rt.VectorPool.Transient)
//	if err != nil {
//		return
//	}
//	return vectors[0], nil
//}

func LoadPersistedColumnDatas(
	ctx context.Context,
	schema *catalog.Schema,
	rt *dbutils.Runtime,
	id *common.ID,
	colIdxs []int,
	location objectio.Location,
	mp *mpool.MPool,
) ([]containers.Vector, error) {
	cols := make([]uint16, 0)
	typs := make([]types.Type, 0)
	vectors := make([]containers.Vector, len(colIdxs))
	phyAddIdx := -1
	for i, colIdx := range colIdxs {
		if colIdx == catalog.COLIDX_COMMITS {
			cols = append(cols, objectio.SEQNUM_COMMITTS)
			typs = append(typs, catalog.CommitTSType)
			continue
		}
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
	var vecs []containers.Vector
	var err error
	//Extend lifetime of vectors is without the function.
	//need to copy. closeFunc will be nil.
	vecs, _, err = blockio.LoadColumns2(
		ctx, cols,
		typs,
		rt.Fs.Service,
		location,
		fileservice.Policy(0),
		true,
		rt.VectorPool.Transient)
	if err != nil {
		return nil, err
	}
	for i, vec := range vecs {
		idx := i
		if idx >= phyAddIdx && phyAddIdx > -1 {
			idx++
		}
		vectors[idx] = vec
	}
	return vectors, nil
}

func MakeImmuIndex(
	ctx context.Context,
	meta *catalog.ObjectEntry,
	bf objectio.BloomFilter,
	rt *dbutils.Runtime,
) (idx indexwrapper.ImmutIndex, err error) {
	stats, err := meta.MustGetObjectStats()
	if err != nil {
		return
	}
	idx = indexwrapper.NewImmutIndex(
		stats.SortKeyZoneMap(), bf, stats.ObjectLocation(),
	)
	return
}
