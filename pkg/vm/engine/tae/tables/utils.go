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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/indexwrapper"
)

func PreparePhyAddrData(
	id *objectio.Blockid, startRow, length uint32, pool *containers.VectorPool,
) (col containers.Vector, err error) {
	col = pool.GetVector(&objectio.RowidType)
	vec := col.GetDownstreamVector()
	m := col.GetAllocator()
	if err = objectio.ConstructRowidColumnTo(
		vec, id, startRow, length, m,
	); err != nil {
		col.Close()
		col = nil
	}
	return
}

func LoadPersistedColumnData(
	ctx context.Context,
	schema *catalog.Schema,
	rt *dbutils.Runtime,
	id *common.ID,
	colIdxs []int,
	location objectio.Location,
	mp *mpool.MPool,
	tsForAppendable *types.TS,
	needCopy bool,
) ([]containers.Vector, *nulls.Nulls, func(), error) {
	cols := make([]uint16, 0, len(colIdxs))
	typs := make([]types.Type, 0, len(colIdxs))
	vectors := make([]containers.Vector, len(colIdxs))
	outputPositions := make([]int, 0, len(colIdxs))
	commitTSIdx := -1
	abortIdx := -1
	var deletes *nulls.Nulls
	for i, colIdx := range colIdxs {
		switch colIdx {
		case objectio.SEQNUM_COMMITTS:
			cols = append(cols, objectio.SEQNUM_COMMITTS)
			typs = append(typs, objectio.TSType)
			outputPositions = append(outputPositions, i)
			commitTSIdx = len(cols) - 1
			continue
		case objectio.SEQNUM_ABORT:
			cols = append(cols, objectio.SEQNUM_ABORT)
			typs = append(typs, types.T_bool.ToType())
			outputPositions = append(outputPositions, i)
			abortIdx = len(cols) - 1
			continue
		}
		def := schema.ColDefs[colIdx]
		if def.IsPhyAddr() {
			vec, err := PreparePhyAddrData(&id.BlockID, 0, location.Rows(), rt.VectorPool.Transient)
			if err != nil {
				for _, existing := range vectors {
					if existing != nil {
						existing.Close()
					}
				}
				return nil, deletes, nil, err
			}
			vectors[i] = vec
			continue
		}
		cols = append(cols, def.SeqNum)
		typs = append(typs, def.Type)
		outputPositions = append(outputPositions, i)
	}
	if tsForAppendable != nil {
		if commitTSIdx == -1 {
			cols = append(cols, objectio.SEQNUM_COMMITTS)
			typs = append(typs, types.T_TS.ToType())
			outputPositions = append(outputPositions, -1)
			commitTSIdx = len(cols) - 1
		}
		if abortIdx == -1 {
			cols = append(cols, objectio.SEQNUM_ABORT)
			typs = append(typs, types.T_bool.ToType())
			outputPositions = append(outputPositions, -1)
			abortIdx = len(cols) - 1
		}
	}
	if len(cols) == 0 {
		return vectors, deletes, nil, nil
	}
	if commitTSIdx >= 0 {
		meta, metaErr := objectio.FastLoadObjectMeta(ctx, &location, false, rt.Fs)
		if metaErr != nil {
			return nil, deletes, nil, metaErr
		}
		block := meta.MustGetMeta(objectio.SchemaData).GetBlockMeta(uint32(location.ID()))
		layout := objectio.ResolveSpecialColumnLayout(block)
		if _, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); !ok &&
			!block.BlockHeader().Appendable() &&
			block.GetColumnCount() == 3 &&
			block.GetMaxSeqnum() == 2 &&
			block.ColumnMeta(0).DataType() == uint8(types.T_Rowid) &&
			block.ColumnMeta(2).DataType() == uint8(types.T_TS) {
			// Backup rewrites from older releases stored commitTS as the
			// trailing ordinary column instead of declaring it special.
			cols[commitTSIdx] = 2
		}
	}
	var vecs []containers.Vector
	var release func()
	var err error
	vecs, release, err = ioutil.LoadColumns2(
		ctx, cols,
		typs,
		rt.Fs,
		location,
		fileservice.GetFileServicePolicy(ctx),
		needCopy,
		rt.VectorPool.Transient)
	if err != nil {
		for _, vec := range vectors {
			if vec != nil {
				vec.Close()
			}
		}
		return nil, deletes, nil, err
	}
	if tsForAppendable != nil {
		commits := vector.MustFixedColNoTypeCheck[types.TS](vecs[commitTSIdx].GetDownstreamVector())
		abortVec := vecs[abortIdx]
		var aborts []bool
		if !abortVec.IsConstNull() {
			aborts = vector.MustFixedColNoTypeCheck[bool](abortVec.GetDownstreamVector())
		}
		for i := range commits {
			if commits[i].GT(tsForAppendable) || (aborts != nil && aborts[i]) {
				if deletes == nil {
					deletes = nulls.NewWithSize(int(location.Rows()))
				}
				deletes.Add(uint64(i))
			}
		}
	}
	for i, vec := range vecs {
		outputPos := outputPositions[i]
		if outputPos == -1 {
			vec.Close()
			continue
		}
		vectors[outputPos] = vec
	}
	return vectors, deletes, release, nil
}

func MakeImmuIndex(
	ctx context.Context,
	meta *catalog.ObjectEntry,
	bf objectio.BloomFilter,
	rt *dbutils.Runtime,
) (idx indexwrapper.ImmutIndex, err error) {
	idx = indexwrapper.NewImmutIndex(
		meta.SortKeyZoneMap(), bf, meta.ObjectLocation(),
	)
	return
}
