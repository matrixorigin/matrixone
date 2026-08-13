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
	isTombstone bool,
) ([]containers.Vector, *nulls.Nulls, func(), error) {
	cols := make([]uint16, 0, len(colIdxs))
	typs := make([]types.Type, 0, len(colIdxs))
	vectors := make([]containers.Vector, len(colIdxs))
	phyAddIdx := -1
	committsIdx := -1
	assignedCommitts := false
	var deletes *nulls.Nulls
	for i, colIdx := range colIdxs {
		if colIdx == objectio.SEQNUM_COMMITTS {
			cols = append(cols, objectio.SEQNUM_COMMITTS)
			typs = append(typs, objectio.TSType)
			committsIdx = len(cols) - 1
			assignedCommitts = true
			continue
		}
		def := schema.ColDefs[colIdx]
		if def.IsPhyAddr() {
			vec, err := PreparePhyAddrData(&id.BlockID, 0, location.Rows(), rt.VectorPool.Transient)
			if err != nil {
				return nil, deletes, nil, err
			}
			phyAddIdx = i
			vectors[phyAddIdx] = vec
			continue
		}
		cols = append(cols, def.SeqNum)
		typs = append(typs, def.Type)
	}
	if len(cols) == 0 {
		return vectors, deletes, nil, nil
	}
	if tsForAppendable != nil {
		if committsIdx == -1 {
			cols = append(cols, objectio.SEQNUM_COMMITTS)
			typs = append(typs, types.T_TS.ToType())
			committsIdx = len(cols) - 1
			defer func() {
				cols = cols[:len(cols)-1]
				typs = typs[:len(typs)-1]
			}()
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
		if phyAddIdx >= 0 && vectors[phyAddIdx] != nil {
			vectors[phyAddIdx].Close()
		}
		return nil, deletes, nil, err
	}
	cleanupLoaded := func(closePhysicalAddr bool) {
		for _, vec := range vecs {
			vec.Close()
		}
		if release != nil {
			release()
			release = nil
		}
		vecs = nil
		if closePhysicalAddr && phyAddIdx >= 0 && vectors[phyAddIdx] != nil {
			vectors[phyAddIdx].Close()
			vectors[phyAddIdx] = nil
		}
	}
	var validatedTombstoneCommitTS ioutil.TombstoneCommitTSColumn
	if isTombstone && committsIdx >= 0 {
		// The legacy physical-column mapping is intentionally gated by catalog
		// tombstone identity: an ordinary three-column object can have the same
		// physical metadata signature.
		validatedTombstoneCommitTS, err = ioutil.ValidateTombstoneCommitTSColumn(
			int(location.Rows()),
			vecs[committsIdx].GetDownstreamVector(),
		)
		if err != nil && vecs[committsIdx].IsConstNull() {
			objectMeta, metaErr := objectio.FastLoadObjectMeta(ctx, &location, false, rt.Fs)
			if metaErr != nil {
				cleanupLoaded(true)
				return nil, deletes, nil, metaErr
			}
			blockMeta := objectMeta.MustDataMeta().GetBlockMeta(uint32(location.ID()))
			if legacyCommitTS, ok := ioutil.ResolveLegacyBackupTombstoneCommitTS(blockMeta); ok {
				cleanupLoaded(false)
				cols[committsIdx] = legacyCommitTS
				vecs, release, err = ioutil.LoadColumns2(
					ctx, cols,
					typs,
					rt.Fs,
					location,
					fileservice.GetFileServicePolicy(ctx),
					needCopy,
					rt.VectorPool.Transient,
				)
				if err != nil {
					if phyAddIdx >= 0 && vectors[phyAddIdx] != nil {
						vectors[phyAddIdx].Close()
					}
					return nil, deletes, nil, err
				}
				validatedTombstoneCommitTS, err = ioutil.ValidateTombstoneCommitTSColumn(
					int(location.Rows()),
					vecs[committsIdx].GetDownstreamVector(),
				)
			} else if isCNCreatedTombstoneBlock(blockMeta) {
				// CN-created tombstones persist only rowid and primary key. Their
				// per-object CreatedAt is the logical commit timestamp, so keep
				// the synthesized const-null vector for the caller to replace.
				err = nil
			}
		}
		if err != nil {
			cleanupLoaded(true)
			return nil, deletes, nil, err
		}
	}
	if tsForAppendable != nil {
		if validatedTombstoneCommitTS.IsPresent() {
			for i := range int(location.Rows()) {
				commitTS := validatedTombstoneCommitTS.At(i)
				if commitTS.GT(tsForAppendable) {
					if deletes == nil {
						deletes = nulls.NewWithSize(int(location.Rows()))
					}
					deletes.Add(uint64(i))
				}
			}
		} else {
			commits := vector.MustFixedColNoTypeCheck[types.TS](vecs[committsIdx].GetDownstreamVector())
			for i := range commits {
				if !commits[i].GT(tsForAppendable) {
					continue
				}
				if deletes == nil {
					deletes = nulls.NewWithSize(int(location.Rows()))
				}
				deletes.Add(uint64(i))
			}
		}
		if !assignedCommitts {
			vecs[committsIdx].Close()
			vecs = vecs[:len(vecs)-1]
		}
	}
	for i, vec := range vecs {
		idx := i
		if idx >= phyAddIdx && phyAddIdx > -1 {
			idx++
		}
		vectors[idx] = vec
	}
	return vectors, deletes, release, nil
}

func isCNCreatedTombstoneBlock(block objectio.BlockObject) bool {
	// Preserve the established two-column [rowid, pk] layout whose logical
	// commit timestamp is the containing object's CreatedAt.
	return !block.BlockHeader().Appendable() &&
		block.GetColumnCount() == uint16(len(objectio.TombstoneSeqnums_CN_Created)) &&
		block.GetMetaColumnCount() == uint16(len(objectio.TombstoneSeqnums_CN_Created)) &&
		block.GetMaxSeqnum() == objectio.TombstoneAttr_PK_SeqNum &&
		block.ColumnMeta(objectio.TombstoneAttr_Rowid_SeqNum).DataType() == uint8(types.T_Rowid) &&
		block.ColumnMeta(objectio.TombstoneAttr_PK_SeqNum).DataType() != uint8(types.T_any)
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
