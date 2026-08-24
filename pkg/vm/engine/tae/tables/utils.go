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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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

func closePersistedVectors(vectors []containers.Vector) {
	for _, vec := range vectors {
		if vec != nil {
			vec.Close()
		}
	}
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
	if ctx == nil || schema == nil || rt == nil || id == nil || mp == nil || rt.Fs == nil ||
		rt.VectorPool.Transient == nil {
		return nil, nil, nil, moerr.NewInvalidInputNoCtx(
			"persisted column load requires context, schema, runtime, id, file service, vector pool, and mpool",
		)
	}
	cols := make([]uint16, 0, len(colIdxs))
	typs := make([]types.Type, 0, len(colIdxs))
	vectors := make([]containers.Vector, len(colIdxs))
	outputPositions := make([]int, 0, len(colIdxs))
	physicalAddrPositions := make([]int, 0, 1)
	commitTSIdx := -1
	abortIdx := -1
	tombstoneRowIDIdx := -1
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
		if colIdx < 0 || colIdx >= len(schema.ColDefs) {
			closePersistedVectors(vectors)
			return nil, deletes, nil, moerr.NewInvalidInputNoCtxf(
				"persisted column index %d is outside schema with %d columns",
				colIdx, len(schema.ColDefs),
			)
		}
		def := schema.ColDefs[colIdx]
		if def == nil {
			closePersistedVectors(vectors)
			return nil, deletes, nil, moerr.NewInternalErrorNoCtxf(
				"persisted schema column %d is nil", colIdx,
			)
		}
		if def.IsPhyAddr() {
			// Synthesize the physical address only after the actual block row
			// count is known. Location.Rows can be an ObjectStats-derived
			// estimate for intermediate short blocks.
			physicalAddrPositions = append(physicalAddrPositions, i)
			continue
		}
		cols = append(cols, def.SeqNum)
		typs = append(typs, def.Type)
		outputPositions = append(outputPositions, i)
		if isTombstone && def.SeqNum == objectio.TombstoneAttr_Rowid_SeqNum {
			tombstoneRowIDIdx = len(cols) - 1
		}
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
		objectMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, rt.Fs)
		if err != nil {
			return nil, deletes, nil, err
		}
		dataMeta, metaErr := ioutil.GetDataMetaForLocation(objectMeta, location)
		if metaErr != nil {
			return nil, deletes, nil, metaErr
		}
		rowCount := dataMeta.GetBlockMeta(uint32(location.ID())).GetRows()
		for _, outputPos := range physicalAddrPositions {
			vectors[outputPos], err = PreparePhyAddrData(
				&id.BlockID, 0, rowCount, rt.VectorPool.Transient,
			)
			if err != nil {
				closePersistedVectors(vectors)
				return nil, deletes, nil, err
			}
		}
		return vectors, deletes, nil, nil
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
		closePersistedVectors(vectors)
		return nil, deletes, nil, err
	}
	cleanupLoaded := func(closeOutputs bool) {
		closePersistedVectors(vecs)
		vecs = nil
		if release != nil {
			release()
			release = nil
		}
		if closeOutputs {
			closePersistedVectors(vectors)
			for i := range vectors {
				vectors[i] = nil
			}
		}
	}

	rowCount := 0
	if len(vecs) > 0 && vecs[0] != nil {
		rowCount = vecs[0].Length()
	}
	validateLoaded := func() error {
		if len(vecs) != len(cols) {
			return moerr.NewInternalErrorNoCtxf(
				"persisted block returned %d columns, expected %d", len(vecs), len(cols),
			)
		}
		for pos, vec := range vecs {
			if vec == nil || vec.Length() != rowCount {
				return moerr.NewInternalErrorNoCtxf(
					"persisted block column %d has invalid logical row count", pos,
				)
			}
		}
		if tombstoneRowIDIdx >= 0 {
			if _, validateErr := ioutil.ValidateTombstoneRowIDColumn(
				rowCount, vecs[tombstoneRowIDIdx].GetDownstreamVector(),
			); validateErr != nil {
				return moerr.NewInternalErrorNoCtxf(
					"persisted tombstone has invalid rowid column: %v", validateErr,
				)
			}
		}
		return nil
	}
	if err = validateLoaded(); err != nil {
		cleanupLoaded(true)
		return nil, deletes, nil, err
	}
	for _, outputPos := range physicalAddrPositions {
		vectors[outputPos], err = PreparePhyAddrData(
			&id.BlockID, 0, uint32(rowCount), rt.VectorPool.Transient,
		)
		if err != nil {
			cleanupLoaded(true)
			return nil, deletes, nil, err
		}
	}
	var commitTSs ioutil.TombstoneCommitTSColumn
	if commitTSIdx >= 0 && (isTombstone || tsForAppendable != nil) {
		commitTSs, err = ioutil.ValidateTombstoneCommitTSColumn(
			rowCount,
			vecs[commitTSIdx].GetDownstreamVector(),
		)
		if err != nil && isTombstone && vecs[commitTSIdx].IsConstNull() {
			objectMeta, metaErr := objectio.FastLoadObjectMeta(ctx, &location, false, rt.Fs)
			if metaErr != nil {
				cleanupLoaded(true)
				return nil, deletes, nil, metaErr
			}
			dataMeta, dataMetaErr := ioutil.GetDataMetaForLocation(objectMeta, location)
			if dataMetaErr != nil {
				cleanupLoaded(true)
				return nil, deletes, nil, dataMetaErr
			}
			blockMeta := dataMeta.GetBlockMeta(uint32(location.ID()))
			if legacyCommitTS, ok := ioutil.ResolveLegacyBackupTombstoneCommitTS(blockMeta); ok {
				cleanupLoaded(false)
				cols[commitTSIdx] = legacyCommitTS
				vecs, release, err = ioutil.LoadColumns2(
					ctx,
					cols,
					typs,
					rt.Fs,
					location,
					fileservice.GetFileServicePolicy(ctx),
					needCopy,
					rt.VectorPool.Transient,
				)
				if err != nil {
					closePersistedVectors(vectors)
					return nil, deletes, nil, err
				}
				if err = validateLoaded(); err != nil {
					cleanupLoaded(true)
					return nil, deletes, nil, err
				}
				commitTSs, err = ioutil.ValidateTombstoneCommitTSColumn(
					rowCount,
					vecs[commitTSIdx].GetDownstreamVector(),
				)
			} else if isCNCreatedTombstoneBlock(blockMeta) {
				// CN-created tombstones persist only rowid and primary key. Their
				// containing object's CreatedAt is the logical commit timestamp.
				commitTSs = ioutil.TombstoneCommitTSColumn{}
				err = nil
			}
		}
		if err != nil {
			cleanupLoaded(true)
			return nil, deletes, nil, err
		}
	}

	var aborts ioutil.TombstoneAbortColumn
	if abortIdx >= 0 {
		aborts, err = ioutil.ValidateTombstoneAbortColumn(
			rowCount,
			vecs[abortIdx].GetDownstreamVector(),
		)
		if err != nil {
			cleanupLoaded(true)
			return nil, deletes, nil, err
		}
	}
	if tsForAppendable != nil {
		if !commitTSs.IsPresent() {
			cleanupLoaded(true)
			return nil, deletes, nil, moerr.NewInvalidInputNoCtx(
				"appendable object commit-ts column is unavailable",
			)
		}
		for i := 0; i < rowCount; i++ {
			commitTS := commitTSs.At(i)
			if commitTS.Equal(&txnif.UncommitTS) || commitTS.GT(tsForAppendable) ||
				(aborts.IsPresent() && aborts.At(i)) {
				if deletes == nil {
					deletes = nulls.NewWithSize(rowCount)
				}
				deletes.Add(uint64(i))
			}
		}
	}
	for pos, vec := range vectors {
		if vec != nil && vec.Length() != rowCount {
			cleanupLoaded(true)
			return nil, deletes, nil, moerr.NewInternalErrorNoCtxf(
				"persisted synthesized column %d has invalid logical row count", pos,
			)
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

func isCNCreatedTombstoneBlock(block objectio.BlockObject) bool {
	baseColumns := uint16(len(objectio.TombstoneSeqnums_CN_Created))
	if block.GetColumnCount() < baseColumns || block.GetMetaColumnCount() < baseColumns ||
		block.BlockHeader().Appendable() ||
		block.GetMaxSeqnum() != objectio.TombstoneAttr_PK_SeqNum ||
		block.ColumnMeta(objectio.TombstoneAttr_Rowid_SeqNum).DataType() != uint8(types.T_Rowid) ||
		block.ColumnMeta(objectio.TombstoneAttr_PK_SeqNum).DataType() == uint8(types.T_any) {
		return false
	}
	layout := objectio.ResolveSpecialColumnLayout(block)
	if _, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
		return false
	}
	if _, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
		return false
	}
	expectedColumns := baseColumns
	if layout.PhysicalAddr != objectio.InvalidSpecialColumnPosition {
		if layout.PhysicalAddr != expectedColumns ||
			block.ColumnMeta(layout.PhysicalAddr).DataType() != uint8(types.T_Rowid) {
			return false
		}
		expectedColumns++
	}
	return block.GetColumnCount() == expectedColumns &&
		block.GetMetaColumnCount() == expectedColumns
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
