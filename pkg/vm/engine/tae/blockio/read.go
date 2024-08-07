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
	"math"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
)

func removeIf[T any](data []T, pred func(t T) bool) []T {
	// from plan.RemoveIf
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

type ReadFilterSearchFuncType func([]*vector.Vector) []int64

type BlockReadFilter struct {
	HasFakePK          bool
	Valid              bool
	SortedSearchFunc   ReadFilterSearchFuncType
	UnSortedSearchFunc ReadFilterSearchFuncType
}

// ReadDataByFilter only read block data from storage by filter, don't apply deletes.
func ReadDataByFilter(
	ctx context.Context,
	sid string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	searchFunc ReadFilterSearchFuncType,
	fs fileservice.FileService,
	mp *mpool.MPool,
	tableName string,
) (sels []int64, err error) {
	bat, rowidIdx, deleteMask, release, err := readBlockData(ctx, columns, colTypes, info, ts, fs, mp, nil, fileservice.Policy(0))
	if err != nil {
		return
	}
	defer release()
	if rowidIdx >= 0 {
		panic("use rowid to filter, seriouslly?")
	}

	sels = searchFunc(bat.Vecs)
	if !deleteMask.IsEmpty() {
		sels = removeIf(sels, func(i int64) bool {
			return deleteMask.Contains(uint64(i))
		})
	}
	sels, err = ds.ApplyTombstones(ctx, info.BlockID, sels)
	if err != nil {
		return
	}
	return
}

// BlockDataReadNoCopy only read block data from storage, don't apply deletes.
func BlockDataReadNoCopy(
	ctx context.Context,
	sid string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	fs fileservice.FileService,
	mp *mpool.MPool,
	vp engine.VectorPool,
	policy fileservice.Policy,
) (*batch.Batch, *nulls.Bitmap, func(), error) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	var (
		rowidPos   int
		deleteMask nulls.Bitmap
		loaded     *batch.Batch
		release    func()
		err        error
	)

	defer func() {
		if err != nil {
			if release != nil {
				release()
			}
		}
	}()

	// read block data from storage specified by meta location
	if loaded, rowidPos, deleteMask, release, err = readBlockData(
		ctx, columns, colTypes, info, ts, fs, mp, vp, policy,
	); err != nil {
		return nil, nil, nil, err
	}
	tombstones, err := ds.GetTombstones(ctx, info.BlockID)
	if err != nil {
		return nil, nil, nil, err
	}

	// merge deletes from tombstones
	deleteMask.Merge(tombstones)

	// build rowid column if needed
	if rowidPos >= 0 {
		if loaded.Vecs[rowidPos], err = buildRowidColumn(
			info, nil, mp, vp,
		); err != nil {

			return nil, nil, nil, err
		}
		release = func() {
			release()
			loaded.Vecs[rowidPos].Free(mp)
		}
	}
	loaded.SetRowCount(loaded.Vecs[0].Length())
	return loaded, &deleteMask, release, nil
}

// BlockDataRead only read block data from storage, don't apply deletes.
func BlockDataRead(
	ctx context.Context,
	sid string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts timestamp.Timestamp,
	filterSeqnums []uint16,
	filterColTypes []types.Type,
	filter BlockReadFilter,
	fs fileservice.FileService,
	mp *mpool.MPool,
	vp engine.VectorPool,
	policy fileservice.Policy,
	tableName string,
) (*batch.Batch, error) {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	var (
		sels []int64
		err  error
	)

	var searchFunc ReadFilterSearchFuncType
	if (filter.HasFakePK || !info.Sorted) && filter.UnSortedSearchFunc != nil {
		searchFunc = filter.UnSortedSearchFunc
	} else if info.Sorted && filter.SortedSearchFunc != nil {
		searchFunc = filter.SortedSearchFunc
	}

	if searchFunc != nil {
		if sels, err = ReadDataByFilter(
			ctx, sid, info, ds, filterSeqnums, filterColTypes,
			types.TimestampToTS(ts), searchFunc, fs, mp, tableName,
		); err != nil {
			return nil, err
		}
		v2.TaskSelReadFilterTotal.Inc()
		if len(sels) == 0 {
			RecordReadFilterSelectivity(sid, 1, 1)
			v2.TaskSelReadFilterHit.Inc()
		} else {
			RecordReadFilterSelectivity(sid, 0, 1)
		}

		if len(sels) == 0 {
			result := batch.NewWithSize(len(colTypes))
			for i, typ := range colTypes {
				if vp == nil {
					result.Vecs[i] = vector.NewVec(typ)
				} else {
					result.Vecs[i] = vp.GetVector(typ)
				}
			}
			return result, nil
		}
	}

	columnBatch, err := BlockDataReadInner(
		ctx, sid, info, ds, columns, colTypes,
		types.TimestampToTS(ts), sels, fs, mp, vp, policy,
	)
	if err != nil {
		return nil, err
	}

	columnBatch.SetRowCount(columnBatch.Vecs[0].Length())
	return columnBatch, nil
}

func BlockCompactionRead(
	ctx context.Context,
	location objectio.Location,
	deletes []int64,
	seqnums []uint16,
	colTypes []types.Type,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (*batch.Batch, error) {

	loaded, release, err := LoadColumns(ctx, seqnums, colTypes, fs, location, mp, fileservice.Policy(0))
	if err != nil {
		return nil, err
	}
	defer release()
	if len(deletes) == 0 {
		return loaded, nil
	}
	result := batch.NewWithSize(len(loaded.Vecs))
	for i, col := range loaded.Vecs {
		typ := *col.GetType()
		result.Vecs[i] = vector.NewVec(typ)
		if err = vector.GetUnionAllFunction(typ, mp)(result.Vecs[i], col); err != nil {
			break
		}
		result.Vecs[i].Shrink(deletes, true)
	}

	if err != nil {
		for _, col := range result.Vecs {
			if col != nil {
				col.Free(mp)
			}
		}
		return nil, err
	}
	result.SetRowCount(result.Vecs[0].Length())
	return result, nil
}

// BlockDataReadInner only read data,don't apply deletes.
func BlockDataReadInner(
	ctx context.Context,
	sid string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	selectRows []int64, // if selectRows is not empty, it was already filtered by filter
	fs fileservice.FileService,
	mp *mpool.MPool,
	vp engine.VectorPool,
	policy fileservice.Policy,
) (result *batch.Batch, err error) {
	var (
		rowidPos    int
		deletedRows []int64
		deleteMask  nulls.Bitmap
		loaded      *batch.Batch
		release     func()
	)

	// read block data from storage specified by meta location
	if loaded, rowidPos, deleteMask, release, err = readBlockData(
		ctx, columns, colTypes, info, ts, fs, mp, vp, policy,
	); err != nil {
		return
	}
	defer release()
	// assemble result batch for return
	result = batch.NewWithSize(len(loaded.Vecs))

	if len(selectRows) > 0 {
		// NOTE: it always goes here if there is a filter and the block is sorted
		// and there are selected rows after applying the filter and delete mask

		// build rowid column if needed
		if rowidPos >= 0 {
			if loaded.Vecs[rowidPos], err = buildRowidColumn(
				info, selectRows, mp, vp,
			); err != nil {
				return
			}
		}

		// assemble result batch only with selected rows
		for i, col := range loaded.Vecs {
			typ := *col.GetType()
			if typ.Oid == types.T_Rowid {
				result.Vecs[i] = col
				continue
			}
			if vp == nil {
				result.Vecs[i] = vector.NewVec(typ)
			} else {
				result.Vecs[i] = vp.GetVector(typ)
			}
			if err = result.Vecs[i].PreExtendWithArea(len(selectRows), 0, mp); err != nil {
				break
			}
			if err = result.Vecs[i].Union(col, selectRows, mp); err != nil {
				break
			}
		}
		if err != nil {
			for _, col := range result.Vecs {
				if col != nil {
					col.Free(mp)
				}
			}
		}
		return
	}

	tombstones, err := ds.GetTombstones(ctx, info.BlockID)
	if err != nil {
		return
	}

	// merge deletes from tombstones
	deleteMask.Merge(tombstones)

	// Note: it always goes here if no filter or the block is not sorted

	// transform delete mask to deleted rows
	// TODO: avoid this transformation
	if !deleteMask.IsEmpty() {
		deletedRows = deleteMask.ToI64Arrary()
		// logutil.Debugf("deleted/length: %d/%d=%f",
		// 	len(deletedRows),
		// 	loaded.Vecs[0].Length(),
		// 	float64(len(deletedRows))/float64(loaded.Vecs[0].Length()))
	}

	// build rowid column if needed
	if rowidPos >= 0 {
		if loaded.Vecs[rowidPos], err = buildRowidColumn(
			info, nil, mp, vp,
		); err != nil {
			return
		}
	}

	// assemble result batch
	for i, col := range loaded.Vecs {
		typ := *col.GetType()

		if typ.Oid == types.T_Rowid {
			// rowid is already allocted by the mpool, no need to create a new vector
			result.Vecs[i] = col
		} else {
			// for other types, we need to create a new vector
			if vp == nil {
				result.Vecs[i] = vector.NewVec(typ)
			} else {
				result.Vecs[i] = vp.GetVector(typ)
			}
			// copy the data from loaded vector to result vector
			// TODO: avoid this allocation and copy
			if err = vector.GetUnionAllFunction(typ, mp)(result.Vecs[i], col); err != nil {
				break
			}
		}
		if len(deletedRows) > 0 {
			result.Vecs[i].Shrink(deletedRows, true)
		}
	}

	// if any error happens, free the result batch allocated
	if err != nil {
		for _, col := range result.Vecs {
			if col != nil {
				col.Free(mp)
			}
		}
	}
	return
}

func getRowsIdIndex(colIndexes []uint16, colTypes []types.Type) (int, []uint16, []types.Type) {
	idx := -1
	for i, typ := range colTypes {
		if typ.Oid == types.T_Rowid {
			idx = i
			break
		}
	}
	if idx < 0 {
		return idx, colIndexes, colTypes
	}
	idxes := make([]uint16, 0, len(colTypes)-1)
	typs := make([]types.Type, 0, len(colTypes)-1)
	idxes = append(idxes, colIndexes[:idx]...)
	idxes = append(idxes, colIndexes[idx+1:]...)
	typs = append(typs, colTypes[:idx]...)
	typs = append(typs, colTypes[idx+1:]...)
	return idx, idxes, typs
}

func buildRowidColumn(
	info *objectio.BlockInfo,
	sels []int64,
	m *mpool.MPool,
	vp engine.VectorPool,
) (col *vector.Vector, err error) {
	if vp == nil {
		col = vector.NewVec(objectio.RowidType)
	} else {
		col = vp.GetVector(objectio.RowidType)
	}
	if len(sels) == 0 {
		err = objectio.ConstructRowidColumnTo(
			col,
			&info.BlockID,
			0,
			info.MetaLocation().Rows(),
			m,
		)
	} else {
		err = objectio.ConstructRowidColumnToWithSels(
			col,
			&info.BlockID,
			sels,
			m,
		)
	}
	if err != nil {
		col.Free(m)
		col = nil
	}
	return
}

func readBlockData(
	ctx context.Context,
	colIndexes []uint16,
	colTypes []types.Type,
	info *objectio.BlockInfo,
	ts types.TS,
	fs fileservice.FileService,
	m *mpool.MPool,
	vp engine.VectorPool,
	policy fileservice.Policy,
) (bat *batch.Batch, rowidPos int, deleteMask nulls.Bitmap, release func(), err error) {
	rowidPos, idxes, typs := getRowsIdIndex(colIndexes, colTypes)

	readColumns := func(cols []uint16) (result *batch.Batch, loaded *batch.Batch, err error) {
		if len(cols) == 0 && rowidPos >= 0 {
			// only read rowid column on non appendable block, return early
			result = batch.NewWithSize(1)
			// result.Vecs[0] = rowid
			release = func() {}
			return
		}

		if loaded, release, err = LoadColumns(ctx, cols, typs, fs, info.MetaLocation(), m, policy); err != nil {
			return
		}
		colPos := 0
		result = batch.NewWithSize(len(colTypes))
		for i, typ := range colTypes {
			if typ.Oid != types.T_Rowid {
				result.Vecs[i] = loaded.Vecs[colPos]
				colPos++
			}
		}
		return
	}

	readABlkColumns := func(cols []uint16) (result *batch.Batch, deletes nulls.Bitmap, err error) {
		var loaded *batch.Batch
		// appendable block should be filtered by committs
		cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if result, loaded, err = readColumns(cols); err != nil {
			return
		}

		t0 := time.Now()
		aborts := vector.MustFixedCol[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedCol[types.TS](loaded.Vecs[len(loaded.Vecs)-2])
		for i := 0; i < len(commits); i++ {
			if aborts[i] || commits[i].Greater(&ts) {
				deletes.Add(uint64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), deletes.Count())
		return
	}

	if info.EntryState {
		bat, deleteMask, err = readABlkColumns(idxes)
	} else {
		bat, _, err = readColumns(idxes)
	}

	return
}

func ReadBlockDelete(
	ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService,
) (bat *batch.Batch, isPersistedByCN bool, release func(), err error) {
	isPersistedByCN, err = IsPersistedByCN(ctx, deltaloc, fs)
	if err != nil {
		return
	}
	bat, release, err = ReadBlockDeleteBySchema(ctx, deltaloc, fs, isPersistedByCN)
	return
}

func ReadBlockDeleteBySchema(
	ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService, isPersistedByCN bool,
) (bat *batch.Batch, release func(), err error) {
	var cols []uint16
	if isPersistedByCN {
		cols = []uint16{0, 1}
	} else {
		cols = []uint16{0, 1, 2, 3}
	}
	bat, release, err = LoadTombstoneColumns(ctx, cols, nil, fs, deltaloc, nil)
	return
}

func IsPersistedByCN(
	ctx context.Context, deltaloc objectio.Location, fs fileservice.FileService,
) (bool, error) {
	objectMeta, err := objectio.FastLoadObjectMeta(ctx, &deltaloc, false, fs)
	if err != nil {
		return false, err
	}
	meta, ok := objectMeta.TombstoneMeta()
	if !ok {
		meta = objectMeta.MustDataMeta()
	}
	blkmeta := meta.GetBlockMeta(uint32(deltaloc.ID()))
	columnCount := blkmeta.GetColumnCount()
	return columnCount == 2, nil
}

func EvalDeleteRowsByTimestamp(
	deletes *batch.Batch, ts types.TS, blockid *types.Blockid,
) (rows *nulls.Bitmap) {
	if deletes == nil {
		return
	}
	// record visible delete rows
	rows = nulls.NewWithSize(64)

	rowids := vector.MustFixedCol[types.Rowid](deletes.Vecs[0])
	tss := vector.MustFixedCol[types.TS](deletes.Vecs[1])
	aborts := deletes.Vecs[3]

	start, end := FindIntervalForBlock(rowids, blockid)

	for i := start; i < end; i++ {
		abort := vector.GetFixedAt[bool](aborts, i)
		if abort || tss[i].Greater(&ts) {
			continue
		}
		row := rowids[i].GetRowOffset()
		rows.Add(uint64(row))
	}
	return
}

func EvalDeleteRowsByTimestampForDeletesPersistedByCN(
	deletes *batch.Batch, ts types.TS, committs types.TS,
) (rows *nulls.Bitmap) {
	if deletes == nil || ts.Less(&committs) {
		return
	}
	// record visible delete rows
	rows = nulls.NewWithSize(0)
	rowids := vector.MustFixedCol[types.Rowid](deletes.Vecs[0])

	for _, rowid := range rowids {
		row := rowid.GetRowOffset()
		rows.Add(uint64(row))
	}
	return
}

// BlockPrefetch is the interface for cn to call read ahead
// columns  Which columns should be taken for columns
// service  fileservice
// infos [s3object name][block]
// FIXME: using objectio.BlockInfoSlice
func BlockPrefetch(
	sid string,
	idxes []uint16,
	service fileservice.FileService,
	infos []*objectio.BlockInfo,
	prefetchFile bool,
) error {
	if len(infos) == 0 {
		return nil
	}

	// build reader
	pref, err := BuildPrefetchParams(service, infos[0].MetaLocation())
	if err != nil {
		return err
	}

	// Generate prefetch task
	for i := range infos {
		pref.AddBlock(idxes, []uint16{infos[i].MetaLocation().ID()})
	}

	pref.prefetchFile = prefetchFile
	err = MustGetPipeline(sid).Prefetch(pref)
	if err != nil {
		return err
	}

	return nil
}

func RecordReadDel(
	sid string,
	total, read, bisect time.Duration,
) {
	MustGetPipeline(sid).stats.selectivityStats.RecordReadDel(total, read, bisect)
}

func RecordReadFilterSelectivity(
	sid string,
	hit, total int,
) {
	MustGetPipeline(sid).stats.selectivityStats.RecordReadFilterSelectivity(hit, total)
}

func RecordBlockSelectivity(
	sid string,
	hit, total int,
) {
	MustGetPipeline(sid).stats.selectivityStats.RecordBlockSelectivity(hit, total)
}

func RecordColumnSelectivity(
	sid string,
	hit, total int,
) {
	MustGetPipeline(sid).stats.selectivityStats.RecordColumnSelectivity(hit, total)
}

func ExportSelectivityString(sid string) string {
	return MustGetPipeline(sid).stats.selectivityStats.ExportString()
}

func FindIntervalForBlock(rowids []types.Rowid, id *types.Blockid) (start int, end int) {
	lowRowid := objectio.NewRowid(id, 0)
	highRowid := objectio.NewRowid(id, math.MaxUint32)
	i, j := 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value >= lowRowid
		if !rowids[m].Less(*lowRowid) {
			j = m
		} else {
			i = m + 1
		}
	}
	start = i

	i, j = 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value > highRowid
		if highRowid.Less(rowids[m]) {
			j = m
		} else {
			i = m + 1
		}
	}
	end = i
	return
}
