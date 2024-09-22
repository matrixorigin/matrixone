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
	"sort"
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

// ReadDataByFilter only read block data from storage by filter, don't apply deletes.
func ReadDataByFilter(
	ctx context.Context,
	isTombstone bool,
	tableName string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	searchFunc objectio.ReadFilterSearchFuncType,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (sels []int64, err error) {
	bat, rowidIdx, deleteMask, release, err := readBlockData(
		ctx, isTombstone, columns, colTypes, info, ts, fileservice.Policy(0), mp, fs,
	)
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
	if len(sels) == 0 {
		return
	}
	sels, err = ds.ApplyTombstones(ctx, info.BlockID, sels, engine.Policy_CheckAll)
	return
}

// BlockDataReadNoCopy only read block data from storage, don't apply deletes.
func BlockDataReadNoCopy(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	policy fileservice.Policy,
	mp *mpool.MPool,
	fs fileservice.FileService,
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
		ctx, false, columns, colTypes, info, ts, policy, mp, fs,
	); err != nil {
		return nil, nil, nil, err
	}
	tombstones, err := ds.GetTombstones(ctx, info.BlockID)
	if err != nil {
		return nil, nil, nil, err
	}

	// merge deletes from tombstones
	deleteMask.Or(tombstones)

	// build rowid column if needed
	if rowidPos >= 0 {
		if loaded.Vecs[rowidPos], err = buildRowidColumn(
			info, nil, mp,
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
	isTombstone bool,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts timestamp.Timestamp,
	filterSeqnums []uint16,
	filterColTypes []types.Type,
	filter objectio.BlockReadFilter,
	policy fileservice.Policy,
	tableName string,
	bat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) error {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	var (
		sels []int64
		err  error
	)

	searchFunc := filter.DecideSearchFunc(info.IsSorted())

	if searchFunc != nil {
		if sels, err = ReadDataByFilter(
			ctx, isTombstone, tableName, info, ds, filterSeqnums, filterColTypes,
			types.TimestampToTS(ts), searchFunc, mp, fs,
		); err != nil {
			return err
		}
		v2.TaskSelReadFilterTotal.Inc()
		if len(sels) == 0 {
			v2.TaskSelReadFilterHit.Inc()
		}

		if len(sels) == 0 {
			return nil
		}
	}

	err = BlockDataReadInner(
		ctx, isTombstone, info, ds, columns, colTypes,
		types.TimestampToTS(ts), sels, policy, bat, mp, fs,
	)
	if err != nil {
		return err
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
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

func windowCNBatch(bat *batch.Batch, start, end uint64) error {
	var err error
	for i, vec := range bat.Vecs {
		bat.Vecs[i], err = vec.Window(int(start), int(end))
		if err != nil {
			return err
		}
	}
	return nil
}

func BlockDataReadBackup(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	idxes []uint16,
	ts types.TS,
	fs fileservice.FileService,
) (loaded *batch.Batch, sortKey uint16, err error) {
	if len(idxes) == 0 {
		loaded, sortKey, err = LoadOneBlock(ctx, fs, info.MetaLocation(), objectio.SchemaData)
	} else {
		loaded, sortKey, err = LoadOneBlockWithIndex(ctx, fs, idxes, info.MetaLocation(), objectio.SchemaData)
	}
	// read block data from storage specified by meta location
	if err != nil {
		return
	}
	if !ts.IsEmpty() {
		commitTs := types.TS{}
		for v := 0; v < loaded.Vecs[0].Length(); v++ {
			err = commitTs.Unmarshal(loaded.Vecs[len(loaded.Vecs)-1].GetRawBytesAt(v))
			if err != nil {
				return
			}
			if commitTs.Greater(&ts) {
				err = windowCNBatch(loaded, 0, uint64(v))
				if err != nil {
					return
				}
				logutil.Debug("[BlockDataReadBackup]",
					zap.String("commitTs", commitTs.ToString()),
					zap.String("ts", ts.ToString()),
					zap.String("location", info.MetaLocation().String()))
				break
			}
		}
	}
	tombstones, err := ds.GetTombstones(ctx, info.BlockID)
	if err != nil {
		return
	}
	rows := tombstones.ToI64Arrary()
	if len(rows) > 0 {
		loaded.Shrink(rows, true)
	}
	return
}

// BlockDataReadInner only read data,don't apply deletes.
func BlockDataReadInner(
	ctx context.Context,
	isTombstone bool,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	selectRows []int64, // if selectRows is not empty, it was already filtered by filter
	policy fileservice.Policy,
	bat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var (
		rowidPos    int
		deletedRows []int64
		deleteMask  nulls.Bitmap
		loaded      *batch.Batch
		release     func()
	)

	// read block data from storage specified by meta location
	if loaded, rowidPos, deleteMask, release, err = readBlockData(
		ctx, isTombstone, columns, colTypes, info, ts, policy, mp, fs,
	); err != nil {
		return
	}
	defer release()
	// assemble result batch for return
	//result = batch.NewWithSize(len(loaded.Vecs))

	if len(selectRows) > 0 {
		// NOTE: it always goes here if there is a filter and the block is sorted
		// and there are selected rows after applying the filter and delete mask

		// build rowid column if needed
		if rowidPos >= 0 {
			if loaded.Vecs[rowidPos], err = buildRowidColumn(
				info, selectRows, mp,
			); err != nil {
				return
			}
			defer func() {
				loaded.Vecs[rowidPos].Free(mp)
			}()
		}

		// assemble result batch only with selected rows
		if !isTombstone {
			for i, col := range loaded.Vecs {
				typ := *col.GetType()
				if typ.Oid == types.T_Rowid {
					err = bat.Vecs[i].UnionBatch(col, 0, col.Length(), nil, mp)
					if err != nil {
						return
					}
					continue
				}
				if err = bat.Vecs[i].PreExtendWithArea(len(selectRows), 0, mp); err != nil {
					break
				}
				if err = bat.Vecs[i].Union(col, selectRows, mp); err != nil {
					break
				}
			}
		} else {
			for i, col := range loaded.Vecs {
				if err = bat.Vecs[i].PreExtendWithArea(len(selectRows), 0, mp); err != nil {
					break
				}
				if err = bat.Vecs[i].Union(col, selectRows, mp); err != nil {
					break
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
	deleteMask.Or(tombstones)

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
			info, nil, mp,
		); err != nil {
			return
		}
		defer func() {
			loaded.Vecs[rowidPos].Free(mp)
		}()
	}

	// assemble result batch
	for i, col := range loaded.Vecs {
		typ := *col.GetType()
		// TODO: avoid this allocation and copy
		if err = vector.GetUnionAllFunction(typ, mp)(bat.Vecs[i], col); err != nil {
			break
		}
		if len(deletedRows) > 0 {
			bat.Vecs[i].Shrink(deletedRows, true)
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
) (col *vector.Vector, err error) {
	col = vector.NewVec(objectio.RowidType)
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
	isTombstone bool,
	colIndexes []uint16,
	colTypes []types.Type,
	info *objectio.BlockInfo,
	ts types.TS,
	policy fileservice.Policy,
	m *mpool.MPool,
	fs fileservice.FileService,
) (bat *batch.Batch, rowidPos int, deleteMask nulls.Bitmap, release func(), err error) {
	var (
		idxes []uint16
		typs  []types.Type
	)
	if isTombstone {
		rowidPos = -1
		idxes = colIndexes
		typs = colTypes
	} else {
		rowidPos, idxes, typs = getRowsIdIndex(colIndexes, colTypes)
	}

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
		if !isTombstone {
			for i, typ := range colTypes {
				if typ.Oid != types.T_Rowid {
					result.Vecs[i] = loaded.Vecs[colPos]
					colPos++
				}
			}
		} else {
			for i := range colTypes {
				result.Vecs[i] = loaded.Vecs[colPos]
				colPos++
			}
		}
		return
	}

	readABlkColumns := func(cols []uint16) (result *batch.Batch, deletes nulls.Bitmap, err error) {
		var loaded *batch.Batch
		// appendable block should be filtered by committs
		//cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted
		cols = append(cols, objectio.SEQNUM_COMMITTS) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if result, loaded, err = readColumns(cols); err != nil {
			return
		}

		t0 := time.Now()
		//aborts := vector.MustFixedColWithTypeCheck[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedColWithTypeCheck[types.TS](loaded.Vecs[len(loaded.Vecs)-1])
		for i := 0; i < len(commits); i++ {
			if commits[i].Greater(&ts) {
				deletes.Add(uint64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), deletes.Count())
		return
	}

	if info.IsAppendable() {
		bat, deleteMask, err = readABlkColumns(idxes)
	} else {
		bat, _, err = readColumns(idxes)
	}

	return
}

func ReadDeletes(
	ctx context.Context,
	deltaLoc objectio.Location,
	fs fileservice.FileService,
	isPersistedByCN bool,
) (*batch.Batch, objectio.ObjectDataMeta, func(), error) {

	var cols []uint16
	var typs []types.Type

	if isPersistedByCN {
		cols = []uint16{objectio.TombstoneAttr_Rowid_SeqNum}
		typs = []types.Type{objectio.RowidType}
	} else {
		cols = []uint16{objectio.TombstoneAttr_Rowid_SeqNum, objectio.TombstoneAttr_CommitTs_SeqNum}
		typs = []types.Type{objectio.RowidType, objectio.TSType}
	}
	return LoadTombstoneColumns(
		ctx, cols, typs, fs, deltaLoc, nil, fileservice.Policy(0),
	)
}

func EvalDeleteMaskFromDNCreatedTombstones(
	deletes *batch.Batch,
	meta objectio.BlockObject,
	ts types.TS,
	blockid *types.Blockid,
) (rows *nulls.Bitmap) {
	if deletes == nil {
		return
	}
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](deletes.Vecs[0])
	start, end := FindStartEndOfBlockFromSortedRowids(rowids, blockid)

	noTSCheck := false
	if end-start > 10 {
		// fast path is true if the maxTS is less than the snapshotTS
		// this means that all the rows between start and end are visible
		idx := objectio.GetTombstoneCommitTSAttrIdx(meta.GetMetaColumnCount())
		noTSCheck = meta.MustGetColumn(idx).ZoneMap().FastLEValue(ts[:], 0)
	}
	if noTSCheck {
		for i := end - 1; i >= start; i-- {
			row := rowids[i].GetRowOffset()
			if rows == nil {
				rows = nulls.NewWithSize(int(row) + 1)
			}
			rows.Add(uint64(row))
		}
	} else {
		tss := vector.MustFixedColWithTypeCheck[types.TS](deletes.Vecs[1])
		for i := end - 1; i >= start; i-- {
			if tss[i].Greater(&ts) {
				continue
			}
			row := rowids[i].GetRowOffset()
			if rows == nil {
				rows = nulls.NewWithSize(int(row) + 1)
			}
			rows.Add(uint64(row))
		}
	}

	return
}

func EvalDeleteMaskFromCNCreatedTombstones(
	bid types.Blockid,
	deletes *batch.Batch,
) (rows *nulls.Bitmap) {
	if deletes == nil {
		return
	}
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](deletes.Vecs[0])

	start, end := FindStartEndOfBlockFromSortedRowids(rowids, &bid)
	for i := end - 1; i >= start; i-- {
		row := rowids[i].GetRowOffset()
		if rows == nil {
			rows = nulls.NewWithSize(int(row) + 1)
		}
		rows.Add(uint64(row))
	}

	return
}

func FindStartEndOfBlockFromSortedRowids(rowids []types.Rowid, id *types.Blockid) (start int, end int) {
	lowRowid := objectio.NewRowid(id, 0)
	highRowid := objectio.NewRowid(id, math.MaxUint32)
	i, j := 0, len(rowids)
	for i < j {
		m := (i + j) / 2
		// first value >= lowRowid
		if !rowids[m].LT(lowRowid) {
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
		if highRowid.LT(&rowids[m]) {
			j = m
		} else {
			i = m + 1
		}
	}
	end = i
	return
}

func IsRowDeletedByLocation(
	ctx context.Context,
	snapshotTS *types.TS,
	row *objectio.Rowid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleted bool, err error) {
	data, _, release, err := ReadDeletes(ctx, location, fs, createdByCN)
	if err != nil {
		return
	}
	defer release()
	if data.RowCount() == 0 {
		return
	}
	rowids := vector.MustFixedColNoTypeCheck[types.Rowid](data.Vecs[0])
	idx := sort.Search(len(rowids), func(i int) bool {
		return rowids[i].GE(row)
	})
	if createdByCN {
		deleted = idx < len(rowids)
	} else {
		tss := vector.MustFixedColNoTypeCheck[types.TS](data.Vecs[1])
		for i := idx; i < len(rowids); i++ {
			if !rowids[i].EQ(row) {
				break
			}
			if tss[i].LE(snapshotTS) {
				deleted = true
				break
			}
		}
	}
	return
}

func FillBlockDeleteMask(
	ctx context.Context,
	snapshotTS types.TS,
	blockId types.Blockid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleteMask *nulls.Nulls, err error) {
	var (
		rows             *nulls.Nulls
		release          func()
		persistedDeletes *batch.Batch
		meta             objectio.ObjectDataMeta
	)

	if !location.IsEmpty() {
		if persistedDeletes, meta, release, err = ReadDeletes(ctx, location, fs, createdByCN); err != nil {
			return nil, err
		}
		defer release()

		if createdByCN {
			rows = EvalDeleteMaskFromCNCreatedTombstones(blockId, persistedDeletes)
		} else {
			rows = EvalDeleteMaskFromDNCreatedTombstones(
				persistedDeletes, meta.GetBlockMeta(uint32(location.ID())), snapshotTS, &blockId,
			)
		}

		if rows != nil {
			deleteMask = rows
		}
	}

	return deleteMask, nil
}
