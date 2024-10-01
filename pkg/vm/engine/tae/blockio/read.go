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
	tableName string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	searchFunc objectio.ReadFilterSearchFuncType,
	cacheBat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (sels []int64, err error) {
	// PXU TODO: temporary solution, need to be refactored
	// cannot filter by physical address column now
	bat, deleteMask, release, err := readBlockData(
		ctx,
		columns,
		colTypes,
		-1,
		info,
		ts,
		fileservice.Policy(0),
		cacheBat,
		mp,
		fs,
	)
	if err != nil {
		return
	}
	defer release()

	sels = searchFunc(bat.Vecs)
	if !deleteMask.IsEmpty() {
		sels = removeIf(sels, func(i int64) bool {
			return deleteMask.Contains(uint64(i))
		})
	}
	if len(sels) == 0 {
		return
	}
	sels, err = ds.ApplyTombstones(ctx, &info.BlockID, sels, engine.Policy_CheckAll)
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
		deleteMask nulls.Bitmap
		release    func()
		loaded     *batch.Batch
		err        error
	)

	defer func() {
		if err != nil {
			if release != nil {
				release()
			}
		}
	}()

	cacheBat := batch.EmptyBatchWithSize(len(columns) + 1)

	phyAddrColumnPos := -1
	for i := range columns {
		if columns[i] == objectio.SEQNUM_ROWID {
			phyAddrColumnPos = i
			break
		}
	}

	// read block data from storage specified by meta location
	if loaded, deleteMask, release, err = readBlockData(
		ctx, columns, colTypes, phyAddrColumnPos, info, ts, policy, &cacheBat, mp, fs,
	); err != nil {
		return nil, nil, nil, err
	}
	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		release()
		return nil, nil, nil, err
	}

	// merge deletes from tombstones
	deleteMask.Or(tombstones)

	// build rowid column if needed
	if phyAddrColumnPos >= 0 {
		loaded.Vecs[phyAddrColumnPos] = vector.NewVec(objectio.RowidType)
		if err = buildRowidColumn(
			info, loaded.Vecs[phyAddrColumnPos], nil, mp,
		); err != nil {
			release()
			return nil, nil, nil, err
		}
		release = func() {
			release()
			loaded.Vecs[phyAddrColumnPos].Free(mp)
		}
	}
	loaded.SetRowCount(loaded.Vecs[0].Length())
	return loaded, &deleteMask, release, nil
}

// BlockDataRead only read block data from storage, don't apply deletes.
func BlockDataRead(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	ts timestamp.Timestamp,
	filterSeqnums []uint16,
	filterColTypes []types.Type,
	filter objectio.BlockReadFilter,
	policy fileservice.Policy,
	tableName string,
	bat *batch.Batch,
	cacheBat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) error {
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	snapshotTS := types.TimestampToTS(ts)

	var (
		sels []int64
		err  error
	)

	searchFunc := filter.DecideSearchFunc(info.IsSorted())

	if searchFunc != nil {
		if sels, err = ReadDataByFilter(
			ctx,
			tableName,
			info,
			ds,
			filterSeqnums,
			filterColTypes,
			snapshotTS,
			searchFunc,
			cacheBat,
			mp,
			fs,
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
		ctx,
		info,
		ds,
		columns,
		colTypes,
		phyAddrColumnPos,
		snapshotTS,
		sels,
		policy,
		bat,
		cacheBat,
		mp,
		fs,
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
	cacheBat := batch.EmptyBatchWithSize(len(seqnums) + 1)

	release, err := LoadColumns(
		ctx, seqnums, colTypes, fs, location, &cacheBat, mp, fileservice.Policy(0),
	)
	if err != nil {
		return nil, err
	}
	defer release()
	if len(deletes) == 0 {
		return cacheBat.Slice(0, len(seqnums)), nil
	}
	result := batch.NewWithSize(len(seqnums))
	for i, col := range cacheBat.Vecs {
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
			if commitTs.GT(&ts) {
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
	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
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
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	ts types.TS,
	selectRows []int64, // if selectRows is not empty, it was already filtered by filter
	policy fileservice.Policy,
	bat *batch.Batch,
	cacheBat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	var (
		deletedRows []int64
		deleteMask  nulls.Bitmap
		release     func()
		loaded      *batch.Batch
	)

	// read block data from storage specified by meta location
	if loaded, deleteMask, release, err = readBlockData(
		ctx,
		columns,
		colTypes,
		phyAddrColumnPos,
		info,
		ts,
		policy,
		cacheBat,
		mp,
		fs,
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
		if phyAddrColumnPos >= 0 {
			if err = buildRowidColumn(
				info, bat.Vecs[phyAddrColumnPos], selectRows, mp,
			); err != nil {
				return
			}
		}

		// assemble result batch only with selected rows
		for i, col := range loaded.Vecs {
			if phyAddrColumnPos == i {
				continue
			}
			if err = bat.Vecs[i].PreExtendWithArea(len(selectRows), 0, mp); err != nil {
				break
			}
			if err = bat.Vecs[i].Union(col, selectRows, mp); err != nil {
				break
			}
		}
		return
	}

	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
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
	if phyAddrColumnPos >= 0 {
		if err = buildRowidColumn(
			info, bat.Vecs[phyAddrColumnPos], nil, mp,
		); err != nil {
			return
		}
	}

	// assemble result batch
	for i, col := range loaded.Vecs {
		if i == phyAddrColumnPos {
		} else {
			if err = bat.Vecs[i].UnionBatch(col, 0, col.Length(), nil, mp); err != nil {
				break
			}
		}
		if len(deletedRows) > 0 {
			bat.Vecs[i].Shrink(deletedRows, true)
		}
	}
	return
}

func excludePhyAddrColumn(
	colIndexes []uint16, colTypes []types.Type, phyAddrColumnPos int,
) ([]uint16, []types.Type) {
	if phyAddrColumnPos < 0 {
		return colIndexes, colTypes
	}
	idxes := make([]uint16, 0, len(colTypes)-1)
	typs := make([]types.Type, 0, len(colTypes)-1)
	idxes = append(idxes, colIndexes[:phyAddrColumnPos]...)
	idxes = append(idxes, colIndexes[phyAddrColumnPos+1:]...)
	typs = append(typs, colTypes[:phyAddrColumnPos]...)
	typs = append(typs, colTypes[phyAddrColumnPos+1:]...)
	return idxes, typs
}

func buildRowidColumn(
	info *objectio.BlockInfo,
	vec *vector.Vector,
	sels []int64,
	m *mpool.MPool,
) (err error) {
	if len(sels) == 0 {
		err = objectio.ConstructRowidColumnTo(
			vec,
			&info.BlockID,
			0,
			info.MetaLocation().Rows(),
			m,
		)
	} else {
		err = objectio.ConstructRowidColumnToWithSels(
			vec,
			&info.BlockID,
			sels,
			m,
		)
	}
	return
}

func readBlockData(
	ctx context.Context,
	colIndexes []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	info *objectio.BlockInfo,
	ts types.TS,
	policy fileservice.Policy,
	cacheBat *batch.Batch,
	m *mpool.MPool,
	fs fileservice.FileService,
) (bat *batch.Batch, deleteMask nulls.Bitmap, release func(), err error) {

	idxes, typs := excludePhyAddrColumn(colIndexes, colTypes, phyAddrColumnPos)

	readColumns := func(cols []uint16, cacheBat2 *batch.Batch) (result *batch.Batch, err error) {
		if len(cols) == 0 && phyAddrColumnPos >= 0 {
			// only read rowid column on non appendable block, return early
			result = batch.NewWithSize(1)
			// result.Vecs[0] = rowid
			release = func() {}
			return
		}

		if release, err = LoadColumns(
			ctx, cols, typs, fs, info.MetaLocation(), cacheBat2, m, policy,
		); err != nil {
			return
		}
		colPos := 0
		result = batch.NewWithSize(len(colTypes))
		for i := range colTypes {
			if i != phyAddrColumnPos {
				result.Vecs[i] = cacheBat2.Vecs[colPos]
				colPos++
			}
		}
		return
	}

	readABlkColumns := func(cols []uint16, cacheBat2 *batch.Batch) (
		result *batch.Batch, deletes nulls.Bitmap, err error,
	) {
		// appendable block should be filtered by committs
		//cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted
		cols = append(cols, objectio.SEQNUM_COMMITTS) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if result, err = readColumns(cols, cacheBat2); err != nil {
			return
		}

		t0 := time.Now()
		//aborts := vector.MustFixedColWithTypeCheck[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedColWithTypeCheck[types.TS](cacheBat2.Vecs[len(cols)-1])
		for i := 0; i < len(commits); i++ {
			if commits[i].GT(&ts) {
				deletes.Add(uint64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(), time.Since(t0), ts.ToString(), deletes.Count())
		return
	}

	if info.IsAppendable() {
		bat, deleteMask, err = readABlkColumns(idxes, cacheBat)
	} else {
		bat, err = readColumns(idxes, cacheBat)
	}

	return
}

func ReadDeletes(
	ctx context.Context,
	deltaLoc objectio.Location,
	fs fileservice.FileService,
	isPersistedByCN bool,
	cacheBat *batch.Batch,
) (objectio.ObjectDataMeta, func(), error) {

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
		ctx, cols, typs, fs, deltaLoc, cacheBat, nil, fileservice.Policy(0),
	)
}

func EvalDeleteMaskFromDNCreatedTombstones(
	deletedRows *vector.Vector,
	commitTSVec *vector.Vector,
	meta objectio.BlockObject,
	ts *types.TS,
	blockid *types.Blockid,
) (rows *nulls.Bitmap) {
	if deletedRows == nil {
		return
	}
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](deletedRows)
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
		tss := vector.MustFixedColWithTypeCheck[types.TS](commitTSVec)
		for i := end - 1; i >= start; i-- {
			if tss[i].GT(ts) {
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
	bid *types.Blockid,
	deletedRows *vector.Vector,
) (rows *nulls.Bitmap) {
	if deletedRows == nil {
		return
	}
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](deletedRows)

	start, end := FindStartEndOfBlockFromSortedRowids(rowids, bid)
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
	var hidden objectio.HiddenColumnSelection
	if !createdByCN {
		hidden = hidden | objectio.HiddenColumnSelection_CommitTS
	}

	attrs := objectio.GetTombstoneAttrs(hidden)
	data := batch.EmptyBatchWithAttrs(attrs)
	_, release, err := ReadDeletes(ctx, location, fs, createdByCN, &data)
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
	snapshotTS *types.TS,
	blockId *types.Blockid,
	location objectio.Location,
	fs fileservice.FileService,
	createdByCN bool,
) (deleteMask *nulls.Nulls, err error) {
	var (
		rows    *nulls.Nulls
		release func()
		meta    objectio.ObjectDataMeta
		hidden  objectio.HiddenColumnSelection
	)

	if !createdByCN {
		hidden = hidden | objectio.HiddenColumnSelection_CommitTS
	}

	attrs := objectio.GetTombstoneAttrs(hidden)
	persistedDeletes := batch.EmptyBatchWithAttrs(attrs)

	if !location.IsEmpty() {
		if meta, release, err = ReadDeletes(
			ctx, location, fs, createdByCN, &persistedDeletes,
		); err != nil {
			return nil, err
		}
		defer release()

		if createdByCN {
			rows = EvalDeleteMaskFromCNCreatedTombstones(blockId, persistedDeletes.Vecs[0])
		} else {
			rows = EvalDeleteMaskFromDNCreatedTombstones(
				persistedDeletes.Vecs[0],
				persistedDeletes.Vecs[1],
				meta.GetBlockMeta(uint32(location.ID())),
				snapshotTS,
				blockId,
			)
		}

		if rows != nil {
			deleteMask = rows
		}
	}

	return deleteMask, nil
}
