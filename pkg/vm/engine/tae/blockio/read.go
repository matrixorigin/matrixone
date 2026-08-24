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
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"go.uber.org/zap"
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
// Right now, it cannot support filter by physical address column.
// len(columns) == len(colTypes) >= 1 (supports multiple columns for optimization)
func ReadDataByFilter(
	ctx context.Context,
	tableName string,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	ts types.TS,
	searchFunc objectio.ReadFilterSearchFuncType,
	cachedSearch *objectio.ReadFilterSearch,
	cachedSearchSorted bool,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (sels []int64, err error) {
	if cachedSearch != nil {
		cacheVectors.Free(mp)
		var visibilityTS *types.TS
		if info.IsAppendable() {
			visibilityTS = &ts
		}
		sels, _, err = ioutil.LoadColumnDataBySearch(
			ctx,
			columns[0],
			colTypes[0],
			fs,
			info.MetaLocation(),
			cachedSearch,
			cachedSearchSorted,
			visibilityTS,
			mp,
			fileservice.Policy(0),
		)
		if err != nil {
			return
		}
	} else {
		deleteMask, release, readErr := readBlockData(
			ctx,
			columns,
			colTypes,
			-1,
			info,
			ts,
			fileservice.Policy(0),
			cacheVectors,
			mp,
			fs,
		)
		if readErr != nil {
			return nil, readErr
		}
		defer release()
		defer deleteMask.Release()

		sels = searchFunc(cacheVectors)
		if !deleteMask.IsEmpty() {
			sels = removeIf(sels, func(i int64) bool {
				return deleteMask.Contains(uint64(i))
			})
		}
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
	if ctx == nil || info == nil || ds == nil || mp == nil || fs == nil {
		return nil, nil, nil, moerr.NewInvalidInputNoCtx(
			"no-copy block read requires context, block info, data source, mpool, and file service",
		)
	}
	if len(columns) == 0 || len(columns) != len(colTypes) {
		return nil, nil, nil, moerr.NewInvalidInputNoCtxf(
			"no-copy block read has %d columns and %d column types", len(columns), len(colTypes),
		)
	}
	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debugf("read block %s, columns %v, types %v", info.BlockID.String(), columns, colTypes)
	}

	var (
		deleteMask objectio.Bitmap
		release    func()
		err        error
	)

	transferRelease := false
	defer func() {
		if !transferRelease && release != nil {
			release()
		}
	}()

	cacheVectors := containers.NewVectors(len(columns) + 2)

	phyAddrColumnPos := -1
	for i := range columns {
		if columns[i] == objectio.SEQNUM_ROWID {
			phyAddrColumnPos = i
			break
		}
	}

	// read block data from storage specified by meta location
	if deleteMask, release, err = readBlockData(
		ctx, columns, colTypes, phyAddrColumnPos, info, ts, policy, cacheVectors, mp, fs,
	); err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		deleteMask.Release()
	}()

	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		tombstones.Release()
		return nil, nil, nil, err
	}

	// merge deletes from tombstones
	if !deleteMask.IsValid() {
		deleteMask = tombstones
	} else {
		deleteMask.Or(tombstones)
		tombstones.Release()
	}
	outputBat := batch.NewWithSize(len(columns))
	actualRows := -1
	if phyAddrColumnPos < 0 || len(columns) > 1 || info.IsAppendable() {
		actualRows = cacheVectors[0].Length()
	} else {
		// A BlockInfo synthesized from ObjectStats estimates every non-final
		// block as BlockMaxRows. Resolve the exact physical cardinality before
		// constructing a rowid-only result.
		location := info.MetaLocation()
		objectMeta, metaErr := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if metaErr != nil {
			return nil, nil, nil, metaErr
		}
		dataMeta, metaErr := ioutil.GetDataMetaForLocation(objectMeta, location)
		if metaErr != nil {
			return nil, nil, nil, metaErr
		}
		actualRows = int(dataMeta.GetBlockMeta(uint32(location.ID())).GetRows())
	}

	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos != phyAddrColumnPos {
			outputBat.Vecs[outputColPos] = &cacheVectors[loadedColumnPos]
			loadedColumnPos++
		} else {
			outputBat.Vecs[outputColPos] = vector.NewOffHeapVecWithType(objectio.RowidType)
			if err = objectio.ConstructRowidColumnTo(
				outputBat.Vecs[phyAddrColumnPos], &info.BlockID, 0, uint32(actualRows), mp,
			); err != nil {
				outputBat.Vecs[phyAddrColumnPos].Free(mp)
				return nil, nil, nil, err
			}
			loadedRelease := release
			rowIDVec := outputBat.Vecs[phyAddrColumnPos]
			release = func() {
				if loadedRelease != nil {
					loadedRelease()
				}
				rowIDVec.Free(mp)
			}
		}
	}
	for pos, vec := range outputBat.Vecs {
		if vec == nil {
			return nil, nil, nil, moerr.NewInternalErrorNoCtxf(
				"no-copy block column %d is nil", pos,
			)
		}
		if vec.Length() != actualRows {
			return nil, nil, nil, moerr.NewInternalErrorNoCtxf(
				"no-copy block column %d has %d rows, expected %d", pos, vec.Length(), actualRows,
			)
		}
	}
	outputBat.SetRowCount(actualRows)

	// FIXME: w-zr
	var retMask *nulls.Bitmap

	if !deleteMask.IsEmpty() {
		retMask = &nulls.Bitmap{}
		retMask.OrBitmap(deleteMask.Bitmap())
	}
	transferRelease = true
	return outputBat, retMask, release, nil
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
	orderByLimit *objectio.IndexReaderTopOp,
	policy fileservice.Policy,
	tableName string,
	bat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) error {
	return blockDataRead(
		ctx,
		info,
		ds,
		columns,
		colTypes,
		phyAddrColumnPos,
		ts,
		filterSeqnums,
		filterColTypes,
		filter,
		orderByLimit,
		policy,
		tableName,
		bat,
		cacheVectors,
		mp,
		fs,
		nil,
		nil,
		nil,
	)
}

// BlockDataReadWithFilter applies a residual filter after materializing only
// earlyColumns, then reads the other columns for the surviving rows. It is
// intentionally limited to scans without storage TopN; callers must use the
// eager BlockDataRead path when orderByLimit is active. preFilterRows is the
// live row count after storage visibility and tombstones but before the
// residual filter.
func BlockDataReadWithFilter(
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
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
	earlyColumns []int,
	applyFilter engine.ReaderFilter,
) (preFilterRows int, err error) {
	if applyFilter == nil {
		return 0, moerr.NewInvalidInputNoCtx("nil residual block filter")
	}
	if err := validateEarlyColumns(earlyColumns, len(columns)); err != nil {
		return 0, err
	}
	err = blockDataRead(
		ctx,
		info,
		ds,
		columns,
		colTypes,
		phyAddrColumnPos,
		ts,
		filterSeqnums,
		filterColTypes,
		filter,
		nil,
		policy,
		tableName,
		bat,
		cacheVectors,
		mp,
		fs,
		earlyColumns,
		applyFilter,
		&preFilterRows,
	)
	return preFilterRows, err
}

func blockDataRead(
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
	orderByLimit *objectio.IndexReaderTopOp,
	policy fileservice.Policy,
	tableName string,
	bat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
	earlyColumns []int,
	applyFilter engine.ReaderFilter,
	preFilterRows *int,
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
			filter.CachedSearch,
			info.IsSorted() && !filter.HasFakePK,
			cacheVectors,
			mp,
			fs,
		); err != nil {
			return err
		}
		v2.TxnSelReadFilterTotal.Observe(1.0)

		if len(sels) == 0 {
			v2.TxnSelReadFilterFiltered.Observe(1.0)
			return nil
		}
	}

	if applyFilter == nil {
		err = BlockDataReadInner(
			ctx,
			info,
			ds,
			columns,
			colTypes,
			phyAddrColumnPos,
			snapshotTS,
			sels,
			orderByLimit,
			policy,
			bat,
			cacheVectors,
			mp,
			fs,
		)
	} else {
		err = blockDataReadWithFilter(
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
			cacheVectors,
			mp,
			fs,
			earlyColumns,
			applyFilter,
			preFilterRows,
		)
	}
	if err != nil {
		return err
	}

	if applyFilter == nil {
		bat.SetRowCount(bat.Vecs[0].Length())
	}
	return nil
}

func CopyBlockData(
	ctx context.Context,
	location objectio.Location,
	deletes []int64,
	seqnums []uint16,
	colTypes []types.Type,
	outputBat *batch.Batch,
	fs fileservice.FileService,
	mp *mpool.MPool,
) (err error) {
	var (
		release      func()
		cacheVectors = containers.NewVectors(len(seqnums))
	)

	if release, _, err = ioutil.LoadColumns(
		ctx, seqnums, colTypes, fs, location, cacheVectors, mp, fileservice.Policy(0),
	); err != nil {
		return
	}
	defer release()

	if err = containers.VectorsCopyToBatch(
		cacheVectors, outputBat, mp,
	); err != nil {
		return
	}
	outputBat.Shrink(deletes, true)
	return
}

type rowBoundedTombstoneDataSource interface {
	GetTombstonesWithRowCount(
		context.Context,
		*objectio.Blockid,
		int,
	) (objectio.Bitmap, error)
}

func BlockDataReadBackup(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	idxes []uint16,
	ts types.TS,
	fs fileservice.FileService,
) (loaded *batch.Batch, sortKey uint16, err error) {
	if ctx == nil || info == nil || ds == nil || fs == nil {
		err = moerr.NewInvalidInputNoCtx(
			"backup block read requires context, block info, data source, and file service",
		)
		return
	}
	location := info.MetaLocation()
	requestedColumnCount := len(idxes)
	commitPos, abortPos := -1, -1
	if len(idxes) == 0 {
		var layout objectio.SpecialColumnLayout
		loaded, sortKey, layout, err = ioutil.LoadOneBlockWithSpecialLayout(
			ctx, fs, location, objectio.SchemaData,
		)
		if pos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
			commitPos = int(pos)
		}
		if pos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
			abortPos = int(pos)
		}
	} else {
		objectMeta, metaErr := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if metaErr != nil {
			err = metaErr
			return
		}
		dataMeta, metaErr := ioutil.GetDataMetaForLocation(objectMeta, location)
		if metaErr != nil {
			err = metaErr
			return
		}
		blockMeta := dataMeta.GetBlockMeta(uint32(location.ID()))
		layout := objectio.ResolveSpecialColumnLayout(blockMeta)
		loadIdxes := slices.Clone(idxes)
		if pos, ok := layout.Resolve(objectio.SEQNUM_COMMITTS); ok {
			commitPos = slices.Index(loadIdxes, pos)
			if commitPos < 0 {
				commitPos = len(loadIdxes)
				loadIdxes = append(loadIdxes, pos)
			}
		}
		if pos, ok := layout.Resolve(objectio.SEQNUM_ABORT); ok {
			abortPos = slices.Index(loadIdxes, pos)
			if abortPos < 0 {
				abortPos = len(loadIdxes)
				loadIdxes = append(loadIdxes, pos)
			}
		}
		loaded, sortKey, err = ioutil.LoadOneBlockWithIndex(
			ctx, fs, loadIdxes, location, objectio.SchemaData,
		)
	}
	// read block data from storage specified by meta location
	if err != nil {
		return
	}
	if loaded == nil {
		err = moerr.NewInternalError(ctx, "backup block loader returned no batch")
		return
	}
	loadedForCleanup := loaded
	defer func() {
		if err != nil {
			loadedForCleanup.Clean(common.DebugAllocator)
			loaded = nil
			return
		}
		if requestedColumnCount > 0 {
			for i := requestedColumnCount; i < len(loaded.Vecs); i++ {
				loaded.Vecs[i].Free(common.DebugAllocator)
				loaded.Vecs[i] = nil
			}
			loaded.Vecs = loaded.Vecs[:requestedColumnCount]
			if len(loaded.Attrs) > requestedColumnCount {
				loaded.Attrs = loaded.Attrs[:requestedColumnCount]
			}
		}
	}()
	if len(loaded.Vecs) == 0 || requestedColumnCount > len(loaded.Vecs) {
		err = moerr.NewInternalError(ctx, "backup block loader returned an invalid column set")
		return
	}
	rowCount := loaded.RowCount()
	if rowCount == 0 {
		rowCount = loaded.Vecs[0].Length()
	}
	for pos, vec := range loaded.Vecs {
		if vec == nil || vec.Length() != rowCount {
			err = moerr.NewInternalErrorf(
				ctx, "backup block column %d has invalid logical row count", pos,
			)
			return
		}
	}
	var tombstones objectio.Bitmap
	if bounded, ok := ds.(rowBoundedTombstoneDataSource); ok {
		tombstones, err = bounded.GetTombstonesWithRowCount(
			ctx, &info.BlockID, rowCount,
		)
	} else {
		tombstones, err = ds.GetTombstones(ctx, &info.BlockID)
	}
	if err != nil {
		tombstones.Release()
		return
	}
	defer tombstones.Release()
	if tombstones.Count() > rowCount {
		err = moerr.NewInternalErrorf(
			ctx,
			"backup tombstone contains %d rows for a block with %d rows",
			tombstones.Count(),
			rowCount,
		)
		return
	}
	tombstoneRows := tombstones.ToI64Array(nil)
	for _, row := range tombstoneRows {
		if row < 0 || row >= int64(rowCount) {
			err = moerr.NewInternalErrorf(
				ctx,
				"backup tombstone row offset %d is outside block row count %d",
				row,
				rowCount,
			)
			return
		}
	}
	if commitPos < 0 {
		if !ts.IsEmpty() {
			err = moerr.NewInternalError(ctx, "backup object has no commit timestamp")
			return
		}
		if len(tombstoneRows) > 0 {
			logutil.Info("[BlockDataReadBackup Shrink]", zap.String("location", location.String()), zap.Int("rows", len(tombstoneRows)))
			loaded.Shrink(tombstoneRows, true)
		}
		return
	}
	if commitPos >= len(loaded.Vecs) || loaded.Vecs[commitPos] == nil ||
		(abortPos >= 0 && (abortPos >= len(loaded.Vecs) || loaded.Vecs[abortPos] == nil)) {
		err = moerr.NewInternalError(ctx, "backup block has invalid visibility columns")
		return
	}

	commitTSs, err := ioutil.ValidateTombstoneCommitTSColumn(
		rowCount, loaded.Vecs[commitPos],
	)
	if err != nil {
		return
	}
	var aborts ioutil.TombstoneAbortColumn
	if abortPos >= 0 {
		aborts, err = ioutil.ValidateTombstoneAbortColumn(rowCount, loaded.Vecs[abortPos])
		if err != nil {
			return
		}
	}
	visibleRows := make([]int64, 0, rowCount)
	for row := 0; row < rowCount; row++ {
		commitTS := commitTSs.At(row)
		if (!ts.IsEmpty() && commitTS.GT(&ts)) ||
			commitTS.Equal(&txnif.UncommitTS) ||
			(aborts.IsPresent() && aborts.At(row)) ||
			tombstones.Contains(uint64(row)) {
			continue
		}
		visibleRows = append(visibleRows, int64(row))
	}
	if len(visibleRows) != rowCount {
		loaded.Shrink(visibleRows, false)
		logutil.Info("[BlockDataReadBackup]",
			zap.String("ts", ts.ToString()),
			zap.String("location", location.String()),
			zap.Int("rows", len(visibleRows)))
	}
	return
}

func HandleOrderByLimitOnIVFFlatIndex(
	ctx context.Context,
	selectRows []int64,
	vecCol *vector.Vector,
	orderByLimit *objectio.IndexReaderTopOp,
) ([]int64, []float64, error) {
	return objectio.TopNVector(ctx, selectRows, vecCol, orderByLimit)
}

func fillOutputBatchBySelectedRows(
	info *objectio.BlockInfo,
	columns []uint16,
	phyAddrColumnPos int,
	outputBat *batch.Batch,
	cacheVectors containers.Vectors,
	selectRows []int64,
	orderByLimit *objectio.IndexReaderTopOp,
	dists []float64,
	mp *mpool.MPool,
) (err error) {
	// phyAddrColumnPos >= 0 means one of the columns is the physical address column.
	// The physical address column should be generated by blockid + rowid.
	if phyAddrColumnPos >= 0 {
		if len(selectRows) == 0 {
			outputBat.Vecs[phyAddrColumnPos].CleanOnlyData()
		} else if err = buildRowidColumn(
			info, outputBat.Vecs[phyAddrColumnPos], selectRows, mp,
		); err != nil {
			return err
		}
	}

	// cacheVectors contains all loaded columns from storage, and excludes the
	// physical address column. Fill output columns by selected rows.
	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos == phyAddrColumnPos {
			continue
		}
		if orderByLimit != nil && !orderByLimit.OrderedLimit && loadedColumnPos == int(orderByLimit.ColPos) {
			loadedColumnPos++
			continue
		}
		if err = outputBat.Vecs[outputColPos].PreExtendWithArea(
			len(selectRows), 0, mp,
		); err != nil {
			return err
		}
		if err = outputBat.Vecs[outputColPos].Union(
			&cacheVectors[loadedColumnPos], selectRows, mp,
		); err != nil {
			return err
		}
		loadedColumnPos++
	}

	if orderByLimit != nil && !orderByLimit.OrderedLimit {
		if len(outputBat.Vecs) == len(columns) {
			distVec := vector.NewVec(types.T_float64.ToType())
			if err = vector.AppendFixedList(distVec, dists, nil, mp); err != nil {
				return err
			}
			outputBat.Vecs = append(outputBat.Vecs, distVec)
		} else {
			if err = vector.AppendFixedList(outputBat.Vecs[len(outputBat.Vecs)-1], dists, nil, mp); err != nil {
				return err
			}
		}
	}

	return nil
}

// materializeBlockData copies block columns directly from pinned FileService
// cache entries into the output batch. Cached Vectors remain scoped inside
// ObjectIO; only caller-owned output data survives this call.
func materializeBlockData(
	ctx context.Context,
	info *objectio.BlockInfo,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	selectRows []int64,
	visibilityTS *types.TS,
	policy fileservice.Policy,
	outputBat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (deleteMask objectio.Bitmap, err error) {
	if len(columns) != len(colTypes) {
		return deleteMask, moerr.NewInvalidInputNoCtxf(
			"block column count %d does not match type count %d",
			len(columns), len(colTypes),
		)
	}
	if phyAddrColumnPos < -1 || phyAddrColumnPos >= len(columns) {
		return deleteMask, moerr.NewInvalidInputNoCtxf(
			"physical address column position %d out of range [0, %d)",
			phyAddrColumnPos, len(columns),
		)
	}
	if len(outputBat.Vecs) < len(columns) {
		return deleteMask, moerr.NewInvalidInputNoCtxf(
			"block output vector count %d is smaller than column count %d",
			len(outputBat.Vecs), len(columns),
		)
	}
	for i := range columns {
		if outputBat.Vecs[i] == nil {
			return deleteMask, moerr.NewInvalidInputNoCtxf("nil output vector for block column %d", columns[i])
		}
	}

	idxes, typs := excludePhyAddrColumn(columns, colTypes, phyAddrColumnPos)
	destinations := outputBat.Vecs[:len(columns)]
	if phyAddrColumnPos >= 0 {
		destinations = make([]*vector.Vector, 0, len(idxes))
		for i := range columns {
			if i != phyAddrColumnPos {
				destinations = append(destinations, outputBat.Vecs[i])
			}
		}
	}

	deleteMask, _, err = ioutil.LoadColumnsDataInto(
		ctx,
		idxes,
		typs,
		fs,
		info.MetaLocation(),
		destinations,
		selectRows,
		visibilityTS,
		mp,
		policy,
	)
	if err != nil {
		return
	}

	if phyAddrColumnPos >= 0 {
		if selectRows != nil && len(selectRows) == 0 {
			outputBat.Vecs[phyAddrColumnPos].CleanOnlyData()
		} else if err = buildRowidColumn(
			info,
			outputBat.Vecs[phyAddrColumnPos],
			selectRows,
			mp,
		); err != nil {
			deleteMask.Release()
			deleteMask = objectio.NullBitmap
		}
	}
	return
}

func validateEarlyColumns(earlyColumns []int, columnCount int) error {
	if len(earlyColumns) == 0 || len(earlyColumns) >= columnCount {
		return moerr.NewInvalidInputNoCtxf(
			"early column count %d must be in [1, %d)",
			len(earlyColumns),
			columnCount,
		)
	}
	previous := -1
	for _, pos := range earlyColumns {
		if pos < 0 || pos >= columnCount {
			return moerr.NewInvalidInputNoCtxf(
				"early column position %d out of range [0, %d)",
				pos,
				columnCount,
			)
		}
		if pos <= previous {
			return moerr.NewInvalidInputNoCtx("early column positions must be sorted and unique")
		}
		previous = pos
	}
	return nil
}

func columnPositionsComplement(earlyColumns []int, columnCount int) []int {
	lateColumns := make([]int, 0, columnCount-len(earlyColumns))
	earlyIdx := 0
	for pos := 0; pos < columnCount; pos++ {
		if earlyIdx < len(earlyColumns) && earlyColumns[earlyIdx] == pos {
			earlyIdx++
			continue
		}
		lateColumns = append(lateColumns, pos)
	}
	return lateColumns
}

func validateLateMaterializationOutput(
	columns []uint16,
	colTypes []types.Type,
	outputBat *batch.Batch,
) error {
	if len(columns) != len(colTypes) {
		return moerr.NewInvalidInputNoCtxf(
			"block column count %d does not match type count %d",
			len(columns),
			len(colTypes),
		)
	}
	if outputBat == nil {
		return moerr.NewInvalidInputNoCtx("nil output batch for late block read")
	}
	if len(outputBat.Vecs) < len(columns) {
		return moerr.NewInvalidInputNoCtxf(
			"block output vector count %d is smaller than column count %d",
			len(outputBat.Vecs),
			len(columns),
		)
	}
	for pos := range columns {
		if outputBat.Vecs[pos] == nil {
			return moerr.NewInvalidInputNoCtxf("nil output vector for block column %d", columns[pos])
		}
	}
	return nil
}

// materializeBlockColumnsAtPositions builds a non-owning batch view over the
// caller's destination vectors. The view never releases those vectors.
func materializeBlockColumnsAtPositions(
	ctx context.Context,
	info *objectio.BlockInfo,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	positions []int,
	selectRows []int64,
	visibilityTS *types.TS,
	policy fileservice.Policy,
	outputBat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (objectio.Bitmap, error) {
	if outputBat == nil {
		return objectio.NullBitmap, moerr.NewInvalidInputNoCtx("nil output batch for block columns")
	}
	selectedColumns := make([]uint16, len(positions))
	selectedTypes := make([]types.Type, len(positions))
	selectedOutput := batch.NewWithSize(len(positions))
	selectedPhyAddrPos := -1
	for i, pos := range positions {
		if pos < 0 || pos >= len(columns) || pos >= len(colTypes) || pos >= len(outputBat.Vecs) {
			return objectio.NullBitmap, moerr.NewInvalidInputNoCtxf(
				"block column position %d is out of range",
				pos,
			)
		}
		selectedColumns[i] = columns[pos]
		selectedTypes[i] = colTypes[pos]
		selectedOutput.Vecs[i] = outputBat.Vecs[pos]
		if pos == phyAddrColumnPos {
			selectedPhyAddrPos = i
		}
	}
	return materializeBlockData(
		ctx,
		info,
		selectedColumns,
		selectedTypes,
		selectedPhyAddrPos,
		selectRows,
		visibilityTS,
		policy,
		selectedOutput,
		mp,
		fs,
	)
}

func validateReaderFilterResult(
	result engine.ReaderFilterResult,
	inputRows int,
	outputRows int,
) error {
	if result.All {
		if outputRows != inputRows {
			return moerr.NewInvalidInputNoCtxf(
				"residual filter marked all rows selected but changed row count from %d to %d",
				inputRows,
				outputRows,
			)
		}
		return nil
	}
	if len(result.Sels) != outputRows {
		return moerr.NewInvalidInputNoCtxf(
			"residual filter returned %d selections for %d output rows",
			len(result.Sels),
			outputRows,
		)
	}
	previous := int64(-1)
	for _, row := range result.Sels {
		if row < 0 || row >= int64(inputRows) {
			return moerr.NewInvalidInputNoCtxf(
				"residual filter row %d out of range [0, %d)",
				row,
				inputRows,
			)
		}
		if row <= previous {
			return moerr.NewInvalidInputNoCtx("residual filter rows must be sorted and unique")
		}
		previous = row
	}
	return nil
}

func blockDataReadWithFilter(
	ctx context.Context,
	info *objectio.BlockInfo,
	ds engine.DataSource,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	ts types.TS,
	storageSelectRows []int64,
	policy fileservice.Policy,
	outputBat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
	earlyColumns []int,
	applyFilter engine.ReaderFilter,
	preFilterRows *int,
) (err error) {
	if err = validateLateMaterializationOutput(columns, colTypes, outputBat); err != nil {
		return err
	}
	cacheVectors.Free(mp)

	var (
		deleteMask       objectio.Bitmap
		physicalRows     []int64
		physicalRowCount int
	)
	if storageSelectRows != nil {
		ignoredMask, readErr := materializeBlockColumnsAtPositions(
			ctx,
			info,
			columns,
			colTypes,
			phyAddrColumnPos,
			earlyColumns,
			storageSelectRows,
			nil,
			policy,
			outputBat,
			mp,
			fs,
		)
		ignoredMask.Release()
		if readErr != nil {
			return readErr
		}
		physicalRows = storageSelectRows
	} else {
		if ds == nil {
			return moerr.NewInvalidInputNoCtx("nil data source for late block read")
		}
		tombstones, tombstoneErr := ds.GetTombstones(ctx, &info.BlockID)
		if tombstoneErr != nil {
			tombstones.Release()
			return tombstoneErr
		}
		var visibilityTS *types.TS
		if info.IsAppendable() {
			visibilityTS = &ts
		}
		if deleteMask, err = materializeBlockColumnsAtPositions(
			ctx,
			info,
			columns,
			colTypes,
			phyAddrColumnPos,
			earlyColumns,
			nil,
			visibilityTS,
			policy,
			outputBat,
			mp,
			fs,
		); err != nil {
			tombstones.Release()
			return err
		}
		if !deleteMask.IsValid() {
			deleteMask = tombstones
		} else {
			deleteMask.Or(tombstones)
			tombstones.Release()
		}
		defer deleteMask.Release()

		physicalRowCount = outputBat.Vecs[earlyColumns[0]].Length()
		if !deleteMask.IsEmpty() {
			deleted := vector.GetSels()
			defer func() { vector.PutSels(deleted) }()
			deleted = deleteMask.ToI64Array(&deleted)
			for _, pos := range earlyColumns {
				outputBat.Vecs[pos].Shrink(deleted, true)
			}
		}
	}

	liveRows := outputBat.Vecs[earlyColumns[0]].Length()
	outputBat.SetRowCount(liveRows)
	for _, pos := range earlyColumns {
		if outputBat.Vecs[pos].Length() != liveRows {
			return moerr.NewInvalidInputNoCtxf(
				"early column %d has %d rows before filtering, expected %d",
				pos,
				outputBat.Vecs[pos].Length(),
				liveRows,
			)
		}
	}
	if preFilterRows != nil {
		*preFilterRows = liveRows
	}
	if liveRows == 0 {
		return nil
	}

	filterResult, err := applyFilter(outputBat, earlyColumns)
	if err != nil {
		return err
	}
	if err = validateReaderFilterResult(filterResult, liveRows, outputBat.RowCount()); err != nil {
		return err
	}
	for _, pos := range earlyColumns {
		if outputBat.Vecs[pos].Length() != outputBat.RowCount() {
			return moerr.NewInvalidInputNoCtxf(
				"residual filter left early column %d with %d rows for batch row count %d",
				pos,
				outputBat.Vecs[pos].Length(),
				outputBat.RowCount(),
			)
		}
	}
	if outputBat.RowCount() == 0 {
		return nil
	}

	selectedPhysicalRows := physicalRows
	if physicalRows == nil && !deleteMask.IsEmpty() {
		// Map only the surviving logical rows back to block offsets. In
		// particular, a zero-result filter returns above without building an
		// O(block rows) live-row slice.
		selectedPhysicalRows = vector.GetSels()
		defer func() { vector.PutSels(selectedPhysicalRows) }()
		nextSelected := 0
		liveRow := int64(0)
		for physicalRow := 0; physicalRow < physicalRowCount; physicalRow++ {
			if deleteMask.Contains(uint64(physicalRow)) {
				continue
			}
			if filterResult.All ||
				(nextSelected < len(filterResult.Sels) && filterResult.Sels[nextSelected] == liveRow) {
				selectedPhysicalRows = append(selectedPhysicalRows, int64(physicalRow))
				if !filterResult.All {
					nextSelected++
				}
			}
			liveRow++
		}
	} else if !filterResult.All {
		if physicalRows == nil {
			selectedPhysicalRows = filterResult.Sels
		} else {
			selectedPhysicalRows = vector.GetSels()
			defer func() { vector.PutSels(selectedPhysicalRows) }()
			for _, row := range filterResult.Sels {
				selectedPhysicalRows = append(selectedPhysicalRows, physicalRows[row])
			}
		}
	}

	lateColumns := columnPositionsComplement(earlyColumns, len(columns))
	ignoredMask, readErr := materializeBlockColumnsAtPositions(
		ctx,
		info,
		columns,
		colTypes,
		phyAddrColumnPos,
		lateColumns,
		selectedPhysicalRows,
		nil,
		policy,
		outputBat,
		mp,
		fs,
	)
	ignoredMask.Release()
	if readErr != nil {
		return readErr
	}
	for _, pos := range lateColumns {
		if outputBat.Vecs[pos].Length() != outputBat.RowCount() {
			return moerr.NewInvalidInputNoCtxf(
				"late column %d has %d rows for batch row count %d",
				pos,
				outputBat.Vecs[pos].Length(),
				outputBat.RowCount(),
			)
		}
	}
	return nil
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
	orderByLimit *objectio.IndexReaderTopOp,
	policy fileservice.Policy,
	outputBat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	// A filter has already applied appendable visibility and tombstones to
	// selectRows. For the common point/range lookup path, copy only those rows
	// while the cache entries are pinned. Vector TopN still needs the complete
	// source Vector and therefore keeps the existing owned-vector path below.
	if selectRows != nil &&
		(orderByLimit == nil || orderByLimit.OrderedLimit) {
		cacheVectors.Free(mp)
		if orderByLimit != nil {
			selectRows, _, err = handleOrderByLimitOnSelectRows(
				ctx,
				selectRows,
				orderByLimit,
				info,
				phyAddrColumnPos,
				nil,
			)
			if err != nil {
				return err
			}
		}
		_, err = materializeBlockData(
			ctx,
			info,
			columns,
			colTypes,
			phyAddrColumnPos,
			selectRows,
			nil,
			policy,
			outputBat,
			mp,
			fs,
		)
		return err
	}

	var (
		deletedRows []int64
		deleteMask  objectio.Bitmap
		release     func()
	)

	// Normal scans must materialize the complete result, but they do not need
	// an intermediate owned copy of each cached varlen column. Read the output
	// columns and appendable commit-ts in one pinned IOVector, copy once into the
	// output batch, then apply the same persisted/in-memory delete masks as the
	// legacy path.
	if orderByLimit == nil {
		cacheVectors.Free(mp)
		if ds == nil {
			return moerr.NewInvalidInputNoCtx("nil data source for full block read")
		}
		tombstones, tombstoneErr := ds.GetTombstones(ctx, &info.BlockID)
		if tombstoneErr != nil {
			tombstones.Release()
			return tombstoneErr
		}
		var visibilityTS *types.TS
		if info.IsAppendable() {
			visibilityTS = &ts
		}
		if deleteMask, err = materializeBlockData(
			ctx,
			info,
			columns,
			colTypes,
			phyAddrColumnPos,
			nil,
			visibilityTS,
			policy,
			outputBat,
			mp,
			fs,
		); err != nil {
			tombstones.Release()
			return err
		}
		if !deleteMask.IsValid() {
			deleteMask = tombstones
		} else {
			deleteMask.Or(tombstones)
			tombstones.Release()
		}
		defer deleteMask.Release()

		if !deleteMask.IsEmpty() {
			arr := vector.GetSels()
			defer vector.PutSels(arr)
			deletedRows = deleteMask.ToI64Array(&arr)
			for i := range columns {
				outputBat.Vecs[i].Shrink(deletedRows, true)
			}
		}
		return nil
	}

	// Persisted vector TopN only needs read access to the complete embedding
	// column. Compute distances while that one cache entry is pinned, then copy
	// only the selected rows from the remaining output columns. Appendable
	// blocks keep the legacy path because their visibility columns participate
	// in the same read lifetime.
	if !orderByLimit.OrderedLimit && !info.IsAppendable() {
		if ds == nil {
			return moerr.NewInvalidInputNoCtx("nil data source for vector topn read")
		}
		topColPos := int(orderByLimit.ColPos)
		if topColPos < 0 || topColPos >= len(columns) || topColPos == phyAddrColumnPos {
			return moerr.NewInvalidInputNoCtxf(
				"vector topn column position %d is invalid for %d block columns",
				topColPos,
				len(columns),
			)
		}

		inputRows := selectRows
		if inputRows == nil {
			tombstones, tombstoneErr := ds.GetTombstones(ctx, &info.BlockID)
			if tombstoneErr != nil {
				return tombstoneErr
			}
			defer tombstones.Release()
			inputRows = buildTopInputRows(int(info.MetaLocation().Rows()), tombstones)
		}

		var dists []float64
		selectRows, dists, _, err = ioutil.LoadColumnDataByTopN(
			ctx,
			columns[topColPos],
			colTypes[topColPos],
			fs,
			info.MetaLocation(),
			inputRows,
			orderByLimit,
			mp,
			policy,
		)
		if err != nil {
			return err
		}
		return materializeVectorTopNRows(
			ctx,
			info,
			columns,
			colTypes,
			phyAddrColumnPos,
			topColPos,
			selectRows,
			dists,
			policy,
			outputBat,
			mp,
			fs,
		)
	}

	// read block data from storage specified by meta location
	if deleteMask, release, err = readBlockData(
		ctx,
		columns,
		colTypes,
		phyAddrColumnPos,
		info,
		ts,
		policy,
		cacheVectors,
		mp,
		fs,
	); err != nil {
		return
	}
	defer release()
	defer deleteMask.Release()

	// len(selectRows) > 0 means it was already filtered by pk filter
	if len(selectRows) > 0 {
		var dists []float64

		// The selected-row fast path above already returned when orderByLimit
		// was nil (or is an ordered-limit); this remaining path is vector TopN.
		selectRows, dists, err = handleOrderByLimitOnSelectRows(ctx, selectRows, orderByLimit, info, phyAddrColumnPos, cacheVectors)
		if err != nil {
			return err
		}

		return fillOutputBatchBySelectedRows(
			info,
			columns,
			phyAddrColumnPos,
			outputBat,
			cacheVectors,
			selectRows,
			orderByLimit,
			dists,
			mp,
		)
	}

	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		return
	}

	// merge deletes from tombstones
	if !deleteMask.IsValid() {
		deleteMask = tombstones
	} else {
		deleteMask.Or(tombstones)
		tombstones.Release()
	}

	// Note: it always goes here if no filter or the block is not sorted

	// transform delete mask to deleted rows
	// TODO: avoid this transformation
	if !deleteMask.IsEmpty() {
		arr := vector.GetSels()
		defer func() {
			vector.PutSels(arr)
		}()

		deletedRows = deleteMask.ToI64Array(&arr)
	}

	if shouldFallbackOrderedLimitToFullBlockRead(orderByLimit, info) {
		// Ordered-limit pushdown can only prune sorted blocks. Fall back to the
		// normal UnionBatch+Shrink path on unsorted blocks to avoid building
		// full row-index slices that do not eliminate any rows.
		orderByLimit = nil
	}

	// No pre-filter rows, but vector TopN pushdown is requested:
	// apply TopN on live rows (exclude tombstones first), then materialize selected rows.
	if orderByLimit != nil {
		var dists []float64
		selectRows, dists, err = handleOrderByLimitOnLiveRows(ctx, orderByLimit, info, phyAddrColumnPos, deleteMask, cacheVectors)
		if err != nil {
			return err
		}

		return fillOutputBatchBySelectedRows(
			info,
			columns,
			phyAddrColumnPos,
			outputBat,
			cacheVectors,
			selectRows,
			orderByLimit,
			dists,
			mp,
		)
	}

	// build rowid column if needed
	if phyAddrColumnPos >= 0 {
		if err = buildRowidColumn(
			info, outputBat.Vecs[phyAddrColumnPos], nil, mp,
		); err != nil {
			return
		}
	}

	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos != phyAddrColumnPos {
			loadedCol := &cacheVectors[loadedColumnPos]
			if err = outputBat.Vecs[outputColPos].UnionBatch(
				loadedCol,
				0,
				loadedCol.Length(),
				nil,
				mp,
			); err != nil {
				break
			}
			loadedColumnPos++
		}
		if len(deletedRows) > 0 {
			outputBat.Vecs[outputColPos].Shrink(deletedRows, true)
		}
	}
	return
}

func materializeVectorTopNRows(
	ctx context.Context,
	info *objectio.BlockInfo,
	columns []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	topColPos int,
	selectRows []int64,
	dists []float64,
	policy fileservice.Policy,
	outputBat *batch.Batch,
	mp *mpool.MPool,
	fs fileservice.FileService,
) error {
	loadColumns := make([]uint16, 0, len(columns)-1)
	loadTypes := make([]types.Type, 0, len(columns)-1)
	destinations := make([]*vector.Vector, 0, len(columns)-1)
	for pos := range columns {
		if pos == phyAddrColumnPos || pos == topColPos {
			continue
		}
		loadColumns = append(loadColumns, columns[pos])
		loadTypes = append(loadTypes, colTypes[pos])
		destinations = append(destinations, outputBat.Vecs[pos])
	}
	if len(loadColumns) > 0 {
		if _, _, err := ioutil.LoadColumnsDataInto(
			ctx,
			loadColumns,
			loadTypes,
			fs,
			info.MetaLocation(),
			destinations,
			selectRows,
			nil,
			mp,
			policy,
		); err != nil {
			return err
		}
	}

	outputBat.Vecs[topColPos].CleanOnlyData()
	if phyAddrColumnPos >= 0 {
		if len(selectRows) == 0 {
			outputBat.Vecs[phyAddrColumnPos].CleanOnlyData()
		} else if err := buildRowidColumn(
			info,
			outputBat.Vecs[phyAddrColumnPos],
			selectRows,
			mp,
		); err != nil {
			return err
		}
	}

	var distVec *vector.Vector
	if len(outputBat.Vecs) == len(columns) {
		distVec = vector.NewVec(types.T_float64.ToType())
		outputBat.Vecs = append(outputBat.Vecs, distVec)
	} else {
		distVec = outputBat.Vecs[len(columns)]
	}
	return vector.AppendFixedList(distVec, dists, nil, mp)
}

// buildTopInputRows constructs a slice of live row indices by excluding rows
// present in the deleteMask. Returns nil when there is nothing to filter.
func buildTopInputRows(length int, deleteMask objectio.Bitmap) []int64 {
	if length <= 0 || deleteMask.IsEmpty() {
		return nil
	}
	capHint := length - deleteMask.Count()
	if capHint < 0 {
		capHint = 0
	}
	rows := make([]int64, 0, capHint)
	for i := 0; i < length; i++ {
		if !deleteMask.Contains(uint64(i)) {
			rows = append(rows, int64(i))
		}
	}
	return rows
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

// This func load columns from storage of specified column indexes
// No memory copy, the loaded data is directly stored in the cacheVectors
// if `phyAddrColumnPos` >= 0, it means one of the columns is the physical address column,
// which is not loaded from storage, but generated by the blockid and rowid. We should exclude it.
// `release` is a function to release the pinned memory cache
// Example 1:
// colIndexes = [0, 1, 2, 3], phyAddrColumnPos = 2
// 1) exclude the physical address column => [0, 1, 3]
// 2) load columns [0, 1, 3] from storage into cacheVectors[0, 1, 2]
// Example 2:
// colIndexes = [0, 1, 2, 3], phyAddrColumnPos = -1
// load columns [0, 1, 2, 3] from storage into cacheVectors[0, 1, 2, 3]
func readBlockData(
	ctx context.Context,
	colIndexes []uint16,
	colTypes []types.Type,
	phyAddrColumnPos int,
	info *objectio.BlockInfo,
	ts types.TS,
	policy fileservice.Policy,
	cacheVectors containers.Vectors,
	m *mpool.MPool,
	fs fileservice.FileService,
) (
	deleteMask objectio.Bitmap,
	release func(),
	err error,
) {
	cacheVectors.Free(m)
	defer func() {
		if err != nil && release != nil {
			release()
			release = nil
		}
	}()

	idxes, typs := excludePhyAddrColumn(colIndexes, colTypes, phyAddrColumnPos)

	readColumns := func(
		cols []uint16,
		columnTypes []types.Type,
		cacheVectors2 containers.Vectors,
	) (err2 error) {
		if len(cols) == 0 && phyAddrColumnPos >= 0 {
			// only read rowid column on non appendable block, return early
			release = func() {}
			return
		}

		release, _, err2 = ioutil.LoadColumns(
			ctx, cols, columnTypes, fs, info.MetaLocation(), cacheVectors2, m, policy,
		)
		return
	}

	readABlkColumns := func(
		cols []uint16,
		cacheVectors2 containers.Vectors,
	) (
		deletes objectio.Bitmap,
		err2 error,
	) {
		// Appendable blocks are filtered by both MVCC special columns. The
		// object reader synthesizes a NULL abort vector for old commitTS-only
		// objects, which is interpreted as "no aborted rows".
		readCols := make([]uint16, len(cols), len(cols)+2)
		copy(readCols, cols)
		readCols = append(readCols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT)
		readTypes := make([]types.Type, len(typs), len(typs)+2)
		copy(readTypes, typs)
		readTypes = append(readTypes, objectio.TSType, types.T_bool.ToType())

		if err2 = readColumns(readCols, readTypes, cacheVectors2); err2 != nil {
			return
		}

		t0 := time.Now()
		abortVec := &cacheVectors2[len(readCols)-1]
		commitVec := &cacheVectors2[len(readCols)-2]
		// Decoded vectors know the actual block cardinality. MetaLocation.Rows
		// can be an ObjectStats-derived estimate for intermediate short blocks.
		rowCount := commitVec.Length()
		for pos := range readCols {
			if cacheVectors2[pos].Length() != rowCount {
				return objectio.Bitmap{}, moerr.NewInternalErrorf(
					ctx,
					"appendable block column %d has %d rows, expected %d",
					pos, cacheVectors2[pos].Length(), rowCount,
				)
			}
		}
		commits, err2 := ioutil.ValidateTombstoneCommitTSColumn(rowCount, commitVec)
		if err2 != nil {
			return objectio.Bitmap{}, err2
		}
		aborts, err2 := ioutil.ValidateTombstoneAbortColumn(rowCount, abortVec)
		if err2 != nil {
			return objectio.Bitmap{}, err2
		}

		deletes = objectio.GetReusableBitmap()
		for i := 0; i < rowCount; i++ {
			commitTS := commits.At(i)
			if commitTS.Equal(&txnif.UncommitTS) || commitTS.GT(&ts) ||
				(aborts.IsPresent() && aborts.At(i)) {
				deletes.Add(uint64(i))
			}
		}
		logutil.Debugf(
			"blockread %s scan filter cost %v: base %s filter out %v\n ",
			info.BlockID.String(),
			time.Since(t0),
			ts.ToString(),
			deletes.Count(),
		)
		return
	}

	if info.IsAppendable() {
		deleteMask, err = readABlkColumns(idxes, cacheVectors)
	} else {
		err = readColumns(idxes, typs, cacheVectors)
	}

	return
}

func handleOrderByLimitOnSelectRows(
	ctx context.Context,
	selectRows []int64,
	orderByLimit *objectio.IndexReaderTopOp,
	info *objectio.BlockInfo,
	phyAddrColumnPos int,
	cacheVectors containers.Vectors,
) ([]int64, []float64, error) {
	if orderByLimit.OrderedLimit {
		if info == nil || !info.IsSorted() || uint64(len(selectRows)) <= orderByLimit.Limit {
			return selectRows, nil, nil
		}
		limit := int(orderByLimit.Limit)
		if orderByLimit.Desc {
			return selectRows[len(selectRows)-limit:], nil, nil
		}
		return selectRows[:limit], nil, nil
	}

	vecColPos := orderByLimit.ColPos
	if phyAddrColumnPos >= 0 && vecColPos > int32(phyAddrColumnPos) {
		vecColPos--
	}
	vecCol := &cacheVectors[vecColPos]

	return HandleOrderByLimitOnIVFFlatIndex(ctx, selectRows, vecCol, orderByLimit)
}

func handleOrderByLimitOnLiveRows(
	ctx context.Context,
	orderByLimit *objectio.IndexReaderTopOp,
	info *objectio.BlockInfo,
	phyAddrColumnPos int,
	deleteMask objectio.Bitmap,
	cacheVectors containers.Vectors,
) ([]int64, []float64, error) {
	vecColPos := orderByLimit.ColPos
	if phyAddrColumnPos >= 0 && vecColPos > int32(phyAddrColumnPos) {
		vecColPos--
	}
	if orderByLimit.OrderedLimit {
		rowCount := 0
		if int(vecColPos) < len(cacheVectors) {
			rowCount = cacheVectors[vecColPos].Length()
		} else if info != nil {
			rowCount = int(info.MetaLocation().Rows())
		}
		return buildOrderedLiveRows(rowCount, deleteMask, orderByLimit, info != nil && info.IsSorted()), nil, nil
	}

	vecCol := &cacheVectors[vecColPos]
	selectRows := buildTopInputRows(vecCol.Length(), deleteMask)
	return HandleOrderByLimitOnIVFFlatIndex(ctx, selectRows, vecCol, orderByLimit)
}

func shouldFallbackOrderedLimitToFullBlockRead(
	orderByLimit *objectio.IndexReaderTopOp,
	info *objectio.BlockInfo,
) bool {
	return orderByLimit != nil && orderByLimit.OrderedLimit && (info == nil || !info.IsSorted())
}

func buildOrderedLiveRows(
	rowCount int,
	deleteMask objectio.Bitmap,
	orderByLimit *objectio.IndexReaderTopOp,
	isSorted bool,
) []int64 {
	if rowCount <= 0 {
		return nil
	}
	if !isSorted {
		if deleteMask.IsEmpty() {
			return buildContiguousRows(0, rowCount)
		}
		return buildTopInputRows(rowCount, deleteMask)
	}
	limit := rowCount
	if orderByLimit.Limit > 0 && orderByLimit.Limit < uint64(rowCount) {
		limit = int(orderByLimit.Limit)
	}
	if orderByLimit.Desc {
		return buildOrderedLiveRowsDesc(rowCount, deleteMask, limit)
	}
	return buildOrderedLiveRowsAsc(rowCount, deleteMask, limit)
}

func buildOrderedLiveRowsAsc(rowCount int, deleteMask objectio.Bitmap, limit int) []int64 {
	if deleteMask.IsEmpty() {
		return buildContiguousRows(0, limit)
	}
	rows := make([]int64, 0, limit)
	for i := 0; i < rowCount && len(rows) < limit; i++ {
		if !deleteMask.Contains(uint64(i)) {
			rows = append(rows, int64(i))
		}
	}
	return rows
}

func buildOrderedLiveRowsDesc(rowCount int, deleteMask objectio.Bitmap, limit int) []int64 {
	if deleteMask.IsEmpty() {
		return buildContiguousRows(rowCount-limit, rowCount)
	}
	rows := make([]int64, 0, limit)
	for i := rowCount - 1; i >= 0 && len(rows) < limit; i-- {
		if !deleteMask.Contains(uint64(i)) {
			rows = append(rows, int64(i))
		}
	}
	slices.Reverse(rows)
	return rows
}

func buildContiguousRows(start, end int) []int64 {
	if end <= start {
		return nil
	}
	rows := make([]int64, end-start)
	for i := range rows {
		rows[i] = int64(start + i)
	}
	return rows
}
