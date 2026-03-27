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
	"container/heap"
	"context"
	"fmt"
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (sels []int64, err error) {
	// PXU TODO: temporary solution, need to be refactored
	// cannot filter by physical address column now
	deleteMask, release, err := readBlockData(
		ctx,
		columns,
		colTypes,
		-1,
		info,
		ds,
		ts,
		fileservice.Policy(0),
		cacheVectors,
		mp,
		fs,
	)
	if err != nil {
		return
	}
	defer release()
	defer deleteMask.Release()

	sels = searchFunc(cacheVectors)
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
		deleteMask objectio.Bitmap
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

	cacheVectors := containers.NewVectors(len(columns) + 1)

	phyAddrColumnPos := -1
	for i := range columns {
		if columns[i] == objectio.SEQNUM_ROWID {
			phyAddrColumnPos = i
			break
		}
	}

	// read block data from storage specified by meta location
	if deleteMask, release, err = readBlockData(
		ctx, columns, colTypes, phyAddrColumnPos, info, ds, ts, policy, cacheVectors, mp, fs,
	); err != nil {
		return nil, nil, nil, err
	}
	defer deleteMask.Release()

	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		release()
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

	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos != phyAddrColumnPos {
			outputBat.Vecs[outputColPos] = &cacheVectors[loadedColumnPos]
			loadedColumnPos++
		} else {
			outputBat.Vecs[outputColPos] = vector.NewVec(objectio.RowidType)
			if err = buildRowidColumn(
				info, outputBat.Vecs[phyAddrColumnPos], nil, mp,
			); err != nil {
				release()
				return nil, nil, nil, err
			}
			release = func() {
				release()
				outputBat.Vecs[phyAddrColumnPos].Free(mp)
			}
		}
	}
	outputBat.SetRowCount(outputBat.Vecs[0].Length())

	// FIXME: w-zr
	var retMask *nulls.Bitmap

	if !deleteMask.IsEmpty() {
		retMask = &nulls.Bitmap{}
		retMask.OrBitmap(deleteMask.Bitmap())
	}
	return outputBat, retMask, release, nil
}

// BlockDataReadWait waits for the TopN calculation job initiated by Launch to complete.
// It then materializes the selected rows and their distances into the output batch.
func BlockDataReadWait(
	ctx context.Context,
	job *IVFFlatIndexJob,
	info *objectio.BlockInfo,
	columns []uint16,
	phyAddrColumnPos int,
	orderByLimit *objectio.IndexReaderTopOp,
	bat *batch.Batch,
	cacheVectors containers.Vectors,
	mp *mpool.MPool,
) error {
	if job == nil {
		if bat.Vecs[0].Length() > 0 {
			bat.SetRowCount(bat.Vecs[0].Length())
		}
		return nil
	}
	if job.Release != nil {
		defer job.Release()
	}

	selectRows, dists, err := handleOrderByLimitOnSelectRowsWait(ctx, job)
	if err != nil {
		return err
	}

	err = fillOutputBatchBySelectedRows(
		info,
		columns,
		phyAddrColumnPos,
		bat,
		cacheVectors,
		selectRows,
		orderByLimit,
		dists,
		mp,
	)
	if err != nil {
		return err
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

// BlockDataRead is a synchronous wrapper around BlockDataReadLaunch and BlockDataReadWait.
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
	job, err := BlockDataReadLaunch(
		ctx, info, ds, columns, colTypes, phyAddrColumnPos, ts,
		filterSeqnums, filterColTypes, filter, orderByLimit,
		policy, tableName, bat, cacheVectors, mp, fs,
		metric.GPUThresholdSync,
	)
	if err != nil {
		return err
	}
	return BlockDataReadWait(
		ctx, job, info, columns, phyAddrColumnPos, orderByLimit, bat, cacheVectors, mp,
	)
}

// BlockDataReadLaunch initiates a block read and potentially a TopN pruning job.
// It returns an IVFFlatIndexJob handle if an asynchronous computation was launched.
func BlockDataReadLaunch(
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
	minWorkSize uint64,
) (*IVFFlatIndexJob, error) {
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
			cacheVectors,
			mp,
			fs,
		); err != nil {
			return nil, err
		}
		v2.TxnSelReadFilterTotal.Observe(1.0)

		if len(sels) == 0 {
			v2.TxnSelReadFilterFiltered.Observe(1.0)
			return nil, nil
		}
	}

	return BlockDataReadInnerLaunch(
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
		minWorkSize,
	)
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

	if release, err = ioutil.LoadColumns(
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
		loaded, sortKey, err = ioutil.LoadOneBlock(ctx, fs, info.MetaLocation(), objectio.SchemaData)
	} else {
		loaded, sortKey, err = ioutil.LoadOneBlockWithIndex(ctx, fs, idxes, info.MetaLocation(), objectio.SchemaData)
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
				logutil.Info("[BlockDataReadBackup]",
					zap.String("commitTs", commitTs.ToString()),
					zap.String("ts", ts.ToString()),
					zap.String("location", info.MetaLocation().String()),
					zap.Int("rows", v))
				break
			}
		}
	}
	tombstones, err := ds.GetTombstones(ctx, &info.BlockID)
	if err != nil {
		return
	}
	defer tombstones.Release()
	rows := tombstones.ToI64Array(nil)
	if len(rows) > 0 {
		logutil.Info("[BlockDataReadBackup Shrink]", zap.String("location", info.MetaLocation().String()), zap.Int("rows", len(rows)))
		loaded.Shrink(rows, true)
	}
	return
}

type IVFFlatIndexJob struct {
	JobHandle     metric.PairwiseJobHandle
	SelectRows    []int64
	PairwiseDists []float32
	OrderByLimit  *objectio.IndexReaderTopOp
	Release       func()
}

// CleanupIVFFlatIndexJob drains a job that was launched but will never be waited on
// (e.g. when the reader is closed mid-stream due to an error). It waits for any
// pending GPU computation to finish before freeing the associated C memory via Release.
func CleanupIVFFlatIndexJob(job *IVFFlatIndexJob) {
	if job == nil {
		return
	}
	if job.JobHandle.IsValid() {
		metric.PairwiseDistanceWait(job.JobHandle, job.OrderByLimit.MetricType) //nolint:errcheck
	}
	if job.Release != nil {
		job.Release()
	}
}

// HandleOrderByLimitOnIVFFlatIndexLaunch initiates the TopN pruning and distance calculation
// for an IVFFlat index. It returns a job handle that can be used to wait for completion.
// This allows the caller to overlap this potentially expensive calculation (especially on GPU)
// with other tasks like fetching the next block's metadata.
func HandleOrderByLimitOnIVFFlatIndexLaunch(
	ctx context.Context,
	selectRows []int64,
	vecCol *vector.Vector,
	orderByLimit *objectio.IndexReaderTopOp,
	minWorkSize uint64,
) (*IVFFlatIndexJob, error) {
	if selectRows == nil {
		selectRows = make([]int64, vecCol.Length())
		for i := range selectRows {
			selectRows[i] = int64(i)
		}
	}

	nullsBm := vecCol.GetNulls()
	selectRows = slices.DeleteFunc(selectRows, func(row int64) bool {
		return nullsBm.Contains(uint64(row))
	})

	switch orderByLimit.Typ {
	case types.T_array_float32:
		rhs := types.BytesToArray[float32](orderByLimit.NumVec)
		dim := len(rhs)
		if dim == 0 {
			return nil, moerr.NewInternalError(ctx, "empty query vector")
		}
		nX := len(selectRows)
		if nX == 0 {
			return nil, nil
		}

		lhs := make([][]float32, nX)
		for i, row := range selectRows {
			lhs[i] = types.BytesToArray[float32](vecCol.GetBytesAt(int(row)))
		}

		pairwiseDists := make([]float32, nX)

		// Launch asynchronously (GPU or CPU based on build tags)
		handle, err := metric.PairwiseDistanceLaunch(
			lhs,
			[][]float32{rhs},
			orderByLimit.MetricType,
			0, // Default deviceID
			pairwiseDists,
			minWorkSize,
		)
		if err != nil {
			return nil, err
		}

		return &IVFFlatIndexJob{
			JobHandle:     handle,
			SelectRows:    selectRows,
			PairwiseDists: pairwiseDists,
			OrderByLimit:  orderByLimit,
		}, nil

	case types.T_array_float64:
		rhs := types.BytesToArray[float64](orderByLimit.NumVec)
		dim := len(rhs)
		if dim == 0 {
			return nil, moerr.NewInternalError(ctx, "empty query vector")
		}
		nX := len(selectRows)
		if nX == 0 {
			return nil, nil
		}

		lhs := make([][]float64, nX)
		for i, row := range selectRows {
			lhs[i] = types.BytesToArray[float64](vecCol.GetBytesAt(int(row)))
		}

		pairwiseDists := make([]float32, nX)

		// Launch asynchronously (GPU or CPU based on build tags)
		handle, err := metric.PairwiseDistanceLaunch(
			lhs,
			[][]float64{rhs},
			orderByLimit.MetricType,
			0, // Default deviceID
			pairwiseDists,
			minWorkSize,
		)
		if err != nil {
			return nil, err
		}

		return &IVFFlatIndexJob{
			JobHandle:     handle,
			SelectRows:    selectRows,
			PairwiseDists: pairwiseDists,
			OrderByLimit:  orderByLimit,
		}, nil

	default:
		return nil, moerr.NewInternalError(ctx, fmt.Sprintf("only support float32/float64 type for topn: %s", orderByLimit.Typ))
	}
}

// HandleOrderByLimitOnIVFFlatIndexWait waits for the completion of the TopN job
// initiated by the Launch function. It performs the final sorting and pruning
// of the results based on the calculated distances.
func HandleOrderByLimitOnIVFFlatIndexWait(
	ctx context.Context,
	job *IVFFlatIndexJob,
) ([]int64, []float64, error) {
	if job == nil {
		return nil, nil, nil
	}

	// Wait for completion
	_, err := metric.PairwiseDistanceWait(job.JobHandle, job.OrderByLimit.MetricType)
	if err != nil {
		return nil, nil, err
	}

	selectRows := job.SelectRows
	pairwiseDists := job.PairwiseDists
	orderByLimit := job.OrderByLimit
	nX := len(selectRows)

	resIdx := 0
	sels := make([]int64, nX)
	dists := make([]float64, nX)

	for i, row := range selectRows {
		dist64 := float64(pairwiseDists[i])

		if orderByLimit.LowerBoundType == plan.BoundType_INCLUSIVE {
			if dist64 < orderByLimit.LowerBound {
				continue
			}
		} else if orderByLimit.LowerBoundType == plan.BoundType_EXCLUSIVE {
			if dist64 <= orderByLimit.LowerBound {
				continue
			}
		}
		if orderByLimit.UpperBoundType == plan.BoundType_INCLUSIVE {
			if dist64 > orderByLimit.UpperBound {
				continue
			}
		} else if orderByLimit.UpperBoundType == plan.BoundType_EXCLUSIVE {
			if dist64 >= orderByLimit.UpperBound {
				continue
			}
		}

		if len(orderByLimit.DistHeap) >= int(orderByLimit.Limit) {
			if dist64 < orderByLimit.DistHeap[0] {
				orderByLimit.DistHeap[0] = dist64
				heap.Fix(&orderByLimit.DistHeap, 0)
			} else {
				continue
			}
		} else {
			heap.Push(&orderByLimit.DistHeap, dist64)
		}

		sels[resIdx] = row
		dists[resIdx] = dist64
		resIdx++
	}
	sels = sels[:resIdx]
	dists = dists[:resIdx]

	finalIdx := 0
	for i := 0; i < len(sels); i++ {
		if dists[i] <= orderByLimit.DistHeap[0] {
			sels[finalIdx] = sels[i]
			dists[finalIdx] = dists[i]
			finalIdx++
		}
	}
	return sels[:finalIdx], dists[:finalIdx], nil
}

// HandleOrderByLimitOnIVFFlatIndex is a synchronous wrapper around Launch and Wait.
func HandleOrderByLimitOnIVFFlatIndex(
	ctx context.Context,
	selectRows []int64,
	vecCol *vector.Vector,
	orderByLimit *objectio.IndexReaderTopOp,
) ([]int64, []float64, error) {
	job, err := HandleOrderByLimitOnIVFFlatIndexLaunch(ctx, selectRows, vecCol, orderByLimit, metric.GPUThresholdSync)
	if err != nil {
		return nil, nil, err
	}
	return HandleOrderByLimitOnIVFFlatIndexWait(ctx, job)
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
		outputBat.Vecs[phyAddrColumnPos].CleanOnlyData()
		if len(selectRows) > 0 {
			if err = buildRowidColumn(
				info, outputBat.Vecs[phyAddrColumnPos], selectRows, mp,
			); err != nil {
				return err
			}
		}
	}

	// cacheVectors contains all loaded columns from storage, and excludes the
	// physical address column. Fill output columns by selected rows.
	loadedColumnPos := 0
	for outputColPos := range columns {
		if outputColPos == phyAddrColumnPos {
			continue
		}
		if len(selectRows) == 0 {
			outputBat.Vecs[outputColPos].CleanOnlyData()
			loadedColumnPos++
			continue
		}
		outputBat.Vecs[outputColPos].CleanOnlyData()
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

	if orderByLimit != nil {
		if len(outputBat.Vecs) == len(columns) {
			distVec := vector.NewVec(types.T_float64.ToType())
			if err = vector.AppendFixedList(distVec, dists, nil, mp); err != nil {
				distVec.Free(mp)
				return err
			}
			outputBat.Vecs = append(outputBat.Vecs, distVec)
		} else {
			distVec := outputBat.Vecs[len(outputBat.Vecs)-1]
			distVec.CleanOnlyData()
			if err := vector.AppendFixedList(distVec, dists, nil, mp); err != nil {
				return err
			}
		}
	}

	return nil
}

// BlockDataReadInner only read data,don't apply deletes.
func BlockDataReadInnerLaunch(
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
	minWorkSize uint64,
) (job *IVFFlatIndexJob, err error) {
	var (
		deletedRows []int64
		deleteMask  objectio.Bitmap
		release     func()
	)

	// read block data from storage specified by meta location
	if deleteMask, release, err = readBlockData(
		ctx,
		columns,
		colTypes,
		phyAddrColumnPos,
		info,
		ds,
		ts,
		policy,
		cacheVectors,
		mp,
		fs,
	); err != nil {
		return
	}
	defer func() {
		if job == nil {
			release()
		}
	}()
	defer deleteMask.Release()

	// len(selectRows) > 0 means it was already filtered by pk filter
	if len(selectRows) > 0 {
		if orderByLimit != nil {
			job, err = handleOrderByLimitOnSelectRowsLaunch(ctx, selectRows, orderByLimit, phyAddrColumnPos, cacheVectors, minWorkSize)
			if job != nil {
				job.Release = release
			}
			return
		}

		err = fillOutputBatchBySelectedRows(
			info,
			columns,
			phyAddrColumnPos,
			outputBat,
			cacheVectors,
			selectRows,
			orderByLimit,
			nil,
			mp,
		)
		return nil, err
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

	// No pre-filter rows, but vector TopN pushdown is requested:
	// apply TopN on live rows (exclude tombstones first), then materialize selected rows.
	if orderByLimit != nil {
		topInputRows := buildTopInputRows(int(info.MetaLocation().Rows()), deleteMask)
		job, err = handleOrderByLimitOnSelectRowsLaunch(ctx, topInputRows, orderByLimit, phyAddrColumnPos, cacheVectors, minWorkSize)
		if job != nil {
			job.Release = release
		}
		return
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
	return nil, err
}

// buildTopInputRows constructs a slice of live row indices by excluding rows
// present in the deleteMask. Returns nil if deleteMask is empty.
func buildTopInputRows(length int, deleteMask objectio.Bitmap) []int64 {
	if deleteMask.IsEmpty() {
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
	_ engine.DataSource,
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

	idxes, typs := excludePhyAddrColumn(colIndexes, colTypes, phyAddrColumnPos)

	readColumns := func(
		cols []uint16,
		cacheVectors2 containers.Vectors,
	) (err2 error) {
		if len(cols) == 0 && phyAddrColumnPos >= 0 {
			// only read rowid column on non appendable block, return early
			release = func() {}
			return
		}

		release, err2 = ioutil.LoadColumns(
			ctx, cols, typs, fs, info.MetaLocation(), cacheVectors2, m, policy,
		)
		if err2 != nil {
			return
		}
		return
	}

	readABlkColumns := func(
		cols []uint16,
		cacheVectors2 containers.Vectors,
	) (
		deletes objectio.Bitmap,
		err2 error,
	) {
		// appendable block should be filtered by committs
		//cols = append(cols, objectio.SEQNUM_COMMITTS, objectio.SEQNUM_ABORT) // committs, aborted
		cols = append(cols, objectio.SEQNUM_COMMITTS) // committs, aborted

		// no need to add typs, the two columns won't be generated
		if err2 = readColumns(
			cols, cacheVectors2,
		); err2 != nil {
			return
		}

		deletes = objectio.GetReusableBitmap()

		t0 := time.Now()
		//aborts := vector.MustFixedColWithTypeCheck[bool](loaded.Vecs[len(loaded.Vecs)-1])
		commits := vector.MustFixedColWithTypeCheck[types.TS](&cacheVectors2[len(cols)-1])
		for i := 0; i < len(commits); i++ {
			if commits[i].GT(&ts) {
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
		err = readColumns(idxes, cacheVectors)
	}

	return
}

func handleOrderByLimitOnSelectRowsLaunch(
	ctx context.Context,
	selectRows []int64,
	orderByLimit *objectio.IndexReaderTopOp,
	phyAddrColumnPos int,
	cacheVectors containers.Vectors,
	minWorkSize uint64,
) (*IVFFlatIndexJob, error) {
	vecColPos := orderByLimit.ColPos
	if phyAddrColumnPos >= 0 && vecColPos > int32(phyAddrColumnPos) {
		vecColPos--
	}
	vecCol := &cacheVectors[vecColPos]

	return HandleOrderByLimitOnIVFFlatIndexLaunch(ctx, selectRows, vecCol, orderByLimit, minWorkSize)
}

func handleOrderByLimitOnSelectRowsWait(
	ctx context.Context,
	job *IVFFlatIndexJob,
) ([]int64, []float64, error) {
	return HandleOrderByLimitOnIVFFlatIndexWait(ctx, job)
}

func handleOrderByLimitOnSelectRows(
	ctx context.Context,
	selectRows []int64,
	orderByLimit *objectio.IndexReaderTopOp,
	phyAddrColumnPos int,
	cacheVectors containers.Vectors,
) ([]int64, []float64, error) {
	job, err := handleOrderByLimitOnSelectRowsLaunch(ctx, selectRows, orderByLimit, phyAddrColumnPos, cacheVectors, metric.GPUThresholdSync)
	if err != nil {
		return nil, nil, err
	}
	return handleOrderByLimitOnSelectRowsWait(ctx, job)
}
