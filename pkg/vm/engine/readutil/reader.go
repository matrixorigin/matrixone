// Copyright 2022 Matrix Origin
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

package readutil

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap"
)

// -----------------------------------------------------------------
// ------------------------ withFilterMixin ------------------------
// -----------------------------------------------------------------

func (mixin *withFilterMixin) reset() {
	// Cleanup reusableTempVec and other resources before resetting filter
	if mixin.filterState.filter.Cleanup != nil {
		mixin.filterState.filter.Cleanup()
	}
	if mixin.filterHint.BF != nil {
		mixin.filterHint.BF.Free()
		mixin.filterHint.BF = nil
	}
	mixin.filterState.filter = objectio.BlockReadFilter{}
	mixin.filterState.memFilter = MemPKFilter{}
	mixin.columns.indexOfFirstSortedColumn = -1
	mixin.columns.seqnums = nil
	mixin.columns.colTypes = nil
}

func (mixin *withFilterMixin) tryUpdateTombstoneColumns(cols []string) {
	pkColIdx := mixin.tableDef.Name2ColIndex[mixin.tableDef.Pkey.PkeyColName]
	pkCol := mixin.tableDef.Cols[pkColIdx]

	mixin.columns.seqnums = []uint16{0, 1}
	mixin.columns.colTypes = []types.Type{
		types.T_Rowid.ToType(),
		plan2.ExprType2Type(&pkCol.Typ)}

	mixin.columns.colTypes[1].Scale = pkCol.Typ.Scale
	mixin.columns.colTypes[1].Width = pkCol.Typ.Width

	if len(cols) == len(objectio.TombstoneAttrs_TN_Created) {
		mixin.columns.seqnums = append(mixin.columns.seqnums, 2)
		mixin.columns.colTypes = append(mixin.columns.colTypes, types.T_TS.ToType())
	}

	mixin.filterState.seqnums = mixin.columns.seqnums[:]
	mixin.filterState.colTypes = mixin.columns.colTypes[:]
}

// when the reader.Read is called for a new block, it will always
// call tryUpdate to update the seqnums
// NOTE: here we assume the tryUpdate is always called with the same cols
// for all blocks and it will only be updated once
func (mixin *withFilterMixin) tryUpdateColumns(cols []string) {
	if len(cols) == len(mixin.columns.seqnums) {
		return
	}

	if len(mixin.columns.seqnums) != 0 {
		panic(moerr.NewInternalErrorNoCtx("withFilterMixin tryUpdate called with different cols"))
	}

	// record the column selectivity
	chit, ctotal := len(cols), len(mixin.tableDef.Cols)
	// record per-read column counts for histogram metrics
	if ctotal >= chit {
		v2.TxnColumnReadCountHistogram.Observe(float64(chit))
		v2.TxnColumnTotalCountHistogram.Observe(float64(ctotal))
	}

	mixin.columns.seqnums = make([]uint16, len(cols))
	mixin.columns.colTypes = make([]types.Type, len(cols))
	// mixin.columns.colNulls = make([]bool, len(cols))
	mixin.columns.indexOfFirstSortedColumn = -1
	mixin.filterState.pkSeqNum = -1

	pkPos := -1

	if slices.Equal(cols, objectio.TombstoneAttrs_CN_Created) ||
		slices.Equal(cols, objectio.TombstoneAttrs_TN_Created) {
		mixin.tryUpdateTombstoneColumns(cols)
		return
	}

	for i, column := range cols {
		column = strings.ToLower(column)
		if objectio.IsPhysicalAddr(column) {
			mixin.columns.seqnums[i] = objectio.SEQNUM_ROWID
			mixin.columns.colTypes[i] = objectio.RowidType
			mixin.columns.phyAddrPos = i
		} else {
			if plan2.GetSortOrderByName(mixin.tableDef, column) == 0 {
				mixin.columns.indexOfFirstSortedColumn = i
			}
			colIdx := mixin.tableDef.Name2ColIndex[column]
			colDef := mixin.tableDef.Cols[colIdx]
			mixin.columns.seqnums[i] = uint16(colDef.Seqnum)

			if mixin.tableDef.Pkey != nil && mixin.tableDef.Pkey.PkeyColName == column {
				// primary key is in the cols
				pkPos = i
			}
			mixin.columns.colTypes[i] = plan2.ExprType2Type(&colDef.Typ)
			mixin.columns.colTypes[i].Scale = colDef.Typ.Scale
			mixin.columns.colTypes[i].Width = colDef.Typ.Width
		}
	}

	if pkPos != -1 {
		// For composite primary key, optimize BloomFilter filtering by using __mo_index_pri_col directly
		// if all conditions are met (IVF entries table, has BF, last PK col is __mo_index_pri_col, query includes it).
		if mixin.tableDef.Pkey != nil && len(mixin.tableDef.Pkey.Names) > 1 {
			// Composite primary key: check optimization conditions.
			lastPKColName := strings.ToLower(mixin.tableDef.Pkey.Names[len(mixin.tableDef.Pkey.Names)-1])

			// Check all conditions for optimization (must all be satisfied):
			// 1. Table type is IVF entries table.
			isIVFEntriesTable := mixin.tableDef.TableType == catalog.SystemSI_IVFFLAT_TblType_Entries
			// 2. Has bloom filter.
			hasBF := mixin.filterState.hasBF
			// 3. Last PK column is __mo_index_pri_col.
			isLastColPriCol := lastPKColName == strings.ToLower(catalog.IndexTablePrimaryColName)
			// 4. Query includes __mo_index_pri_col.
			lastPKColPos := -1
			for i, col := range cols {
				if strings.ToLower(col) == lastPKColName {
					lastPKColPos = i
					break
				}
			}

			if isIVFEntriesTable && hasBF && isLastColPriCol && lastPKColPos != -1 {
				// All conditions met: use both PK column and __mo_index_pri_col for filtering.
				// cacheVectors[0] will be used for PK filtering.
				// cacheVectors[1] will be used for BF filtering (directly, no unpacking needed).
				mixin.filterState.seqnums = []uint16{mixin.columns.seqnums[pkPos], mixin.columns.seqnums[lastPKColPos]}
				mixin.filterState.colTypes = []types.Type{mixin.columns.colTypes[pkPos], mixin.columns.colTypes[lastPKColPos]}
			} else {
				// Conditions not met: use composite PK column only.
				mixin.filterState.seqnums = []uint16{mixin.columns.seqnums[pkPos]}
				mixin.filterState.colTypes = mixin.columns.colTypes[pkPos : pkPos+1]
			}
		} else {
			// Single primary key or non-composite: use the primary key column directly.
			mixin.filterState.seqnums = []uint16{mixin.columns.seqnums[pkPos]}
			mixin.filterState.colTypes = mixin.columns.colTypes[pkPos : pkPos+1]
		}
	}

	if mixin.tableDef.Pkey != nil {
		colIdx := mixin.tableDef.Name2ColIndex[mixin.tableDef.Pkey.PkeyColName]
		colDef := mixin.tableDef.Cols[colIdx]
		mixin.filterState.pkSeqNum = int32(colDef.Seqnum)
	}
}

// -----------------------------------------------------------------
// ------------------------ emptyReader ----------------------------
// -----------------------------------------------------------------

func (r *EmptyReader) SetFilterZM(objectio.ZoneMap) {
}

func (r *EmptyReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (r *EmptyReader) SetOrderBy([]*plan.OrderBySpec) {
}

func (r *EmptyReader) SetIndexParam(param *plan.IndexReaderParam) {
}

func (r *EmptyReader) Close() error {
	return nil
}

func (r *EmptyReader) Read(
	_ context.Context,
	_ []string,
	_ *plan.Expr,
	_ *mpool.MPool,
	_ *batch.Batch,
) (bool, error) {
	return true, nil
}

func prepareGatherStats(ctx context.Context) (context.Context, int64, int64) {
	ctx = perfcounter.WithCounterSet(ctx, objectio.BlkReadStats.CounterSet)
	return ctx, objectio.BlkReadStats.CounterSet.FileService.Cache.Read.Load(),
		objectio.BlkReadStats.CounterSet.FileService.Cache.Hit.Load()
}

func gatherStats(lastNumRead, lastNumHit int64) {
	numRead := objectio.BlkReadStats.CounterSet.FileService.Cache.Read.Load()
	numHit := objectio.BlkReadStats.CounterSet.FileService.Cache.Hit.Load()

	curNumRead := numRead - lastNumRead
	curNumHit := numHit - lastNumHit

	if curNumRead > curNumHit {
		objectio.BlkReadStats.BlkCacheHitStats.Record(0, 1)
	} else {
		objectio.BlkReadStats.BlkCacheHitStats.Record(1, 1)
	}

	objectio.BlkReadStats.EntryCacheHitStats.Record(int(curNumHit), int(curNumRead))
}

// -----------------------------------------------------------------
// ------------------------ mergeReader ----------------------------
// -----------------------------------------------------------------

type withFilterMixin struct {
	fs       fileservice.FileService
	ts       timestamp.Timestamp
	tableDef *plan.TableDef
	name     string

	// columns used for reading
	columns struct {
		seqnums    []uint16
		colTypes   []types.Type
		phyAddrPos int

		indexOfFirstSortedColumn int
	}

	filterState struct {
		//point select for primary key
		expr      *plan.Expr
		filter    objectio.BlockReadFilter
		memFilter MemPKFilter
		seqnums   []uint16 // seqnums of the columns in the filter
		pkSeqNum  int32
		colTypes  []types.Type
		hasBF     bool // whether bloom filter is available
	}

	orderByLimit *objectio.IndexReaderTopOp
	filterHint   engine.FilterHint
}

type reader struct {
	withFilterMixin

	source engine.DataSource

	readBlockCnt uint64 // count of blocks this reader has read
	threshHold   uint64 //if read block cnt > threshold, will skip memcache write for reader

	// cacheVectors is used for vector reuse
	cacheVectors containers.Vectors
}

type mergeReader struct {
	rds []engine.Reader
}

type EmptyReader struct {
}

func NewMergeReader(readers []engine.Reader) *mergeReader {
	return &mergeReader{
		rds: readers,
	}
}

func (r *mergeReader) SetFilterZM(zm objectio.ZoneMap) {
	for i := range r.rds {
		r.rds[i].SetFilterZM(zm)
	}
}

func (r *mergeReader) GetOrderBy() []*plan.OrderBySpec {
	for i := range r.rds {
		if r.rds[i].GetOrderBy() != nil {
			return r.rds[i].GetOrderBy()
		}
	}
	return nil
}

func (r *mergeReader) SetOrderBy(orderby []*plan.OrderBySpec) {
	for i := range r.rds {
		r.rds[i].SetOrderBy(orderby)
	}
}

func (r *mergeReader) SetIndexParam(param *plan.IndexReaderParam) {
	for i := range r.rds {
		r.rds[i].SetIndexParam(param)
	}
}

func (r *mergeReader) Close() error {
	return nil
}

func (r *mergeReader) Read(
	ctx context.Context,
	cols []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) (bool, error) {
	start := time.Now()
	defer func() {
		v2.TxnMergeReaderDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if len(r.rds) == 0 {
		return true, nil
	}
	for len(r.rds) > 0 {
		isEnd, err := r.rds[0].Read(ctx, cols, expr, mp, outBatch)
		if err != nil {
			for _, rd := range r.rds {
				rd.Close()
			}
			return false, err
		}
		if isEnd {
			r.rds = r.rds[1:]
		} else {
			if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
				logutil.Debug("merge reader catch batch")
			}
			return false, nil
		}
	}
	return true, nil
}

// -----------------------------------------------------------------
func NewReader(
	ctx context.Context,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	fs fileservice.FileService,
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	expr *plan.Expr,
	//orderedScan bool, // it should be included in filter or expr.
	source engine.DataSource,
	threshHold uint64,
	filterHint engine.FilterHint,
) (*reader, error) {

	baseFilter, err := ConstructBasePKFilter(
		expr,
		tableDef,
		mp,
	)
	if err != nil {
		return nil, err
	}

	memFilter, err := NewMemPKFilter(
		tableDef,
		ts,
		packerPool,
		baseFilter,
		filterHint,
	)
	if err != nil {
		return nil, err
	}

	blockFilter, err := ConstructBlockPKFilter(
		catalog.IsFakePkName(tableDef.Pkey.PkeyColName),
		baseFilter,
		filterHint.BF,
	)
	if err != nil {
		return nil, err
	}

	r := &reader{
		withFilterMixin: withFilterMixin{
			fs:         fs,
			ts:         ts,
			tableDef:   tableDef,
			name:       tableDef.Name,
			filterHint: filterHint,
		},
		source: source,
	}
	r.columns.phyAddrPos = -1
	r.filterState.expr = expr
	r.filterState.filter = blockFilter
	r.filterState.memFilter = memFilter
	r.filterState.hasBF = filterHint.BF != nil && filterHint.BF.Valid()
	r.threshHold = threshHold
	return r, nil
}

func (r *reader) Close() error {
	r.source.Close()
	r.withFilterMixin.reset()
	if r.cacheVectors.Allocated() > 0 {
		logutil.Fatal("cache vector is not empty")
	}
	r.cacheVectors = nil
	return nil
}

func (r *reader) SetOrderBy(orderby []*plan.OrderBySpec) {
	r.source.SetOrderBy(orderby)
}

func (r *reader) SetIndexParam(param *plan.IndexReaderParam) {
	if param == nil {
		return
	}

	orderFunc := param.OrderBy[0].Expr.GetF()
	if orderFunc == nil {
		panic("order function is nil")
	}

	col := orderFunc.Args[0].GetCol()
	if col == nil {
		panic("column is nil")
	}

	numVec := orderFunc.Args[1].GetLit().GetVecVal()
	if len(numVec) == 0 {
		return
	}

	metricType, ok := metric.DistFuncNameToMetricType[orderFunc.Func.ObjName]
	if !ok {
		panic("unsupported order function")
	}

	if r.orderByLimit == nil {
		r.orderByLimit = &objectio.IndexReaderTopOp{}
	}

	r.orderByLimit.Typ = types.T(orderFunc.Args[0].Typ.Id)
	r.orderByLimit.MetricType = metricType
	r.orderByLimit.ColPos = col.ColPos
	r.orderByLimit.NumVec = []byte(numVec)
	r.orderByLimit.Limit = param.Limit.GetLit().GetU64Val()

	if param.DistRange != nil {
		r.orderByLimit.LowerBoundType = param.DistRange.LowerBoundType
		r.orderByLimit.LowerBound = param.DistRange.LowerBound.GetLit().GetDval()
		r.orderByLimit.UpperBoundType = param.DistRange.UpperBoundType
		r.orderByLimit.UpperBound = param.DistRange.UpperBound.GetLit().GetDval()

		if param.OrigFuncName == metric.DistFn_L2Distance {
			r.orderByLimit.LowerBound *= r.orderByLimit.LowerBound
			r.orderByLimit.UpperBound *= r.orderByLimit.UpperBound
		}
	}

	r.orderByLimit.DistHeap = make(objectio.Float64Heap, 0, r.orderByLimit.Limit)
}

func (r *reader) GetOrderBy() []*plan.OrderBySpec {
	return r.source.GetOrderBy()
}

func (r *reader) SetFilterZM(zm objectio.ZoneMap) {
	r.source.SetFilterZM(zm)
}

func (r *reader) Read(
	ctx context.Context,
	cols []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	outBatch *batch.Batch,
) (isEnd bool, err error) {
	outBatch.CleanOnlyData()

	var dataState engine.DataState
	var blkInfo *objectio.BlockInfo

	start := time.Now()
	defer func() {
		v2.TxnBlockReaderDurationHistogram.Observe(time.Since(start).Seconds())
		if err != nil || dataState == engine.End {
			r.Close()
		}
		if injected, logLevel := objectio.LogReaderInjected("", r.name); injected || err != nil {
			if err != nil {
				logutil.Error(
					"LOGREADER-ERROR",
					zap.String("name", r.name),
					zap.Error(err),
				)
				return
			}
			if isEnd {
				return
			}
			blkStr := "nil"
			if blkInfo != nil {
				blkStr = blkInfo.String()
			}
			if logLevel == 0 {
				logutil.Info(
					"DEBUG-SLOW-TXN-READER",
					zap.String("name", r.name),
					zap.String("ts", r.ts.DebugString()),
					zap.Int("data-len", outBatch.RowCount()),
					zap.Duration("duration", time.Since(start)),
					zap.String("blk", blkStr),
					zap.Error(err),
				)
			} else {
				maxLogCnt := 10
				if logLevel > 1 {
					maxLogCnt = outBatch.RowCount()
				}
				logutil.Info(
					"LOGREADER-INJECTED-1",
					zap.String("name", r.name),
					zap.String("ts", r.ts.DebugString()),
					zap.Duration("duration", time.Since(start)),
					zap.Error(err),
					zap.String("data", common.MoBatchToString(outBatch, maxLogCnt)),
					zap.String("blk", blkStr),
				)
			}
		}

		if v := ctx.Value(defines.ReaderSummaryKey{}); v != nil {
			buf := v.(*bytes.Buffer)
			switch dataState {
			case engine.InMem:
				buf.WriteString(fmt.Sprintf("[InMem] Source %v || Data %v",
					r.source.String(),
					common.MoBatchToString(outBatch, 5),
				),
				)
			case engine.Persisted:
				if outBatch.RowCount() > 0 {
					buf.WriteString(fmt.Sprintf("[Blk] Info %v || Data %v",
						blkInfo.String(), common.MoBatchToString(outBatch, 5)),
					)
				}
			}
		}

	}()

	r.tryUpdateColumns(cols)

	// source.Next() expects outBatch.Vecs to be aligned with cols/seqnums.
	// For vector TopN pushdown we may have an extra distVec appended in the previous
	// Read(), so detach it before source.Next() to avoid seqNums out-of-range panic in
	// InMem paths. We keep one float64 distVec for reuse to avoid repeated allocations.
	var detachedDistVec *vector.Vector
	if r.orderByLimit != nil && len(outBatch.Vecs) > len(cols) {
		if candidate := outBatch.Vecs[len(cols)]; candidate != nil &&
			candidate.GetType().Oid == types.T_float64 {
			candidate.CleanOnlyData()
			detachedDistVec = candidate
		}
		for i := len(cols); i < len(outBatch.Vecs); i++ {
			vec := outBatch.Vecs[i]
			if vec != nil && vec != detachedDistVec {
				vec.Free(mp)
			}
			// Clear references in the backing array so detached vectors can be reused/freed
			// explicitly instead of being retained implicitly by slice capacity.
			outBatch.Vecs[i] = nil
		}
		outBatch.Vecs = outBatch.Vecs[:len(cols)]
	}
	// If Read() exits early (error/end) before re-attaching the detached distVec, release it.
	defer func() {
		if detachedDistVec != nil {
			detachedDistVec.Free(mp)
		}
	}()

	blkInfo, state, err := r.source.Next(
		ctx,
		cols,
		r.columns.colTypes,
		r.columns.seqnums,
		r.filterState.pkSeqNum,
		&r.filterState.memFilter,
		mp,
		outBatch)

	dataState = state

	if err != nil {
		return false, err
	}
	if state == engine.End {
		return true, nil
	}
	if state == engine.InMem {
		if r.orderByLimit != nil {
			sels, dists, err := blockio.HandleOrderByLimitOnIVFFlatIndex(ctx, nil, outBatch.Vecs[r.orderByLimit.ColPos], r.orderByLimit)
			if err != nil {
				return false, err
			}

			// Keep batch cardinality consistent with pushed-down vector TopN result.
			// When sels is empty, batch.Shuffle is a no-op, so we must clear outBatch
			// explicitly; otherwise rowCount can stay > 0 while distVec is empty.
			if len(sels) == 0 {
				outBatch.CleanOnlyData()
			} else if err := outBatch.Shuffle(sels, mp); err != nil {
				return false, err
			}

			// Reuse the detached distVec when possible to avoid per-batch allocation.
			distVec := detachedDistVec
			if distVec == nil {
				distVec = vector.NewVec(types.T_float64.ToType())
			}
			detachedDistVec = nil
			if err := vector.AppendFixedList(distVec, dists, nil, mp); err != nil {
				return false, err
			}
			outBatch.Vecs = append(outBatch.Vecs, distVec)
		}

		return false, nil
	}
	//read block
	filter := r.withFilterMixin.filterState.filter

	statsCtx, numRead, numHit := ctx, int64(0), int64(0)
	if filter.Valid {
		// try to store the blkReadStats CounterSet into ctx, so that
		// it can record the mem cache hit stats when call MemCache.Read() later soon.
		statsCtx, numRead, numHit = prepareGatherStats(ctx)
	}

	var policy fileservice.Policy

	if r.readBlockCnt > r.threshHold {
		policy = fileservice.SkipMemoryCacheWrites
	}
	r.readBlockCnt++

	if len(r.cacheVectors) == 0 {
		r.cacheVectors = containers.NewVectors(len(r.columns.seqnums) + 1)
	}
	if r.orderByLimit != nil && detachedDistVec != nil {
		// Re-attach the detached distVec so BlockDataRead can take its fast reuse branch.
		outBatch.Vecs = append(outBatch.Vecs, detachedDistVec)
		detachedDistVec = nil
	}

	err = blockio.BlockDataRead(
		statsCtx,
		blkInfo,
		r.source,
		r.columns.seqnums,
		r.columns.colTypes,
		r.columns.phyAddrPos,
		r.ts,
		r.filterState.seqnums,
		r.filterState.colTypes,
		filter,
		r.orderByLimit,
		policy,
		r.name,
		outBatch,
		r.cacheVectors,
		mp,
		r.fs,
	)
	if err != nil {
		return false, err
	}

	if outBatch.RowCount() == 1 {
		// found one row in this blk for the pk equal, record it
		r.withFilterMixin.filterState.memFilter.RecordExactHit()
	}

	if filter.Valid {
		// we collect mem cache hit related statistics info for blk read here
		gatherStats(numRead, numHit)
	}

	outBatch.SetAttributes(cols)

	if blkInfo.IsSorted() && r.columns.indexOfFirstSortedColumn != -1 {
		outBatch.GetVector(int32(r.columns.indexOfFirstSortedColumn)).SetSorted(true)
	}

	return false, nil
}

func GetThresholdForReader(readerNum int) uint64 {
	if readerNum <= 8 {
		return uint64(1024 / readerNum)
	}
	return 128
}
