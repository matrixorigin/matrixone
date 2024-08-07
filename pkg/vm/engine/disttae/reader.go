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

package disttae

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// -----------------------------------------------------------------
// ------------------------ withFilterMixin ------------------------
// -----------------------------------------------------------------

func (mixin *withFilterMixin) reset() {
	mixin.filterState.evaluated = false
	mixin.filterState.filter = blockio.BlockReadFilter{}
	mixin.columns.pkPos = -1
	mixin.columns.indexOfFirstSortedColumn = -1
	mixin.columns.seqnums = nil
	mixin.columns.colTypes = nil
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
	v2.TaskSelColumnTotal.Add(float64(ctotal))
	v2.TaskSelColumnHit.Add(float64(ctotal - chit))
	blockio.RecordColumnSelectivity(mixin.proc.GetService(), chit, ctotal)

	mixin.columns.seqnums = make([]uint16, len(cols))
	mixin.columns.colTypes = make([]types.Type, len(cols))
	// mixin.columns.colNulls = make([]bool, len(cols))
	mixin.columns.pkPos = -1
	mixin.columns.indexOfFirstSortedColumn = -1

	for i, column := range cols {
		column = strings.ToLower(column)
		if column == catalog.Row_ID {
			mixin.columns.seqnums[i] = objectio.SEQNUM_ROWID
			mixin.columns.colTypes[i] = objectio.RowidType
		} else {
			if plan2.GetSortOrderByName(mixin.tableDef, column) == 0 {
				mixin.columns.indexOfFirstSortedColumn = i
			}
			colIdx := mixin.tableDef.Name2ColIndex[column]
			colDef := mixin.tableDef.Cols[colIdx]
			mixin.columns.seqnums[i] = uint16(colDef.Seqnum)

			if mixin.tableDef.Pkey != nil && mixin.tableDef.Pkey.PkeyColName == column {
				// primary key is in the cols
				mixin.columns.pkPos = i
			}
			mixin.columns.colTypes[i] = types.T(colDef.Typ.Id).ToType()
			mixin.columns.colTypes[i].Scale = colDef.Typ.Scale
			mixin.columns.colTypes[i].Width = colDef.Typ.Width
		}
	}

	if mixin.columns.pkPos != -1 {
		// here we will select the primary key column from the vectors, and
		// use the search function to find the offset of the primary key.
		// it returns the offset of the primary key in the pk vector.
		// if the primary key is not found, it returns empty slice
		mixin.filterState.evaluated = true
		//mixin.filterState.filter = filter
		mixin.filterState.seqnums = []uint16{mixin.columns.seqnums[mixin.columns.pkPos]}
		mixin.filterState.colTypes = mixin.columns.colTypes[mixin.columns.pkPos : mixin.columns.pkPos+1]

		// records how many blks one reader needs to read when having filter
		//objectio.BlkReadStats.BlksByReaderStats.Record(1, blkCnt)
	}
}

// -----------------------------------------------------------------
// ------------------------ emptyReader ----------------------------
// -----------------------------------------------------------------

func (r *emptyReader) SetFilterZM(objectio.ZoneMap) {
}

func (r *emptyReader) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (r *emptyReader) SetOrderBy([]*plan.OrderBySpec) {
}

func (r *emptyReader) Close() error {
	return nil
}

func (r *emptyReader) Read(
	_ context.Context,
	_ []string,
	_ *plan.Expr,
	_ *mpool.MPool,
	_ engine.VectorPool,
) (*batch.Batch, error) {
	return nil, nil
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

func (r *mergeReader) Close() error {
	return nil
}

func (r *mergeReader) Read(
	ctx context.Context,
	cols []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (*batch.Batch, error) {
	start := time.Now()
	defer func() {
		v2.TxnMergeReaderDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if len(r.rds) == 0 {
		return nil, nil
	}
	for len(r.rds) > 0 {
		bat, err := r.rds[0].Read(ctx, cols, expr, mp, vp)
		if err != nil {
			for _, rd := range r.rds {
				rd.Close()
			}
			return nil, err
		}
		if bat == nil {
			r.rds = r.rds[1:]
		}
		if bat != nil {
			if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
				logutil.Debug(testutil.OperatorCatchBatch("merge reader", bat))
			}
			return bat, nil
		}
	}
	return nil, nil
}

// -----------------------------------------------------------------

func NewReader(
	ctx context.Context,
	proc *process.Process, //it comes from transaction if reader run in local,otherwise it comes from remote compile.
	e *Engine,
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	expr *plan.Expr,
	//orderedScan bool, // it should be included in filter or expr.
	source engine.DataSource,
) *reader {

	baseFilter := newBasePKFilter(
		expr,
		tableDef,
		proc,
	)

	packerPool := e.packerPool
	memFilter := newMemPKFilter(
		tableDef,
		ts,
		packerPool,
		baseFilter,
	)

	blockFilter := newBlockReadPKFilter(
		tableDef.Pkey.PkeyColName,
		baseFilter,
	)

	r := &reader{
		withFilterMixin: withFilterMixin{
			ctx:      ctx,
			fs:       e.fs,
			ts:       ts,
			proc:     proc,
			tableDef: tableDef,
		},
		memFilter: memFilter,
		source:    source,
		ts:        ts,
	}
	r.filterState.expr = expr
	r.filterState.filter = blockFilter
	return r
}

func (r *reader) Close() error {
	r.source.Close()
	r.withFilterMixin.reset()
	return nil
}

func (r *reader) SetOrderBy(orderby []*plan.OrderBySpec) {
	//r.OrderBy = orderby
	r.source.SetOrderBy(orderby)
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
	vp engine.VectorPool,
) (bat *batch.Batch, err error) {

	var dataState engine.DataState

	start := time.Now()
	defer func() {
		v2.TxnBlockReaderDurationHistogram.Observe(time.Since(start).Seconds())
		if err != nil || dataState == engine.End {
			r.Close()
		}
	}()

	r.tryUpdateColumns(cols)

	bat, blkInfo, state, err := r.source.Next(
		ctx,
		cols,
		r.columns.colTypes,
		r.columns.seqnums,
		r.memFilter,
		mp,
		vp,
	)

	dataState = state

	if err != nil {
		return
	}
	if state == engine.End {
		return nil, nil
	}
	if state == engine.InMem {
		return
	}
	//read block
	filter := r.withFilterMixin.filterState.filter

	statsCtx, numRead, numHit := r.ctx, int64(0), int64(0)
	if filter.Valid {
		// try to store the blkReadStats CounterSet into ctx, so that
		// it can record the mem cache hit stats when call MemCache.Read() later soon.
		statsCtx, numRead, numHit = prepareGatherStats(r.ctx)
	}

	var policy fileservice.Policy
	if r.scanType == LARGE || r.scanType == NORMAL {
		policy = fileservice.SkipMemoryCacheWrites
	}

	bat, err = blockio.BlockDataRead(
		statsCtx,
		r.withFilterMixin.proc.GetService(),
		blkInfo,
		r.source,
		r.columns.seqnums,
		r.columns.colTypes,
		r.ts,
		r.filterState.seqnums,
		r.filterState.colTypes,
		filter,
		r.fs, mp, vp, policy,
		r.tableDef.Name,
	)
	if err != nil {
		return nil, err
	}

	if filter.Valid {
		// we collect mem cache hit related statistics info for blk read here
		gatherStats(numRead, numHit)
	}

	bat.SetAttributes(cols)

	if blkInfo.Sorted && r.columns.indexOfFirstSortedColumn != -1 {
		bat.GetVector(int32(r.columns.indexOfFirstSortedColumn)).SetSorted(true)
	}

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debug(testutil.OperatorCatchBatch("block reader", bat))
	}

	return
}
