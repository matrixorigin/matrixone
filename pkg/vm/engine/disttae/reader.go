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

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/testutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

// -----------------------------------------------------------------
// ------------------------ withFilterMixin ------------------------
// -----------------------------------------------------------------

func (mixin *withFilterMixin) reset() {
	mixin.filterState.evaluated = false
	mixin.filterState.filter = nil
	mixin.columns.pkPos = -1
	mixin.columns.rowidPos = -1
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
	mixin.columns.seqnums = make([]uint16, len(cols))
	mixin.columns.colTypes = make([]types.Type, len(cols))
	// mixin.columns.colNulls = make([]bool, len(cols))
	mixin.columns.pkPos = -1
	mixin.columns.rowidPos = -1
	mixin.columns.indexOfFirstSortedColumn = -1
	for i, column := range cols {
		if column == catalog.Row_ID {
			mixin.columns.rowidPos = i
			mixin.columns.seqnums[i] = objectio.SEQNUM_ROWID
			mixin.columns.colTypes[i] = objectio.RowidType
		} else {
			if plan2.GetSortOrder(mixin.tableDef, column) == 0 {
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
			// if colDef.Default != nil {
			// 	mixin.columns.colNulls[i] = colDef.Default.NullAbility
			// }
		}
	}
}

func (mixin *withFilterMixin) getReadFilter() (filter blockio.ReadFilter) {
	if mixin.filterState.evaluated {
		filter = mixin.filterState.filter
		return
	}

	// if no primary key is included in the columns or no filter expr is given,
	// no filter is needed
	if mixin.columns.pkPos == -1 || mixin.filterState.expr == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		return
	}

	// evaluate the search function for the filter
	// if the search function is not found, no filter is needed
	// primary key must be used by the expr in one of the following patterns:
	// A: $pk = const_value
	// B: const_value = $pk
	// C: {A|B} and {A|B}
	// D: {A|B|C} [and {A|B|C}]*
	// for other patterns, no filter is needed
	ok, searchFunc := getBinarySearchFuncByExpr(
		mixin.filterState.expr,
		mixin.tableDef.Pkey.PkeyColName,
		mixin.columns.colTypes[mixin.columns.pkPos].Oid,
	)
	if !ok || searchFunc == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		return
	}

	// here we will select the primary key column from the vectors, and
	// use the search function to find the offset of the primary key.
	// it returns the offset of the primary key in the pk vector.
	// if the primary key is not found, it returns empty slice
	filter = func(vecs []*vector.Vector) []int32 {
		vec := vecs[mixin.columns.pkPos]
		row := searchFunc(vec)
		if row < 0 {
			return nil
		}
		return []int32{int32(row)}
	}
	mixin.filterState.evaluated = true
	mixin.filterState.filter = filter
	return
}

// -----------------------------------------------------------------
// ------------------------ emptyReader ----------------------------
// -----------------------------------------------------------------

func (r *emptyReader) Close() error {
	return nil
}

func (r *emptyReader) Read(_ context.Context, _ []string,
	_ *plan.Expr, _ *mpool.MPool, _ engine.VectorPool) (*batch.Batch, error) {
	return nil, nil
}

// -----------------------------------------------------------------
// ------------------------ blockReader ----------------------------
// -----------------------------------------------------------------

func newBlockReader(
	ctx context.Context,
	tableDef *plan.TableDef,
	ts timestamp.Timestamp,
	blks []*catalog.BlockInfo,
	filterExpr *plan.Expr,
	fs fileservice.FileService,
) *blockReader {
	r := &blockReader{
		withFilterMixin: withFilterMixin{
			ctx:      ctx,
			fs:       fs,
			ts:       ts,
			tableDef: tableDef,
		},
		blks: blks,
	}
	r.filterState.expr = filterExpr
	return r
}

func (r *blockReader) Close() error {
	r.withFilterMixin.reset()
	return nil
}

func (r *blockReader) Read(
	ctx context.Context,
	cols []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (*batch.Batch, error) {
	// if the block list is empty, return nil
	if len(r.blks) == 0 {
		return nil, nil
	}

	// move to the next block at the end of this call
	defer func() {
		r.blks = r.blks[1:]
		r.currentStep++
	}()

	// get the current block to be read
	blockInfo := r.blks[0]

	// try to update the columns
	// the columns is only updated once for all blocks
	r.tryUpdateColumns(cols)

	//prefetch some objects
	for len(r.steps) > 0 && r.steps[0] == r.currentStep {
		blockio.BlockPrefetch(r.columns.seqnums, r.fs, [][]*catalog.BlockInfo{r.infos[0]})
		r.infos = r.infos[1:]
		r.steps = r.steps[1:]
	}

	// get the block read filter
	filter := r.getReadFilter()

	// read the block
	bat, err := blockio.BlockRead(
		r.ctx, blockInfo, nil, r.columns.seqnums, r.columns.colTypes, r.ts, filter, r.fs, mp, vp,
	)
	if err != nil {
		return nil, err
	}
	bat.SetAttributes(cols)

	if blockInfo.Sorted && r.columns.indexOfFirstSortedColumn != -1 {
		bat.GetVector(int32(r.columns.indexOfFirstSortedColumn)).SetSorted(true)
	}

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debug(testutil.OperatorCatchBatch("block reader", bat))
	}
	return bat, nil
}

// -----------------------------------------------------------------
// ---------------------- blockMergeReader -------------------------
// -----------------------------------------------------------------

func newBlockMergeReader(
	ctx context.Context,
	txnTable *txnTable,
	ts timestamp.Timestamp,
	blks []ModifyBlockMeta,
	filterExpr *plan.Expr,
	fs fileservice.FileService,
) *blockMergeReader {
	r := &blockMergeReader{
		withFilterMixin: withFilterMixin{
			ctx:      ctx,
			tableDef: txnTable.tableDef,
			ts:       ts,
			fs:       fs,
		},
		blks:  blks,
		table: txnTable,
	}
	r.filterState.expr = filterExpr
	return r
}

func (r *blockMergeReader) Close() error {
	r.withFilterMixin.reset()
	r.table = nil
	r.blks = nil
	return nil
}

func (r *blockMergeReader) Read(
	ctx context.Context,
	cols []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (*batch.Batch, error) {
	// if the block list is empty, return nil
	if len(r.blks) == 0 {
		return nil, nil
	}

	// move to the next block at the end of this call
	defer func() {
		r.blks[0].deletes = nil
		r.blks = r.blks[1:]
	}()

	// get the current block to be read
	info := &r.blks[0].meta

	// try to update the columns
	r.tryUpdateColumns(cols)

	// load deletes from txn.blockId_dn_delete_metaLoc_batch
	if _, ok := r.table.db.txn.blockId_dn_delete_metaLoc_batch[info.BlockID]; ok {
		deletes, err := r.table.LoadDeletesForBlock(info.BlockID)
		if err != nil {
			return nil, err
		}
		//TODO:: need to optimize .
		r.blks[0].deletes = append(r.blks[0].deletes, deletes...)
	}

	// load deletes from partition state for the specified block
	{
		state, err := r.table.getPartitionState(ctx)
		if err != nil {
			return nil, err
		}
		ts := types.TimestampToTS(r.ts)
		iter := state.NewRowsIter(ts, &info.BlockID, true)
		for iter.Next() {
			entry := iter.Entry()
			_, offset := entry.RowID.Decode()
			r.blks[0].deletes = append(r.blks[0].deletes, int64(offset))
		}
		iter.Close()
	}

	filter := r.getReadFilter()

	bat, err := blockio.BlockRead(
		r.ctx, info, r.blks[0].deletes, r.columns.seqnums, r.columns.colTypes, r.ts, filter, r.fs, mp, vp,
	)
	if err != nil {
		return nil, err
	}
	bat.SetAttributes(cols)

	if logutil.GetSkip1Logger().Core().Enabled(zap.DebugLevel) {
		logutil.Debug(testutil.OperatorCatchBatch("block merge reader", bat))
	}
	return bat, nil
}

// -----------------------------------------------------------------
// ------------------------ mergeReader ----------------------------
// -----------------------------------------------------------------

func NewMergeReader(readers []engine.Reader) *mergeReader {
	return &mergeReader{
		rds: readers,
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
