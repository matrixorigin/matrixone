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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
	mixin.sels = nil
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
	blockio.RecordColumnSelectivity(len(cols), len(mixin.tableDef.Cols))

	mixin.columns.seqnums = make([]uint16, len(cols))
	mixin.columns.colTypes = make([]types.Type, len(cols))
	// mixin.columns.colNulls = make([]bool, len(cols))
	mixin.columns.pkPos = -1
	mixin.columns.rowidPos = -1
	mixin.columns.indexOfFirstSortedColumn = -1
	compPKName2Pos := make(map[string]struct{})
	positions := make(map[string]int)
	if mixin.tableDef.Pkey != nil && mixin.tableDef.Pkey.CompPkeyCol != nil {
		pk := mixin.tableDef.Pkey
		for _, name := range pk.Names {
			compPKName2Pos[name] = struct{}{}
		}
	}
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

			if _, ok := compPKName2Pos[column]; ok {
				positions[column] = i
			}

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
	if len(positions) != 0 {
		for _, name := range mixin.tableDef.Pkey.Names {
			if pos, ok := positions[name]; !ok {
				break
			} else {
				mixin.columns.compPKPositions = append(mixin.columns.compPKPositions, uint16(pos))
			}
		}
	}
}

func (mixin *withFilterMixin) getReadFilter(proc *process.Process) (
	filter blockio.ReadFilter,
) {
	if mixin.filterState.evaluated {
		filter = mixin.filterState.filter
		return
	}
	pk := mixin.tableDef.Pkey
	if pk == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		return
	}
	if pk.CompPkeyCol == nil {
		return mixin.getNonCompositPKFilter(proc)
	}
	return mixin.getCompositPKFilter(proc)
}

func (mixin *withFilterMixin) getCompositPKFilter(proc *process.Process) (
	filter blockio.ReadFilter,
) {
	// if no primary key is included in the columns or no filter expr is given,
	// no filter is needed
	if len(mixin.columns.compPKPositions) == 0 || mixin.filterState.expr == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		return
	}

	// evaluate
	pkNames := mixin.tableDef.Pkey.Names
	pkVals := make([]*plan.Const, len(pkNames))
	ok, hasNull := getCompositPKVals(mixin.filterState.expr, pkNames, pkVals, proc)

	if !ok || pkVals[0] == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		mixin.filterState.hasNull = hasNull
		return
	}
	cnt := getValidCompositePKCnt(pkVals)
	pkVals = pkVals[:cnt]

	filterFuncs := make([]func(*vector.Vector, []int32, *[]int32), len(pkVals))
	for i := range filterFuncs {
		filterFuncs[i] = getCompositeFilterFuncByExpr(pkVals[i], i == 0)
	}

	filter = func(vecs []*vector.Vector) []int32 {
		var (
			inputSels []int32
		)
		for i := range filterFuncs {
			vec := vecs[i]
			mixin.sels = mixin.sels[:0]
			filterFuncs[i](vec, inputSels, &mixin.sels)
			if len(mixin.sels) == 0 {
				break
			}
			inputSels = mixin.sels
		}
		// logutil.Debugf("%s: %d/%d", mixin.tableDef.Name, len(res), vecs[0].Length())

		return mixin.sels
	}

	mixin.filterState.evaluated = true
	mixin.filterState.filter = filter
	mixin.filterState.seqnums = make([]uint16, 0, len(mixin.columns.compPKPositions))
	mixin.filterState.colTypes = make([]types.Type, 0, len(mixin.columns.compPKPositions))
	for _, pos := range mixin.columns.compPKPositions {
		mixin.filterState.seqnums = append(mixin.filterState.seqnums, mixin.columns.seqnums[pos])
		mixin.filterState.colTypes = append(mixin.filterState.colTypes, mixin.columns.colTypes[pos])
	}
	return
}

func (mixin *withFilterMixin) getNonCompositPKFilter(proc *process.Process) blockio.ReadFilter {
	// if no primary key is included in the columns or no filter expr is given,
	// no filter is needed
	if mixin.columns.pkPos == -1 || mixin.filterState.expr == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		return nil
	}

	// evaluate the search function for the filter
	// if the search function is not found, no filter is needed
	// primary key must be used by the expr in one of the following patterns:
	// A: $pk = const_value
	// B: const_value = $pk
	// C: {A|B} and {A|B}
	// D: {A|B|C} [and {A|B|C}]*
	// for other patterns, no filter is needed
	ok, hasNull, searchFunc := getNonCompositePKSearchFuncByExpr(
		mixin.filterState.expr,
		mixin.tableDef.Pkey.PkeyColName,
		mixin.columns.colTypes[mixin.columns.pkPos].Oid,
		proc,
	)
	if !ok || searchFunc == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		mixin.filterState.hasNull = hasNull
		return nil
	}

	// here we will select the primary key column from the vectors, and
	// use the search function to find the offset of the primary key.
	// it returns the offset of the primary key in the pk vector.
	// if the primary key is not found, it returns empty slice
	mixin.filterState.evaluated = true
	mixin.filterState.filter = searchFunc
	mixin.filterState.seqnums = []uint16{uint16(mixin.columns.seqnums[mixin.columns.pkPos])}
	mixin.filterState.colTypes = mixin.columns.colTypes[mixin.columns.pkPos : mixin.columns.pkPos+1]
	return searchFunc
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
	proc *process.Process,
) *blockReader {
	r := &blockReader{
		withFilterMixin: withFilterMixin{
			ctx:      ctx,
			fs:       fs,
			ts:       ts,
			proc:     proc,
			tableDef: tableDef,
		},
		blks: blks,
	}
	r.filterState.expr = filterExpr
	return r
}

func (r *blockReader) Close() error {
	r.withFilterMixin.reset()
	r.blks = nil
	r.buffer = nil
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
		r.buffer = r.buffer[:0]
		r.currentStep++
	}()

	// get the current block to be read
	blockInfo := r.blks[0]

	// try to update the columns
	// the columns is only updated once for all blocks
	r.tryUpdateColumns(cols)

	// get the block read filter
	filter := r.getReadFilter(r.proc)

	// if any null expr is found in the primary key (composite primary keys), quick return
	if r.filterState.hasNull {
		return nil, nil
	}

	//prefetch some objects
	for len(r.steps) > 0 && r.steps[0] == r.currentStep {
		if filter != nil && blockInfo.Sorted {
			blockio.BlockPrefetch(r.filterState.seqnums, r.fs, [][]*catalog.BlockInfo{r.infos[0]})
		} else {
			blockio.BlockPrefetch(r.columns.seqnums, r.fs, [][]*catalog.BlockInfo{r.infos[0]})
		}
		r.infos = r.infos[1:]
		r.steps = r.steps[1:]
	}

	// read the block
	bat, err := blockio.BlockRead(
		r.ctx, blockInfo, r.buffer, r.columns.seqnums, r.columns.colTypes, r.ts,
		r.filterState.seqnums,
		r.filterState.colTypes,
		filter,
		r.fs, mp, vp,
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
	dirtyBlks []*catalog.BlockInfo,
	filterExpr *plan.Expr,
	fs fileservice.FileService,
	proc *process.Process,
) *blockMergeReader {
	r := &blockMergeReader{
		table: txnTable,
		blockReader: newBlockReader(
			ctx,
			txnTable.getTableDef(),
			ts,
			dirtyBlks,
			filterExpr,
			fs,
			proc,
		),
	}
	return r
}

func (r *blockMergeReader) Close() error {
	r.blockReader.Close()
	r.table = nil
	return nil
}

func (r *blockMergeReader) prefetchDeletes() error {
	//load delta locations for r.blocks.
	if !r.loaded {
		for _, info := range r.blks {
			bats, ok := r.table.db.txn.blockId_dn_delete_metaLoc_batch[info.BlockID]
			if !ok {
				return nil
			}
			for _, bat := range bats {
				vs := vector.MustStrCol(bat.GetVector(0))
				for _, deltaLoc := range vs {
					location, err := blockio.EncodeLocationFromString(deltaLoc)
					if err != nil {
						return err
					}
					r.deletaLocs[location.Name().String()] =
						append(r.deletaLocs[location.Name().String()], location)
				}
			}

		}
		// Get Single Col pk index
		for idx, colDef := range r.tableDef.Cols {
			if colDef.Name == r.tableDef.Pkey.PkeyColName {
				r.pkidx = idx
				break
			}
		}
		r.loaded = true
	}

	//prefetch the deletes
	for name, locs := range r.deletaLocs {
		pref, err := blockio.BuildPrefetchParams(r.fs, locs[0])
		if err != nil {
			return err
		}
		for _, loc := range locs {
			//rowid + pk
			pref.AddBlock([]uint16{0, uint16(r.pkidx)}, []uint16{loc.ID()})

		}
		delete(r.deletaLocs, name)
		return blockio.PrefetchWithMerged(pref)
	}
	return nil
}

func (r *blockMergeReader) loadDeletes(ctx context.Context) error {
	if len(r.blks) == 0 {
		return nil
	}
	info := r.blks[0]
	// load deletes from txn.blockId_dn_delete_metaLoc_batch
	err := r.table.LoadDeletesForBlock(info.BlockID, &r.buffer)
	if err != nil {
		return err
	}
	// load deletes from partition state for the specified block
	{
		state, err := r.table.getPartitionState(ctx)
		if err != nil {
			return err
		}
		ts := types.TimestampToTS(r.ts)
		iter := state.NewRowsIter(ts, &info.BlockID, true)
		for iter.Next() {
			entry := iter.Entry()
			_, offset := entry.RowID.Decode()
			r.buffer = append(r.buffer, int64(offset))
		}
		iter.Close()
	}

	//TODO:: if r.table.writes is a map , the time complexity could be O(1)
	//load deletes from txn.writes for the specified block
	for _, entry := range r.table.writes {
		if entry.isGeneratedByTruncate() {
			continue
		}
		if entry.typ == DELETE && entry.fileName == "" {
			vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
			for _, v := range vs {
				id, offset := v.Decode()
				if id == info.BlockID {
					r.buffer = append(r.buffer, int64(offset))
				}
			}
		}
	}
	//load deletes from txn.deletedBlocks.
	txn := r.table.db.txn
	txn.deletedBlocks.getDeletedOffsetsByBlock(&info.BlockID, &r.buffer)
	return nil
}

func (r *blockMergeReader) Read(
	ctx context.Context,
	cols []string,
	expr *plan.Expr,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (*batch.Batch, error) {
	//prefetch deletes for r.blks
	if err := r.prefetchDeletes(); err != nil {
		return nil, err
	}
	//load deletes for the specified block
	if err := r.loadDeletes(ctx); err != nil {
		return nil, err
	}
	return r.blockReader.Read(ctx, cols, expr, mp, vp)
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
