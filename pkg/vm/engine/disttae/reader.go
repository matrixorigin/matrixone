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
	"sort"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

var checkPrimaryKeyOnly bool

func init() {
	checkPrimaryKeyOnly = true
}

// -----------------------------------------------------------------
// ------------------------ withFilterMixin ------------------------
// -----------------------------------------------------------------

func (mixin *withFilterMixin) reset() {
	mixin.filterState.evaluated = false
	mixin.filterState.filter = nil
	mixin.columns.pkPos = -1
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
	chit, ctotal := len(cols), len(mixin.tableDef.Cols)
	v2.TaskSelColumnTotal.Add(float64(ctotal))
	v2.TaskSelColumnHit.Add(float64(ctotal - chit))
	blockio.RecordColumnSelectivity(chit, ctotal)

	mixin.columns.seqnums = make([]uint16, len(cols))
	mixin.columns.colTypes = make([]types.Type, len(cols))
	// mixin.columns.colNulls = make([]bool, len(cols))
	mixin.columns.pkPos = -1
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
			mixin.columns.seqnums[i] = objectio.SEQNUM_ROWID
			mixin.columns.colTypes[i] = objectio.RowidType
		} else {
			if plan2.GetSortOrderByName(mixin.tableDef, column) == 0 {
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

func (mixin *withFilterMixin) getReadFilter(proc *process.Process, blkCnt int) (
	filter blockio.ReadFilter,
) {
	if mixin.filterState.evaluated {
		filter = mixin.filterState.filter
		return
	}
	pk := mixin.tableDef.Pkey
	if mixin.filterState.expr == nil || pk == nil {
		mixin.filterState.evaluated = true
		mixin.filterState.filter = nil
		return
	}
	if pk.CompPkeyCol == nil || checkPrimaryKeyOnly {
		return mixin.getNonCompositPKFilter(proc, blkCnt)
	}
	return mixin.getCompositPKFilter(proc, blkCnt)
}

func (mixin *withFilterMixin) getCompositPKFilter(proc *process.Process, blkCnt int) (
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
	pkVals := make([]*plan.Literal, len(pkNames))
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
	// records how many blks one reader needs to read when having filter
	objectio.BlkReadStats.BlksByReaderStats.Record(1, blkCnt)
	return
}

func (mixin *withFilterMixin) getNonCompositPKFilter(proc *process.Process, blkCnt int) blockio.ReadFilter {
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
	mixin.filterState.seqnums = []uint16{mixin.columns.seqnums[mixin.columns.pkPos]}
	mixin.filterState.colTypes = mixin.columns.colTypes[mixin.columns.pkPos : mixin.columns.pkPos+1]

	// records how many blks one reader needs to read when having filter
	objectio.BlkReadStats.BlksByReaderStats.Record(1, blkCnt)
	return searchFunc
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
	blks []*objectio.BlockInfo,
	filterExpr *plan.Expr,
	fs fileservice.FileService,
	proc *process.Process,
) *blockReader {
	for _, blk := range blks {
		trace.GetService().TxnReadBlock(
			proc.TxnOperator,
			tableDef.TblId,
			blk.BlockID[:])
	}
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

func (r *blockReader) SetFilterZM(zm objectio.ZoneMap) {
	if !r.filterZM.IsInited() {
		r.filterZM = zm.Clone()
		return
	}
	if r.desc && r.filterZM.CompareMax(zm) < 0 {
		r.filterZM = zm.Clone()
		return
	}
	if !r.desc && r.filterZM.CompareMin(zm) > 0 {
		r.filterZM = zm.Clone()
		return
	}
}

func (r *blockReader) GetOrderBy() []*plan.OrderBySpec {
	return r.OrderBy
}

func (r *blockReader) SetOrderBy(orderby []*plan.OrderBySpec) {
	r.OrderBy = orderby
}

func (r *blockReader) needReadBlkByZM(i int) bool {
	zm := r.blockZMS[i]
	if !r.filterZM.IsInited() || !zm.IsInited() {
		return true
	}
	if r.desc {
		return r.filterZM.CompareMax(zm) <= 0
	} else {
		return r.filterZM.CompareMin(zm) >= 0
	}
}

func (r *blockReader) getBlockZMs() {
	orderByCol, _ := r.OrderBy[0].Expr.Expr.(*plan.Expr_Col)
	orderByColIDX := int(r.tableDef.Cols[int(orderByCol.Col.ColPos)].Seqnum)

	r.blockZMS = make([]index.ZM, len(r.blks))
	var objDataMeta objectio.ObjectDataMeta
	var location objectio.Location
	for i := range r.blks {
		location = r.blks[i].MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			objMeta, err := objectio.FastLoadObjectMeta(r.ctx, &location, false, r.fs)
			if err != nil {
				panic("load object meta error when ordered scan!")
			}
			objDataMeta = objMeta.MustDataMeta()
		}
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		r.blockZMS[i] = blkMeta.ColumnMeta(uint16(orderByColIDX)).ZoneMap()
	}
}

func (r *blockReader) sortBlockList() {
	helper := make([]*blockSortHelper, len(r.blks))
	for i := range r.blks {
		helper[i] = &blockSortHelper{}
		helper[i].blk = r.blks[i]
		helper[i].zm = r.blockZMS[i]
	}
	if r.desc {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMax(zm2) > 0
		})
	} else {
		sort.Slice(helper, func(i, j int) bool {
			zm1 := helper[i].zm
			if !zm1.IsInited() {
				return true
			}
			zm2 := helper[j].zm
			if !zm2.IsInited() {
				return false
			}
			return zm1.CompareMin(zm2) < 0
		})
	}

	for i := range helper {
		r.blks[i] = helper[i].blk
		r.blockZMS[i] = helper[i].zm
	}
}

func (r *blockReader) deleteFirstNBlocks(n int) {
	r.blks = r.blks[n:]
	if len(r.OrderBy) > 0 {
		r.blockZMS = r.blockZMS[n:]
	}
}

func (r *blockReader) Read(
	ctx context.Context,
	cols []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (bat *batch.Batch, err error) {
	start := time.Now()
	defer func() {
		v2.TxnBlockReaderDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	// for ordered scan, sort blocklist by zonemap info, and then filter by zonemap
	if len(r.OrderBy) > 0 {
		if !r.sorted {
			r.desc = r.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0
			r.getBlockZMs()
			r.sortBlockList()
			r.sorted = true
		}
		i := 0
		for i < len(r.blks) {
			if r.needReadBlkByZM(i) {
				break
			}
			i++
		}
		r.deleteFirstNBlocks(i)
	}
	// if the block list is empty, return nil
	if len(r.blks) == 0 {
		return nil, nil
	}

	// move to the next block at the end of this call
	defer func() {
		r.deleteFirstNBlocks(1)
		r.buffer = r.buffer[:0]
		r.currentStep++
	}()

	// get the current block to be read
	blockInfo := r.blks[0]

	// try to update the columns
	// the columns is only updated once for all blocks
	r.tryUpdateColumns(cols)

	// get the block read filter
	filter := r.getReadFilter(r.proc, len(r.blks))

	// if any null expr is found in the primary key (composite primary keys), quick return
	if r.filterState.hasNull {
		return nil, nil
	}

	if !r.dontPrefetch {
		//prefetch some objects
		for len(r.steps) > 0 && r.steps[0] == r.currentStep {
			// always true for now, will optimize this in the future
			prefetchFile := r.scanType == SMALL || r.scanType == LARGE || r.scanType == NORMAL
			if filter != nil && blockInfo.Sorted {
				err = blockio.BlockPrefetch(r.filterState.seqnums, r.fs, [][]*objectio.BlockInfo{r.infos[0]}, prefetchFile)
			} else {
				err = blockio.BlockPrefetch(r.columns.seqnums, r.fs, [][]*objectio.BlockInfo{r.infos[0]}, prefetchFile)
			}
			if err != nil {
				return nil, err
			}
			r.infos = r.infos[1:]
			r.steps = r.steps[1:]
		}
	}

	statsCtx, numRead, numHit := r.ctx, int64(0), int64(0)
	if filter != nil {
		// try to store the blkReadStats CounterSet into ctx, so that
		// it can record the mem cache hit stats when call MemCache.Read() later soon.
		statsCtx, numRead, numHit = r.prepareGatherStats()
	}

	// read the block
	var policy fileservice.Policy
	if r.scanType == LARGE || r.scanType == NORMAL {
		policy = fileservice.SkipMemoryCacheWrites
	}
	bat, err = blockio.BlockRead(
		statsCtx, blockInfo, r.buffer, r.columns.seqnums, r.columns.colTypes, r.ts,
		r.filterState.seqnums,
		r.filterState.colTypes,
		filter,
		r.fs, mp, vp, policy,
	)
	if err != nil {
		return nil, err
	}

	if filter != nil {
		// we collect mem cache hit related statistics info for blk read here
		r.gatherStats(numRead, numHit)
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

func (r *blockReader) prepareGatherStats() (context.Context, int64, int64) {
	ctx := perfcounter.WithCounterSet(r.ctx, objectio.BlkReadStats.CounterSet)
	return ctx, objectio.BlkReadStats.CounterSet.FileService.Cache.Read.Load(),
		objectio.BlkReadStats.CounterSet.FileService.Cache.Hit.Load()
}

func (r *blockReader) gatherStats(lastNumRead, lastNumHit int64) {
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
// ---------------------- blockMergeReader -------------------------
// -----------------------------------------------------------------

func newBlockMergeReader(
	ctx context.Context,
	txnTable *txnTable,
	pkVal []byte,
	ts timestamp.Timestamp,
	dirtyBlks []*objectio.BlockInfo,
	filterExpr *plan.Expr,
	fs fileservice.FileService,
	proc *process.Process,
) *blockMergeReader {
	r := &blockMergeReader{
		table: txnTable,
		blockReader: newBlockReader(
			ctx,
			txnTable.GetTableDef(ctx),
			ts,
			dirtyBlks,
			filterExpr,
			fs,
			proc,
		),
		pkVal:      pkVal,
		deletaLocs: make(map[string][]objectio.Location),
	}
	return r
}

func (r *blockMergeReader) Close() error {
	r.table = nil
	return r.blockReader.Close()
}

func (r *blockMergeReader) prefetchDeletes() error {
	//load delta locations for r.blocks.
	r.table.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer r.table.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	if !r.loaded {
		for _, info := range r.blks {
			bats, ok := r.table.getTxn().blockId_tn_delete_metaLoc_batch.data[info.BlockID]

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
			pref.AddBlockWithType([]uint16{0, uint16(r.pkidx)}, []uint16{loc.ID()}, uint16(objectio.SchemaTombstone))

		}
		delete(r.deletaLocs, name)
		return blockio.PrefetchWithMerged(pref)
	}
	return nil
}

func (r *blockMergeReader) loadDeletes(ctx context.Context, cols []string) error {
	if len(r.blks) == 0 {
		return nil
	}
	info := r.blks[0]

	r.tryUpdateColumns(cols)
	// load deletes from txn.blockId_dn_delete_metaLoc_batch
	err := r.table.LoadDeletesForBlock(info.BlockID, &r.buffer)
	if err != nil {
		return err
	}

	// load deletes from partition state for the specified block
	filter := r.getReadFilter(r.proc, len(r.blks))

	state, err := r.table.getPartitionState(ctx)
	if err != nil {
		return err
	}
	ts := types.TimestampToTS(r.ts)

	if filter != nil && info.Sorted && len(r.pkVal) > 0 {
		iter := state.NewPrimaryKeyDelIter(
			ts,
			logtailreplay.Prefix(r.pkVal),
			info.BlockID,
		)
		for iter.Next() {
			entry := iter.Entry()
			if !entry.Deleted {
				continue
			}
			_, offset := entry.RowID.Decode()
			r.buffer = append(r.buffer, int64(offset))
		}
		iter.Close()
	} else {
		iter := state.NewRowsIter(ts, &info.BlockID, true)
		currlen := len(r.buffer)
		for iter.Next() {
			entry := iter.Entry()
			_, offset := entry.RowID.Decode()
			r.buffer = append(r.buffer, int64(offset))
		}
		v2.TaskLoadMemDeletesPerBlockHistogram.Observe(float64(len(r.buffer) - currlen))
		iter.Close()
	}

	//TODO:: if r.table.writes is a map , the time complexity could be O(1)
	//load deletes from txn.writes for the specified block
	r.table.getTxn().forEachTableWrites(
		r.table.db.databaseId,
		r.table.tableId,
		r.table.getTxn().GetSnapshotWriteOffset(), func(entry Entry) {
			if entry.isGeneratedByTruncate() {
				return
			}
			if (entry.typ == DELETE || entry.typ == DELETE_TXN) && entry.fileName == "" {
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					id, offset := v.Decode()
					if id == info.BlockID {
						r.buffer = append(r.buffer, int64(offset))
					}
				}
			}
		})
	//load deletes from txn.deletedBlocks.
	txn := r.table.getTxn()
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
	start := time.Now()
	defer func() {
		v2.TxnBlockMergeReaderDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	//prefetch deletes for r.blks
	if err := r.prefetchDeletes(); err != nil {
		return nil, err
	}
	//load deletes for the specified block
	if err := r.loadDeletes(ctx, cols); err != nil {
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
