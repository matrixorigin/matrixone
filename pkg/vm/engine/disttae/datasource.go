// Copyright 2021-2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	batchPrefetchSize = 1000
)

func NewRemoteDataSource(
	ctx context.Context,
	proc *process.Process,
	fs fileservice.FileService,
	snapshotTS timestamp.Timestamp,
	relData engine.RelData,
) (source *RemoteDataSource) {
	return &RemoteDataSource{
		data: relData,
		ctx:  ctx,
		proc: proc,
		fs:   fs,
		ts:   types.TimestampToTS(snapshotTS),
	}
}

func NewLocalDataSource(
	ctx context.Context,
	table *txnTable,
	txnOffset int,
	rangesSlice objectio.BlockInfoSlice,
	skipReadMem bool,
	policy engine.TombstoneApplyPolicy,
) (source *LocalDataSource, err error) {

	source = &LocalDataSource{}
	source.fs = table.getTxn().engine.fs
	source.ctx = ctx
	source.mp = table.proc.Load().Mp()
	source.tombstonePolicy = policy

	if rangesSlice != nil && rangesSlice.Len() > 0 {
		if bytes.Equal(
			objectio.EncodeBlockInfo(*rangesSlice.Get(0)),
			objectio.EmptyBlockInfoBytes) {
			rangesSlice = rangesSlice.Slice(1, rangesSlice.Len())
		}

		source.rangeSlice = rangesSlice
	}

	state, err := table.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	source.table = table
	source.pState = state
	source.txnOffset = txnOffset
	source.snapshotTS = types.TimestampToTS(table.getTxn().op.SnapshotTS())

	source.iteratePhase = engine.InMem
	if skipReadMem {
		source.iteratePhase = engine.Persisted
	}

	return source, nil
}

// --------------------------------------------------------------------------------
//	RemoteDataSource defines and APIs
// --------------------------------------------------------------------------------

type RemoteDataSource struct {
	ctx  context.Context
	proc *process.Process

	fs fileservice.FileService
	ts types.TS

	batchPrefetchCursor int
	cursor              int
	data                engine.RelData
}

func (rs *RemoteDataSource) Next(
	_ context.Context,
	_ []string,
	_ []types.Type,
	seqNums []uint16,
	_ any,
	_ *mpool.MPool,
	_ engine.VectorPool,
	_ *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {

	rs.batchPrefetch(seqNums)

	if rs.cursor >= rs.data.DataCnt() {
		return nil, engine.End, nil
	}
	rs.cursor++
	cur := rs.data.GetBlockInfo(rs.cursor - 1)
	return &cur, engine.Persisted, nil
}

func (rs *RemoteDataSource) batchPrefetch(seqNums []uint16) {
	if rs.batchPrefetchCursor >= rs.data.DataCnt() ||
		rs.cursor < rs.batchPrefetchCursor {
		return
	}

	bathSize := min(batchPrefetchSize, rs.data.DataCnt()-rs.cursor)

	begin := rs.batchPrefetchCursor
	end := begin + bathSize

	bids := make([]objectio.Blockid, end-begin)
	blks := make([]*objectio.BlockInfo, end-begin)
	for idx := begin; idx < end; idx++ {
		blk := rs.data.GetBlockInfo(idx)
		blks[idx-begin] = &blk
		bids[idx-begin] = blk.BlockID
	}

	// prefetch blk data
	err := blockio.BlockPrefetch(
		rs.proc.GetService(), seqNums, rs.fs, blks, true)
	if err != nil {
		logutil.Errorf("pefetch block data: %s", err.Error())
	}

	rs.data.GetTombstones().PrefetchTombstones(rs.proc.GetService(), rs.fs, bids)

	rs.batchPrefetchCursor = end
}

func (rs *RemoteDataSource) Close() {
	rs.cursor = 0
}

func (rs *RemoteDataSource) applyInMemTombstones(
	bid types.Blockid,
	rowsOffset []int64,
	deletedRows *nulls.Nulls,
) (leftRows []int64) {
	tombstones := rs.data.GetTombstones()
	if tombstones == nil || !tombstones.HasAnyInMemoryTombstone() {
		return rowsOffset
	}
	return rs.data.GetTombstones().ApplyInMemTombstones(
		bid,
		rowsOffset,
		deletedRows)
}

func (rs *RemoteDataSource) applyPersistedTombstones(
	ctx context.Context,
	bid types.Blockid,
	rowsOffset []int64,
	mask *nulls.Nulls,
) (leftRows []int64, err error) {
	tombstones := rs.data.GetTombstones()
	if tombstones == nil || !tombstones.HasAnyTombstoneFile() {
		return rowsOffset, nil
	}

	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		rs.fs,
		rs.ts,
		bid,
		rowsOffset,
		mask)
}

func (rs *RemoteDataSource) ApplyTombstones(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int64,
	applyPolicy engine.TombstoneApplyPolicy,
) (left []int64, err error) {

	slices.SortFunc(rowsOffset, func(a, b int64) int {
		return int(a - b)
	})

	left = rs.applyInMemTombstones(bid, rowsOffset, nil)

	left, err = rs.applyPersistedTombstones(ctx, bid, left, nil)
	if err != nil {
		return
	}
	return
}

func (rs *RemoteDataSource) GetTombstones(
	ctx context.Context, bid objectio.Blockid,
) (mask *nulls.Nulls, err error) {

	mask = &nulls.Nulls{}
	mask.InitWithSize(8192)

	rs.applyInMemTombstones(bid, nil, mask)

	_, err = rs.applyPersistedTombstones(ctx, bid, nil, mask)
	if err != nil {
		return
	}

	return mask, nil
}

func (rs *RemoteDataSource) SetOrderBy(_ []*plan.OrderBySpec) {

}

func (rs *RemoteDataSource) GetOrderBy() []*plan.OrderBySpec {
	return nil
}

func (rs *RemoteDataSource) SetFilterZM(_ objectio.ZoneMap) {

}

// --------------------------------------------------------------------------------
//	LocalDataSource defines and APIs
// --------------------------------------------------------------------------------

type LocalDataSource struct {
	rangeSlice objectio.BlockInfoSlice
	pState     *logtailreplay.PartitionState

	memPKFilter *MemPKFilter
	pStateRows  struct {
		insIter logtailreplay.RowsIter
	}

	table     *txnTable
	wsCursor  int
	txnOffset int

	// runtime config
	rc struct {
		batchPrefetchCursor int
		WorkspaceLocked     bool
		//SkipPStateDeletes   bool
	}

	mp  *mpool.MPool
	ctx context.Context
	fs  fileservice.FileService

	rangesCursor int
	snapshotTS   types.TS
	iteratePhase engine.DataState

	//TODO:: It's so ugly, need to refactor
	//for order by
	desc     bool
	blockZMS []index.ZM
	sorted   bool // blks need to be sorted by zonemap
	OrderBy  []*plan.OrderBySpec

	filterZM        objectio.ZoneMap
	tombstonePolicy engine.TombstoneApplyPolicy
}

func (ls *LocalDataSource) String() string {
	blks := make([]*objectio.BlockInfo, ls.rangeSlice.Len())
	for i := range blks {
		blks[i] = ls.rangeSlice.Get(i)
	}

	return fmt.Sprintf("snapshot: %s, phase: %v, txnOffset: %d, rangeCursor: %d, blk list: %v",
		ls.snapshotTS.ToString(),
		ls.iteratePhase,
		ls.txnOffset,
		ls.rangesCursor,
		blks)
}

func (ls *LocalDataSource) SetOrderBy(orderby []*plan.OrderBySpec) {
	ls.OrderBy = orderby
}

func (ls *LocalDataSource) GetOrderBy() []*plan.OrderBySpec {
	return ls.OrderBy
}

func (ls *LocalDataSource) SetFilterZM(zm objectio.ZoneMap) {
	if !ls.filterZM.IsInited() {
		ls.filterZM = zm.Clone()
		return
	}
	if ls.desc && ls.filterZM.CompareMax(zm) < 0 {
		ls.filterZM = zm.Clone()
		return
	}
	if !ls.desc && ls.filterZM.CompareMin(zm) > 0 {
		ls.filterZM = zm.Clone()
		return
	}
}

func (ls *LocalDataSource) needReadBlkByZM(i int) bool {
	zm := ls.blockZMS[i]
	if !ls.filterZM.IsInited() || !zm.IsInited() {
		return true
	}
	if ls.desc {
		return ls.filterZM.CompareMax(zm) <= 0
	} else {
		return ls.filterZM.CompareMin(zm) >= 0
	}
}

func (ls *LocalDataSource) getBlockZMs() {
	orderByCol, _ := ls.OrderBy[0].Expr.Expr.(*plan.Expr_Col)

	def := ls.table.tableDef
	orderByColIDX := int(def.Cols[int(orderByCol.Col.ColPos)].Seqnum)

	sliceLen := ls.rangeSlice.Len()
	ls.blockZMS = make([]index.ZM, sliceLen)
	var objDataMeta objectio.ObjectDataMeta
	var location objectio.Location
	for i := ls.rangesCursor; i < sliceLen; i++ {
		location = ls.rangeSlice.Get(i).MetaLocation()
		if !objectio.IsSameObjectLocVsMeta(location, objDataMeta) {
			objMeta, err := objectio.FastLoadObjectMeta(ls.ctx, &location, false, ls.fs)
			if err != nil {
				panic("load object meta error when ordered scan!")
			}
			objDataMeta = objMeta.MustDataMeta()
		}
		blkMeta := objDataMeta.GetBlockMeta(uint32(location.ID()))
		ls.blockZMS[i] = blkMeta.ColumnMeta(uint16(orderByColIDX)).ZoneMap()
	}
}

func (ls *LocalDataSource) sortBlockList() {
	sliceLen := ls.rangeSlice.Len()
	// FIXME: no pointer in helper
	helper := make([]*blockSortHelper, sliceLen)
	for i := range sliceLen {
		helper[i] = &blockSortHelper{}
		helper[i].blk = ls.rangeSlice.Get(i)
		helper[i].zm = ls.blockZMS[i]
	}
	ls.rangeSlice = make(objectio.BlockInfoSlice, ls.rangeSlice.Size())

	if ls.desc {
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
		ls.rangeSlice.Set(i, helper[i].blk)
		//ls.ranges[i] = helper[i].blk
		ls.blockZMS[i] = helper[i].zm
	}
}

func (ls *LocalDataSource) Close() {
	if ls.pStateRows.insIter != nil {
		ls.pStateRows.insIter.Close()
		ls.pStateRows.insIter = nil
	}
}

func (ls *LocalDataSource) Next(
	ctx context.Context,
	cols []string,
	types []types.Type,
	seqNums []uint16,
	filter any,
	mp *mpool.MPool,
	vp engine.VectorPool,
	bat *batch.Batch,
) (*objectio.BlockInfo, engine.DataState, error) {

	if ls.memPKFilter == nil {
		ff := filter.(MemPKFilter)
		ls.memPKFilter = &ff
	}

	if len(cols) == 0 {
		return nil, engine.End, nil
	}

	// bathed prefetch block data and deletes
	ls.batchPrefetch(seqNums)

	for {
		switch ls.iteratePhase {
		case engine.InMem:
			bat.CleanOnlyData()
			err := ls.iterateInMemData(ctx, cols, types, seqNums, bat, mp, vp)
			if err != nil {
				return nil, engine.InMem, err
			}

			if bat.RowCount() == 0 {
				ls.iteratePhase = engine.Persisted
				continue
			}

			return nil, engine.InMem, nil

		case engine.Persisted:
			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, engine.End, nil
			}

			ls.handleOrderBy()

			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, engine.End, nil
			}

			blk := ls.rangeSlice.Get(ls.rangesCursor)
			ls.rangesCursor++

			return blk, engine.Persisted, nil

		case engine.End:
			return nil, ls.iteratePhase, nil
		}
	}
}

func (ls *LocalDataSource) handleOrderBy() {
	// for ordered scan, sort blocklist by zonemap info, and then filter by zonemap
	if len(ls.OrderBy) > 0 {
		if !ls.sorted {
			ls.desc = ls.OrderBy[0].Flag&plan.OrderBySpec_DESC != 0
			ls.getBlockZMs()
			ls.sortBlockList()
			ls.sorted = true
		}
		i := ls.rangesCursor
		sliceLen := ls.rangeSlice.Len()
		for i < sliceLen {
			if ls.needReadBlkByZM(i) {
				break
			}
			i++
		}
		ls.rangesCursor = i
	}
}

func (ls *LocalDataSource) iterateInMemData(
	ctx context.Context,
	cols []string,
	colTypes []types.Type,
	seqNums []uint16,
	bat *batch.Batch,
	mp *mpool.MPool,
	vp engine.VectorPool,
) (err error) {

	bat.SetRowCount(0)

	if err = ls.filterInMemUnCommittedInserts(seqNums, mp, bat); err != nil {
		return err
	}

	if err = ls.filterInMemCommittedInserts(colTypes, seqNums, mp, bat); err != nil {
		return err
	}

	return nil
}

func checkWorkspaceEntryType(tbl *txnTable, entry Entry, isInsert bool) bool {
	if entry.DatabaseId() != tbl.db.databaseId || entry.TableId() != tbl.tableId {
		return false
	}

	// within a txn, the later statement could delete the previous
	// inserted rows, the left rows will be recorded in `batSelectList`.
	// if no rows left, this bat can be seen deleted.
	//
	// Note that: some row have been deleted, but some left
	if isInsert {
		if entry.typ != INSERT ||
			entry.bat == nil ||
			entry.bat.IsEmpty() ||
			entry.bat.Attrs[0] == catalog.BlockMeta_MetaLoc {
			return false
		}
		if left, exist := tbl.getTxn().batchSelectList[entry.bat]; exist && len(left) == 0 {
			// all rows have deleted in this bat
			return false
		} else if len(left) > 0 {
			// FIXME: if len(left) > 0, we need to exclude the deleted rows in this batch
			logutil.Fatal("FIXME: implement later")
		}
		return true
	}

	// handle delete entry
	return (entry.typ == DELETE) && (entry.fileName == "")
}

func (ls *LocalDataSource) filterInMemUnCommittedInserts(
	seqNums []uint16,
	mp *mpool.MPool,
	bat *batch.Batch,
) error {

	if ls.wsCursor >= ls.txnOffset {
		return nil
	}

	ls.table.getTxn().Lock()
	ls.rc.WorkspaceLocked = true
	defer func() {
		ls.table.getTxn().Unlock()
		ls.rc.WorkspaceLocked = false
	}()

	rows := 0
	writes := ls.table.getTxn().writes
	maxRows := int(options.DefaultBlockMaxRows)
	if len(writes) == 0 {
		return nil
	}

	var retainedRowIds []types.Rowid

	for ; ls.wsCursor < ls.txnOffset; ls.wsCursor++ {
		if writes[ls.wsCursor].bat == nil {
			continue
		}

		if rows+writes[ls.wsCursor].bat.RowCount() > maxRows {
			break
		}

		entry := ls.table.getTxn().writes[ls.wsCursor]

		if ok := checkWorkspaceEntryType(ls.table, entry, true); !ok {
			continue
		}

		retainedRowIds = vector.MustFixedCol[types.Rowid](entry.bat.Vecs[0])
		offsets := rowIdsToOffset(retainedRowIds, int64(0)).([]int64)

		b, _ := retainedRowIds[0].Decode()
		sels, err := ls.ApplyTombstones(ls.ctx, b, offsets, engine.Policy_CheckUnCommittedOnly)
		if err != nil {
			return err
		}

		if len(sels) == 0 {
			continue
		}

		rows += len(sels)

		for i, destVec := range bat.Vecs {
			uf := vector.GetUnionOneFunction(*destVec.GetType(), mp)

			colIdx := int(seqNums[i])
			if colIdx != objectio.SEQNUM_ROWID {
				colIdx++
			} else {
				colIdx = 0
			}

			for j := range sels {
				if err = uf(destVec, entry.bat.Vecs[colIdx], int64(j)); err != nil {
					return err
				}
			}
		}
	}

	bat.SetRowCount(bat.Vecs[0].Length())
	return nil
}

func (ls *LocalDataSource) filterInMemCommittedInserts(
	colTypes []types.Type, seqNums []uint16, mp *mpool.MPool, bat *batch.Batch,
) error {

	// in meme committed insert only need to apply deletes that exists
	// in workspace and flushed to s3 but not commit.
	//ls.rc.SkipPStateDeletes = true
	//defer func() {
	//	ls.rc.SkipPStateDeletes = false
	//}()

	if bat.RowCount() >= int(options.DefaultBlockMaxRows) {
		return nil
	}

	var (
		err error
		sel []int64
	)

	if ls.pStateRows.insIter == nil {
		if ls.memPKFilter.SpecFactory == nil {
			ls.pStateRows.insIter = ls.pState.NewRowsIter(ls.snapshotTS, nil, false)
		} else {
			ls.pStateRows.insIter = ls.pState.NewPrimaryKeyIter(
				ls.memPKFilter.TS, ls.memPKFilter.SpecFactory(ls.memPKFilter))
		}
	}

	var minTS types.TS = types.MaxTs()
	var batRowIdx int
	if batRowIdx = slices.Index(bat.Attrs, catalog.Row_ID); batRowIdx == -1 {
		batRowIdx = len(bat.Attrs)
		bat.Attrs = append(bat.Attrs, catalog.Row_ID)
		bat.Vecs = append(bat.Vecs, vector.NewVec(types.T_Rowid.ToType()))

		defer func() {
			bat.Attrs = bat.Attrs[:len(bat.Attrs)-1]
			bat.Vecs[len(bat.Vecs)-1].Free(mp)
			bat.Vecs = bat.Vecs[:len(bat.Vecs)-1]
		}()
	}

	applyPolicy := engine.TombstoneApplyPolicy(
		engine.Policy_SkipCommittedInMemory | engine.Policy_SkipCommittedS3)

	applyOffset := 0

	goon := true
	for goon && bat.Vecs[0].Length() < int(options.DefaultBlockMaxRows) {
		//minTS = types.MaxTs()
		for bat.Vecs[0].Length() < int(options.DefaultBlockMaxRows) {
			if goon = ls.pStateRows.insIter.Next(); !goon {
				break
			}

			entry := ls.pStateRows.insIter.Entry()
			b, o := entry.RowID.Decode()

			//if minTS.Greater(&entry.Time) {
			//	minTS = entry.Time
			//}

			sel, err = ls.ApplyTombstones(ls.ctx, b, []int64{int64(o)}, applyPolicy)
			if err != nil {
				return err
			}

			if len(sel) == 0 {
				continue
			}

			for i, name := range bat.Attrs {
				if name == catalog.Row_ID {
					if err = vector.AppendFixed(
						bat.Vecs[i],
						entry.RowID,
						false,
						mp); err != nil {
						return err
					}
				} else {
					idx := 2 /*rowid and commits*/ + seqNums[i]
					if int(idx) >= len(entry.Batch.Vecs) /*add column*/ ||
						entry.Batch.Attrs[idx] == "" /*drop column*/ {
						err = vector.AppendAny(
							bat.Vecs[i],
							nil,
							true,
							mp)
					} else {
						err = bat.Vecs[i].UnionOne(
							entry.Batch.Vecs[int(2+seqNums[i])],
							entry.Offset,
							mp,
						)
					}
					if err != nil {
						return err
					}
				}
			}
		}

		rowIds := vector.MustFixedCol[types.Rowid](bat.Vecs[batRowIdx])
		deleted, err := ls.batchApplyTombstoneObjects(minTS, rowIds[applyOffset:])
		if err != nil {
			return err
		}

		for i := range deleted {
			deleted[i] += int64(applyOffset)
		}

		bat.Shrink(deleted, true)
		applyOffset = bat.Vecs[0].Length()
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func loadBlockDeletesByLocation(
	ctx context.Context,
	fs fileservice.FileService,
	blockId types.Blockid,
	location objectio.Location,
	snapshotTS types.TS,
) (deleteMask *nulls.Nulls, err error) {

	var (
		rows *nulls.Nulls
		//bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	if !location.IsEmpty() {
		//t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, location, fs); err != nil {
			return nil, err
		}
		defer release()

		//readCost := time.Since(t1)

		if persistedByCN {
			rows = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(blockId, persistedDeletes)
		} else {
			//t2 := time.Now()
			rows = blockio.EvalDeleteRowsByTimestamp(persistedDeletes, snapshotTS, &blockId)
			//bisect = time.Since(t2)
		}

		if rows != nil {
			deleteMask = rows
		}

		//readTotal := time.Since(t1)
		//blockio.RecordReadDel(readTotal, readCost, bisect)
	}

	return deleteMask, nil
}

// ApplyTombstones check if any deletes exist in
//  1. unCommittedInmemDeletes:
//     a. workspace writes
//     b. flushed to s3
//     c. raw rowId offset deletes (not flush yet)
//  3. committedInmemDeletes
//  4. committedPersistedTombstone
func (ls *LocalDataSource) ApplyTombstones(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int64,
	dynamicPolicy engine.TombstoneApplyPolicy,
) ([]int64, error) {

	if len(rowsOffset) == 0 {
		return nil, nil
	}

	slices.SortFunc(rowsOffset, func(a, b int64) int {
		return int(a - b)
	})

	var err error

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 &&
		dynamicPolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		rowsOffset = ls.applyWorkspaceEntryDeletes(bid, rowsOffset, nil)
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}
	if ls.tombstonePolicy&engine.Policy_SkipUncommitedS3 == 0 &&
		dynamicPolicy&engine.Policy_SkipUncommitedS3 == 0 {
		rowsOffset, err = ls.applyWorkspaceFlushedS3Deletes(bid, rowsOffset, nil)
		if err != nil {
			return nil, err
		}
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 &&
		dynamicPolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		rowsOffset = ls.applyWorkspaceRawRowIdDeletes(bid, rowsOffset, nil)
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}
	if ls.tombstonePolicy&engine.Policy_SkipCommittedInMemory == 0 &&
		dynamicPolicy&engine.Policy_SkipCommittedInMemory == 0 {
		rowsOffset = ls.applyPStateInMemDeletes(bid, rowsOffset, nil)
	}
	if len(rowsOffset) == 0 {
		return nil, nil
	}
	if ls.tombstonePolicy&engine.Policy_SkipCommittedS3 == 0 &&
		dynamicPolicy&engine.Policy_SkipCommittedS3 == 0 {
		rowsOffset, err = ls.applyPStateTombstoneObjects(bid, rowsOffset, nil)
		if err != nil {
			return nil, err
		}
	}

	return rowsOffset, nil
}

func (ls *LocalDataSource) GetTombstones(
	ctx context.Context, bid objectio.Blockid,
) (deletedRows *nulls.Nulls, err error) {

	deletedRows = &nulls.Nulls{}
	deletedRows.InitWithSize(8192)

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceEntryDeletes(bid, nil, deletedRows)
	}
	if ls.tombstonePolicy&engine.Policy_SkipUncommitedS3 == 0 {
		_, err = ls.applyWorkspaceFlushedS3Deletes(bid, nil, deletedRows)
		if err != nil {
			return nil, err
		}
	}

	if ls.tombstonePolicy&engine.Policy_SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceRawRowIdDeletes(bid, nil, deletedRows)
	}

	if ls.tombstonePolicy&engine.Policy_SkipCommittedInMemory == 0 {
		ls.applyPStateInMemDeletes(bid, nil, deletedRows)
	}

	//_, err = ls.applyPStatePersistedDeltaLocation(bid, nil, deletedRows)
	_, err = ls.applyPStateTombstoneObjects(bid, nil, deletedRows)
	if err != nil {
		return nil, err
	}

	return deletedRows, nil
}

// will return the rows which applied deletes if the `leftRows` is not empty,
// or the deletes will only record into the `deleteRows` bitmap.
func fastApplyDeletedRows(
	leftRows []int64,
	deletedRows *nulls.Nulls,
	o uint32,
) []int64 {
	if len(leftRows) != 0 {
		if x, found := sort.Find(len(leftRows), func(i int) int {
			return int(int64(o) - leftRows[i])
		}); found {
			leftRows = append(leftRows[:x], leftRows[x+1:]...)
		}
	} else if deletedRows != nil {
		deletedRows.Add(uint64(o))
	}

	return leftRows
}

func (ls *LocalDataSource) applyWorkspaceEntryDeletes(
	bid objectio.Blockid,
	offsets []int64,
	deletedRows *nulls.Nulls,
) (leftRows []int64) {

	leftRows = offsets

	// may have locked in `filterInMemUnCommittedInserts`
	if !ls.rc.WorkspaceLocked {
		ls.table.getTxn().Lock()
		defer ls.table.getTxn().Unlock()
	}

	done := false
	writes := ls.table.getTxn().writes[:ls.txnOffset]

	var delRowIds []types.Rowid

	for idx := range writes {
		if ok := checkWorkspaceEntryType(ls.table, writes[idx], false); !ok {
			continue
		}

		delRowIds = vector.MustFixedCol[types.Rowid](writes[idx].bat.Vecs[0])
		for _, delRowId := range delRowIds {
			b, o := delRowId.Decode()
			if bid.Compare(b) != 0 {
				continue
			}

			leftRows = fastApplyDeletedRows(leftRows, deletedRows, o)
			if leftRows != nil && len(leftRows) == 0 {
				done = true
				break
			}
		}

		if done {
			break
		}
	}

	return leftRows
}

// if blks comes from unCommitted flushed s3 deletes, the
// blkCommitTS can be zero.
func applyDeletesWithinDeltaLocations(
	ctx context.Context,
	fs fileservice.FileService,
	bid objectio.Blockid,
	snapshotTS types.TS,
	blkCommitTS types.TS,
	offsets []int64,
	deletedRows *nulls.Nulls,
	locations ...objectio.Location,
) (leftRows []int64, err error) {

	if offsets != nil {
		leftRows = make([]int64, 0, len(offsets))
	}

	var mask *nulls.Nulls

	for _, loc := range locations {
		if mask, err = loadBlockDeletesByLocation(
			ctx, fs, bid, loc[:], snapshotTS); err != nil {
			return nil, err
		}

		if offsets != nil {
			leftRows = removeIf(offsets, func(t int64) bool {
				return mask.Contains(uint64(t))
			})

		} else if deletedRows != nil {
			deletedRows.Or(mask)
		}
	}

	return leftRows, nil
}

func (ls *LocalDataSource) applyWorkspaceFlushedS3Deletes(
	bid objectio.Blockid,
	offsets []int64,
	deletedRows *nulls.Nulls,
) (leftRows []int64, err error) {

	leftRows = offsets
	var locations []objectio.Location

	// cannot hold the lock too long
	{
		s3FlushedDeletes := &ls.table.getTxn().blockId_tn_delete_metaLoc_batch
		s3FlushedDeletes.RWMutex.Lock()
		defer s3FlushedDeletes.RWMutex.Unlock()

		if len(s3FlushedDeletes.data[bid]) == 0 || ls.pState.BlockPersisted(bid) {
			return
		}

		locations = make([]objectio.Location, 0, len(s3FlushedDeletes.data[bid]))

		for _, bat := range s3FlushedDeletes.data[bid] {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
			for i := range vs {
				loc, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					return nil, err
				}

				locations = append(locations, loc)
			}
		}
	}

	return applyDeletesWithinDeltaLocations(
		ls.ctx,
		ls.fs,
		bid,
		ls.snapshotTS,
		types.TS{},
		offsets,
		deletedRows,
		locations...)
}

func (ls *LocalDataSource) applyWorkspaceRawRowIdDeletes(
	bid objectio.Blockid,
	offsets []int64,
	deletedRows *nulls.Nulls,
) (leftRows []int64) {

	leftRows = offsets

	rawRowIdDeletes := ls.table.getTxn().deletedBlocks
	rawRowIdDeletes.RWMutex.RLock()
	defer rawRowIdDeletes.RWMutex.RUnlock()

	for _, o := range rawRowIdDeletes.offsets[bid] {
		leftRows = fastApplyDeletedRows(leftRows, deletedRows, uint32(o))
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	return leftRows
}

func (ls *LocalDataSource) applyPStateInMemDeletes(
	bid objectio.Blockid,
	offsets []int64,
	deletedRows *nulls.Nulls,
) (leftRows []int64) {

	//if ls.rc.SkipPStateDeletes {
	//	return offsets
	//}

	var delIter logtailreplay.RowsIter

	if ls.memPKFilter == nil || ls.memPKFilter.SpecFactory == nil {
		delIter = ls.pState.NewRowsIter(ls.snapshotTS, &bid, true)
	} else {
		delIter = ls.pState.NewPrimaryKeyDelIter(
			ls.memPKFilter.TS,
			ls.memPKFilter.SpecFactory(ls.memPKFilter), bid)
	}

	leftRows = offsets

	for delIter.Next() {
		_, o := delIter.Entry().RowID.Decode()
		leftRows = fastApplyDeletedRows(leftRows, deletedRows, o)
		if leftRows != nil && len(leftRows) == 0 {
			break
		}
	}

	delIter.Close()

	return leftRows
}

func (ls *LocalDataSource) applyPStateTombstoneObjects(
	bid objectio.Blockid,
	offsets []int64,
	deletedRows *nulls.Nulls,
) ([]int64, error) {

	//if ls.rc.SkipPStateDeletes {
	//	return offsets, nil
	//}

	if ls.pState.ApproxTombstoneObjectsNum() == 0 {
		return offsets, nil
	}

	scanOp := func(onTombstone func(tombstone logtailreplay.ObjectEntry) (bool, error)) (err error) {
		return ForeachTombstoneObject(ls.snapshotTS, onTombstone, ls.pState)
	}

	if deletedRows == nil {
		deletedRows = &nulls.Nulls{}
		deletedRows.InitWithSize(8192)
	}

	if err := GetTombstonesByBlockId(
		ls.ctx,
		ls.fs,
		bid,
		ls.snapshotTS,
		deletedRows,
		scanOp); err != nil {
		return nil, err
	}

	offsets = removeIf(offsets, func(t int64) bool {
		return deletedRows.Contains(uint64(t))
	})

	return offsets, nil
}

func (ls *LocalDataSource) batchPrefetch(seqNums []uint16) {
	if ls.rc.batchPrefetchCursor >= ls.rangeSlice.Len() ||
		ls.rangesCursor < ls.rc.batchPrefetchCursor {
		return
	}

	batchSize := min(batchPrefetchSize, ls.rangeSlice.Len()-ls.rangesCursor)

	begin := ls.rangesCursor
	end := ls.rangesCursor + batchSize

	blks := make([]*objectio.BlockInfo, end-begin)
	for idx := begin; idx < end; idx++ {
		blks[idx-begin] = ls.rangeSlice.Get(idx)
	}

	// prefetch blk data
	err := blockio.BlockPrefetch(
		ls.table.proc.Load().GetService(), seqNums, ls.fs, blks, true)
	if err != nil {
		logutil.Errorf("pefetch block data: %s", err.Error())
	}

	ls.table.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer ls.table.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	// prefetch cn flushed but not committed deletes
	var ok bool
	var bats []*batch.Batch
	var locs []objectio.Location = make([]objectio.Location, 0)

	pkColIdx := ls.table.tableDef.Pkey.PkeyColId

	for idx := begin; idx < end; idx++ {
		if bats, ok = ls.table.getTxn().blockId_tn_delete_metaLoc_batch.data[ls.rangeSlice.Get(idx).BlockID]; !ok {
			continue
		}

		locs = locs[:0]
		for _, bat := range bats {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
			for i := range vs {
				location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
				}
				locs = append(locs, location)
			}
		}

		if len(locs) == 0 {
			continue
		}

		pref, err := blockio.BuildPrefetchParams(ls.fs, locs[0])
		if err != nil {
			logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
		}

		for _, loc := range locs {
			//rowId + pk
			pref.AddBlockWithType([]uint16{0, uint16(pkColIdx)}, []uint16{loc.ID()}, uint16(objectio.SchemaTombstone))
		}

		if err = blockio.PrefetchWithMerged(ls.table.proc.Load().GetService(), pref); err != nil {
			logutil.Errorf("prefetch cn flushed s3 deletes: %s", err.Error())
		}
	}

	ls.rc.batchPrefetchCursor = end
}

func GetTombstonesByBlockId(
	ctx context.Context,
	fs fileservice.FileService,
	bid objectio.Blockid,
	snapshot types.TS,
	deleteMask *nulls.Nulls,
	scanOp func(func(tombstone logtailreplay.ObjectEntry) (bool, error)) error,
) (err error) {

	var (
		totalBlk     int
		zmBreak      int
		blBreak      int
		loaded       int
		totalScanned int
	)

	onTombstone := func(obj logtailreplay.ObjectEntry) (bool, error) {
		totalScanned++
		if !obj.ZMIsEmpty() {
			objZM := obj.SortKeyZoneMap()
			if skip := !objZM.PrefixEq(bid[:]); skip {
				zmBreak++
				return true, nil
			}
		}

		var objMeta objectio.ObjectMeta

		location := obj.Location()

		if objMeta, err = objectio.FastLoadObjectMeta(
			ctx, &location, false, fs,
		); err != nil {
			return false, err
		}
		dataMeta := objMeta.MustDataMeta()

		blkCnt := int(dataMeta.BlockCount())
		totalBlk += blkCnt

		startIdx := sort.Search(blkCnt, func(i int) bool {
			return dataMeta.GetBlockMeta(uint32(i)).MustGetColumn(0).ZoneMap().AnyGEByValue(bid[:])
		})

		for pos := startIdx; pos < blkCnt; pos++ {
			blkMeta := dataMeta.GetBlockMeta(uint32(pos))
			columnZonemap := blkMeta.MustGetColumn(0).ZoneMap()
			// block id is the prefix of the rowid and zonemap is min-max of rowid
			// !PrefixEq means there is no rowid of this block in this zonemap, so skip
			if !columnZonemap.PrefixEq(bid[:]) {
				if columnZonemap.PrefixGT(bid[:]) {
					// all zone maps are sorted by the rowid
					// if the block id is less than the prefix of the min rowid, skip the rest blocks
					break
				}
				continue
			}
			loaded++
			tombstoneLoc := catalog2.BuildLocation(obj.ObjectStats, uint16(pos), options.DefaultBlockMaxRows)

			var mask *nulls.Nulls

			if mask, err = loadBlockDeletesByLocation(
				ctx, fs, bid, tombstoneLoc, snapshot,
			); err != nil {
				return false, err
			}

			deleteMask.Or(mask)
		}
		return true, nil
	}

	err = scanOp(onTombstone)

	v2.TxnReaderEachBLKLoadedTombstoneHistogram.Observe(float64(loaded))
	v2.TxnReaderScannedTotalTombstoneHistogram.Observe(float64(totalScanned))
	if totalScanned != 0 {
		v2.TxnReaderTombstoneZMSelectivityHistogram.Observe(float64(zmBreak) / float64(totalScanned))
	}
	if totalBlk != 0 {
		v2.TxnReaderTombstoneBLSelectivityHistogram.Observe(float64(blBreak) / float64(totalBlk))
	}

	return err
}

func (ls *LocalDataSource) batchApplyTombstoneObjects(
	minTS types.TS,
	rowIds []types.Rowid,
) (deleted []int64, err error) {
	//maxTombstoneTS := ls.pState.MaxTombstoneCreateTS()
	//if maxTombstoneTS.Less(&minTS) {
	//	return nil, nil
	//}

	if ls.pState.ApproxTombstoneObjectsNum() == 0 {
		return nil, nil
	}

	iter, err := ls.pState.NewObjectsIter(ls.snapshotTS, true, true)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var (
		exist    bool
		bf       objectio.BloomFilter
		bfIndex  index.StaticFilter
		location objectio.Location

		loaded        *batch.Batch
		persistedByCN bool
		release       func()
	)

	anyIf := func(check func(row types.Rowid) bool) bool {
		for _, r := range rowIds {
			if check(r) {
				return true
			}
		}
		return false
	}

	for iter.Next() && len(rowIds) > len(deleted) {
		obj := iter.Entry()

		if !obj.ZMIsEmpty() {
			objZM := obj.SortKeyZoneMap()

			if !anyIf(func(row types.Rowid) bool {
				return objZM.PrefixEq(row.BorrowBlockID()[:])
			}) {
				continue
			}
		}

		if bf, err = objectio.FastLoadBF(
			ls.ctx, obj.Location(), false, ls.fs); err != nil {
			return nil, err
		}

		for idx := 0; idx < int(obj.BlkCnt()) && len(rowIds) > len(deleted); idx++ {
			buf := bf.GetBloomFilter(uint32(idx))
			bfIndex = index.NewEmptyBloomFilterWithType(index.HBF)
			if err = index.DecodeBloomFilter(bfIndex, buf); err != nil {
				return nil, err
			}

			if !anyIf(func(row types.Rowid) bool {
				exist, err = bfIndex.PrefixMayContainsKey(row.BorrowBlockID()[:], index.PrefixFnID_Block, 2)
				if exist || err != nil {
					return true
				}
				return false
			}) {
				continue
			}

			if err != nil {
				return nil, err
			}

			location = catalog2.BuildLocation(obj.ObjectStats, uint16(idx), options.DefaultBlockMaxRows)

			if loaded, persistedByCN, release, err = blockio.ReadBlockDelete(ls.ctx, location, ls.fs); err != nil {
				return nil, err
			}

			var deletedRowIds []types.Rowid
			var commit []types.TS

			deletedRowIds = vector.MustFixedCol[types.Rowid](loaded.Vecs[0])
			if !persistedByCN {
				commit = vector.MustFixedCol[types.TS](loaded.Vecs[1])
			}

			for i := range rowIds {
				s, e := blockio.FindIntervalForBlock(deletedRowIds, rowIds[i].BorrowBlockID())
				for j := s; j < e; j++ {
					if rowIds[i].Equal(deletedRowIds[j]) && (commit == nil || commit[j].LessEq(&ls.snapshotTS)) {
						deleted = append(deleted, int64(i))
						break
					}
				}
			}

			release()
		}
	}

	return deleted, nil
}
