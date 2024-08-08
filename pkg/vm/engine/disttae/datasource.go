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
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type TombstoneApplyPolicy uint64

const (
	Policy_SkipUncommitedInMemory = 1 << iota
	Policy_SkipCommittedInMemory
	Policy_SkipUncommitedS3
	Policy_SkipCommittedS3
)

const (
	Policy_CheckAll             = 0
	Policy_CheckCommittedS3Only = Policy_SkipUncommitedInMemory | Policy_SkipCommittedInMemory | Policy_SkipUncommitedS3
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
	policy TombstoneApplyPolicy,
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
) (*batch.Batch, *objectio.BlockInfo, engine.DataState, error) {

	rs.batchPrefetch(seqNums)

	if rs.cursor >= rs.data.DataCnt() {
		return nil, nil, engine.End, nil
	}
	rs.cursor++
	cur := rs.data.GetBlockInfo(rs.cursor - 1)
	return nil, &cur, engine.Persisted, nil
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

	apply := func(
		ctx context.Context,
		loc objectio.Location,
		cts types.TS,
		rowsOffset []int64,
		deleted *nulls.Nulls) (left []int64, err error) {

		deletes, err := loadBlockDeletesByDeltaLoc(ctx, rs.fs, bid, loc, rs.ts, cts)
		if err != nil {
			return nil, err
		}

		if rowsOffset != nil {
			for _, offset := range rowsOffset {
				if deletes.Contains(uint64(offset)) {
					continue
				}
				left = append(left, offset)
			}
		} else if deleted != nil {
			deleted.Merge(deletes)
		}

		return
	}

	return rs.data.GetTombstones().ApplyPersistedTombstones(
		ctx,
		bid,
		rowsOffset,
		mask,
		apply)
}

func (rs *RemoteDataSource) ApplyTombstones(
	ctx context.Context,
	bid objectio.Blockid,
	rowsOffset []int64,
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
		SkipPStateDeletes   bool
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
	tombstonePolicy TombstoneApplyPolicy
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
) (*batch.Batch, *objectio.BlockInfo, engine.DataState, error) {

	if ls.memPKFilter == nil {
		ff := filter.(MemPKFilter)
		ls.memPKFilter = &ff
	}

	if len(cols) == 0 {
		return nil, nil, engine.End, nil
	}

	// bathed prefetch block data and deletes
	ls.batchPrefetch(seqNums)

	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(len(types))
		bat.Attrs = append(bat.Attrs, cols...)

		for i := 0; i < len(types); i++ {
			if vp == nil {
				bat.Vecs[i] = vector.NewVec(types[i])
			} else {
				bat.Vecs[i] = vp.GetVector(types[i])
			}
		}
		return bat
	}

	for {
		switch ls.iteratePhase {
		case engine.InMem:
			bat := buildBatch()
			freeBatch := func() {
				if vp == nil {
					bat.Clean(mp)
				} else {
					vp.PutBatch(bat)
				}
			}
			err := ls.iterateInMemData(ctx, cols, types, seqNums, bat, mp, vp)
			if err != nil {
				freeBatch()
				return nil, nil, engine.InMem, err
			}

			if bat.RowCount() == 0 {
				freeBatch()
				ls.iteratePhase = engine.Persisted
				continue
			}

			return bat, nil, engine.InMem, nil

		case engine.Persisted:
			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, nil, engine.End, nil
			}

			ls.handleOrderBy()

			if ls.rangesCursor >= ls.rangeSlice.Len() {
				return nil, nil, engine.End, nil
			}

			blk := ls.rangeSlice.Get(ls.rangesCursor)
			ls.rangesCursor++

			return nil, blk, engine.Persisted, nil

		case engine.End:
			return nil, nil, ls.iteratePhase, nil
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
		sels, err := ls.ApplyTombstones(ls.ctx, b, offsets)
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
	ls.rc.SkipPStateDeletes = true
	defer func() {
		ls.rc.SkipPStateDeletes = false
	}()

	if bat.RowCount() >= int(options.DefaultBlockMaxRows) {
		return nil
	}

	var (
		err          error
		sel          []int64
		appendedRows = bat.RowCount()
	)

	appendFunctions := make([]func(*vector.Vector, *vector.Vector, int64) error, len(bat.Attrs))
	for i := range bat.Attrs {
		appendFunctions[i] = vector.GetUnionOneFunction(colTypes[i], mp)
	}

	if ls.pStateRows.insIter == nil {
		if ls.memPKFilter.SpecFactory == nil {
			ls.pStateRows.insIter = ls.pState.NewRowsIter(ls.snapshotTS, nil, false)
		} else {
			ls.pStateRows.insIter = ls.pState.NewPrimaryKeyIter(
				ls.memPKFilter.TS, ls.memPKFilter.SpecFactory(ls.memPKFilter))
		}
	}

	for appendedRows < int(options.DefaultBlockMaxRows) && ls.pStateRows.insIter.Next() {
		entry := ls.pStateRows.insIter.Entry()
		b, o := entry.RowID.Decode()

		sel, err = ls.ApplyTombstones(ls.ctx, b, []int64{int64(o)})
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
					err = appendFunctions[i](
						bat.Vecs[i],
						entry.Batch.Vecs[int(2+seqNums[i])],
						entry.Offset,
					)
				}
				if err != nil {
					return err
				}
			}
		}
		appendedRows++
	}

	bat.SetRowCount(bat.Vecs[0].Length())

	return nil
}

func loadBlockDeletesByDeltaLoc(
	ctx context.Context,
	fs fileservice.FileService,
	blockId types.Blockid,
	deltaLoc objectio.Location,
	snapshotTS, blockCommitTS types.TS,
) (deleteMask *nulls.Nulls, err error) {

	var (
		rows *nulls.Nulls
		//bisect           time.Duration
		release          func()
		persistedByCN    bool
		persistedDeletes *batch.Batch
	)

	if !deltaLoc.IsEmpty() {
		//t1 := time.Now()

		if persistedDeletes, persistedByCN, release, err = blockio.ReadBlockDelete(ctx, deltaLoc, fs); err != nil {
			return nil, err
		}
		defer release()

		//readCost := time.Since(t1)

		if persistedByCN {
			rows = blockio.EvalDeleteRowsByTimestampForDeletesPersistedByCN(persistedDeletes, snapshotTS, blockCommitTS)
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
) ([]int64, error) {

	slices.SortFunc(rowsOffset, func(a, b int64) int {
		return int(a - b)
	})

	var err error

	rowsOffset = ls.applyWorkspaceEntryDeletes(bid, rowsOffset, nil)
	rowsOffset, err = ls.applyWorkspaceFlushedS3Deletes(bid, rowsOffset, nil)
	if err != nil {
		return nil, err
	}

	rowsOffset = ls.applyWorkspaceRawRowIdDeletes(bid, rowsOffset, nil)
	rowsOffset = ls.applyPStateInMemDeletes(bid, rowsOffset, nil)
	rowsOffset, err = ls.applyPStatePersistedDeltaLocation(bid, rowsOffset, nil)
	if err != nil {
		return nil, err
	}

	return rowsOffset, nil
}

func (ls *LocalDataSource) GetTombstones(
	ctx context.Context, bid objectio.Blockid,
) (deletedRows *nulls.Nulls, err error) {

	deletedRows = &nulls.Nulls{}
	deletedRows.InitWithSize(8192)

	if ls.tombstonePolicy&Policy_SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceEntryDeletes(bid, nil, deletedRows)
	}
	if ls.tombstonePolicy&Policy_SkipUncommitedS3 == 0 {
		_, err = ls.applyWorkspaceFlushedS3Deletes(bid, nil, deletedRows)
		if err != nil {
			return nil, err
		}
	}

	if ls.tombstonePolicy&Policy_SkipUncommitedInMemory == 0 {
		ls.applyWorkspaceRawRowIdDeletes(bid, nil, deletedRows)
	}

	if ls.tombstonePolicy&Policy_SkipCommittedInMemory == 0 {
		ls.applyPStateInMemDeletes(bid, nil, deletedRows)
	}

	_, err = ls.applyPStatePersistedDeltaLocation(bid, nil, deletedRows)
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
		if mask, err = loadBlockDeletesByDeltaLoc(
			ctx, fs, bid, loc[:], snapshotTS, blkCommitTS); err != nil {
			return nil, err
		}

		if offsets != nil {
			for _, offset := range offsets {
				if mask.Contains(uint64(offset)) {
					continue
				}
				leftRows = append(leftRows, offset)
			}
		} else if deletedRows != nil {
			deletedRows.Merge(mask)
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

	if ls.rc.SkipPStateDeletes {
		return offsets
	}

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

func (ls *LocalDataSource) applyPStatePersistedDeltaLocation(
	bid objectio.Blockid,
	offsets []int64,
	deletedRows *nulls.Nulls,
) (leftRows []int64, err error) {

	if ls.rc.SkipPStateDeletes {
		return offsets, nil
	}

	deltaLoc, commitTS, ok := ls.pState.GetBockDeltaLoc(bid)
	if !ok {
		return offsets, nil
	}

	return applyDeletesWithinDeltaLocations(
		ls.ctx,
		ls.fs,
		bid,
		ls.snapshotTS,
		commitTS,
		offsets,
		deletedRows,
		deltaLoc[:])
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

	// prefetch blk delta location
	for idx := begin; idx < end; idx++ {
		if loc, _, ok := ls.pState.GetBockDeltaLoc(ls.rangeSlice.Get(idx).BlockID); ok {
			if err = blockio.PrefetchTombstone(
				ls.table.proc.Load().GetService(), []uint16{0, 1, 2},
				[]uint16{objectio.Location(loc[:]).ID()}, ls.fs, objectio.Location(loc[:])); err != nil {
				logutil.Errorf("prefetch block delta location: %s", err.Error())
			}
		}
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
