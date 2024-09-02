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
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	commonUtil "github.com/matrixorigin/matrixone/pkg/common/util"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/deletion"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	AllColumns = "*"
)

var traceFilterExprInterval atomic.Uint64
var traceFilterExprInterval2 atomic.Uint64

var _ engine.Relation = new(txnTable)

func (tbl *txnTable) getEngine() engine.Engine {
	return tbl.getTxn().engine
}

func (tbl *txnTable) getTxn() *Transaction {
	return tbl.db.getTxn()
}

func (tbl *txnTable) Stats(ctx context.Context, sync bool) (*pb.StatsInfo, error) {
	_, err := tbl.getPartitionState(ctx)
	if err != nil {
		logutil.Errorf("failed to get partition state of table %d: %v", tbl.tableId, err)
		return nil, err
	}
	if !tbl.db.op.IsSnapOp() {
		e := tbl.getEngine()
		return e.Stats(ctx, pb.StatsInfoKey{
			AccId:      tbl.accountId,
			DatabaseID: tbl.db.databaseId,
			TableID:    tbl.tableId,
		}, sync), nil
	}
	info, err := tbl.stats(ctx)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (tbl *txnTable) stats(ctx context.Context) (*pb.StatsInfo, error) {
	partitionState, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	e := tbl.db.getEng()
	approxObjectNum := int64(partitionState.ApproxDataObjectsNum())
	if approxObjectNum == 0 {
		// There are no objects flushed yet.
		return nil, nil
	}

	stats := plan2.NewStatsInfo()
	req := newUpdateStatsRequest(
		tbl.tableDef,
		partitionState,
		e.fs,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		approxObjectNum,
		stats,
	)
	if err := UpdateStats(ctx, req, nil); err != nil {
		logutil.Errorf("failed to init stats info for table %d", tbl.tableId)
		return nil, err
	}
	return stats, nil
}

func (tbl *txnTable) Rows(ctx context.Context) (uint64, error) {
	var rows uint64
	deletes := make(map[types.Rowid]struct{})

	rows += tbl.getUncommittedRows(deletes)

	v, err := tbl.getCommittedRows(
		ctx,
		deletes,
	)
	if err != nil {
		return 0, err
	}

	return v + rows, nil
}

func (tbl *txnTable) Size(ctx context.Context, columnName string) (uint64, error) {
	ts := types.TimestampToTS(tbl.db.op.SnapshotTS())
	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return 0, err
	}

	szInPart := uint64(0)
	neededCols := make(map[string]*plan.ColDef)
	cols := tbl.GetTableDef(ctx).Cols
	found := false

	for i := range cols {
		if columnName == AllColumns || cols[i].Name == columnName {
			neededCols[cols[i].Name] = cols[i]
			found = true
		}
	}

	if !found {
		return 0, moerr.NewInvalidInputf(ctx, "bad input column name %v", columnName)
	}

	deletes := make(map[types.Rowid]struct{})
	tbl.getTxn().ForEachTableWrites(
		tbl.db.databaseId,
		tbl.tableId,
		tbl.getTxn().GetSnapshotWriteOffset(),
		func(entry Entry) {
			if entry.typ == INSERT {
				for i, s := range entry.bat.Attrs {
					if _, ok := neededCols[s]; ok {
						szInPart += uint64(entry.bat.Vecs[i].Size())
					}
				}
			} else {
				if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
					vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
					for _, v := range vs {
						deletes[v] = struct{}{}
					}
				}
			}
		})

	iter := part.NewRowsIter(ts, nil, false)
	defer func() { _ = iter.Close() }()
	for iter.Next() {
		entry := iter.Entry()
		if _, ok := deletes[entry.RowID]; ok {
			continue
		}

		for i, s := range entry.Batch.Attrs {
			if _, ok := neededCols[s]; ok {
				szInPart += uint64(entry.Batch.Vecs[i].Size() / entry.Batch.Vecs[i].Length())
			}
		}
	}

	s, _ := tbl.Stats(ctx, true)
	if s == nil {
		return szInPart, nil
	}
	if columnName == AllColumns {
		var ret uint64
		for _, z := range s.SizeMap {
			ret += z
		}
		return ret + szInPart, nil
	}
	sz, ok := s.SizeMap[columnName]
	if !ok {
		return 0, moerr.NewInvalidInputf(ctx, "bad input column name %v", columnName)
	}
	return sz + szInPart, nil
}

func ForeachVisibleDataObject(
	state *logtailreplay.PartitionState,
	ts types.TS,
	fn func(obj logtailreplay.ObjectEntry) error,
	executor ConcurrentExecutor,
) (err error) {
	iter, err := state.NewObjectsIter(ts, true, false)
	if err != nil {
		return err
	}
	defer iter.Close()
	var wg sync.WaitGroup
	for iter.Next() {
		entry := iter.Entry()
		if executor != nil {
			wg.Add(1)
			executor.AppendTask(func() error {
				defer wg.Done()
				return fn(entry)
			})
		} else {
			if err = fn(entry); err != nil {
				break
			}
		}
	}
	if executor != nil {
		wg.Wait()
	}
	return
}

// not accurate!  only used by stats
func (tbl *txnTable) ApproxObjectsNum(ctx context.Context) int {
	part, err := tbl.getPartitionState(ctx)
	if err != nil {
		return 0
	}
	return part.ApproxDataObjectsNum()
}

func (tbl *txnTable) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	var (
		err  error
		part *logtailreplay.PartitionState
	)
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return nil, nil, err
	}

	var inited bool
	cols := tbl.GetTableDef(ctx).GetCols()
	dataLength := len(cols) - 1

	tableVal := make([][2]any, dataLength)
	tableTypes := make([]uint8, dataLength)
	zms := make([]objectio.ZoneMap, dataLength)

	var meta objectio.ObjectDataMeta
	var objMeta objectio.ObjectMeta
	fs, err := fileservice.Get[fileservice.FileService](
		tbl.getTxn().proc.Base.FileService,
		defines.SharedFileServiceName)
	if err != nil {
		return nil, nil, err
	}
	var updateMu sync.Mutex
	onObjFn := func(obj logtailreplay.ObjectEntry) error {
		var err error
		location := obj.Location()
		if objMeta, err = objectio.FastLoadObjectMeta(ctx, &location, false, fs); err != nil {
			return err
		}
		updateMu.Lock()
		defer updateMu.Unlock()
		meta = objMeta.MustDataMeta()
		if inited {
			for idx := range zms {
				zm := meta.MustGetColumn(uint16(cols[idx].Seqnum)).ZoneMap()
				if !zm.IsInited() {
					continue
				}
				index.UpdateZM(zms[idx], zm.GetMaxBuf())
				index.UpdateZM(zms[idx], zm.GetMinBuf())
			}
		} else {
			for idx := range zms {
				zms[idx] = meta.MustGetColumn(uint16(cols[idx].Seqnum)).ZoneMap()
				tableTypes[idx] = uint8(cols[idx].Typ.Id)
			}
			inited = true
		}

		return nil
	}

	if err = ForeachVisibleDataObject(
		part,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		onObjFn,
		nil,
	); err != nil {
		return nil, nil, err
	}

	if !inited {
		return nil, nil, moerr.NewInvalidInputNoCtx("table meta is nil")
	}

	for idx, zm := range zms {
		tableVal[idx] = [2]any{zm.GetMin(), zm.GetMax()}
	}

	return tableVal, tableTypes, nil
}

func (tbl *txnTable) GetColumMetadataScanInfo(ctx context.Context, name string) ([]*plan.MetadataScanInfo, error) {
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	cols := tbl.GetTableDef(ctx).GetCols()
	found := false
	n := 0
	for _, c := range cols {
		// TODO: We can keep hidden column but need a better way to know
		// whether it has the colmeta or not
		if !c.Hidden && (c.Name == name || name == AllColumns) {
			n++
			found = true
		}
	}
	if !found {
		return nil, moerr.NewInvalidInputf(ctx, "bad input column name %v", name)
	}

	needCols := make([]*plan.ColDef, 0, n)
	for _, c := range cols {
		if !c.Hidden && (c.Name == name || name == AllColumns) {
			needCols = append(needCols, c)
		}
	}

	fs, err := fileservice.Get[fileservice.FileService](
		tbl.getTxn().proc.Base.FileService,
		defines.SharedFileServiceName)
	if err != nil {
		return nil, err
	}
	infoList := make([]*plan.MetadataScanInfo, 0, state.ApproxDataObjectsNum())
	var updateMu sync.Mutex
	onObjFn := func(obj logtailreplay.ObjectEntry) error {
		createTs, err := obj.CreateTime.Marshal()
		if err != nil {
			return err
		}
		deleteTs, err := obj.DeleteTime.Marshal()
		if err != nil {
			return err
		}

		location := obj.Location()
		objName := location.Name().String()
		if name == AllColumns && obj.StatsValid() {
			// no need to load object meta
			for _, col := range needCols {
				infoList = append(infoList, &plan.MetadataScanInfo{
					ColName:      col.Name,
					IsHidden:     col.Hidden,
					ObjectName:   objName,
					ObjLoc:       location,
					CreateTs:     createTs,
					DeleteTs:     deleteTs,
					RowCnt:       int64(obj.Rows()),
					ZoneMap:      objectio.EmptyZm[:],
					CompressSize: int64(obj.ObjectStats.Size()),
					OriginSize:   int64(obj.ObjectStats.OriginSize()),
				})
			}
			return nil
		}

		objMeta, err := objectio.FastLoadObjectMeta(ctx, &location, false, fs)
		if err != nil {
			return err
		}
		updateMu.Lock()
		defer updateMu.Unlock()
		meta := objMeta.MustDataMeta()
		rowCnt := int64(meta.BlockHeader().Rows())

		for _, col := range needCols {
			colMeta := meta.MustGetColumn(uint16(col.Seqnum))
			infoList = append(infoList, &plan.MetadataScanInfo{
				ColName:      col.Name,
				IsHidden:     col.Hidden,
				ObjectName:   objName,
				ObjLoc:       location,
				CreateTs:     createTs,
				DeleteTs:     deleteTs,
				RowCnt:       rowCnt,
				NullCnt:      int64(colMeta.NullCnt()),
				CompressSize: int64(colMeta.Location().Length()),
				OriginSize:   int64(colMeta.Location().OriginSize()),
				ZoneMap:      colMeta.ZoneMap(),
			})
		}
		return nil
	}

	if err = ForeachVisibleDataObject(
		state,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		onObjFn,
		nil,
	); err != nil {
		return nil, err
	}

	var logStr string
	for i, col := range needCols {
		if i > 0 {
			logStr += ", "
		}
		logStr += col.GetName()
	}
	logutil.Infof("cols in GetColumMetadataScanInfo: %s, result len: %d", logStr, len(infoList))

	return infoList, nil
}

func (tbl *txnTable) GetDirtyPersistedBlks(state *logtailreplay.PartitionState) []types.Blockid {
	tbl.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer tbl.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	dirtyBlks := make([]types.Blockid, 0)
	for blk := range tbl.getTxn().blockId_tn_delete_metaLoc_batch.data {
		if !state.BlockPersisted(blk) {
			continue
		}
		dirtyBlks = append(dirtyBlks, blk)
	}
	return dirtyBlks
}

func (tbl *txnTable) LoadDeletesForBlock(bid types.Blockid, offsets *[]int64) (err error) {
	tbl.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer tbl.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	bats, ok := tbl.getTxn().blockId_tn_delete_metaLoc_batch.data[bid]
	if !ok {
		return nil
	}
	for _, bat := range bats {
		vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
		for i := range vs {
			location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
			if err != nil {
				return err
			}
			rowIdBat, release, err := blockio.LoadColumns(
				tbl.getTxn().proc.Ctx,
				[]uint16{0},
				nil,
				tbl.getTxn().engine.fs,
				location,
				tbl.getTxn().proc.GetMPool(),
				fileservice.Policy(0))
			if err != nil {
				return err
			}
			defer release()
			rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
			for _, rowId := range rowIds {
				_, offset := rowId.Decode()
				*offsets = append(*offsets, int64(offset))
			}
		}
	}
	return nil
}

// LoadDeletesForMemBlocksIn loads deletes for memory blocks whose data resides in PartitionState.rows
func (tbl *txnTable) LoadDeletesForMemBlocksIn(
	state *logtailreplay.PartitionState,
	deletesRowId map[types.Rowid]uint8) error {

	tbl.getTxn().blockId_tn_delete_metaLoc_batch.RLock()
	defer tbl.getTxn().blockId_tn_delete_metaLoc_batch.RUnlock()

	for blk, bats := range tbl.getTxn().blockId_tn_delete_metaLoc_batch.data {
		//if blk is in partitionState.blks, it means that blk is persisted.
		if state.BlockPersisted(blk) {
			continue
		}
		for _, bat := range bats {
			vs, area := vector.MustVarlenaRawData(bat.GetVector(0))
			for i := range vs {
				location, err := blockio.EncodeLocationFromString(vs[i].UnsafeGetString(area))
				if err != nil {
					return err
				}
				rowIdBat, release, err := blockio.LoadColumns(
					tbl.getTxn().proc.Ctx,
					[]uint16{0},
					nil,
					tbl.getTxn().engine.fs,
					location,
					tbl.getTxn().proc.GetMPool(),
					fileservice.Policy(0))
				if err != nil {
					return err
				}
				defer release()
				rowIds := vector.MustFixedCol[types.Rowid](rowIdBat.GetVector(0))
				for _, rowId := range rowIds {
					if deletesRowId != nil {
						deletesRowId[rowId] = 0
					}
				}
			}
		}

	}
	return nil
}

func (tbl *txnTable) GetEngineType() engine.EngineType {
	return engine.Disttae
}

func (tbl *txnTable) resetSnapshot() {
	tbl._partState.Store(nil)
}

func (tbl *txnTable) CollectTombstones(
	ctx context.Context, txnOffset int,
) (engine.Tombstoner, error) {
	tombstone := NewEmptyTombstoneData()

	offset := txnOffset
	if tbl.db.op.IsSnapOp() {
		offset = tbl.getTxn().GetSnapshotWriteOffset()
	}

	//collect in memory

	//collect uncommitted in-memory tombstones from txn.writes
	tbl.getTxn().ForEachTableWrites(tbl.db.databaseId, tbl.tableId,
		offset, func(entry Entry) {
			if entry.typ == INSERT {
				return
			}
			//entry.typ == DELETE
			if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
				/*
					CASE:
					create table t1(a int);
					begin;
					truncate t1; //txnDatabase.Truncate will DELETE mo_tables
					show tables; // t1 must be shown
				*/
				//if entry.IsGeneratedByTruncate() {
				//	return
				//}
				//deletes in txn.Write maybe comes from PartitionState.Rows ,
				// PartitionReader need to skip them.
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				tombstone.rowids = append(tombstone.rowids, vs...)
			}
		})

	//collect uncommitted in-memory tombstones belongs to blocks persisted by CN writing S3
	tbl.getTxn().deletedBlocks.getDeletedRowIDs(&tombstone.rowids)

	//collect committed in-memory tombstones from partition state.
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}
	{
		ts := tbl.db.op.SnapshotTS()
		iter := state.NewRowsIter(types.TimestampToTS(ts), nil, true)
		for iter.Next() {
			entry := iter.Entry()
			//bid, o := entry.RowID.Decode()
			tombstone.rowids = append(tombstone.rowids, entry.RowID)
		}
		iter.Close()
	}

	//collect uncommitted persisted tombstones.
	if err := tbl.getTxn().getUncommittedS3Tombstone(&tombstone.files); err != nil {
		return nil, err
	}

	tombstone.SortInMemory()
	//collect committed persisted tombstones from partition state.
	//state.GetTombstoneDeltaLocs(tombstone.blk2CommitLoc)
	snapshot := types.TimestampToTS(tbl.db.op.Txn().SnapshotTS)
	err = state.CollectTombstoneObjects(snapshot, &tombstone.files)
	return tombstone, err
}

// Ranges returns all unmodified blocks from the table.
// Parameters:
//   - ctx: Context used to control the lifecycle of the request.
//   - exprs: A slice of expressions used to filter data.
//   - txnOffset: Transaction offset used to specify the starting position for reading data.
func (tbl *txnTable) Ranges(
	ctx context.Context,
	exprs []*plan.Expr,
	txnOffset int,
) (data engine.RelData, err error) {
	return tbl.doRanges(
		ctx,
		exprs,
		tbl.collectUnCommittedObjects(txnOffset),
	)
}

func (tbl *txnTable) doRanges(
	ctx context.Context,
	exprs []*plan.Expr,
	uncommittedObjects []objectio.ObjectStats,
) (data engine.RelData, err error) {
	sid := tbl.proc.Load().GetService()
	start := time.Now()
	seq := tbl.db.op.NextSequence()

	var blocks objectio.BlockInfoSlice

	trace.GetService(sid).AddTxnDurationAction(
		tbl.db.op,
		client.RangesEvent,
		seq,
		tbl.tableId,
		0,
		nil)

	defer func() {
		cost := time.Since(start)

		var (
			step, slowStep uint64
		)

		rangesLen := blocks.Len()
		if rangesLen < 5 {
			step = uint64(1)
		} else if rangesLen < 10 {
			step = uint64(5)
		} else if rangesLen < 20 {
			step = uint64(10)
		} else {
			slowStep = uint64(1)
		}
		tbl.enableLogFilterExpr.Store(false)
		if traceFilterExprInterval.Add(step) >= 500000 {
			traceFilterExprInterval.Store(0)
			tbl.enableLogFilterExpr.Store(true)
		}
		if traceFilterExprInterval2.Add(slowStep) >= 20 {
			traceFilterExprInterval2.Store(0)
			tbl.enableLogFilterExpr.Store(true)
		}

		if rangesLen >= 50 {
			tbl.enableLogFilterExpr.Store(true)
		}

		if tbl.enableLogFilterExpr.Load() {
			logutil.Info(
				"TXN-FILTER-RANGE-LOG",
				zap.String("name", tbl.tableDef.Name),
				zap.String("exprs", plan2.FormatExprs(exprs)),
				zap.Int("ranges-len", blocks.Len()),
				zap.Uint64("tbl-id", tbl.tableId),
				zap.String("txn", tbl.db.op.Txn().DebugString()),
			)
		}

		trace.GetService(sid).AddTxnAction(
			tbl.db.op,
			client.RangesEvent,
			seq,
			tbl.tableId,
			int64(blocks.Len()),
			"blocks",
			err)

		trace.GetService(sid).AddTxnDurationAction(
			tbl.db.op,
			client.RangesEvent,
			seq,
			tbl.tableId,
			cost,
			err)

		v2.TxnTableRangeDurationHistogram.Observe(cost.Seconds())
		if err != nil {
			logutil.Errorf("txn: %s, error: %v", tbl.db.op.Txn().DebugString(), err)
		}
	}()

	// get the table's snapshot
	var part *logtailreplay.PartitionState
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return
	}

	blocks.AppendBlockInfo(objectio.EmptyBlockInfo)

	if err = tbl.rangesOnePart(
		ctx,
		part,
		tbl.GetTableDef(ctx),
		exprs,
		&blocks,
		tbl.proc.Load(),
		uncommittedObjects,
	); err != nil {
		return
	}

	data = &blockListRelData{
		blklist: blocks,
	}

	return
}

// txn can read :
//  1. snapshot data:
//      1>. committed block data resides in S3.
//      2>. partition state data resides in memory. read by partitionReader.

//      deletes(rowids) for committed block exist in the following four places:
//      1. in delta location formed by TN writing S3. read by blockReader.
//      2. in CN's partition state, read by partitionReader.
//  	3. in txn's workspace(txn.writes) being deleted by txn, read by partitionReader.
//  	4. in delta location being deleted through CN writing S3, read by blockMergeReader.

//  2. data in txn's workspace:
//     1>.Raw batch data resides in txn.writes,read by partitionReader.
//     2>.CN blocks resides in S3, read by blockReader.

var slowPathCounter atomic.Int64

// rangesOnePart collect blocks which are visible to this txn,
// include committed blocks and uncommitted blocks by CN writing S3.
// notice that only clean blocks can be distributed into remote CNs.
func (tbl *txnTable) rangesOnePart(
	ctx context.Context,
	state *logtailreplay.PartitionState, // snapshot state of this transaction
	tableDef *plan.TableDef, // table definition (schema)
	exprs []*plan.Expr, // filter expression
	outBlocks *objectio.BlockInfoSlice, // output marshaled block list after filtering
	proc *process.Process, // process of this transaction
	uncommittedObjects []objectio.ObjectStats,
) (err error) {
	var done bool

	if done, err = TryFastFilterBlocks(
		ctx,
		tbl,
		tbl.db.op.SnapshotTS(),
		tbl.tableDef,
		exprs,
		state,
		nil,
		uncommittedObjects,
		outBlocks,
		tbl.getTxn().engine.fs,
		tbl.proc.Load(),
	); err != nil {
		return err
	} else if done {
		return nil
	}

	if slowPathCounter.Add(1) >= 1000 {
		slowPathCounter.Store(0)
		logutil.Info(
			"SLOW-RANGES:",
			zap.String("table", tbl.tableDef.Name),
			zap.String("exprs", plan2.FormatExprs(exprs)),
		)
	}

	// for dynamic parameter, substitute param ref and const fold cast expression here to improve performance
	newExprs, err := plan2.ConstandFoldList(exprs, tbl.proc.Load(), true)
	if err == nil {
		exprs = newExprs
	}

	var (
		objMeta    objectio.ObjectMeta
		zms        []objectio.ZoneMap
		vecs       []*vector.Vector
		skipObj    bool
		auxIdCnt   int32
		loadObjCnt uint32
		s3BlkCnt   uint32
	)

	defer func() {
		for i := range vecs {
			if vecs[i] != nil {
				vecs[i].Free(proc.Mp())
			}
		}
	}()

	// check if expr is monotonic, if not, we can skip evaluating expr for each block
	for _, expr := range exprs {
		auxIdCnt += plan2.AssignAuxIdForExpr(expr, auxIdCnt)
	}

	columnMap := make(map[int]int)
	if auxIdCnt > 0 {
		zms = make([]objectio.ZoneMap, auxIdCnt)
		vecs = make([]*vector.Vector, auxIdCnt)
		plan2.GetColumnMapByExprs(exprs, tableDef, columnMap)
	}

	errCtx := errutil.ContextWithNoReport(ctx, true)

	if err = ForeachSnapshotObjects(
		tbl.db.op.SnapshotTS(),
		func(obj logtailreplay.ObjectInfo, isCommitted bool) (err2 error) {
			var meta objectio.ObjectDataMeta
			skipObj = false

			s3BlkCnt += obj.BlkCnt()
			if auxIdCnt > 0 {
				location := obj.ObjectLocation()
				loadObjCnt++
				if objMeta, err2 = objectio.FastLoadObjectMeta(
					errCtx, &location, false, tbl.getTxn().engine.fs,
				); err2 != nil {
					return
				}

				meta = objMeta.MustDataMeta()
				// here we only eval expr on the object meta if it has more than one blocks
				if meta.BlockCount() > 2 {
					for _, expr := range exprs {
						if !colexec.EvaluateFilterByZoneMap(
							errCtx, proc, expr, meta, columnMap, zms, vecs,
						) {
							skipObj = true
							break
						}
					}
				}
			}
			if skipObj {
				return
			}

			if obj.Rows() == 0 && meta.IsEmpty() {
				loadObjCnt++
				location := obj.ObjectLocation()
				if objMeta, err2 = objectio.FastLoadObjectMeta(
					errCtx, &location, false, tbl.getTxn().engine.fs,
				); err2 != nil {
					return
				}
				meta = objMeta.MustDataMeta()
			}

			ForeachBlkInObjStatsList(true, meta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				skipBlk := false

				if auxIdCnt > 0 {
					// eval filter expr on the block
					for _, expr := range exprs {
						if !colexec.EvaluateFilterByZoneMap(errCtx, proc, expr, blkMeta, columnMap, zms, vecs) {
							skipBlk = true
							break
						}
					}

					// if the block is not needed, skip it
					if skipBlk {
						return true
					}
				}

				blk.Sorted = obj.Sorted
				blk.Appendable = obj.Appendable

				outBlocks.AppendBlockInfo(blk)

				return true

			},
				obj.ObjectStats,
			)
			return
		},
		state,
		nil,
		uncommittedObjects...,
	); err != nil {
		return
	}

	bhit, btotal := outBlocks.Len()-1, int(s3BlkCnt)
	v2.TaskSelBlockTotal.Add(float64(btotal))
	v2.TaskSelBlockHit.Add(float64(btotal - bhit))
	blockio.RecordBlockSelectivity(proc.GetService(), bhit, btotal)
	if btotal > 0 {
		v2.TxnRangesSlowPathLoadObjCntHistogram.Observe(float64(loadObjCnt))
		v2.TxnRangesSlowPathSelectedBlockCntHistogram.Observe(float64(bhit))
		v2.TxnRangesSlowPathBlockSelectivityHistogram.Observe(float64(bhit) / float64(btotal))
	}
	return
}

// Parameters:
//   - txnOffset: Transaction writes offset used to specify the starting position for reading data.
//   - fromSnapshot: Boolean indicating if the data is from a snapshot.
func (tbl *txnTable) collectUnCommittedObjects(txnOffset int) []objectio.ObjectStats {
	var unCommittedObjects []objectio.ObjectStats

	if tbl.db.op.IsSnapOp() {
		txnOffset = tbl.getTxn().GetSnapshotWriteOffset()
	}
	tbl.getTxn().ForEachTableWrites(
		tbl.db.databaseId,
		tbl.tableId,
		txnOffset,
		func(entry Entry) {
			stats := objectio.ObjectStats{}
			if entry.bat == nil || entry.bat.IsEmpty() {
				return
			}
			if entry.typ != INSERT ||
				len(entry.bat.Attrs) < 2 ||
				entry.bat.Attrs[1] != catalog.ObjectMeta_ObjectStats {
				return
			}
			for i := 0; i < entry.bat.Vecs[1].Length(); i++ {
				stats.UnMarshal(entry.bat.Vecs[1].GetBytesAt(i))
				unCommittedObjects = append(unCommittedObjects, stats)
			}
		})

	return unCommittedObjects
}

//func (tbl *txnTable) collectDirtyBlocks(
//	state *logtailreplay.PartitionState,
//	uncommittedObjects []objectio.ObjectStats,
//	txnOffset int, // Transaction writes offset used to specify the starting position for reading data.
//) map[types.Blockid]struct{} {
//	dirtyBlks := make(map[types.Blockid]struct{})
//	//collect partitionState.dirtyBlocks which may be invisible to this txn into dirtyBlks.
//	{
//		iter := state.NewDirtyBlocksIter()
//		for iter.Next() {
//			entry := iter.Entry()
//			//lazy load deletes for block.
//			dirtyBlks[entry] = struct{}{}
//		}
//		iter.Close()
//
//	}
//
//	//only collect dirty blocks in PartitionState.blocks into dirtyBlks.
//	for _, bid := range tbl.GetDirtyPersistedBlks(state) {
//		dirtyBlks[bid] = struct{}{}
//	}
//
//	if tbl.getTxn().hasDeletesOnUncommitedObject() {
//		ForeachBlkInObjStatsList(true, nil, func(blk objectio.BlockInfo, _ objectio.BlockObject) bool {
//			if tbl.getTxn().hasUncommittedDeletesOnBlock(&blk.BlockID) {
//				dirtyBlks[blk.BlockID] = struct{}{}
//			}
//			return true
//		}, uncommittedObjects...)
//	}
//
//	if tbl.db.op.IsSnapOp() {
//		txnOffset = tbl.getTxn().GetSnapshotWriteOffset()
//	}
//
//	tbl.getTxn().ForEachTableWrites(
//		tbl.db.databaseId,
//		tbl.tableId,
//		txnOffset,
//		func(entry Entry) {
//			// the CN workspace can only handle `INSERT` and `DELETE` operations. Other operations will be skipped,
//			// TODO Adjustments will be made here in the future
//			if entry.typ == DELETE || entry.typ == DELETE_TXN {
//				if entry.IsGeneratedByTruncate() {
//					return
//				}
//				//deletes in tbl.writes maybe comes from PartitionState.rows or PartitionState.blocks.
//				if entry.fileName == "" &&
//					entry.tableId != catalog.MO_DATABASE_ID && entry.tableId != catalog.MO_TABLES_ID && entry.tableId != catalog.MO_COLUMNS_ID {
//					vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
//					for _, v := range vs {
//						id, _ := v.Decode()
//						dirtyBlks[id] = struct{}{}
//					}
//				}
//			}
//		})
//
//	return dirtyBlks
//}

// the return defs has no rowid column
func (tbl *txnTable) TableDefs(ctx context.Context) ([]engine.TableDef, error) {
	//return tbl.defs, nil
	// I don't understand why the logic now is not to get all the tableDef. Don't understand.
	// copy from tae's logic
	defs := make([]engine.TableDef, 0, len(tbl.defs))
	defs = append(defs, &engine.VersionDef{Version: tbl.version})
	if tbl.comment != "" {
		commentDef := new(engine.CommentDef)
		commentDef.Comment = tbl.comment
		defs = append(defs, commentDef)
	}
	if tbl.partitioned > 0 || tbl.partition != "" {
		partitionDef := new(engine.PartitionDef)
		partitionDef.Partitioned = tbl.partitioned
		partitionDef.Partition = tbl.partition
		defs = append(defs, partitionDef)
	}

	if tbl.viewdef != "" {
		viewDef := new(engine.ViewDef)
		viewDef.View = tbl.viewdef
		defs = append(defs, viewDef)
	}
	if len(tbl.constraint) > 0 {
		c := &engine.ConstraintDef{}
		err := c.UnmarshalBinary(tbl.constraint)
		if err != nil {
			return nil, err
		}
		defs = append(defs, c)
	}
	for i, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Name != catalog.Row_ID {
				defs = append(defs, tbl.defs[i])
			}
		}
	}
	pro := new(engine.PropertiesDef)
	pro.Properties = append(pro.Properties, engine.Property{
		Key:   catalog.SystemRelAttr_Kind,
		Value: string(tbl.relKind),
	})
	if tbl.createSql != "" {
		pro.Properties = append(pro.Properties, engine.Property{
			Key:   catalog.SystemRelAttr_CreateSQL,
			Value: tbl.createSql,
		})
	}
	defs = append(defs, pro)
	return defs, nil
}

func (tbl *txnTable) GetTableDef(ctx context.Context) *plan.TableDef {
	if tbl.tableDef == nil {
		var clusterByDef *plan.ClusterByDef
		var cols []*plan.ColDef
		var defs []*plan.TableDef_DefType
		var properties []*plan.Property
		var TableType string
		var Createsql string
		var partitionInfo *plan.PartitionByDef
		var viewSql *plan.ViewDef
		var foreignKeys []*plan.ForeignKeyDef
		var primarykey *plan.PrimaryKeyDef
		var indexes []*plan.IndexDef
		var refChildTbls []uint64
		var hasRowId bool

		i := int32(0)
		name2index := make(map[string]int32)
		for _, def := range tbl.defs {
			if attr, ok := def.(*engine.AttributeDef); ok {
				name := strings.ToLower(attr.Attr.Name)
				name2index[name] = i
				cols = append(cols, &plan.ColDef{
					ColId:      attr.Attr.ID,
					Name:       name,
					OriginName: attr.Attr.Name,
					Typ: plan.Type{
						Id:          int32(attr.Attr.Type.Oid),
						Width:       attr.Attr.Type.Width,
						Scale:       attr.Attr.Type.Scale,
						AutoIncr:    attr.Attr.AutoIncrement,
						Table:       tbl.tableName,
						NotNullable: attr.Attr.Default != nil && !attr.Attr.Default.NullAbility,
						Enumvalues:  attr.Attr.EnumVlaues,
					},
					Primary:   attr.Attr.Primary,
					Default:   attr.Attr.Default,
					OnUpdate:  attr.Attr.OnUpdate,
					Comment:   attr.Attr.Comment,
					ClusterBy: attr.Attr.ClusterBy,
					Hidden:    attr.Attr.IsHidden,
					Seqnum:    uint32(attr.Attr.Seqnum),
				})
				if attr.Attr.ClusterBy {
					clusterByDef = &plan.ClusterByDef{
						Name: name,
					}
				}
				if attr.Attr.Name == catalog.Row_ID {
					hasRowId = true
				}
				i++
			}
		}

		if tbl.comment != "" {
			properties = append(properties, &plan.Property{
				Key:   catalog.SystemRelAttr_Comment,
				Value: tbl.comment,
			})
		}

		if tbl.partitioned > 0 {
			p := &plan.PartitionByDef{}
			err := p.UnMarshalPartitionInfo(([]byte)(tbl.partition))
			if err != nil {
				//panic(fmt.Sprintf("cannot unmarshal partition metadata information: %s", err))
				return nil
			}
			partitionInfo = p
		}

		if tbl.viewdef != "" {
			viewSql = &plan.ViewDef{
				View: tbl.viewdef,
			}
		}

		if len(tbl.constraint) > 0 {
			c := &engine.ConstraintDef{}
			err := c.UnmarshalBinary(tbl.constraint)
			if err != nil {
				//panic(fmt.Sprintf("cannot unmarshal table constraint information: %s", err))
				return nil
			}
			for _, ct := range c.Cts {
				switch k := ct.(type) {
				case *engine.IndexDef:
					indexes = k.Indexes
				case *engine.ForeignKeyDef:
					foreignKeys = k.Fkeys
				case *engine.RefChildTableDef:
					refChildTbls = k.Tables
				case *engine.PrimaryKeyDef:
					primarykey = k.Pkey
				case *engine.StreamConfigsDef:
					properties = append(properties, k.Configs...)
				}
			}
		}

		properties = append(properties, &plan.Property{
			Key:   catalog.SystemRelAttr_Kind,
			Value: tbl.relKind,
		})
		TableType = tbl.relKind

		if tbl.createSql != "" {
			properties = append(properties, &plan.Property{
				Key:   catalog.SystemRelAttr_CreateSQL,
				Value: tbl.createSql,
			})
			Createsql = tbl.createSql
		}

		if len(properties) > 0 {
			defs = append(defs, &plan.TableDef_DefType{
				Def: &plan.TableDef_DefType_Properties{
					Properties: &plan.PropertiesDef{
						Properties: properties,
					},
				},
			})
		}

		if primarykey != nil && primarykey.PkeyColName == catalog.CPrimaryKeyColName {
			primarykey.CompPkeyCol = plan2.GetColDefFromTable(cols, catalog.CPrimaryKeyColName)
		}
		if clusterByDef != nil && util.JudgeIsCompositeClusterByColumn(clusterByDef.Name) {
			clusterByDef.CompCbkeyCol = plan2.GetColDefFromTable(cols, clusterByDef.Name)
		}
		if !hasRowId {
			rowIdCol := plan2.MakeRowIdColDef()
			cols = append(cols, rowIdCol)
		}

		tbl.tableDef = &plan.TableDef{
			TblId:         tbl.tableId,
			Name:          tbl.tableName,
			DbName:        tbl.db.databaseName,
			Cols:          cols,
			Name2ColIndex: name2index,
			Defs:          defs,
			TableType:     TableType,
			Createsql:     Createsql,
			Pkey:          primarykey,
			ViewSql:       viewSql,
			Partition:     partitionInfo,
			Fkeys:         foreignKeys,
			RefChildTbls:  refChildTbls,
			ClusterBy:     clusterByDef,
			Indexes:       indexes,
			Version:       tbl.version,
		}
	}
	return tbl.tableDef
}

func (tbl *txnTable) CopyTableDef(ctx context.Context) *plan.TableDef {
	tbl.GetTableDef(ctx)
	return plan2.DeepCopyTableDef(tbl.tableDef, true)
}

func (tbl *txnTable) UpdateConstraint(ctx context.Context, c *engine.ConstraintDef) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("cannot update table constraint in snapshot operation")
	}
	ct, err := c.MarshalBinary()
	if err != nil {
		return err
	}
	req := api.NewUpdateConstraintReq(tbl.db.databaseId, tbl.tableId, string(ct))

	return tbl.AlterTable(ctx, c, []*api.AlterTableReq{req})
}

// Note:
//
// 1. It is insufficeint to use txn.CreateTable to check, which contains newly-created table or newly-altered table in txn.
// Imagine altering a normal table twice in a single txn,
// and then the second alter will be treated as an operation on a newly-created table if txn.CreateTable is used.
//
// 2. This check depends on replaying all catalog cache when cn starts.
func (tbl *txnTable) isCreatedInTxn() bool {
	if tbl.db.op.IsSnapOp() {
		// if the operation is snapshot read, isCreatedInTxn can not be called by AlterTable
		// So if the snapshot read want to subcribe logtail tail, let it go ahead.
		return false
	}
	idAckedbyTN := tbl.db.getEng().GetLatestCatalogCache().
		GetTableByIdAndTime(tbl.accountId, tbl.db.databaseId, tbl.tableId, tbl.db.op.SnapshotTS())
	return idAckedbyTN == nil
}

func (tbl *txnTable) AlterTable(ctx context.Context, c *engine.ConstraintDef, reqs []*api.AlterTableReq) error {
	// AlterTale Inplace do not touch columns, we don't use NextSeqNum at the moment.
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("cannot alter table in snapshot operation")
	}

	var err error
	var checkCstr []byte
	oldTableName := tbl.tableName
	olddefs := tbl.defs
	oldPart := tbl.partitioned
	oldPartInfo := tbl.partition
	oldComment := tbl.comment
	oldConstraint := tbl.constraint
	// The fact that the tableDef brought by alter requests can appended to the tail of original defs presupposes:
	// 1. late arriving tableDef will overwrite the existing tableDef
	// 2. any TableDef about columns, like AttritebuteDef, PrimaryKeyDef, or CluterbyDef do not change, ensuring genColumnsFromDefs works well
	appendDef := make([]engine.TableDef, 0)

	txn := tbl.getTxn()
	restore := func() {
		for _, req := range reqs {
			switch req.GetKind() {
			case api.AlterKind_AddPartition:
				tbl.partitioned = oldPart
				tbl.partition = oldPartInfo
			case api.AlterKind_UpdateComment:
				tbl.comment = oldComment
			case api.AlterKind_UpdateConstraint:
				tbl.constraint = oldConstraint
			case api.AlterKind_RenameTable:
				tbl.tableName = oldTableName
			}
		}
		tbl.defs = olddefs
		tbl.tableDef = nil
		tbl.GetTableDef(ctx)
	}
	txn.Lock()
	txn.restoreTxnTableFunc = append(txn.restoreTxnTableFunc, restore)
	txn.Unlock()

	// update tbl properties and reconstruct supplement TableDef
	for _, req := range reqs {
		switch req.GetKind() {
		case api.AlterKind_AddPartition:
			tbl.partitioned = 1
			info, err := req.GetAddPartition().GetPartitionDef().MarshalPartitionInfo()
			if err != nil {
				return err
			}
			tbl.partition = string(info)
			appendDef = append(appendDef, &engine.PartitionDef{
				Partitioned: 1,
				Partition:   tbl.partition,
			})
		case api.AlterKind_UpdateComment:
			tbl.comment = req.GetUpdateComment().Comment
			appendDef = append(appendDef, &engine.CommentDef{Comment: tbl.comment})
		case api.AlterKind_UpdateConstraint:
			// do not modify, leave it to marshaling `c`
			checkCstr = req.GetUpdateCstr().Constraints
			if c == nil {
				panic("mismatch cstr AlterTable")
			}
			appendDef = append(appendDef, c)
		case api.AlterKind_RenameTable:
			tbl.tableName = req.GetRenameTable().NewName
		default:
			panic("not supported")
		}
	}

	if c != nil {
		if tbl.constraint, err = c.MarshalBinary(); err != nil {
			return err
		}
	}

	if len(checkCstr) > 0 && !bytes.Equal(tbl.constraint, checkCstr) {
		panic("not equal cstr")
	}

	// update TableDef
	tbl.defs = append(tbl.defs, appendDef...)
	tbl.tableDef = nil
	tbl.GetTableDef(ctx)

	// 0. check if the table is created in txn.
	// For a table created in txn, alter means to recreate table and put relating dml/alter batch behind the new create batch.
	// For a normal table, alter means sending Alter request to TN, no creating command, and no dropping command.
	createdInTxn := tbl.isCreatedInTxn()
	if !createdInTxn {
		tbl.version += 1
		// For normal Alter, send Alter request to TN
		reqPayload := make([][]byte, 0, len(reqs))
		for _, req := range reqs {
			payload, err := req.Marshal()
			if err != nil {
				return err
			}
			reqPayload = append(reqPayload, payload)
		}
		bat, err := catalog.GenTableAlterTuple(reqPayload, txn.proc.Mp())
		if err != nil {
			return err
		}
		if _, err = txn.WriteBatch(ALTER, "", tbl.accountId, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, bat, txn.tnStores[0]); err != nil {
			bat.Clean(txn.proc.Mp())
			return err
		}
	}

	//------------------------------------------------------------------------------------------------------------------
	// 1. delete old table metadata
	if _, err := tbl.db.deleteTable(ctx, oldTableName, true, !createdInTxn); err != nil {
		return err
	}

	//------------------------------------------------------------------------------------------------------------------
	// 2. insert new table metadata
	if err := tbl.db.createWithID(ctx, tbl.tableName, tbl.tableId, tbl.defs, !createdInTxn); err != nil {
		return err
	}

	if createdInTxn {
		// 3. adjust writes for the table
		txn.Lock()
		for i, n := 0, len(txn.writes); i < n; i++ {
			if cur := txn.writes[i]; cur.tableId == tbl.tableId && cur.bat != nil && cur.bat.RowCount() > 0 {
				if sels, exist := txn.batchSelectList[cur.bat]; exist && len(sels) == 0 {
					continue
				}
				txn.writes = append(txn.writes, txn.writes[i]) // copy by value
				transfered := &txn.writes[len(txn.writes)-1]
				transfered.tableName = tbl.tableName // in case renaming
				transfered.bat, err = cur.bat.Dup(txn.proc.Mp())
				if err != nil {
					return err
				}
				txn.batchSelectList[cur.bat] = []int64{}
			}
		}
		txn.Unlock()
	}

	return nil
}

func (tbl *txnTable) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("cannot rename table in snapshot operation")
	}
	req := &api.AlterTableReq{}
	if err := req.Unmarshal(constraint[0]); err != nil {
		return err
	}
	return tbl.AlterTable(ctx, nil, []*api.AlterTableReq{req})
}

func (tbl *txnTable) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	var attrs []*engine.Attribute
	for _, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			attrs = append(attrs, &attr.Attr)
		}
	}
	return attrs, nil
}

func (tbl *txnTable) GetPrimaryKeys(ctx context.Context) ([]*engine.Attribute, error) {
	attrs := make([]*engine.Attribute, 0, 1)
	for _, def := range tbl.defs {
		if attr, ok := def.(*engine.AttributeDef); ok {
			if attr.Attr.Primary {
				attrs = append(attrs, &attr.Attr)
			}
		}
	}
	return attrs, nil
}

func (tbl *txnTable) GetHideKeys(ctx context.Context) ([]*engine.Attribute, error) {
	attrs := make([]*engine.Attribute, 0, 1)
	attrs = append(attrs, &engine.Attribute{
		IsHidden: true,
		IsRowId:  true,
		Name:     catalog.Row_ID,
		Type:     types.New(types.T_Rowid, 0, 0),
		Primary:  true,
	})
	return attrs, nil
}

func (tbl *txnTable) Write(ctx context.Context, bat *batch.Batch) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("write operation is not allowed in snapshot transaction")
	}
	if bat == nil || bat.RowCount() == 0 {
		return nil
	}
	// for writing S3 Block
	if bat.Attrs[0] == catalog.BlockMeta_BlockInfo {
		tbl.getTxn().hasS3Op.Store(true)
		//bocks maybe come from different S3 object, here we just need to make sure fileName is not Nil.
		fileName := objectio.DecodeBlockInfo(bat.Vecs[0].GetBytesAt(0)).MetaLocation().Name().String()
		return tbl.getTxn().WriteFile(
			INSERT,
			tbl.accountId,
			tbl.db.databaseId,
			tbl.tableId,
			tbl.db.databaseName,
			tbl.tableName,
			fileName,
			bat,
			tbl.getTxn().tnStores[0])
	}
	ibat, err := util.CopyBatch(bat, tbl.getTxn().proc)
	if err != nil {
		return err
	}
	if _, err := tbl.getTxn().WriteBatch(
		INSERT,
		"",
		tbl.accountId,
		tbl.db.databaseId,
		tbl.tableId,
		tbl.db.databaseName,
		tbl.tableName,
		ibat,
		tbl.getTxn().tnStores[0],
	); err != nil {
		ibat.Clean(tbl.getTxn().proc.Mp())
		return err
	}
	return tbl.getTxn().dumpBatch(tbl.getTxn().GetSnapshotWriteOffset())
}

func (tbl *txnTable) Update(ctx context.Context, bat *batch.Batch) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("update operation is not allowed in snapshot transaction")
	}
	return nil
}

//	blkId(string)     deltaLoc(string)                   type(int)
//
// |-----------|-----------------------------------|----------------|
// |  blk_id   |   batch.Marshal(deltaLoc)         |  FlushDeltaLoc | TN Block
// |  blk_id   |   batch.Marshal(uint32 offset)    |  CNBlockOffset | CN Block
// |  blk_id   |   batch.Marshal(rowId)            |  RawRowIdBatch | TN Blcok
// |  blk_id   |   batch.Marshal(uint32 offset)    | RawBatchOffset | RawBatch (in txn workspace)
func (tbl *txnTable) EnhanceDelete(bat *batch.Batch, name string) error {
	blkId, typ_str := objectio.Str2Blockid(name[:len(name)-2]), string(name[len(name)-1])
	typ, err := strconv.ParseInt(typ_str, 10, 64)
	if err != nil {
		return err
	}
	switch typ {
	case deletion.FlushDeltaLoc:
		tbl.getTxn().hasS3Op.Store(true)
		location, err := blockio.EncodeLocationFromString(bat.Vecs[0].UnsafeGetStringAt(0))
		if err != nil {
			return err
		}
		fileName := location.Name().String()
		copBat, err := util.CopyBatch(bat, tbl.getTxn().proc)
		if err != nil {
			return err
		}
		if err := tbl.getTxn().WriteFile(DELETE, tbl.accountId, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, fileName, copBat, tbl.getTxn().tnStores[0]); err != nil {
			return err
		}

		tbl.getTxn().blockId_tn_delete_metaLoc_batch.RWMutex.Lock()
		tbl.getTxn().blockId_tn_delete_metaLoc_batch.data[*blkId] =
			append(tbl.getTxn().blockId_tn_delete_metaLoc_batch.data[*blkId], copBat)
		tbl.getTxn().blockId_tn_delete_metaLoc_batch.RWMutex.Unlock()

	case deletion.CNBlockOffset:
	case deletion.RawBatchOffset:
	case deletion.RawRowIdBatch:
		logutil.Infof("data return by remote pipeline\n")
		bat = tbl.getTxn().deleteBatch(bat, tbl.db.databaseId, tbl.tableId)
		if bat.RowCount() == 0 {
			return nil
		}
		if err = tbl.writeTnPartition(tbl.getTxn().proc.Ctx, bat); err != nil {
			return err
		}
	default:
		tbl.getTxn().hasS3Op.Store(true)
		panic(moerr.NewInternalErrorNoCtxf("Unsupport type for table delete %d", typ))
	}
	return nil
}

func (tbl *txnTable) ensureSeqnumsAndTypesExpectRowid() {
	if tbl.seqnums != nil && tbl.typs != nil {
		return
	}
	n := len(tbl.tableDef.Cols) - 1
	idxs := make([]uint16, 0, n)
	typs := make([]types.Type, 0, n)
	for i := 0; i < len(tbl.tableDef.Cols)-1; i++ {
		col := tbl.tableDef.Cols[i]
		idxs = append(idxs, uint16(col.Seqnum))
		typs = append(typs, vector.ProtoTypeToType(&col.Typ))
	}
	tbl.seqnums = idxs
	tbl.typs = typs
}

// TODO:: do prefetch read and parallel compaction
func (tbl *txnTable) compaction(
	compactedBlks map[objectio.ObjectLocation][]int64,
) ([]objectio.BlockInfo, []objectio.ObjectStats, error) {
	s3writer, err := colexec.NewS3Writer(tbl.getTxn().proc, tbl.tableDef, 0)
	if err != nil {
		return nil, nil, err
	}
	tbl.ensureSeqnumsAndTypesExpectRowid()

	for blkmetaloc, deletes := range compactedBlks {
		//blk.MetaLocation()
		bat, e := blockio.BlockCompactionRead(
			tbl.getTxn().proc.Ctx,
			blkmetaloc[:],
			deletes,
			tbl.seqnums,
			tbl.typs,
			tbl.getTxn().engine.fs,
			tbl.getTxn().proc.GetMPool())
		if e != nil {
			return nil, nil, e
		}
		if bat.RowCount() == 0 {
			continue
		}
		s3writer.StashBatch(tbl.getTxn().proc, bat)
		bat.Clean(tbl.getTxn().proc.GetMPool())
	}
	return s3writer.SortAndSync(tbl.getTxn().proc)
}

func (tbl *txnTable) Delete(
	ctx context.Context, bat *batch.Batch, name string,
) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("delete operation is not allowed in snapshot transaction")
	}
	//for S3 delete
	if name != catalog.Row_ID {
		return tbl.EnhanceDelete(bat, name)
	}
	bat = tbl.getTxn().deleteBatch(bat, tbl.db.databaseId, tbl.tableId)
	if bat.RowCount() == 0 {
		return nil
	}
	return tbl.writeTnPartition(ctx, bat)
}

func (tbl *txnTable) writeTnPartition(_ context.Context, bat *batch.Batch) error {
	ibat, err := util.CopyBatch(bat, tbl.getTxn().proc)
	if err != nil {
		return err
	}
	if _, err := tbl.getTxn().WriteBatch(DELETE, "", tbl.accountId, tbl.db.databaseId, tbl.tableId,
		tbl.db.databaseName, tbl.tableName, ibat, tbl.getTxn().tnStores[0]); err != nil {
		ibat.Clean(tbl.getTxn().proc.Mp())
		return err
	}
	return nil
}

func (tbl *txnTable) AddTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *txnTable) DelTableDef(ctx context.Context, def engine.TableDef) error {
	return nil
}

func (tbl *txnTable) GetTableID(ctx context.Context) uint64 {
	return tbl.tableId
}

// GetTableName implements the engine.Relation interface.
func (tbl *txnTable) GetTableName() string {
	return tbl.tableName
}

func (tbl *txnTable) GetDBID(ctx context.Context) uint64 {
	return tbl.db.databaseId
}

// for test
func buildRemoteDS(
	ctx context.Context,
	tbl *txnTable,
	txnOffset int,
	relData engine.RelData,
) (source engine.DataSource, err error) {

	tombstones, err := tbl.CollectTombstones(ctx, txnOffset)
	if err != nil {
		return nil, err
	}
	//tombstones.Init()

	if err = relData.AttachTombstones(tombstones); err != nil {
		return nil, err
	}
	buf, err := relData.MarshalBinary()
	if err != nil {
		return
	}

	newRelData, err := UnmarshalRelationData(buf)
	if err != nil {
		return
	}

	source = NewRemoteDataSource(
		ctx,
		tbl.proc.Load(),
		tbl.getTxn().engine.fs,
		tbl.db.op.SnapshotTS(),
		newRelData,
	)
	return
}

// for ut
func BuildLocalDataSource(
	ctx context.Context,
	rel engine.Relation,
	ranges engine.RelData,
	txnOffset int,
) (source engine.DataSource, err error) {

	var (
		ok  bool
		tbl *txnTable
	)

	if tbl, ok = rel.(*txnTable); !ok {
		tbl = rel.(*txnTableDelegate).origin
	}

	return tbl.buildLocalDataSource(ctx, txnOffset, ranges, engine.Policy_CheckAll)
}

func (tbl *txnTable) buildLocalDataSource(
	ctx context.Context,
	txnOffset int,
	relData engine.RelData,
	policy engine.TombstoneApplyPolicy,
) (source engine.DataSource, err error) {

	switch relData.GetType() {
	case engine.RelDataBlockList:
		ranges := relData.GetBlockInfoSlice()
		skipReadMem := !bytes.Equal(
			objectio.EncodeBlockInfo(*ranges.Get(0)), objectio.EmptyBlockInfoBytes)

		if tbl.db.op.IsSnapOp() {
			txnOffset = tbl.getTxn().GetSnapshotWriteOffset()
		}

		forceBuildRemoteDS := false
		if force, tbls := engine.GetForceBuildRemoteDS(); force {
			for _, t := range tbls {
				if t == tbl.tableId {
					forceBuildRemoteDS = true
				}
			}
		}
		if skipReadMem && forceBuildRemoteDS {
			source, err = buildRemoteDS(ctx, tbl, txnOffset, relData)
		} else {
			source, err = NewLocalDataSource(
				ctx,
				tbl,
				txnOffset,
				ranges,
				skipReadMem,
				policy,
			)
		}

	default:
		logutil.Fatalf("unsupported rel data type: %v", relData.GetType())
	}

	return source, err
}

// BuildReaders BuildReader creates a new list of Readers to read data from the table.
// Parameters:
//   - ctx: Context used to control the lifecycle of the request.
//   - num: The number of Readers to create.
//   - expr: Expression used to filter data.
//   - ranges: Byte array representing the data range to read.
//   - orderedScan: Whether to scan the data in order.
//   - txnOffset: Transaction offset used to specify the starting position for reading data.
func (tbl *txnTable) BuildReaders(
	ctx context.Context,
	p any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	tombstonePolicy engine.TombstoneApplyPolicy,
) ([]engine.Reader, error) {
	var rds []engine.Reader
	proc := p.(*process.Process)
	//copy from NewReader.
	if plan2.IsFalseExpr(expr) {
		return []engine.Reader{new(emptyReader)}, nil
	}

	if orderBy && num != 1 {
		return nil, moerr.NewInternalErrorNoCtx("orderBy only support one reader")
	}

	//relData maybe is nil, indicate that only read data from memory.
	if relData == nil || relData.DataCnt() == 0 {
		relData = NewEmptyBlockListRelationData()
		relData.AppendBlockInfo(objectio.EmptyBlockInfo)
	}
	blkCnt := relData.DataCnt()
	newNum := num
	if blkCnt < num {
		newNum = blkCnt
		for i := 0; i < num-blkCnt; i++ {
			rds = append(rds, new(emptyReader))
		}
	}

	scanType := determineScanType(relData, newNum)
	def := tbl.GetTableDef(ctx)
	mod := blkCnt % newNum
	divide := blkCnt / newNum
	var shard engine.RelData
	for i := 0; i < newNum; i++ {
		if i == 0 {
			shard = relData.DataSlice(i*divide, (i+1)*divide+mod)
		} else {
			shard = relData.DataSlice(i*divide+mod, (i+1)*divide+mod)
		}
		ds, err := tbl.buildLocalDataSource(ctx, txnOffset, shard, tombstonePolicy)
		if err != nil {
			return nil, err
		}
		rd, err := NewReader(
			ctx,
			proc,
			tbl.getTxn().engine,
			def,
			tbl.db.op.SnapshotTS(),
			expr,
			ds,
		)
		if err != nil {
			return nil, err
		}

		rd.scanType = scanType
		rds = append(rds, rd)
	}
	return rds, nil
}

func (tbl *txnTable) getPartitionState(
	ctx context.Context,
) (*logtailreplay.PartitionState, error) {
	if !tbl.db.op.IsSnapOp() {
		if tbl._partState.Load() == nil {
			ps, err := tbl.tryToSubscribe(ctx)
			if err != nil {
				return nil, err
			}
			if ps == nil {
				ps = tbl.getTxn().engine.GetOrCreateLatestPart(tbl.db.databaseId, tbl.tableId).Snapshot()
			}
			tbl._partState.Store(ps)
		}
		return tbl._partState.Load(), nil
	}

	// for snapshot txnOp
	if tbl._partState.Load() == nil {
		ps, err := tbl.getTxn().engine.getOrCreateSnapPart(
			ctx,
			tbl,
			types.TimestampToTS(tbl.db.op.Txn().SnapshotTS))
		if err != nil {
			return nil, err
		}
		tbl._partState.Store(ps)
	}
	return tbl._partState.Load(), nil
}

func (tbl *txnTable) tryToSubscribe(ctx context.Context) (ps *logtailreplay.PartitionState, err error) {
	defer func() {
		if err == nil {
			tbl.getTxn().engine.globalStats.notifyLogtailUpdate(tbl.tableId)
		}
	}()

	if tbl.isCreatedInTxn() {
		return
	}

	return tbl.getTxn().engine.PushClient().toSubscribeTable(ctx, tbl)

}

func (tbl *txnTable) PKPersistedBetween(
	p *logtailreplay.PartitionState,
	from types.TS,
	to types.TS,
	keys *vector.Vector,
) (bool, error) {

	ctx := tbl.proc.Load().Ctx
	fs := tbl.getTxn().engine.fs
	primaryIdx := tbl.primaryIdx

	var (
		meta objectio.ObjectDataMeta
		bf   objectio.BloomFilter
	)

	candidateBlks := make(map[types.Blockid]*objectio.BlockInfo)

	//only check data objects.
	delObjs, cObjs := p.GetChangedObjsBetween(from.Next(), types.MaxTs())
	isFakePK := tbl.GetTableDef(ctx).Pkey.PkeyColName == catalog.FakePrimaryKeyColName
	if err := ForeachCommittedObjects(cObjs, delObjs, p,
		func(obj logtailreplay.ObjectInfo) (err2 error) {
			var zmCkecked bool
			if !isFakePK {
				// if the object info contains a pk zonemap, fast-check with the zonemap
				if !obj.ZMIsEmpty() {
					if !obj.SortKeyZoneMap().AnyIn(keys) {
						return
					}
					zmCkecked = true
				}
			}

			var objMeta objectio.ObjectMeta
			location := obj.Location()

			// load object metadata
			if objMeta, err2 = objectio.FastLoadObjectMeta(
				ctx, &location, false, fs,
			); err2 != nil {
				return
			}

			// reset bloom filter to nil for each object
			meta = objMeta.MustDataMeta()

			// check whether the object is skipped by zone map
			// If object zone map doesn't contains the pk value, we need to check bloom filter
			if !zmCkecked {
				if !meta.MustGetColumn(uint16(primaryIdx)).ZoneMap().AnyIn(keys) {
					return
				}
			}

			bf = nil
			//fake pk has no bf
			if !isFakePK {
				if bf, err2 = objectio.LoadBFWithMeta(
					ctx, meta, location, fs,
				); err2 != nil {
					return
				}
			}

			ForeachBlkInObjStatsList(false, meta,
				func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
					if !blkMeta.IsEmpty() &&
						!blkMeta.MustGetColumn(uint16(primaryIdx)).ZoneMap().AnyIn(keys) {
						return true
					}
					//fake pk has no bf
					if !isFakePK {
						blkBf := bf.GetBloomFilter(uint32(blk.BlockID.Sequence()))
						blkBfIdx := index.NewEmptyBloomFilter()
						if err2 = index.DecodeBloomFilter(blkBfIdx, blkBf); err2 != nil {
							return false
						}
						var exist bool
						lowerBound, upperBound := blkMeta.MustGetColumn(uint16(primaryIdx)).ZoneMap().SubVecIn(keys)
						if exist = blkBfIdx.MayContainsAny(keys, lowerBound, upperBound); !exist {
							return true
						}
					}

					blk.Sorted = obj.Sorted
					blk.Appendable = obj.Appendable
					blk.PartitionNum = -1
					candidateBlks[blk.BlockID] = &blk
					return true
				}, obj.ObjectStats)

			return
		}); err != nil {
		return true, err
	}

	var filter blockio.ReadFilterSearchFuncType
	buildFilter := func() (blockio.ReadFilterSearchFuncType, error) {
		//keys must be sorted.
		keys.InplaceSort()
		bytes, _ := keys.MarshalBinary()
		colExpr := newColumnExpr(0, plan2.MakePlan2Type(keys.GetType()), tbl.tableDef.Pkey.PkeyColName)
		inExpr := plan2.MakeInExpr(
			tbl.proc.Load().Ctx,
			colExpr,
			int32(keys.Length()),
			bytes,
			false)

		basePKFilter, err := newBasePKFilter(inExpr, tbl.tableDef, tbl.proc.Load())
		if err != nil {
			return nil, err
		}

		blockReadPKFilter, err := newBlockReadPKFilter(tbl.tableDef.Pkey.PkeyColName, basePKFilter)
		if err != nil {
			return nil, err
		}

		return blockReadPKFilter.SortedSearchFunc, nil
	}

	var unsortedFilter blockio.ReadFilterSearchFuncType
	buildUnsortedFilter := func() blockio.ReadFilterSearchFuncType {
		return getNonSortedPKSearchFuncByPKVec(keys)
	}

	//read block ,check if keys exist in the block.
	pkDef := tbl.tableDef.Cols[tbl.primaryIdx]
	pkSeq := pkDef.Seqnum
	pkType := types.T(pkDef.Typ.Id).ToType()
	for _, blk := range candidateBlks {
		bat, release, err := blockio.LoadColumns(
			ctx,
			[]uint16{uint16(pkSeq)},
			[]types.Type{pkType},
			fs,
			blk.MetaLocation(),
			tbl.proc.Load().GetMPool(),
			fileservice.Policy(0),
		)
		if err != nil {
			return true, err
		}
		defer release()

		if !blk.Sorted {
			if unsortedFilter == nil {
				unsortedFilter = buildUnsortedFilter()
			}
			sels := unsortedFilter(bat.Vecs)
			if len(sels) > 0 {
				return true, nil
			}
			continue
		}

		//for sorted block, we can use binary search to find the keys.
		if filter == nil {
			filter, err = buildFilter()
			if filter == nil || err != nil {
				logutil.Warn("build filter failed, switch to linear search",
					zap.Uint32("accid", tbl.accountId),
					zap.Uint64("tableid", tbl.tableId),
					zap.String("tablename", tbl.tableName))
				filter = buildUnsortedFilter()
			}
			if err != nil {
				return false, err
			}
		}
		sels := filter(bat.Vecs)
		if len(sels) > 0 {
			return true, nil
		}
	}
	return false, nil
}

func (tbl *txnTable) PrimaryKeysMayBeModified(
	ctx context.Context,
	from types.TS,
	to types.TS,
	keysVector *vector.Vector,
) (bool, error) {
	if tbl.db.op.IsSnapOp() {
		return false,
			moerr.NewInternalErrorNoCtx("primary key modification is not allowed in snapshot transaction")
	}
	part, err := tbl.getTxn().engine.LazyLoadLatestCkp(ctx, tbl)
	if err != nil {
		return false, err
	}

	snap := part.Snapshot()
	var packer *types.Packer
	put := tbl.getTxn().engine.packerPool.Get(&packer)
	defer put.Put()
	packer.Reset()

	keys := logtailreplay.EncodePrimaryKeyVector(keysVector, packer)
	exist, flushed := snap.PKExistInMemBetween(from, to, keys)
	if exist {
		return true, nil
	}
	if !flushed {
		return false, nil
	}

	// if tbl.tableName == catalog.MO_DATABASE ||
	// 	tbl.tableName == catalog.MO_TABLES ||
	// 	tbl.tableName == catalog.MO_COLUMNS {
	// 	logutil.Warnf("mo table:%s always exist in memory", tbl.tableName)
	// 	return true, nil
	// }

	//need check pk whether exist on S3 block.
	return tbl.PKPersistedBetween(
		snap,
		from,
		to,
		keysVector)
}

func (tbl *txnTable) MergeObjects(
	ctx context.Context,
	objStats []objectio.ObjectStats,
	targetObjSize uint32,
) (*api.MergeCommitEntry, error) {
	if len(objStats) < 2 {
		return nil, moerr.NewInternalErrorNoCtx("no matching objects")
	}

	snapshot := types.TimestampToTS(tbl.getTxn().op.SnapshotTS())
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	sortKeyPos, sortKeyIsPK := tbl.getSortKeyPosAndSortKeyIsPK()
	objInfos := make([]logtailreplay.ObjectInfo, 0, len(objStats))
	for _, objstat := range objStats {
		info, exist := state.GetObject(*objstat.ObjectShortName())
		if !exist || (!info.DeleteTime.IsEmpty() && info.DeleteTime.LessEq(&snapshot)) {
			logutil.Errorf("object not visible: %s", info.String())
			return nil, moerr.NewInternalErrorNoCtxf("object %s not exist", objstat.ObjectName().String())
		}
		objInfos = append(objInfos, info)
	}

	if len(objInfos) < 2 {
		return nil, moerr.NewInternalErrorNoCtx("no matching objects")
	}

	tbl.ensureSeqnumsAndTypesExpectRowid()

	taskHost, err := newCNMergeTask(
		ctx, tbl, snapshot, // context
		sortKeyPos, sortKeyIsPK, // schema
		objInfos, // targets
		targetObjSize)
	if err != nil {
		return nil, err
	}

	err = mergesort.DoMergeAndWrite(ctx, tbl.getTxn().op.Txn().DebugString(), sortKeyPos, taskHost, false)
	if err != nil {
		taskHost.commitEntry.Err = err.Error()
		return taskHost.commitEntry, err
	}

	if !taskHost.DoTransfer() {
		return taskHost.commitEntry, nil
	}

	return dumpTransferInfo(ctx, taskHost)
}

func (tbl *txnTable) GetNonAppendableObjectStats(ctx context.Context) ([]objectio.ObjectStats, error) {
	snapshot := types.TimestampToTS(tbl.getTxn().op.SnapshotTS())
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	sortKeyPos, _ := tbl.getSortKeyPosAndSortKeyIsPK()
	objStats := make([]objectio.ObjectStats, 0, tbl.ApproxObjectsNum(ctx))

	err = ForeachVisibleDataObject(state, snapshot, func(obj logtailreplay.ObjectEntry) error {
		if obj.Appendable {
			return nil
		}
		if sortKeyPos != -1 {
			sortKeyZM := obj.SortKeyZoneMap()
			if !sortKeyZM.IsInited() {
				return nil
			}
		}
		objStats = append(objStats, obj.ObjectStats)
		return nil
	}, nil)
	if err != nil {
		return nil, err
	}
	return objStats, nil
}

func (tbl *txnTable) getSortKeyPosAndSortKeyIsPK() (int, bool) {
	sortKeyPos := -1
	sortKeyIsPK := false
	if tbl.primaryIdx >= 0 && tbl.tableDef.Cols[tbl.primaryIdx].Name != catalog.FakePrimaryKeyColName {
		if tbl.clusterByIdx < 0 {
			sortKeyPos = tbl.primaryIdx
			sortKeyIsPK = true
		} else {
			panic(fmt.Sprintf("bad schema pk %v, ck %v", tbl.primaryIdx, tbl.clusterByIdx))
		}
	} else if tbl.clusterByIdx >= 0 {
		sortKeyPos = tbl.clusterByIdx
		sortKeyIsPK = false
	}
	return sortKeyPos, sortKeyIsPK
}

func dumpTransferInfo(ctx context.Context, mergeTask *cnMergeTask) (*api.MergeCommitEntry, error) {
	rowCnt := 0
	for _, m := range mergeTask.transferMaps {
		rowCnt += len(m)
	}

	// If transfer info is small, send it to tn directly.
	// transfer info size is only related to row count.
	// For api.TransDestPos, 5*10^5 rows is 52*5*10^5 ~= 26MB
	// For api.TransferDestPos, 5*10^5 rows is 12*5*10^5 ~= 6MB
	if rowCnt < 500000 {
		size := len(mergeTask.transferMaps)
		mappings := make([]api.BlkTransMap, size)
		for i := 0; i < size; i++ {
			mappings[i] = api.BlkTransMap{
				M: make(map[int32]api.TransDestPos, len(mergeTask.transferMaps[i])),
			}
		}
		mergeTask.commitEntry.Booking = &api.BlkTransferBooking{
			Mappings: mappings,
		}

		for i, m := range mergeTask.transferMaps {
			for r, pos := range m {
				mergeTask.commitEntry.Booking.Mappings[i].M[int32(r)] = api.TransDestPos{
					ObjIdx: int32(pos.ObjIdx),
					BlkIdx: int32(pos.BlkIdx),
					RowIdx: int32(pos.RowIdx),
				}
			}
		}
		return mergeTask.commitEntry, nil
	}

	// if transfer info is too large, write it down to s3
	if err := writeTransferInfoToS3(ctx, mergeTask); err != nil {
		return mergeTask.commitEntry, err
	}
	var locStr strings.Builder
	locations := mergeTask.commitEntry.BookingLoc
	blkCnt := types.DecodeInt32(commonUtil.UnsafeStringToBytes(locations[0]))
	for _, filepath := range locations[blkCnt+1:] {
		locStr.WriteString(filepath)
		locStr.WriteString(",")
	}
	logutil.Infof("mergeblocks %v-%v on cn: write s3 transfer info %v",
		mergeTask.host.tableId, mergeTask.host.tableName, locStr.String())

	return mergeTask.commitEntry, nil
}

func writeTransferInfoToS3(ctx context.Context, taskHost *cnMergeTask) (err error) {
	defer func() {
		if err != nil {
			locations := taskHost.commitEntry.BookingLoc
			for _, filepath := range locations {
				_ = taskHost.fs.Delete(ctx, filepath)
			}
		}
	}()

	return writeTransferMapsToS3(ctx, taskHost)
}

func writeTransferMapsToS3(ctx context.Context, taskHost *cnMergeTask) (err error) {
	bookingMaps := taskHost.transferMaps

	blkCnt := int32(len(bookingMaps))
	totalRows := 0

	// BookingLoc layout:
	// | blockCnt | Blk1RowCnt | Blk2RowCnt | ... | filepath1 | filepath2 | ... |
	taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc,
		commonUtil.UnsafeBytesToString(types.EncodeInt32(&blkCnt)))
	for _, m := range bookingMaps {
		rowCnt := int32(len(m))
		taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc,
			commonUtil.UnsafeBytesToString(types.EncodeInt32(&rowCnt)))
		totalRows += len(m)
	}

	columns := []string{"src_blk", "src_row", "dest_obj", "dest_blk", "dest_row"}
	colTypes := []types.T{types.T_int32, types.T_uint32, types.T_uint8, types.T_uint16, types.T_uint32}
	batchSize := min(200*mpool.MB/len(columns)/int(unsafe.Sizeof(int32(0))), totalRows)
	buffer := batch.New(true, columns)
	releases := make([]func(), len(columns))
	for i := range columns {
		t := colTypes[i].ToType()
		vec, release := taskHost.GetVector(&t)
		err := vec.PreExtend(batchSize, taskHost.GetMPool())
		if err != nil {
			return err
		}
		buffer.Vecs[i] = vec
		releases[i] = release
	}
	objRowCnt := 0
	for blkIdx, transMap := range bookingMaps {
		for rowIdx, destPos := range transMap {
			if err = vector.AppendFixed(buffer.Vecs[0], int32(blkIdx), false, taskHost.GetMPool()); err != nil {
				return err
			}
			if err = vector.AppendFixed(buffer.Vecs[1], rowIdx, false, taskHost.GetMPool()); err != nil {
				return nil
			}
			if err = vector.AppendFixed(buffer.Vecs[2], destPos.ObjIdx, false, taskHost.GetMPool()); err != nil {
				return nil
			}
			if err = vector.AppendFixed(buffer.Vecs[3], destPos.BlkIdx, false, taskHost.GetMPool()); err != nil {
				return nil
			}
			if err = vector.AppendFixed(buffer.Vecs[4], destPos.RowIdx, false, taskHost.GetMPool()); err != nil {
				return nil
			}

			buffer.SetRowCount(buffer.RowCount() + 1)
			objRowCnt++

			if objRowCnt*len(columns)*int(unsafe.Sizeof(int32(0))) > 200*mpool.MB {
				filename := blockio.EncodeTmpFileName("tmp", "merge_"+uuid.NewString(), time.Now().UTC().Unix())
				writer, err := objectio.NewObjectWriterSpecial(objectio.WriterTmp, filename, taskHost.fs)
				if err != nil {
					return err
				}

				_, err = writer.Write(buffer)
				if err != nil {
					return err
				}
				buffer.CleanOnlyData()

				_, err = writer.WriteEnd(ctx)
				if err != nil {
					return err
				}
				taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc, filename)
				objRowCnt = 0
			}
		}
	}

	// write remaining data
	if buffer.RowCount() != 0 {
		filename := blockio.EncodeTmpFileName("tmp", "merge_"+uuid.NewString(), time.Now().UTC().Unix())
		writer, err := objectio.NewObjectWriterSpecial(objectio.WriterTmp, filename, taskHost.fs)
		if err != nil {
			return err
		}

		_, err = writer.Write(buffer)
		if err != nil {
			return err
		}
		buffer.CleanOnlyData()

		_, err = writer.WriteEnd(ctx)
		if err != nil {
			return err
		}
		taskHost.commitEntry.BookingLoc = append(taskHost.commitEntry.BookingLoc, filename)
	}

	taskHost.commitEntry.Booking = nil
	return nil
}

func (tbl *txnTable) getUncommittedRows(
	deletes map[types.Rowid]struct{},
) uint64 {
	rows := uint64(0)
	tbl.getTxn().ForEachTableWrites(
		tbl.db.databaseId,
		tbl.tableId,
		tbl.getTxn().GetSnapshotWriteOffset(),
		func(entry Entry) {
			if entry.typ == INSERT {
				rows = rows + uint64(entry.bat.RowCount())
			} else {
				if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
					vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
					for _, v := range vs {
						deletes[v] = struct{}{}
					}
				}
			}
		},
	)
	return rows
}

func (tbl *txnTable) getCommittedRows(
	ctx context.Context,
	deletes map[types.Rowid]struct{},
) (uint64, error) {
	rows := uint64(0)
	ts := types.TimestampToTS(tbl.db.op.SnapshotTS())
	partition, err := tbl.getPartitionState(ctx)
	if err != nil {
		return 0, err
	}
	iter := partition.NewRowsIter(ts, nil, false)
	defer func() { _ = iter.Close() }()
	for iter.Next() {
		entry := iter.Entry()
		if _, ok := deletes[entry.RowID]; ok {
			continue
		}
		rows++
	}
	s, _ := tbl.Stats(ctx, true)
	if s == nil {
		return rows, nil
	}
	return uint64(s.TableCnt) + rows, nil
}
