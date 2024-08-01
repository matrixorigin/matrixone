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

	"github.com/docker/go-units"
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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
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
	approxObjectNum := int64(partitionState.ApproxObjectsNum())
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
		return 0, moerr.NewInvalidInput(ctx, "bad input column name %v", columnName)
	}

	deletes := make(map[types.Rowid]struct{})
	tbl.getTxn().ForEachTableWrites(
		tbl.db.databaseId,
		tbl.tableId,
		tbl.getTxn().GetSnapshotWriteOffset(),
		func(entry Entry) {
			if entry.typ == INSERT || entry.typ == INSERT_TXN {
				for i, s := range entry.bat.Attrs {
					if _, ok := neededCols[s]; ok {
						szInPart += uint64(entry.bat.Vecs[i].Size())
					}
				}
			} else {
				if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
					// CASE:
					// create table t1(a int);
					// begin;
					// truncate t1; //txnDatabase.Truncate will DELETE mo_tables
					// show tables; // t1 must be shown
					if entry.databaseId == catalog.MO_CATALOG_ID &&
						entry.tableId == catalog.MO_TABLES_ID &&
						entry.truncate {
						return
					}
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
		return 0, moerr.NewInvalidInput(ctx, "bad input column name %v", columnName)
	}
	return sz + szInPart, nil
}

func ForeachVisibleDataObject(
	state *logtailreplay.PartitionState,
	ts types.TS,
	fn func(obj logtailreplay.ObjectEntry) error,
	executor ConcurrentExecutor,
) (err error) {
	iter, err := state.NewObjectsIter(ts, true)
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
	return part.ApproxObjectsNum()
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
		return nil, moerr.NewInvalidInput(ctx, "bad input column name %v", name)
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
	infoList := make([]*plan.MetadataScanInfo, 0, state.ApproxObjectsNum())
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
					ColName:    col.Name,
					IsHidden:   col.Hidden,
					ObjectName: objName,
					ObjLoc:     location,
					CreateTs:   createTs,
					DeleteTs:   deleteTs,
					RowCnt:     int64(obj.Rows()),
					ZoneMap:    objectio.EmptyZm[:],
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
			rowIdBat, release, err := blockio.LoadTombstoneColumns(
				tbl.getTxn().proc.Ctx,
				[]uint16{0},
				nil,
				tbl.getTxn().engine.fs,
				location,
				tbl.getTxn().proc.GetMPool())
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
	deletesRowId map[types.Rowid]uint8,
) error {

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
				rowIdBat, release, err := blockio.LoadTombstoneColumns(
					tbl.getTxn().proc.Ctx,
					[]uint16{0},
					nil,
					tbl.getTxn().engine.fs,
					location,
					tbl.getTxn().proc.GetMPool())
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

func (tbl *txnTable) reset(newId uint64) {
	//if the table is truncated first time, the table id is saved into the oldTableId
	if tbl.oldTableId == 0 {
		tbl.oldTableId = tbl.tableId
	}
	tbl.tableId = newId
	tbl._partState.Store(nil)
}

func (tbl *txnTable) resetSnapshot() {
	tbl._partState.Store(nil)
}

// CollectTombstones collects in memory tombstones and tombstone objects.
func (tbl *txnTable) CollectTombstones(
	ctx context.Context, txnOffset int,
) (engine.Tombstoner, error) {
	tombstone := buildTombstoneWithDeltaLoc()

	offset := txnOffset
	if tbl.db.op.IsSnapOp() {
		offset = tbl.getTxn().GetSnapshotWriteOffset()
	}

	//collect in memory

	//collect uncommitted in-memory tombstones from txn.writes
	tbl.getTxn().ForEachTableWrites(tbl.db.databaseId, tbl.tableId,
		offset, func(entry Entry) {
			if entry.typ == INSERT || entry.typ == INSERT_TXN {
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
				if entry.IsGeneratedByTruncate() {
					return
				}
				//deletes in txn.Write maybe comes from PartitionState.Rows ,
				// PartitionReader need to skip them.
				vs := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
				for _, v := range vs {
					bid, o := v.Decode()
					tombstone.inMemTombstones[bid] = append(tombstone.inMemTombstones[bid], int32(o))
				}
			}
		})

	//collect uncommitted in-memory tombstones belongs to blocks persisted by CN writing S3
	tbl.getTxn().deletedBlocks.getDeletedRowIDs(tombstone.inMemTombstones)

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
			bid, o := entry.RowID.Decode()
			tombstone.inMemTombstones[bid] = append(tombstone.inMemTombstones[bid], int32(o))
		}
		iter.Close()
	}

	//collect uncommitted persisted tombstones.
	if err := tbl.getTxn().getUncommittedS3Tombstone(tombstone.blk2UncommitLoc); err != nil {
		return nil, err
	}
	//collect committed persisted tombstones from partition state.
	state.GetTombstoneDeltaLocs(tombstone.blk2CommitLoc)
	return tombstone, nil
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
	sid := tbl.proc.Load().GetService()
	start := time.Now()
	seq := tbl.db.op.NextSequence()

	var blocks objectio.BlockInfoSliceInProgress

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

	blocks.AppendBlockInfo(objectio.EmptyBlockInfoInProgress)

	if err = tbl.rangesOnePart(
		ctx,
		part,
		tbl.GetTableDef(ctx),
		exprs,
		&blocks,
		tbl.proc.Load(),
		txnOffset,
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
	outBlocks *objectio.BlockInfoSliceInProgress, // output marshaled block list after filtering
	proc *process.Process, // process of this transaction
	txnOffset int,
) (err error) {
	var done bool

	uncommittedObjects := tbl.collectUnCommittedObjects(txnOffset)

	if done, err = TryFastFilterBlocks(
		ctx,
		tbl,
		txnOffset,
		tbl.db.op.SnapshotTS(),
		tbl.tableDef,
		exprs,
		state,
		uncommittedObjects,
		//&dirtyBlks,
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

			ForeachBlkInObjStatsListInProgress(true, meta, func(blk objectio.BlockInfoInProgress, blkMeta objectio.BlockObject) bool {
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
				blk.EntryState = obj.EntryState
				blk.CommitTs = obj.CommitTS
				//if obj.HasDeltaLoc {
				//	_, commitTs, ok := state.GetBockDeltaLoc(blk.BlockID)
				//	if ok {
				//		blk.CommitTs = commitTs
				//	}
				//}

				outBlocks.AppendBlockInfo(blk)

				return true

			},
				obj.ObjectStats,
			)
			return
		},
		state,
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
			if entry.typ == INSERT_TXN {
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
	tbl.tableDef.DbName = tbl.db.databaseName
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
	bat, err := genTableConstraintTuple(tbl.tableId, tbl.db.databaseId, tbl.tableName, tbl.db.databaseName,
		ct, tbl.getTxn().proc.Mp())
	if err != nil {
		return err
	}
	if err = tbl.getTxn().WriteBatch(UPDATE, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, tbl.getTxn().tnStores[0], -1, false, false); err != nil {
		bat.Clean(tbl.getTxn().proc.Mp())
		return err
	}
	tbl.constraint = ct
	tbl.tableDef = nil
	tbl.GetTableDef(ctx)
	return nil
}

func (tbl *txnTable) AlterTable(
	ctx context.Context, c *engine.ConstraintDef, constraint [][]byte,
) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("cannot alter table in snapshot operation")
	}
	ct, err := c.MarshalBinary()
	if err != nil {
		return err
	}
	bat, err := genTableAlterTuple(constraint, tbl.getTxn().proc.Mp())
	if err != nil {
		return err
	}
	if err = tbl.getTxn().WriteBatch(ALTER, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, tbl.getTxn().tnStores[0], -1, false, false); err != nil {
		bat.Clean(tbl.getTxn().proc.Mp())
		return err
	}
	tbl.constraint = ct
	// add tbl.partition = partition

	tbl.tableDef = nil

	// update TableDef
	tbl.GetTableDef(ctx)
	return nil
}

func (tbl *txnTable) TableRenameInTxn(ctx context.Context, constraint [][]byte) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("cannot rename table in snapshot operation")
	}
	// 1. delete cn metadata of table
	accountId, userId, roleId, err := getAccessInfo(ctx)
	if err != nil {
		return err
	}
	databaseId := tbl.GetDBID(ctx)
	db := tbl.db
	oldTableName := tbl.tableName

	var id uint64
	var rowid types.Rowid
	var rowids []types.Rowid
	key := genTableKey(accountId, tbl.tableName, databaseId)
	if value, ok := tbl.db.getTxn().createMap.Load(key); ok {
		tbl.db.getTxn().createMap.Delete(key)
		table := value.(*txnTable)
		id = table.tableId
		rowid = table.rowid
		rowids = table.rowids
		if tbl != table {
			panic("The table object in createMap should be the current table object")
		}
	} else if value, ok := tbl.db.getTxn().tableCache.tableMap.Load(key); ok {
		table := value.(*txnTableDelegate).origin
		id = table.tableId
		rowid = table.rowid
		rowids = table.rowids
		if tbl != table {
			panic("The table object in tableCache should be the current table object")
		}
		tbl.db.getTxn().tableCache.tableMap.Delete(key)
	} else {
		// I think it is unnecessary to make a judgment on this branch because the table is already in use, so it must be in the cache
		item := &cache.TableItem{
			Name:       tbl.tableName,
			DatabaseId: databaseId,
			AccountId:  accountId,
			Ts:         db.op.SnapshotTS(),
		}
		if ok := tbl.db.getTxn().engine.getLatestCatalogCache().GetTable(item); !ok {
			return moerr.GetOkExpectedEOB()
		}
		id = item.Id
		rowid = item.Rowid
		rowids = item.Rowids
	}

	bat, err := genDropTableTuple(rowid, id, db.databaseId, tbl.tableName,
		db.databaseName, tbl.db.getTxn().proc.Mp())
	if err != nil {
		return err
	}
	for _, store := range tbl.db.getTxn().tnStores {
		if err := tbl.db.getTxn().WriteBatch(DELETE_TXN, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1, false, false); err != nil {
			bat.Clean(tbl.db.getTxn().proc.Mp())
			return err
		}
	}

	// Add writeBatch(delete,mo_columns) to filter table in mo_columns.
	// Every row in writeBatch(delete,mo_columns) needs rowid
	for _, rid := range rowids {
		bat, err = genDropColumnTuple(rid, tbl.db.getTxn().proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range tbl.db.getTxn().tnStores {
			if err = tbl.db.getTxn().WriteBatch(DELETE_TXN, 0, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
				catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, store, -1, false, false); err != nil {
				bat.Clean(tbl.db.getTxn().proc.Mp())
				return err
			}
		}
	}
	tbl.db.getTxn().deletedTableMap.Store(key, id)

	//------------------------------------------------------------------------------------------------------------------
	// 2. send alter message to DN
	bat, err = genTableAlterTuple(constraint, tbl.db.getTxn().proc.Mp())
	if err != nil {
		return err
	}
	if err = tbl.db.getTxn().WriteBatch(ALTER, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
		catalog.MO_CATALOG, catalog.MO_TABLES, bat, tbl.db.getTxn().tnStores[0], -1, false, false); err != nil {
		bat.Clean(tbl.db.getTxn().proc.Mp())
		return err
	}

	req := &api.AlterTableReq{}
	if err = req.Unmarshal(constraint[0]); err != nil {
		return err
	} else {
		rename_table := req.Operation.(*api.AlterTableReq_RenameTable)
		newTblName := rename_table.RenameTable.NewName
		tbl.tableName = newTblName
	}
	tbl.tableDef = nil
	tbl.GetTableDef(ctx)

	//------------------------------------------------------------------------------------------------------------------
	// 3. insert new table metadata
	newtbl := new(txnTable)
	newtbl.accountId = accountId

	newRowId, err := tbl.db.getTxn().allocateID(ctx)
	if err != nil {
		return err
	}
	newtbl.rowid = types.DecodeFixed[types.Rowid](types.EncodeSlice([]uint64{newRowId}))
	newtbl.comment = tbl.comment
	newtbl.relKind = tbl.relKind
	newtbl.createSql = tbl.createSql
	newtbl.viewdef = tbl.viewdef
	newtbl.partitioned = tbl.partitioned
	newtbl.partition = tbl.partition
	newtbl.constraint = tbl.constraint
	newtbl.primaryIdx = tbl.primaryIdx
	newtbl.primarySeqnum = tbl.primarySeqnum
	newtbl.clusterByIdx = tbl.clusterByIdx
	newtbl.db = db
	newtbl.defs = tbl.defs
	newtbl.tableName = tbl.tableName
	newtbl.tableId = tbl.tableId
	newtbl.CopyTableDef(ctx)

	{
		sql := getSql(ctx)
		bat, err := genCreateTableTuple(newtbl, sql, accountId, userId, roleId, newtbl.tableName,
			newtbl.tableId, db.databaseId, db.databaseName, newtbl.rowid, true, tbl.db.getTxn().proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range tbl.db.getTxn().tnStores {
			if err := tbl.db.getTxn().WriteBatch(INSERT_TXN, 0, catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID,
				catalog.MO_CATALOG, catalog.MO_TABLES, bat, store, -1, true, false); err != nil {
				bat.Clean(tbl.db.getTxn().proc.Mp())
				return err
			}
		}
	}

	cols, err := genColumns(accountId, newtbl.tableName, db.databaseName, newtbl.tableId, db.databaseId, newtbl.defs)
	if err != nil {
		return err
	}

	newtbl.rowids = make([]types.Rowid, len(cols))
	for i, col := range cols {
		newtbl.rowids[i] = tbl.db.getTxn().genRowId()
		bat, err := genCreateColumnTuple(col, newtbl.rowids[i], true, tbl.db.getTxn().proc.Mp())
		if err != nil {
			return err
		}
		for _, store := range tbl.db.getTxn().tnStores {
			if err := tbl.db.getTxn().WriteBatch(INSERT_TXN, 0, catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID,
				catalog.MO_CATALOG, catalog.MO_COLUMNS, bat, store, -1, true, false); err != nil {
				bat.Clean(tbl.db.getTxn().proc.Mp())
				return err
			}
		}
		if col.constraintType == catalog.SystemColPKConstraint {
			newtbl.primaryIdx = i
			newtbl.primarySeqnum = i
		}
		if col.isClusterBy == 1 {
			newtbl.clusterByIdx = i
		}
	}

	newkey := genTableKey(accountId, newtbl.tableName, databaseId)
	newtbl.getTxn().addCreateTable(newkey, newtbl)
	newtbl.getTxn().deletedTableMap.Delete(newkey)
	//---------------------------------------------------------------------------------
	for i := 0; i < len(newtbl.getTxn().writes); i++ {
		if newtbl.getTxn().writes[i].tableId == catalog.MO_DATABASE_ID ||
			newtbl.getTxn().writes[i].tableId == catalog.MO_TABLES_ID ||
			newtbl.getTxn().writes[i].tableId == catalog.MO_COLUMNS_ID {
			continue
		}

		if newtbl.getTxn().writes[i].tableName == oldTableName {
			newtbl.getTxn().writes[i].tableName = tbl.tableName
			logutil.Infof("copy table '%s' has been rename to '%s' in txn", oldTableName, tbl.tableName)
		}
	}
	//---------------------------------------------------------------------------------
	return nil
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
		fileName := objectio.DecodeBlockInfoInProgress(bat.Vecs[0].GetBytesAt(0)).MetaLocation().Name().String()
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
	if err := tbl.getTxn().WriteBatch(
		INSERT,
		tbl.accountId,
		tbl.db.databaseId,
		tbl.tableId,
		tbl.db.databaseName,
		tbl.tableName,
		ibat,
		tbl.getTxn().tnStores[0],
		tbl.primaryIdx,
		false,
		false); err != nil {
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
		tbl.writeTnPartition(tbl.getTxn().proc.Ctx, bat)
	default:
		tbl.getTxn().hasS3Op.Store(true)
		panic(moerr.NewInternalErrorNoCtx("Unsupport type for table delete %d", typ))
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
) ([]objectio.BlockInfoInProgress, []objectio.ObjectStats, error) {
	s3writer := &colexec.S3Writer{}
	s3writer.SetTableName(tbl.tableName)
	s3writer.SetSchemaVer(tbl.version)
	_, err := s3writer.GenerateWriter(tbl.getTxn().proc)
	if err != nil {
		return nil, nil, err
	}
	tbl.ensureSeqnumsAndTypesExpectRowid()
	s3writer.SetSeqnums(tbl.seqnums)

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
		s3writer.WriteBlock(bat)
		bat.Clean(tbl.getTxn().proc.GetMPool())

	}
	createdBlks, stats, err := s3writer.WriteEndBlocks(tbl.getTxn().proc)
	if err != nil {
		return nil, nil, err
	}
	return createdBlks, stats, nil
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
	if err := tbl.getTxn().WriteBatch(DELETE, tbl.accountId, tbl.db.databaseId, tbl.tableId,
		tbl.db.databaseName, tbl.tableName, ibat, tbl.getTxn().tnStores[0], tbl.primaryIdx, false, false); err != nil {
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

	relData.AttachTombstones(tombstones)

	source = NewRemoteDataSource(
		ctx,
		tbl.proc.Load(),
		tbl.getTxn().engine.fs,
		tbl.db.op.SnapshotTS(),
		relData,
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

	return tbl.buildLocalDataSource(ctx, txnOffset, ranges, CheckAll)
}

func (tbl *txnTable) buildLocalDataSource(
	ctx context.Context,
	txnOffset int,
	relData engine.RelData,
	policy SkipCheckPolicy,
) (source engine.DataSource, err error) {

	switch relData.GetType() {
	case engine.RelDataBlockList:
		ranges := relData.GetBlockInfoSlice()
		skipReadMem := !bytes.Equal(
			objectio.EncodeBlockInfoInProgress(*ranges.Get(0)), objectio.EmptyBlockInfoInProgressBytes)

		if tbl.db.op.IsSnapOp() {
			txnOffset = tbl.getTxn().GetSnapshotWriteOffset()
		}

		if skipReadMem && engine.GetForceBuildRemoteDS() {
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
) ([]engine.Reader, error) {
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
		relData = buildBlockListRelationData()
		relData.AppendBlockInfo(objectio.EmptyBlockInfoInProgress)
	}
	blkCnt := relData.DataCnt()
	if blkCnt < num {
		return nil, moerr.NewInternalErrorNoCtx("not enough blocks")
	}

	scanType := determineScanType(relData, num)
	def := tbl.GetTableDef(ctx)
	mod := blkCnt % num
	divide := blkCnt / num
	var shard engine.RelData
	var rds []engine.Reader
	for i := 0; i < num; i++ {
		if i == 0 {
			shard = relData.DataSlice(i*divide, (i+1)*divide+mod)
		} else {
			shard = relData.DataSlice(i*divide+mod, (i+1)*divide+mod)
		}
		ds, err := tbl.buildLocalDataSource(ctx, txnOffset, shard, CheckAll)
		if err != nil {
			return nil, err
		}
		rd := NewReader(
			ctx,
			proc,
			tbl.getTxn().engine,
			def,
			tbl.db.op.SnapshotTS(),
			expr,
			ds,
		)
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

	// if the table is created in this txn, skip
	accountId, err := defines.GetAccountId(ctx)
	if err != nil {
		return
	}
	if _, created := tbl.getTxn().createMap.Load(
		genTableKey(accountId, tbl.tableName, tbl.db.databaseId)); created {
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

	candidateBlks := make(map[types.Blockid]*objectio.BlockInfoInProgress)

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

			ForeachBlkInObjStatsListInProgress(false, meta,
				func(blk objectio.BlockInfoInProgress, blkMeta objectio.BlockObject) bool {
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
					blk.EntryState = obj.EntryState
					blk.CommitTs = obj.CommitTS
					if obj.HasDeltaLoc {
						_, commitTs, ok := p.GetBockDeltaLoc(blk.BlockID)
						if ok {
							blk.CommitTs = commitTs
						}
					}
					blk.PartitionNum = -1
					candidateBlks[blk.BlockID] = &blk
					return true
				}, obj.ObjectStats)

			return
		}); err != nil {
		return true, err
	}

	var filter blockio.ReadFilterSearchFuncType
	buildFilter := func() blockio.ReadFilterSearchFuncType {
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

		basePKFilter := newBasePKFilter(inExpr, tbl.tableDef, tbl.proc.Load())
		blockReadPKFilter := newBlockReadPKFilter(tbl.tableDef.Pkey.PkeyColName, basePKFilter)

		return blockReadPKFilter.SortedSearchFunc
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
			filter = buildFilter()
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
	//for mo_tables, mo_database, mo_columns, pk always exist in memory.
	if tbl.tableName == catalog.MO_DATABASE ||
		tbl.tableName == catalog.MO_TABLES ||
		tbl.tableName == catalog.MO_COLUMNS {
		logutil.Warnf("mo table:%s always exist in memory", tbl.tableName)
		return true, nil
	}
	//need check pk whether exist on S3 block.
	return tbl.PKPersistedBetween(
		snap,
		from,
		to,
		keysVector)
}

// TODO::refactor in next PR
func (tbl *txnTable) transferDeletes(
	ctx context.Context,
	state *logtailreplay.PartitionState,
	deleteObjs,
	createObjs map[objectio.ObjectNameShort]struct{},
) error {
	var blks []objectio.BlockInfoInProgress
	sid := tbl.proc.Load().GetService()
	relData := buildBlockListRelationData()
	relData.AppendBlockInfo(objectio.EmptyBlockInfoInProgress)
	ds, err := tbl.buildLocalDataSource(ctx, 0, relData, SkipCheckPolicy(CheckCommittedS3Only))
	if err != nil {
		return err
	}
	{
		fs, err := fileservice.Get[fileservice.FileService](
			tbl.proc.Load().GetFileService(),
			defines.SharedFileServiceName)
		if err != nil {
			return err
		}
		var objDataMeta objectio.ObjectDataMeta
		var objMeta objectio.ObjectMeta
		for name := range createObjs {
			if obj, ok := state.GetObject(name); ok {
				objectStats := obj.ObjectStats
				location := obj.Location()
				if objMeta, err = objectio.FastLoadObjectMeta(
					ctx,
					&location,
					false,
					fs); err != nil {
					return err
				}
				objDataMeta = objMeta.MustDataMeta()
				for i := 0; i < int(objectStats.BlkCnt()); i++ {
					blkMeta := objDataMeta.GetBlockMeta(uint32(i))
					metaLoc := blockio.EncodeLocation(
						obj.Location().Name(),
						obj.Location().Extent(),
						blkMeta.GetRows(),
						blkMeta.GetID(),
					)
					bid := objectio.BuildObjectBlockid(objectStats.ObjectName(), uint16(i))
					blkInfo := objectio.BlockInfoInProgress{
						BlockID:    *bid,
						EntryState: obj.EntryState,
						Sorted:     obj.Sorted,
						MetaLoc:    *(*[objectio.LocationLen]byte)(unsafe.Pointer(&metaLoc[0])),
						CommitTs:   obj.CommitTS,
					}
					if obj.HasDeltaLoc {
						_, commitTs, ok := state.GetBockDeltaLoc(blkInfo.BlockID)
						if ok {
							blkInfo.CommitTs = commitTs
						}
					}
					blks = append(blks, blkInfo)
				}
			}
		}

	}

	for _, entry := range tbl.getTxn().writes {
		if entry.IsGeneratedByTruncate() || entry.tableId != tbl.tableId {
			continue
		}
		if (entry.typ == DELETE || entry.typ == DELETE_TXN) && entry.fileName == "" {
			pkVec := entry.bat.GetVector(1)
			rowids := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
			beTransfered := 0
			toTransfer := 0
			for i, rowid := range rowids {
				blkid, _ := rowid.Decode()
				if _, ok := deleteObjs[*objectio.ShortName(&blkid)]; ok {
					toTransfer++
					newId, ok, err := tbl.readNewRowid(pkVec, i, blks, ds)
					if err != nil {
						return err
					}
					if ok {
						newBlockID, _ := newId.Decode()
						trace.GetService(sid).ApplyTransferRowID(
							tbl.db.op.Txn().ID,
							tbl.tableId,
							rowids[i][:],
							newId[:],
							blkid[:],
							newBlockID[:],
							pkVec,
							i)
						rowids[i] = newId
						beTransfered++
					}
				}
			}
			if beTransfered != toTransfer {
				return moerr.NewInternalErrorNoCtx("transfer deletes failed")
			}
		}
	}
	return nil
}

func (tbl *txnTable) readNewRowid(
	vec *vector.Vector, row int, blks []objectio.BlockInfoInProgress, ds engine.DataSource,
) (types.Rowid, bool, error) {
	var auxIdCnt int32
	var typ plan.Type
	var rowid types.Rowid
	var objMeta objectio.ObjectMeta

	columns := []uint16{objectio.SEQNUM_ROWID}
	colTypes := []types.Type{objectio.RowidType}
	tableDef := tbl.GetTableDef(context.TODO())
	for _, col := range tableDef.Cols {
		if col.Name == tableDef.Pkey.PkeyColName {
			typ = col.Typ
			columns = append(columns, uint16(col.Seqnum))
			colTypes = append(colTypes, types.T(col.Typ.Id).ToType())
		}
	}
	constExpr := getConstExpr(int32(vec.GetType().Oid),
		rule.GetConstantValue(vec, true, uint64(row)))
	filter, err := tbl.newPkFilter(newColumnExpr(1, typ, tableDef.Pkey.PkeyColName), constExpr)
	if err != nil {
		return rowid, false, err
	}
	columnMap := make(map[int]int)
	auxIdCnt += plan2.AssignAuxIdForExpr(filter, auxIdCnt)
	zms := make([]objectio.ZoneMap, auxIdCnt)
	vecs := make([]*vector.Vector, auxIdCnt)
	plan2.GetColumnMapByExprs([]*plan.Expr{filter}, tableDef, columnMap)
	objFilterMap := make(map[objectio.ObjectNameShort]bool)
	for _, blk := range blks {
		location := blk.MetaLocation()
		if hit, ok := objFilterMap[*location.ShortName()]; !ok {
			if objMeta, err = objectio.FastLoadObjectMeta(
				tbl.proc.Load().Ctx, &location, false, tbl.getTxn().engine.fs,
			); err != nil {
				return rowid, false, err
			}
			hit = colexec.EvaluateFilterByZoneMap(tbl.proc.Load().Ctx, tbl.proc.Load(), filter,
				objMeta.MustDataMeta(), columnMap, zms, vecs)
			objFilterMap[*location.ShortName()] = hit
			if !hit {
				continue
			}
		} else if !hit {
			continue
		}
		// eval filter expr on the block
		blkMeta := objMeta.MustDataMeta().GetBlockMeta(uint32(location.ID()))
		if !colexec.EvaluateFilterByZoneMap(tbl.proc.Load().Ctx, tbl.proc.Load(), filter,
			blkMeta, columnMap, zms, vecs) {
			continue
		}
		// rowid + pk
		bat, err := blockio.BlockDataRead(
			tbl.proc.Load().Ctx, tbl.proc.Load().GetService(), &blk, ds, columns, colTypes, tbl.db.op.SnapshotTS(),
			nil, nil, blockio.BlockReadFilter{},
			tbl.getTxn().engine.fs, tbl.proc.Load().Mp(), tbl.proc.Load(), fileservice.Policy(0), "",
		)
		if err != nil {
			return rowid, false, err
		}
		vec, err := colexec.EvalExpressionOnce(tbl.getTxn().proc, filter, []*batch.Batch{bat})
		if err != nil {
			return rowid, false, err
		}
		bs := vector.MustFixedCol[bool](vec)
		for i, b := range bs {
			if b {
				rowids := vector.MustFixedCol[types.Rowid](bat.Vecs[0])
				vec.Free(tbl.proc.Load().Mp())
				bat.Clean(tbl.proc.Load().Mp())
				return rowids[i], true, nil
			}
		}
		vec.Free(tbl.proc.Load().Mp())
		bat.Clean(tbl.proc.Load().Mp())
	}
	return rowid, false, nil
}

func (tbl *txnTable) newPkFilter(pkExpr, constExpr *plan.Expr) (*plan.Expr, error) {
	return plan2.BindFuncExprImplByPlanExpr(tbl.proc.Load().Ctx, "=", []*plan.Expr{pkExpr, constExpr})
}

func (tbl *txnTable) MergeObjects(
	ctx context.Context,
	objstats []objectio.ObjectStats,
	policyName string,
	targetObjSize uint32,
) (*api.MergeCommitEntry, error) {
	snapshot := types.TimestampToTS(tbl.getTxn().op.SnapshotTS())
	state, err := tbl.getPartitionState(ctx)
	if err != nil {
		return nil, err
	}

	sortkeyPos := -1
	sortkeyIsPK := false
	if tbl.primaryIdx >= 0 && tbl.tableDef.Cols[tbl.primaryIdx].Name != catalog.FakePrimaryKeyColName {
		if tbl.clusterByIdx < 0 {
			sortkeyPos = tbl.primaryIdx
			sortkeyIsPK = true
		} else {
			panic(fmt.Sprintf("bad schema pk %v, ck %v", tbl.primaryIdx, tbl.clusterByIdx))
		}
	} else if tbl.clusterByIdx >= 0 {
		sortkeyPos = tbl.clusterByIdx
		sortkeyIsPK = false
	}

	var objInfos []logtailreplay.ObjectInfo
	if len(objstats) != 0 {
		objInfos = make([]logtailreplay.ObjectInfo, 0, len(objstats))
		for _, objstat := range objstats {
			info, exist := state.GetObject(*objstat.ObjectShortName())
			if !exist || (!info.DeleteTime.IsEmpty() && info.DeleteTime.LessEq(&snapshot)) {
				logutil.Errorf("object not visible: %s", info.String())
				return nil, moerr.NewInternalErrorNoCtx("object %s not exist", objstat.ObjectName().String())
			}
			objInfos = append(objInfos, info)
		}
	} else {
		objInfos = make([]logtailreplay.ObjectInfo, 0, len(objstats))
		iter, err := state.NewObjectsIter(snapshot, true)
		if err != nil {
			logutil.Errorf("txn: %s, error: %v", tbl.db.op.Txn().DebugString(), err)
			return nil, err
		}
		for iter.Next() {
			obj := iter.Entry().ObjectInfo
			if obj.EntryState {
				continue
			}
			if sortkeyPos != -1 {
				sortKeyZM := obj.SortKeyZoneMap()
				if !sortKeyZM.IsInited() {
					continue
				}
			}
			objInfos = append(objInfos, obj)
		}
		if len(policyName) != 0 {
			objInfos, err = applyMergePolicy(ctx, policyName, sortkeyPos, objInfos)
			if err != nil {
				return nil, err
			}
		}
	}

	if len(objInfos) < 2 {
		return nil, moerr.NewInternalErrorNoCtx("no matching objects")
	}

	tbl.ensureSeqnumsAndTypesExpectRowid()

	taskHost, err := newCNMergeTask(
		ctx, tbl, snapshot, state, // context
		sortkeyPos, sortkeyIsPK, // schema
		objInfos, // targets
		targetObjSize)
	if err != nil {
		return nil, err
	}

	err = mergesort.DoMergeAndWrite(ctx, tbl.getTxn().op.Txn().DebugString(), sortkeyPos, taskHost)
	if err != nil {
		taskHost.commitEntry.Err = err.Error()
		return taskHost.commitEntry, err
	}

	if !taskHost.DoTransfer() {
		return taskHost.commitEntry, nil
	}

	// if transfer info is too large, write it down to s3
	// transfer info size is only related to row count.
	rowCnt := 0
	for _, m := range taskHost.transferMaps {
		rowCnt += len(m)
	}
	// if transfer info is small, send it to tn directly.
	if rowCnt < 8000000 {
		size := len(taskHost.transferMaps)
		mappings := make([]api.BlkTransMap, size)
		for i := 0; i < size; i++ {
			mappings[i] = api.BlkTransMap{
				M: make(map[int32]api.TransDestPos),
			}
		}
		taskHost.commitEntry.Booking = &api.BlkTransferBooking{
			Mappings: mappings,
		}

		for i, m := range taskHost.transferMaps {
			for r, pos := range m {
				taskHost.commitEntry.Booking.Mappings[i].M[r] = api.TransDestPos{
					ObjIdx: pos.ObjIdx,
					BlkIdx: pos.BlkIdx,
					RowIdx: pos.RowIdx,
				}
			}
		}
	} else {
		if err := dumpTransferInfo(ctx, taskHost); err != nil {
			return taskHost.commitEntry, err
		}
		var locStr strings.Builder
		locations := taskHost.commitEntry.BookingLoc
		for _, filepath := range locations {
			locStr.WriteString(filepath)
			locStr.WriteString(",")
		}
		logutil.Infof("mergeblocks %v-%v on cn: write s3 transfer info %v",
			tbl.tableId, tbl.tableName, locStr.String())
	}

	// commit this to tn
	return taskHost.commitEntry, nil
}

func dumpTransferInfo(ctx context.Context, taskHost *cnMergeTask) (err error) {
	defer func() {
		if err != nil {
			locations := taskHost.commitEntry.BookingLoc
			for _, filepath := range locations {
				_ = taskHost.fs.Delete(ctx, filepath)
			}
		}
	}()
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
	batchSize := min(200*mpool.MB/len(columns)/int(unsafe.Sizeof(int32(0))), totalRows)

	buffer := batch.New(true, columns)

	releases := make([]func(), len(columns))
	for i := range columns {
		t := types.T_int32.ToType()
		vec, release := taskHost.GetVector(&t)
		err = vec.PreExtend(batchSize, taskHost.GetMPool())
		if err != nil {
			return err
		}
		buffer.Vecs[i] = vec
		releases[i] = release
	}
	objRowCnt := 0
	for blkIdx, transMap := range bookingMaps {
		for rowIdx, destPos := range transMap {
			vector.AppendFixed(buffer.Vecs[0], int32(blkIdx), false, taskHost.GetMPool())
			vector.AppendFixed(buffer.Vecs[1], rowIdx, false, taskHost.GetMPool())
			vector.AppendFixed(buffer.Vecs[2], destPos.ObjIdx, false, taskHost.GetMPool())
			vector.AppendFixed(buffer.Vecs[3], destPos.BlkIdx, false, taskHost.GetMPool())
			vector.AppendFixed(buffer.Vecs[4], destPos.RowIdx, false, taskHost.GetMPool())

			buffer.SetRowCount(buffer.RowCount() + 1)
			objRowCnt++

			if objRowCnt*len(columns)*int(unsafe.Sizeof(int32(0))) > 200*mpool.MB {
				filename := blockio.EncodeTmpFileName("tmp", "merge", time.Now().UTC().Unix())
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
		filename := blockio.EncodeTmpFileName("tmp", "merge", time.Now().UTC().Unix())
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
	return
}

func applyMergePolicy(
	ctx context.Context,
	policyName string,
	sortKeyPos int,
	objInfos []logtailreplay.ObjectInfo,
) ([]logtailreplay.ObjectInfo, error) {
	arg := cutBetween(policyName, "(", ")")
	if strings.HasPrefix(policyName, "small") {
		size := uint32(110 * common.Const1MBytes)
		i, err := units.RAMInBytes(arg)
		if err == nil && 10*common.Const1MBytes < i && i < 250*common.Const1MBytes {
			size = uint32(i)
		}
		return logtailreplay.NewSmall(size).Filter(objInfos), nil
	} else if strings.HasPrefix(policyName, "overlap") {
		if sortKeyPos == -1 {
			return objInfos, nil
		}
		maxObjects := 100
		i, err := strconv.Atoi(arg)
		if err == nil {
			maxObjects = i
		}
		return logtailreplay.NewOverlap(maxObjects).Filter(objInfos), nil
	}

	return nil, moerr.NewInvalidInput(ctx, "invalid merge policy name")
}

func cutBetween(s, start, end string) string {
	i := strings.Index(s, start)
	if i >= 0 {
		j := strings.Index(s[i:], end)
		if j >= 0 {
			return s[i+len(start) : i+j]
		}
	}
	return ""
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
			if entry.typ == INSERT || entry.typ == INSERT_TXN {
				rows = rows + uint64(entry.bat.RowCount())
			} else {
				if entry.bat.GetVector(0).GetType().Oid == types.T_Rowid {
					/*
						CASE:
						create table t1(a int);
						begin;
						truncate t1; //txnDatabase.Truncate will DELETE mo_tables
						show tables; // t1 must be shown
					*/
					if entry.databaseId == catalog.MO_CATALOG_ID &&
						entry.tableId == catalog.MO_TABLES_ID &&
						entry.truncate {
						return
					}
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
