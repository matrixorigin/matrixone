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
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
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
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/shardservice"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
)

const (
	AllColumns = "*"
)

var traceFilterExprInterval atomic.Uint64
var traceFilterExprInterval2 atomic.Uint64

var _ engine.Relation = new(txnTable)

func newTxnTable(
	ctx context.Context,
	db *txnDatabase,
	item cache.TableItem,
) (engine.Relation, error) {
	txn := db.getTxn()
	process := txn.proc
	eng := txn.engine

	tbl := &txnTableDelegate{
		origin: newTxnTableWithItem(
			db,
			item,
			process,
			eng,
		),
	}

	ps := process.GetPartitionService()
	if ps.Enabled() && item.IsIndexTable() {
		_, _, r, err := eng.GetRelationById(
			ctx,
			db.op,
			item.ExtraInfo.ParentTableID,
		)
		if err != nil && !strings.Contains(err.Error(), "can not find table by id") {
			return nil, err
		}
		tbl.parent = r
	}

	tbl.isLocal = tbl.isLocalFunc

	if db.databaseId != catalog.MO_CATALOG_ID {
		if ps.Enabled() && (item.IsPartitionTable() || tbl.IsPartitionIndexTable()) {
			proc := tbl.origin.proc.Load()

			var combined *combinedTxnTable
			if item.IsPartitionTable() {
				metadata, err := ps.GetPartitionMetadata(
					ctx,
					item.Id,
					db.op,
				)
				if err != nil {
					return nil, err
				}
				combined = newCombinedTxnTable(
					tbl.origin,
					getPartitionTableFunc(proc, metadata, db),
					getPruneTablesFunc(proc, metadata, db),
					getPartitionPrunePKFunc(proc, metadata, db),
				)
			} else {
				relations, err := tbl.getPartitionIndexesTables(process)
				if err != nil {
					return nil, err
				}
				combined = newCombinedTxnTable(
					tbl.origin,
					getArrayTableFunc(relations),
					getArrayPruneTablesFunc(relations),
					getArrayPrunePKFunc(relations),
				)
			}

			tbl.combined.tbl = combined
			tbl.combined.is = true
		}

		tbl.shard.service = shardservice.GetService(process.GetService())
		tbl.shard.is = false

		if tbl.shard.service.Config().Enable {
			tableID, policy, is, err := tbl.shard.service.GetShardInfo(item.Id)
			if err != nil {
				return nil, err
			}

			tbl.shard.is = is
			tbl.shard.policy = policy
			tbl.shard.tableID = tableID
		}
	}

	return tbl, nil
}

func (tbl *txnTable) getEngine() engine.Engine {
	return tbl.eng
}

func (tbl *txnTable) getTxn() *Transaction {
	return tbl.db.getTxn()
}

// true if the prefetch is received
// false if the prefetch is rejected
func (tbl *txnTable) PrefetchAllMeta(ctx context.Context) bool {
	// TODO: remove this check
	if !tbl.db.op.IsSnapOp() {
		return tbl.eng.PrefetchTableMeta(
			ctx,
			pb.StatsInfoKey{
				AccId:      tbl.accountId,
				DatabaseID: tbl.db.databaseId,
				TableID:    tbl.tableId,
			},
		)
	}
	return true
}

func (tbl *txnTable) Stats(ctx context.Context, sync bool) (*pb.StatsInfo, error) {
	//Stats only stats the committed data of the table.
	if tbl.db.getTxn().tableOps.existCreatedInTxn(tbl.tableId) ||
		strings.ToUpper(tbl.relKind) == "V" {
		return nil, nil
	}
	return tbl.getEngine().Stats(ctx, pb.StatsInfoKey{
		AccId:      tbl.accountId,
		DatabaseID: tbl.db.databaseId,
		TableID:    tbl.tableId,
		TableName:  tbl.tableName,
		DbName:     tbl.db.databaseName,
	}, sync), nil
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
					vs := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
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

func ForeachVisibleObjects(
	state *logtailreplay.PartitionState,
	ts types.TS,
	fn func(obj objectio.ObjectEntry) error,
	executor ConcurrentExecutor,
	visitTombstone bool,
) (err error) {
	iter, err := state.NewObjectsIter(ts, true, visitTombstone)
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
	onObjFn := func(obj objectio.ObjectEntry) error {
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

	if err = ForeachVisibleObjects(
		part,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		onObjFn,
		nil,
		false,
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

func (tbl *txnTable) GetColumMetadataScanInfo(ctx context.Context, name string, visitTombstone bool) ([]*plan.MetadataScanInfo, error) {
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
	onObjFn := func(obj objectio.ObjectEntry) error {
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

	if err = ForeachVisibleObjects(
		state,
		types.TimestampToTS(tbl.db.op.SnapshotTS()),
		onObjFn,
		nil,
		visitTombstone,
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

	return infoList, nil
}

func (tbl *txnTable) GetEngineType() engine.EngineType {
	return engine.Disttae
}

func (tbl *txnTable) GetProcess() any {
	return tbl.proc.Load()
}

func (tbl *txnTable) CollectTombstones(
	ctx context.Context,
	txnOffset int,
	policy engine.TombstoneCollectPolicy,
) (engine.Tombstoner, error) {
	tombstone := readutil.NewEmptyTombstoneData()

	//collect uncommitted tombstones

	if policy&engine.Policy_CollectUncommittedTombstones != 0 {

		offset := txnOffset
		if tbl.db.op.IsSnapOp() {
			offset = tbl.getTxn().GetSnapshotWriteOffset()
		}

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
					vs := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
					tombstone.AppendInMemory(vs...)
				}
			})

		//collect uncommitted in-memory tombstones belongs to blocks persisted by CN writing S3
		tbl.getTxn().deletedBlocks.getDeletedRowIDs(func(row types.Rowid) {
			tombstone.AppendInMemory(row)
		})

		//collect uncommitted persisted tombstones.
		if err := tbl.getTxn().getUncommittedS3Tombstone(
			func(stats *objectio.ObjectStats) {
				tombstone.AppendFiles(*stats)
			}); err != nil {
			return nil, err
		}
	}

	//collect committed tombstones.

	if policy&engine.Policy_CollectCommittedTombstones != 0 {

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
				tombstone.AppendInMemory(entry.RowID)
			}
			iter.Close()
		}

		//tombstone.SortInMemory()
		//collect committed persisted tombstones from partition state.
		snapshot := types.TimestampToTS(tbl.db.op.Txn().SnapshotTS)
		err = state.CollectTombstoneObjects(snapshot,
			func(stats *objectio.ObjectStats) {
				tombstone.AppendFiles(*stats)
			})
		if err != nil {
			return nil, err
		}
	}
	tombstone.SortInMemory()
	return tombstone, nil
}

// Ranges returns all unmodified blocks from the table.
// Parameters:
//   - ctx: Context used to control the lifecycle of the request.
//   - exprs: A slice of expressions used to filter data.
//   - txnOffset: Transaction offset used to specify the starting position for reading data.
func (tbl *txnTable) Ranges(ctx context.Context, rangesParam engine.RangesParam) (data engine.RelData, err error) {
	if len(rangesParam.BlockFilters) == 0 && rangesParam.PreAllocBlocks > 128 && !rangesParam.DontSupportRelData {
		//no block filters, no agg optimization, not partition table, then we can return object list instead of block list
		return tbl.getObjList(ctx, rangesParam)
	}
	return tbl.doRanges(ctx, rangesParam)
}

func (tbl *txnTable) getObjList(ctx context.Context, rangesParam engine.RangesParam) (data engine.RelData, err error) {
	needUncommited := rangesParam.Policy&engine.Policy_CollectUncommittedData != 0

	var part *logtailreplay.PartitionState
	if part, err = tbl.getPartitionState(ctx); err != nil {
		return
	}

	objRelData := &readutil.ObjListRelData{
		NeedFirstEmpty: needUncommited,
		Rsp:            rangesParam.Rsp,
		PState:         part,
	}

	if needUncommited {
		objRelData.TotalBlocks = 1 // first empty block
		uncommittedObjects, _ := tbl.collectUnCommittedDataObjs(rangesParam.TxnOffset)
		for i := range uncommittedObjects {
			objRelData.AppendObj(&uncommittedObjects[i])
		}
	}

	// get the table's snapshot
	if !(rangesParam.Policy&engine.Policy_CollectCommittedData != 0) {
		part = nil
	}

	if err = ForeachSnapshotObjects(
		tbl.db.op.SnapshotTS(),
		func(obj objectio.ObjectEntry, isCommitted bool) (err2 error) {
			//if need to shuffle objects
			if plan2.ShouldSkipObjByShuffle(rangesParam.Rsp, &obj.ObjectStats) {
				return
			}
			objRelData.AppendObj(&obj.ObjectStats)
			return
		},
		part, nil, nil...); err != nil {
		return nil, err
	}

	data = objRelData
	return
}

func (tbl *txnTable) doRanges(ctx context.Context, rangesParam engine.RangesParam) (data engine.RelData, err error) {
	sid := tbl.proc.Load().GetService()
	start := time.Now()
	seq := tbl.db.op.NextSequence()

	var part *logtailreplay.PartitionState
	var uncommittedObjects []objectio.ObjectStats
	blocks := objectio.PreAllocBlockInfoSlice(rangesParam.PreAllocBlocks)
	if rangesParam.Policy&engine.Policy_CollectCommittedInmemData != 0 ||
		rangesParam.Policy&engine.Policy_CollectUncommittedInmemData != 0 {
		blocks.AppendBlockInfo(&objectio.EmptyBlockInfo)
	}

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
		} else if rangesLen < 30 {
			step = uint64(10)
		} else {
			slowStep = uint64(1)
		}
		tbl.enableLogFilterExpr.Store(false)
		if traceFilterExprInterval.Add(step) >= 10000000 {
			traceFilterExprInterval.Store(0)
			tbl.enableLogFilterExpr.Store(true)
		}
		if traceFilterExprInterval2.Add(slowStep) >= 500 {
			traceFilterExprInterval2.Store(0)
			tbl.enableLogFilterExpr.Store(true)
		}

		if rangesLen >= 100 {
			tbl.enableLogFilterExpr.Store(true)
		}

		if ok, _ := objectio.RangesLogInjected(tbl.db.databaseName, tbl.tableDef.Name); ok ||
			err != nil ||
			tbl.enableLogFilterExpr.Load() ||
			cost > 5*time.Second {
			logutil.Info(
				"txn.table.ranges.log",
				zap.String("name", tbl.tableDef.Name),
				zap.String("exprs", plan2.FormatExprs(
					rangesParam.BlockFilters, plan2.FormatOption{
						ExpandVec: false,
						MaxDepth:  5,
					},
				)),
				zap.Uint64("tbl-id", tbl.tableId),
				zap.String("txn", tbl.db.op.Txn().DebugString()),
				zap.Int("blocks", blocks.Len()),
				zap.String("ps", fmt.Sprintf("%p", part)),
				zap.Duration("cost", cost),
				zap.Error(err),
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

	if rangesParam.Policy&engine.Policy_CollectUncommittedPersistedData != 0 {
		uncommittedObjects, _ = tbl.collectUnCommittedDataObjs(rangesParam.TxnOffset)
	}

	// get the table's snapshot
	if rangesParam.Policy&engine.Policy_CollectCommittedPersistedData != 0 {
		if part, err = tbl.getPartitionState(ctx); err != nil {
			return
		}
	}

	if err = tbl.rangesOnePart(
		ctx,
		part,
		tbl.GetTableDef(ctx),
		rangesParam,
		&blocks,
		tbl.proc.Load(),
		uncommittedObjects,
	); err != nil {
		return
	}

	if part == nil {
		if part, err = tbl.getPartitionState(ctx); err != nil {
			return
		}
	}

	blklist := readutil.NewBlockListRelationData(
		0,
		readutil.WithPartitionState(part))
	blklist.SetBlockList(blocks)
	data = blklist

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
	tableDef *plan.TableDef,
	rangesParam engine.RangesParam,
	outBlocks *objectio.BlockInfoSlice, // output marshaled block list after filtering
	proc *process.Process,
	uncommittedObjects []objectio.ObjectStats,
) (err error) {
	var done bool

	if done, err = readutil.TryFastFilterBlocks(
		ctx,
		tbl.db.op.SnapshotTS(),
		tbl.tableDef,
		rangesParam,
		state,
		nil,
		uncommittedObjects,
		outBlocks,
		tbl.PrefetchAllMeta,
		tbl.getTxn().engine.fs,
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
			zap.String("exprs", plan2.FormatExprs(
				rangesParam.BlockFilters,
				plan2.FormatOption{
					ExpandVec:       true,
					ExpandVecMaxLen: 2,
					MaxDepth:        5,
				},
			)),
		)
	}

	hasFoldExpr := plan2.HasFoldExprForList(rangesParam.BlockFilters)
	if hasFoldExpr {
		rangesParam.BlockFilters = nil
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
	for _, expr := range rangesParam.BlockFilters {
		auxIdCnt += plan2.AssignAuxIdForExpr(expr, auxIdCnt)
	}

	columnMap := make(map[int]int)
	if auxIdCnt > 0 {
		zms = make([]objectio.ZoneMap, auxIdCnt)
		vecs = make([]*vector.Vector, auxIdCnt)
		plan2.GetColumnMapByExprs(rangesParam.BlockFilters, tableDef, columnMap)
	}

	errCtx := errutil.ContextWithNoReport(ctx, true)

	if err = ForeachSnapshotObjects(
		tbl.db.op.SnapshotTS(),
		func(obj objectio.ObjectEntry, isCommitted bool) (err2 error) {
			//if need to shuffle objects
			if plan2.ShouldSkipObjByShuffle(rangesParam.Rsp, &obj.ObjectStats) {
				return
			}
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
					for _, expr := range rangesParam.BlockFilters {
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

			objectio.ForeachBlkInObjStatsList(true, meta, func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				skipBlk := false

				if auxIdCnt > 0 {
					// eval filter expr on the block
					for _, expr := range rangesParam.BlockFilters {
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

				blk.SetFlagByObjStats(&obj.ObjectStats)

				outBlocks.AppendBlockInfo(&blk)

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
func (tbl *txnTable) collectUnCommittedDataObjs(txnOffset int) ([]objectio.ObjectStats, map[objectio.ObjectNameShort]struct{}) {
	var unCommittedObjects []objectio.ObjectStats
	unCommittedObjNames := make(map[objectio.ObjectNameShort]struct{})

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
				unCommittedObjNames[*stats.ObjectShortName()] = struct{}{}
			}
		})

	return unCommittedObjects, unCommittedObjNames
}

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
			if !objectio.IsPhysicalAddr(attr.Attr.Name) {
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

func (tbl *txnTable) RefeshTableDef(ctx context.Context) {
	tbl.tableDef = nil
	tbl.GetTableDef(ctx)
}

func (tbl *txnTable) GetTableDef(ctx context.Context) *plan.TableDef {
	if tbl.tableDef == nil {
		var clusterByDef *plan.ClusterByDef
		var cols []*plan.ColDef
		var defs []*plan.TableDef_DefType
		var properties []*plan.Property
		var TableType string
		var Createsql string
		var viewSql *plan.ViewDef
		var foreignKeys []*plan.ForeignKeyDef
		var primarykey *plan.PrimaryKeyDef
		var indexes []*plan.IndexDef
		var refChildTbls []uint64
		var hasRowId bool
		var partition *plan.Partition

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
				if objectio.IsPhysicalAddr(attr.Attr.Name) {
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
			partition = &plan.Partition{}
			err := partition.Unmarshal(([]byte)(tbl.partition))
			if err != nil {
				//panic(fmt.Sprintf("cannot unmarshal partition metadata information: %s", err))
				return nil
			}
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
			Fkeys:         foreignKeys,
			RefChildTbls:  refChildTbls,
			ClusterBy:     clusterByDef,
			Indexes:       indexes,
			Version:       tbl.version,
			DbId:          tbl.GetDBID(ctx),
			Partition:     partition,
			LogicalId:     tbl.logicalId,
		}
		if tbl.extraInfo != nil {
			tbl.tableDef.FeatureFlag = tbl.extraInfo.FeatureFlag
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

func (tbl *txnTable) isCreatedInTxn(_ context.Context) (bool, error) {
	// test or mo_table_stats
	if tbl.fake {
		return false, nil
	}

	if tbl.remoteWorkspace {
		return tbl.createdInTxn, nil
	}

	if tbl.db.op.IsSnapOp() || catalog.IsSystemTable(tbl.tableId) {
		// if the operation is snapshot read, isCreatedInTxn can not be called by AlterTable
		// So if the snapshot read want to subcribe logtail tail, let it go ahead.
		return false, nil
	}

	return tbl.db.getTxn().tableOps.existCreatedInTxn(tbl.tableId), nil

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
			case api.AlterKind_ReplaceDef:
				// Rollback for ReplaceDef is handled by restoring defs
			case api.AlterKind_RenameColumn:
				// RenameColumn takes effect in form of ReplaceDef
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
	var hasReplaceDef bool
	var replaceDefReq *api.AlterTableReq
	renameColMap := make(map[string]string)
	for _, req := range reqs {
		switch req.GetKind() {
		case api.AlterKind_AddPartition:
			// TODO: reimplement partition
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
		case api.AlterKind_ReplaceDef:
			hasReplaceDef = true
			replaceDefReq = req
		case api.AlterKind_RenameColumn:
			hasReplaceDef = true
			re := req.GetRenameCol()
			renameColMap[re.OldName] = re.NewName
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

	var baseDefs []engine.TableDef
	// update TableDef
	if hasReplaceDef {
		// When ReplaceDef exists, replace the entire table definition
		replaceDef := replaceDefReq.GetReplaceDef()
		baseDefs, _, _ = engine.PlanDefsToExeDefs(replaceDef.Def)
		baseDefs = append(baseDefs, engine.PlanColsToExeCols(replaceDef.Def.Cols)...)
	} else {
		baseDefs = append([]engine.TableDef{}, tbl.defs...)
	}

	// 0. check if the table is created in txn.
	// For a table created in txn, alter means to recreate table and put relating dml/alter batch behind the new create batch.
	// For a normal table, alter means sending Alter request to TN, no creating command, and no dropping command.
	createdInTxn, err := tbl.isCreatedInTxn(ctx)
	if err != nil {
		return err
	}
	if !createdInTxn {
		tbl.version += 1
		appendDef = append(appendDef, &engine.VersionDef{Version: tbl.version})
		// For normal Alter, send Alter request to TN
		reqPayload := make([][]byte, 0, len(reqs))
		for _, req := range reqs {
			if req.GetKind() == api.AlterKind_ReplaceDef {
				// ReplaceDef works only in CN, do not send it to TN
				continue
			}
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

	// update table defs after deleting old table metadata
	tbl.defs = append(baseDefs, appendDef...)
	tbl.RefeshTableDef(ctx)

	ctx = context.WithValue(ctx, defines.LogicalIdKey{}, tbl.logicalId)

	//------------------------------------------------------------------------------------------------------------------
	// 2. insert new table metadata
	if err := tbl.db.createWithID(ctx, tbl.tableName, tbl.tableId, tbl.defs, !createdInTxn, tbl.extraInfo); err != nil {
		return err
	}
	if createdInTxn {
		// 3. adjust writes for the table
		txn.Lock()
		for i, n := 0, len(txn.writes); i < n; i++ {
			if cur := txn.writes[i]; cur.tableId == tbl.tableId && cur.bat != nil && cur.bat.RowCount() > 0 {
				if sels, exist := txn.batchSelectList[cur.bat]; exist && len(sels) == cur.bat.RowCount() {
					continue
				}
				txn.writes = append(txn.writes, txn.writes[i]) // copy by value
				transfered := &txn.writes[len(txn.writes)-1]
				transfered.tableName = tbl.tableName // in case renaming
				transfered.bat, err = cur.bat.Dup(txn.proc.Mp())
				if len(renameColMap) > 0 {
					for i, attr := range transfered.bat.Attrs {
						if newName, ok := renameColMap[attr]; ok {
							transfered.bat.Attrs[i] = newName
						}
					}
				}
				if err != nil {
					return err
				}
				for j := 0; j < cur.bat.RowCount(); j++ {
					txn.batchSelectList[cur.bat] = append(txn.batchSelectList[cur.bat], int64(j))
				}

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
	return tbl.getTxn().dumpBatch(ctx, tbl.getTxn().GetSnapshotWriteOffset())
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
		typs = append(typs, vector.ProtoTypeToType(col.Typ))
	}
	tbl.seqnums = idxs
	tbl.typs = typs
}

func (tbl *txnTable) rewriteObjectByDeletion(
	ctx context.Context,
	obj objectio.ObjectStats,
	blockDeletes map[objectio.Blockid][]int64,
) (*batch.Batch, string, error) {

	proc := tbl.proc.Load()

	var (
		err error
		fs  fileservice.FileService
	)

	if fs, err = colexec.GetSharedFSFromProc(proc); err != nil {
		return nil, "", err
	}

	s3Writer := colexec.NewCNS3DataWriter(
		proc.Mp(), fs, tbl.tableDef, -1, false,
	)

	defer func() { s3Writer.Close() }()

	var (
		bat      *batch.Batch
		stats    []objectio.ObjectStats
		fileName string
	)

	defer func() {
		if bat != nil {
			bat.Clean(proc.Mp())
		}
	}()

	objectio.ForeachBlkInObjStatsList(
		true,
		nil,
		func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
			deletes := blockDeletes[blk.BlockID]
			slices.Sort(deletes)
			if bat == nil {
				bat = batch.NewWithSize(len(tbl.seqnums))
			}
			bat.CleanOnlyData()

			if err = blockio.CopyBlockData(
				tbl.getTxn().proc.Ctx,
				blk.MetaLoc[:],
				deletes,
				tbl.seqnums,
				tbl.typs,
				bat,
				tbl.getTxn().engine.fs,
				tbl.getTxn().proc.GetMPool(),
			); err != nil {
				return false
			}

			if bat.RowCount() == 0 {
				return true
			}

			if err = s3Writer.Write(ctx, bat); err != nil {
				return false
			}

			return true
		},
		obj,
	)

	if err != nil {
		return nil, fileName, err
	}

	if stats, err = s3Writer.Sync(ctx); err != nil {
		return nil, fileName, err
	}

	if bat, err = s3Writer.FillBlockInfoBat(); err != nil {
		return nil, fileName, err
	}

	if len(stats) != 0 {
		fileName = stats[0].ObjectLocation().String()
	}

	ret, err := bat.Dup(proc.Mp())

	return ret, fileName, err
}

func (tbl *txnTable) Delete(
	ctx context.Context, bat *batch.Batch, name string,
) error {
	if tbl.db.op.IsSnapOp() {
		return moerr.NewInternalErrorNoCtx("delete operation is not allowed in snapshot transaction")
	}

	var (
		deletionTyp = bat.Attrs[0]
	)

	// this should not happen, just in case slice out of bound
	//if len(bat.Attrs) == 0 {
	//	logutil.Info("deletion bat has empty attr",
	//		zap.String("name", name),
	//		zap.String("bat", common.MoBatchToString(bat, 20)))
	//
	//	if objectio.IsPhysicalAddr(name) {
	//		deletionTyp = name
	//	} else {
	//		typStr := string(name[len(name)-1])
	//		typ, err := strconv.ParseInt(typStr, 10, 64)
	//		if err != nil {
	//			return err
	//		}
	//
	//		if typ == deletion.FlushDeltaLoc {
	//			deletionTyp = catalog.ObjectMeta_ObjectStats
	//		}
	//	}
	//}

	switch deletionTyp {
	case catalog.Row_ID:
		bat = tbl.getTxn().deleteBatch(bat, tbl.db.databaseId, tbl.tableId)
		if bat.RowCount() == 0 {
			return nil
		}
		return tbl.writeTnPartition(ctx, bat)

	case catalog.ObjectMeta_ObjectStats:
		tbl.getTxn().hasS3Op.Store(true)
		stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(0))
		fileName := stats.ObjectLocation().String()

		if err := tbl.getTxn().WriteFile(DELETE, tbl.accountId, tbl.db.databaseId, tbl.tableId,
			tbl.db.databaseName, tbl.tableName, fileName, bat, tbl.getTxn().tnStores[0]); err != nil {
			return err
		}

		return nil

	default:
		panic(moerr.NewInternalErrorNoCtxf(
			"Unsupport type for table delete %s", common.MoBatchToString(bat, 100)))
	}
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

	tombstones, err := tbl.CollectTombstones(ctx, txnOffset, engine.Policy_CollectAllTombstones)
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

	newRelData, err := readutil.UnmarshalRelationData(buf)
	if err != nil {
		return
	}

	source = readutil.NewRemoteDataSource(
		ctx,
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

	return tbl.buildLocalDataSource(
		ctx,
		txnOffset,
		ranges,
		engine.Policy_CheckAll,
		engine.GeneralLocalDataSource)
}

func extractPStateFromRelData(
	ctx context.Context,
	tbl *txnTable,
	relData engine.RelData,
) (*logtailreplay.PartitionState, error) {

	var part any

	if x1, o1 := relData.(*readutil.ObjListRelData); o1 {
		part = x1.PState
	} else if x2, o2 := relData.(*readutil.BlockListRelData); o2 {
		part = x2.GetPState()
	}

	if part == nil {
		// why the partition will be nil ??
		sql := ""
		if p := tbl.proc.Load().GetStmtProfile(); p != nil {
			sql = p.GetSqlOfStmt()
		}

		logutil.Warn("RELDATA-WITH-EMPTY-PSTATE",
			zap.String("db", tbl.db.databaseName),
			zap.String("table", tbl.tableName),
			zap.String("sql", commonUtil.Abbreviate(sql, 500)),
			zap.String("relDataType", fmt.Sprintf("%T", relData)),
			zap.String("relDataContent", relData.String()),
			zap.String("stack", string(debug.Stack())))

		pState, err := tbl.getPartitionState(ctx)
		if err != nil {
			return nil, err
		}

		return pState, nil
	}

	return part.(*logtailreplay.PartitionState), nil
}

func (tbl *txnTable) buildLocalDataSource(
	ctx context.Context,
	txnOffset int,
	relData engine.RelData,
	policy engine.TombstoneApplyPolicy,
	category engine.DataSourceType,
) (source engine.DataSource, err error) {

	switch relData.GetType() {
	case engine.RelDataObjList, engine.RelDataBlockList:
		var pState *logtailreplay.PartitionState

		pState, err = extractPStateFromRelData(ctx, tbl, relData)
		if err != nil {
			return nil, err
		}

		ranges := relData.GetBlockInfoSlice()
		skipReadMem := !ranges.Get(0).IsMemBlk()

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
				pState,
				ranges,
				relData.GetTombstones(),
				skipReadMem,
				policy,
				category,
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
	filterHint engine.FilterHint,
) ([]engine.Reader, error) {
	var rds []engine.Reader
	proc := p.(*process.Process)

	//if orderBy && num != 1 {
	//	return nil, moerr.NewInternalErrorNoCtx("orderBy only support one reader")
	//}

	//relData maybe is nil, indicate that only read data from memory.
	if relData == nil || relData.DataCnt() == 0 {
		part, err := tbl.getPartitionState(ctx)
		if err != nil {
			return nil, err
		}

		relData = readutil.NewBlockListRelationData(
			1,
			readutil.WithPartitionState(part))
	}

	blkCnt := relData.DataCnt()
	newNum := num
	if blkCnt < num {
		newNum = blkCnt
		for i := 0; i < num-blkCnt; i++ {
			rds = append(rds, new(readutil.EmptyReader))
		}
	}

	def := tbl.GetTableDef(ctx)
	shards := relData.Split(newNum)
	for i := 0; i < newNum; i++ {
		ds, err := tbl.buildLocalDataSource(ctx, txnOffset, shards[i], tombstonePolicy, engine.GeneralLocalDataSource)
		if err != nil {
			return nil, err
		}
		rd, err := readutil.NewReader(
			ctx,
			proc.Mp(),
			tbl.getTxn().engine.packerPool,
			tbl.getTxn().engine.fs,
			def,
			tbl.db.op.SnapshotTS(),
			expr,
			ds,
			readutil.GetThresholdForReader(newNum),
			filterHint,
		)
		if err != nil {
			return nil, err
		}

		rds = append(rds, rd)
	}
	return rds, nil
}

func (tbl *txnTable) BuildShardingReaders(
	ctx context.Context,
	p any,
	expr *plan.Expr,
	relData engine.RelData,
	num int,
	txnOffset int,
	orderBy bool,
	tombstonePolicy engine.TombstoneApplyPolicy,
) ([]engine.Reader, error) {
	panic("Not Support")
}

func (tbl *txnTable) getPartitionState(
	ctx context.Context,
) (ps *logtailreplay.PartitionState, err error) {
	// defer func() {
	// 	if tbl.tableId == catalog.MO_COLUMNS_ID {
	// 		logutil.Info("open partition state for mo_columns",
	// 			zap.String("txn", tbl.db.op.Txn().DebugString()),
	// 			zap.String("desc", ps.Desc(true)),
	// 			zap.String("pointer", fmt.Sprintf("%p", ps)))
	// 	}
	// }()

	var (
		eng          = tbl.eng.(*Engine)
		createdInTxn bool
	)

	createdInTxn, err = tbl.isCreatedInTxn(ctx)
	if err != nil {
		return nil, err
	}

	// no need to subscribe a view
	// for issue #19192
	if createdInTxn || strings.ToUpper(tbl.relKind) == "V" {
		//return an empty partition state.
		ps = tbl.getTxn().engine.GetOrCreateLatestPart(
			ctx,
			uint64(tbl.accountId),
			tbl.db.databaseId,
			tbl.tableId).Snapshot()
		return
	}

	// Subscribe a latest partition state
	if ps, err = eng.PushClient().toSubscribeTable(
		ctx,
		uint64(tbl.accountId),
		tbl.tableId,
		tbl.tableName,
		tbl.db.databaseId,
		tbl.db.databaseName,
	); err != nil {
		logutil.Error(
			"Txn-Table-ToSubscribeTable-Failed",
			zap.String("db-name", tbl.db.databaseName),
			zap.Uint64("db-id", tbl.db.databaseId),
			zap.String("tbl-name", tbl.tableName),
			zap.Uint64("tbl-id", tbl.tableId),
			zap.String("txn-info", tbl.db.op.Txn().DebugString()),
			zap.Bool("is-snapshot-op", tbl.db.op.IsSnapOp()),
			zap.Error(err),
		)

		// if the table not exists, try snapshot read
		if !moerr.IsMoErrCode(err, moerr.ErrNoSuchTable) {
			return nil, err
		}

	} else if ps != nil && ps.CanServe(types.TimestampToTS(tbl.db.op.SnapshotTS())) {
		return
	}

	//Try to create a snapshot partition state for the table through consume the history checkpoints.
	start, end := types.MaxTs(), types.MinTs()
	if ps != nil {
		start, end = ps.GetDuration()
	}
	logutil.Info(
		"Txn-Table-GetSSPS",
		zap.String("table-name", tbl.tableName),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("txn", tbl.db.op.Txn().DebugString()),
		zap.String("ps", fmt.Sprintf("%p", ps)),
		zap.String("start", start.ToString()),
		zap.String("end", end.ToString()),
		zap.Bool("is-snapshot", tbl.db.op.IsSnapOp()),
		zap.Error(err),
	)

	//If ps == nil, it indicates that subscribe failed due to 1: network timeout,
	//2:table id is too old for snapshot read,pls ref to issue:https://github.com/matrixorigin/matrixone/issues/17012

	// To get a partition state for snapshot read, we need to consume the history checkpoints.
	var (
		logger = logutil.Info
		msg    string
	)
	ps, err = tbl.getTxn().engine.getOrCreateSnapPartBy(
		ctx,
		tbl,
		types.TimestampToTS(tbl.db.op.Txn().SnapshotTS))

	start, end = types.MaxTs(), types.MinTs()
	if ps != nil {
		start, end = ps.GetDuration()
		msg = "table.get.snapshot.state.succeed"
	} else {
		logger = logutil.Error
		msg = "table.get.snapshot.state.failed"
	}

	logger(
		msg,
		zap.String("table-name", tbl.tableName),
		zap.Uint64("table-id", tbl.tableId),
		zap.String("txn", tbl.db.op.Txn().DebugString()),
		zap.String("ps", fmt.Sprintf("%p", ps)),
		zap.String("start", start.ToString()),
		zap.String("end", end.ToString()),
		zap.Bool("is-snapshot", tbl.db.op.IsSnapOp()),
		zap.Error(err),
	)
	return
}

func (tbl *txnTable) PKPersistedBetween(
	p *logtailreplay.PartitionState,
	from types.TS,
	to types.TS,
	keys *vector.Vector,
	checkTombstone bool,
) (changed bool, err error) {

	v2.TxnPKChangeCheckTotalCounter.Inc()
	defer func() {
		if err != nil && changed {
			v2.TxnPKChangeCheckChangedCounter.Inc()
		}
	}()
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
		func(obj objectio.ObjectEntry) (err2 error) {
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

			objectio.ForeachBlkInObjStatsList(false, meta,
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

					blk.SetFlagByObjStats(&obj.ObjectStats)

					candidateBlks[blk.BlockID] = &blk
					return true
				}, obj.ObjectStats)

			return
		}); err != nil {
		return true, err
	}

	keys.InplaceSort()
	bytes, _ := keys.MarshalBinary()
	colExpr := readutil.NewColumnExpr(0, plan2.MakePlan2Type(keys.GetType()), tbl.tableDef.Pkey.PkeyColName)
	inExpr := plan2.MakeInExpr(
		tbl.proc.Load().Ctx,
		colExpr,
		int32(keys.Length()),
		bytes,
		false)

	basePKFilter, err := readutil.ConstructBasePKFilter(inExpr, tbl.tableDef, tbl.proc.Load().Mp())
	if err != nil {
		return false, err
	}

	filter, err := readutil.ConstructBlockPKFilter(
		catalog.IsFakePkName(tbl.tableDef.Pkey.PkeyColName),
		basePKFilter,
		nil,
	)
	if err != nil {
		return false, err
	}

	buildUnsortedFilter := func() objectio.ReadFilterSearchFuncType {
		inner := LinearSearchOffsetByValFactory(keys)
		return func(cacheVectors containers.Vectors) []int64 {
			if len(cacheVectors) == 0 || cacheVectors[0].Length() == 0 {
				return nil
			}
			return inner(&cacheVectors[0])
		}
	}

	cacheVectors := containers.NewVectors(1)
	//read block ,check if keys exist in the block.
	pkDef := tbl.tableDef.Cols[tbl.primaryIdx]
	pkSeq := pkDef.Seqnum
	pkType := plan2.ExprType2Type(&pkDef.Typ)
	if len(candidateBlks) > 0 {
		v2.TxnPKChangeCheckIOCounter.Inc()
	}
	for _, blk := range candidateBlks {
		release, err := ioutil.LoadColumns(
			ctx,
			[]uint16{uint16(pkSeq)},
			[]types.Type{pkType},
			fs,
			blk.MetaLocation(),
			cacheVectors,
			tbl.proc.Load().GetMPool(),
			fileservice.Policy(0),
		)
		if err != nil {
			return true, err
		}

		searchFunc := filter.DecideSearchFunc(blk.IsSorted())
		if searchFunc == nil {
			searchFunc = buildUnsortedFilter()
		}

		sels := searchFunc(cacheVectors)
		release()
		if len(sels) > 0 {
			return true, nil
		}
	}
	if checkTombstone {
		return p.HasTombstoneChanged(from, to), nil
	} else {
		return false, nil
	}
}

func (tbl *txnTable) PrimaryKeysMayBeUpserted(
	ctx context.Context,
	from types.TS,
	to types.TS,
	batch *batch.Batch,
	pkIndex int32,
) (bool, error) {
	keysVector := batch.GetVector(pkIndex)
	return tbl.primaryKeysMayBeChanged(ctx, from, to, keysVector, false)
}

func (tbl *txnTable) PrimaryKeysMayBeModified(
	ctx context.Context,
	from types.TS,
	to types.TS,
	batch *batch.Batch,
	pkIndex int32,
	_ int32,
) (bool, error) {
	keysVector := batch.GetVector(pkIndex)
	return tbl.primaryKeysMayBeChanged(ctx, from, to, keysVector, true)
}

func (tbl *txnTable) primaryKeysMayBeChanged(
	ctx context.Context,
	from types.TS,
	to types.TS,
	keysVector *vector.Vector,
	checkTombstone bool,
) (bool, error) {
	start := time.Now()
	defer func() {
		v2.TxnPKMayBeChangedDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	v2.TxnPKMayBeChangedTotalCounter.Inc()

	if tbl.db.op.IsSnapOp() {
		return false,
			moerr.NewInternalErrorNoCtx("primary key modification is not allowed in snapshot transaction")
	}
	// Measure LazyLoadLatestCkp duration
	lazyLoadStart := time.Now()
	part, err := tbl.eng.(*Engine).LazyLoadLatestCkp(
		ctx,
		uint64(tbl.accountId),
		tbl.tableId,
		tbl.tableName,
		tbl.db.databaseId,
		tbl.db.databaseName)
	v2.TxnLazyLoadCkpDurationHistogram.Observe(time.Since(lazyLoadStart).Seconds())
	if err != nil {
		return false, err
	}
	snap := part.Snapshot()

	var packer *types.Packer
	put := tbl.eng.(*Engine).packerPool.Get(&packer)
	defer put.Put()
	packer.Reset()

	keys := readutil.EncodePrimaryKeyVector(keysVector, packer)
	// Measure PKExistInMemBetween duration
	memCheckStart := time.Now()
	exist, flushed := snap.PKExistInMemBetween(from, to, keys)
	v2.TxnPKExistInMemDurationHistogram.Observe(time.Since(memCheckStart).Seconds())

	if exist {
		v2.TxnPKMayBeChangedMemHitCounter.Inc()
		return true, nil
	}
	if !flushed {
		v2.TxnPKMayBeChangedMemNotFlushedCounter.Inc()
		return false, nil
	}

	//need check pk whether exist on S3 block.
	v2.TxnPKMayBeChangedPersistedCounter.Inc()
	return tbl.PKPersistedBetween(
		snap,
		from,
		to,
		keysVector, checkTombstone)
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

	// check object visibility and set object stats.
	for i, objstat := range objStats {
		info, exist := state.GetObject(*objstat.ObjectShortName())
		if !exist || (!info.DeleteTime.IsEmpty() && info.DeleteTime.LE(&snapshot)) {
			logutil.Errorf("object not visible: %s", info.String())
			return nil, moerr.NewInternalErrorNoCtxf("object %s not exist", objstat.ObjectName().String())
		}
		objectio.SetObjectStats(&objstat, &info.ObjectStats)
		objStats[i] = objstat
	}

	tbl.ensureSeqnumsAndTypesExpectRowid()

	taskHost, err := newCNMergeTask(
		ctx, tbl, snapshot, // context
		sortKeyPos, sortKeyIsPK, // schema
		objStats, // targets
		targetObjSize)
	if err != nil {
		return nil, err
	}

	err = mergesort.DoMergeAndWrite(ctx, tbl.getTxn().op.Txn().DebugString(), sortKeyPos, taskHost)
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

	err = ForeachVisibleObjects(state, snapshot, func(obj objectio.ObjectEntry) error {
		if obj.GetAppendable() {
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
	}, nil, false)
	if err != nil {
		return nil, err
	}
	return objStats, nil
}

// Reset what?
// TODO: txnTable should be stateless
func (tbl *txnTable) Reset(op client.TxnOperator) error {
	ws := op.GetWorkspace()
	if ws == nil {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("workspace is nil when reset relation %s:%s",
			tbl.db.databaseName, tbl.tableName))
	}
	txn, ok := ws.(*Transaction)
	if !ok {
		return moerr.NewInternalErrorNoCtx("failed to assert txn")
	}
	tbl.db.op = op
	tbl.proc.Store(txn.proc)
	tbl.createdInTxn = false
	tbl.lastTS = op.SnapshotTS()
	return nil
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
	buffer := batch.New(columns)
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
				filename := ioutil.EncodeTmpFileName("tmp", "merge_"+uuid.NewString(), time.Now().UTC().Unix())
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
		filename := ioutil.EncodeTmpFileName("tmp", "merge_"+uuid.NewString(), time.Now().UTC().Unix())
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
					vs := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
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

func (tbl *txnTable) GetExtraInfo() *api.SchemaExtra {
	return tbl.extraInfo
}
