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
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/compare"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/mergeutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/txn/trace"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"go.uber.org/zap"
)

func transferInmemTombstones(
	ctx context.Context,
	txn *Transaction,
	start, end types.TS,
) (err error) {

	return txn.forEachTableHasDeletesLocked(
		false,
		func(tbl *txnTable) error {
			state, err := tbl.getPartitionState(ctx)
			if err != nil {
				return err
			}

			deleteObjs, createObjs := state.GetChangedObjsBetween(start, end)

			trace.GetService(txn.proc.GetService()).ApplyFlush(
				tbl.db.op.Txn().ID,
				tbl.tableId,
				start.ToTimestamp(),
				tbl.db.op.SnapshotTS(),
				len(deleteObjs))

			if len(deleteObjs) > 0 {
				if err := transferTombstones(
					ctx,
					tbl,
					state,
					deleteObjs,
					createObjs,
					txn.proc.Mp(),
					txn.engine.fs,
				); err != nil {
					return err
				}
			}
			return nil
		})
}

func transferTombstoneObjects(
	ctx context.Context,
	txn *Transaction,
	start, end types.TS,
) (err error) {

	var (
		fs   fileservice.FileService
		logs []zap.Field
		flow *TransferFlow
	)

	if fs, err = colexec.GetSharedFSFromProc(txn.proc); err != nil {
		return
	}

	return txn.forEachTableHasDeletesLocked(
		true,
		func(tbl *txnTable) error {
			now := time.Now()
			if flow, logs, err = ConstructCNTombstoneObjectsTransferFlow(
				ctx, start, end, tbl, txn, txn.proc.Mp(), fs); err != nil {
				return err
			} else if flow == nil {
				logutil.Info("CN-TRANSFER-TOMBSTONE-OBJ", logs...)
				return nil
			}

			defer func() {
				err = flow.Close()
			}()

			if err = flow.Process(ctx); err != nil {
				return err
			}

			slist, tail := flow.GetResult()
			if len(tail) > 0 {
				logutil.Fatal("tombstone sinker tail size is not zero",
					zap.Int("tail", len(tail)))
			}

			bat := colexec.AllocCNS3ResultBat(true, false)
			if err = bat.PreExtend(txn.proc.Mp(), len(slist)); err != nil {
				return err
			}

			obj := make([]string, 0, len(slist))
			for i := range slist {
				obj = append(obj, slist[i].ObjectName().ObjectId().ShortStringEx())

				if err = vector.AppendBytes(
					bat.GetVector(0),
					slist[i].Marshal(),
					false,
					txn.proc.Mp(),
				); err != nil {
					return err
				}

				bat.SetRowCount(bat.Vecs[0].Length())
			}

			if bat.RowCount() > 0 {
				fileName := slist[0].ObjectName().String()
				if err = txn.WriteFileLocked(
					DELETE,
					tbl.accountId, tbl.db.databaseId, tbl.tableId,
					tbl.db.databaseName, tbl.tableName, fileName,
					bat, txn.tnStores[0],
				); err != nil {
					return err
				}
			}

			logs = append(logs,
				zap.String("txn-id", txn.op.Txn().DebugString()),
				zap.String("table", fmt.Sprintf("%s(%d)-%s(%d)",
					tbl.db.databaseName, tbl.db.databaseId, tbl.tableName, tbl.tableId)),
				zap.Duration("time-spent", time.Since(now)),
				zap.Int("transferred-row-cnt", flow.transferred.rowCnt),
				zap.String("new-files", strings.Join(obj, "; ")),
				zap.String("from", start.ToString()),
				zap.String("to", end.ToString()),
				zap.String("transferred obj", fmt.Sprintf("%v", flow.transferred.objDetails)))

			logutil.Info("CN-TRANSFER-TOMBSTONE-OBJ", logs...)

			return nil
		})
}

func transferTombstones(
	ctx context.Context,
	table *txnTable,
	pState *logtailreplay.PartitionState,
	deletedObjects, createdObjects map[objectio.ObjectNameShort]struct{},
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	if len(deletedObjects) == 0 || len(createdObjects) == 0 {
		return
	}
	wantDetail := false
	var transferCnt int
	start := time.Now()
	v2.TransferTombstonesCountHistogram.Observe(1)
	defer func() {
		duration := time.Since(start)
		if duration > time.Millisecond*500 || err != nil || wantDetail {
			logutil.Info(
				"TRANSFER-TOMBSTONE-SLOW-LOG",
				zap.Duration("duration", duration),
				zap.Int("count", transferCnt),
				zap.String("table-name", table.tableDef.Name),
				zap.Uint64("table-id", table.tableId),
				zap.Int("deleted-objects", len(deletedObjects)),
				zap.Int("created-objects", len(createdObjects)),
				zap.Error(err),
			)
		}
		v2.TransferTombstonesDurationHistogram.Observe(duration.Seconds())
	}()

	var objectList []objectio.ObjectStats
	for name := range createdObjects {
		if obj, ok := pState.GetObject(name); ok {
			objectList = append(objectList, obj.ObjectStats)
		}
	}

	if len(objectList) >= 10 {
		proc := table.proc.Load()
		for _, obj := range objectList {
			ioutil.Prefetch(proc.GetService(), fs, obj.ObjectLocation())
		}
	}

	txnWrites := table.getTxn().writes

	var (
		transferIntents *vector.Vector
		targetRowids    *vector.Vector
		searchPKColumn  *vector.Vector
		searchEntryPos  *vector.Vector
		searchBatPos    *vector.Vector
		readPKColumn    *vector.Vector

		entryPosMask objectio.Bitmap
	)

	defer func() {
		if transferIntents != nil {
			transferIntents.Free(mp)
		}

		if targetRowids != nil {
			targetRowids.Free(mp)
		}

		if searchPKColumn != nil {
			searchPKColumn.Free(mp)
		}

		if searchEntryPos != nil {
			searchEntryPos.Free(mp)
		}

		if searchBatPos != nil {
			searchBatPos.Free(mp)
		}

		if readPKColumn != nil {
			readPKColumn.Free(mp)
		}

		entryPosMask.Release()
	}()

	entryPosMask = objectio.GetReusableBitmap()

	// loop the transaction workspace to transfer all tombstones
	for i, entry := range txnWrites {
		// skip all entries not table-realted
		// skip all non-delete entries
		if entry.tableId != table.tableId ||
			entry.typ != DELETE ||
			entry.fileName != "" || entry.bat == nil {
			continue
		}

		// column 0 is rowid, column 1 is pk
		// fetch rowid and pk column data
		rowIds := vector.MustFixedColWithTypeCheck[types.Rowid](entry.bat.GetVector(0))
		pkColumn := entry.bat.GetVector(1)

		for j, rowId := range rowIds {
			blockId := rowId.BorrowBlockID()
			// if the block of the rowid is not in the deleted objects, skip transfer
			if _, deleted := deletedObjects[*objectio.ShortName(blockId)]; !deleted {
				continue
			}

			entryPosMask.Add(uint64(i))

			if transferIntents == nil {
				transferIntents = vector.NewVec(types.T_Rowid.ToType())
				targetRowids = vector.NewVec(types.T_Rowid.ToType())
				searchPKColumn = vector.NewVec(*pkColumn.GetType())
				searchEntryPos = vector.NewVec(types.T_int32.ToType())
				searchBatPos = vector.NewVec(types.T_int32.ToType())
				readPKColumn = vector.NewVec(*pkColumn.GetType())
			}

			if err = vector.AppendFixed[types.Rowid](transferIntents, rowId, false, mp); err != nil {
				return
			}

			if err = vector.AppendFixed[int32](searchEntryPos, int32(i), false, mp); err != nil {
				return
			}

			if err = vector.AppendFixed[int32](searchBatPos, int32(j), false, mp); err != nil {
				return
			}

			if err = searchPKColumn.UnionOne(pkColumn, int64(j), mp); err != nil {
				return
			}

			if transferIntents.Length() >= 8192 {
				transferCnt += transferIntents.Length()
				if err = batchTransferToTombstones(
					ctx,
					table,
					txnWrites,
					objectList,
					transferIntents,
					targetRowids,
					searchPKColumn,
					searchEntryPos,
					searchBatPos,
					readPKColumn,
					mp,
					fs,
					wantDetail,
				); err != nil {
					return
				}
			}
		}
	}

	if transferIntents != nil && transferIntents.Length() > 0 {
		transferCnt += transferIntents.Length()
		if err = batchTransferToTombstones(
			ctx,
			table,
			txnWrites,
			objectList,
			transferIntents,
			targetRowids,
			searchPKColumn,
			searchEntryPos,
			searchBatPos,
			readPKColumn,
			mp,
			fs,
			wantDetail,
		); err != nil {
			return
		}
	}

	iter := entryPosMask.Bitmap().Iterator()
	for iter.HasNext() {
		idx := iter.Next()
		entry := txnWrites[idx]

		if !catalog.IsSystemTable(entry.tableId) &&
			entry.bat != nil && entry.bat.RowCount() > 1 {
			if err = mergeutil.SortColumnsByIndex(entry.bat.Vecs, 0, mp); err != nil {
				return
			}
		}
	}

	return nil
}

func batchTransferToTombstones(
	ctx context.Context,
	table *txnTable,
	txnWrites []Entry,
	objectList []objectio.ObjectStats,
	transferIntents *vector.Vector,
	targetRowids *vector.Vector,
	searchPKColumn *vector.Vector,
	searchEntryPos *vector.Vector,
	searchBatPos *vector.Vector,
	readPKColumn *vector.Vector,
	mp *mpool.MPool,
	fs fileservice.FileService,
	wantDetail bool,
) (err error) {
	if err = targetRowids.PreExtend(transferIntents.Length(), mp); err != nil {
		return
	}

	if err = mergeutil.SortColumnsByIndex(
		[]*vector.Vector{searchPKColumn, searchEntryPos, searchBatPos},
		0,
		mp,
	); err != nil {
		return
	}

	if err = doTransferRowids(
		ctx,
		table,
		objectList,
		transferIntents,
		targetRowids,
		searchPKColumn,
		readPKColumn,
		mp,
		fs,
	); err != nil {
		return
	}

	if err = mergeutil.SortColumnsByIndex(
		[]*vector.Vector{readPKColumn, targetRowids},
		0,
		mp,
	); err != nil {
		return
	}

	// compare readPKColumn with searchPKColumn. equal is expected
	if wantDetail {
		typ := searchPKColumn.GetType()
		comp := compare.New(*typ, false, false)
		comp.Set(0, searchPKColumn)
		comp.Set(1, readPKColumn)

		errPKVec1 := vector.NewVec(*typ)
		errPKVec2 := vector.NewVec(*typ)
		errCnt := 0
		defer func() {
			errPKVec1.Free(mp)
			errPKVec2.Free(mp)
		}()
		for i, last := 0, searchPKColumn.Length(); i < last; i++ {
			res := comp.Compare(0, 1, int64(i), int64(i))
			if res != 0 {
				errCnt++
				if errCnt > 20 {
					continue
				}
				if err = errPKVec1.UnionOne(searchPKColumn, int64(i), mp); err != nil {
					return
				}
				if err = errPKVec2.UnionOne(readPKColumn, int64(i), mp); err != nil {
					return
				}
			}
		}
		if errCnt > 0 {
			logutil.Error(
				"TRANSFER-ROWIDS-ERROR-DETAIL-LOG",
				zap.String("table-name", table.tableDef.Name),
				zap.Uint64("table-id", table.tableId),
				zap.Int("error-count", errCnt),
				zap.String("expect-pks", common.MoVectorToString(errPKVec1, errPKVec1.Length())),
				zap.String("actual-pks", common.MoVectorToString(errPKVec2, errPKVec2.Length())),
			)
			err = moerr.NewInternalErrorNoCtx("transfer rowids failed, pk mismatch")
		}
		if err != nil {
			return
		}
	}

	entryPositions := vector.MustFixedColWithTypeCheck[int32](searchEntryPos)
	batPositions := vector.MustFixedColWithTypeCheck[int32](searchBatPos)
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](targetRowids)

	for pos, endPos := 0, searchPKColumn.Length(); pos < endPos; pos++ {
		entryIdx := entryPositions[pos]
		entry := txnWrites[entryIdx]

		if err = vector.SetFixedAtWithTypeCheck[types.Rowid](
			entry.bat.GetVector(0),
			int(batPositions[pos]),
			rowids[pos],
		); err != nil {
			return
		}
	}

	searchPKColumn.Reset(*searchPKColumn.GetType())
	searchEntryPos.Reset(*searchEntryPos.GetType())
	searchBatPos.Reset(*searchBatPos.GetType())
	targetRowids.Reset(*targetRowids.GetType())
	transferIntents.Reset(*transferIntents.GetType())
	readPKColumn.Reset(*readPKColumn.GetType())
	return
}

// doTransferRowids transfers rowids from transferIntents to targetRowids
func doTransferRowids(
	ctx context.Context,
	table *txnTable,
	objectList []objectio.ObjectStats,
	transferIntents, targetRowids *vector.Vector,
	searchPKColumn, readPKColumn *vector.Vector,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	now := time.Now()
	defer func() {
		duration := time.Since(now)
		v2.BatchTransferTombstonesDurationHistogram.Observe(duration.Seconds())
	}()

	pkColumName := table.GetTableDef(ctx).Pkey.PkeyColName
	expr := readutil.ConstructInExpr(ctx, pkColumName, searchPKColumn)
	rangesParam := engine.RangesParam{
		BlockFilters:   []*plan.Expr{expr},
		PreAllocBlocks: 2,
		TxnOffset:      0,
		Policy:         engine.Policy_CollectAllData,
	}

	var blockList objectio.BlockInfoSlice
	if _, err = readutil.TryFastFilterBlocks(
		ctx,
		table.db.op.SnapshotTS(),
		table.GetTableDef(ctx),
		rangesParam,
		nil,
		objectList,
		nil,
		&blockList,
		nil,
		fs,
	); err != nil {
		return
	}

	part, err := table.getPartitionState(ctx)
	if err != nil {
		return err
	}

	relData := readutil.NewBlockListRelationData(
		1,
		readutil.WithPartitionState(part))

	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(blockList.Get(i))
	}

	readers, err := table.BuildReaders(
		ctx,
		table.proc.Load(),
		expr,
		relData,
		1,
		0,
		false,
		engine.Policy_CheckCommittedOnly,
		engine.FilterHint{Must: true},
	)
	if err != nil {
		return
	}
	defer func() {
		readers[0].Close()
	}()

	attrs := []string{
		pkColumName,
		objectio.PhysicalAddr_Attr,
	}
	attrTypes := []types.Type{
		*readPKColumn.GetType(),
		objectio.RowidType,
	}
	bat := batch.NewWithSchema(true, attrs, attrTypes)
	defer func() {
		bat.Clean(mp)
	}()
	var isEnd bool
	for {
		bat.CleanOnlyData()
		isEnd, err = readers[0].Read(
			ctx,
			attrs,
			expr,
			mp,
			bat,
		)
		if err != nil {
			return
		}
		if isEnd {
			break
		}

		if err = readPKColumn.UnionBatch(
			bat.GetVector(0), 0, bat.RowCount(), nil, mp,
		); err != nil {
			return
		}
		if err = targetRowids.UnionBatch(
			bat.GetVector(1), 0, bat.RowCount(), nil, mp,
		); err != nil {
			return
		}
	}

	if targetRowids.Length() != transferIntents.Length() {
		err = moerr.NewInternalErrorNoCtxf(
			"transfer rowids failed, length mismatch, expect %d, got %d",
			transferIntents.Length(),
			targetRowids.Length(),
		)
		logutil.Error(
			"TRANSFER-ROWIDS-ERROR-LEN-MISMATCH",
			zap.Error(err),
			zap.String("table-name", table.tableDef.Name),
			zap.Uint64("table-id", table.tableId),
			zap.String("intents", common.MoVectorToString(transferIntents, 20)),
			zap.String("actual", common.MoVectorToString(targetRowids, 20)),
		)
	}

	return
}
