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
	"time"

	"go.uber.org/zap"

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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
)

func ConstructInExpr(
	ctx context.Context,
	colName string,
	colVec *vector.Vector,
) *plan.Expr {
	data, _ := colVec.MarshalBinary()
	colExpr := newColumnExpr(0, plan2.MakePlan2Type(colVec.GetType()), colName)
	return plan2.MakeInExpr(
		ctx,
		colExpr,
		int32(colVec.Length()),
		data,
		false,
	)
}

func TransferTombstones(
	ctx context.Context,
	table *txnTable,
	state *logtailreplay.PartitionStateInProgress,
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
		if obj, ok := state.GetObject(name); ok {
			objectList = append(objectList, obj.ObjectStats)
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
	}()

	// loop the transaction workspace to transfer all tombstones
	for i, entry := range txnWrites {
		// skip all entries not table-realted
		// skip all non-delete entries
		if entry.tableId != table.tableId ||
			entry.typ != DELETE ||
			entry.fileName != "" {
			continue
		}

		// column 0 is rowid, column 1 is pk
		// fetch rowid and pk column data
		rowids := vector.MustFixedCol[types.Rowid](entry.bat.GetVector(0))
		pkColumn := entry.bat.GetVector(1)

		for j, rowid := range rowids {
			blockId := rowid.BorrowBlockID()
			// if the block of the rowid is not in the deleted objects, skip transfer
			if _, deleted := deletedObjects[*objectio.ShortName(blockId)]; !deleted {
				continue
			}
			if transferIntents == nil {
				transferIntents = vector.NewVec(types.T_Rowid.ToType())
				targetRowids = vector.NewVec(types.T_Rowid.ToType())
				searchPKColumn = vector.NewVec(*pkColumn.GetType())
				searchEntryPos = vector.NewVec(types.T_int32.ToType())
				searchBatPos = vector.NewVec(types.T_int32.ToType())
				readPKColumn = vector.NewVec(*pkColumn.GetType())
			}
			if err = vector.AppendFixed[types.Rowid](transferIntents, rowid, false, mp); err != nil {
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
	if err = mergesort.SortColumnsByIndex(
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

	if err = mergesort.SortColumnsByIndex(
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

	entryPositions := vector.MustFixedCol[int32](searchEntryPos)
	batPositions := vector.MustFixedCol[int32](searchBatPos)
	rowids := vector.MustFixedCol[types.Rowid](targetRowids)
	for pos, endPos := 0, searchPKColumn.Length(); pos < endPos; pos++ {
		entry := txnWrites[entryPositions[pos]]
		if err = vector.SetFixedAt[types.Rowid](
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
	expr := ConstructInExpr(ctx, pkColumName, searchPKColumn)

	var blockList objectio.BlockInfoSlice
	if _, err = TryFastFilterBlocks(
		ctx,
		table,
		table.db.op.SnapshotTS(),
		table.GetTableDef(ctx),
		[]*plan.Expr{expr},
		nil,
		objectList,
		nil,
		&blockList,
		fs,
		table.proc.Load(),
	); err != nil {
		return
	}
	relData := NewEmptyBlockListRelationData()
	relData.AppendBlockInfo(objectio.EmptyBlockInfo) // read partition insert
	for i, end := 0, blockList.Len(); i < end; i++ {
		relData.AppendBlockInfo(*blockList.Get(i))
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
	)
	if err != nil {
		return
	}
	defer func() {
		readers[0].Close()
	}()

	attrs := []string{
		pkColumName,
		catalog.Row_ID,
	}
	buildBatch := func() *batch.Batch {
		bat := batch.NewWithSize(2)
		bat.Attrs = append(bat.Attrs, attrs...)

		bat.Vecs[0] = vector.NewVec(*readPKColumn.GetType())
		bat.Vecs[1] = vector.NewVec(types.T_Rowid.ToType())
		return bat
	}
	bat := buildBatch()
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
			nil,
			bat,
		)
		if err != nil {
			return
		}
		if isEnd {
			break
		}
		if err = vector.GetUnionAllFunction(
			*readPKColumn.GetType(), mp,
		)(
			readPKColumn, bat.GetVector(0),
		); err != nil {
			return
		}

		if err = vector.GetUnionAllFunction(
			*targetRowids.GetType(), mp,
		)(
			targetRowids, bat.GetVector(1),
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
