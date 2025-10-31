// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var _ Reader = new(tableReader)
var _ TableReader = new(tableReader)

var RetryableErrorPrefix = "retryable error:"

const (
	DefaultFrequency = 200 * time.Millisecond
)

type WatermarkUpdater interface {
	RemoveCachedWM(ctx context.Context, key *WatermarkKey) (err error)
	UpdateWatermarkErrMsg(ctx context.Context, key *WatermarkKey, errMsg string) (err error)
	GetFromCache(ctx context.Context, key *WatermarkKey) (watermark types.TS, err error)
	GetOrAddCommitted(ctx context.Context, key *WatermarkKey, watermark *types.TS) (ret types.TS, err error)
	UpdateWatermarkOnly(ctx context.Context, key *WatermarkKey, watermark *types.TS) (err error)
}

type tableReader struct {
	cnTxnClient          client.TxnClient
	cnEngine             engine.Engine
	mp                   *mpool.MPool
	packerPool           *fileservice.Pool[*types.Packer]
	accountId            uint64
	taskId               string
	info                 *DbTableInfo
	sinker               Sinker
	wMarkUpdater         WatermarkUpdater
	tick                 *time.Ticker
	force                bool
	initSnapshotSplitTxn bool
	runningReaders       *sync.Map
	startTs, endTs       types.TS
	noFull               bool
	frequency            time.Duration

	tableDef                           *plan.TableDef
	insTsColIdx, insCompositedPkColIdx int
	delTsColIdx, delCompositedPkColIdx int

	wg sync.WaitGroup
}

var NewTableReader = func(
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	accountId uint64,
	taskId string,
	info *DbTableInfo,
	sinker Sinker,
	wMarkUpdater WatermarkUpdater,
	tableDef *plan.TableDef,
	initSnapshotSplitTxn bool,
	runningReaders *sync.Map,
	startTs, endTs types.TS,
	noFull bool,
	frequency string,
) Reader {
	var tick *time.Ticker
	var dur time.Duration
	if frequency != "" {
		dur = parseFrequencyToDuration(frequency)
	} else {
		dur = 200 * time.Millisecond
	}
	tick = time.NewTicker(dur)
	reader := &tableReader{
		cnTxnClient:          cnTxnClient,
		cnEngine:             cnEngine,
		mp:                   mp,
		packerPool:           packerPool,
		accountId:            accountId,
		taskId:               taskId,
		info:                 info,
		sinker:               sinker,
		wMarkUpdater:         wMarkUpdater,
		tick:                 tick,
		initSnapshotSplitTxn: initSnapshotSplitTxn,
		runningReaders:       runningReaders,
		startTs:              startTs,
		endTs:                endTs,
		noFull:               noFull,
		tableDef:             tableDef,
		frequency:            dur,
	}

	// batch columns layout:
	// 1. data: user defined cols | cpk (if needed) | commit-ts
	// 2. tombstone: pk/cpk | commit-ts
	reader.insTsColIdx, reader.insCompositedPkColIdx = len(tableDef.Cols)-1, len(tableDef.Cols)-2
	reader.delTsColIdx, reader.delCompositedPkColIdx = 1, 0
	// if single col pk, there's no additional cpk col
	if len(tableDef.Pkey.Names) == 1 {
		reader.insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}
	return reader
}

func (reader *tableReader) Info() *DbTableInfo {
	return reader.info
}

func (reader *tableReader) GetWg() *sync.WaitGroup {
	return &reader.wg
}

func (reader *tableReader) Close() {
	reader.sinker.Close()
}

func (reader *tableReader) forceNextInterval(wait time.Duration) {
	logutil.Info(
		"CDC-TableReader-ResetNextInterval",
		zap.String("info", reader.info.String()),
		zap.Duration("wait", wait),
	)
	reader.force = true
	reader.tick.Reset(wait)
}

func (reader *tableReader) Run(
	ctx context.Context,
	ar *ActiveRoutine,
) {
	key := GenDbTblKey(reader.info.SourceDbName, reader.info.SourceTblName)
	if _, loaded := reader.runningReaders.LoadOrStore(key, reader); loaded {
		logutil.Warn(
			"CDC-TableReader-DuplicateRunning",
			zap.String("info", reader.info.String()),
			zap.String("task-id", reader.taskId),
			zap.Uint64("account-id", reader.accountId),
		)
		reader.Close()
		return
	}
	logutil.Info(
		"CDC-TableReader-RunStart",
		zap.String("info", reader.info.String()),
		zap.Uint64("account-id", reader.accountId),
		zap.String("task-id", reader.taskId),
	)
	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.AddActiveRunner(reader.info)
	}

	var err error
	var retryable bool
	reader.wg.Add(1)
	defer func() {
		defer reader.wg.Done()
		wKey := WatermarkKey{
			AccountId: reader.accountId,
			TaskId:    reader.taskId,
			DBName:    reader.info.SourceDbName,
			TableName: reader.info.SourceTblName,
		}
		defer reader.wMarkUpdater.RemoveCachedWM(ctx, &wKey)

		if err != nil {
			errMsg := err.Error()
			if retryable {
				errMsg = RetryableErrorPrefix + errMsg
			}
			if err = reader.wMarkUpdater.UpdateWatermarkErrMsg(
				ctx,
				&wKey,
				errMsg,
			); err != nil {
				logutil.Error(
					"CDC-TableReader-UpdateWatermarkErrMsgFailed",
					zap.String("info", reader.info.String()),
					zap.String("save-err-msg", errMsg),
					zap.Error(err),
				)
			}
		}
		reader.Close()
		reader.runningReaders.Delete(key)
		if detector != nil && detector.cdcStateManager != nil {
			detector.cdcStateManager.RemoveActiveRunner(reader.info)
		}
		logutil.Info(
			"CDC-TableReader-RunEnd",
			zap.String("info", reader.info.String()),
			zap.Uint64("account-id", reader.accountId),
			zap.String("task-id", reader.taskId),
		)
	}()

	lastSync, err := reader.getLastSyncTime(ctx)
	if err != nil {
		logutil.Errorf("CDC-TableReader GetLastSyncTime failed: %v", err)
		lastSync = time.Time{}
	}

	if reader.frequency <= 0 {
		reader.frequency = 200 * time.Millisecond
	}
	nextSyncTime := time.Now()
	if !lastSync.IsZero() {
		nextSyncTime = lastSync.Add(reader.frequency)
	}

	var wait time.Duration
	if now := time.Now(); now.Before(nextSyncTime) {
		wait = nextSyncTime.Sub(now)
	} else {
		wait = 200 * time.Millisecond
	}

	reader.forceNextInterval(wait)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		case <-reader.tick.C:
		}

		if reader.force {
			reader.force = false
			reader.tick.Reset(reader.frequency)
			logutil.Info(
				"CDC-TableReader-ResetNextInterval",
				zap.String("info", reader.info.String()),
				zap.Duration("frequency", reader.frequency),
			)
		}
		if retryable, err = reader.readTable(ctx, ar); err != nil {
			logutil.Errorf("CDC-TableReader (%v) failed, err: %v", reader.info, err)
			return
		}

	}
}

func (reader *tableReader) getLastSyncTime(ctx context.Context) (time.Time, error) {
	wKey := WatermarkKey{
		AccountId: reader.accountId,
		TaskId:    reader.taskId,
		DBName:    reader.info.SourceDbName,
		TableName: reader.info.SourceTblName,
	}

	ts, err := reader.wMarkUpdater.GetFromCache(ctx, &wKey)
	if err == nil && !ts.ToTimestamp().IsEmpty() {
		return ts.ToTimestamp().ToStdTime(), nil
	}

	defaultTS := reader.startTs
	ts, err = reader.wMarkUpdater.GetOrAddCommitted(ctx, &wKey, &defaultTS)
	if err != nil {
		return time.Time{}, err
	}
	return ts.ToTimestamp().ToStdTime(), nil
}

var readTableWithTxn = func(
	reader *tableReader,
	ctx context.Context,
	txnOp client.TxnOperator,
	packer *types.Packer,
	ar *ActiveRoutine,
) (retryable bool, err error) {
	retry(ctx, ar, func() error {
		var retryWithNewReader bool
		retryable, retryWithNewReader, err = reader.readTableWithTxn(ctx, txnOp, packer, ar)
		if retryWithNewReader {
			return nil
		}
		return err
	}, CDCDefaultRetryTimes, CDCDefaultRetryDuration)
	return
}

func (reader *tableReader) readTable(ctx context.Context, ar *ActiveRoutine) (retryable bool, err error) {
	//step1 : create an txnOp
	txnOp, err := GetTxnOp(ctx, reader.cnEngine, reader.cnTxnClient, "readMultipleTables")

	if err != nil {
		return false, err
	}
	defer func() {
		FinishTxnOp(ctx, err, txnOp, reader.cnEngine)
	}()

	EnterRunSql(txnOp)
	defer func() {
		ExitRunSql(txnOp)
	}()

	if err = GetTxn(ctx, reader.cnEngine, txnOp); err != nil {
		return false, err
	}

	var packer *types.Packer
	put := reader.packerPool.Get(&packer)
	defer put.Put()

	//step2 : read table
	retryable, err = readTableWithTxn(reader, ctx, txnOp, packer, ar)
	// if stale read, try to reset watermark
	if moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
		if !reader.noFull && !reader.startTs.IsEmpty() {
			err = moerr.NewInternalErrorf(ctx, "cdc tableReader(%v) stale read, and startTs(%v) is set, end", reader.info, reader.startTs)
			return
		}

		// reset sinker
		reader.sinker.Reset()
		// reset watermark to startTs, will read from the beginning at next round
		watermark := reader.startTs
		if reader.noFull {
			watermark = types.TimestampToTS(txnOp.SnapshotTS())
		}

		key := WatermarkKey{
			AccountId: reader.accountId,
			TaskId:    reader.taskId,
			DBName:    reader.info.SourceDbName,
			TableName: reader.info.SourceTblName,
		}
		reader.wMarkUpdater.UpdateWatermarkOnly(
			ctx,
			&key,
			&watermark,
		)
		logutil.Info(
			"CDC-TableReader-ResetWatermark",
			zap.String("info", reader.info.String()),
			zap.Uint64("account-id", reader.accountId),
			zap.String("task-id", reader.taskId),
			zap.String("watermark", watermark.ToString()),
		)
		err = nil
		reader.forceNextInterval(DefaultFrequency)
	}
	return
}

func (reader *tableReader) readTableWithTxn(
	ctx context.Context,
	txnOp client.TxnOperator,
	packer *types.Packer,
	ar *ActiveRoutine,
) (retryable bool, retryWithNewReader bool, err error) {
	defer func() {
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
				retryable = true
				retryWithNewReader = true
			}
			logutil.Error(
				"CDC-TableReader",
				zap.String("info", reader.info.String()),
				zap.Uint64("account-id", reader.accountId),
				zap.String("task-id", reader.taskId),
				zap.Bool("retryable", retryable),
				zap.Bool("retryWithNewReader", retryWithNewReader),
				zap.Error(err),
			)
		}
	}()
	var rel engine.Relation
	var changes engine.ChangesHandle

	//step1 : get relation
	if _, _, rel, err = GetRelationById(ctx, reader.cnEngine, txnOp, reader.info.SourceTblId); err != nil {
		// truncate table
		retryable = true
		retryWithNewReader = true
		return
	}

	key := WatermarkKey{
		AccountId: reader.accountId,
		TaskId:    reader.taskId,
		DBName:    reader.info.SourceDbName,
		TableName: reader.info.SourceTblName,
	}
	//step2 : define time range
	//	from = last wmark
	//  to = txn operator snapshot ts
	fromTs, err := reader.wMarkUpdater.GetFromCache(ctx, &key)
	if err != nil {
		return
	}

	if !reader.endTs.IsEmpty() && fromTs.GE(&reader.endTs) {
		return
	}
	toTs := types.TimestampToTS(GetSnapshotTS(txnOp))
	if !reader.endTs.IsEmpty() && toTs.GT(&reader.endTs) {
		toTs = reader.endTs
	}

	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.UpdateActiveRunner(reader.info, fromTs, toTs, true)
		defer detector.cdcStateManager.UpdateActiveRunner(reader.info, fromTs, toTs, false)
	}
	start := time.Now()
	changes, err = CollectChanges(ctx, rel, fromTs, toTs, reader.mp)

	v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
	if err != nil {
		return
	}
	defer changes.Close()

	//step3: pull data
	var insertData, deleteData *batch.Batch
	var insertAtmBatch, deleteAtmBatch *AtomicBatch
	var hasBegin bool

	defer func() {
		if insertData != nil {
			insertData.Clean(reader.mp)
		}
		if deleteData != nil {
			deleteData.Clean(reader.mp)
		}
		if insertAtmBatch != nil {
			insertAtmBatch.Close()
		}
		if deleteAtmBatch != nil {
			deleteAtmBatch.Close()
		}

		current, err := reader.wMarkUpdater.GetFromCache(ctx, &key)
		if err != nil {
			return
		}
		// if current != toTs, means this procedure end abnormally, need to rollback
		// e.g. encounter errors, or interrupted by user (pause/cancel)
		if !current.Equal(&toTs) {
			logutil.Error(
				"CDC-TableReader-ReadTableWithTxnEndAbnormally",
				zap.String("info", reader.info.String()),
				zap.Uint64("account-id", reader.accountId),
				zap.String("task-id", reader.taskId),
				zap.String("current", current.ToString()),
				zap.String("to", toTs.ToString()),
			)
			if hasBegin {
				// clear previous error
				reader.sinker.ClearError()
				reader.sinker.SendRollback()
				reader.sinker.SendDummy()
				if rollbackErr := reader.sinker.Error(); rollbackErr != nil {
					logutil.Error(
						"CDC-TableReader-SendRollbackFailed",
						zap.String("info", reader.info.String()),
						zap.String("task-id", reader.taskId),
						zap.Uint64("account-id", reader.accountId),
						zap.Error(rollbackErr),
					)
				}
			}
		}
	}()

	allocateAtomicBatchIfNeed := func(atomicBatch *AtomicBatch) *AtomicBatch {
		if atomicBatch == nil {
			atomicBatch = NewAtomicBatch(reader.mp)
		}
		return atomicBatch
	}

	var currentHint engine.ChangesHandle_Hint
	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		default:
		}
		// check sinker error of last round
		if err = reader.sinker.Error(); err != nil {
			return
		}

		v2.CdcMpoolInUseBytesGauge.Set(float64(reader.mp.Stats().NumCurrBytes.Load()))
		start = time.Now()
		insertData, deleteData, currentHint, err = changes.Next(ctx, reader.mp)
		v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
		if err != nil {
			return
		}

		// both nil denote no more data (end of this tail)
		if insertData == nil && deleteData == nil {
			// heartbeat, send remaining data in sinker
			reader.sinker.Sink(ctx, &DecoderOutput{
				noMoreData: true,
				fromTs:     fromTs,
				toTs:       toTs,
			})

			// send a dummy to guarantee last piece of snapshot/tail send successfully
			reader.sinker.SendDummy()
			if err = reader.sinker.Error(); err == nil {
				if hasBegin {
					// error may not be caught immediately
					reader.sinker.SendCommit()
					// so send a dummy sql to guarantee previous commit is sent successfully
					reader.sinker.SendDummy()
					err = reader.sinker.Error()
				}

				// if commit successfully, update watermark
				if err == nil {
					if err = reader.wMarkUpdater.UpdateWatermarkOnly(
						ctx,
						&key,
						&toTs,
					); err != nil {
						logutil.Error(
							"CDC-TableReader-UpdateWatermarkOnlyFailed",
							zap.String("info", reader.info.String()),
							zap.String("task-id", reader.taskId),
							zap.Uint64("account-id", reader.accountId),
							zap.String("to", toTs.ToString()),
							zap.Error(err),
						)
					}
				}
			}
			return
		}

		addStartMetrics(insertData, deleteData)

		switch currentHint {
		case engine.ChangesHandle_Snapshot:
			// output sql in a txn
			if !hasBegin && !reader.initSnapshotSplitTxn {
				reader.sinker.SendBegin()
				hasBegin = true
			}

			// transform into insert instantly
			reader.sinker.Sink(ctx, &DecoderOutput{
				outputTyp:     OutputTypeSnapshot,
				checkpointBat: insertData,
				fromTs:        fromTs,
				toTs:          toTs,
			})
			addSnapshotEndMetrics(insertData)
			insertData.Clean(reader.mp)
		case engine.ChangesHandle_Tail_wip:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, reader.insTsColIdx, reader.insCompositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, reader.delTsColIdx, reader.delCompositedPkColIdx)
		case engine.ChangesHandle_Tail_done:
			insertAtmBatch = allocateAtomicBatchIfNeed(insertAtmBatch)
			deleteAtmBatch = allocateAtomicBatchIfNeed(deleteAtmBatch)
			insertAtmBatch.Append(packer, insertData, reader.insTsColIdx, reader.insCompositedPkColIdx)
			deleteAtmBatch.Append(packer, deleteData, reader.delTsColIdx, reader.delCompositedPkColIdx)

			//if !strings.Contains(reader.info.SourceTblName, "order") {
			//	logutil.Errorf("tableReader(%s)[%s, %s], insertAtmBatch: %s, deleteAtmBatch: %s",
			//		reader.info.SourceTblName, fromTs.ToString(), toTs.ToString(),
			//		insertAtmBatch.DebugString(reader.tableDef, false),
			//		deleteAtmBatch.DebugString(reader.tableDef, true))
			//}

			// output sql in a txn
			if !hasBegin {
				reader.sinker.SendBegin()
				hasBegin = true
			}

			reader.sinker.Sink(ctx, &DecoderOutput{
				outputTyp:      OutputTypeTail,
				insertAtmBatch: insertAtmBatch,
				deleteAtmBatch: deleteAtmBatch,
				fromTs:         fromTs,
				toTs:           toTs,
			})
			addTailEndMetrics(insertAtmBatch)
			addTailEndMetrics(deleteAtmBatch)
			insertAtmBatch.Close()
			deleteAtmBatch.Close()
			// reset, allocate new when next wip/done
			insertAtmBatch = nil
			deleteAtmBatch = nil
		}
	}
}
