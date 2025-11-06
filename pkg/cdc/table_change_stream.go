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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

var _ ChangeReader = new(TableChangeStream)

// TableChangeStream captures and streams table changes for CDC
// This is a complete CDC data pipeline that:
// - Monitors table changes continuously
// - Manages transactions with dual-layer safety
// - Processes and transforms change data
// - Handles retries and error recovery
// - Maintains watermarks and state
type TableChangeStream struct {
	// Core V2 components
	txnManager    *TransactionManager
	dataProcessor *DataProcessor

	// Infrastructure
	cnTxnClient client.TxnClient
	cnEngine    engine.Engine
	mp          *mpool.MPool
	packerPool  *fileservice.Pool[*types.Packer]

	// Table identity
	accountId uint64
	taskId    string
	tableInfo *DbTableInfo
	tableDef  *plan.TableDef

	// Downstream
	sinker           Sinker
	watermarkUpdater WatermarkUpdater
	watermarkKey     *WatermarkKey

	// Lifecycle management
	tick             *time.Ticker
	frequency        time.Duration
	force            bool
	runningReaders   *sync.Map
	runningReaderKey string
	wg               sync.WaitGroup

	// Configuration
	initSnapshotSplitTxn bool
	startTs, endTs       types.TS
	noFull               bool

	// Column indices (for AtomicBatch)
	insTsColIdx           int
	insCompositedPkColIdx int
	delTsColIdx           int
	delCompositedPkColIdx int

	// State
	lastError error
	retryable bool
}

// NewTableChangeStream creates a new table change stream
var NewTableChangeStream = func(
	cnTxnClient client.TxnClient,
	cnEngine engine.Engine,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	accountId uint64,
	taskId string,
	tableInfo *DbTableInfo,
	sinker Sinker,
	watermarkUpdater WatermarkUpdater,
	tableDef *plan.TableDef,
	initSnapshotSplitTxn bool,
	runningReaders *sync.Map,
	startTs, endTs types.TS,
	noFull bool,
	frequency time.Duration,
) *TableChangeStream {
	// Parse frequency
	if frequency <= 0 {
		frequency = DefaultFrequency
	}

	// Create watermark key
	watermarkKey := &WatermarkKey{
		AccountId: accountId,
		TaskId:    taskId,
		DBName:    tableInfo.SourceDbName,
		TableName: tableInfo.SourceTblName,
	}

	// Create transaction manager
	txnManager := NewTransactionManager(
		sinker,
		watermarkUpdater,
		accountId,
		taskId,
		tableInfo.SourceDbName,
		tableInfo.SourceTblName,
	)

	// Calculate column indices
	// batch columns layout:
	// 1. data: user defined cols | cpk (if needed) | commit-ts
	// 2. tombstone: pk/cpk | commit-ts
	insTsColIdx := len(tableDef.Cols) - 1
	insCompositedPkColIdx := len(tableDef.Cols) - 2
	delTsColIdx := 1
	delCompositedPkColIdx := 0

	// if single col pk, there's no additional cpk col
	if len(tableDef.Pkey.Names) == 1 {
		insCompositedPkColIdx = int(tableDef.Name2ColIndex[tableDef.Pkey.Names[0]])
	}

	// Create data processor
	dataProcessor := NewDataProcessor(
		sinker,
		txnManager,
		mp,
		packerPool,
		insTsColIdx,
		insCompositedPkColIdx,
		delTsColIdx,
		delCompositedPkColIdx,
		initSnapshotSplitTxn,
		accountId,
		taskId,
		tableInfo.SourceDbName,
		tableInfo.SourceTblName,
	)

	return &TableChangeStream{
		txnManager:            txnManager,
		dataProcessor:         dataProcessor,
		cnTxnClient:           cnTxnClient,
		cnEngine:              cnEngine,
		mp:                    mp,
		packerPool:            packerPool,
		accountId:             accountId,
		taskId:                taskId,
		tableInfo:             tableInfo,
		tableDef:              tableDef,
		sinker:                sinker,
		watermarkUpdater:      watermarkUpdater,
		watermarkKey:          watermarkKey,
		tick:                  time.NewTicker(frequency),
		frequency:             frequency,
		runningReaders:        runningReaders,
		runningReaderKey:      GenDbTblKey(tableInfo.SourceDbName, tableInfo.SourceTblName),
		initSnapshotSplitTxn:  initSnapshotSplitTxn,
		startTs:               startTs,
		endTs:                 endTs,
		noFull:                noFull,
		insTsColIdx:           insTsColIdx,
		insCompositedPkColIdx: insCompositedPkColIdx,
		delTsColIdx:           delTsColIdx,
		delCompositedPkColIdx: delCompositedPkColIdx,
	}
}

// Run starts the change stream
func (s *TableChangeStream) Run(ctx context.Context, ar *ActiveRoutine) {
	// 1. Check for duplicate readers
	if _, loaded := s.runningReaders.LoadOrStore(s.runningReaderKey, s); loaded {
		logutil.Warn(
			"CDC-TableChangeStream-DuplicateRunning",
			zap.String("table", s.tableInfo.String()),
			zap.String("task-id", s.taskId),
			zap.Uint64("account-id", s.accountId),
		)
		s.Close()
		return
	}

	logutil.Info(
		"CDC-TableChangeStream-Start",
		zap.String("table", s.tableInfo.String()),
		zap.Uint64("account-id", s.accountId),
		zap.String("task-id", s.taskId),
	)

	// 2. Register with CDC detector
	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.AddActiveRunner(s.tableInfo)
	}

	// 3. Setup lifecycle
	s.wg.Add(1)
	defer s.cleanup(ctx)

	// 4. Calculate initial delay based on last sync time
	if err := s.calculateInitialDelay(ctx); err != nil {
		logutil.Error(
			"CDC-TableChangeStream-CalculateInitialDelayFailed",
			zap.String("table", s.tableInfo.String()),
			zap.Error(err),
		)
	}

	// 5. Main loop
	for {
		select {
		case <-ctx.Done():
			return
		case <-ar.Pause:
			return
		case <-ar.Cancel:
			return
		case <-s.tick.C:
		}

		// Reset frequency if forced
		if s.force {
			s.force = false
			s.tick.Reset(s.frequency)
		}

		// Process one round
		if err := s.processOneRound(ctx, ar); err != nil {
			s.lastError = err
			logutil.Error(
				"CDC-TableChangeStream-ProcessFailed",
				zap.String("table", s.tableInfo.String()),
				zap.Bool("retryable", s.retryable),
				zap.Error(err),
			)
			return
		}
	}
}

// cleanup performs cleanup when the stream stops
func (s *TableChangeStream) cleanup(ctx context.Context) {
	defer s.wg.Done()

	// Remove from running readers
	s.runningReaders.Delete(s.runningReaderKey)

	// Remove watermark cache
	if err := s.watermarkUpdater.RemoveCachedWM(ctx, s.watermarkKey); err != nil {
		logutil.Error(
			"CDC-TableChangeStream-RemoveCachedWMFailed",
			zap.String("table", s.tableInfo.String()),
			zap.Error(err),
		)
	}

	// Persist error message if any
	if s.lastError != nil {
		errorCtx := &ErrorContext{
			IsRetryable:     s.retryable,
			IsPauseOrCancel: IsPauseOrCancelError(s.lastError.Error()),
		}

		if err := s.watermarkUpdater.UpdateWatermarkErrMsg(ctx, s.watermarkKey, s.lastError.Error(), errorCtx); err != nil {
			logutil.Error(
				"CDC-TableChangeStream-UpdateWatermarkErrMsgFailed",
				zap.String("table", s.tableInfo.String()),
				zap.Error(err),
			)
		}
	}

	// Close sinker
	s.Close()

	// Unregister from CDC detector
	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.RemoveActiveRunner(s.tableInfo)
	}

	logutil.Info(
		"CDC-TableChangeStream-End",
		zap.String("table", s.tableInfo.String()),
		zap.Uint64("account-id", s.accountId),
		zap.String("task-id", s.taskId),
	)
}

// calculateInitialDelay calculates initial delay based on last sync time
func (s *TableChangeStream) calculateInitialDelay(ctx context.Context) error {
	lastSync, err := s.getLastSyncTime(ctx)
	if err != nil {
		logutil.Warn(
			"CDC-TableChangeStream-GetLastSyncTimeFailed",
			zap.String("table", s.tableInfo.String()),
			zap.Error(err),
		)
		return nil // Not fatal, continue with default
	}

	// Calculate next sync time
	nextSyncTime := lastSync.Add(s.frequency)
	now := time.Now()

	var wait time.Duration
	if now.Before(nextSyncTime) {
		wait = nextSyncTime.Sub(now)
	} else {
		wait = DefaultFrequency
	}

	s.forceNextInterval(wait)
	return nil
}

// getLastSyncTime gets the last sync time from watermark
func (s *TableChangeStream) getLastSyncTime(ctx context.Context) (time.Time, error) {
	ts, err := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
	if err == nil && !ts.ToTimestamp().IsEmpty() {
		return ts.ToTimestamp().ToStdTime(), nil
	}

	// Try to get from committed cache
	defaultTS := s.startTs
	ts, err = s.watermarkUpdater.GetOrAddCommitted(ctx, s.watermarkKey, &defaultTS)
	if err != nil {
		return time.Time{}, err
	}
	return ts.ToTimestamp().ToStdTime(), nil
}

// forceNextInterval forces the next tick interval
func (s *TableChangeStream) forceNextInterval(wait time.Duration) {
	logutil.Info(
		"CDC-TableChangeStream-ForceNextInterval",
		zap.String("table", s.tableInfo.String()),
		zap.Duration("wait", wait),
	)
	s.force = true
	s.tick.Reset(wait)
}

// processOneRound processes one round of change streaming
func (s *TableChangeStream) processOneRound(ctx context.Context, ar *ActiveRoutine) error {
	// Create transaction operator
	txnOp, err := GetTxnOp(ctx, s.cnEngine, s.cnTxnClient, "tableChangeStream")
	if err != nil {
		return err
	}
	defer FinishTxnOp(ctx, err, txnOp, s.cnEngine)

	EnterRunSql(txnOp)
	defer ExitRunSql(txnOp)

	if err = GetTxn(ctx, s.cnEngine, txnOp); err != nil {
		return err
	}

	// Get packer from pool
	var packer *types.Packer
	put := s.packerPool.Get(&packer)
	defer put.Put()

	// Process with transaction
	err = s.processWithTxn(ctx, txnOp, packer, ar)

	// Handle StaleRead error
	if moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
		return s.handleStaleRead(ctx, txnOp)
	}

	return err
}

// processWithTxn processes changes within a transaction
func (s *TableChangeStream) processWithTxn(
	ctx context.Context,
	txnOp client.TxnOperator,
	packer *types.Packer,
	ar *ActiveRoutine,
) error {
	// Get relation
	_, _, rel, err := GetRelationById(ctx, s.cnEngine, txnOp, s.tableInfo.SourceTblId)
	if err != nil {
		// Table may have been truncated
		s.retryable = true
		return err
	}

	// Get time range
	fromTs, err := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
	if err != nil {
		return err
	}

	// Check if reached end time
	if !s.endTs.IsEmpty() && fromTs.GE(&s.endTs) {
		logutil.Info(
			"CDC-TableChangeStream-ReachedEndTs",
			zap.String("table", s.tableInfo.String()),
			zap.String("from-ts", fromTs.ToString()),
			zap.String("end-ts", s.endTs.ToString()),
		)
		return nil // Graceful end
	}

	toTs := types.TimestampToTS(GetSnapshotTS(txnOp))
	if !s.endTs.IsEmpty() && toTs.GT(&s.endTs) {
		toTs = s.endTs
	}

	// Update CDC detector
	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.UpdateActiveRunner(s.tableInfo, fromTs, toTs, true)
		defer detector.cdcStateManager.UpdateActiveRunner(s.tableInfo, fromTs, toTs, false)
	}

	// Collect changes
	start := time.Now()
	changesHandle, err := CollectChanges(ctx, rel, fromTs, toTs, s.mp)
	v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}

	// Create change collector
	collector := NewChangeCollector(
		changesHandle,
		s.mp,
		fromTs,
		toTs,
		s.accountId,
		s.taskId,
		s.tableInfo.SourceDbName,
		s.tableInfo.SourceTblName,
	)
	defer collector.Close()

	// Set transaction range for data processor
	s.dataProcessor.SetTransactionRange(fromTs, toTs)
	defer s.dataProcessor.Cleanup()

	// Ensure cleanup on any error
	defer func() {
		if err != nil || ctx.Err() != nil {
			if cleanupErr := s.txnManager.EnsureCleanup(ctx); cleanupErr != nil {
				logutil.Error(
					"CDC-TableChangeStream-EnsureCleanupFailed",
					zap.String("table", s.tableInfo.String()),
					zap.Error(cleanupErr),
				)
			}
		}
	}()

	// Process changes
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ar.Pause:
			return moerr.NewInternalErrorf(ctx, "paused")
		case <-ar.Cancel:
			return moerr.NewInternalErrorf(ctx, "cancelled")
		default:
		}

		// Update memory pool metrics
		v2.CdcMpoolInUseBytesGauge.Set(float64(s.mp.Stats().NumCurrBytes.Load()))

		// Get next change
		start = time.Now()
		changeData, err := collector.Next(ctx)
		v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
		if err != nil {
			return err
		}

		// Process change
		if err = s.dataProcessor.ProcessChange(ctx, changeData); err != nil {
			return err
		}

		// If no more data, we're done
		if changeData.Type == ChangeTypeNoMoreData {
			return nil
		}
	}
}

// handleStaleRead handles StaleRead error by resetting watermark
func (s *TableChangeStream) handleStaleRead(ctx context.Context, txnOp client.TxnOperator) error {
	// If startTs is set and noFull is false, StaleRead is fatal
	if !s.noFull && !s.startTs.IsEmpty() {
		s.retryable = false
		return moerr.NewInternalErrorf(
			ctx,
			"CDC tableChangeStream %s stale read with startTs %s set, cannot recover",
			s.tableInfo.String(),
			s.startTs.ToString(),
		)
	}

	// Reset sinker
	s.sinker.Reset()

	// Determine reset watermark
	watermark := s.startTs
	if s.noFull && txnOp != nil {
		watermark = types.TimestampToTS(txnOp.SnapshotTS())
	}

	// Update watermark
	if err := s.watermarkUpdater.UpdateWatermarkOnly(ctx, s.watermarkKey, &watermark); err != nil {
		logutil.Error(
			"CDC-TableChangeStream-UpdateWatermarkOnlyFailed",
			zap.String("table", s.tableInfo.String()),
			zap.String("watermark", watermark.ToString()),
			zap.Error(err),
		)
	}

	logutil.Info(
		"CDC-TableChangeStream-ResetWatermarkOnStaleRead",
		zap.String("table", s.tableInfo.String()),
		zap.String("watermark", watermark.ToString()),
	)

	// Force next interval
	s.forceNextInterval(DefaultFrequency)

	// Mark as retryable and swallow error
	s.retryable = true
	return nil
}

// Close closes the change stream
func (s *TableChangeStream) Close() {
	s.sinker.Close()
}

// Info returns the table information
// Wait blocks until the reader goroutine completes
func (s *TableChangeStream) Wait() {
	s.wg.Wait()
}

// GetTableInfo returns the source table information
func (s *TableChangeStream) GetTableInfo() *DbTableInfo {
	return s.tableInfo
}
