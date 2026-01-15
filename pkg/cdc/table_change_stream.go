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
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
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
	start            sync.WaitGroup
	runCancel        context.CancelFunc
	cancelOnce       sync.Once

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
	stateMu            sync.Mutex // Protects lastError, retryable, and cleanupRollbackErr for consistency
	lastError          error
	retryable          bool        // Protected by stateMu, updated together with lastError
	cleanupRollbackErr error       // Set by processWithTxn defer if rollback fails (protected by stateMu)
	hasSucceeded       atomic.Bool // Tracks if reader has successfully processed data at least once

	// Retry state with exponential backoff
	retryCount      int           // Current retry count for the same error type
	lastErrorType   string        // Last error type for tracking consecutive errors
	nextRetryDelay  time.Duration // Next retry delay (exponential backoff)
	lastErrorTime   time.Time     // When last error occurred
	originalError   error         // Original error that triggered retry (preserved during retries)
	originalErrType string        // Type of original error (for error type consistency check)

	// Observability
	progressTracker *ProgressTracker

	// Watermark stall detection
	watermarkStallThreshold   time.Duration
	noProgressWarningInterval time.Duration
	lastWatermarkAdvance      time.Time
	noProgressSince           time.Time
	lastNoProgressWarning     time.Time

	// Retry configuration
	maxRetryCount      int           // Max retry count before converting to non-retryable
	retryBackoffBase   time.Duration // Base delay for exponential backoff
	retryBackoffMax    time.Duration // Max delay for exponential backoff
	retryBackoffFactor float64       // Factor for exponential backoff
}

type TableChangeStreamOption func(*tableChangeStreamOptions)

type tableChangeStreamOptions struct {
	watermarkStallThreshold   time.Duration
	noProgressWarningInterval time.Duration
	maxRetryCount             int           // Max retry count before converting to non-retryable
	retryBackoffBase          time.Duration // Base delay for exponential backoff
	retryBackoffMax           time.Duration // Max delay for exponential backoff
	retryBackoffFactor        float64       // Factor for exponential backoff
}

const (
	defaultWatermarkStallThreshold   = time.Minute
	defaultNoProgressWarningInterval = 10 * time.Second
	defaultMaxRetryCount             = 3
	defaultRetryBackoffBase          = 200 * time.Millisecond
	defaultRetryBackoffMax           = 30 * time.Second
	defaultRetryBackoffFactor        = 2.0
)

func defaultTableChangeStreamOptions() *tableChangeStreamOptions {
	opts := &tableChangeStreamOptions{}
	opts.fillDefaults()
	return opts
}

func (opts *tableChangeStreamOptions) fillDefaults() {
	if opts.watermarkStallThreshold <= 0 {
		opts.watermarkStallThreshold = defaultWatermarkStallThreshold
	}
	if opts.noProgressWarningInterval <= 0 {
		opts.noProgressWarningInterval = defaultNoProgressWarningInterval
	}
	if opts.maxRetryCount <= 0 {
		opts.maxRetryCount = defaultMaxRetryCount
	}
	if opts.retryBackoffBase <= 0 {
		opts.retryBackoffBase = defaultRetryBackoffBase
	}
	if opts.retryBackoffMax <= 0 {
		opts.retryBackoffMax = defaultRetryBackoffMax
	}
	if opts.retryBackoffFactor <= 1.0 {
		opts.retryBackoffFactor = defaultRetryBackoffFactor
	}
}

// WithWatermarkStallThreshold configures how long snapshot stagnation is tolerated before surfacing an error.
func WithWatermarkStallThreshold(threshold time.Duration) TableChangeStreamOption {
	return func(opts *tableChangeStreamOptions) {
		opts.watermarkStallThreshold = threshold
	}
}

// WithNoProgressWarningInterval configures how frequently warnings are emitted when snapshot timestamps stall.
func WithNoProgressWarningInterval(interval time.Duration) TableChangeStreamOption {
	return func(opts *tableChangeStreamOptions) {
		opts.noProgressWarningInterval = interval
	}
}

// WithMaxRetryCount configures the maximum retry count before converting retryable errors to non-retryable.
func WithMaxRetryCount(count int) TableChangeStreamOption {
	return func(opts *tableChangeStreamOptions) {
		opts.maxRetryCount = count
	}
}

// WithRetryBackoff configures exponential backoff parameters for retryable errors.
func WithRetryBackoff(base, max time.Duration, factor float64) TableChangeStreamOption {
	return func(opts *tableChangeStreamOptions) {
		opts.retryBackoffBase = base
		opts.retryBackoffMax = max
		opts.retryBackoffFactor = factor
	}
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
	options ...TableChangeStreamOption,
) *TableChangeStream {
	// Parse frequency
	if frequency <= 0 {
		frequency = DefaultFrequency
	}

	opts := defaultTableChangeStreamOptions()
	for _, opt := range options {
		if opt != nil {
			opt(opts)
		}
	}
	opts.fillDefaults()

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

	// Create progress tracker for observability
	progressTracker := NewProgressTracker(
		accountId,
		taskId,
		tableInfo.SourceDbName,
		tableInfo.SourceTblName,
	)

	if attachable, ok := sinker.(interface{ AttachProgressTracker(*ProgressTracker) }); ok {
		attachable.AttachProgressTracker(progressTracker)
	}

	stream := &TableChangeStream{
		txnManager:                txnManager,
		dataProcessor:             dataProcessor,
		cnTxnClient:               cnTxnClient,
		cnEngine:                  cnEngine,
		mp:                        mp,
		packerPool:                packerPool,
		accountId:                 accountId,
		taskId:                    taskId,
		tableInfo:                 tableInfo,
		tableDef:                  tableDef,
		sinker:                    sinker,
		watermarkUpdater:          watermarkUpdater,
		watermarkKey:              watermarkKey,
		tick:                      time.NewTicker(frequency),
		frequency:                 frequency,
		runningReaders:            runningReaders,
		runningReaderKey:          GenDbTblKey(tableInfo.SourceDbName, tableInfo.SourceTblName),
		initSnapshotSplitTxn:      initSnapshotSplitTxn,
		startTs:                   startTs,
		endTs:                     endTs,
		noFull:                    noFull,
		insTsColIdx:               insTsColIdx,
		insCompositedPkColIdx:     insCompositedPkColIdx,
		delTsColIdx:               delTsColIdx,
		delCompositedPkColIdx:     delCompositedPkColIdx,
		progressTracker:           progressTracker,
		watermarkStallThreshold:   opts.watermarkStallThreshold,
		noProgressWarningInterval: opts.noProgressWarningInterval,
		lastWatermarkAdvance:      time.Now(),
		// Retry configuration (stored for access in retry logic)
		maxRetryCount:      opts.maxRetryCount,
		retryBackoffBase:   opts.retryBackoffBase,
		retryBackoffMax:    opts.retryBackoffMax,
		retryBackoffFactor: opts.retryBackoffFactor,
	}

	tableLabel := progressTracker.tableKey()
	v2.CdcTableStuckGauge.WithLabelValues(tableLabel).Set(0)
	v2.CdcTableLastActivityTimestamp.WithLabelValues(tableLabel).Set(float64(time.Now().Unix()))

	stream.start.Add(1)
	return stream
}

// Run starts the change stream
func (s *TableChangeStream) Run(ctx context.Context, ar *ActiveRoutine) {
	streamCtx, cancel := context.WithCancel(ctx)
	s.runCancel = cancel

	s.wg.Add(1)
	s.start.Done()

	// 1. Check for duplicate readers
	if _, loaded := s.runningReaders.LoadOrStore(s.runningReaderKey, s); loaded {
		logutil.Warn(
			"cdc.table_stream.duplicate_running",
			zap.String("table", s.tableInfo.String()),
			zap.String("task-id", s.taskId),
			zap.Uint64("account-id", s.accountId),
		)
		s.wg.Done()
		s.Close()
		return
	}

	logutil.Info(
		"cdc.table_stream.start",
		zap.String("table", s.tableInfo.String()),
		zap.Uint64("account-id", s.accountId),
		zap.String("task-id", s.taskId),
		zap.String("frequency", s.frequency.String()),
		zap.String("start-ts", s.startTs.ToString()),
		zap.String("end-ts", s.endTs.ToString()),
		zap.Bool("no-full", s.noFull),
	)

	// 2. Register with CDC detector
	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.AddActiveRunner(s.tableInfo)
	}

	// 3. Register with observability manager
	obsManager := GetObservabilityManager()
	obsManager.RegisterTableStream(s)
	defer obsManager.UnregisterTableStream(s)

	// 4. Setup lifecycle
	defer s.cleanup(ctx)

	// 5. Initialize progress tracker
	s.progressTracker.SetState("idle")

	// 5. Calculate initial delay based on last sync time
	if err := s.calculateInitialDelay(streamCtx); err != nil {
		logutil.Error(
			"cdc.table_stream.calculate_initial_delay_failed",
			zap.String("table", s.tableInfo.String()),
			zap.Error(err),
		)
	}

	// 6. Main loop
	for {
		select {
		case <-streamCtx.Done():
			logutil.Info(
				"cdc.table_stream.stop_context_done",
				zap.String("table", s.tableInfo.String()),
				zap.String("task-id", s.taskId),
			)
			s.cancelRun()
			return
		case <-ar.Pause:
			// Get current watermark when receiving pause signal in main loop
			currentWatermark, _ := s.watermarkUpdater.GetFromCache(streamCtx, s.watermarkKey)
			logutil.Info(
				"cdc.table_stream.stop_pause_signal",
				zap.String("task-id", s.taskId),
				zap.String("table", s.tableInfo.String()),
				zap.String("current-watermark", currentWatermark.ToString()),
			)
			s.cancelRun()
			return
		case <-ar.Cancel:
			// Get current watermark when receiving cancel signal in main loop
			currentWatermark, _ := s.watermarkUpdater.GetFromCache(streamCtx, s.watermarkKey)
			logutil.Info(
				"cdc.table_stream.stop_cancel_signal",
				zap.String("task-id", s.taskId),
				zap.String("table", s.tableInfo.String()),
				zap.String("current-watermark", currentWatermark.ToString()),
			)
			s.cancelRun()
			return
		case <-s.tick.C:
			logutil.Debug(
				"cdc.table_stream.tick",
				zap.String("table", s.tableInfo.String()),
				zap.String("task-id", s.taskId),
			)
		}

		// Reset frequency if forced
		if s.force {
			s.force = false
			s.tick.Reset(s.frequency)
		}

		// Process one round with detailed tracking
		s.progressTracker.SetState("processing")
		roundStart := time.Now()
		logutil.Debug(
			"cdc.table_stream.process_round_start",
			zap.String("table", s.tableInfo.String()),
			zap.String("task-id", s.taskId),
		)
		err := s.processOneRound(streamCtx, ar)

		if err != nil {
			// Update lastError and retryable atomically for consistency
			s.updateErrorState(err)

			// Check if cleanup failed with rollback error (set by processWithTxn defer)
			// If so, override retryable to false even if main error is retryable
			// Rollback failure indicates system state may be inconsistent, so should not retry
			// However, preserve retryable state if error is a pause/cancel control signal
			s.stateMu.Lock()
			if s.cleanupRollbackErr != nil {
				// Mark as non-retryable unless it's a pause/cancel control signal
				if !IsPauseOrCancelError(err.Error()) {
					s.retryable = false
				}
			}
			retryable := s.retryable
			retryCount := s.retryCount
			nextRetryDelay := s.nextRetryDelay
			errType := s.lastErrorType
			s.stateMu.Unlock()

			s.progressTracker.SetState("error")
			s.progressTracker.RecordError(err)
			s.progressTracker.EndRound(false, err)

			logutil.Error(
				"cdc.table_stream.process_failed",
				zap.String("table", s.tableInfo.String()),
				zap.Bool("retryable", retryable),
				zap.Int("retry-count", retryCount),
				zap.String("error-type", errType),
				zap.Duration("next-retry-delay", nextRetryDelay),
				zap.Duration("round-duration", time.Since(roundStart)),
				zap.Error(err),
			)

			// Record retry metrics
			tableLabel := s.tableInfo.String()
			v2.CdcTableStreamRetryCounter.WithLabelValues(tableLabel, errType, "attempted").Inc()
			if nextRetryDelay > 0 {
				v2.CdcTableStreamRetryDelayHistogram.WithLabelValues(tableLabel, errType).Observe(nextRetryDelay.Seconds())
			}

			// If non-retryable or exceeded max retry count, stop processing
			if !retryable || retryCount > s.maxRetryCount {
				// Ensure cleanup is called before stopping (processWithTxn's defer should have already called it,
				// but we call it again here to be safe, especially for the case where retryCount > maxRetryCount)
				// This ensures sinker errors are cleared even if we stop due to exceeding max retry count
				_ = s.txnManager.EnsureCleanup(streamCtx)

				// Preserve original error for deterministic error reporting
				// This ensures the error that triggered retry is preserved in lastError,
				// not auxiliary errors encountered during retry attempts
				s.stateMu.Lock()
				originalErr := s.originalError
				originalErrType := s.originalErrType
				if originalErr != nil {
					// Restore original error to lastError for final error reporting
					s.lastError = originalErr
					// Record that original error was preserved
					tableLabel := s.tableInfo.String()
					v2.CdcTableStreamOriginalErrorPreservedCounter.WithLabelValues(tableLabel, originalErrType).Inc()
				}
				s.stateMu.Unlock()

				// Record retry exhaustion
				tableLabel := s.tableInfo.String()
				if retryCount > s.maxRetryCount {
					v2.CdcTableStreamRetryCounter.WithLabelValues(tableLabel, errType, "exhausted").Inc()
				} else {
					v2.CdcTableStreamRetryCounter.WithLabelValues(tableLabel, errType, "failed").Inc()
				}
				return
			}

			// Apply exponential backoff: wait before next retry
			// Use nextRetryDelay if calculated, otherwise use frequency
			backoffDelay := nextRetryDelay
			if backoffDelay <= 0 {
				backoffDelay = s.frequency
			}

			logutil.Info(
				"cdc.table_stream.retry_with_backoff",
				zap.String("table", s.tableInfo.String()),
				zap.Int("retry-count", retryCount),
				zap.String("error-type", errType),
				zap.Duration("backoff-delay", backoffDelay),
			)

			// Reset tick with backoff delay
			s.tick.Reset(backoffDelay)
			// Continue loop to wait for next tick
			continue
		}

		// Success: reset retry state and use normal frequency
		s.stateMu.Lock()
		hadRetries := s.retryCount > 0
		lastErrType := s.lastErrorType
		s.retryCount = 0
		s.lastErrorType = ""
		s.nextRetryDelay = 0
		s.originalError = nil  // Clear original error on success
		s.originalErrType = "" // Clear original error type on success
		s.stateMu.Unlock()

		// Record successful retry if there were previous retries
		if hadRetries && lastErrType != "" {
			tableLabel := s.tableInfo.String()
			v2.CdcTableStreamRetryCounter.WithLabelValues(tableLabel, lastErrType, "succeeded").Inc()
		}

		logutil.Debug(
			"cdc.table_stream.process_round_done",
			zap.String("table", s.tableInfo.String()),
			zap.String("task-id", s.taskId),
			zap.Duration("round-duration", time.Since(roundStart)),
		)
		s.progressTracker.SetState("idle")
	}
}

// cleanup performs cleanup when the stream stops
func (s *TableChangeStream) cleanup(ctx context.Context) {
	startTime := time.Now()
	logutil.Debug(
		"cdc.table_stream.cleanup_start",
		zap.String("table", s.tableInfo.String()),
		zap.Uint64("account-id", s.accountId),
		zap.String("task-id", s.taskId),
	)
	defer s.wg.Done()
	defer func() {
		// Decrement table stream state gauge on cleanup
		if s.progressTracker != nil {
			state, _ := s.progressTracker.GetState()
			if state != "" {
				v2.CdcTableStreamTotalGauge.WithLabelValues(state).Dec()
			}
		}
		logutil.Debug(
			"cdc.table_stream.cleanup_done",
			zap.String("table", s.tableInfo.String()),
			zap.Uint64("account-id", s.accountId),
			zap.String("task-id", s.taskId),
			zap.Duration("cost", time.Since(startTime)),
		)
	}()

	// Remove from running readers
	s.runningReaders.Delete(s.runningReaderKey)

	// Remove watermark cache
	removeStart := time.Now()
	if err := s.watermarkUpdater.RemoveCachedWM(ctx, s.watermarkKey); err != nil {
		logutil.Error(
			"cdc.table_stream.remove_cached_watermark_failed",
			zap.String("table", s.tableInfo.String()),
			zap.Error(err),
		)
	} else {
		logutil.Debug(
			"cdc.table_stream.remove_cached_watermark_done",
			zap.String("table", s.tableInfo.String()),
			zap.Duration("cost", time.Since(removeStart)),
		)
	}

	// Persist error message if any
	// Read lastError and retryable atomically for consistency
	s.stateMu.Lock()
	lastError := s.lastError
	retryable := s.retryable
	s.stateMu.Unlock()

	if lastError != nil {
		errorCtx := &ErrorContext{
			IsRetryable:     retryable,
			IsPauseOrCancel: IsPauseOrCancelError(lastError.Error()),
		}

		if err := s.watermarkUpdater.UpdateWatermarkErrMsg(ctx, s.watermarkKey, lastError.Error(), errorCtx); err != nil {
			logutil.Error(
				"cdc.table_stream.update_watermark_errmsg_failed",
				zap.String("table", s.tableInfo.String()),
				zap.Error(err),
			)
		}
	}

	// Close sinker
	closeStart := time.Now()
	s.Close()
	logutil.Debug(
		"cdc.table_stream.close_done",
		zap.String("table", s.tableInfo.String()),
		zap.Duration("cost", time.Since(closeStart)),
	)

	// Unregister from CDC detector
	if detector != nil && detector.cdcStateManager != nil {
		detector.cdcStateManager.RemoveActiveRunner(s.tableInfo)
	}

	// Clean up table-level metrics to prevent stale metrics after task deletion
	// This ensures metrics are removed even if watermark cache was already cleared
	if s.progressTracker != nil {
		tableLabel := s.progressTracker.tableKey()
		v2.CdcTableLastActivityTimestamp.DeleteLabelValues(tableLabel)
		v2.CdcTableStuckGauge.DeleteLabelValues(tableLabel)
		// Note: Other metrics like CdcWatermarkLagSeconds are cleaned up by watermark_updater
		// orphan cleanup logic, but we clean up table-specific metrics here for completeness
	}

	logutil.Debug(
		"cdc.table_stream.end",
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
			"cdc.table_stream.get_last_sync_time_failed",
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
		"cdc.table_stream.force_next_interval",
		zap.String("table", s.tableInfo.String()),
		zap.Duration("wait", wait),
	)
	s.force = true
	s.tick.Reset(wait)
}

// processOneRound processes one round of change streaming
func (s *TableChangeStream) processOneRound(ctx context.Context, ar *ActiveRoutine) error {
	// Clear previous cleanup rollback error
	s.stateMu.Lock()
	s.cleanupRollbackErr = nil
	s.stateMu.Unlock()

	// Create transaction operator
	txnOp, err := GetTxnOp(ctx, s.cnEngine, s.cnTxnClient, "tableChangeStream")
	if err != nil {
		return err
	}
	defer FinishTxnOp(ctx, err, txnOp, s.cnEngine)

	finishRunSQL := EnterRunSql(ctx, txnOp, "<cdc.table_change_stream>")
	defer finishRunSQL()

	if err = GetTxn(ctx, s.cnEngine, txnOp); err != nil {
		return err
	}

	// Get packer from pool
	var packer *types.Packer
	put := s.packerPool.Get(&packer)
	defer put.Put()

	// Process with transaction
	// Note: processWithTxn's defer will set cleanupRollbackErr if rollback fails
	err = s.processWithTxn(ctx, txnOp, packer, ar)

	// Check if cleanup failed with rollback error (set by processWithTxn's defer)
	s.stateMu.Lock()
	cleanupRollbackErr := s.cleanupRollbackErr
	s.stateMu.Unlock()

	// Handle StaleRead error
	if moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
		recoveryErr := s.handleStaleRead(ctx, txnOp)
		// If recovery succeeded (nil), mark as retryable and continue
		if recoveryErr == nil {
			// Recovery succeeded - mark as retryable for scheduler to retry
			s.stateMu.Lock()
			s.retryable = true
			s.stateMu.Unlock()
			return nil // Swallow error, continue processing
		}
		// Recovery failed - return error (will be marked non-retryable in determineRetryable)
		return recoveryErr
	}

	// If cleanup failed with rollback error, mark as non-retryable but return original error
	// Rollback failure indicates system state may be inconsistent, so should not retry
	// However, preserve retryable state if error is a pause/cancel control signal
	if cleanupRollbackErr != nil {
		s.stateMu.Lock()
		// Mark as non-retryable unless it's a pause/cancel control signal
		if err == nil || !IsPauseOrCancelError(err.Error()) {
			s.retryable = false
		}
		s.stateMu.Unlock()
		// Return original error (not cleanup error) as expected by tests
		return err
	}

	return err
}

// updateErrorState updates lastError and retryable atomically
// Also tracks retry count and error type for exponential backoff
// Design: Preserves original error during retries to ensure deterministic error reporting
func (s *TableChangeStream) updateErrorState(err error) {
	if err == nil {
		return
	}
	retryable := s.determineRetryable(err)
	errType := s.classifyErrorType(err)

	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	// If retryable is already true (e.g., from successful StaleRead recovery),
	// and new error is a control signal (pause/cancel), preserve the retryable state.
	// This ensures that successful recovery states are not overwritten by control signals.
	if s.retryable && IsPauseOrCancelError(err.Error()) {
		// Preserve retryable = true, but still update lastError
		s.lastError = err
		return
	}

	// Preserve original error during retries to ensure deterministic error reporting
	// Priority order:
	// 1. Check if it's an auxiliary error first (should never replace original, even if non-retryable)
	// 2. If no original error exists, set as original
	// 3. If non-retryable and not auxiliary, replace original (fatal error takes precedence)
	// 4. If different error type (new primary error), replace original
	isAuxiliaryError := s.isAuxiliaryError(err, errType)
	if isAuxiliaryError {
		// Auxiliary error: preserve original error, don't update originalError
		// But still update lastError for logging and state tracking
		s.lastError = err
		s.retryable = retryable
		// Don't update retry count for auxiliary errors
		// Record auxiliary error metric
		tableLabel := s.tableInfo.String()
		v2.CdcTableStreamAuxiliaryErrorCounter.WithLabelValues(tableLabel, errType).Inc()
		return
	}

	if s.originalError == nil {
		// First error: set as original
		s.originalError = err
		s.originalErrType = errType
	} else if !retryable {
		// Non-retryable error (and not auxiliary): replace original (fatal error takes precedence)
		s.originalError = err
		s.originalErrType = errType
	} else if s.originalErrType != errType {
		// Different error type: new primary error, replace original
		s.originalError = err
		s.originalErrType = errType
		s.retryCount = 1
		s.lastErrorType = errType
		s.lastErrorTime = time.Now()
		s.nextRetryDelay = s.calculateBackoffDelay(errType, s.retryCount)
		s.lastError = err
		s.retryable = retryable
		return
	}
	// Same error type as original: continue retry with same original error

	// Update error state
	s.lastError = err
	s.retryable = retryable

	// Track retry count and error type for exponential backoff
	if retryable {
		// If same error type, increment retry count
		if s.lastErrorType == errType {
			s.retryCount++
		} else {
			// Different error type, reset retry count
			s.retryCount = 1
			s.lastErrorType = errType
		}
		s.lastErrorTime = time.Now()

		// Calculate next retry delay based on error type and retry count
		s.nextRetryDelay = s.calculateBackoffDelay(errType, s.retryCount)
	} else {
		// Non-retryable error, reset retry state
		s.retryCount = 0
		s.lastErrorType = ""
		s.nextRetryDelay = 0
	}
}

// isAuxiliaryError checks if an error is an auxiliary error that occurs during cleanup/retry
// and should not replace the original error. Examples: GetFromCache failures during EnsureCleanup
func (s *TableChangeStream) isAuxiliaryError(err error, errType string) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()

	// Errors that occur during cleanup/auxiliary operations should not replace original error
	// These are typically infrastructure errors that happen during retry attempts
	auxiliaryPatterns := []string{
		"get_from_cache",
		"cache error",
		"ensure_cleanup",
	}

	for _, pattern := range auxiliaryPatterns {
		if strings.Contains(strings.ToLower(errMsg), pattern) {
			return true
		}
	}

	return false
}

// determineRetryable determines if an error is retryable based on error type and context
func (s *TableChangeStream) determineRetryable(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()

	// Control signals (pause/cancel) are not retryable
	if IsPauseOrCancelError(errMsg) {
		return false
	}

	// Check for MatrixOne system/network errors first (before string matching)
	// These errors indicate temporary system unavailability and should be retryable
	if moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) ||
		moerr.IsMoErrCode(err, moerr.ErrTNShardNotFound) ||
		moerr.IsMoErrCode(err, moerr.ErrRpcError) ||
		moerr.IsMoErrCode(err, moerr.ErrClientClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendClosed) {
		return true
	}

	// Check for context timeout errors (system/network issues)
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// StaleRead errors are retryable if recovery is possible
	if moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
		// If startTs is set and noFull is false, StaleRead is fatal (handled in handleStaleRead)
		// If handleStaleRead returns nil, recovery succeeded (retryable)
		// If handleStaleRead returns error, recovery failed (non-retryable)
		return false // Will be updated in handleStaleRead, but default to false for safety
	}

	// Table relation errors (table truncated, etc.) are retryable
	if strings.Contains(errMsg, "relation") || strings.Contains(errMsg, "truncated") {
		return true
	}

	// Watermark stall errors are retryable
	if strings.Contains(errMsg, "stuck") || strings.Contains(errMsg, "stall") {
		return true
	}

	// Commit failures are retryable (usually transient network issues)
	if strings.Contains(errMsg, "commit") {
		return true
	}

	// Connection issues are retryable (check before begin/rollback to catch network errors)
	if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "network") || strings.Contains(errMsg, "unavailable") ||
		strings.Contains(errMsg, "rpc") || strings.Contains(errMsg, "backend") ||
		strings.Contains(errMsg, "shard") {
		return true
	}

	// Begin failures: retryable if network-related, non-retryable if state-related
	if strings.Contains(errMsg, "begin") {
		// Network-related begin failures are retryable
		if strings.Contains(errMsg, "connection") || strings.Contains(errMsg, "timeout") ||
			strings.Contains(errMsg, "network") || strings.Contains(errMsg, "unavailable") ||
			strings.Contains(errMsg, "rpc") || strings.Contains(errMsg, "backend") {
			return true
		}
		// State-related begin failures (e.g., "transaction already active") are non-retryable
		return false
	}

	// Rollback failures are non-retryable (system state may be inconsistent)
	// These are handled by default case (non-retryable)

	// Simple "sinker error" without context (usually fatal, occurs before transaction starts)
	// is non-retryable. Only retryable if it has more context indicating transient issues.
	if strings.Contains(errMsg, "sinker") {
		// If it's just "sinker error" without more context, it's likely fatal
		// But if it has context like "circuit breaker", it might be retryable
		if strings.Contains(errMsg, "circuit") || strings.Contains(errMsg, "timeout") {
			return true
		}
		// Default: simple sinker error is non-retryable (fatal)
		return false
	}

	// Default: non-retryable for unknown errors
	return false
}

// classifyErrorType classifies error into categories for different retry strategies
func (s *TableChangeStream) classifyErrorType(err error) string {
	if err == nil {
		return ""
	}

	errMsg := err.Error()

	// Network/system errors - fast retry with shorter backoff
	if moerr.IsMoErrCode(err, moerr.ErrRPCTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrNoAvailableBackend) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendCannotConnect) ||
		moerr.IsMoErrCode(err, moerr.ErrTNShardNotFound) ||
		moerr.IsMoErrCode(err, moerr.ErrRpcError) ||
		moerr.IsMoErrCode(err, moerr.ErrClientClosed) ||
		moerr.IsMoErrCode(err, moerr.ErrBackendClosed) ||
		errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(errMsg, "connection") ||
		strings.Contains(errMsg, "timeout") ||
		strings.Contains(errMsg, "network") ||
		strings.Contains(errMsg, "unavailable") ||
		strings.Contains(errMsg, "rpc") ||
		strings.Contains(errMsg, "backend") {
		return "network"
	}

	// Commit errors - medium retry with moderate backoff
	if strings.Contains(errMsg, "commit") {
		return "commit"
	}

	// StaleRead errors - fast retry (usually resolves quickly)
	if moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
		return "stale_read"
	}

	// Table relation errors - slow retry (may need table recreation)
	if strings.Contains(errMsg, "relation") || strings.Contains(errMsg, "truncated") {
		return "table_relation"
	}

	// Watermark stall errors - slow retry (may need time for data availability)
	if strings.Contains(errMsg, "stuck") || strings.Contains(errMsg, "stall") {
		return "watermark_stall"
	}

	// Sinker errors with context - medium retry
	if strings.Contains(errMsg, "sinker") {
		if strings.Contains(errMsg, "circuit") || strings.Contains(errMsg, "timeout") {
			return "sinker_transient"
		}
		return "sinker"
	}

	// Default category
	return "unknown"
}

// calculateBackoffDelay calculates exponential backoff delay based on error type and retry count
// Different error types use different backoff strategies
func (s *TableChangeStream) calculateBackoffDelay(errType string, retryCount int) time.Duration {
	base := s.retryBackoffBase
	max := s.retryBackoffMax
	factor := s.retryBackoffFactor

	// Adjust base delay based on error type
	switch errType {
	case "network":
		// Network errors: fast retry (shorter base delay)
		base = base / 2 // 100ms default
		max = max / 2   // 15s default
	case "stale_read":
		// StaleRead errors: very fast retry (usually resolves quickly)
		base = base / 4 // 50ms default
		max = max / 4   // 7.5s default
	case "commit":
		// Commit errors: medium retry (standard backoff)
		// Use default values
	case "table_relation", "watermark_stall":
		// Table/watermark errors: slow retry (longer backoff, may need time)
		base = base * 2 // 400ms default
		max = max * 2   // 60s default
	case "sinker_transient":
		// Sinker transient errors: medium retry
		// Use default values
	default:
		// Unknown errors: standard backoff
		// Use default values
	}

	// Calculate exponential backoff: base * (factor ^ (retryCount - 1))
	delay := float64(base) * math.Pow(factor, float64(retryCount-1))
	result := time.Duration(delay)

	// Cap at max
	if result > max {
		result = max
	}

	return result
}

// clearErrorOnFirstSuccess clears error message on first successful data processing
// This preserves lazy batch processing design (asynchronous, eventual consistency)
func (s *TableChangeStream) clearErrorOnFirstSuccess(ctx context.Context) {
	if !s.hasSucceeded.CompareAndSwap(false, true) {
		return
	}
	// Clear error asynchronously (preserves lazy batch processing design)
	if err := s.watermarkUpdater.UpdateWatermarkErrMsg(ctx, s.watermarkKey, "", nil); err != nil {
		s.hasSucceeded.Store(false)
		logutil.Warn(
			"cdc.table_stream.clear_error_failed",
			zap.String("table", s.tableInfo.String()),
			zap.Error(err),
		)
		// Don't fail the operation if error clearing fails
	}
}

// processWithTxn processes changes within a transaction
func (s *TableChangeStream) processWithTxn(
	ctx context.Context,
	txnOp client.TxnOperator,
	packer *types.Packer,
	ar *ActiveRoutine,
) (err error) {
	// Get relation
	s.progressTracker.SetState("reading")
	_, _, rel, err := GetRelationById(ctx, s.cnEngine, txnOp, s.tableInfo.SourceTblId)
	if err != nil {
		// Table may have been truncated - retryable will be determined in processOneRound
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
			"cdc.table_stream.reached_end_ts",
			zap.String("table", s.tableInfo.String()),
			zap.String("from-ts", fromTs.ToString()),
			zap.String("end-ts", s.endTs.ToString()),
		)
		// Clear error on first success (lazy, eventual consistency)
		s.clearErrorOnFirstSuccess(ctx)
		s.progressTracker.EndRound(true, nil)
		return nil // Graceful end
	}

	toTs := types.TimestampToTS(GetSnapshotTS(txnOp))
	tsCapped := false
	if !s.endTs.IsEmpty() && toTs.GT(&s.endTs) {
		toTs = s.endTs
		tsCapped = true
	}

	// Consolidated debug log for time range (avoid excessive INFO logging in hot path)
	logutil.Debug(
		"cdc.table_stream.time_range",
		zap.String("table", s.tableInfo.String()),
		zap.String("from-ts", fromTs.ToString()),
		zap.String("to-ts", toTs.ToString()),
		zap.String("end-ts", s.endTs.ToString()),
		zap.Bool("ts-capped", tsCapped),
	)

	if !toTs.GT(&fromTs) {
		logutil.Info(
			"cdc.table_stream.no_progress_detected",
			zap.String("table", s.tableInfo.String()),
			zap.String("from-ts", fromTs.ToString()),
			zap.String("to-ts", toTs.ToString()),
			zap.Bool("to-ts-gt-from-ts", toTs.GT(&fromTs)),
		)
		return s.handleSnapshotNoProgress(ctx, fromTs, toTs)
	}

	s.resetWatermarkStallState()

	// Start tracking this round
	s.progressTracker.StartRound(fromTs, toTs)
	s.progressTracker.SetTargetWatermark(toTs)

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
		s.progressTracker.EndRound(false, err)
		return err
	}

	logutil.Debug(
		"cdc.table_stream.collect_changes",
		zap.String("table", s.tableInfo.String()),
		zap.String("from-ts", fromTs.ToString()),
		zap.String("to-ts", toTs.ToString()),
		zap.Duration("duration", time.Since(start)),
	)

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
					"cdc.table_stream.ensure_cleanup_failed",
					zap.String("table", s.tableInfo.String()),
					zap.Error(cleanupErr),
				)
				// Rollback failure makes the error non-retryable (system state may be inconsistent)
				// Store it for processOneRound to return
				if strings.Contains(cleanupErr.Error(), "rollback") {
					s.stateMu.Lock()
					s.cleanupRollbackErr = cleanupErr
					s.stateMu.Unlock()
				}
			}
		}
	}()

	// Process changes
	s.progressTracker.SetState("processing")
	batchCount := uint64(0)

	for {
		select {
		case <-ctx.Done():
			s.progressTracker.EndRound(false, ctx.Err())
			return ctx.Err()
		case <-ar.Pause:
			// Get current watermark before pause
			currentWatermark, _ := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
			logutil.Info(
				"cdc.table_stream.pause_signal_received",
				zap.String("task-id", s.taskId),
				zap.String("table", s.tableInfo.String()),
				zap.String("current-watermark", currentWatermark.ToString()),
				zap.String("from-ts", fromTs.ToString()),
				zap.String("to-ts", toTs.ToString()),
			)
			err := moerr.NewInternalErrorf(ctx, "paused")
			s.progressTracker.EndRound(false, err)
			return err
		case <-ar.Cancel:
			// Get current watermark before cancel
			currentWatermark, _ := s.watermarkUpdater.GetFromCache(ctx, s.watermarkKey)
			logutil.Info(
				"cdc.table_stream.cancel_signal_received",
				zap.String("task-id", s.taskId),
				zap.String("table", s.tableInfo.String()),
				zap.String("current-watermark", currentWatermark.ToString()),
				zap.String("from-ts", fromTs.ToString()),
				zap.String("to-ts", toTs.ToString()),
			)
			err := moerr.NewInternalErrorf(ctx, "cancelled")
			s.progressTracker.EndRound(false, err)
			return err
		default:
		}

		// Update memory pool metrics
		v2.CdcMpoolInUseBytesGauge.Set(float64(s.mp.Stats().NumCurrBytes.Load()))

		// Get next change
		start = time.Now()
		changeData, err := collector.Next(ctx)
		v2.CdcReadDurationHistogram.Observe(time.Since(start).Seconds())
		if err != nil {
			s.progressTracker.EndRound(false, err)
			return err
		}

		// FIX: Check pause before processing NoMoreData
		// NoMoreData triggers commit which updates watermark
		// If pause signal arrived after last select but before commit,
		// we must catch it here to prevent watermark update during pause
		// This fixes the race condition that caused 1144 rows loss in production
		if changeData.Type == ChangeTypeNoMoreData {
			select {
			case <-ar.Pause:
				logutil.Info(
					"cdc.table_stream.pause_before_final_commit",
					zap.String("task-id", s.taskId),
					zap.String("table", s.tableInfo.String()),
					zap.String("from-ts", fromTs.ToString()),
					zap.String("to-ts", toTs.ToString()),
				)
				s.progressTracker.EndRound(false, moerr.NewInternalErrorf(ctx, "paused"))
				return moerr.NewInternalErrorf(ctx, "paused before final commit")
			case <-ar.Cancel:
				logutil.Info(
					"cdc.table_stream.cancel_before_final_commit",
					zap.String("task-id", s.taskId),
					zap.String("table", s.tableInfo.String()),
					zap.String("from-ts", fromTs.ToString()),
					zap.String("to-ts", toTs.ToString()),
				)
				s.progressTracker.EndRound(false, moerr.NewInternalErrorf(ctx, "cancelled"))
				return moerr.NewInternalErrorf(ctx, "cancelled before final commit")
			default:
			}
		}

		// Process change
		if err = s.dataProcessor.ProcessChange(ctx, changeData); err != nil {
			s.progressTracker.EndRound(false, err)
			return err
		}

		// Track batch processing
		if changeData.Type != ChangeTypeNoMoreData {
			batchCount++
			var rows uint64
			if changeData.InsertBatch != nil {
				rows = uint64(changeData.InsertBatch.RowCount())
			}
			if changeData.DeleteBatch != nil {
				rows += uint64(changeData.DeleteBatch.RowCount())
			}

			// Record batch with estimated size
			s.progressTracker.RecordBatch(rows, rows*100) // Rough estimate: 100 bytes per row

			if batchCount%10 == 0 {
				logutil.Info(
					"cdc.table_stream.processing_progress",
					zap.String("table", s.tableInfo.String()),
					zap.Uint64("batches-processed", batchCount),
					zap.Uint64("total-rows", s.progressTracker.totalRowsProcessed.Load()),
					zap.String("from-ts", fromTs.ToString()),
					zap.String("to-ts", toTs.ToString()),
				)
			}
		}

		// If no more data, we're done
		if changeData.Type == ChangeTypeNoMoreData {
			// Clear error on first success (lazy, eventual consistency)
			s.clearErrorOnFirstSuccess(ctx)

			// Mark successful round completion
			s.progressTracker.EndRound(true, nil)
			s.progressTracker.UpdateWatermark(toTs)
			s.progressTracker.RecordTransaction()
			s.onWatermarkAdvanced()

			logutil.Debug(
				"cdc.table_stream.round_complete",
				zap.String("task-id", s.taskId),
				zap.String("table", s.tableInfo.String()),
				zap.Uint64("batches-processed", batchCount),
				zap.Uint64("round-rows", s.progressTracker.currentRoundRows.Load()),
				zap.String("from-ts", fromTs.ToString()),
				zap.String("to-ts", toTs.ToString()),
			)
			if s.watermarkUpdater.IsCircuitBreakerOpen(s.watermarkKey) {
				failures := s.watermarkUpdater.GetCommitFailureCount(s.watermarkKey)
				logutil.Warn(
					"cdc.table_stream.watermark_circuit_open",
					zap.String("table", s.tableInfo.String()),
					zap.Uint32("failure-count", failures),
				)
			}

			return nil
		}
	}
}

func (s *TableChangeStream) handleSnapshotNoProgress(ctx context.Context, fromTs, snapshotTs types.TS) error {
	now := time.Now()
	tableLabel := s.progressTracker.tableKey()

	s.progressTracker.RecordRetry()
	v2.CdcTableNoProgressCounter.WithLabelValues(tableLabel).Inc()
	v2.CdcTableLastActivityTimestamp.WithLabelValues(tableLabel).Set(float64(now.Unix()))

	if s.noProgressSince.IsZero() {
		s.noProgressSince = now
	}

	stalledFor := now.Sub(s.noProgressSince)
	v2.CdcTableStuckGauge.WithLabelValues(tableLabel).Set(1)

	if s.lastNoProgressWarning.IsZero() ||
		now.Sub(s.lastNoProgressWarning) >= s.noProgressWarningInterval {
		logutil.Warn(
			"cdc.table_stream.snapshot_not_advanced",
			zap.String("table", s.tableInfo.String()),
			zap.String("from-ts", fromTs.ToString()),
			zap.String("snapshot-ts", snapshotTs.ToString()),
			zap.Duration("stall-duration", stalledFor),
			zap.Duration("threshold", s.watermarkStallThreshold),
		)
		s.lastNoProgressWarning = now
	}

	if stalledFor >= s.watermarkStallThreshold {
		err := moerr.NewInternalErrorf(
			ctx,
			"CDC tableChangeStream %s snapshot timestamp stuck for %v (threshold %v)",
			s.tableInfo.String(),
			stalledFor,
			s.watermarkStallThreshold,
		)
		// Update error state atomically (for both direct calls and through processOneRound)
		s.updateErrorState(err)
		return err
	}

	return nil
}

func (s *TableChangeStream) resetWatermarkStallState() {
	if s.noProgressSince.IsZero() && s.lastNoProgressWarning.IsZero() {
		return
	}

	s.noProgressSince = time.Time{}
	s.lastNoProgressWarning = time.Time{}
	v2.CdcTableStuckGauge.WithLabelValues(s.progressTracker.tableKey()).Set(0)
}

func (s *TableChangeStream) onWatermarkAdvanced() {
	now := time.Now()
	s.lastWatermarkAdvance = now
	s.resetWatermarkStallState()
	v2.CdcTableLastActivityTimestamp.WithLabelValues(
		s.progressTracker.tableKey(),
	).Set(float64(now.Unix()))
}

// handleStaleRead handles StaleRead error by resetting watermark
// Returns error with retryable flag determined by recoverability
func (s *TableChangeStream) handleStaleRead(ctx context.Context, txnOp client.TxnOperator) error {
	// If startTs is set and noFull is false, StaleRead is fatal (non-retryable)
	if !s.noFull && !s.startTs.IsEmpty() {
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
			"cdc.table_stream.update_watermark_only_failed",
			zap.String("table", s.tableInfo.String()),
			zap.String("watermark", watermark.ToString()),
			zap.Error(err),
		)
		// Recovery failed - non-retryable
		return moerr.NewInternalErrorf(
			ctx,
			"CDC tableChangeStream %s stale read recovery failed to update watermark: %v",
			s.tableInfo.String(),
			err,
		)
	}

	logutil.Info(
		"cdc.table_stream.reset_watermark_on_stale_read",
		zap.String("table", s.tableInfo.String()),
		zap.String("watermark", watermark.ToString()),
	)

	// Force next interval
	s.forceNextInterval(DefaultFrequency)

	// Recovery succeeded - return nil to swallow error
	// retryable will be set to true in processOneRound
	return nil
}

// GetRetryable returns the retryable flag in a thread-safe way
func (s *TableChangeStream) GetRetryable() bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	return s.retryable
}

// Close closes the change stream
func (s *TableChangeStream) Close() {
	s.cancelRun()
	s.sinker.Close()
}

// Info returns the table information
// Wait blocks until the reader goroutine completes
func (s *TableChangeStream) Wait() {
	s.start.Wait()
	s.wg.Wait()
}

// GetTableInfo returns the source table information
func (s *TableChangeStream) GetTableInfo() *DbTableInfo {
	return s.tableInfo
}

func (s *TableChangeStream) cancelRun() {
	if s.runCancel == nil {
		return
	}
	s.cancelOnce.Do(func() {
		s.runCancel()
	})
}
