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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

const (
	initialSyncStateNotStarted uint32 = iota
	initialSyncStateRunning
	initialSyncStateSuccess
	initialSyncStateFailed
)

const (
	initialSyncStatusNotStartedValue = 0
	initialSyncStatusRunningValue    = 1
	initialSyncStatusSuccessValue    = 2
	initialSyncStatusFailedValue     = 3
)

// ProgressTracker tracks the progress of CDC processing for a single table
// This provides detailed observability into what's happening at each stage
type ProgressTracker struct {
	// Table identity
	accountId uint64
	taskId    string
	dbName    string
	tableName string

	// Watermark state
	currentWatermark     types.TS
	targetWatermark      types.TS
	lastWatermarkUpdate  time.Time
	watermarkUpdateCount atomic.Uint64

	// Processing state
	state            string // "idle", "reading", "processing", "committing", "error"
	lastStateChange  time.Time
	stateChangeCount atomic.Uint64

	// Progress counters
	totalRounds           atomic.Uint64 // Total number of processing rounds
	successfulRounds      atomic.Uint64 // Number of successful rounds
	failedRounds          atomic.Uint64 // Number of failed rounds
	totalRowsProcessed    atomic.Uint64 // Total rows processed
	totalBytesProcessed   atomic.Uint64 // Total bytes processed
	totalBatchesProcessed atomic.Uint64 // Total batches processed
	totalTransactions     atomic.Uint64 // Total transactions committed
	totalRetries          atomic.Uint64 // Total retry attempts
	totalSQLExecuted      atomic.Uint64 // Total SQL statements executed

	// Timing metrics
	lastRoundStartTime  time.Time
	lastRoundDuration   time.Duration
	avgRoundDuration    time.Duration
	maxRoundDuration    time.Duration
	totalProcessingTime time.Duration

	// Current round details
	currentRoundStartTime time.Time
	currentRoundFromTs    types.TS
	currentRoundToTs      types.TS
	currentRoundRows      atomic.Uint64
	currentRoundBatches   atomic.Uint64

	// Error tracking
	lastError     error
	lastErrorTime time.Time
	errorCount    atomic.Uint64

	// Initial sync tracking
	initialSyncState   atomic.Uint32
	initialSyncStart   time.Time
	initialSyncEnd     time.Time
	initialSyncRows    atomic.Uint64
	initialSyncBytes   atomic.Uint64
	initialSyncBatches atomic.Uint64
	initialSyncSQL     atomic.Uint64
	initialSyncFromTs  types.TS
	initialSyncToTs    types.TS

	// Lock for non-atomic fields
	mu sync.RWMutex

	// Last log time (to control log frequency)
	lastLogTime time.Time
	logInterval time.Duration
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(accountId uint64, taskId, dbName, tableName string) *ProgressTracker {
	pt := &ProgressTracker{
		accountId:       accountId,
		taskId:          taskId,
		dbName:          dbName,
		tableName:       tableName,
		state:           "idle",
		lastStateChange: time.Now(),
		logInterval:     30 * time.Second, // Log progress every 30 seconds by default
	}
	pt.initialSyncState.Store(initialSyncStateNotStarted)
	v2.CdcInitialSyncStatusGauge.WithLabelValues(pt.tableKey()).Set(initialSyncStatusNotStartedValue)
	return pt
}

// SetState updates the current state
func (pt *ProgressTracker) SetState(state string) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.state != state {
		pt.state = state
		pt.lastStateChange = time.Now()
		pt.stateChangeCount.Add(1)

		logutil.Debug(
			"cdc.progress_tracker.state_change",
			zap.String("table", pt.tableKey()),
			zap.String("new-state", state),
			zap.Uint64("state-changes", pt.stateChangeCount.Load()),
		)
	}
}

// GetState returns the current state and how long it has been in this state
func (pt *ProgressTracker) GetState() (state string, duration time.Duration) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()
	return pt.state, time.Since(pt.lastStateChange)
}

// StartRound marks the start of a new processing round
func (pt *ProgressTracker) StartRound(fromTs, toTs types.TS) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.ensureInitialSyncLocked(fromTs, toTs)
	if pt.initialSyncState.Load() == initialSyncStateRunning {
		if pt.initialSyncFromTs.IsEmpty() {
			pt.initialSyncFromTs = fromTs
		}
		pt.initialSyncToTs = toTs
	}

	pt.currentRoundStartTime = time.Now()
	pt.currentRoundFromTs = fromTs
	pt.currentRoundToTs = toTs
	pt.currentRoundRows.Store(0)
	pt.currentRoundBatches.Store(0)
	pt.totalRounds.Add(1)

	logutil.Debug(
		"cdc.progress_tracker.round_start",
		zap.String("table", pt.tableKey()),
		zap.String("from-ts", fromTs.ToString()),
		zap.String("to-ts", toTs.ToString()),
		zap.Uint64("round", pt.totalRounds.Load()),
		zap.String("state", pt.state),
	)
}

func (pt *ProgressTracker) ensureInitialSyncLocked(fromTs, toTs types.TS) {
	state := pt.initialSyncState.Load()
	if state == initialSyncStateRunning || state == initialSyncStateSuccess {
		return
	}

	pt.initialSyncState.Store(initialSyncStateRunning)
	pt.initialSyncStart = time.Now()
	pt.initialSyncEnd = time.Time{}
	pt.initialSyncRows.Store(0)
	pt.initialSyncBytes.Store(0)
	pt.initialSyncBatches.Store(0)
	pt.initialSyncSQL.Store(0)
	pt.initialSyncFromTs = fromTs
	pt.initialSyncToTs = toTs

	tableLabel := pt.tableKey()
	v2.CdcInitialSyncStatusGauge.WithLabelValues(tableLabel).Set(initialSyncStatusRunningValue)
	v2.CdcInitialSyncStartTimestamp.WithLabelValues(tableLabel).Set(float64(pt.initialSyncStart.Unix()))
	v2.CdcInitialSyncEndTimestamp.WithLabelValues(tableLabel).Set(0)
	v2.CdcInitialSyncRowsGauge.WithLabelValues(tableLabel).Set(0)
	v2.CdcInitialSyncBytesGauge.WithLabelValues(tableLabel).Set(0)
	v2.CdcInitialSyncSQLGauge.WithLabelValues(tableLabel).Set(0)

	logutil.Info(
		"cdc.progress_tracker.initial_sync_start",
		zap.String("table", tableLabel),
		zap.String("from-ts", fromTs.ToString()),
		zap.String("to-ts", toTs.ToString()),
		zap.Bool("restart", state == initialSyncStateFailed),
	)
}

// EndRound marks the end of a processing round
func (pt *ProgressTracker) EndRound(success bool, err error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.currentRoundStartTime.IsZero() {
		return // No active round
	}

	duration := time.Since(pt.currentRoundStartTime)
	pt.lastRoundDuration = duration
	pt.lastRoundStartTime = pt.currentRoundStartTime
	pt.totalProcessingTime += duration

	// Update average and max duration
	rounds := pt.totalRounds.Load()
	if rounds > 0 {
		pt.avgRoundDuration = pt.totalProcessingTime / time.Duration(rounds)
	}
	if duration > pt.maxRoundDuration {
		pt.maxRoundDuration = duration
	}

	if success {
		pt.successfulRounds.Add(1)
	} else {
		pt.failedRounds.Add(1)
		pt.lastError = err
		pt.lastErrorTime = time.Now()
		pt.errorCount.Add(1)
	}

	currentRoundRows := pt.currentRoundRows.Load()
	currentRoundBatches := pt.currentRoundBatches.Load()
	tableLabel := pt.tableKey()

	logutil.Debug(
		"cdc.progress_tracker.round_end",
		zap.String("table", pt.tableKey()),
		zap.Bool("success", success),
		zap.Duration("duration", duration),
		zap.Uint64("rows", currentRoundRows),
		zap.Uint64("batches", currentRoundBatches),
		zap.String("from-ts", pt.currentRoundFromTs.ToString()),
		zap.String("to-ts", pt.currentRoundToTs.ToString()),
		zap.Uint64("total-rounds", rounds),
		zap.Uint64("successful-rounds", pt.successfulRounds.Load()),
		zap.Uint64("failed-rounds", pt.failedRounds.Load()),
		zap.Error(err),
	)

	if success {
		if duration > 0 {
			throughput := float64(currentRoundRows) / duration.Seconds()
			v2.CdcThroughputRowsPerSecond.WithLabelValues(tableLabel).Set(throughput)
		} else {
			v2.CdcThroughputRowsPerSecond.WithLabelValues(tableLabel).Set(0)
		}
		if !pt.currentRoundToTs.IsEmpty() {
			ts := pt.currentRoundToTs.ToTimestamp()
			if !ts.IsEmpty() {
				lag := time.Since(ts.ToStdTime()).Seconds()
				if lag < 0 {
					lag = 0
				}
				v2.CdcWatermarkLagSeconds.WithLabelValues(tableLabel).Set(lag)
			}
		}
		v2.CdcTableLastActivityTimestamp.WithLabelValues(tableLabel).Set(float64(time.Now().Unix()))
	}

	pt.handleInitialSyncRoundLocked(success, err)

	// Reset current round
	pt.currentRoundStartTime = time.Time{}
}

func (pt *ProgressTracker) handleInitialSyncRoundLocked(success bool, err error) {
	if pt.initialSyncState.Load() != initialSyncStateRunning {
		return
	}

	tableLabel := pt.tableKey()
	pt.initialSyncEnd = time.Now()
	rows := pt.initialSyncRows.Load()
	bytes := pt.initialSyncBytes.Load()
	batches := pt.initialSyncBatches.Load()
	sqlCount := pt.initialSyncSQL.Load()
	duration := pt.initialSyncEnd.Sub(pt.initialSyncStart)
	if duration > 0 {
		v2.CdcInitialSyncDurationHistogram.WithLabelValues(tableLabel).Observe(duration.Seconds())
	}
	v2.CdcInitialSyncRowsGauge.WithLabelValues(tableLabel).Set(float64(rows))
	v2.CdcInitialSyncBytesGauge.WithLabelValues(tableLabel).Set(float64(bytes))
	v2.CdcInitialSyncSQLGauge.WithLabelValues(tableLabel).Set(float64(sqlCount))

	if success {
		pt.initialSyncState.Store(initialSyncStateSuccess)
		v2.CdcInitialSyncStatusGauge.WithLabelValues(tableLabel).Set(initialSyncStatusSuccessValue)
		v2.CdcInitialSyncEndTimestamp.WithLabelValues(tableLabel).Set(float64(pt.initialSyncEnd.Unix()))

		logutil.Info(
			"cdc.progress_tracker.initial_sync_complete",
			zap.String("table", tableLabel),
			zap.Duration("duration", duration),
			zap.Uint64("rows", rows),
			zap.Uint64("batches", batches),
			zap.Uint64("sql-count", sqlCount),
			zap.Uint64("bytes-estimate", bytes),
			zap.String("initial-from-ts", pt.initialSyncFromTs.ToString()),
			zap.String("initial-to-ts", pt.initialSyncToTs.ToString()),
		)
		return
	}

	pt.initialSyncState.Store(initialSyncStateFailed)
	v2.CdcInitialSyncStatusGauge.WithLabelValues(tableLabel).Set(initialSyncStatusFailedValue)
	v2.CdcInitialSyncEndTimestamp.WithLabelValues(tableLabel).Set(float64(pt.initialSyncEnd.Unix()))

	logutil.Warn(
		"cdc.progress_tracker.initial_sync_failed",
		zap.String("table", tableLabel),
		zap.Error(err),
		zap.Uint64("rows", rows),
		zap.Uint64("batches", batches),
		zap.Uint64("sql-count", sqlCount),
		zap.Uint64("bytes-estimate", bytes),
		zap.String("initial-from-ts", pt.initialSyncFromTs.ToString()),
		zap.String("initial-to-ts", pt.initialSyncToTs.ToString()),
	)
}

// RecordBatch records processing of a batch
func (pt *ProgressTracker) RecordBatch(rows, bytes uint64) {
	pt.currentRoundRows.Add(rows)
	pt.currentRoundBatches.Add(1)
	pt.totalRowsProcessed.Add(rows)
	pt.totalBytesProcessed.Add(bytes)
	pt.totalBatchesProcessed.Add(1)

	if pt.initialSyncState.Load() == initialSyncStateRunning {
		pt.initialSyncRows.Add(rows)
		pt.initialSyncBytes.Add(bytes)
		pt.initialSyncBatches.Add(1)
	}

	// Log progress periodically
	pt.maybeLogProgress()
}

// RecordTransaction records a committed transaction
func (pt *ProgressTracker) RecordTransaction() {
	pt.totalTransactions.Add(1)
}

// RecordRetry records a retry attempt
func (pt *ProgressTracker) RecordRetry() {
	pt.totalRetries.Add(1)
}

// RecordSQL records execution of SQL statements by the sinker
func (pt *ProgressTracker) RecordSQL(count uint64) {
	pt.totalSQLExecuted.Add(count)
	if pt.initialSyncState.Load() == initialSyncStateRunning {
		pt.initialSyncSQL.Add(count)
	}
}

// UpdateWatermark records a watermark update
func (pt *ProgressTracker) UpdateWatermark(newWatermark types.TS) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	oldWatermark := pt.currentWatermark
	pt.currentWatermark = newWatermark
	pt.lastWatermarkUpdate = time.Now()
	pt.watermarkUpdateCount.Add(1)

	logutil.Debug(
		"cdc.progress_tracker.watermark_update",
		zap.String("table", pt.tableKey()),
		zap.String("old-watermark", oldWatermark.ToString()),
		zap.String("new-watermark", newWatermark.ToString()),
		zap.Uint64("update-count", pt.watermarkUpdateCount.Load()),
		zap.Duration("time-since-last-update", time.Since(pt.lastWatermarkUpdate)),
	)
}

// SetTargetWatermark sets the target watermark
func (pt *ProgressTracker) SetTargetWatermark(target types.TS) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.targetWatermark = target
}

// RecordError records an error
func (pt *ProgressTracker) RecordError(err error) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	pt.lastError = err
	pt.lastErrorTime = time.Now()
	pt.errorCount.Add(1)

	logutil.Error(
		"cdc.progress_tracker.error",
		zap.String("table", pt.tableKey()),
		zap.Error(err),
		zap.Uint64("error-count", pt.errorCount.Load()),
		zap.String("state", pt.state),
	)
}

// maybeLogProgress logs progress if enough time has passed
func (pt *ProgressTracker) maybeLogProgress() {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	if time.Since(pt.lastLogTime) < pt.logInterval {
		return
	}

	pt.lastLogTime = time.Now()
	pt.logProgressLocked()
}

// LogProgress logs the current progress (forced)
func (pt *ProgressTracker) LogProgress() {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.logProgressLocked()
}

// logProgressLocked logs progress (must hold lock)
func (pt *ProgressTracker) logProgressLocked() {
	state, stateAge := pt.state, time.Since(pt.lastStateChange)

	logutil.Info(
		"cdc.progress_tracker.progress",
		zap.String("table", pt.tableKey()),
		zap.String("state", state),
		zap.Duration("state-age", stateAge),
		zap.String("current-watermark", pt.currentWatermark.ToString()),
		zap.String("target-watermark", pt.targetWatermark.ToString()),
		zap.Duration("time-since-last-watermark-update", time.Since(pt.lastWatermarkUpdate)),
		zap.Uint64("watermark-updates", pt.watermarkUpdateCount.Load()),
		zap.Uint64("total-rounds", pt.totalRounds.Load()),
		zap.Uint64("successful-rounds", pt.successfulRounds.Load()),
		zap.Uint64("failed-rounds", pt.failedRounds.Load()),
		zap.Uint64("total-rows", pt.totalRowsProcessed.Load()),
		zap.Uint64("total-batches", pt.totalBatchesProcessed.Load()),
		zap.Uint64("total-txns", pt.totalTransactions.Load()),
		zap.Uint64("total-sql", pt.totalSQLExecuted.Load()),
		zap.Duration("avg-round-duration", pt.avgRoundDuration),
		zap.Duration("max-round-duration", pt.maxRoundDuration),
		zap.Duration("last-round-duration", pt.lastRoundDuration),
		zap.Uint64("current-round-rows", pt.currentRoundRows.Load()),
		zap.Uint64("current-round-batches", pt.currentRoundBatches.Load()),
		zap.Uint64("initial-sync-rows", pt.initialSyncRows.Load()),
		zap.Uint64("initial-sync-batches", pt.initialSyncBatches.Load()),
		zap.Uint64("initial-sync-sql", pt.initialSyncSQL.Load()),
		zap.Uint32("initial-sync-state", pt.initialSyncState.Load()),
	)
}

// GetStats returns current statistics
func (pt *ProgressTracker) GetStats() map[string]interface{} {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	initialStart := pt.initialSyncStart
	initialEnd := pt.initialSyncEnd
	initialDuration := 0.0
	if !initialStart.IsZero() && !initialEnd.IsZero() && initialEnd.After(initialStart) {
		initialDuration = initialEnd.Sub(initialStart).Seconds()
	}
	initialStartUnix := int64(0)
	if !initialStart.IsZero() {
		initialStartUnix = initialStart.Unix()
	}
	initialEndUnix := int64(0)
	if !initialEnd.IsZero() {
		initialEndUnix = initialEnd.Unix()
	}

	return map[string]interface{}{
		"table":                       pt.tableKey(),
		"state":                       pt.state,
		"state_age_seconds":           time.Since(pt.lastStateChange).Seconds(),
		"current_watermark":           pt.currentWatermark.ToString(),
		"target_watermark":            pt.targetWatermark.ToString(),
		"watermark_lag_seconds":       time.Since(pt.lastWatermarkUpdate).Seconds(),
		"watermark_updates":           pt.watermarkUpdateCount.Load(),
		"total_rounds":                pt.totalRounds.Load(),
		"successful_rounds":           pt.successfulRounds.Load(),
		"failed_rounds":               pt.failedRounds.Load(),
		"total_rows_processed":        pt.totalRowsProcessed.Load(),
		"total_bytes_processed":       pt.totalBytesProcessed.Load(),
		"total_batches_processed":     pt.totalBatchesProcessed.Load(),
		"total_transactions":          pt.totalTransactions.Load(),
		"total_retries":               pt.totalRetries.Load(),
		"total_sql_executed":          pt.totalSQLExecuted.Load(),
		"avg_round_duration_seconds":  pt.avgRoundDuration.Seconds(),
		"max_round_duration_seconds":  pt.maxRoundDuration.Seconds(),
		"last_round_duration_seconds": pt.lastRoundDuration.Seconds(),
		"error_count":                 pt.errorCount.Load(),
		"initial_sync_state":          pt.initialSyncState.Load(),
		"initial_sync_rows":           pt.initialSyncRows.Load(),
		"initial_sync_bytes":          pt.initialSyncBytes.Load(),
		"initial_sync_batches":        pt.initialSyncBatches.Load(),
		"initial_sync_sql":            pt.initialSyncSQL.Load(),
		"initial_sync_start_unix":     initialStartUnix,
		"initial_sync_end_unix":       initialEndUnix,
		"initial_sync_duration_sec":   initialDuration,
		"initial_sync_from_ts":        pt.initialSyncFromTs.ToString(),
		"initial_sync_to_ts":          pt.initialSyncToTs.ToString(),
	}
}

// CheckStuck detects if the tracker is stuck (no progress for a long time)
func (pt *ProgressTracker) CheckStuck(stuckThreshold time.Duration) (bool, string) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	stateAge := time.Since(pt.lastStateChange)
	watermarkAge := time.Since(pt.lastWatermarkUpdate)

	// Check if state hasn't changed for too long
	if stateAge > stuckThreshold {
		return true, fmt.Sprintf("state '%s' unchanged for %v", pt.state, stateAge)
	}

	// Check if watermark hasn't updated for too long (and we're not idle)
	if pt.state != "idle" && watermarkAge > stuckThreshold {
		return true, fmt.Sprintf("watermark not updated for %v", watermarkAge)
	}

	// Check if current round is taking too long
	if !pt.currentRoundStartTime.IsZero() {
		roundAge := time.Since(pt.currentRoundStartTime)
		if roundAge > stuckThreshold {
			return true, fmt.Sprintf("current round running for %v", roundAge)
		}
	}

	return false, ""
}

func (pt *ProgressTracker) tableKey() string {
	return fmt.Sprintf("%d.%s.%s.%s", pt.accountId, pt.taskId, pt.dbName, pt.tableName)
}

// ProgressMonitor monitors multiple progress trackers and detects stuck tables
type ProgressMonitor struct {
	trackers map[string]*ProgressTracker
	mu       sync.RWMutex

	// Configuration
	checkInterval  time.Duration
	stuckThreshold time.Duration
	logInterval    time.Duration

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewProgressMonitor creates a new progress monitor
func NewProgressMonitor(checkInterval, stuckThreshold, logInterval time.Duration) *ProgressMonitor {
	ctx, cancel := context.WithCancel(context.Background())
	return &ProgressMonitor{
		trackers:       make(map[string]*ProgressTracker),
		checkInterval:  checkInterval,
		stuckThreshold: stuckThreshold,
		logInterval:    logInterval,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Register registers a progress tracker
func (pm *ProgressMonitor) Register(tracker *ProgressTracker) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := tracker.tableKey()
	pm.trackers[key] = tracker
	tracker.logInterval = pm.logInterval

	logutil.Info(
		"cdc.progress_monitor.register",
		zap.String("table", key),
		zap.Int("total-trackers", len(pm.trackers)),
	)
}

// Unregister unregisters a progress tracker
func (pm *ProgressMonitor) Unregister(tracker *ProgressTracker) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := tracker.tableKey()
	delete(pm.trackers, key)

	logutil.Info(
		"cdc.progress_monitor.unregister",
		zap.String("table", key),
		zap.Int("total-trackers", len(pm.trackers)),
	)
}

// Start starts the monitoring goroutine
func (pm *ProgressMonitor) Start() {
	pm.wg.Add(1)
	go pm.monitorLoop()

	logutil.Info("cdc.progress_monitor.started")
}

// Stop stops the monitoring goroutine
func (pm *ProgressMonitor) Stop() {
	pm.cancel()
	pm.wg.Wait()

	logutil.Info("cdc.progress_monitor.stopped")
}

// monitorLoop is the main monitoring loop
func (pm *ProgressMonitor) monitorLoop() {
	defer pm.wg.Done()

	ticker := time.NewTicker(pm.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.checkAllTrackers()
		}
	}
}

// checkAllTrackers checks all trackers for stuck conditions
func (pm *ProgressMonitor) checkAllTrackers() {
	pm.mu.RLock()
	trackers := make([]*ProgressTracker, 0, len(pm.trackers))
	for _, tracker := range pm.trackers {
		trackers = append(trackers, tracker)
	}
	pm.mu.RUnlock()

	stuckCount := 0
	for _, tracker := range trackers {
		if stuck, reason := tracker.CheckStuck(pm.stuckThreshold); stuck {
			stuckCount++
			state, stateAge := tracker.GetState()

			logutil.Warn(
				"cdc.progress_monitor.stuck_detected",
				zap.String("table", tracker.tableKey()),
				zap.String("reason", reason),
				zap.String("state", state),
				zap.Duration("state-age", stateAge),
			)

			// Force log progress for stuck tracker
			tracker.LogProgress()
		}
	}

	if stuckCount > 0 || len(trackers) > 0 {
		logutil.Info(
			"cdc.progress_monitor.summary",
			zap.Int("total-tables", len(trackers)),
			zap.Int("stuck-tables", stuckCount),
		)
	}
}
