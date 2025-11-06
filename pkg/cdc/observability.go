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
	"go.uber.org/zap"
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

	// Lock for non-atomic fields
	mu sync.RWMutex

	// Last log time (to control log frequency)
	lastLogTime time.Time
	logInterval time.Duration
}

// NewProgressTracker creates a new progress tracker
func NewProgressTracker(accountId uint64, taskId, dbName, tableName string) *ProgressTracker {
	return &ProgressTracker{
		accountId:       accountId,
		taskId:          taskId,
		dbName:          dbName,
		tableName:       tableName,
		state:           "idle",
		lastStateChange: time.Now(),
		logInterval:     30 * time.Second, // Log progress every 30 seconds by default
	}
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
			"CDC-ProgressTracker-StateChange",
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

	pt.currentRoundStartTime = time.Now()
	pt.currentRoundFromTs = fromTs
	pt.currentRoundToTs = toTs
	pt.currentRoundRows.Store(0)
	pt.currentRoundBatches.Store(0)
	pt.totalRounds.Add(1)

	logutil.Debug(
		"CDC-ProgressTracker-RoundStart",
		zap.String("table", pt.tableKey()),
		zap.String("from-ts", fromTs.ToString()),
		zap.String("to-ts", toTs.ToString()),
		zap.Uint64("round", pt.totalRounds.Load()),
		zap.String("state", pt.state),
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

	logutil.Debug(
		"CDC-ProgressTracker-RoundEnd",
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

	// Reset current round
	pt.currentRoundStartTime = time.Time{}
}

// RecordBatch records processing of a batch
func (pt *ProgressTracker) RecordBatch(rows, bytes uint64) {
	pt.currentRoundRows.Add(rows)
	pt.currentRoundBatches.Add(1)
	pt.totalRowsProcessed.Add(rows)
	pt.totalBytesProcessed.Add(bytes)
	pt.totalBatchesProcessed.Add(1)

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

// UpdateWatermark records a watermark update
func (pt *ProgressTracker) UpdateWatermark(newWatermark types.TS) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	oldWatermark := pt.currentWatermark
	pt.currentWatermark = newWatermark
	pt.lastWatermarkUpdate = time.Now()
	pt.watermarkUpdateCount.Add(1)

	logutil.Debug(
		"CDC-ProgressTracker-WatermarkUpdate",
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
		"CDC-ProgressTracker-Error",
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
		"CDC-ProgressTracker-Progress",
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
		zap.Duration("avg-round-duration", pt.avgRoundDuration),
		zap.Duration("max-round-duration", pt.maxRoundDuration),
		zap.Duration("last-round-duration", pt.lastRoundDuration),
		zap.Uint64("current-round-rows", pt.currentRoundRows.Load()),
		zap.Uint64("current-round-batches", pt.currentRoundBatches.Load()),
	)
}

// GetStats returns current statistics
func (pt *ProgressTracker) GetStats() map[string]interface{} {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

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
		"avg_round_duration_seconds":  pt.avgRoundDuration.Seconds(),
		"max_round_duration_seconds":  pt.maxRoundDuration.Seconds(),
		"last_round_duration_seconds": pt.lastRoundDuration.Seconds(),
		"error_count":                 pt.errorCount.Load(),
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
		"CDC-ProgressMonitor-Register",
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
		"CDC-ProgressMonitor-Unregister",
		zap.String("table", key),
		zap.Int("total-trackers", len(pm.trackers)),
	)
}

// Start starts the monitoring goroutine
func (pm *ProgressMonitor) Start() {
	pm.wg.Add(1)
	go pm.monitorLoop()

	logutil.Info("CDC-ProgressMonitor-Started")
}

// Stop stops the monitoring goroutine
func (pm *ProgressMonitor) Stop() {
	pm.cancel()
	pm.wg.Wait()

	logutil.Info("CDC-ProgressMonitor-Stopped")
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
				"CDC-ProgressMonitor-StuckDetected",
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
			"CDC-ProgressMonitor-Summary",
			zap.Int("total-tables", len(trackers)),
			zap.Int("stuck-tables", stuckCount),
		)
	}
}
