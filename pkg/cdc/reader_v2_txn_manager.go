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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// TransactionManager manages the transaction lifecycle
// Key responsibilities:
// 1. Track transaction state using TransactionTracker
// 2. Interact with Sinker (SendBegin/Commit/Rollback)
// 3. Interact with WatermarkUpdater (update watermark)
// 4. Implement dual-layer safety (tracker + watermark)
type TransactionManager struct {
	sinker           Sinker
	watermarkUpdater WatermarkUpdater
	watermarkKey     *WatermarkKey

	// Current transaction tracker
	tracker *TransactionTracker

	// Logging context
	accountId uint64
	taskId    string
	dbName    string
	tableName string
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(
	sinker Sinker,
	watermarkUpdater WatermarkUpdater,
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
) *TransactionManager {
	return &TransactionManager{
		sinker:           sinker,
		watermarkUpdater: watermarkUpdater,
		watermarkKey: &WatermarkKey{
			AccountId: accountId,
			TaskId:    taskId,
			DBName:    dbName,
			TableName: tableName,
		},
		accountId: accountId,
		taskId:    taskId,
		dbName:    dbName,
		tableName: tableName,
	}
}

// BeginTransaction starts a new transaction
// This should be called when we have data to send
func (tm *TransactionManager) BeginTransaction(ctx context.Context, fromTs, toTs types.TS) error {
	if tm.tracker != nil && tm.tracker.NeedsRollback() {
		logutil.Warn(
			"CDC-TransactionManager-BeginWithUnfinished",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		// Rollback previous transaction first
		if err := tm.RollbackTransaction(ctx); err != nil {
			return err
		}
	}

	// Create new tracker
	tm.tracker = NewTransactionTracker(fromTs, toTs)

	// Send BEGIN to sinker
	tm.sinker.SendBegin()

	// Check for errors
	if err := tm.sinker.Error(); err != nil {
		logutil.Error(
			"CDC-TransactionManager-SendBeginFailed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		return err
	}

	// Mark as begun
	tm.tracker.MarkBegin()

	logutil.Debug(
		"CDC-TransactionManager-BeginSuccess",
		zap.String("task-id", tm.taskId),
		zap.Uint64("account-id", tm.accountId),
		zap.String("db", tm.dbName),
		zap.String("table", tm.tableName),
		zap.String("from-ts", fromTs.ToString()),
		zap.String("to-ts", toTs.ToString()),
	)

	return nil
}

// CommitTransaction commits the current transaction
// Key steps (ORDER MATTERS):
// 1. Send COMMIT to sinker
// 2. Update watermark (persistent proof)
// 3. Mark tracker as committed (memory state)
func (tm *TransactionManager) CommitTransaction(ctx context.Context) error {
	if tm.tracker == nil {
		logutil.Warn(
			"CDC-TransactionManager-CommitWithoutTracker",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if !tm.tracker.hasBegin {
		logutil.Warn(
			"CDC-TransactionManager-CommitWithoutBegin",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	toTs := tm.tracker.GetToTs()

	logutil.Debug(
		"CDC-TransactionManager-CommitTransaction-Start",
		zap.String("task-id", tm.taskId),
		zap.String("db", tm.dbName),
		zap.String("table", tm.tableName),
		zap.String("from-ts", tm.tracker.GetFromTs().ToString()),
		zap.String("to-ts", toTs.ToString()),
	)

	// Step 1: Send COMMIT to sinker
	tm.sinker.SendCommit()
	// Send dummy to ensure COMMIT is sent
	tm.sinker.SendDummy()

	// Check for errors
	if err := tm.sinker.Error(); err != nil {
		logutil.Error(
			"CDC-TransactionManager-SendCommitFailed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.String("to-ts", toTs.ToString()),
			zap.Error(err),
		)
		return err
	}

	// Step 2: Update watermark (persistent proof of success)
	// This MUST happen BEFORE marking tracker as committed
	if err := tm.watermarkUpdater.UpdateWatermarkOnly(
		ctx,
		tm.watermarkKey,
		&toTs,
	); err != nil {
		logutil.Error(
			"CDC-TransactionManager-UpdateWatermarkFailed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.String("to-ts", toTs.ToString()),
			zap.Error(err),
		)
		// Note: UpdateWatermarkOnly always returns nil (eventual consistency)
		// But we log it anyway for monitoring
	}

	// Step 3: Mark tracker as committed (memory state sync)
	tm.tracker.MarkCommit()
	tm.tracker.MarkWatermarkUpdated()

	logutil.Debug(
		"CDC-TransactionManager-CommitSuccess",
		zap.String("task-id", tm.taskId),
		zap.Uint64("account-id", tm.accountId),
		zap.String("db", tm.dbName),
		zap.String("table", tm.tableName),
		zap.String("to-ts", toTs.ToString()),
	)

	return nil
}

// RollbackTransaction rolls back the current transaction
func (tm *TransactionManager) RollbackTransaction(ctx context.Context) error {
	if tm.tracker == nil {
		logutil.Warn(
			"CDC-TransactionManager-RollbackWithoutTracker",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if !tm.tracker.hasBegin {
		logutil.Debug(
			"CDC-TransactionManager-RollbackWithoutBegin",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if tm.tracker.hasRolledBack {
		logutil.Debug(
			"CDC-TransactionManager-AlreadyRolledBack",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	// Clear any previous errors before rollback
	tm.sinker.ClearError()

	// Send ROLLBACK to sinker
	tm.sinker.SendRollback()
	// Send dummy to ensure ROLLBACK is sent
	tm.sinker.SendDummy()

	// Check for errors
	if err := tm.sinker.Error(); err != nil {
		logutil.Error(
			"CDC-TransactionManager-SendRollbackFailed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		// Mark as rolled back even if it failed
		// to avoid infinite retry loops
		tm.tracker.MarkRollback()
		return err
	}

	// Mark tracker as rolled back
	tm.tracker.MarkRollback()

	logutil.Debug(
		"CDC-TransactionManager-RollbackSuccess",
		zap.String("task-id", tm.taskId),
		zap.Uint64("account-id", tm.accountId),
		zap.String("db", tm.dbName),
		zap.String("table", tm.tableName),
	)

	return nil
}

// EnsureCleanup ensures proper transaction cleanup
// This implements the dual-layer safety check:
// Layer 1: Check tracker state (fast, explicit)
// Layer 2: Verify watermark (reliable, persistent)
func (tm *TransactionManager) EnsureCleanup(ctx context.Context) error {
	if tm.tracker == nil {
		return nil
	}

	// Layer 1: Check explicit transaction state
	if tm.tracker.NeedsRollback() {
		logutil.Warn(
			"CDC-TransactionManager-EnsureCleanup-TrackerNeedsRollback",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return tm.RollbackTransaction(ctx)
	}

	// Layer 2: Verify watermark (dual-layer safety)
	toTs := tm.tracker.GetToTs()
	current, err := tm.watermarkUpdater.GetFromCache(ctx, tm.watermarkKey)

	if err != nil {
		// Even if GetFromCache fails, use tracker state
		logutil.Warn(
			"CDC-TransactionManager-EnsureCleanup-GetFromCacheFailed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		// Fallback to tracker state
		if tm.tracker.hasBegin && !tm.tracker.hasCommitted {
			return tm.RollbackTransaction(ctx)
		}
		return nil
	}

	// Final guard: Even if tracker says committed, but watermark not updated
	if !current.Equal(&toTs) && tm.tracker.hasBegin {
		logutil.Error(
			"CDC-TransactionManager-EnsureCleanup-WatermarkMismatch",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.String("expected", toTs.ToString()),
			zap.String("actual", current.ToString()),
		)
		return tm.RollbackTransaction(ctx)
	}

	return nil
}

// GetTracker returns the current transaction tracker
func (tm *TransactionManager) GetTracker() *TransactionTracker {
	return tm.tracker
}

// Reset resets the transaction manager for a new transaction
func (tm *TransactionManager) Reset() {
	tm.tracker = nil
}
