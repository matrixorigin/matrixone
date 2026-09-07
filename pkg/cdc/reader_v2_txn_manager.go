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
	"sync"

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
//
// Concurrency & Locking:
//   - All PUBLIC methods on TransactionManager are serialized by an internal mutex.
//     This guarantees safe access and mutation of the internal TransactionTracker.
//   - DO NOT call any other PUBLIC TransactionManager API while holding the mutex.
//     If a public method needs to rollback while holding the lock, it MUST call
//     the private rollbackLocked instead of the public RollbackTransaction to avoid
//     re-entrant locking and potential deadlocks.
type TransactionManager struct {
	sinker              Sinker
	watermarkUpdater    WatermarkUpdater
	watermarkKey        *WatermarkKey
	ownerFence          *OwnerFence
	watermarkGeneration uint64

	// Protects tracker and transactional state transitions
	mu sync.Mutex

	// Current transaction tracker
	tracker *TransactionTracker

	// Logging context
	accountId uint64
	taskId    string
	dbName    string
	tableName string
}

type targetOwnershipReleaser interface {
	releaseTargetOwnership() error
}

// joinErrorsPreservingSingle preserves a lone error's concrete identity. Some
// callers classify moerr values by concrete type; errors.Join(err, nil) would
// unnecessarily replace that value with a joinError. If both operations fail,
// retain both causes for diagnosis.
func joinErrorsPreservingSingle(first, second error) error {
	if first == nil {
		return second
	}
	if second == nil {
		return first
	}
	return errors.Join(first, second)
}

func (tm *TransactionManager) releaseTargetOwnership() error {
	if releaser, ok := tm.sinker.(targetOwnershipReleaser); ok {
		return releaser.releaseTargetOwnership()
	}
	return nil
}

// SetOwnerFence installs the durable daemon-claim check used by stable-epoch
// tasks. Legacy/direct users leave it nil.
func (tm *TransactionManager) SetOwnerFence(fence *OwnerFence) {
	tm.ownerFence = fence
}

// SetWatermarkGeneration binds stable progress to the source table incarnation
// that produced it. Legacy callers leave generation zero.
func (tm *TransactionManager) SetWatermarkGeneration(sourceTableID uint64) {
	tm.watermarkGeneration = sourceTableID
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
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.tracker != nil && tm.tracker.NeedsRollback() {
		logutil.Warn(
			"cdc.txn_manager.begin_with_unfinished",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		// Rollback previous transaction first
		if err := tm.rollbackLocked(ctx); err != nil {
			return err
		}
	}

	// If tracker exists and is already rolled back
	if tm.tracker != nil && tm.tracker.IsCompleted() {
		existingFromTs := tm.tracker.GetFromTs()
		existingToTs := tm.tracker.GetToTs()
		if existingFromTs.Equal(&fromTs) && existingToTs.Equal(&toTs) {
			// Same data range, already rolled back - this should not happen in normal flow
			// as processTailDone should check IsCompleted() before calling BeginTransaction
			// But if it does happen, clear tracker to allow retry
			tm.tracker = nil
		} else {
			// Different data range - clear old tracker
			tm.tracker = nil
		}
	}

	// Create new tracker
	tm.tracker = NewTransactionTracker(fromTs, toTs)

	// Send BEGIN to sinker
	tm.sinker.SendBegin()

	// Check for errors
	if err := tm.sinker.Error(); err != nil {
		logutil.Error(
			"cdc.txn_manager.send_begin_failed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		return joinErrorsPreservingSingle(err, tm.releaseTargetOwnership())
	}

	// Mark as begun
	tm.tracker.MarkBegin()

	logutil.Debug(
		"cdc.txn_manager.begin_success",
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
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.commitLocked(ctx, true)
}

// CommitTransactionWithoutWatermark commits an intermediate, retry-safe
// initial-snapshot group. The caller must guarantee that a retry reads the same
// immutable source epoch. Only the final group may publish the watermark.
func (tm *TransactionManager) CommitTransactionWithoutWatermark(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.commitLocked(ctx, false)
}

func (tm *TransactionManager) commitLocked(ctx context.Context, updateWatermark bool) error {
	if tm.tracker == nil {
		logutil.Warn(
			"cdc.txn_manager.commit_without_tracker",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if !tm.tracker.hasBegin {
		logutil.Warn(
			"cdc.txn_manager.commit_without_begin",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	toTs := tm.tracker.GetToTs()

	logutil.Debug(
		"cdc.txn_manager.commit_start",
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
			"cdc.txn_manager.send_commit_failed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.String("to-ts", toTs.ToString()),
			zap.Error(err),
		)
		// Commit failures can be ambiguous, but the executor has already
		// detached the sql.Tx. Do not retain the task/table advisory lock while
		// this pipeline waits for cleanup or retry; a replacement owner must be
		// able to reacquire it and replay from the durable watermark.
		return joinErrorsPreservingSingle(err, tm.releaseTargetOwnership())
	}

	if updateWatermark {
		// Step 2: Update watermark (persistent proof of success). This MUST
		// happen before marking the tracker as committed. Intermediate snapshot
		// groups deliberately skip this step.
		if err := tm.watermarkUpdater.UpdateWatermarkOnly(
			WithWatermarkOwnerFence(ctx, tm.ownerFence, tm.watermarkGeneration),
			tm.watermarkKey,
			&toTs,
		); err != nil {
			logutil.Error(
				"cdc.txn_manager.update_watermark_failed",
				zap.String("task-id", tm.taskId),
				zap.Uint64("account-id", tm.accountId),
				zap.String("db", tm.dbName),
				zap.String("table", tm.tableName),
				zap.String("to-ts", toTs.ToString()),
				zap.Error(err),
			)
			return joinErrorsPreservingSingle(err, tm.releaseTargetOwnership())
		}
	}

	// The target lock protects externally visible effects, not pipeline idle
	// time. Release it after every committed transaction, including an
	// intermediate initial-snapshot group. A replacement can then make
	// progress if this owner stalls while collecting the next group.
	if err := tm.releaseTargetOwnership(); err != nil {
		return err
	}

	// Step 3: Mark tracker as committed (memory state sync)
	tm.tracker.MarkCommit()
	if updateWatermark {
		tm.tracker.MarkWatermarkUpdated()
	}

	logutil.Debug(
		"cdc.txn_manager.commit_success",
		zap.String("task-id", tm.taskId),
		zap.Uint64("account-id", tm.accountId),
		zap.String("db", tm.dbName),
		zap.String("table", tm.tableName),
		zap.String("to-ts", toTs.ToString()),
		zap.Bool("watermark-updated", updateWatermark),
	)

	// Step 4: Clean up tracker to allow next transaction to begin
	// This is critical: without cleanup, processTailDone will see tracker.hasBegin == true
	// and won't call BeginTransaction again
	tm.tracker = nil

	return nil
}

// RollbackTransaction rolls back the current transaction
func (tm *TransactionManager) RollbackTransaction(ctx context.Context) error {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.tracker == nil {
		logutil.Warn(
			"cdc.txn_manager.rollback_without_tracker",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if !tm.tracker.hasBegin {
		logutil.Debug(
			"cdc.txn_manager.rollback_without_begin",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if tm.tracker.hasRolledBack {
		logutil.Debug(
			"cdc.txn_manager.already_rolled_back",
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
			"cdc.txn_manager.send_rollback_failed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		// Mark as rolled back even if it failed
		// to avoid infinite retry loops
		tm.tracker.MarkRollback()
		// Keep tracker to ensure EnsureCleanup is idempotent
		// BeginTransaction will clear it when starting a new transaction
		return err
	}

	// Mark tracker as rolled back
	tm.tracker.MarkRollback()

	// Keep tracker instead of setting to nil
	// Reason: This ensures EnsureCleanup is idempotent - if called again,
	// it will see hasRolledBack == true and won't trigger another rollback.
	// BeginTransaction will clear the tracker when starting a new transaction
	// (either for retry of same data or new data range).

	logutil.Debug(
		"cdc.txn_manager.rollback_success",
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
	tm.mu.Lock()
	defer tm.mu.Unlock()
	if tm.tracker == nil {
		return nil
	}

	// Layer 1: Check explicit transaction state
	if tm.tracker.NeedsRollback() {
		logutil.Warn(
			"cdc.txn_manager.ensure_cleanup_tracker_needs_rollback",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return tm.rollbackLocked(ctx)
	}

	// Layer 2: Verify watermark (dual-layer safety)
	toTs := tm.tracker.GetToTs()
	current, err := tm.watermarkUpdater.GetFromCache(ctx, tm.watermarkKey)

	if err != nil {
		// Even if GetFromCache fails, use tracker state
		logutil.Warn(
			"cdc.txn_manager.ensure_cleanup_get_from_cache_failed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		// Fallback to tracker state
		if tm.tracker.hasBegin && !tm.tracker.hasCommitted {
			return tm.rollbackLocked(ctx)
		}
		return nil
	}

	// Final guard: Even if tracker says committed, but watermark not updated
	if !current.Equal(&toTs) && tm.tracker.hasBegin {
		logutil.Error(
			"cdc.txn_manager.ensure_cleanup_watermark_mismatch",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.String("expected", toTs.ToString()),
			zap.String("actual", current.ToString()),
		)
		return tm.rollbackLocked(ctx)
	}

	return nil
}

// GetTracker returns the current transaction tracker
func (tm *TransactionManager) GetTracker() *TransactionTracker {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	return tm.tracker
}

// Reset resets the transaction manager for a new transaction
func (tm *TransactionManager) Reset() {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tracker = nil
}

// rollbackLocked rolls back the current transaction.
// NOTE: tm.mu MUST be held by the caller.
// This function is INTERNAL-ONLY and is used to avoid re-entrancy/deadlocks
// when a public method needs to perform a rollback while already holding tm.mu.
func (tm *TransactionManager) rollbackLocked(ctx context.Context) error {
	if tm.tracker == nil {
		logutil.Warn(
			"cdc.txn_manager.rollback_without_tracker",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if !tm.tracker.hasBegin {
		logutil.Debug(
			"cdc.txn_manager.rollback_without_begin",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
		)
		return nil
	}

	if tm.tracker.hasRolledBack {
		logutil.Debug(
			"cdc.txn_manager.already_rolled_back",
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
			"cdc.txn_manager.send_rollback_failed",
			zap.String("task-id", tm.taskId),
			zap.Uint64("account-id", tm.accountId),
			zap.String("db", tm.dbName),
			zap.String("table", tm.tableName),
			zap.Error(err),
		)
		// Mark as rolled back even if it failed
		// to avoid infinite retry loops
		tm.tracker.MarkRollback()
		// Keep tracker to ensure EnsureCleanup is idempotent
		// BeginTransaction will clear it when starting a new transaction
		return err
	}

	// Mark tracker as rolled back
	tm.tracker.MarkRollback()

	// Keep tracker instead of setting to nil
	// Reason: This ensures EnsureCleanup is idempotent - if called again,
	// it will see hasRolledBack == true and won't trigger another rollback.
	// BeginTransaction will clear the tracker when starting a new transaction
	// (either for retry of same data or new data range).

	logutil.Debug(
		"cdc.txn_manager.rollback_success",
		zap.String("task-id", tm.taskId),
		zap.Uint64("account-id", tm.accountId),
		zap.String("db", tm.dbName),
		zap.String("table", tm.tableName),
	)

	return nil
}
