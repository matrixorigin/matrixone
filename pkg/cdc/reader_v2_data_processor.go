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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

const (
	initialSnapshotTxnBatchLimit = 8
	initialSnapshotTxnByteLimit  = 512 * mpool.MB
)

// DataProcessor processes change data and sends to sinker
// Key responsibilities:
// 1. Process different types of changes (Snapshot/TailWip/TailDone/NoMoreData)
// 2. Accumulate TailWip/TailDone data into AtomicBatch
// 3. Coordinate with TransactionManager (decide when to BEGIN)
// 4. Send data to Sinker
// 5. Handle resource cleanup
type DataProcessor struct {
	// Sinker to send data to
	sinker Sinker

	// Transaction manager
	txnManager *TransactionManager

	// Memory pool
	mp *mpool.MPool

	// Packer pool for encoding primary keys
	packerPool *fileservice.Pool[*types.Packer]

	// Table definition for column indices
	insTsColIdx           int
	insCompositedPkColIdx int
	delTsColIdx           int
	delCompositedPkColIdx int

	// Current accumulated atomic batches
	insertAtmBatch *AtomicBatch
	deleteAtmBatch *AtomicBatch

	// Mutex for cleanup operations
	cleanupMu sync.Mutex

	// Enabled only when TableChangeStream has a persisted stable source epoch.
	// Intermediate groups may then be replayed safely without advancing the
	// watermark.
	initSnapshotSplitTxn bool
	snapshotTxnBatches   int
	snapshotTxnBytes     uint64
	// snapshotGroup stages a bounded stable-epoch group before opening the
	// target transaction. Source collection must never run while the target
	// advisory lock is held: a taskservice-partitioned old owner could otherwise
	// block its replacement behind that session indefinitely.
	snapshotGroup []*DecoderOutput

	// Logging context
	accountId uint64
	taskId    string
	dbName    string
	tableName string

	// Current transaction range
	fromTs types.TS
	toTs   types.TS
}

// NewDataProcessor creates a new data processor
func NewDataProcessor(
	sinker Sinker,
	txnManager *TransactionManager,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	insTsColIdx int,
	insCompositedPkColIdx int,
	delTsColIdx int,
	delCompositedPkColIdx int,
	initSnapshotSplitTxn bool,
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
) *DataProcessor {
	return &DataProcessor{
		sinker:                sinker,
		txnManager:            txnManager,
		mp:                    mp,
		packerPool:            packerPool,
		insTsColIdx:           insTsColIdx,
		insCompositedPkColIdx: insCompositedPkColIdx,
		delTsColIdx:           delTsColIdx,
		delCompositedPkColIdx: delCompositedPkColIdx,
		initSnapshotSplitTxn:  initSnapshotSplitTxn,
		accountId:             accountId,
		taskId:                taskId,
		dbName:                dbName,
		tableName:             tableName,
	}
}

// SetTransactionRange sets the from/to timestamps for the current transaction
func (dp *DataProcessor) SetTransactionRange(fromTs, toTs types.TS) {
	dp.fromTs = fromTs
	dp.toTs = toTs
}

// ProcessChange processes a single ChangeData
// Returns error if processing fails. The caller retains ownership of every
// non-nil batch in data; a successful ownership transfer clears that field.
func (dp *DataProcessor) ProcessChange(ctx context.Context, data *ChangeData) error {
	// Check sinker error from last round
	if err := dp.sinker.Error(); err != nil {
		logutil.Error(
			"cdc.data_processor.sinker_error",
			zap.String("task-id", dp.taskId),
			zap.Uint64("account-id", dp.accountId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.Error(err),
		)
		return err
	}

	switch data.Type {
	case ChangeTypeSnapshot:
		return dp.processSnapshot(ctx, data)
	case ChangeTypeTailWip:
		return dp.processTailWip(ctx, data)
	case ChangeTypeTailDone:
		return dp.processTailDone(ctx, data)
	case ChangeTypeNoMoreData:
		return dp.processNoMoreData(ctx)
	default:
		logutil.Warn(
			"cdc.data_processor.unknown_change_type",
			zap.String("task-id", dp.taskId),
			zap.Uint64("account-id", dp.accountId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.String("type", data.Type.String()),
		)
		return nil
	}
}

// processSnapshot processes snapshot data
func (dp *DataProcessor) processSnapshot(ctx context.Context, data *ChangeData) error {
	rows := 0
	if data.InsertBatch != nil {
		rows = data.InsertBatch.RowCount()
	}

	logutil.Debug(
		"cdc.data_processor.process_snapshot_start",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Int("rows", rows),
		zap.String("from-ts", dp.fromTs.ToString()),
		zap.String("to-ts", dp.toTs.ToString()),
	)

	// Skip if no data (empty table snapshot)
	if rows == 0 {
		logutil.Debug(
			"cdc.data_processor.process_snapshot_skip_empty",
			zap.String("task-id", dp.taskId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
		)
		return nil
	}

	batchBytes := uint64(data.InsertBatch.Allocated())
	if dp.initSnapshotSplitTxn {
		// The next batch is already owned, so rotate before staging it when adding
		// it would cross the transaction bound. The current group has no target
		// transaction yet; flushing is the only interval that owns the target lock.
		if dp.shouldRotateSnapshotTxn(batchBytes) {
			if err := dp.commitPendingSnapshotGroup(ctx); err != nil {
				return err
			}
		}
		dp.stageSnapshot(data, batchBytes)
		// Do not wait for a ninth permit while retaining all eight permits. The byte
		// case also flushes promptly once the indivisible current batch reaches the
		// bound. Partial groups may be flushed earlier by admission backpressure.
		if dp.snapshotTxnBatches >= initialSnapshotTxnBatchLimit ||
			dp.snapshotTxnBytes >= uint64(initialSnapshotTxnByteLimit) {
			if err := dp.commitPendingSnapshotGroup(ctx); err != nil {
				return err
			}
		}
		dp.logSnapshotComplete(rows)
		return nil
	}

	if err := dp.beginSnapshotTransaction(ctx); err != nil {
		return err
	}

	// Legacy atomic snapshots preserve the existing immediate transfer path.
	dp.sinker.Sink(ctx, &DecoderOutput{
		outputTyp:      OutputTypeSnapshot,
		checkpointBat:  data.InsertBatch,
		fromTs:         dp.fromTs,
		toTs:           dp.toTs,
		mp:             dp.mp,
		snapshotPermit: data.snapshotPermit,
	})
	data.InsertBatch = nil
	data.snapshotPermit = nil
	dp.snapshotTxnBatches++
	dp.snapshotTxnBytes += batchBytes

	// Note: For initSnapshotSplitTxn mode, we DON'T update watermark after each batch
	// because snapshot data might span multiple batches.
	// Watermark should only be updated when ALL snapshot data is processed (in processNoMoreData)

	dp.logSnapshotComplete(rows)

	return nil
}

func (dp *DataProcessor) logSnapshotComplete(rows int) {
	logutil.Debug(
		"cdc.data_processor.process_snapshot_complete",
		zap.String("task-id", dp.taskId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Int("rows", rows),
		zap.String("from-ts", dp.fromTs.ToString()),
		zap.String("to-ts", dp.toTs.ToString()),
	)
}

func (dp *DataProcessor) beginSnapshotTransaction(ctx context.Context) error {
	tracker := dp.txnManager.GetTracker()
	if tracker != nil && tracker.hasBegin {
		return nil
	}
	if err := dp.txnManager.BeginTransaction(ctx, dp.fromTs, dp.toTs); err != nil {
		logutil.Error(
			"cdc.data_processor.begin_transaction_failed",
			zap.String("task-id", dp.taskId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.Error(err),
		)
		return err
	}
	return nil
}

func (dp *DataProcessor) stageSnapshot(data *ChangeData, batchBytes uint64) {
	dp.snapshotGroup = append(dp.snapshotGroup, &DecoderOutput{
		outputTyp:      OutputTypeSnapshot,
		checkpointBat:  data.InsertBatch,
		fromTs:         dp.fromTs,
		toTs:           dp.toTs,
		mp:             dp.mp,
		snapshotPermit: data.snapshotPermit,
	})
	data.InsertBatch = nil
	data.snapshotPermit = nil
	dp.snapshotTxnBatches++
	dp.snapshotTxnBytes += batchBytes
}

// sendPendingSnapshotGroup opens the target effect interval only after source
// collection has produced the complete bounded group. On successful return all
// staged batches have transferred to the sinker, while the target transaction
// remains active for the caller to commit with or without a watermark.
func (dp *DataProcessor) sendPendingSnapshotGroup(ctx context.Context) error {
	if len(dp.snapshotGroup) == 0 {
		return nil
	}
	if err := dp.beginSnapshotTransaction(ctx); err != nil {
		return err
	}
	for i, output := range dp.snapshotGroup {
		dp.sinker.Sink(ctx, output)
		dp.snapshotGroup[i] = nil
	}
	dp.snapshotGroup = dp.snapshotGroup[:0]
	return nil
}

// commitPendingSnapshotGroup completes an intermediate stable-epoch group
// without advancing the watermark. The transaction manager synchronously
// drains the sink command queue before releasing target ownership.
func (dp *DataProcessor) commitPendingSnapshotGroup(ctx context.Context) error {
	if len(dp.snapshotGroup) == 0 {
		return nil
	}
	if err := dp.sendPendingSnapshotGroup(ctx); err != nil {
		return err
	}
	if err := dp.txnManager.CommitTransactionWithoutWatermark(ctx); err != nil {
		logutil.Error(
			"cdc.data_processor.commit_snapshot_group_failed",
			zap.String("task-id", dp.taskId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.Int("group-batches", dp.snapshotTxnBatches),
			zap.Uint64("group-bytes", dp.snapshotTxnBytes),
			zap.Error(err),
		)
		return err
	}
	dp.resetSnapshotTxnGroup()
	return nil
}

func (dp *DataProcessor) hasPendingSnapshotGroup() bool {
	return len(dp.snapshotGroup) > 0
}

func (dp *DataProcessor) shouldRotateSnapshotTxn(nextBatchBytes uint64) bool {
	if !dp.initSnapshotSplitTxn || dp.snapshotTxnBatches == 0 {
		return false
	}
	if dp.snapshotTxnBatches >= initialSnapshotTxnBatchLimit {
		return true
	}
	limit := uint64(initialSnapshotTxnByteLimit)
	return dp.snapshotTxnBytes >= limit || nextBatchBytes > limit-dp.snapshotTxnBytes
}

func (dp *DataProcessor) resetSnapshotTxnGroup() {
	dp.snapshotTxnBatches = 0
	dp.snapshotTxnBytes = 0
}

// processTailWip processes tail work-in-progress data (accumulate)
func (dp *DataProcessor) processTailWip(ctx context.Context, data *ChangeData) error {
	hasInsert := data.InsertBatch != nil
	hasDelete := data.DeleteBatch != nil
	insertRows := 0
	deleteRows := 0
	if data.InsertBatch != nil {
		insertRows = data.InsertBatch.RowCount()
	}
	if data.DeleteBatch != nil {
		deleteRows = data.DeleteBatch.RowCount()
	}

	logutil.Debug(
		"cdc.data_processor.process_tail_wip",
		zap.String("task-id", dp.taskId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Int("insert-rows", insertRows),
		zap.Int("delete-rows", deleteRows),
	)

	// Get packer from pool
	var packer *types.Packer
	put := dp.packerPool.Get(&packer)
	defer put.Put()

	// Allocate atomic batches if needed
	if dp.insertAtmBatch == nil {
		dp.insertAtmBatch = NewAtomicBatch(dp.mp)
	}
	if dp.deleteAtmBatch == nil {
		dp.deleteAtmBatch = NewAtomicBatch(dp.mp)
	}

	// Append to atomic batches
	dp.insertAtmBatch.Append(packer, data.InsertBatch, dp.insTsColIdx, dp.insCompositedPkColIdx)
	dp.deleteAtmBatch.Append(packer, data.DeleteBatch, dp.delTsColIdx, dp.delCompositedPkColIdx)
	data.InsertBatch = nil
	data.DeleteBatch = nil

	logutil.Debug(
		"cdc.data_processor.process_tail_wip",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Bool("has-insert", hasInsert),
		zap.Bool("has-delete", hasDelete),
		zap.Int("insert-rows", dp.insertAtmBatch.RowCount()),
		zap.Int("delete-rows", dp.deleteAtmBatch.RowCount()),
	)

	return nil
}

// processTailDone processes tail done data (accumulate and send)
func (dp *DataProcessor) processTailDone(ctx context.Context, data *ChangeData) error {
	// Get packer from pool
	var packer *types.Packer
	put := dp.packerPool.Get(&packer)
	defer put.Put()

	// Allocate atomic batches if needed
	if dp.insertAtmBatch == nil {
		dp.insertAtmBatch = NewAtomicBatch(dp.mp)
	}
	if dp.deleteAtmBatch == nil {
		dp.deleteAtmBatch = NewAtomicBatch(dp.mp)
	}

	// Append to atomic batches
	dp.insertAtmBatch.Append(packer, data.InsertBatch, dp.insTsColIdx, dp.insCompositedPkColIdx)
	dp.deleteAtmBatch.Append(packer, data.DeleteBatch, dp.delTsColIdx, dp.delCompositedPkColIdx)
	data.InsertBatch = nil
	data.DeleteBatch = nil

	// Begin transaction if not already begun
	tracker := dp.txnManager.GetTracker()
	if tracker == nil || !tracker.hasBegin || tracker.IsCompleted() {
		if err := dp.txnManager.BeginTransaction(ctx, dp.fromTs, dp.toTs); err != nil {
			return err
		}
	} else {
		// Transaction already active - update the toTs to the latest value
		// This is important when multiple Tail batches are processed in one transaction
		tracker.UpdateToTs(dp.toTs)
	}

	// Send accumulated data to sinker
	// Get row counts before Sink() since Sink() is asynchronous and batches
	// may be closed by the sinker goroutine after being queued
	insertRows := 0
	deleteRows := 0
	if dp.insertAtmBatch != nil {
		insertRows = dp.insertAtmBatch.RowCount()
	}
	if dp.deleteAtmBatch != nil {
		deleteRows = dp.deleteAtmBatch.RowCount()
	}

	dp.sinker.Sink(ctx, &DecoderOutput{
		outputTyp:      OutputTypeTail,
		insertAtmBatch: dp.insertAtmBatch,
		deleteAtmBatch: dp.deleteAtmBatch,
		fromTs:         dp.fromTs,
		toTs:           dp.toTs,
	})

	logutil.Debug(
		"cdc.data_processor.process_tail_done",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Int("insert-rows", insertRows),
		zap.Int("delete-rows", deleteRows),
		zap.String("from-ts", dp.fromTs.ToString()),
		zap.String("to-ts", dp.toTs.ToString()),
	)

	// Note: Sink() takes ownership of the atomic batches
	// Don't Close them here - they might still be used by Sinker asynchronously
	// The Sinker or Command should be responsible for closing them
	// For now, just reset our references
	dp.insertAtmBatch = nil
	dp.deleteAtmBatch = nil

	return nil
}

// processNoMoreData processes end of data (send heartbeat and commit)
func (dp *DataProcessor) processNoMoreData(ctx context.Context) error {
	// Stable snapshots stage source batches without a target transaction. Open
	// the final effect interval only now, after collection has terminated.
	if err := dp.sendPendingSnapshotGroup(ctx); err != nil {
		return err
	}
	// Send heartbeat (no more data marker)
	dp.sinker.Sink(ctx, &DecoderOutput{
		noMoreData: true,
		fromTs:     dp.fromTs,
		toTs:       dp.toTs,
	})

	// Send dummy to guarantee last data is sent successfully
	dp.sinker.SendDummy()

	// Check for errors
	if err := dp.sinker.Error(); err != nil {
		logutil.Error(
			"cdc.data_processor.no_more_data_sinker_error",
			zap.String("task-id", dp.taskId),
			zap.Uint64("account-id", dp.accountId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.Error(err),
		)
		return err
	}

	// Commit transaction if one is active
	tracker := dp.txnManager.GetTracker()
	if tracker != nil && tracker.hasBegin {
		tracker.UpdateToTs(dp.toTs)
		logutil.Debug(
			"cdc.data_processor.no_more_data_committing",
			zap.String("task-id", dp.taskId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.String("from-ts", dp.fromTs.ToString()),
			zap.String("to-ts", dp.toTs.ToString()),
		)
		if err := dp.txnManager.CommitTransaction(ctx); err != nil {
			logutil.Error(
				"cdc.data_processor.no_more_data_commit_failed",
				zap.String("task-id", dp.taskId),
				zap.Uint64("account-id", dp.accountId),
				zap.String("db", dp.dbName),
				zap.String("table", dp.tableName),
				zap.Error(err),
			)
			return err
		}
		dp.resetSnapshotTxnGroup()
		logutil.Debug(
			"cdc.data_processor.no_more_data_commit_success",
			zap.String("task-id", dp.taskId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.String("from-ts", dp.fromTs.ToString()),
			zap.String("to-ts", dp.toTs.ToString()),
		)
	} else {
		// Even if no transaction is active (e.g., initSnapshotSplitTxn=true),
		// we still need to update watermark as a heartbeat to indicate progress.
		// This ensures watermark advances even when there's no data change.
		tableLabel := dp.dbName + "." + dp.tableName

		// Metrics: heartbeat watermark update
		v2.CdcHeartbeatCounter.WithLabelValues(tableLabel).Inc()

		logutil.Debug(
			"cdc.data_processor.no_more_data_heartbeat_update",
			zap.String("task-id", dp.taskId),
			zap.String("db", dp.dbName),
			zap.String("table", dp.tableName),
			zap.String("from-ts", dp.fromTs.ToString()),
			zap.String("to-ts", dp.toTs.ToString()),
		)

		if err := dp.txnManager.watermarkUpdater.UpdateWatermarkOnly(
			WithWatermarkOwnerFence(
				ctx, dp.txnManager.ownerFence, dp.txnManager.watermarkGeneration),
			dp.txnManager.watermarkKey,
			&dp.toTs,
		); err != nil {
			logutil.Error(
				"cdc.data_processor.no_more_data_update_watermark_failed",
				zap.String("task-id", dp.taskId),
				zap.Uint64("account-id", dp.accountId),
				zap.String("db", dp.dbName),
				zap.String("table", dp.tableName),
				zap.String("to-ts", dp.toTs.ToString()),
				zap.Error(err),
			)
			return err
		}
	}

	logutil.Debug(
		"cdc.data_processor.process_no_more_data",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
	)

	return nil
}

// Cleanup cleans up any remaining resources
// This should be called in defer to ensure cleanup even on errors
// This method is safe to call concurrently and is idempotent
func (dp *DataProcessor) Cleanup() {
	dp.cleanupMu.Lock()
	defer dp.cleanupMu.Unlock()

	if dp.insertAtmBatch != nil {
		dp.insertAtmBatch.Close()
		dp.insertAtmBatch = nil
	}
	if dp.deleteAtmBatch != nil {
		dp.deleteAtmBatch.Close()
		dp.deleteAtmBatch = nil
	}
	for i, output := range dp.snapshotGroup {
		if output != nil {
			output.Close()
			dp.snapshotGroup[i] = nil
		}
	}
	dp.snapshotGroup = nil
	dp.resetSnapshotTxnGroup()

	logutil.Debug(
		"cdc.data_processor.cleanup",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
	)
}

// GetInsertAtmBatch returns the current insert atomic batch (for testing)
func (dp *DataProcessor) GetInsertAtmBatch() *AtomicBatch {
	return dp.insertAtmBatch
}

// GetDeleteAtmBatch returns the current delete atomic batch (for testing)
func (dp *DataProcessor) GetDeleteAtmBatch() *AtomicBatch {
	return dp.deleteAtmBatch
}
