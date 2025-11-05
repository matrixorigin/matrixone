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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
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

	// Configuration
	initSnapshotSplitTxn bool // Whether to split snapshot into separate transactions

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
// Returns error if processing fails
func (dp *DataProcessor) ProcessChange(ctx context.Context, data *ChangeData) error {
	// Check sinker error from last round
	if err := dp.sinker.Error(); err != nil {
		logutil.Error(
			"CDC-DataProcessor-SinkerError",
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
			"CDC-DataProcessor-UnknownChangeType",
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
	// Begin transaction if needed (unless initSnapshotSplitTxn is set)
	tracker := dp.txnManager.GetTracker()
	if tracker == nil || !tracker.hasBegin {
		if !dp.initSnapshotSplitTxn {
			if err := dp.txnManager.BeginTransaction(ctx, dp.fromTs, dp.toTs); err != nil {
				return err
			}
		}
	}

	// Send snapshot data to sinker
	dp.sinker.Sink(ctx, &DecoderOutput{
		outputTyp:     OutputTypeSnapshot,
		checkpointBat: data.InsertBatch,
		fromTs:        dp.fromTs,
		toTs:          dp.toTs,
	})

	// Note: We don't clean data.InsertBatch here because Sink() takes ownership

	logutil.Debug(
		"CDC-DataProcessor-ProcessSnapshot",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Bool("has-insert", data.InsertBatch != nil),
	)

	return nil
}

// processTailWip processes tail work-in-progress data (accumulate)
func (dp *DataProcessor) processTailWip(ctx context.Context, data *ChangeData) error {
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

	logutil.Debug(
		"CDC-DataProcessor-ProcessTailWip",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Bool("has-insert", data.InsertBatch != nil),
		zap.Bool("has-delete", data.DeleteBatch != nil),
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

	// Begin transaction if not already begun
	tracker := dp.txnManager.GetTracker()
	if tracker == nil || !tracker.hasBegin {
		if err := dp.txnManager.BeginTransaction(ctx, dp.fromTs, dp.toTs); err != nil {
			return err
		}
	}

	// Send accumulated data to sinker
	dp.sinker.Sink(ctx, &DecoderOutput{
		outputTyp:      OutputTypeTail,
		insertAtmBatch: dp.insertAtmBatch,
		deleteAtmBatch: dp.deleteAtmBatch,
		fromTs:         dp.fromTs,
		toTs:           dp.toTs,
	})

	logutil.Debug(
		"CDC-DataProcessor-ProcessTailDone",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
		zap.Int("insert-rows", dp.insertAtmBatch.RowCount()),
		zap.Int("delete-rows", dp.deleteAtmBatch.RowCount()),
	)

	// Close atomic batches (Sink() takes ownership, but we need to clean our references)
	dp.insertAtmBatch.Close()
	dp.deleteAtmBatch.Close()

	// Reset for next batch
	dp.insertAtmBatch = nil
	dp.deleteAtmBatch = nil

	return nil
}

// processNoMoreData processes end of data (send heartbeat and commit)
func (dp *DataProcessor) processNoMoreData(ctx context.Context) error {
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
			"CDC-DataProcessor-NoMoreData-SinkerError",
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
		if err := dp.txnManager.CommitTransaction(ctx); err != nil {
			logutil.Error(
				"CDC-DataProcessor-NoMoreData-CommitFailed",
				zap.String("task-id", dp.taskId),
				zap.Uint64("account-id", dp.accountId),
				zap.String("db", dp.dbName),
				zap.String("table", dp.tableName),
				zap.Error(err),
			)
			return err
		}
	}

	logutil.Debug(
		"CDC-DataProcessor-ProcessNoMoreData",
		zap.String("task-id", dp.taskId),
		zap.Uint64("account-id", dp.accountId),
		zap.String("db", dp.dbName),
		zap.String("table", dp.tableName),
	)

	return nil
}

// Cleanup cleans up any remaining resources
// This should be called in defer to ensure cleanup even on errors
func (dp *DataProcessor) Cleanup() {
	if dp.insertAtmBatch != nil {
		dp.insertAtmBatch.Close()
		dp.insertAtmBatch = nil
	}
	if dp.deleteAtmBatch != nil {
		dp.deleteAtmBatch.Close()
		dp.deleteAtmBatch = nil
	}

	logutil.Debug(
		"CDC-DataProcessor-Cleanup",
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
