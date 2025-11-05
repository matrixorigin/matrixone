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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"go.uber.org/zap"
)

// ChangeType represents the type of change data
type ChangeType int

const (
	// ChangeTypeSnapshot - Snapshot data (full table scan at fromTs)
	ChangeTypeSnapshot ChangeType = iota
	// ChangeTypeTailWip - Incremental data (work in progress, accumulating)
	ChangeTypeTailWip
	// ChangeTypeTailDone - Incremental data (done, ready to commit)
	ChangeTypeTailDone
	// ChangeTypeNoMoreData - No more data available
	ChangeTypeNoMoreData
)

func (ct ChangeType) String() string {
	switch ct {
	case ChangeTypeSnapshot:
		return "Snapshot"
	case ChangeTypeTailWip:
		return "TailWip"
	case ChangeTypeTailDone:
		return "TailDone"
	case ChangeTypeNoMoreData:
		return "NoMoreData"
	default:
		return "Unknown"
	}
}

// ChangeData represents a batch of change data
type ChangeData struct {
	// Type of change
	Type ChangeType

	// Insert data (new rows)
	InsertBatch *batch.Batch

	// Delete data (tombstones)
	DeleteBatch *batch.Batch

	// Original engine hint
	Hint engine.ChangesHandle_Hint
}

// HasData returns true if there is insert or delete data
func (cd *ChangeData) HasData() bool {
	return cd.InsertBatch != nil || cd.DeleteBatch != nil
}

// Clean cleans up the batch data
func (cd *ChangeData) Clean(mp *mpool.MPool) {
	if cd.InsertBatch != nil {
		cd.InsertBatch.Clean(mp)
		cd.InsertBatch = nil
	}
	if cd.DeleteBatch != nil {
		cd.DeleteBatch.Clean(mp)
		cd.DeleteBatch = nil
	}
}

// ChangeCollector collects changes from engine
// Key responsibilities:
// 1. Wrap engine.ChangesHandle for cleaner interface
// 2. Provide typed change data (Snapshot/TailWip/TailDone/NoMoreData)
// 3. Handle resource cleanup
type ChangeCollector struct {
	// Engine changes handle
	changesHandle engine.ChangesHandle

	// Memory pool
	mp *mpool.MPool

	// Context for collection
	fromTs types.TS
	toTs   types.TS

	// Logging context
	accountId uint64
	taskId    string
	dbName    string
	tableName string

	// State
	closed bool
}

// NewChangeCollector creates a new change collector
func NewChangeCollector(
	changesHandle engine.ChangesHandle,
	mp *mpool.MPool,
	fromTs, toTs types.TS,
	accountId uint64,
	taskId string,
	dbName string,
	tableName string,
) *ChangeCollector {
	return &ChangeCollector{
		changesHandle: changesHandle,
		mp:            mp,
		fromTs:        fromTs,
		toTs:          toTs,
		accountId:     accountId,
		taskId:        taskId,
		dbName:        dbName,
		tableName:     tableName,
	}
}

// Next retrieves the next batch of changes
// Returns:
// - *ChangeData: The change data (nil if error or no more data)
// - error: Any error encountered
//
// When both insert and delete batches are nil and no error:
// - Type will be ChangeTypeNoMoreData
func (cc *ChangeCollector) Next(ctx context.Context) (*ChangeData, error) {
	if cc.closed {
		logutil.Warn(
			"CDC-ChangeCollector-NextAfterClose",
			zap.String("task-id", cc.taskId),
			zap.Uint64("account-id", cc.accountId),
			zap.String("db", cc.dbName),
			zap.String("table", cc.tableName),
		)
		return &ChangeData{Type: ChangeTypeNoMoreData}, nil
	}

	// Call engine's Next
	insertBatch, deleteBatch, hint, err := cc.changesHandle.Next(ctx, cc.mp)
	if err != nil {
		logutil.Error(
			"CDC-ChangeCollector-NextFailed",
			zap.String("task-id", cc.taskId),
			zap.Uint64("account-id", cc.accountId),
			zap.String("db", cc.dbName),
			zap.String("table", cc.tableName),
			zap.Error(err),
		)
		return nil, err
	}

	// Convert engine hint to ChangeType
	var changeType ChangeType
	if insertBatch == nil && deleteBatch == nil {
		changeType = ChangeTypeNoMoreData
	} else {
		switch hint {
		case engine.ChangesHandle_Snapshot:
			changeType = ChangeTypeSnapshot
		case engine.ChangesHandle_Tail_wip:
			changeType = ChangeTypeTailWip
		case engine.ChangesHandle_Tail_done:
			changeType = ChangeTypeTailDone
		default:
			logutil.Warn(
				"CDC-ChangeCollector-UnknownHint",
				zap.String("task-id", cc.taskId),
				zap.Uint64("account-id", cc.accountId),
				zap.String("db", cc.dbName),
				zap.String("table", cc.tableName),
				zap.Int("hint", int(hint)),
			)
			changeType = ChangeTypeTailDone
		}
	}

	logutil.Debug(
		"CDC-ChangeCollector-Next",
		zap.String("task-id", cc.taskId),
		zap.Uint64("account-id", cc.accountId),
		zap.String("db", cc.dbName),
		zap.String("table", cc.tableName),
		zap.String("type", changeType.String()),
		zap.Bool("has-insert", insertBatch != nil),
		zap.Bool("has-delete", deleteBatch != nil),
	)

	return &ChangeData{
		Type:        changeType,
		InsertBatch: insertBatch,
		DeleteBatch: deleteBatch,
		Hint:        hint,
	}, nil
}

// Close closes the change collector
func (cc *ChangeCollector) Close() error {
	if cc.closed {
		return nil
	}

	cc.closed = true

	if cc.changesHandle != nil {
		if err := cc.changesHandle.Close(); err != nil {
			logutil.Error(
				"CDC-ChangeCollector-CloseFailed",
				zap.String("task-id", cc.taskId),
				zap.Uint64("account-id", cc.accountId),
				zap.String("db", cc.dbName),
				zap.String("table", cc.tableName),
				zap.Error(err),
			)
			return err
		}
	}

	logutil.Debug(
		"CDC-ChangeCollector-Closed",
		zap.String("task-id", cc.taskId),
		zap.Uint64("account-id", cc.accountId),
		zap.String("db", cc.dbName),
		zap.String("table", cc.tableName),
	)

	return nil
}

// IsClosed returns true if the collector is closed
func (cc *ChangeCollector) IsClosed() bool {
	return cc.closed
}

// GetFromTs returns the fromTs of this collection
func (cc *ChangeCollector) GetFromTs() types.TS {
	return cc.fromTs
}

// GetToTs returns the toTs of this collection
func (cc *ChangeCollector) GetToTs() types.TS {
	return cc.toTs
}
