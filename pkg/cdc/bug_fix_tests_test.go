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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Bug #1: TransactionTracker.UpdateToTs() updates both toTs and expectedWatermark
// Without this fix, watermark would not advance when multiple tail batches are processed
func TestTransactionTracker_UpdateToTs(t *testing.T) {
	fromTs := types.BuildTS(100, 1)
	toTs1 := types.BuildTS(200, 1)
	toTs2 := types.BuildTS(300, 1)
	toTs3 := types.BuildTS(400, 1)

	tracker := NewTransactionTracker(fromTs, toTs1)

	assert.Equal(t, fromTs, tracker.GetFromTs())
	assert.Equal(t, toTs1, tracker.GetToTs())
	assert.Equal(t, toTs1, tracker.GetExpectedWatermark())

	tracker.UpdateToTs(toTs2)
	assert.Equal(t, toTs2, tracker.GetToTs())
	assert.Equal(t, toTs2, tracker.GetExpectedWatermark())

	tracker.UpdateToTs(toTs3)
	assert.Equal(t, toTs3, tracker.GetToTs())
	assert.Equal(t, toTs3, tracker.GetExpectedWatermark())

	assert.Equal(t, fromTs, tracker.GetFromTs(), "fromTs should not be affected by UpdateToTs")
}

// Bug #1: CommitTransaction must use the latest toTs after multiple UpdateToTs calls
// Scenario: Multiple tail batches processed in one transaction
// Expected: Watermark should advance to the final toTs, not the initial one
func TestTransactionManager_CommitTransaction_UsesLatestToTs(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.BuildTS(100, 1)
	toTs1 := types.BuildTS(200, 1)
	toTs2 := types.BuildTS(300, 1)
	toTs3 := types.BuildTS(400, 1)

	err := tm.BeginTransaction(ctx, fromTs, toTs1)
	require.NoError(t, err)

	tracker := tm.GetTracker()
	require.NotNil(t, tracker)
	assert.Equal(t, toTs1, tracker.GetToTs())

	tracker.UpdateToTs(toTs2)
	assert.Equal(t, toTs2, tracker.GetToTs())

	tracker.UpdateToTs(toTs3)
	assert.Equal(t, toTs3, tracker.GetToTs())

	err = tm.CommitTransaction(ctx)
	require.NoError(t, err)

	wm, err := updater.GetFromCache(ctx, tm.watermarkKey)
	require.NoError(t, err)
	assert.Equal(t, toTs3, wm, "Watermark should be updated to the latest toTs (400), not initial toTs (200)")
}

// Bug #4: Empty InsertBatch should fail validation in Sinker (Fail Fast principle)
// This ensures DataProcessor must filter out empty batches before sending to Sinker
func TestCommand_Validate_EmptyInsertBatch(t *testing.T) {
	fromTs := types.BuildTS(100, 1)
	toTs := types.BuildTS(200, 1)

	cmd := &Command{
		Type:        CmdInsertBatch,
		InsertBatch: &batch.Batch{},
		Meta: CommandMetadata{
			FromTs: fromTs,
			ToTs:   toTs,
		},
	}

	err := cmd.Validate()
	assert.Error(t, err, "Empty InsertBatch should fail validation")
	assert.Contains(t, err.Error(), "InsertBatch has no rows")
}

// Bug #6: AtomicBatch.Close() sets Rows to nil, subsequent RowCount() will panic
// This test verifies the Fail Fast behavior
func TestAtomicBatch_Close_SetsNil(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	bat := NewAtomicBatch(mp)
	require.NotNil(t, bat.Rows)

	bat.Close()

	assert.Nil(t, bat.Rows, "Close() should set Rows to nil")

	assert.Panics(t, func() {
		_ = bat.RowCount()
	}, "RowCount() after Close() should panic (Fail Fast)")
}
