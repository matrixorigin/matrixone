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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
)

// mockDataProcessorSinker for testing DataProcessor
type mockDataProcessorSinker struct {
	*mockSinker // Embed the mockSinker from txn_manager_test
	sinkCalls   []*DecoderOutput
}

func (m *mockDataProcessorSinker) Sink(ctx context.Context, data *DecoderOutput) {
	m.sinkCalls = append(m.sinkCalls, data)
}

func (m *mockDataProcessorSinker) reset() {
	m.mockSinker.reset()
	m.sinkCalls = nil
}

func TestNewDataProcessor(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	assert.NotNil(t, dp)
	assert.Equal(t, sinker, dp.sinker)
	assert.Equal(t, txnMgr, dp.txnManager)
	assert.Equal(t, mp, dp.mp)
	assert.Equal(t, 1, dp.insTsColIdx)
	assert.Equal(t, 0, dp.insCompositedPkColIdx)
	assert.False(t, dp.initSnapshotSplitTxn)
}

func TestDataProcessor_SetTransactionRange(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	dp.SetTransactionRange(fromTs, toTs)

	assert.Equal(t, fromTs, dp.fromTs)
	assert.Equal(t, toTs, dp.toTs)
}

func TestDataProcessor_ProcessChange_Snapshot(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false, // initSnapshotSplitTxn = false
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	dp.SetTransactionRange(fromTs, toTs)

	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: &batch.Batch{},
	}

	err = dp.ProcessChange(ctx, data)

	assert.NoError(t, err)
	assert.True(t, sinker.beginCalled)
	assert.Equal(t, 1, len(sinker.sinkCalls))
	assert.Equal(t, OutputTypeSnapshot, sinker.sinkCalls[0].outputTyp)
}

func TestDataProcessor_ProcessChange_Snapshot_WithSplitTxn(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, true, // initSnapshotSplitTxn = true
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	dp.SetTransactionRange(fromTs, toTs)

	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: &batch.Batch{},
	}

	err = dp.ProcessChange(ctx, data)

	assert.NoError(t, err)
	assert.False(t, sinker.beginCalled) // Should NOT begin for split snapshot
	assert.Equal(t, 1, len(sinker.sinkCalls))
}

func TestDataProcessor_ProcessChange_TailWip(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	dp.SetTransactionRange(fromTs, toTs)

	data := &ChangeData{
		Type:        ChangeTypeTailWip,
		InsertBatch: nil, // Use nil to avoid AtomicBatch.Append panic with empty batch
		DeleteBatch: nil,
	}

	err = dp.ProcessChange(ctx, data)

	assert.NoError(t, err)
	assert.False(t, sinker.beginCalled)       // Should NOT begin for TailWip
	assert.Equal(t, 0, len(sinker.sinkCalls)) // Should NOT sink yet
	assert.NotNil(t, dp.insertAtmBatch)       // Should accumulate
	assert.NotNil(t, dp.deleteAtmBatch)
}

func TestDataProcessor_ProcessChange_TailDone(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	dp.SetTransactionRange(fromTs, toTs)

	data := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: nil, // Use nil to avoid AtomicBatch.Append panic with empty batch
		DeleteBatch: nil,
	}

	err = dp.ProcessChange(ctx, data)

	assert.NoError(t, err)
	assert.True(t, sinker.beginCalled)        // Should begin for TailDone
	assert.Equal(t, 1, len(sinker.sinkCalls)) // Should sink
	assert.Equal(t, OutputTypeTail, sinker.sinkCalls[0].outputTyp)
	assert.Nil(t, dp.insertAtmBatch) // Should reset after sink
	assert.Nil(t, dp.deleteAtmBatch)
}

func TestDataProcessor_ProcessChange_NoMoreData(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	dp.SetTransactionRange(fromTs, toTs)

	// Begin a transaction first
	err = txnMgr.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	sinker.reset()

	data := &ChangeData{
		Type: ChangeTypeNoMoreData,
	}

	err = dp.ProcessChange(ctx, data)

	assert.NoError(t, err)
	assert.True(t, sinker.dummyCalled)        // Should send dummy
	assert.True(t, sinker.commitCalled)       // Should commit
	assert.Equal(t, 1, len(sinker.sinkCalls)) // Should send heartbeat
	assert.True(t, sinker.sinkCalls[0].noMoreData)
}

func TestDataProcessor_ProcessChange_SinkerError(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	sinker.err = moerr.NewInternalError(ctx, "test error")
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: &batch.Batch{},
	}

	err = dp.ProcessChange(ctx, data)

	assert.Error(t, err)
	assert.Equal(t, 0, len(sinker.sinkCalls)) // Should not sink
}

func TestDataProcessor_Cleanup(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	// Set some atomic batches
	dp.insertAtmBatch = NewAtomicBatch(mp)
	dp.deleteAtmBatch = NewAtomicBatch(mp)

	dp.Cleanup()

	assert.Nil(t, dp.insertAtmBatch)
	assert.Nil(t, dp.deleteAtmBatch)
}

func TestDataProcessor_GetAtmBatches(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, false,
		1, "task1", "db1", "table1",
	)

	assert.Nil(t, dp.GetInsertAtmBatch())
	assert.Nil(t, dp.GetDeleteAtmBatch())

	dp.insertAtmBatch = NewAtomicBatch(mp)
	dp.deleteAtmBatch = NewAtomicBatch(mp)

	assert.NotNil(t, dp.GetInsertAtmBatch())
	assert.NotNil(t, dp.GetDeleteAtmBatch())
}
