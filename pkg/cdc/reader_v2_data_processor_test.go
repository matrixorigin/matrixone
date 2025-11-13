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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

type dataProcessorRecordingSinker struct {
	*recordingSinker
	sinkCalls []*DecoderOutput
}

func newDataProcessorRecordingSinker() *dataProcessorRecordingSinker {
	return &dataProcessorRecordingSinker{
		recordingSinker: newRecordingSinker(),
	}
}

func (s *dataProcessorRecordingSinker) Sink(ctx context.Context, data *DecoderOutput) {
	s.mu.Lock()
	s.ops = append(s.ops, "sink")
	s.sinkCalls = append(s.sinkCalls, data)
	s.mu.Unlock()
}

func (s *dataProcessorRecordingSinker) reset() {
	s.mu.Lock()
	s.err = nil
	s.rollbackErr = nil
	s.commitErr = nil
	s.beginErr = nil
	s.sinkCalls = nil
	s.mu.Unlock()
	s.resetOps()
}

func (s *dataProcessorRecordingSinker) sinkCallsSnapshot() []*DecoderOutput {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*DecoderOutput, len(s.sinkCalls))
	copy(cp, s.sinkCalls)
	return cp
}

type dataProcessorHarness struct {
	dp     *DataProcessor
	sinker *dataProcessorRecordingSinker
	txnMgr *TransactionManager
	update *mockWatermarkUpdater
	mp     *mpool.MPool
}

func newDataProcessorHarness(t *testing.T, splitSnapshot bool) *dataProcessorHarness {
	t.Helper()

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() {
		mpool.DeleteMPool(mp)
	})

	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	sinker := newDataProcessorRecordingSinker()
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	dp := NewDataProcessor(
		sinker,
		txnMgr,
		mp,
		packerPool,
		1, 0,
		1, 0,
		splitSnapshot,
		1,
		"task1",
		"db1",
		"table1",
	)

	return &dataProcessorHarness{
		dp:     dp,
		sinker: sinker,
		txnMgr: txnMgr,
		update: updater,
		mp:     mp,
	}
}

func buildBatch(t *testing.T, mp *mpool.MPool, pkVals []int32, ts types.TS) *batch.Batch {
	t.Helper()

	if len(pkVals) == 0 {
		return nil
	}

	bat := batch.New([]string{"pk", "ts"})
	pkVec := vector.NewVec(types.T_int32.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())

	for range pkVals {
		if err := vector.AppendFixed(tsVec, ts, false, mp); err != nil {
			t.Fatalf("append ts: %v", err)
		}
	}

	for _, v := range pkVals {
		if err := vector.AppendFixed(pkVec, v, false, mp); err != nil {
			t.Fatalf("append pk: %v", err)
		}
	}

	bat.Vecs[0] = pkVec
	bat.Vecs[1] = tsVec
	bat.SetRowCount(len(pkVals))
	return bat
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

	// Create a non-empty batch (empty batch is skipped after Bug #4 fix)
	bat := batch.New([]string{"id"})
	vec := vector.NewVec(types.T_int32.ToType())
	_ = vector.AppendFixed(vec, int32(1), false, mp)
	bat.Vecs[0] = vec
	bat.SetRowCount(1)

	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: bat,
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

	// Create a non-empty batch (empty batch is skipped after Bug #4 fix)
	bat := batch.New([]string{"id"})
	vec := vector.NewVec(types.T_int32.ToType())
	_ = vector.AppendFixed(vec, int32(1), false, mp)
	bat.Vecs[0] = vec
	bat.SetRowCount(1)

	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: bat,
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

func TestDataProcessor_ProcessTailDone_BeginFailureRetainsBatches(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	beginErr := moerr.NewInternalError(ctx, "begin failure")
	h.sinker.setBeginError(beginErr)

	data := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to),
	}

	err := h.dp.ProcessChange(ctx, data)
	require.ErrorIs(t, err, beginErr)

	assert.NotNil(t, h.dp.insertAtmBatch)
	assert.Equal(t, []string{"begin"}, h.sinker.opsSnapshot())
	assert.Len(t, h.sinker.sinkCallsSnapshot(), 0)
}

func TestDataProcessor_ProcessTailDone_UpdatesActiveTransaction(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to1 := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to1)

	data1 := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to1),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data1))

	tracker := h.txnMgr.GetTracker()
	require.NotNil(t, tracker)

	to2 := types.BuildTS(3, 0)
	h.dp.SetTransactionRange(to1, to2)
	data2 := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{2}, to2),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data2))

	require.Equal(t, to2, h.txnMgr.GetTracker().GetToTs())
	assert.Equal(t, []string{"begin", "sink", "sink"}, h.sinker.opsSnapshot())
}

func TestDataProcessor_ProcessNoMoreData_CommitFailureRequiresRollback(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to),
	}))

	h.sinker.resetOps()
	commitErr := moerr.NewInternalError(ctx, "commit failure")
	h.sinker.setCommitError(commitErr)

	to2 := types.BuildTS(3, 0)
	h.dp.SetTransactionRange(to, to2)
	err := h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData})
	require.ErrorIs(t, err, commitErr)

	assert.True(t, h.txnMgr.GetTracker().NeedsRollback())
	assert.Equal(t, []string{"sink", "dummy", "commit", "dummy"}, h.sinker.opsSnapshot())
}

func TestDataProcessor_NoMoreData_HeartbeatUpdatesWatermark(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, true) // split snapshot, no txn

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)
	h.txnMgr.Reset()

	err := h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData})
	require.NoError(t, err)

	assert.True(t, h.update.updateCalled)

	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 1)
	assert.True(t, calls[0].noMoreData)
	assert.Equal(t, []string{"sink", "dummy"}, h.sinker.opsSnapshot())
}
