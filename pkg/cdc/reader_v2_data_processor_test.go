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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockDataProcessorSinker for testing DataProcessor
type mockDataProcessorSinker struct {
	*mockSinker // Embed the mockSinker from txn_manager_test
	sinkCalls   []*DecoderOutput
}

// targetOwnershipProbeSinker models the one-owner target lock without SQL. A
// failed nonblocking acquisition makes the test fail through Sinker.Error.
type targetOwnershipProbeSinker struct {
	*mockDataProcessorSinker
	token chan struct{}
	owns  bool
}

func newTargetOwnershipProbeSinker(token chan struct{}) *targetOwnershipProbeSinker {
	return &targetOwnershipProbeSinker{
		mockDataProcessorSinker: &mockDataProcessorSinker{mockSinker: &mockSinker{}},
		token:                   token,
	}
}

func (s *targetOwnershipProbeSinker) SendBegin() {
	s.mockDataProcessorSinker.SendBegin()
	select {
	case <-s.token:
		s.owns = true
	default:
		s.err = moerr.NewInternalErrorNoCtx("target ownership is still held")
	}
}

func (s *targetOwnershipProbeSinker) releaseTargetOwnership() error {
	s.releaseCalled = true
	if s.owns {
		s.owns = false
		s.token <- struct{}{}
	}
	return s.releaseErr
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

func (s *dataProcessorRecordingSinker) sinkCallsSnapshot() []*DecoderOutput {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]*DecoderOutput, len(s.sinkCalls))
	copy(cp, s.sinkCalls)
	return cp
}

// transactionalSnapshotSinker models target transaction visibility so retry
// tests can distinguish staged snapshot rows from durably committed rows.
type transactionalSnapshotSinker struct {
	*recordingSinker
	staged  map[int32]struct{}
	durable map[int32]struct{}
}

func newTransactionalSnapshotSinker() *transactionalSnapshotSinker {
	return &transactionalSnapshotSinker{
		recordingSinker: newRecordingSinker(),
		durable:         make(map[int32]struct{}),
	}
}

func cloneSnapshotKeys(src map[int32]struct{}) map[int32]struct{} {
	dst := make(map[int32]struct{}, len(src))
	for key := range src {
		dst[key] = struct{}{}
	}
	return dst
}

func (s *transactionalSnapshotSinker) SendBegin() {
	s.recordingSinker.SendBegin()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.staged = cloneSnapshotKeys(s.durable)
	}
}

func (s *transactionalSnapshotSinker) Sink(_ context.Context, data *DecoderOutput) {
	s.mu.Lock()
	s.ops = append(s.ops, "sink")
	switch data.outputTyp {
	case OutputTypeSnapshot:
		if data.checkpointBat == nil {
			break
		}
		for row := 0; row < data.checkpointBat.RowCount(); row++ {
			key := vector.GetFixedAtNoTypeCheck[int32](data.checkpointBat.Vecs[0], row)
			s.staged[key] = struct{}{}
		}
	case OutputTypeTail:
		if data.deleteAtmBatch != nil {
			for _, bat := range data.deleteAtmBatch.Batches {
				for row := 0; row < bat.RowCount(); row++ {
					key := vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], row)
					delete(s.staged, key)
				}
			}
		}
		if data.insertAtmBatch != nil {
			for _, bat := range data.insertAtmBatch.Batches {
				for row := 0; row < bat.RowCount(); row++ {
					key := vector.GetFixedAtNoTypeCheck[int32](bat.Vecs[0], row)
					s.staged[key] = struct{}{}
				}
			}
		}
	}
	s.mu.Unlock()
	data.Close()
}

func (s *transactionalSnapshotSinker) SendCommit() {
	s.recordingSinker.SendCommit()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err == nil {
		s.durable = cloneSnapshotKeys(s.staged)
	}
}

func (s *transactionalSnapshotSinker) SendRollback() {
	s.recordingSinker.SendRollback()
	s.mu.Lock()
	s.staged = cloneSnapshotKeys(s.durable)
	s.mu.Unlock()
}

func (s *transactionalSnapshotSinker) durableKeys() map[int32]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneSnapshotKeys(s.durable)
}

func (s *transactionalSnapshotSinker) resetTarget() {
	s.mu.Lock()
	s.durable = make(map[int32]struct{})
	s.staged = nil
	s.mu.Unlock()
}

type slowDataProcessorSinker struct {
	*dataProcessorRecordingSinker
	delay time.Duration
}

func newSlowDataProcessorSinker(delay time.Duration) *slowDataProcessorSinker {
	return &slowDataProcessorSinker{
		dataProcessorRecordingSinker: newDataProcessorRecordingSinker(),
		delay:                        delay,
	}
}

func (s *slowDataProcessorSinker) SendBegin() {
	time.Sleep(s.delay)
	s.record("begin")
}

func (s *slowDataProcessorSinker) SendCommit() {
	time.Sleep(s.delay)
	s.record("commit")
}

func (s *slowDataProcessorSinker) SendRollback() {
	time.Sleep(s.delay)
	s.record("rollback")
}

func (s *slowDataProcessorSinker) SendDummy() {
	time.Sleep(s.delay / 2)
	s.record("dummy")
}

func (s *slowDataProcessorSinker) Sink(ctx context.Context, data *DecoderOutput) {
	time.Sleep(s.delay / 2)
	s.dataProcessorRecordingSinker.Sink(ctx, data)
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
	defer func() {
		for _, output := range sinker.sinkCalls {
			output.Close()
		}
	}()
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
	defer dp.Cleanup()

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
	assert.False(t, sinker.beginCalled, "source collection must not hold a target transaction")
	assert.Empty(t, sinker.sinkCalls)
	assert.True(t, dp.hasPendingSnapshotGroup())

	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))
	assert.True(t, sinker.beginCalled)
	assert.Equal(t, 2, len(sinker.sinkCalls))
	assert.Equal(t, OutputTypeSnapshot, sinker.sinkCalls[0].outputTyp)
	assert.True(t, sinker.sinkCalls[1].noMoreData)
}

func TestSplitSnapshotSourceWaitDoesNotBlockReplacementTargetOwnership(t *testing.T) {
	ctx := context.Background()
	targetToken := make(chan struct{}, 1)
	targetToken <- struct{}{}
	oldSinker := newTargetOwnershipProbeSinker(targetToken)
	oldTxnMgr := NewTransactionManager(oldSinker, newMockWatermarkUpdater(), 1, "task1", "db1", "table1")
	mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	pool := fileservice.NewPool(1, func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() }, func(p *types.Packer) { p.Close() })
	oldProcessor := NewDataProcessor(oldSinker, oldTxnMgr, mp, pool, 1, 0, 1, 0, true,
		1, "task1", "db1", "table1")
	defer oldProcessor.Cleanup()
	to := types.BuildTS(2, 0)
	oldProcessor.SetTransactionRange(types.TS{}, to)
	require.NoError(t, oldProcessor.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: buildBatch(t, mp, []int32{1}, to),
	}))
	require.False(t, oldSinker.owns)
	require.Nil(t, oldTxnMgr.GetTracker())

	// Model the old collector waiting indefinitely for its next source batch.
	// The replacement must acquire target ownership before that wait is released.
	collectorWaiting := make(chan struct{})
	releaseCollector := make(chan struct{})
	collectorDone := make(chan struct{})
	go func() {
		close(collectorWaiting)
		<-releaseCollector
		close(collectorDone)
	}()
	<-collectorWaiting

	replacementSinker := newTargetOwnershipProbeSinker(targetToken)
	replacementTxnMgr := NewTransactionManager(
		replacementSinker, newMockWatermarkUpdater(), 1, "task1", "db1", "table1")
	require.NoError(t, replacementTxnMgr.BeginTransaction(ctx, types.TS{}, to))
	require.True(t, replacementSinker.owns)
	select {
	case <-collectorDone:
		t.Fatal("old source collector was released before replacement ownership")
	default:
	}
	require.NoError(t, replacementTxnMgr.EnsureCleanup(ctx))
	close(releaseCollector)
	<-collectorDone
}

func TestDataProcessor_SplitSnapshotCommitsBoundedGroupsWithoutWatermark(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, true)
	t.Cleanup(func() {
		for _, output := range h.sinker.sinkCallsSnapshot() {
			output.Close()
		}
	})
	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	for i := 0; i < 9; i++ {
		require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
			Type:        ChangeTypeSnapshot,
			InsertBatch: buildBatch(t, h.mp, []int32{int32(i + 1)}, to),
		}))
	}
	assert.Nil(t, h.txnMgr.GetTracker(), "the ninth batch is staged without holding target ownership")
	assert.False(t, h.update.updateCalled)
	expectedOps := []string{"begin"}
	for i := 0; i < initialSnapshotTxnBatchLimit; i++ {
		expectedOps = append(expectedOps, "sink")
	}
	expectedOps = append(expectedOps, "commit", "dummy")
	assert.Equal(t, expectedOps, h.sinker.opsSnapshot())

	require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))
	assert.True(t, h.update.updateCalled)
	assert.Nil(t, h.txnMgr.GetTracker())
	expectedOps = append(expectedOps, "begin", "sink", "sink", "dummy", "commit", "dummy")
	assert.Equal(t, expectedOps, h.sinker.opsSnapshot())
}

func TestSplitSnapshotBackpressureFlushesBeforePermitWait(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	limiter := newInitialSnapshotLimiter(1, 1, 1, 100, func() (uint64, bool) {
		return 400, true
	})
	permit, err := limiter.acquire(ctx)
	require.NoError(t, err)
	permit.ObserveBatchBytes(100)

	mp, err := mpool.NewMPool(t.Name(), 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	pool := fileservice.NewPool(1, func() *types.Packer { return types.NewPacker() },
		func(p *types.Packer) { p.Reset() }, func(p *types.Packer) { p.Close() })
	sinker := newTransactionalSnapshotSinker()
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	dp := NewDataProcessor(sinker, txnMgr, mp, pool, 1, 0, 1, 0, true,
		1, "task1", "db1", "table1")
	defer dp.Cleanup()
	to := types.BuildTS(2, 0)
	dp.SetTransactionRange(types.TS{}, to)
	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
		Type:           ChangeTypeSnapshot,
		InsertBatch:    buildBatch(t, mp, []int32{1}, to),
		snapshotPermit: permit,
	}))
	require.True(t, dp.hasPendingSnapshotGroup())
	require.Nil(t, txnMgr.GetTracker(), "staging source data acquired target ownership")

	stream := &TableChangeStream{initialSnapshotLimiter: limiter, dataProcessor: dp}
	stream.initialSyncPending.Store(true)
	next, err := stream.acquireInitialSnapshotPermit(ctx)
	require.NoError(t, err)
	require.NotNil(t, next)
	next.Release()
	require.False(t, dp.hasPendingSnapshotGroup())
	require.Nil(t, txnMgr.GetTracker())
	require.Equal(t, []string{"begin", "sink", "commit", "dummy"}, sinker.opsSnapshot())
	require.Equal(t, map[int32]struct{}{1: {}}, sinker.durableKeys())
}

func TestDataProcessor_PartialSnapshotRetryReplaysStableEpoch(t *testing.T) {
	ctx := context.Background()
	mp, err := mpool.NewMPool("snapshot_retry_atomicity", 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(*types.Packer) {},
	)
	sinker := newTransactionalSnapshotSinker()
	updater := newMockWatermarkUpdater()
	txnMgr := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")
	dp := NewDataProcessor(
		sinker, txnMgr, mp, packerPool,
		1, 0, 1, 0, true,
		1, "task1", "db1", "table1",
	)
	defer dp.Cleanup()

	from := types.TS{}
	firstTo := types.BuildTS(2, 0)
	dp.SetTransactionRange(from, firstTo)
	for key := int32(1); key <= 9; key++ {
		require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
			Type:        ChangeTypeSnapshot,
			InsertBatch: buildBatch(t, mp, []int32{key}, firstTo),
		}))
	}
	expectedCommittedGroup := make(map[int32]struct{}, initialSnapshotTxnBatchLimit)
	for key := int32(1); key <= initialSnapshotTxnBatchLimit; key++ {
		expectedCommittedGroup[key] = struct{}{}
	}
	assert.Equal(t, expectedCommittedGroup, sinker.durableKeys())
	assert.False(t, updater.updateCalled, "intermediate group advanced the watermark")

	readErr := moerr.NewInternalError(ctx, "snapshot read failed")
	sinker.setError(readErr)
	require.ErrorIs(t, dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeSnapshot}), readErr)
	require.NoError(t, txnMgr.EnsureCleanup(ctx))
	dp.Cleanup()
	sinker.ClearError()
	assert.Equal(t, expectedCommittedGroup, sinker.durableKeys(), "rollback changed a committed group")

	// A retry must use the same durable source epoch, so it contains keys 1 and
	// 2 even if they were deleted or changed after that epoch. Those later
	// mutations belong to the incremental interval after the final watermark.
	dp.SetTransactionRange(from, firstTo)
	retryKeys := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: buildBatch(t, mp, retryKeys, firstTo),
	}))
	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

	expected := make(map[int32]struct{}, len(retryKeys))
	for _, key := range retryKeys {
		expected[key] = struct{}{}
	}
	assert.Equal(t, expected, sinker.durableKeys())
	assert.True(t, updater.updateCalled)

	// Apply the source DELETE(1), DELETE(2), INSERT(20) from the interval after
	// the stable epoch. The target must converge to the current source image,
	// not merely to the replayed snapshot.
	tailTo := types.BuildTS(3, 0)
	dp.SetTransactionRange(firstTo, tailTo)
	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, mp, []int32{20}, tailTo),
		DeleteBatch: buildBatch(t, mp, []int32{1, 2}, tailTo),
	}))
	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

	delete(expected, 1)
	delete(expected, 2)
	expected[20] = struct{}{}
	assert.Equal(t, expected, sinker.durableKeys())
}

func TestLateDiscoveredTableStableEpochRestartConverges(t *testing.T) {
	ctx := context.Background()
	epochExecutor := newSnapshotEpochTestExecutor()
	epochUpdater := NewCDCWatermarkUpdater(t.Name(), epochExecutor)
	key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}

	// Model a wildcard task whose creation timestamp is already older than
	// retained history. The late table must use its own current transaction
	// snapshot, never the task-global creation timestamp.
	taskCreation := types.BuildTS(1, 0)
	lateTableSnapshot := types.BuildTS(100, 7)
	epoch, err := epochUpdater.GetOrCreateInitialSnapshotEpoch(
		ctx, key, 42, lateTableSnapshot)
	require.NoError(t, err)
	require.Equal(t, lateTableSnapshot, epoch)
	require.NotEqual(t, taskCreation, epoch)

	mp, err := mpool.NewMPool("late_table_snapshot_retry", 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(*types.Packer) {},
	)
	sinker := newTransactionalSnapshotSinker()
	firstWatermark := newMockWatermarkUpdater()
	firstTxnMgr := NewTransactionManager(sinker, firstWatermark, 1, "task1", "db1", "table1")
	first := NewDataProcessor(
		sinker, firstTxnMgr, mp, packerPool,
		1, 0, 1, 0, true,
		1, "task1", "db1", "table1",
	)
	first.SetTransactionRange(types.TS{}, epoch)
	for value := int32(1); value <= 9; value++ {
		require.NoError(t, first.ProcessChange(ctx, &ChangeData{
			Type:        ChangeTypeSnapshot,
			InsertBatch: buildBatch(t, mp, []int32{value}, epoch),
		}))
	}
	require.Len(t, sinker.durableKeys(), initialSnapshotTxnBatchLimit)
	require.False(t, firstWatermark.updateCalled)

	readErr := moerr.NewInternalError(ctx, "late table snapshot read failed")
	sinker.setError(readErr)
	require.ErrorIs(t, first.ProcessChange(ctx, &ChangeData{Type: ChangeTypeSnapshot}), readErr)
	require.NoError(t, firstTxnMgr.EnsureCleanup(ctx))
	first.Cleanup()
	sinker.ClearError()

	// A new executor process proposes a newer timestamp, but the table-generation
	// row must return the original epoch that already owns durable target groups.
	restartedEpochUpdater := NewCDCWatermarkUpdater(t.Name()+"-restart", epochExecutor)
	retryEpoch, err := restartedEpochUpdater.GetOrCreateInitialSnapshotEpoch(
		ctx, key, 42, types.BuildTS(900, 9))
	require.NoError(t, err)
	require.Equal(t, epoch, retryEpoch)

	retryWatermark := newMockWatermarkUpdater()
	retryTxnMgr := NewTransactionManager(sinker, retryWatermark, 1, "task1", "db1", "table1")
	retry := NewDataProcessor(
		sinker, retryTxnMgr, mp, packerPool,
		1, 0, 1, 0, true,
		1, "task1", "db1", "table1",
	)
	retry.SetTransactionRange(types.TS{}, retryEpoch)
	snapshotKeys := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9}
	require.NoError(t, retry.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: buildBatch(t, mp, snapshotKeys, retryEpoch),
	}))
	require.NoError(t, retry.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

	watermark, err := retryWatermark.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, retryEpoch, watermark)

	// DELETE(1) and the primary-key change 2 -> 20 happened after the stable
	// epoch. Tail catch-up must remove both old keys and publish a live watermark.
	tailTo := types.BuildTS(901, 0)
	retry.SetTransactionRange(retryEpoch, tailTo)
	require.NoError(t, retry.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, mp, []int32{20}, tailTo),
		DeleteBatch: buildBatch(t, mp, []int32{1, 2}, tailTo),
	}))
	require.NoError(t, retry.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

	expected := map[int32]struct{}{3: {}, 4: {}, 5: {}, 6: {}, 7: {}, 8: {}, 9: {}, 20: {}}
	require.Equal(t, expected, sinker.durableKeys())
	watermark, err = retryWatermark.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, tailTo, watermark)
}

func TestRecreatedTableFreshOwnerResetsRetiredGeneration(t *testing.T) {
	ctx := context.Background()
	epochExecutor := newSnapshotEpochTestExecutor()
	epochUpdater := NewCDCWatermarkUpdater(t.Name(), epochExecutor)
	key := &WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "table1"}

	gen11Epoch, reset, err := epochUpdater.GetOrCreateInitialSnapshotEpochForGeneration(
		ctx, key, 11, types.BuildTS(100, 1))
	require.NoError(t, err)
	require.False(t, reset)

	mp, err := mpool.NewMPool("recreated_table_generation", 0, mpool.NoFixed)
	require.NoError(t, err)
	t.Cleanup(func() { mpool.DeleteMPool(mp) })
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(*types.Packer) {},
	)
	sinker := newTransactionalSnapshotSinker()
	gen11Watermark := newMockWatermarkUpdater()
	gen11Txn := NewTransactionManager(sinker, gen11Watermark, 1, "task1", "db1", "table1")
	gen11 := NewDataProcessor(sinker, gen11Txn, mp, packerPool, 1, 0, 1, 0, true, 1, "task1", "db1", "table1")
	gen11.SetTransactionRange(types.TS{}, gen11Epoch)
	for value := int32(1); value <= 9; value++ {
		require.NoError(t, gen11.ProcessChange(ctx, &ChangeData{
			Type: ChangeTypeSnapshot, InsertBatch: buildBatch(t, mp, []int32{value}, gen11Epoch),
		}))
	}
	require.Len(t, sinker.durableKeys(), initialSnapshotTxnBatchLimit)
	require.False(t, gen11Watermark.updateCalled)

	// The first CN disappears and the logical source table is recreated. A
	// fresh detector has no IdChanged memory, so only the retired durable epoch
	// can require the target reset.
	freshUpdater := NewCDCWatermarkUpdater(t.Name()+"-fresh", epochExecutor)
	gen12Epoch, reset, err := freshUpdater.GetOrCreateInitialSnapshotEpochForGeneration(
		ctx, key, 12, types.BuildTS(200, 1))
	require.NoError(t, err)
	require.True(t, reset)
	if reset {
		sinker.resetTarget()
	}

	gen12Watermark := newMockWatermarkUpdater()
	gen12Txn := NewTransactionManager(sinker, gen12Watermark, 1, "task1", "db1", "table1")
	gen12 := NewDataProcessor(sinker, gen12Txn, mp, packerPool, 1, 0, 1, 0, true, 1, "task1", "db1", "table1")
	gen12.SetTransactionRange(types.TS{}, gen12Epoch)
	require.NoError(t, gen12.ProcessChange(ctx, &ChangeData{
		Type: ChangeTypeSnapshot, InsertBatch: buildBatch(t, mp, []int32{20, 30}, gen12Epoch),
	}))
	require.NoError(t, gen12.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))
	require.Equal(t, map[int32]struct{}{20: {}, 30: {}}, sinker.durableKeys())
	watermark, err := gen12Watermark.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, gen12Epoch, watermark)
}

func TestDataProcessor_SnapshotGroupBoundaries(t *testing.T) {
	dp := &DataProcessor{initSnapshotSplitTxn: true}

	dp.snapshotTxnBatches = initialSnapshotTxnBatchLimit - 1
	assert.False(t, dp.shouldRotateSnapshotTxn(1))
	dp.snapshotTxnBatches++
	assert.True(t, dp.shouldRotateSnapshotTxn(1))

	dp.snapshotTxnBatches = 1
	dp.snapshotTxnBytes = uint64(initialSnapshotTxnByteLimit - 1)
	assert.False(t, dp.shouldRotateSnapshotTxn(1))
	assert.True(t, dp.shouldRotateSnapshotTxn(2))

	// One wide engine batch is the indivisible minimum and must not trigger an
	// empty commit before its transaction begins.
	dp.snapshotTxnBatches = 0
	dp.snapshotTxnBytes = 0
	assert.False(t, dp.shouldRotateSnapshotTxn(uint64(initialSnapshotTxnByteLimit+1)))

	dp.initSnapshotSplitTxn = false
	dp.snapshotTxnBatches = initialSnapshotTxnBatchLimit
	assert.False(t, dp.shouldRotateSnapshotTxn(uint64(initialSnapshotTxnByteLimit)))
}

func TestDataProcessor_IntermediateSnapshotCommitFailureDoesNotAdvanceWatermark(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, true)
	t.Cleanup(func() {
		for _, output := range h.sinker.sinkCallsSnapshot() {
			output.Close()
		}
	})
	h.dp.SetTransactionRange(types.TS{}, types.BuildTS(2, 0))

	for i := 0; i < initialSnapshotTxnBatchLimit-1; i++ {
		require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
			Type:        ChangeTypeSnapshot,
			InsertBatch: buildBatch(t, h.mp, []int32{int32(i + 1)}, types.BuildTS(2, 0)),
		}))
	}

	commitErr := moerr.NewInternalError(ctx, "commit connection failure")
	h.sinker.setCommitError(commitErr)
	next := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: buildBatch(t, h.mp, []int32{8}, types.BuildTS(2, 0)),
	}
	require.ErrorIs(t, h.dp.ProcessChange(ctx, next), commitErr)
	next.Clean(h.mp)
	assert.False(t, h.update.updateCalled)
	require.NotNil(t, h.txnMgr.GetTracker())
	assert.True(t, h.txnMgr.GetTracker().NeedsRollback())
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

	assert.Nil(t, data.InsertBatch, "AtomicBatch owns the appended batch after Begin fails")
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
	fenceChecks := 0
	h.txnMgr.SetOwnerFence(NewOwnerFence(func(context.Context) error {
		fenceChecks++
		return nil
	}))

	err := h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData})
	require.NoError(t, err)

	assert.True(t, h.update.updateCalled)
	assert.Zero(t, fenceChecks, "an empty round has no synchronous target effect to fence")

	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 1)
	assert.True(t, calls[0].noMoreData)
	assert.Equal(t, []string{"sink", "dummy"}, h.sinker.opsSnapshot())
}

// TestDataProcessor_CommitFail_EnsureCleanup_ThenRecover verifies that when commit fails
// and tracker requires rollback, EnsureCleanup performs rollback and the next round succeeds.
func TestDataProcessor_CommitFail_EnsureCleanup_ThenRecover(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	// Round 1: begin transaction with a TailDone
	from := types.BuildTS(1, 0)
	to1 := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to1)
	require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to1),
	}))
	require.NotNil(t, h.txnMgr.GetTracker())

	// Inject commit failure on NoMoreData
	h.sinker.resetOps()
	commitErr := moerr.NewInternalError(ctx, "commit failure")
	h.sinker.setCommitError(commitErr)
	to2 := types.BuildTS(3, 0)
	h.dp.SetTransactionRange(to1, to2)
	err := h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData})
	require.ErrorIs(t, err, commitErr)
	require.True(t, h.txnMgr.GetTracker().NeedsRollback())
	// No rollback yet; EnsureCleanup will handle it

	// EnsureCleanup should rollback and clear NeedsRollback
	require.NoError(t, h.txnMgr.EnsureCleanup(ctx))
	require.False(t, h.txnMgr.GetTracker().NeedsRollback())
	ops := h.sinker.opsSnapshot()
	require.Contains(t, ops, "clear")
	require.Contains(t, ops, "rollback")
	require.Contains(t, ops, "dummy")

	// Round 2: clear error and confirm recovery path succeeds
	h.sinker.resetOps()
	h.sinker.setError(nil)
	h.sinker.setCommitError(nil)
	// Reset transaction manager state to start a fresh transaction in next round
	h.txnMgr.Reset()

	// Start a fresh range and process TailDone then NoMoreData
	to3 := types.BuildTS(4, 0)
	h.dp.SetTransactionRange(to2, to3)
	require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{2}, to3),
	}))
	require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

	// Verify we saw a begin and a commit, and no rollback in this successful round
	ops = h.sinker.opsSnapshot()
	hasBegin := false
	hasCommit := false
	hasRollback := false
	for _, op := range ops {
		switch op {
		case "begin":
			hasBegin = true
		case "commit":
			hasCommit = true
		case "rollback":
			hasRollback = true
		}
	}
	require.True(t, hasBegin, "recovery round should begin")
	require.True(t, hasCommit, "recovery round should commit")
	require.False(t, hasRollback, "recovery round should not rollback")
}

// TestDataProcessor_RepeatedCommitFailures_EnsureCleanup_Idempotent runs multiple cycles
// of commit failure -> EnsureCleanup -> recovery, to verify cleanup remains correct and idempotent.
func TestDataProcessor_RepeatedCommitFailures_EnsureCleanup_Idempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)

	for i := 0; i < 3; i++ {
		// Begin via TailDone
		h.dp.SetTransactionRange(from, to)
		require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
			Type:        ChangeTypeTailDone,
			InsertBatch: buildBatch(t, h.mp, []int32{int32(i + 1)}, to),
		}))
		require.NotNil(t, h.txnMgr.GetTracker())

		// Inject commit failure on NoMoreData
		h.sinker.resetOps()
		commitErr := moerr.NewInternalError(ctx, "commit failure")
		h.sinker.setCommitError(commitErr)
		next := (&to).Next()
		h.dp.SetTransactionRange(to, next)
		err := h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData})
		require.ErrorIs(t, err, commitErr)

		// EnsureCleanup must rollback once
		require.NoError(t, h.txnMgr.EnsureCleanup(ctx))
		require.False(t, h.txnMgr.GetTracker().NeedsRollback())
		ops := h.sinker.opsSnapshot()
		rollbackCount := 0
		for _, op := range ops {
			if op == "rollback" {
				rollbackCount++
			}
		}
		require.Equal(t, 1, rollbackCount, "each failure cycle should trigger exactly one rollback")

		// Clear errors and reset transaction manager
		h.sinker.resetOps()
		h.sinker.setCommitError(nil)
		h.txnMgr.Reset()

		// Recovery round
		h.dp.SetTransactionRange(to, next)
		require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
			Type:        ChangeTypeTailDone,
			InsertBatch: buildBatch(t, h.mp, []int32{9}, next),
		}))
		require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

		ops = h.sinker.opsSnapshot()
		// Recovery round should not trigger rollback
		for _, op := range ops {
			require.NotEqual(t, "rollback", op, "recovery round should not rollback")
		}

		// Bump to next window
		from = to
		to = next
	}
}

// TestDataProcessor_RandomizedSequence_WithDelays_NoDeadlock verifies that with
// randomized sequence of Snapshot/TailWip/TailDone/NoMoreData and a slow sinker,
// the processor finishes quickly without deadlocks and preserves basic invariants.
func TestDataProcessor_RandomizedSequence_WithDelays_NoDeadlock(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	sinker := newSlowDataProcessorSinker(3 * time.Millisecond)
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

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	dp.SetTransactionRange(from, to)

	// Prepare randomized sequence
	r := rand.New(rand.NewSource(42))
	typesSeq := make([]ChangeType, 0, 20)
	candidates := []ChangeType{ChangeTypeSnapshot, ChangeTypeTailWip, ChangeTypeTailDone}
	for i := 0; i < 10; i++ {
		typesSeq = append(typesSeq, candidates[r.Intn(len(candidates))])
	}
	// Ensure termination
	typesSeq = append(typesSeq, ChangeTypeNoMoreData)

	// Process sequentially (DataProcessor not guaranteed goroutine-safe),
	// but with slow sinker to simulate interleaving/latency.
	for _, typ := range typesSeq {
		switch typ {
		case ChangeTypeSnapshot:
			b := buildBatch(t, mp, []int32{1}, to)
			require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
				Type:        ChangeTypeSnapshot,
				InsertBatch: b,
			}))
		case ChangeTypeTailWip:
			b := buildBatch(t, mp, []int32{1}, to)
			require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
				Type:        ChangeTypeTailWip,
				InsertBatch: b,
			}))
		case ChangeTypeTailDone:
			// bump toTs to simulate progress
			from = to
			to = (&to).Next()
			dp.SetTransactionRange(from, to)
			b := buildBatch(t, mp, []int32{1}, to)
			require.NoError(t, dp.ProcessChange(ctx, &ChangeData{
				Type:        ChangeTypeTailDone,
				InsertBatch: b,
			}))
		}
	}
	require.NoError(t, dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData}))

	// Invariants:
	ops := sinker.opsSnapshot()
	begin := 0
	commit := 0
	for _, op := range ops {
		if op == "begin" {
			begin++
		}
		if op == "commit" {
			commit++
		}
	}
	require.LessOrEqual(t, begin, 1, "should not start multiple transactions in this simple randomized run")
	require.LessOrEqual(t, commit, 1, "should not commit multiple times in this simple randomized run")
}

// TestDataProcessor_Cleanup_Concurrent tests concurrent Cleanup calls
// Should be idempotent and not panic
func TestDataProcessor_Cleanup_Concurrent(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
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

	// Concurrently call Cleanup
	const numGoroutines = 20
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			dp.Cleanup()
		}()
	}

	wg.Wait()

	// Batches should be cleaned up
	require.Nil(t, dp.insertAtmBatch)
	require.Nil(t, dp.deleteAtmBatch)
}

// TestDataProcessor_SinkerErrorRecovery tests that after sinker error is cleared,
// processing can continue normally
func TestDataProcessor_SinkerErrorRecovery(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	// Set sinker error
	sinkerErr := moerr.NewInternalError(ctx, "sinker error")
	h.sinker.setError(sinkerErr)

	// Try to process - should fail immediately
	data := &ChangeData{
		Type:        ChangeTypeSnapshot,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to),
	}
	err := h.dp.ProcessChange(ctx, data)
	require.Error(t, err)
	require.ErrorIs(t, err, sinkerErr)

	// Clear error
	h.sinker.ClearError()

	// Process should succeed now
	err = h.dp.ProcessChange(ctx, data)
	require.NoError(t, err)
	require.Nil(t, data.InsertBatch, "sinker owns the snapshot batch after transfer")

	// Verify sinker was called
	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 1)
	require.Equal(t, OutputTypeSnapshot, calls[0].outputTyp)
}

// TestDataProcessor_MultipleTailDone_SameTransaction tests that multiple TailDone
// batches update the same transaction's toTs correctly
func TestDataProcessor_MultipleTailDone_SameTransaction(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to1 := types.BuildTS(2, 0)
	to2 := types.BuildTS(3, 0)
	to3 := types.BuildTS(4, 0)

	h.dp.SetTransactionRange(from, to1)

	// First TailDone - should begin transaction
	data1 := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to1),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data1))

	tracker := h.txnMgr.GetTracker()
	require.NotNil(t, tracker)
	require.Equal(t, to1, tracker.GetToTs())

	// Second TailDone - should update toTs to to2
	h.dp.SetTransactionRange(to1, to2)
	data2 := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{2}, to2),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data2))

	require.Equal(t, to2, tracker.GetToTs())

	// Third TailDone - should update toTs to to3
	h.dp.SetTransactionRange(to2, to3)
	data3 := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{3}, to3),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data3))

	require.Equal(t, to3, tracker.GetToTs())

	// Verify all data was sunk
	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 3)
	for i, call := range calls {
		require.Equal(t, OutputTypeTail, call.outputTyp)
		require.NotNil(t, call.insertAtmBatch)
		// Each TailDone batch is independent - batches are cleared after each sink
		require.Equal(t, int64(1), int64(call.insertAtmBatch.RowCount()), "each TailDone batch should have 1 row (batch %d)", i+1)
	}

	// Verify only one begin
	ops := h.sinker.opsSnapshot()
	beginCount := 0
	for _, op := range ops {
		if op == "begin" {
			beginCount++
		}
	}
	require.Equal(t, 1, beginCount, "only one begin should occur for multiple TailDone batches")
}

// TestDataProcessor_TailWipThenTailDone_AccumulatesCorrectly tests that TailWip
// accumulates data and TailDone sends it correctly
func TestDataProcessor_TailWipThenTailDone_AccumulatesCorrectly(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	// Multiple TailWip - should accumulate
	data1 := &ChangeData{
		Type:        ChangeTypeTailWip,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data1))

	data2 := &ChangeData{
		Type:        ChangeTypeTailWip,
		InsertBatch: buildBatch(t, h.mp, []int32{2}, to),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data2))

	// Verify batches are accumulating
	require.NotNil(t, h.dp.insertAtmBatch)
	require.Equal(t, 2, h.dp.insertAtmBatch.RowCount())

	// TailDone - should send accumulated data
	data3 := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{3}, to),
	}
	require.NoError(t, h.dp.ProcessChange(ctx, data3))

	// Verify all data was sunk (1+2+3 = 6 rows total)
	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 1)
	require.Equal(t, OutputTypeTail, calls[0].outputTyp)
	require.NotNil(t, calls[0].insertAtmBatch)
	require.Equal(t, 3, calls[0].insertAtmBatch.RowCount(), "should have accumulated all TailWip + TailDone rows")

	// Batches should be cleared after sink
	require.Nil(t, h.dp.insertAtmBatch)
	require.Nil(t, h.dp.deleteAtmBatch)
}

func TestDataProcessor_TailWipRangeSinksOnce(t *testing.T) {
	const rowCount = int(objectio.BlockMaxRows)
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	for i := 0; i < rowCount; i++ {
		to := types.BuildTS(int64(i+2), 0)
		h.dp.SetTransactionRange(from, to)
		changeType := ChangeTypeTailWip
		if i == rowCount-1 {
			changeType = ChangeTypeTailDone
		}
		require.NoError(t, h.dp.ProcessChange(ctx, &ChangeData{
			Type:        changeType,
			InsertBatch: buildBatch(t, h.mp, []int32{int32(i)}, to),
		}))
		from = to
	}

	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 1)
	require.Equal(t, OutputTypeTail, calls[0].outputTyp)
	require.NotNil(t, calls[0].insertAtmBatch)
	require.Equal(t, rowCount, calls[0].insertAtmBatch.RowCount())
	calls[0].Close()
}

// TestDataProcessor_BeginFailure_BatchesRetained tests that when BeginTransaction
// fails, batches are retained for retry
func TestDataProcessor_BeginFailure_BatchesRetained(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	// Set begin to fail
	beginErr := moerr.NewInternalError(ctx, "begin failure")
	h.sinker.setBeginError(beginErr)

	// Process TailDone - should fail at BeginTransaction
	data := &ChangeData{
		Type:        ChangeTypeTailDone,
		InsertBatch: buildBatch(t, h.mp, []int32{1}, to),
	}
	err := h.dp.ProcessChange(ctx, data)
	require.Error(t, err)
	require.ErrorIs(t, err, beginErr)

	// Batches should be retained for retry
	require.NotNil(t, h.dp.insertAtmBatch)
	require.Equal(t, 1, h.dp.insertAtmBatch.RowCount())

	// No sink calls should occur
	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 0)

	// Verify begin was attempted but failed
	ops := h.sinker.opsSnapshot()
	require.Contains(t, ops, "begin", "begin should be attempted")
	require.NotContains(t, ops, "sink", "sink should not be called when begin fails")
}

// TestDataProcessor_NoMoreData_WithoutActiveTransaction_HeartbeatOnly tests that
// NoMoreData without active transaction only sends heartbeat and updates watermark
func TestDataProcessor_NoMoreData_WithoutActiveTransaction_HeartbeatOnly(t *testing.T) {
	ctx := context.Background()
	h := newDataProcessorHarness(t, false)

	from := types.BuildTS(1, 0)
	to := types.BuildTS(2, 0)
	h.dp.SetTransactionRange(from, to)

	// No active transaction (Reset)
	h.txnMgr.Reset()

	// Process NoMoreData
	err := h.dp.ProcessChange(ctx, &ChangeData{Type: ChangeTypeNoMoreData})
	require.NoError(t, err)

	// Should send heartbeat and update watermark
	calls := h.sinker.sinkCallsSnapshot()
	require.Len(t, calls, 1)
	require.True(t, calls[0].noMoreData)

	ops := h.sinker.opsSnapshot()
	require.Contains(t, ops, "sink", "should send heartbeat")
	require.Contains(t, ops, "dummy", "should send dummy")
	require.NotContains(t, ops, "commit", "should not commit when no active transaction")

	// Watermark should be updated
	require.True(t, h.update.updateCalled)
}
