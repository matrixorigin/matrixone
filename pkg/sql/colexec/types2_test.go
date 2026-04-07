// Copyright 2021 Matrix Origin
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

package colexec

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// mockClientSession is a simple mock implementation of morpc.ClientSession for testing
type mockClientSession struct {
	remoteAddr string
	ctx        context.Context
}

func (m *mockClientSession) RemoteAddress() string {
	return m.remoteAddr
}

func (m *mockClientSession) SessionCtx() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}

func (m *mockClientSession) Write(ctx context.Context, message morpc.Message) error {
	return nil
}

func (m *mockClientSession) AsyncWrite(response morpc.Message) error {
	return nil
}

func (m *mockClientSession) Close() error {
	return nil
}

func (m *mockClientSession) CreateCache(ctx context.Context, cacheID uint64) (morpc.MessageCache, error) {
	return nil, nil
}

func (m *mockClientSession) DeleteCache(cacheID uint64) {}

func (m *mockClientSession) GetCache(cacheID uint64) (morpc.MessageCache, error) {
	return nil, nil
}

// TestCancelPipelineSending_ShouldNotCancelDispatchReceiver tests that
// CancelPipelineSending should NOT cancel dispatch receivers when StopSending message arrives.
// This is a fix for the bug where StopSending message was incorrectly used to cancel
// dispatch receivers, causing "remote receiver is already done" errors.
func TestCancelPipelineSending_ShouldNotCancelDispatchReceiver(t *testing.T) {
	// Setup: Create a server
	srv := NewServer(nil)
	require.NotNil(t, srv)

	// Create a mock client session
	session := &mockClientSession{remoteAddr: "test-addr"}
	streamID := uint64(1)

	// Create a dispatch receiver (isDispatch=true)
	receiverUid := uuid.Must(uuid.NewV7())
	dispatchReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID,
		Uid:          receiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	// Step 1: Register the dispatch receiver
	srv.RecordDispatchPipeline(session, streamID, dispatchReceiver)

	// Verify the receiver is registered and ReceiverDone is false
	require.False(t, dispatchReceiver.ReceiverDone, "ReceiverDone should be false after registration")

	// Step 2: Call CancelPipelineSending (simulating StopSending message)
	// This should NOT cancel the dispatch receiver
	srv.CancelPipelineSending(session, streamID)

	// Step 3: Verify that ReceiverDone is still false
	// This is the key test - before the fix, ReceiverDone would be set to true here
	require.False(t, dispatchReceiver.ReceiverDone,
		"ReceiverDone should remain false after CancelPipelineSending for dispatch receiver. "+
			"StopSending message should not cancel dispatch receivers that are used to receive data.")

	// Verify the receiver is still in the map
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	record, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists, "Receiver should still be in the map")
	require.NotNil(t, record.receiver, "Record should have a receiver")
	require.Equal(t, receiverUid, record.receiver.Uid, "Receiver UID should match")
	require.True(t, record.isDispatch, "Record should be marked as dispatch")
	require.False(t, record.alreadyDone, "Record should not be marked as done")
}

// TestRecordDispatchPipeline tests RecordDispatchPipeline function
func TestRecordDispatchPipeline(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	session := &mockClientSession{remoteAddr: "test-addr"}
	streamID := uint64(2)

	// Test 1: Normal registration
	receiverUid := uuid.Must(uuid.NewV7())
	dispatchReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID,
		Uid:          receiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	srv.RecordDispatchPipeline(session, streamID, dispatchReceiver)

	// Verify the receiver is registered
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	record, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists, "Receiver should be registered")
	require.False(t, record.alreadyDone, "Record should not be marked as done")
	require.True(t, record.isDispatch, "Record should be marked as dispatch")
	require.Equal(t, receiverUid, record.receiver.Uid, "Receiver UID should match")

	// Test 2: Registration when alreadyDone=true and receiver==nil (should clean stale record)
	streamID2 := uint64(3)
	srv.receivedRunningPipeline.Lock()
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[generateRecordKey(session, streamID2)] = runningPipelineInfo{
		alreadyDone: true,
		receiver:    nil,
	}
	srv.receivedRunningPipeline.Unlock()

	receiverUid2 := uuid.Must(uuid.NewV7())
	dispatchReceiver2 := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID2,
		Uid:          receiverUid2,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	srv.RecordDispatchPipeline(session, streamID2, dispatchReceiver2)

	// Verify stale record is cleaned and new receiver is registered
	key2 := generateRecordKey(session, streamID2)
	srv.receivedRunningPipeline.Lock()
	record2, exists2 := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key2]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists2, "Receiver should be registered")
	require.False(t, record2.alreadyDone, "Record should not be marked as done")
	require.Equal(t, receiverUid2, record2.receiver.Uid, "Receiver UID should match")

	// Test 3: Registration when alreadyDone=true and receiver.Uid != dispatchReceiver.Uid (should clean stale record)
	streamID3 := uint64(4)
	oldReceiverUid := uuid.Must(uuid.NewV7())
	oldReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID3,
		Uid:          oldReceiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}
	srv.receivedRunningPipeline.Lock()
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[generateRecordKey(session, streamID3)] = runningPipelineInfo{
		alreadyDone: true,
		receiver:    oldReceiver,
	}
	srv.receivedRunningPipeline.Unlock()

	newReceiverUid := uuid.Must(uuid.NewV7())
	newDispatchReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID3,
		Uid:          newReceiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	srv.RecordDispatchPipeline(session, streamID3, newDispatchReceiver)

	// Verify stale record is cleaned and new receiver is registered
	key3 := generateRecordKey(session, streamID3)
	srv.receivedRunningPipeline.Lock()
	record3, exists3 := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key3]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists3, "Receiver should be registered")
	require.False(t, record3.alreadyDone, "Record should not be marked as done")
	require.Equal(t, newReceiverUid, record3.receiver.Uid, "Receiver UID should match")

	// Test 4: Registration when alreadyDone=true and receiver.Uid == dispatchReceiver.Uid (should set ReceiverDone=true)
	streamID4 := uint64(5)
	sameReceiverUid := uuid.Must(uuid.NewV7())
	sameReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID4,
		Uid:          sameReceiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}
	srv.receivedRunningPipeline.Lock()
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[generateRecordKey(session, streamID4)] = runningPipelineInfo{
		alreadyDone: true,
		receiver:    sameReceiver,
	}
	srv.receivedRunningPipeline.Unlock()

	// Try to register the same receiver again
	srv.RecordDispatchPipeline(session, streamID4, sameReceiver)

	// Verify ReceiverDone is set to true
	require.True(t, sameReceiver.ReceiverDone, "ReceiverDone should be set to true when alreadyDone=true and same UID")
}

// TestRecordBuiltPipeline tests RecordBuiltPipeline function
func TestRecordBuiltPipeline(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	session := &mockClientSession{remoteAddr: "test-addr"}
	streamID := uint64(6)

	// Test 1: Normal registration
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	ctx, cancel := context.WithCancel(context.Background())
	proc.Base.GetContextBase().BuildQueryCtx(ctx)
	_ = cancel // cancel is not used but needed for context

	srv.RecordBuiltPipeline(session, streamID, proc)

	// Verify the pipeline is registered
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	record, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists, "Pipeline should be registered")
	require.False(t, record.alreadyDone, "Record should not be marked as done")
	require.False(t, record.isDispatch, "Record should not be marked as dispatch")
	require.NotNil(t, record.queryCancel, "QueryCancel should be set")

	// Test 2: Registration when alreadyDone=true (should return early)
	streamID2 := uint64(7)
	srv.receivedRunningPipeline.Lock()
	srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[generateRecordKey(session, streamID2)] = runningPipelineInfo{
		alreadyDone: true,
	}
	srv.receivedRunningPipeline.Unlock()

	proc2 := &process.Process{}
	proc2.Base = &process.BaseProcess{}
	ctx2, cancel2 := context.WithCancel(context.Background())
	proc2.Base.GetContextBase().BuildQueryCtx(ctx2)
	_ = cancel2 // cancel2 is not used but needed for context

	srv.RecordBuiltPipeline(session, streamID2, proc2)

	// Verify the record is still the canceled one
	key2 := generateRecordKey(session, streamID2)
	srv.receivedRunningPipeline.Lock()
	record2, exists2 := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key2]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists2, "Record should still exist")
	require.True(t, record2.alreadyDone, "Record should still be marked as done")
	require.NotNil(t, proc2.GetQueryContextError(), "Process should be canceled when a tombstone already exists")
}

// TestCancelPipelineSending tests CancelPipelineSending function
func TestCancelPipelineSending(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	session := &mockClientSession{remoteAddr: "test-addr"}
	streamID := uint64(8)

	// Test 1: Cancel when record exists and isDispatch=false (should cancel)
	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	ctx, cancel := context.WithCancel(context.Background())
	proc.Base.GetContextBase().BuildQueryCtx(ctx)

	srv.RecordBuiltPipeline(session, streamID, proc)

	// Cancel the pipeline
	srv.CancelPipelineSending(session, streamID)

	// Verify context is canceled
	err := proc.GetQueryContextError()
	require.NotNil(t, err, "Query context should be canceled")
	_ = cancel // cancel is not used but needed for context

	// Test 2: Cancel when record exists and isDispatch=true (should not cancel)
	streamID2 := uint64(9)
	receiverUid := uuid.Must(uuid.NewV7())
	dispatchReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID2,
		Uid:          receiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	srv.RecordDispatchPipeline(session, streamID2, dispatchReceiver)

	// Cancel should not affect dispatch receiver
	srv.CancelPipelineSending(session, streamID2)

	require.False(t, dispatchReceiver.ReceiverDone, "Dispatch receiver should not be canceled")

	// Test 3: Cancel before record exists should create a tombstone for later registration.
	streamID3 := uint64(10)
	srv.CancelPipelineSending(session, streamID3)

	key3 := generateRecordKey(session, streamID3)
	srv.receivedRunningPipeline.Lock()
	record3, exists3 := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key3]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists3, "Cancel before registration should leave a tombstone")
	require.True(t, record3.alreadyDone, "Tombstone should mark the pipeline already done")
	require.Nil(t, record3.receiver, "Tombstone should not carry a dispatch receiver")
	require.Nil(t, record3.queryCancel, "Tombstone should not carry a cancel func yet")
}

// TestRemoveRelatedPipeline tests RemoveRelatedPipeline function
func TestRemoveRelatedPipeline(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	session := &mockClientSession{remoteAddr: "test-addr"}
	streamID := uint64(10)

	// Register a pipeline
	receiverUid := uuid.Must(uuid.NewV7())
	dispatchReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID,
		Uid:          receiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	srv.RecordDispatchPipeline(session, streamID, dispatchReceiver)

	// Verify the receiver is registered
	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	_, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	srv.receivedRunningPipeline.Unlock()
	require.True(t, exists, "Receiver should be registered")

	// Remove the pipeline
	srv.RemoveRelatedPipeline(session, streamID)

	// Verify the pipeline is removed
	srv.receivedRunningPipeline.Lock()
	_, existsAfter := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	srv.receivedRunningPipeline.Unlock()
	require.False(t, existsAfter, "Pipeline should be removed")

	// Test removing non-existent pipeline (should not panic)
	srv.RemoveRelatedPipeline(session, streamID+1)
}

func TestCancelPipelineSending_TombstoneAllowsDispatchRegistration(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	session := &mockClientSession{remoteAddr: "test-addr"}
	streamID := uint64(11)

	srv.CancelPipelineSending(session, streamID)

	receiverUid := uuid.Must(uuid.NewV7())
	dispatchReceiver := &process.WrapCs{
		ReceiverDone: false,
		MsgId:        streamID,
		Uid:          receiverUid,
		Cs:           session,
		Err:          make(chan error, 1),
	}

	srv.RecordDispatchPipeline(session, streamID, dispatchReceiver)

	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	record, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	srv.receivedRunningPipeline.Unlock()

	require.True(t, exists, "Dispatch registration should replace the stale tombstone")
	require.False(t, record.alreadyDone, "Dispatch registration should clear the tombstone state")
	require.True(t, record.isDispatch, "Dispatch receiver should register normally after stale tombstone cleanup")
	require.Equal(t, receiverUid, record.receiver.Uid, "Dispatch receiver should be recorded")
	require.False(t, dispatchReceiver.ReceiverDone, "Dispatch receiver should not be spuriously canceled")
}

func TestCancelPipelineSending_TombstoneCleansOnSessionClose(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	session := &mockClientSession{remoteAddr: "test-addr", ctx: ctx}
	streamID := uint64(12)
	streamID2 := uint64(13)

	srv.CancelPipelineSending(session, streamID)
	srv.CancelPipelineSending(session, streamID2)

	key := generateRecordKey(session, streamID)
	key2 := generateRecordKey(session, streamID2)
	srv.receivedRunningPipeline.Lock()
	_, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	_, exists2 := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key2]
	waiterCount := len(srv.receivedRunningPipeline.sessionCleanupWaiters)
	srv.receivedRunningPipeline.Unlock()
	require.True(t, exists, "Cancel before registration should create a tombstone")
	require.True(t, exists2, "Second stop on the same session should also create a tombstone")
	require.Equal(t, 1, waiterCount, "A session should register only one cleanup waiter")

	cancel()

	require.Eventually(t, func() bool {
		srv.receivedRunningPipeline.Lock()
		defer srv.receivedRunningPipeline.Unlock()
		_, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
		_, exists2 := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key2]
		return !exists && !exists2 && len(srv.receivedRunningPipeline.sessionCleanupWaiters) == 0
	}, time.Second, 10*time.Millisecond, "Session close should clean all tombstones for the session")
}

func TestCleanupPipelinesForSession_CancelsRegisteredPipelines(t *testing.T) {
	srv := NewServer(nil)
	require.NotNil(t, srv)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &mockClientSession{remoteAddr: "test-addr", ctx: ctx}
	streamID := uint64(14)

	proc := &process.Process{}
	proc.Base = &process.BaseProcess{}
	queryCtx, queryCancel := context.WithCancel(context.Background())
	defer queryCancel()
	proc.Base.GetContextBase().BuildQueryCtx(queryCtx)

	srv.RecordBuiltPipeline(session, streamID, proc)
	srv.cleanupPipelinesForSession(session)

	key := generateRecordKey(session, streamID)
	srv.receivedRunningPipeline.Lock()
	_, exists := srv.receivedRunningPipeline.fromRpcClientToRelatedPipeline[key]
	waiterCount := len(srv.receivedRunningPipeline.sessionCleanupWaiters)
	srv.receivedRunningPipeline.Unlock()

	require.False(t, exists, "Session cleanup should remove registered pipeline records")
	require.Equal(t, 0, waiterCount, "Session cleanup should remove the waiter registration")
	require.NotNil(t, proc.GetQueryContextError(), "Session cleanup should cancel registered non-dispatch pipelines")
}
