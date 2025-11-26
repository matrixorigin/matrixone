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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// mockClientSession is a simple mock implementation of morpc.ClientSession for testing
type mockClientSession struct {
	remoteAddr string
}

func (m *mockClientSession) RemoteAddress() string {
	return m.remoteAddr
}

func (m *mockClientSession) SessionCtx() context.Context {
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
