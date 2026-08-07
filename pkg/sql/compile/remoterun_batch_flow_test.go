// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/stretchr/testify/require"
)

func TestPipelineBatchFlowNegotiation(t *testing.T) {
	require.Nil(t, newPipelineBatchFlow(0, 1))
	require.Nil(t, newPipelineBatchFlow(1, 0))

	flow := newPipelineBatchFlow(pipelineBatchCreditCount+1, pipelineBatchCreditBytes+1)
	count, bytes := flow.accepted()
	require.Equal(t, pipelineBatchCreditCount, count)
	require.Equal(t, pipelineBatchCreditBytes, bytes)

	var nilFlow *pipelineBatchFlow
	count, bytes = nilFlow.accepted()
	require.Zero(t, count)
	require.Zero(t, bytes)
}

func TestPipelineBatchFlowBoundsAndReleasesCredits(t *testing.T) {
	flow := newPipelineBatchFlow(1, 10)
	seq, err := flow.reserve(context.Background(), context.Background(), 8)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = flow.reserve(canceled, context.Background(), 1)
	require.ErrorIs(t, err, context.Canceled)

	require.NoError(t, flow.acknowledge(seq))
	seq, err = flow.reserve(context.Background(), context.Background(), 20)
	require.NoError(t, err, "one oversized batch must make progress by itself")
	require.Equal(t, uint64(2), seq)
	require.NoError(t, flow.acknowledge(seq))
	require.NoError(t, flow.waitUntilDrained(context.Background(), context.Background(), nil))
}

func TestPipelineBatchFlowCumulativeAckAndRollback(t *testing.T) {
	flow := newPipelineBatchFlow(8, 1024)
	first, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)
	second, err := flow.reserve(context.Background(), context.Background(), 20)
	require.NoError(t, err)
	third, err := flow.reserve(context.Background(), context.Background(), 30)
	require.NoError(t, err)

	flow.rollback(second)
	require.NoError(t, flow.acknowledge(third))
	require.Equal(t, uint64(1), first)
	flow.mu.Lock()
	require.Empty(t, flow.pending)
	require.Zero(t, flow.bytes)
	flow.mu.Unlock()
}

func TestPipelineBatchFlowDrainWaitHonorsCancellation(t *testing.T) {
	flow := newPipelineBatchFlow(1, 1024)
	seq, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t,
		flow.waitUntilDrained(canceled, context.Background(), nil),
		context.Canceled,
		"terminal response must not bypass an outstanding batch")

	require.NoError(t, flow.acknowledge(seq))
	require.NoError(t,
		flow.waitUntilDrained(context.Background(), context.Background(), nil),
		"the ACK must release the terminal-response barrier")
}

func TestPipelineBatchFlowRejectsAckAheadOfSentData(t *testing.T) {
	flow := newPipelineBatchFlow(1, 1024)
	require.Error(t, flow.acknowledge(1))
}

func TestPipelineBatchFlowWaitsForCreditAndObservesConnectionClose(t *testing.T) {
	flow := newPipelineBatchFlow(1, 1024)
	seq, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)

	connectionCtx, closeConnection := context.WithCancel(context.Background())
	closeConnection()
	_, err = flow.reserve(context.Background(), connectionCtx, 10)
	require.Error(t, err)

	flow.rollback(seq)
	flow.rollback(seq)
	flow.rollback(0)
	require.NoError(t, flow.acknowledge(0))
	require.NoError(t, flow.acknowledge(seq))
}

func TestPipelineBatchFlowAbortWakesWaitersAndReleasesAccounting(t *testing.T) {
	flow := newPipelineBatchFlow(1, 1024)
	seq, err := flow.reserve(context.Background(), context.Background(), 10)
	require.NoError(t, err)

	reserveStarted := make(chan struct{})
	reserveDone := make(chan error, 1)
	go func() {
		close(reserveStarted)
		_, err := flow.reserve(context.Background(), context.Background(), 1)
		reserveDone <- err
	}()
	waitStarted := make(chan struct{})
	waitDone := make(chan error, 1)
	go func() {
		close(waitStarted)
		waitDone <- flow.waitUntilDrained(context.Background(), context.Background(), nil)
	}()
	<-reserveStarted
	<-waitStarted

	firstCause := errors.New("stop sending")
	flow.abort(firstCause)
	flow.abort(errors.New("later abort"))

	select {
	case err := <-reserveDone:
		require.ErrorIs(t, err, firstCause)
	case <-time.After(time.Second):
		t.Fatal("abort did not wake a blocked credit reservation")
	}
	select {
	case err := <-waitDone:
		require.ErrorIs(t, err, firstCause)
	case <-time.After(time.Second):
		t.Fatal("abort did not wake the terminal-response drain barrier")
	}

	flow.mu.Lock()
	require.Empty(t, flow.pending)
	require.Zero(t, flow.bytes)
	flow.mu.Unlock()
	require.NoError(t, flow.acknowledge(seq), "an ACK already in flight must be harmless after abort")
}

func TestHandlePipelineBatchAck(t *testing.T) {
	t.Run("unknown lifecycle is a harmless late ack", func(t *testing.T) {
		session := &lifecycleTestSession{ctx: context.Background()}
		require.NoError(t, handlePipelineBatchAck(
			&pipeline.Message{Id: 301, BatchAckSequence: 1}, session))
		require.Zero(t, session.closeCalls)
	})

	t.Run("ack without negotiation poisons the session", func(t *testing.T) {
		session := &lifecycleTestSession{ctx: context.Background()}
		lifecycle, err := registerPipelineStreamLifecycle(session, 302, nil)
		require.NoError(t, err)
		t.Cleanup(lifecycle.remove)

		err = handlePipelineBatchAck(
			&pipeline.Message{Id: 302, BatchAckSequence: 1}, session)
		require.Error(t, err)
		require.Equal(t, 1, session.closeCalls)
	})

	t.Run("negotiated ack releases credit", func(t *testing.T) {
		session := &lifecycleTestSession{ctx: context.Background()}
		lifecycle, err := registerPipelineStreamLifecycle(
			session, 303, newPipelineBatchFlow(1, 1024))
		require.NoError(t, err)
		t.Cleanup(lifecycle.remove)

		seq, err := lifecycle.batchFlow.reserve(context.Background(), context.Background(), 10)
		require.NoError(t, err)
		require.NoError(t, handlePipelineBatchAck(
			&pipeline.Message{Id: 303, BatchAckSequence: seq}, session))
		require.NoError(t, lifecycle.batchFlow.waitUntilDrained(
			context.Background(), context.Background(), nil))
		require.Zero(t, session.closeCalls)
	})

	t.Run("ack ahead of sent data poisons the session", func(t *testing.T) {
		session := &lifecycleTestSession{ctx: context.Background()}
		lifecycle, err := registerPipelineStreamLifecycle(
			session, 304, newPipelineBatchFlow(1, 1024))
		require.NoError(t, err)
		t.Cleanup(lifecycle.remove)

		err = handlePipelineBatchAck(
			&pipeline.Message{Id: 304, BatchAckSequence: 1}, session)
		require.Error(t, err)
		require.Equal(t, 1, session.closeCalls)
	})
}
