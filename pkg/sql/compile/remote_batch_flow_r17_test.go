// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package compile

// Focused unit legs for the r17 run-scoped ownership barrier (issue
// #25590 round-52 review): the no-window accounting flow an admitted
// run late-registers on a fresh stream sequences and accounts every
// emitted batch without imposing any negotiated credit window, drains
// through the same registry envelope the negotiated protocol uses, and
// stamps zero accepted credits so the wire bytes stay identical to a
// stream that never negotiated any. The barrier-universality legs at
// the settle level live in remote_dispatch_transport_r29_test.go; the
// two-CN public run is covered by the BVT corpus.

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// TestIssue25590RunScopedFlowSequencesWithoutWindowR17 proves the flow
// only sequences: every reserve stamps a fresh sequence immediately -
// even far beyond the negotiated credit caps and under a request
// context that is already cancelled, where a windowed flow's reserve
// must refuse to wait - acks retire the outstanding accounting, and
// the drain passes exactly at a fully acked flow and waits otherwise.
func TestIssue25590RunScopedFlowSequencesWithoutWindowR17(t *testing.T) {
	ctx := context.Background()
	flow := newRunScopedPipelineBatchFlow()
	require.NotNil(t, flow)

	// Deterministic no-window discriminator: a cancelled request context
	// refused by the windowed flow at saturation (nothing may wait on
	// it) is irrelevant to the run-scoped flow - it stamps anyway.
	saturated := newPipelineBatchFlow(pipelineBatchCreditCount, pipelineBatchCreditBytes)
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	for i := 0; i < int(pipelineBatchCreditCount); i++ {
		_, err := saturated.reserve(cancelledCtx, ctx, 1)
		require.NoError(t, err)
	}
	_, err := saturated.reserve(cancelledCtx, ctx, 1)
	require.Error(t, err,
		"a windowed flow refuses to wait on a cancelled context once its window is full")

	reserved := make([]uint64, 0, int(pipelineBatchCreditCount)+4)
	for i := uint32(0); i < pipelineBatchCreditCount+4; i++ {
		seq, rErr := flow.reserve(cancelledCtx, ctx, 1)
		require.NoError(t, rErr,
			"the run-scoped flow never blocks and never consults a context that is cancelled")
		reserved = append(reserved, seq)
		require.Equal(t, uint64(i+1), seq,
			"sequences carve in emission order")
	}
	require.Len(t, flow.pending, int(pipelineBatchCreditCount)+4,
		"every emitted batch is outstanding until its ack")

	// The drain waits while an ack is absent: under a dead context the
	// waiting terminal is observable as the context error, never nil.
	deadCtx, deadCancel := context.WithCancel(ctx)
	deadCancel()
	require.Error(t, flow.waitUntilDrained(deadCtx, ctx, nil),
		"the drain cannot pass while any outstanding batch is un-acked")

	// Acks retire the accounting in order; after the last ack the same
	// drain passes.
	for _, seq := range reserved {
		require.NoError(t, flow.acknowledge(seq))
	}
	require.Empty(t, flow.pending)
	require.NoError(t, flow.waitUntilDrained(ctx, ctx, nil),
		"a fully acked flow is drained")
}

// TestIssue25590RunScopedFlowAcceptedStampsZeroR17 pins the wire-shape
// property the plain fresh stream keeps from base: the late flow
// advertises no credits (accepted() is zero-zero), so the response
// messages it carries are byte-identical to a stream with no flow at
// all - the barrier adds accounting, never a window.
func TestIssue25590RunScopedFlowAcceptedStampsZeroR17(t *testing.T) {
	count, bytes := newRunScopedPipelineBatchFlow().accepted()
	require.Zero(t, count)
	require.Zero(t, bytes)

	windowed := newPipelineBatchFlow(pipelineBatchCreditCount, pipelineBatchCreditBytes)
	wCount, wBytes := windowed.accepted()
	require.Equal(t, pipelineBatchCreditCount, wCount,
		"the negotiated flow still carries its requested window for the negotiated protocol")
	require.Equal(t, pipelineBatchCreditBytes, wBytes)
}

// TestIssue25590LateFlowUsesTheLifecycleEnvelopeR17 drives the late
// registration through the SAME registry envelope the negotiated
// stream uses: registerPipelineStreamLifecycle stores the flow by
// (session, id), handlePipelineBatchAck acks it from the transport
// path, abortPipelineBatchFlow stops it by key (the StopSending
// terminal), and removal-by-key turns a flushed late ack into a benign
// miss instead of a poison.
func TestIssue25590LateFlowUsesTheLifecycleEnvelopeR17(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cs := mock_morpc.NewMockClientSession(ctrl)
	cs.EXPECT().Close().Times(0)

	flow := newRunScopedPipelineBatchFlow()
	lifecycle, err := registerPipelineStreamLifecycle(cs, 77, flow)
	require.NoError(t, err)
	require.Equal(t, flow, lifecycle.batchFlow)

	_, err = flow.reserve(ctx, ctx, 1)
	require.NoError(t, err)

	// The transport ack path drives the registered flow by key.
	ack := &pipeline.Message{}
	ack.SetID(77)
	ack.BatchAckSequence = 1
	require.NoError(t, handlePipelineBatchAck(ack, cs))
	require.Empty(t, flow.pending, "the acked sequence retired through the registry envelope")

	// The StopSending terminal aborts by key.
	_, err = flow.reserve(ctx, ctx, 1)
	require.NoError(t, err)
	abortPipelineBatchFlow(cs, 77, process.ErrPipelineStopped)
	require.True(t, flow.wasStoppedByReceiver(),
		"the internal stop reads as receiver-driven for the settlement's non-transfer mapping")
	require.Empty(t, flow.pending)
	ackTwo := &pipeline.Message{}
	ackTwo.SetID(77)
	ackTwo.BatchAckSequence = 2
	require.NoError(t, handlePipelineBatchAck(ackTwo, cs),
		"a late ack on the aborted flow stays benign down the negotiated envelope's own path")

	// Removal closes the barrier's registration window; an ack flushed
	// after the message handling returns is a benign miss.
	lifecycle.remove()
	late := &pipeline.Message{}
	late.SetID(77)
	late.BatchAckSequence = 2
	require.NoError(t, handlePipelineBatchAck(late, cs),
		"after removal an already flushed ACK decodes as a benign miss - it must not poison the connection")
}
