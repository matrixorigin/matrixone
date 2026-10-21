// Copyright 2026 Matrix Origin
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

package compile

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type sequenceGateProbeContext struct {
	context.Context
	doneCalled chan struct{}
	once       sync.Once
}

func (c *sequenceGateProbeContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneCalled) })
	return c.Context.Done()
}

func TestSequenceStatementStateRestoresRetryBaseline(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	info := proc.GetSessionInfo()
	info.SeqCurValues[7] = "prior-curval"
	info.SeqAddValues[8] = "prior-pending"
	info.SeqDeleteKeys = []uint64{9}
	info.SeqLastValue[0] = "prior-lastval"

	c := &Compile{proc: proc, sequenceState: captureSequenceStatementState(proc)}
	// Simulate an attempt that allocated values and changed unrelated sequence
	// bookkeeping before a later operator requested transaction retry.
	info.SeqCurValues[7] = "mutated-curval"
	info.SeqCurValues[10] = "failed-curval"
	info.SeqAddValues[8] = "failed-pending"
	info.SeqAddValues[11] = "failed-new"
	info.SeqDeleteKeys = []uint64{12}
	info.SeqLastValue = nil

	c.restoreSequenceStatementState()
	require.Equal(t, map[uint64]string{7: "prior-curval"}, info.SeqCurValues)
	require.Equal(t, map[uint64]string{8: "prior-pending"}, info.SeqAddValues)
	require.Equal(t, []uint64{9}, info.SeqDeleteKeys)
	require.Equal(t, []string{"prior-lastval"}, info.SeqLastValue)

	// Restored containers are independent from the captured baseline and from
	// one another, so the next generation cannot mutate the snapshot itself.
	info.SeqCurValues[7] = "retry-curval"
	info.SeqAddValues[8] = "retry-pending"
	c.restoreSequenceStatementState()
	require.Equal(t, "prior-curval", info.SeqCurValues[7])
	require.Equal(t, "prior-pending", info.SeqAddValues[8])
}

func TestSequenceStatementRetryRestoresAfterQueuedWorkerQuiesces(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.InitSeq()
	info := proc.GetSessionInfo()
	info.SeqCurValues[7] = "prior-curval"
	info.SeqAddValues[8] = "prior-pending"
	info.SeqDeleteKeys = []uint64{9}
	info.SeqLastValue[0] = "prior-lastval"
	c := &Compile{proc: proc, sequenceState: captureSequenceStatementState(proc)}

	holder, err := proc.AcquireSequence(context.Background())
	require.NoError(t, err)
	var releaseHolderOnce sync.Once
	releaseHolder := func() { releaseHolderOnce.Do(holder) }
	defer releaseHolder()
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	defer cancelWaiter()
	waiterAdmission := make(chan struct{})
	waiterDone := make(chan error, 1)
	child := proc.NewContextChildProc(0)
	child.Ctx = &sequenceGateProbeContext{Context: waiterCtx, doneCalled: waiterAdmission}
	go func() {
		permit, acquireErr := child.AcquireSequence(child.Ctx)
		if acquireErr == nil {
			permit()
		}
		waiterDone <- acquireErr
	}()
	select {
	case <-waiterAdmission:
	case <-time.After(time.Second):
		t.Fatal("queued retry worker did not reach the sequence gate")
	}
	cancelWaiter()
	select {
	case err = <-waiterDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued retry worker did not quiesce after cancellation")
	}

	// The failed attempt may have changed every sequence-state container before
	// the retry boundary. Restore only after the queued worker has terminated;
	// the live gate itself is reused by the rebuilt attempt.
	info.SeqCurValues[7] = "failed-curval"
	info.SeqAddValues[8] = "failed-pending"
	info.SeqDeleteKeys = []uint64{12}
	info.SeqLastValue[0] = "failed-lastval"
	c.restoreSequenceStatementState()
	require.Equal(t, map[uint64]string{7: "prior-curval"}, info.SeqCurValues)
	require.Equal(t, map[uint64]string{8: "prior-pending"}, info.SeqAddValues)
	require.Equal(t, []uint64{9}, info.SeqDeleteKeys)
	require.Equal(t, []string{"prior-lastval"}, info.SeqLastValue)

	releaseHolder()
	followup, err := proc.AcquireSequence(context.Background())
	require.NoError(t, err)
	followup()
}

func TestInternalSequenceStateInitialization(t *testing.T) {
	for _, withEmptyParent := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty-parent=%t", withEmptyParent), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			var state *internalSequenceState
			if withEmptyParent {
				state = &internalSequenceState{}
			}
			initInternalSequenceState(proc, "executor_database", state)
			info := proc.GetSessionInfo()
			require.Equal(t, "executor_database", info.Database)
			require.NotNil(t, info.SeqCurValues)
			require.NotNil(t, info.SeqAddValues)
			require.Equal(t, []string{""}, info.SeqLastValue)
			info.SeqAddValues[1] = "first"
			info.SeqLastValue[0] = "first"
		})
	}
}

func TestInternalSequenceStateIsolatesNestedAttempts(t *testing.T) {
	parent := testutil.NewProcess(t)
	defer parent.Free()
	parent.InitSeq()
	info := parent.GetSessionInfo()
	info.SeqCurValues[7] = "prior-current"
	info.SeqAddValues[8] = "pending"
	info.SeqDeleteKeys = []uint64{9}
	info.SeqLastValue[0] = "prior-last"
	ctx, handoff := attachInternalSequenceState(context.Background(), parent)
	childCtx, consumed := consumeInternalSequenceState(ctx)
	require.Same(t, handoff, consumed)
	_, nested := consumeInternalSequenceState(childCtx)
	require.Nil(t, nested, "metadata SQL must not inherit the population handoff")

	child := testutil.NewProcess(t)
	defer child.Free()
	initInternalSequenceState(child, "child_database", consumed)
	childInfo := child.GetSessionInfo()
	require.Equal(t, info.SeqCurValues, childInfo.SeqCurValues)
	require.Equal(t, info.SeqAddValues, childInfo.SeqAddValues)
	require.Equal(t, info.SeqDeleteKeys, childInfo.SeqDeleteKeys)
	require.Equal(t, info.SeqLastValue, childInfo.SeqLastValue)
	baseline := captureSequenceStatementState(child)
	childInfo.SeqCurValues[7] = "abandoned-current"
	childInfo.SeqAddValues[8] = "abandoned-pending"
	childInfo.SeqDeleteKeys[0] = 10
	childInfo.SeqLastValue[0] = "abandoned-last"
	require.Equal(t, "prior-current", info.SeqCurValues[7])
	require.Equal(t, "pending", info.SeqAddValues[8])
	require.Equal(t, []uint64{9}, info.SeqDeleteKeys)
	require.Equal(t, []string{"prior-last"}, info.SeqLastValue)
	require.Nil(t, handoff.output, "unpublished child attempt must leave no output")

	// The existing compiler retry boundary restores the child's own baseline.
	c := &Compile{proc: child, sequenceState: baseline}
	c.restoreSequenceStatementState()
	require.Equal(t, "pending", childInfo.SeqAddValues[8])
	require.Equal(t, []uint64{9}, childInfo.SeqDeleteKeys)
	childInfo.SeqAddValues[11] = "successful"
	childInfo.SeqLastValue[0] = "successful"
	success := captureSequenceStatementState(child)
	handoff.output = &success
	handoff.output.apply(parent)
	childInfo.SeqAddValues[11] = "later-child-mutation"
	childInfo.SeqDeleteKeys[0] = 12
	childInfo.SeqLastValue[0] = "later-child-mutation"
	require.Equal(t, map[uint64]string{8: "pending", 11: "successful"}, info.SeqAddValues)
	require.Equal(t, []uint64{9}, info.SeqDeleteKeys)
	require.Equal(t, []string{"successful"}, info.SeqLastValue)
	info.SeqAddValues[11] = "later-parent-mutation"
	require.Equal(t, "successful", handoff.output.addValues[11])
}
