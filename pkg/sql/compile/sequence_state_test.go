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

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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

// The CTAS child process is built by NewTopProcess with a fresh SessionInfo,
// so without the handoff carry its transaction-local sequence allocations
// would see a nil obligation store (#30896; v12-review F5). The carried
// store is the session's same set: no clone, no restore, no clear.
func TestInternalSequenceStateCarriesObligationStore(t *testing.T) {
	parent := testutil.NewProcess(t)
	defer parent.Free()
	parent.InitSeq()
	set := process.NewSequenceObligationSet()
	parent.GetSessionInfo().SeqObligations = set

	ctx, handoff := attachInternalSequenceState(context.Background(), parent)
	require.Same(t, set, handoff.obligations)
	childCtx, consumed := consumeInternalSequenceState(ctx)
	require.Same(t, handoff, consumed)
	_, again := consumeInternalSequenceState(childCtx)
	require.Nil(t, again,
		"a consumed handoff must not be republished to nested metadata SQL")

	child := testutil.NewProcess(t)
	defer child.Free()
	initInternalSequenceState(child, "ctas_database", consumed)
	require.Same(t, set, child.GetSequenceObligationSet(),
		"CTAS population must allocate against the session's obligation store")

	// An obligation raised by the child's allocation is visible to the outer
	// transaction's pre-commit stamp through the parent's store.
	ob := process.SequenceObligation{RelID: 42, Db: "d", Table: "s",
		PassStart: 10, Floor: 12}
	require.NoError(t, child.GetSequenceObligationSet().Raise(ob))
	got, ok := parent.GetSequenceObligationSet().Get(42)
	require.True(t, ok)
	require.Equal(t, ob, got)

	// The store is not cloned or cleared by the handoff: the entry the child
	// raised survives the attach/init cycle intact.
	require.Equal(t, []process.SequenceObligation{ob},
		parent.GetSequenceObligationSet().Snapshot())

	// Nested metadata SQL receives no handoff; its process keeps the fresh
	// nil store, so an allocation there still fails loudly in Raise instead
	// of silently losing issued values.
	nested := testutil.NewProcess(t)
	defer nested.Free()
	initInternalSequenceState(nested, "metadata_database", nil)
	require.Nil(t, nested.GetSequenceObligationSet())
	require.Error(t, nested.GetSequenceObligationSet().Raise(ob))
}

// A parent whose session never seeded an obligation store (e.g. a session
// that has not hosted transaction-local generations) must hand a nil store
// down unaltered; the child must not synthesize one.
func TestInternalSequenceStateNilObligationStoreStaysNil(t *testing.T) {
	parent := testutil.NewProcess(t)
	defer parent.Free()
	parent.InitSeq()
	require.Nil(t, parent.GetSequenceObligationSet())

	ctx, handoff := attachInternalSequenceState(context.Background(), parent)
	require.Nil(t, handoff.obligations)
	childCtx, consumed := consumeInternalSequenceState(ctx)
	require.Same(t, handoff, consumed)
	_, again := consumeInternalSequenceState(childCtx)
	require.Nil(t, again,
		"a consumed handoff must not be republished to nested metadata SQL")

	child := testutil.NewProcess(t)
	defer child.Free()
	initInternalSequenceState(child, "ctas_database", consumed)
	require.Nil(t, child.GetSequenceObligationSet())
}

// Retry-transition obligation oracle (#30359 review): the compiler retry
// transition rewinds the failed attempt's workspace writes exactly like a
// frontend statement rollback, so a SETVAL/restart boundary the failed
// attempt staged must come back into enforcement there too. Issued-value
// obligations of the failed attempt stay recorded — envelope-restored
// protection only ever produces gaps, never reuse.
func TestRetryTransitionRestoresStagedSequenceObligations(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, txnOp := newTestTxnClientAndOp(ctrl)

	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = txnOp
	set := process.NewSequenceObligationSet()
	c.proc.GetSessionInfo().SeqObligations = set

	// The failed attempt issued values on a transaction-local generation,
	// then staged a SETVAL boundary mid-statement.
	require.NoError(t, set.Raise(process.SequenceObligation{
		RelID: 5, PassStart: 10, Floor: 12,
	}))
	set.StageRemoval(5)
	_, ok := set.Get(5)
	require.False(t, ok, "staged entries leave enforcement until resolved")

	var wait time.Duration
	require.NoError(t, c.prepareRetryTransition(&wait))
	ob, ok := set.Get(5)
	require.True(t, ok, "the retry transition must restore the staged boundary: its workspace reposition was rewound")
	require.Equal(t, uint64(10), ob.PassStart)
	require.Equal(t, uint64(12), ob.Floor)

	// The same transition envelope-merges an obligation raised after the
	// staged boundary (client-observed post-SETVAL emission), so both
	// sides' issued values stay protected across the retry.
	set.StageRemoval(5)
	require.NoError(t, set.Raise(process.SequenceObligation{
		RelID: 5, PassStart: 60, Floor: 63,
	}))
	require.NoError(t, c.prepareRetryTransition(&wait))
	ob, ok = set.Get(5)
	require.True(t, ok)
	require.Equal(t, uint64(10), ob.PassStart)
	require.Equal(t, uint64(63), ob.Floor)
	require.True(t, ob.IssuedContains(11))
	require.True(t, ob.IssuedContains(62))

	// A transition with nothing staged leaves issued obligations standing.
	require.NoError(t, c.prepareRetryTransition(&wait))
	ob, ok = set.Get(5)
	require.True(t, ok)
	require.Equal(t, uint64(63), ob.Floor)

	// A process without a session obligation store transitions silently.
	c2 := NewMockCompile(t)
	c2.proc.Base.TxnOperator = txnOp
	require.Nil(t, c2.proc.GetSequenceObligationSet())
	require.NoError(t, c2.prepareRetryTransition(&wait))
}

// Retry-transition retained-output discard oracle (#30359 review N1): a
// prepared-cursor EXECUTE retains its result for COM_STMT_FETCH without
// publishing it, so the failed generation's prefix would otherwise sit
// beside the retried generation's result. The transition invokes the
// armed discard hook exactly once per retry, and a nil hook (every
// non-cursor statement and every remote process — the hook is never
// serialized) transitions silently.
func TestRetryTransitionDiscardsRetainedOutput(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	_, txnOp := newTestTxnClientAndOp(ctrl)

	discarded := 0
	c := NewMockCompile(t)
	c.proc.Base.TxnOperator = txnOp
	c.proc.GetSessionInfo().RetryDiscardOutput = func() error { discarded++; return nil }

	var wait time.Duration
	require.NoError(t, c.prepareRetryTransition(&wait))
	require.Equal(t, 1, discarded, "exactly one discard per retry transition")
	require.NoError(t, c.prepareRetryTransition(&wait))
	require.Equal(t, 2, discarded, "each transition discards only its own failed generation")

	c2 := NewMockCompile(t)
	c2.proc.Base.TxnOperator = txnOp
	require.Nil(t, c2.proc.GetSessionInfo().RetryDiscardOutput)
	require.NoError(t, c2.prepareRetryTransition(&wait))
}
