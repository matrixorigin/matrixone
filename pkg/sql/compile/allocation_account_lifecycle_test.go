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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/connector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	groupop "github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergeorder"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/mergetop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/order"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/product"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/top"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRetainingOperatorsActivateAllocationLifecycle(t *testing.T) {
	group := groupop.NewArgument()
	defer group.Release()
	_, ownsAllocation := any(group).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)

	localOrder := order.NewArgument()
	defer localOrder.Release()
	_, ownsAllocation = any(localOrder).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)

	mergeGroup := groupop.NewArgumentMergeGroup()
	defer mergeGroup.Release()
	_, ownsAllocation = any(mergeGroup).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)

	order := mergeorder.NewArgument()
	defer order.Release()
	_, ownsAllocation = any(order).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)

	topN := top.NewArgument()
	defer topN.Release()
	_, ownsAllocation = any(topN).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)

	mergeTopN := mergetop.NewArgument()
	defer mergeTopN.Release()
	_, ownsAllocation = any(mergeTopN).(executionAllocationAccountOwner)
	require.True(t, ownsAllocation)
}

func TestJoinAllocationLifecycleErrorsPreservesSingle(t *testing.T) {
	primary := moerr.NewDuplicateEntryNoCtx("duplicate", "primary")
	secondary := errors.New("cleanup failed")

	require.Same(t, primary, joinAllocationLifecycleErrors(primary, nil))
	require.Same(t, secondary, joinAllocationLifecycleErrors(nil, secondary))
	require.Same(t, primary, joinAllocationLifecycleErrors(primary, primary))

	joined := joinAllocationLifecycleErrors(primary, secondary)
	require.ErrorIs(t, joined, primary)
	require.ErrorIs(t, joined, secondary)
}

type allocationLifecycleErrorOperator struct {
	*colexec.MockOperator
	err     error
	account *mpool.AllocationAccount
}

type allocationLifecycleOwnerOperator struct {
	*colexec.MockOperator
	account               *mpool.AllocationAccount
	failSet               bool
	failClear             bool
	panicClear            bool
	clears                int
	released              bool
	releaseSawLiveAccount bool
}

func (op *allocationLifecycleOwnerOperator) Release() {
	op.released = true
	op.releaseSawLiveAccount = op.account != nil
}

func (op *allocationLifecycleOwnerOperator) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if op.failSet {
		return mpool.ErrAllocationAccountMismatch
	}
	if op.account != nil && op.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	op.account = account
	return nil
}

func (op *allocationLifecycleOwnerOperator) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if op.panicClear {
		panic("test allocation owner clear panic")
	}
	if op.account == nil {
		return nil
	}
	if op.failClear {
		return mpool.ErrAllocationAccountInvariant
	}
	if op.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	op.account = nil
	op.clears++
	return nil
}

func TestStatementAllocationAttemptOwnerTeardownPanicIsTerminalFailure(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	owner := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
		panicClear:   true,
	}
	c.scopes = []*Scope{{RootOp: owner}}

	_, err = c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	err = c.finishAllocationAccountAttempt()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.Len(t, exported, 1)
	require.Equal(
		t,
		mpool.AllocationAccountTerminalInvariantFailure,
		exported[0].State,
	)
	require.Zero(t, registry.LiveAllocationMetadata())
}

func (op *allocationLifecycleErrorOperator) Call(
	*process.Process,
) (vm.CallResult, error) {
	return vm.CancelResult, op.err
}

func (op *allocationLifecycleErrorOperator) SetAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if op.account != nil && op.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	op.account = account
	return nil
}

func (op *allocationLifecycleErrorOperator) ClearAllocationAccount(
	account *mpool.AllocationAccount,
) error {
	if op.account != account {
		return mpool.ErrAllocationAccountMismatch
	}
	op.account = nil
	return nil
}

type allocationLifecycleTestController struct{}

func (*allocationLifecycleTestController) AcquireAllocationCapacity(uint64) error {
	return nil
}

func (*allocationLifecycleTestController) ReleaseAllocationCapacity(uint64) {}

func newRunLifecycleCompile(
	t *testing.T,
) (*Compile, *mpool.AllocationAccountRegistry) {
	t.Helper()
	proc := testutil.NewProcess(t)
	ctrl := gomock.NewController(t)
	txnClient, txnOperator := newTestTxnClientAndOpWithIsolation(
		ctrl,
		txn.TxnIsolation_RC,
	)
	proc.Base.TxnClient = txnClient
	proc.Base.TxnOperator = txnOperator
	proc.ReplaceTopCtx(context.Background())
	c := NewCompile(
		"local",
		"",
		"select 1",
		"",
		"",
		nil,
		proc,
		nil,
		false,
		nil,
		time.Now(),
	)
	c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{}}}
	c.anal = newAnalyzeModule()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := budget.AllocationAccountRegistry()
	require.NoError(t, err)
	return c, registry
}

func newTestAllocationLifecycleCompile(
	t *testing.T,
	registry *mpool.AllocationAccountRegistry,
	exporter func(mpool.AllocationAccountTerminalSnapshot),
) *Compile {
	t.Helper()
	return &Compile{
		proc:                      testutil.NewProcess(t),
		MessageBoard:              message.NewMessageBoard(),
		allocationAccountRegistry: registry,
		allocationAccountLimit:    1 << 20,
		allocationControllerProvider: func() (mpool.AllocationCapacityController, error) {
			return &allocationLifecycleTestController{}, nil
		},
		allocationTerminalExporter: exporter,
	}
}

func TestStatementAllocationAttemptZeroTerminalExportsOnce(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 2)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NotNil(t, attempt)
	buffer, err := c.proc.Mp().AllocAccounted(
		64,
		attempt.account,
		1,
		1,
	)
	require.NoError(t, err)
	c.proc.Mp().Free(buffer)

	require.NoError(t, c.finishAllocationAccountAttempt())
	require.Len(t, exported, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, exported[0].State)
	require.Zero(t, exported[0].Used)
	require.Equal(t, uint64(64), exported[0].Peak)
	_, ok := registry.Resolve(exported[0].Handle)
	require.False(t, ok)

	repeated, err := attempt.finish()
	require.NoError(t, err)
	require.Equal(t, exported[0], repeated)
	require.Len(t, exported, 1)
	require.NotSame(t, c.MessageBoard, c.MessageBoard.Reset())
}

func TestStatementAllocationAttemptLateFreeDrainsTombstone(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 2)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	buffer, err := c.proc.Mp().AllocAccounted(
		64,
		attempt.account,
		1,
		1,
	)
	require.NoError(t, err)

	err = c.finishAllocationAccountAttempt()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.Len(t, exported, 1)
	require.Equal(
		t,
		mpool.AllocationAccountTerminalInvariantFailure,
		exported[0].State,
	)
	require.Equal(t, uint64(cap(buffer)), exported[0].Used)
	require.True(t, registry.AdmissionSuspended())
	_, err = registry.Open(1)
	require.ErrorIs(t, err, mpool.ErrAllocationAdmissionSuspended)

	// The physical allocation retains its original account after the producer
	// process has detached the generation. Its normal Free drains the
	// tombstone; no synthetic release is needed.
	c.proc.Mp().Free(buffer)
	require.False(t, registry.AdmissionSuspended())
	_, ok := registry.Resolve(exported[0].Handle)
	require.False(t, ok)

	c.MessageBoard = c.MessageBoard.Reset()
	next, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NotEqual(t, attempt.account.Handle(), next.account.Handle())
	require.NoError(t, c.finishAllocationAccountAttempt())
	require.Len(t, exported, 2)
}

func TestStatementAllocationAttemptConcurrentTerminalIsOneShot(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	var exports atomic.Int32
	c := newTestAllocationLifecycleCompile(t, registry, func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
		exports.Add(1)
	})
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)

	const contenders = 128
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wait sync.WaitGroup
	wait.Add(contenders)
	for range contenders {
		go func() {
			defer wait.Done()
			<-start
			_, finishErr := attempt.finish()
			errs <- finishErr
		}()
	}
	close(start)
	wait.Wait()
	close(errs)
	for finishErr := range errs {
		require.NoError(t, finishErr)
	}
	require.Equal(t, int32(1), exports.Load())
	c.allocationAttempt = nil
}

func TestStatementAllocationAttemptRejectsOverlappingOpen(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	c := newTestAllocationLifecycleCompile(t, registry, func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
	})
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)

	_, err = c.beginAllocationAccountAttempt()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.NoError(t, c.finishAllocationAccountAttempt())
	_, finishErr := attempt.finish()
	require.NoError(t, finishErr)

	next, err := registry.Open(1)
	require.NoError(t, err, "rejected overlap must not consume a slot")
	_, _, err = registry.CompleteTerminal(next)
	require.NoError(t, err)
}

func TestStatementAllocationAttemptRequiresTerminalExporter(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	c := newTestAllocationLifecycleCompile(t, registry, nil)

	_, err = c.beginAllocationAccountAttempt()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	account, err := registry.Open(1)
	require.NoError(t, err, "failed begin must not consume a generation slot")
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestStatementAllocationAttemptOwnerConfigurationRollsBack(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	configured := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
	}
	rejected := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
		failSet:      true,
	}
	c.scopes = []*Scope{
		{RootOp: configured},
		{RootOp: rejected},
	}

	_, err = c.beginAllocationAccountAttempt()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountMismatch)
	require.Nil(t, configured.account)
	require.Equal(t, 1, configured.clears)
	require.Nil(t, c.allocationAttempt)
	require.Len(t, exported, 1)
	require.Equal(
		t,
		mpool.AllocationAccountTerminalInvariantFailure,
		exported[0].State,
	)

	account, err := registry.Open(1)
	require.NoError(t, err, "failed owner configuration leaked its registry slot")
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestAllocationAccountConfiguresEveryStatementOwner(t *testing.T) {
	first := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
	}
	second := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
	}
	scopes := []*Scope{{RootOp: first}, {RootOp: second}}
	owners, err := collectAllocationAccountOwners(scopes)
	require.NoError(t, err)
	require.Len(t, owners, 2)

	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	configured, err := configureAllocationAccountOwners(owners, account)
	require.NoError(t, err)
	require.Len(t, configured, 2)
	require.Same(t, account, first.account)
	require.Same(t, account, second.account)
	for i := len(configured) - 1; i >= 0; i-- {
		require.NoError(t, configured[i].ClearAllocationAccount(account))
	}
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestAllocationAccountTransportParticipantsDoNotActivateLifecycle(t *testing.T) {
	connectorOp := connector.NewArgument()
	dispatchOp := dispatch.NewArgument()
	t.Cleanup(connectorOp.Release)
	t.Cleanup(dispatchOp.Release)
	c := &Compile{
		proc:         testutil.NewProcess(t),
		MessageBoard: message.NewMessageBoard(),
		scopes: []*Scope{
			{RootOp: connectorOp},
			{RootOp: dispatchOp},
		},
	}
	require.NoError(t, c.ensureAllocationAccountLifecycle(func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
	}))
	require.Nil(t, c.allocationAccountOwners)
	require.Nil(t, c.allocationControllerProvider)
	require.Nil(t, c.allocationAccountRegistry)
	require.NoError(t, c.attachRuntimeAllocationOwners(c.scopes))

	transportOwners := []executionAllocationAccountOwner{
		connectorOp,
		dispatchOp,
	}
	require.False(t, hasAllocationAccountActivator(transportOwners))

	active := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
	}
	require.True(t, hasAllocationAccountActivator(append(transportOwners, active)))
	require.ErrorIs(t, c.attachRuntimeAllocationOwners([]*Scope{{
		RootOp: active,
	}}), mpool.ErrAllocationAccountInvariant)
}

func TestMaterializedSourceActivatesAllocationLifecycle(t *testing.T) {
	c := &Compile{
		proc:                testutil.NewProcess(t),
		MessageBoard:        message.NewMessageBoard(),
		materializedSources: map[int32]*materialized.Source{1: materialized.NewSource(1)},
	}
	require.NoError(t, c.ensureAllocationAccountLifecycle(func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
	}))
	require.NotNil(t, c.allocationControllerProvider)
	require.NotNil(t, c.allocationAccountRegistry)
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NotNil(t, attempt)
	require.NotNil(t, attempt.account)
	require.NoError(t, c.finishAllocationAccountAttempt())
}

func TestCompileClearClosesMaterializedSourceBeforeAccountTerminal(t *testing.T) {
	c, _ := newRunLifecycleCompile(t)
	proc := c.proc
	source := materialized.NewSource(1)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c.materializedSources = map[int32]*materialized.Source{1: source}
	require.NoError(t, c.ensureAllocationAccountLifecycle(func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	}))
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NoError(t, source.Begin(proc.Mp(), materialized.SpillConfig{
		AllocationAccount: attempt.account,
	}))
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, proc.Mp()))
	input.SetRowCount(1)
	require.NoError(t, source.Append(input))
	input.Clean(proc.Mp())
	require.Positive(t, attempt.account.Snapshot().Used)

	c.clear()

	require.Len(t, exported, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, exported[0].State)
	require.Zero(t, exported[0].Used)
}

func TestAllocationAccountCollectsProductConsumerAndHashBuild(t *testing.T) {
	consumer := product.NewArgument()
	producer := hashbuild.NewArgument()
	scopes := []*Scope{{
		RootOp: consumer,
		PreScopes: []*Scope{
			{RootOp: producer},
		},
	}}
	owners, err := collectAllocationAccountOwners(scopes)
	require.NoError(t, err)
	require.Len(t, owners, 2)

	registry, err := mpool.NewAllocationAccountRegistry(1, 32)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	configured, err := configureAllocationAccountOwners(owners, account)
	require.NoError(t, err)
	require.Len(t, configured, 2)
	for i := len(configured) - 1; i >= 0; i-- {
		require.NoError(t, configured[i].ClearAllocationAccount(account))
	}
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
	consumer.Release()
	producer.Release()
}

func TestParallelRuntimeClonesJoinAllocationAttempt(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	template := hashbuild.NewArgument()
	template.NeedHashMap = false
	source := &Scope{
		RootOp: template,
		Proc:   c.proc,
		NodeInfo: engine.Node{
			Mcpu: 2,
		},
	}
	c.scopes = []*Scope{source}
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NotNil(t, attempt)

	parallel, workers := newParallelScope(source)
	require.Len(t, workers, 2)
	for _, worker := range workers {
		hb := worker.RootOp.(*hashbuild.HashBuild)
		require.ErrorIs(t, hb.Prepare(worker.Proc), mpool.ErrAllocationAccountInvalid)
	}
	require.NoError(t, c.attachRuntimeAllocationOwners(workers))
	for _, worker := range workers {
		hb := worker.RootOp.(*hashbuild.HashBuild)
		require.NoError(t, hb.Prepare(worker.Proc))
	}

	require.NoError(t, c.finishAllocationAccountAttempt())
	require.Len(t, exported, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, exported[0].State)
	require.Zero(t, exported[0].Used)
	parallel.release()
	template.Release()
}

func TestStatementAllocationAttemptOwnerTeardownFailureExportsFailure(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	owner := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
		failClear:    true,
	}
	c.scopes = []*Scope{{RootOp: owner}}

	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	err = c.finishAllocationAccountAttempt()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.Len(t, exported, 1)
	require.Equal(
		t,
		mpool.AllocationAccountTerminalInvariantFailure,
		exported[0].State,
	)
	require.Zero(t, exported[0].Used)
	require.False(t, registry.AdmissionSuspended())
	_, ok := registry.Resolve(attempt.account.Handle())
	require.False(t, ok)
}

func TestCompileClearFinalizesAllocationOwnerBeforeRelease(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	var exported []mpool.AllocationAccountTerminalSnapshot
	c := newTestAllocationLifecycleCompile(t, registry, func(
		snapshot mpool.AllocationAccountTerminalSnapshot,
	) {
		exported = append(exported, snapshot)
	})
	c.affectRows = &atomic.Uint64{}
	owner := &allocationLifecycleOwnerOperator{
		MockOperator: colexec.NewMockOperator(),
	}
	c.scopes = []*Scope{{RootOp: owner}}

	_, err = c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	c.clear()

	require.True(t, owner.released)
	require.False(t, owner.releaseSawLiveAccount)
	require.Nil(t, owner.account)
	require.Equal(t, 1, owner.clears)
	require.Len(t, exported, 1)
	require.Equal(t, mpool.AllocationAccountTerminalValid, exported[0].State)
	require.Zero(t, exported[0].Used)
}

func TestCompileAutomaticallyActivatesCompleteHashTableOwner(t *testing.T) {
	proc := testutil.NewProcess(t)
	c := &Compile{
		proc:         proc,
		MessageBoard: message.NewMessageBoard(),
	}
	owner := hashbuild.NewArgument()
	owner.NeedHashMap = true
	owner.Conditions = []*plan.Expr{{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{}},
	}}
	c.scopes = []*Scope{{RootOp: owner}}

	require.NoError(t, c.ensureAllocationAccountLifecycle(func(
		mpool.AllocationAccountTerminalSnapshot,
	) {
	}))
	require.NotNil(t, c.allocationControllerProvider)
	require.NotNil(t, c.allocationAccountRegistry)
	attempt, err := c.beginAllocationAccountAttempt()
	require.NoError(t, err)
	require.NotNil(t, attempt)
	require.NoError(t, owner.ClearAllocationAccount(attempt.account))
	require.NoError(t, c.finishAllocationAccountAttempt())
	_, ok := c.allocationAccountRegistry.Resolve(attempt.account.Handle())
	require.False(t, ok)
	owner.Release()
}

func TestCompileRunFinalizesAllocationAttemptOnCancellation(t *testing.T) {
	c, registry := newRunLifecycleCompile(t)
	// The canceled outer context is observed after runOnce, after the
	// allocation generation has opened.
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	c.proc.ReplaceTopCtx(canceled)
	scope := newScope(magicType(255))
	owner := &allocationLifecycleOwnerOperator{MockOperator: colexec.NewMockOperator()}
	scope.RootOp = owner
	c.scopes = []*Scope{scope}

	_, err := c.Run(0)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, owner.account)
	require.Zero(t, registry.LiveAllocationMetadata())
	c.Release()
}

func TestCompileRunFinalizesAllocationAttemptOnExecutionError(t *testing.T) {
	c, registry := newRunLifecycleCompile(t)
	executionErr := moerr.NewInternalErrorNoCtx("allocation lifecycle test")
	scope := newScope(Normal)
	scope.Proc = c.proc.NewNoContextChildProc(0)
	owner := &allocationLifecycleErrorOperator{
		MockOperator: colexec.NewMockOperator(),
		err:          executionErr,
	}
	scope.RootOp = owner
	c.scopes = []*Scope{scope}

	_, err := c.Run(0)
	require.ErrorIs(t, err, executionErr)
	require.Nil(t, owner.account)
	require.Zero(t, registry.LiveAllocationMetadata())
	c.Release()
}

func TestCompileRunFinalizesAllocationAttemptOnPanic(t *testing.T) {
	c, registry := newRunLifecycleCompile(t)
	scope := newScope(magicType(255))
	owner := &allocationLifecycleOwnerOperator{MockOperator: colexec.NewMockOperator()}
	scope.RootOp = owner
	c.scopes = []*Scope{scope}
	// Force the panic after beginAllocationAccountAttempt and before runOnce.
	c.lockMeta = nil

	require.Panics(t, func() {
		_, _ = c.Run(0)
	})
	require.Nil(t, owner.account)
	require.Zero(t, registry.LiveAllocationMetadata())
	c.Release()
}
