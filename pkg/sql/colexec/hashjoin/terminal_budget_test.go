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

package hashjoin

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type terminalBudgetAdmissionChild struct {
	*colexec.MockOperator
	err error
}

func (child *terminalBudgetAdmissionChild) Call(*process.Process) (vm.CallResult, error) {
	return vm.NewCallResult(), child.err
}

type gatedBroadcastBuildChild struct {
	*colexec.MockOperator
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (child *gatedBroadcastBuildChild) Call(proc *process.Process) (vm.CallResult, error) {
	child.once.Do(func() { close(child.entered) })
	select {
	case <-child.release:
		return child.MockOperator.Call(proc)
	case <-proc.Ctx.Done():
		return vm.NewCallResult(), proc.Ctx.Err()
	}
}

type countingBroadcastProbeChild struct {
	*colexec.MockOperator
	calls atomic.Int32
}

func (child *countingBroadcastProbeChild) Call(proc *process.Process) (vm.CallResult, error) {
	child.calls.Add(1)
	return child.MockOperator.Call(proc)
}

func TestHashJoinCallConvertsTerminalBudgetAdmission(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetMessageBoard(message.NewMessageBoard())

	admission := &process.HashBuildBudgetError{
		Kind:      process.HashBuildBudgetErrorAdmission,
		Component: process.HashBuildBudgetComponentMemory,
		Requested: 2,
		Used:      1,
		Cap:       1,
	}
	child := &terminalBudgetAdmissionChild{
		MockOperator: colexec.NewMockOperator(),
		err:          admission,
	}
	arg := &HashJoin{
		IsShuffle:  true,
		ShuffleIdx: 0,
		JoinMapTag: 91001,
	}
	var callErr error
	t.Cleanup(func() {
		arg.Free(proc, true, callErr)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	arg.OpAnalyzer = process.NewAnalyzer(0, false, false, "terminal-budget-test")
	arg.AppendChild(child)
	message.SendJoinMapResult(
		message.NewJoinMapResult(nil),
		arg.JoinMapTag,
		arg.IsShuffle,
		arg.ShuffleIdx,
		proc.GetMessageBoard(),
	)

	_, callErr = arg.Call(proc)
	require.Error(t, callErr)
	require.True(t, moerr.IsMoErrCode(callErr, moerr.ErrOOM), callErr)
	require.NotErrorIs(t, callErr, process.ErrHashBuildBudgetAdmission)
	require.NotContains(t, callErr.Error(), "convert go error")
	require.NotContains(t, callErr.Error(), process.ErrHashBuildBudgetAdmission.Error())
	require.Contains(t, callErr.Error(), "hash build memory budget exceeded")
}

func TestBroadcastBudgetFailureUnblocksParallelHashJoinConsumers(t *testing.T) {
	const (
		consumerCount = 2
		joinMapTag    = 91002
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rootProc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rootProc.SetMessageBoard(message.NewMessageBoard())
	rootProc.BuildPipelineContext(ctx)
	budget, err := rootProc.GetHashBuildBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<60, budget)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.Zero(t, account.Snapshot().Used)
		_, _, completeErr := registry.CompleteTerminal(account)
		require.NoError(t, completeErr)
		require.Zero(t, budget.Used())
		require.Zero(t, budget.SpillDiskUsed())
		require.Zero(t, budget.SpillFDUsed())
		rootProc.Free()
		require.Zero(t, rootProc.Mp().CurrNB())
	})

	typ := types.T_int32.ToType()
	buildValues := make([]int32, 300_000)
	for i := range buildValues {
		buildValues[i] = int32(i)
	}
	buildBatch := batch.NewWithSize(1)
	buildBatch.Vecs[0] = testutil.MakeInt32Vector(buildValues, nil, rootProc.Mp())
	buildBatch.SetRowCount(len(buildValues))
	releaseBuild := make(chan struct{})
	buildChild := &gatedBroadcastBuildChild{
		MockOperator: colexec.NewMockOperator().WithBatchs([]*batch.Batch{buildBatch}),
		entered:      make(chan struct{}),
		release:      releaseBuild,
	}
	buildProc := rootProc.NewContextChildProc(0)
	buildArg := &hashbuild.HashBuild{
		JoinMapTag:     joinMapTag,
		JoinMapRefCnt:  consumerCount,
		Conditions:     []*plan.Expr{newExpr(0, typ)},
		NeedHashMap:    true,
		SpillThreshold: 1,
	}
	require.NoError(t, buildArg.SetAllocationAccount(account))
	buildArg.AppendChild(buildChild)
	var buildErr error
	t.Cleanup(func() {
		buildArg.Reset(buildProc, buildErr != nil, buildErr)
		buildChild.Reset(buildProc, buildErr != nil, buildErr)
		buildArg.Free(buildProc, buildErr != nil, buildErr)
		buildChild.Free(buildProc, buildErr != nil, buildErr)
		require.NoError(t, buildArg.ClearAllocationAccount(account))
		if buildProc.Cancel != nil {
			buildProc.Cancel(buildErr)
		}
	})
	require.NoError(t, buildChild.Prepare(buildProc))
	require.NoError(t, buildArg.Prepare(buildProc))

	type consumerState struct {
		join  *HashJoin
		probe *countingBroadcastProbeChild
		proc  *process.Process
		err   error
	}
	consumers := make([]consumerState, consumerCount)
	for i := range consumers {
		probe := &countingBroadcastProbeChild{MockOperator: colexec.NewMockOperator()}
		consumerProc := rootProc.NewContextChildProc(0)
		join := &HashJoin{
			JoinType:   plan.Node_LEFT,
			ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 0)},
			LeftTypes:  []types.Type{typ},
			RightTypes: []types.Type{typ},
			EqConds: [][]*plan.Expr{
				{newExpr(0, typ)},
				{newExpr(0, typ)},
			},
			NumCPU:     1,
			IsMerger:   true,
			JoinMapTag: joinMapTag,
		}
		require.NoError(t, join.SetAllocationAccount(account))
		join.AppendChild(probe)
		consumers[i] = consumerState{join: join, probe: probe, proc: consumerProc}
		t.Cleanup(func() {
			state := &consumers[i]
			state.join.Reset(state.proc, state.err != nil, state.err)
			state.probe.Reset(state.proc, state.err != nil, state.err)
			state.join.Free(state.proc, state.err != nil, state.err)
			state.probe.Free(state.proc, state.err != nil, state.err)
			require.NoError(t, state.join.ClearAllocationAccount(account))
			if state.proc.Cancel != nil {
				state.proc.Cancel(state.err)
			}
		})
		require.NoError(t, probe.Prepare(consumerProc))
		require.NoError(t, join.Prepare(consumerProc))
	}

	type execOutcome struct {
		index  int
		result vm.CallResult
		err    error
	}
	var workers sync.WaitGroup
	t.Cleanup(workers.Wait)
	buildOutcome := make(chan execOutcome, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		result, err := vm.Exec(buildArg, buildProc)
		buildOutcome <- execOutcome{result: result, err: err}
	}()
	select {
	case <-buildChild.entered:
	case <-ctx.Done():
		t.Fatalf("HashBuild did not reach the pre-publication barrier: %v", ctx.Err())
	}

	startConsumers := make(chan struct{})
	consumerStarted := make(chan struct{}, consumerCount)
	consumerOutcomes := make(chan execOutcome, consumerCount)
	for i := range consumers {
		workers.Add(1)
		go func(i int) {
			defer workers.Done()
			<-startConsumers
			consumerStarted <- struct{}{}
			result, err := vm.Exec(consumers[i].join, consumers[i].proc)
			consumerOutcomes <- execOutcome{index: i, result: result, err: err}
		}(i)
	}
	// Launch real HashJoin workers while HashBuild is still held before terminal
	// publication. The message package separately covers the lower-level case
	// where every broadcast receiver is already blocked on its registered waiter.
	close(startConsumers)
	for range consumers {
		select {
		case <-consumerStarted:
		case <-ctx.Done():
			t.Fatalf("HashJoin consumers did not start before build publication: %v", ctx.Err())
		}
	}
	close(releaseBuild)

	select {
	case outcome := <-buildOutcome:
		buildErr = outcome.err
		require.Nil(t, outcome.result.Batch)
		require.Error(t, buildErr)
		require.True(t, moerr.IsMoErrCode(buildErr, moerr.ErrOOM), buildErr)
		require.NotErrorIs(t, buildErr, process.ErrHashBuildBudgetAdmission)
		require.Contains(t, buildErr.Error(), "hash build memory budget exceeded")
	case <-ctx.Done():
		t.Fatalf("HashBuild did not publish its terminal error: %v", ctx.Err())
	}

	for range consumers {
		select {
		case outcome := <-consumerOutcomes:
			consumers[outcome.index].err = outcome.err
			require.Nil(t, outcome.result.Batch, "consumer %d emitted a partial probe result", outcome.index)
			require.Error(t, outcome.err)
			require.True(t, moerr.IsMoErrCode(outcome.err, moerr.ErrOOM), outcome.err)
			require.NotErrorIs(t, outcome.err, process.ErrHashBuildBudgetAdmission)
			require.Equal(t, buildErr.Error(), outcome.err.Error())
			require.Zero(t, consumers[outcome.index].probe.calls.Load(),
				"consumer %d must fail before reading probe input", outcome.index)
		case <-ctx.Done():
			t.Fatalf("HashJoin consumer remained blocked after terminal publication: %v", ctx.Err())
		}
	}
	require.Zero(t, buildArg.OpAnalyzer.GetOpStats().ExtraStats["HashBuildSpillStarts"])
}
