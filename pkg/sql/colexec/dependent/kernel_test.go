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

package dependent

import (
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScalarSetPairContinuationWork(t *testing.T) {
	for _, predicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "false", true: "true"}[predicate], func(t *testing.T) {
			p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer p.Free()
			budget, err := p.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := budget.AllocationAccountRegistry()
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, budget)
			require.NoError(t, err)
			defer func() { _, _, err := registry.CompleteTerminal(account); require.NoError(t, err) }()
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
			require.NoError(t, err)
			lease, err := budget.AcquireScalarSetExecution()
			require.NoError(t, err)
			defer lease.Close()
			require.NoError(t, lease.ChargeCandidatePairs(process.ScalarSetCandidatePairLimit-2))
			program, err := colexec.CompileDependentExpressions(p.Ctx, []*plan.Expr{plan2.MakePlan2BoolConstExprWithType(predicate)})
			require.NoError(t, err)
			joins := []*JoinKernel{
				{Join: &loopjoin.LoopJoin{ScalarSetReaggregation: true, ScalarSetLease: lease}, Condition: program},
				{Join: &loopjoin.LoopJoin{ScalarSetReaggregation: true, ScalarSetLease: lease}, Condition: program},
			}
			defer func() {
				for _, join := range joins {
					join.Reset(p, false, nil)
				}
			}()
			for _, join := range joins {
				join.BindDependent(nil, allocation)
				arg := join.Join.(*loopjoin.LoopJoin)
				value, err := arg.PairCondition(p, batch.EmptyForConstFoldBatch, 0, batch.EmptyForConstFoldBatch, 0)
				require.NoError(t, err)
				require.Nil(t, value)
				charged := lease.CandidatePairs()
				task := join.TakeContinuation()
				require.NotNil(t, task)
				require.NoError(t, vm.RunContinuation(p.Ctx, budget, task, nil))
				value, err = arg.PairCondition(p, batch.EmptyForConstFoldBatch, 0, batch.EmptyForConstFoldBatch, 0)
				require.NoError(t, err)
				require.Equal(t, predicate, vector.GetFixedAtNoTypeCheck[bool](value, 0))
				require.Equal(t, charged, lease.CandidatePairs(), "resuming one pair does not charge it twice")
			}
			require.Equal(t, process.ScalarSetCandidatePairLimit, lease.CandidatePairs())
			_, err = joins[0].pair(p, batch.EmptyForConstFoldBatch, 0, batch.EmptyForConstFoldBatch, 0)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrScalarSetResourceBudgetExceeded))
		})
	}
}

type faultKernel struct {
	terminalKernel
	phase        string
	preparePanic bool
}

func (k *faultKernel) Prepare(*process.Process) error {
	if k.preparePanic {
		panic("private prepare")
	}
	return nil
}
func (k *faultKernel) Reset(p *process.Process, failed bool, cause error) {
	k.terminalKernel.Reset(p, failed, cause)
	if k.phase == "reset" {
		panic("reset")
	}
}
func (k *faultKernel) Free(p *process.Process, failed bool, cause error) {
	k.terminalKernel.Free(p, failed, cause)
	if k.phase == "free" {
		panic("free")
	}
}
func (k *faultKernel) ClearAllocationAccount(*mpool.AllocationAccount) error {
	if k.phase == "clear" {
		panic("clear")
	}
	return nil
}

func (k *faultKernel) Release() {
	k.terminalKernel.Release()
	if k.phase == "release" {
		panic("release")
	}
}

func TestNativeKernelPrivatePanicRollback(t *testing.T) {
	for _, phase := range []string{"prepare", "reset", "free", "clear", "release", "factory"} {
		t.Run(phase, func(t *testing.T) {
			p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer p.Free()
			budget, err := p.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := budget.AllocationAccountRegistry()
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, budget)
			require.NoError(t, err)
			defer func() { _, _, _ = registry.CompleteTerminalWithError(account, mpool.ErrAllocationAccountInvariant) }()
			op := &faultKernel{phase: phase, preparePanic: true}
			child := &terminalKernel{}
			op.AppendChild(child)
			owner := &Operator{}
			defer owner.Free(p, true, nil)
			require.NoError(t, owner.SetAllocationAccount(account))
			owner.execution = &Execution{}
			kernel, err := openKernel(p, func(*process.Process, []*DemandInput) (vm.Operator, error) {
				if phase == "factory" {
					panic("unpublished factory")
				}
				return op, nil
			}, 0, account, owner.execution)
			require.Nil(t, kernel)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
			require.Zero(t, budget.Used())
			_ = owner.close(err)
			clearErr := owner.ClearAllocationAccount(account)
			if phase == "prepare" {
				require.NoError(t, clearErr, "fully drained initialization failure remains releasable")
				require.Contains(t, op.events, "release")
			} else {
				require.ErrorIs(t, clearErr, mpool.ErrAllocationAccountInvariant)
				if phase != "release" {
					require.NotContains(t, op.events, "release", "damaged object cannot return to the reuse pool")
				}
			}
			if phase != "factory" {
				require.Contains(t, child.events, "reset")
				require.Contains(t, child.events, "free", "one panic cannot strand another native owner")
			}
			_, _, terminalErr := registry.CompleteTerminalWithError(account, clearErr)
			if phase == "prepare" {
				require.NoError(t, terminalErr)
			} else {
				require.Error(t, terminalErr)
			}
		})
	}
}

func TestNestedKernelCleanupFailureIsSticky(t *testing.T) {
	p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer p.Free()
	budget, err := p.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := budget.AllocationAccountRegistry()
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	defer func() { _, _, _ = registry.CompleteTerminalWithError(account, mpool.ErrAllocationAccountInvariant) }()
	owner := &Operator{}
	defer owner.Free(p, true, nil)
	require.NoError(t, owner.SetAllocationAccount(account))
	e := &Execution{}
	owner.execution = e
	op := &faultKernel{phase: "free"}
	kernel, err := openKernel(p, func(*process.Process, []*DemandInput) (vm.Operator, error) { return op, nil }, 0, account, e)
	require.NoError(t, err)
	nested := &region{execution: e, template: &regionTemplate{owner: 1}, proc: p, budget: budget, cursors: []*cursor{{kernel: kernel}}}
	i := &invocation{region: nested, proc: p, done: true}
	defer func() {
		_ = i.Close(nil)
		_ = owner.close(nil)
		_, _, _ = registry.CompleteTerminalWithError(account, owner.ClearAllocationAccount(account))
	}()
	_, err = i.Step(p.Ctx)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.NoError(t, i.Close(err), "the already-retired region cannot report its error again")
	require.ErrorIs(t, owner.close(err), mpool.ErrAllocationAccountInvariant)
	clearErr := owner.ClearAllocationAccount(account)
	require.ErrorIs(t, clearErr, mpool.ErrAllocationAccountInvariant)
	require.Zero(t, account.Snapshot().Used)
	_, _, err = registry.CompleteTerminalWithError(account, clearErr)
	require.Error(t, err, "zero live bytes alone do not prove successful nested cleanup")
	require.Zero(t, budget.Used())
	require.NotContains(t, op.events, "release")
}

type terminalKernel struct {
	DemandInput
	prepareErr error
	events     []string
	causes     []error
	failed     []bool
}

func (k *terminalKernel) Prepare(*process.Process) error { return k.prepareErr }
func (k *terminalKernel) Reset(_ *process.Process, failed bool, cause error) {
	k.events = append(k.events, "reset")
	k.causes = append(k.causes, cause)
	k.failed = append(k.failed, failed)
}
func (k *terminalKernel) Free(_ *process.Process, failed bool, cause error) {
	k.events = append(k.events, "free")
	k.causes = append(k.causes, cause)
	k.failed = append(k.failed, failed)
}
func (k *terminalKernel) Release() { k.events = append(k.events, "release") }

func TestNativeKernelTerminalCause(t *testing.T) {
	for _, mode := range []string{"success", "execution_failure", "prepare_failure", "factory_failure"} {
		t.Run(mode, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<20, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			cause := errors.New("kernel failed")
			if mode == "success" {
				cause = nil
			}
			op := &terminalKernel{}
			if mode == "prepare_failure" {
				op.prepareErr = cause
			}
			kernel, err := openKernel(proc, func(*process.Process, []*DemandInput) (vm.Operator, error) {
				if mode == "factory_failure" {
					return op, cause
				}
				return op, nil
			}, 0, account, nil)
			if mode == "prepare_failure" || mode == "factory_failure" {
				require.ErrorIs(t, err, cause)
				require.Nil(t, kernel)
			} else {
				require.NoError(t, err)
				defer func() { require.NoError(t, kernel.close(cause)) }()
				require.NoError(t, kernel.close(cause))
				require.NoError(t, kernel.close(cause))
			}
			require.Equal(t, []string{"reset", "free", "release"}, op.events)
			require.Equal(t, []error{cause, cause}, op.causes)
			require.Equal(t, []bool{cause != nil, cause != nil}, op.failed)
			require.Zero(t, budget.Used())
		})
	}
}

func TestSourceEdgeRetiresRoundControlBudget(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, t.Name(), mpool.MustNewZero())
	defer proc.Free()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, budget)
	require.NoError(t, err)
	defer func() {
		account.Seal()
		_, err := registry.Finalize(account)
		require.NoError(t, err)
	}()
	r := &region{proc: proc, budget: budget, execution: &Execution{account: account}}
	for range 3 {
		func() {
			frontier, err := r.newSource(1)
			require.NoError(t, err)
			defer frontier.close()
			one := budget.Used()
			require.Positive(t, one)
			next, err := r.newSource(1)
			require.NoError(t, err)
			defer next.close()
			require.Equal(t, 2*one, budget.Used())
			frontier.close()
			frontier.close()
			require.Equal(t, one, budget.Used())
		}()
		require.Zero(t, budget.Used())
		require.Zero(t, r.charged)
		require.Zero(t, account.Snapshot().Used)
	}
}
