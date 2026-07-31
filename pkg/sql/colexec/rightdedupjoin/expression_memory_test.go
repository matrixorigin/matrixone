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

package rightdedupjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestRightDedupJoinResetReleasesProbeExpressionLease(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_I32Val{I32Val: 1},
		}},
	}
	executors, err := colexec.NewExpressionExecutorsFromPlanExpressions(proc, []*plan.Expr{expr})
	require.NoError(t, err)
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	lease, err := hashbuild.NewExpressionMemoryLease(
		generation, []*plan.Expr{expr}, executors, false)
	require.NoError(t, err)

	arg := &RightDedupJoin{}
	arg.ctr.evecs = []evalVector{{executor: executors[0]}}
	arg.ctr.vecs = make([]*vector.Vector, len(executors))
	arg.ctr.probeExpressionLease = lease
	input := batch.NewWithSize(0)
	input.SetRowCount(4)
	require.NoError(t, arg.ctr.evalJoinConditionBudgeted(input, proc))
	require.Positive(t, generation.Used())

	arg.Reset(proc, false, nil)
	require.Zero(t, generation.Used())
	require.Nil(t, arg.ctr.evecs)
	require.Nil(t, arg.ctr.vecs)
	require.Nil(t, arg.ctr.probeExpressionLease)
}

func TestRightDedupJoinResetReleasesAccountedProbeExpressions(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	const capBytes = uint64(1 << 20)
	budget := process.MustNewHashBuildBudget(capBytes, capBytes)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.OpenWithController(capBytes, generation)
	require.NoError(t, err)
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}}}}
	executors, err := hashbuild.NewAllocationAccountedExpressionExecutorsForAccount(
		proc, []*plan.Expr{expr}, account, hashbuild.HashBuildAllocationOwner)
	require.NoError(t, err)
	arg := &RightDedupJoin{allocationAccount: account}
	arg.ctr.evecs = []evalVector{{executor: executors[0]}}
	arg.ctr.vecs = make([]*vector.Vector, len(executors))
	arg.ctr.probeExpressionsAccounted = true
	input := batch.NewWithSize(0)
	input.SetRowCount(4)
	require.NoError(t, arg.ctr.evalJoinConditionBudgeted(input, proc))
	require.Positive(t, account.Snapshot().Used)

	arg.Reset(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	require.False(t, arg.ctr.probeExpressionsAccounted)
	require.Nil(t, arg.ctr.evecs)
	terminal, _, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.Equal(t, mpool.AllocationAccountTerminalValid, terminal.State)
}

func TestRightDedupJoinAllocationActivationRequiresBothKeySides(t *testing.T) {
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	arg := &RightDedupJoin{Conditions: [][]*plan.Expr{{col}, {col}}}
	require.True(t, arg.AllocationAccountEnabled())
	require.False(t, arg.AllocationAccountActivationBlocked())
	arg.Conditions[1] = []*plan.Expr{nil}
	require.False(t, arg.AllocationAccountEnabled())
	require.True(t, arg.AllocationAccountActivationBlocked())
}
