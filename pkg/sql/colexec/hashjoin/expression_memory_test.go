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

package hashjoin

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

func TestHashJoinResetReleasesProbeExpressionLease(t *testing.T) {
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

	arg := &HashJoin{}
	arg.ctr.eqCondExecs = executors
	arg.ctr.eqCondVecs = make([]*vector.Vector, len(executors))
	arg.ctr.probeExpressionLease = lease
	input := batch.NewWithSize(0)
	input.SetRowCount(4)
	require.NoError(t, arg.ctr.evalJoinConditionBudgeted(input, proc))
	require.Positive(t, generation.Used())

	arg.Reset(proc, false, nil)
	require.Zero(t, generation.Used())
	require.Nil(t, arg.ctr.eqCondExecs)
	require.Nil(t, arg.ctr.eqCondVecs)
	require.Nil(t, arg.ctr.probeExpressionLease)
}
