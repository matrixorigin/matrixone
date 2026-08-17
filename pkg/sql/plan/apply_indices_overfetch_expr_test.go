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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
)

// evalConstUint64 evaluates the expression through the ordinary expression
// executor -- the same route table_function.evalLimitExpression takes at EXECUTE,
// and therefore the route a pre-change CN would use on the pushed node.Limit.
// Folding it with the plan-time constant folder would prove less: `case`,
// `greatest` and `cast` are not folded there, so the check has to run the
// evaluator that actually resolves them.
func evalConstUint64(t *testing.T, cc *MockCompilerContext, expr *planpb.Expr) uint64 {
	t.Helper()
	proc := cc.GetProcess()
	executor, err := colexec.NewExpressionExecutor(proc, DeepCopyExpr(expr))
	require.NoError(t, err)
	defer executor.Free()

	vec, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.NotNil(t, vec)
	require.False(t, vec.IsNull(0), "budget expression evaluated to NULL")

	switch vec.GetType().Oid {
	case types.T_uint64:
		return vector.GetFixedAtWithTypeCheck[uint64](vec, 0)
	case types.T_int64:
		v := vector.GetFixedAtWithTypeCheck[int64](vec, 0)
		require.GreaterOrEqual(t, v, int64(0))
		return uint64(v)
	default:
		t.Fatalf("budget expression has non-integer type %v", vec.GetType())
		return 0
	}
}

// TestOverFetchLimitExprMatchesGoFormula is the contract that keeps a rolling
// upgrade safe. A pre-change CN cannot run overfetch.PostFilterLimit; it only
// evaluates whatever expression arrives on node.Limit. If the two ever disagree,
// old and new CNs pick different candidate budgets for the same query.
//
// k values are chosen to sit on both sides of every bucket boundary of the step
// function, since a wrong comparison operator (< vs <=) only shows up exactly there.
func TestOverFetchLimitExprMatchesGoFormula(t *testing.T) {
	ctx := context.Background()
	cc := NewMockCompilerContext(true)

	// Sweep every k up to past the last bucket, then a few large ones. The dense
	// sweep matters: the float-truncation divergence between Go and SQL CAST only
	// appears at k values whose k*factor has a fractional part (k=51 was the first),
	// which a handful of boundary probes can miss entirely.
	var ks []uint64
	for k := uint64(0); k <= 320; k++ {
		ks = append(ks, k)
	}
	ks = append(ks, 1000, 4095, 100000, 1<<20)

	for _, filteredPostMode := range []bool{false, true} {
		for _, k := range ks {
			// A literal k folds at plan time; assert the fold is the Go value.
			lit := makePlan2Uint64ConstExprWithType(k)
			gotLit, err := BuildOverFetchLimitExpr(ctx, lit, filteredPostMode)
			require.NoError(t, err)
			want := overfetch.PostFilterLimit(k)
			if filteredPostMode {
				want = overfetch.FilteredPostModeLimit(k)
			}
			require.Equal(t, want, evalConstUint64(t, cc, gotLit),
				"literal k=%d filteredPostMode=%v", k, filteredPostMode)

			// The parameterized form must compute the SAME value. Built over a
			// non-literal k so BuildOverFetchLimitExpr takes the expression path,
			// then folded with that k substituted.
			nonLit, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
				makePlan2Uint64ConstExprWithType(k), makePlan2Uint64ConstExprWithType(0)})
			require.NoError(t, err)
			require.Nil(t, nonLit.GetLit(), "test setup: k must not already be a literal")

			gotExpr, err := BuildOverFetchLimitExpr(ctx, nonLit, filteredPostMode)
			require.NoError(t, err)
			require.Nil(t, gotExpr.GetLit(),
				"a parameterized k must produce an expression, not a plan-time constant")
			require.Equal(t, want, evalConstUint64(t, cc, gotExpr),
				"expression k=%d filteredPostMode=%v: old and new CNs would disagree", k, filteredPostMode)
		}
	}

	// A nil limit stays nil -- there is no budget to carry.
	got, err := BuildOverFetchLimitExpr(ctx, nil, false)
	require.NoError(t, err)
	require.Nil(t, got)
}
