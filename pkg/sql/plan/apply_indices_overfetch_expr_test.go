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
	"math"
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
	// Saturation boundary. Both halves of the Go formula clamp at MaxUint64: the
	// k*factor product and the k+10 floor. An expression that lets either one
	// overflow raises "data out of range" for a LIMIT the literal path accepts.
	ks = append(ks, math.MaxUint64-11, math.MaxUint64-10, math.MaxUint64-5, math.MaxUint64)

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

// TestOverFetchLimitExprSaturationBoundaries pins the two clamps in
// overfetch.Limit separately, because the runtime expression reaches them by
// different mechanisms and a change can break one while the other still works.
//
// The dense sweep above cannot isolate them: every k large enough to saturate the
// additive floor also saturates the product, so a regression in the floor clamp
// alone would hide behind a correct product clamp. These cases name which half
// each k exercises.
//
// Why this matters beyond arithmetic: the values here are LIMITs a literal query
// accepts. Before the clamps, the same query as a prepared statement raised
// "data out of range" — so identical SQL succeeded or failed depending only on
// whether the limit arrived as a literal or a parameter, and a pre-change CN
// evaluating node.Limit would disagree with a new CN computing it in Go.
func TestOverFetchLimitExprSaturationBoundaries(t *testing.T) {
	ctx := context.Background()
	cc := NewMockCompilerContext(true)

	cases := []struct {
		k    uint64
		what string
	}{
		{10_000_000_000_000_000_000, "large, neither clamp engages: 1.2*k stays under 2^64"},
		{15_372_286_728_091_293_013, "at the product boundary: 1.2*k lands within an ULP of 2^64"},
		{16_000_000_000_000_000_000, "product clamps, floor does not: k*1.2 >= 2^64 but k+10 fits"},
		{math.MaxUint64 - 11, "product clamps; k+10 still fits by one"},
		{math.MaxUint64 - 10, "product clamps; k+10 is exactly MaxUint64"},
		{math.MaxUint64 - 5, "both clamp"},
		{math.MaxUint64, "both clamp at the top of the domain"},
	}

	for _, filteredPostMode := range []bool{false, true} {
		for _, tc := range cases {
			want := overfetch.PostFilterLimit(tc.k)
			if filteredPostMode {
				want = overfetch.FilteredPostModeLimit(tc.k)
			}

			lit := makePlan2Uint64ConstExprWithType(tc.k)
			gotLit, err := BuildOverFetchLimitExpr(ctx, lit, filteredPostMode)
			require.NoError(t, err, "literal k=%d (%s)", tc.k, tc.what)
			require.Equal(t, want, evalConstUint64(t, cc, gotLit),
				"literal k=%d filteredPostMode=%v (%s)", tc.k, filteredPostMode, tc.what)

			nonLit, err := BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
				makePlan2Uint64ConstExprWithType(tc.k), makePlan2Uint64ConstExprWithType(0)})
			require.NoError(t, err)
			require.Nil(t, nonLit.GetLit(), "test setup: k must not already be a literal")

			gotExpr, err := BuildOverFetchLimitExpr(ctx, nonLit, filteredPostMode)
			require.NoError(t, err, "expression k=%d (%s)", tc.k, tc.what)
			// The failure this guards is an evaluation error, not a wrong number:
			// an unclamped cast raises "data out of range" right here.
			require.Equal(t, want, evalConstUint64(t, cc, gotExpr),
				"expression k=%d filteredPostMode=%v (%s): prepared and literal disagree",
				tc.k, filteredPostMode, tc.what)
		}
	}

	// The clamps must not cost the top of the domain its meaning: MaxUint64 in
	// must still be MaxUint64 out, not the largest-float64-below-2^64 the cast is
	// clamped to (2^64-2048, which is 2047 short).
	require.Equal(t, uint64(math.MaxUint64), overfetch.PostFilterLimit(math.MaxUint64))
	top, err := BuildOverFetchLimitExpr(ctx,
		makePlan2Uint64ConstExprWithType(math.MaxUint64), false)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), evalConstUint64(t, cc, top),
		"saturation must reach MaxUint64 exactly, not the float clamp below it")
}
