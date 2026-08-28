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

package table_function

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
)

// TestOverFetchLimitExprResolvesInTVF is the executor half of the rolling-upgrade
// contract. plan.BuildOverFetchLimitExpr puts the over-fetched budget k' on
// node.Limit; this proves the value the SEARCH actually receives is k', by running
// the planner's real expression through evalLimitExpression -- the exact function
// every vector TVF Prepare uses to turn arg.Limit into its candidate budget, on new
// and pre-change CNs alike.
//
// It also pins the combination step. A new CN takes max(IndexReaderParam.Limit,
// arg.Limit); with the raw k on the former and k' on the latter that must resolve to
// k', not to k, and must not over-fetch a second time.
func TestOverFetchLimitExprResolvesInTVF(t *testing.T) {
	ctx := context.Background()
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	for _, k := range []uint64{0, 1, 2, 9, 10, 11, 49, 50, 51, 99, 100, 199, 200, 201, 1000} {
		want := overfetch.PostFilterLimit(k)

		// A non-literal k, so BuildOverFetchLimitExpr takes the expression path a
		// prepared LIMIT ? would. `k + 0` stands in for the bound parameter.
		nonLit, err := plan2.BindFuncExprImplByPlanExpr(ctx, "+", []*planpb.Expr{
			makeU64(k), makeU64(0)})
		require.NoError(t, err)

		budgetExpr, err := plan2.BuildOverFetchLimitExpr(ctx, nonLit, false)
		require.NoError(t, err)
		require.NotNil(t, budgetExpr)

		// This is what the TVF does with arg.Limit.
		got, err := evalLimitExpression(proc, budgetExpr, 1)
		require.NoError(t, err)

		// And this is the new CN's combination with the raw k on IndexReaderParam.
		raw, err := evalLimitExpression(proc, makeU64(k), 0)
		require.NoError(t, err)
		combined := max(raw, got)

		t.Logf("k=%-6d  IndexReaderParam.Limit=%-6d  node.Limit(expr)=%-8d  TVF budget=%-8d  overfetch.PostFilterLimit=%d",
			k, raw, got, combined, want)

		require.Equal(t, want, got,
			"k=%d: the TVF resolved a different budget than overfetch.PostFilterLimit", k)
		require.Equal(t, want, combined,
			"k=%d: max(raw k, k') must be k' -- otherwise the search under-fetches", k)
	}
}

// TestOverFetchLimitNeverNilForOldExecutor is the regression the rolling-upgrade
// review asked for. A pre-change CN reads arg.Limit and nothing else;
// evalLimitExpression's nil default is 1, so a nil node.Limit makes that CN search
// for a single candidate and silently under-return before the post-filter JOIN.
// The planner must therefore never emit nil here.
func TestOverFetchLimitNeverNilForOldExecutor(t *testing.T) {
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", m)

	// The failure mode being guarded against, stated explicitly.
	underReturned, err := evalLimitExpression(proc, nil, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1), underReturned,
		"a nil arg.Limit resolves to a single candidate on any CN -- this is why it must never be emitted")

	// A real budget resolves to the full over-fetched value instead.
	budgetExpr, err := plan2.BuildOverFetchLimitExpr(context.Background(), makeU64(2), false)
	require.NoError(t, err)
	require.NotNil(t, budgetExpr, "a non-nil limit must always produce a budget")
	got, err := evalLimitExpression(proc, budgetExpr, 1)
	require.NoError(t, err)
	require.Equal(t, overfetch.PostFilterLimit(2), got)
}

func makeU64(v uint64) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_uint64), NotNullable: true},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Isnull: false,
			Value:  &planpb.Literal_U64Val{U64Val: v},
		}},
	}
}
