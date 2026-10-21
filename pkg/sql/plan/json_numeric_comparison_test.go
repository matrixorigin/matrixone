// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestJSONNumericComparisonRetainsTypedResidual(t *testing.T) {
	col := func(typ types.Type, rel, pos int32) *planpb.Expr {
		t := typ.PlanType()
		t.NotNullable = true
		return &planpb.Expr{Typ: t, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	for _, reversed := range []bool{false, true} {
		args := []*planpb.Expr{col(types.T_json.ToType(), 0, 0), col(types.New(types.T_decimal128, 25, 10), 1, 0)}
		if reversed {
			args[0], args[1] = args[1], args[0]
		}
		eq, err := BindFuncExprImplByPlanExpr(context.Background(), "=", args)
		require.NoError(t, err)
		require.Equal(t, args, eq.GetF().Args, "binding must not insert a lossy cast")
		require.False(t, eq.Typ.NotNullable)
		require.False(t, IsEquiJoinCondition(eq))
		require.False(t, isEquiCond(eq, map[int32]bool{0: true}, map[int32]bool{1: true}))
		strict, _, _ := checkStrictJoinPred(eq)
		require.False(t, strict, "typed equality must not imply column substitutability")
		require.False(t, isTruncationSafePredicateExpr(eq))
		left, right := getJoinCondCol(eq, 0, 1)
		require.Nil(t, left)
		require.Nil(t, right)
		require.False(t, IsEquiJoin2([]*planpb.Expr{eq}))
		required, err := RequiredPersistedExpressionProtocolVersion(eq)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion127, required)
		// A separate legal key remains available; the mixed equality is retained.
		key, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{col(types.T_int64.ToType(), 0, 1), col(types.T_int64.ToType(), 1, 1)})
		require.NoError(t, err)
		require.True(t, IsEquiJoin2([]*planpb.Expr{eq, key}))
		builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{{OnList: []*planpb.Expr{eq, key}}}}}
		builder.removeRedundantJoinCond(0, make(map[[2]int32]int), nil)
		require.Equal(t, []*planpb.Expr{eq, key}, builder.qry.Nodes[0].OnList)
	}
	// A numeric indexed column compared with JSON must use the SQL kernel,
	// rather than serialize JSON as a physical index lookup or merge into IN.
	jsonValue := col(types.T_json.ToType(), 1, 0)
	jsonNumber, err := types.ParseStringToByteJson("1.5")
	require.NoError(t, err)
	encoded, err := types.EncodeJson(jsonNumber)
	require.NoError(t, err)
	jsonValue.Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: string(encoded)}}}
	filter, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{col(types.T_int64.ToType(), 0, 0), jsonValue})
	require.NoError(t, err)
	kind, _ := checkIndexFilter(filter.GetF())
	require.Equal(t, UnsupportedIndexCondition, kind)
	_, _, ok := extractEqualConst(filter)
	require.False(t, ok)
	_, domain, _ := classifyDomainConjunct(filter)
	require.Equal(t, domainOther, domain)
	require.False(t, function.CanUseZoneMapComparison(filter.GetF().Func.Obj, filter.GetF().Args))
}
