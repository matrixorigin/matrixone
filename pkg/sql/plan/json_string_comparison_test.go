// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// TestJSONStringComparisonRetainsTypedResidual mirrors the mixed JSON/numeric
// residual contract for SQL character peers (#31121): a mixed JSON/string
// equality has no shared hash-key domain, so key extraction, equality
// substitution and join-condition classification must retain it as a typed
// residual while a separate valid key stays available.
func TestJSONStringComparisonRetainsTypedResidual(t *testing.T) {
	ctx := context.Background()
	col := func(typ types.Type, rel, pos int32) *planpb.Expr {
		p := typ.PlanType()
		p.NotNullable = true
		return &planpb.Expr{Typ: p, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	for _, peerType := range []types.Type{
		types.T_varchar.ToType(),
		types.T_char.ToType(),
		types.T_text.ToType(),
	} {
		for _, reversed := range []bool{false, true} {
			args := []*planpb.Expr{col(types.T_json.ToType(), 0, 0), col(peerType, 1, 0)}
			if reversed {
				args[0], args[1] = args[1], args[0]
			}
			eq, err := BindFuncExprImplByPlanExpr(ctx, "=", args)
			require.NoError(t, err)
			require.Equal(t, args, eq.GetF().Args, "binding must not insert a lossy cast")
			require.False(t, eq.Typ.NotNullable)
			require.False(t, IsEquiJoinCondition(eq),
				"mixed JSON/string equality must not become a hash-join key")
			require.False(t, isEquiCond(eq, map[int32]bool{0: true}, map[int32]bool{1: true}))
			strict, _, _ := checkStrictJoinPred(eq)
			require.False(t, strict, "typed equality must not imply column substitutability")
			require.False(t, isTruncationSafePredicateExpr(eq))
			left, right := getJoinCondCol(eq, 0, 1)
			require.Nil(t, left)
			require.Nil(t, right)
			require.False(t, IsEquiJoin2([]*planpb.Expr{eq}))
			// A separate legal key remains available; the mixed equality is retained.
			key, err := BindFuncExprImplByPlanExpr(ctx, "=",
				[]*planpb.Expr{col(types.T_int64.ToType(), 0, 1), col(types.T_int64.ToType(), 1, 1)})
			require.NoError(t, err)
			require.True(t, IsEquiJoin2([]*planpb.Expr{eq, key}))
			builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{{OnList: []*planpb.Expr{eq, key}}}}}
			builder.removeRedundantJoinCond(0, make(map[[2]int32]int), nil)
			require.Equal(t, []*planpb.Expr{eq, key}, builder.qry.Nodes[0].OnList)
		}
	}

	// A character column compared with an encoded JSON literal must stay a SQL
	// residual: the JSON payload is not a physical character key.
	varcharCol := col(types.T_varchar.ToType(), 0, 0)
	jsonValue := col(types.T_json.ToType(), 1, 0)
	jsonString, err := types.ParseStringToByteJson(`"three"`)
	require.NoError(t, err)
	encoded, err := types.EncodeJson(jsonString)
	require.NoError(t, err)
	jsonValue.Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: string(encoded)}}}
	filter, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{varcharCol, jsonValue})
	require.NoError(t, err)
	kind, _ := checkIndexFilter(filter.GetF())
	require.Equal(t, UnsupportedIndexCondition, kind)
	_, _, ok := extractEqualConst(filter)
	require.False(t, ok, "an encoded JSON payload must not substitute a character constant key")
	_, domain, _ := classifyDomainConjunct(filter)
	require.Equal(t, domainOther, domain)
	require.False(t, function.CanUseZoneMapComparison(filter.GetF().Func.Obj, filter.GetF().Args))
}

// TestJSONStringEqualityPinsSingleJSONStringValue pins the ONLY_FULL_GROUP_BY
// single-value relaxation for a character literal compared with a JSON
// column: the predicate selects exactly one JSON string value, so the filter
// keeps the column single-valued; the prepared adapters keep their runtime
// category uncertainty and must never provide that proof.
func TestJSONStringEqualityPinsSingleJSONStringValue(t *testing.T) {
	ctx := context.Background()
	for _, reversed := range []bool{false, true} {
		args := []*planpb.Expr{
			GetColExpr(types.T_json.ToType().PlanType(), 10, 0),
			makePlan2StringConstExprWithType("three"),
		}
		if reversed {
			args[0], args[1] = args[1], args[0]
		}
		equality, err := BindFuncExprImplByPlanExpr(ctx, "=", args)
		require.NoError(t, err)
		require.True(t, filterListHasSingleValueEqualityOnCol([]*planpb.Expr{equality}, 10, 0),
			"a character literal pins the one JSON string value that equals it")
	}
	for _, name := range []string{function.JsonComparisonParamFunctionName, function.JsonOrderingParamFunctionName} {
		adapter, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
			{Typ: types.T_text.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
		})
		require.NoError(t, err)
		require.False(t, isMySQLFullGroupBySingleValueExpr(adapter),
			"runtime parameter metadata must not provide the single-value proof")
	}
}
