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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestJSONNumericCorrelationsDoNotDefinePartitions(t *testing.T) {
	ctx := context.Background()
	col := func(typ types.T) *planpb.Expr {
		return GetColExpr(typ.ToType().PlanType(), 10, 0)
	}
	corr := func(typ types.T) *planpb.Expr {
		return &planpb.Expr{Typ: typ.ToType().PlanType(), Expr: &planpb.Expr_Corr{
			Corr: &planpb.CorrColRef{RelPos: 20, ColPos: 0, Depth: 1},
		}}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		t.Helper()
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	for _, typ := range []types.T{
		types.T_int8, types.T_int16, types.T_int32, types.T_int64,
		types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
		types.T_float32, types.T_float64,
		types.T_decimal64, types.T_decimal128, types.T_decimal256,
	} {
		for _, innerJSON := range []bool{true, false} {
			for _, reversed := range []bool{false, true} {
				for _, op := range []string{"=", "<=>"} {
					t.Run(fmt.Sprintf("%s/innerJSON=%t/reversed=%t/%s", typ, innerJSON, reversed, op), func(t *testing.T) {
						innerType, outerType := typ, types.T_json
						if innerJSON {
							innerType, outerType = outerType, innerType
						}
						args := []*planpb.Expr{col(innerType), corr(outerType)}
						if reversed {
							args[0], args[1] = args[1], args[0]
						}
						keys, ok := correlatedPaginationPartitionKeys([]*planpb.Expr{bind(op, args...)})
						require.False(t, ok)
						require.Nil(t, keys)
					})
				}
			}
		}
	}

	for _, typ := range []types.T{types.T_int64, types.T_json} {
		keys, ok := correlatedPaginationPartitionKeys([]*planpb.Expr{bind("=", col(typ), corr(typ))})
		require.True(t, ok)
		require.Len(t, keys, 1)
		require.Equal(t, typ, types.T(keys[0].Typ.Id))
	}
	cast, err := appendCastBeforeExpr(ctx, col(types.T_json), types.T_float32.ToType().PlanType())
	require.NoError(t, err)
	keys, ok := correlatedPaginationPartitionKeys([]*planpb.Expr{bind("=", cast, corr(types.T_float32))})
	require.True(t, ok)
	require.Len(t, keys, 1)
	require.Equal(t, types.T_float32, types.T(keys[0].Typ.Id))

	// An outer-only mixed predicate does not divide the inner input at all.
	keys, ok = correlatedPaginationPartitionKeys([]*planpb.Expr{bind("=", corr(types.T_json), MakePlan2Int64ConstExprWithType(1))})
	require.True(t, ok)
	require.Empty(t, keys)
	valid := bind("=", col(types.T_int64), corr(types.T_int64))
	mixed := bind("=", col(types.T_json), corr(types.T_float32))
	keys, ok = correlatedPaginationPartitionKeys([]*planpb.Expr{bind("and", valid, mixed)})
	require.False(t, ok)
	require.Nil(t, keys, "a safe conjunct must not hide an unsafe partition proof")
}

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
