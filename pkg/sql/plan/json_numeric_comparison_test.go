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
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestJSONNumericPredicatesDoNotDefinePhysicalLockKeys(t *testing.T) {
	for _, operation := range []string{
		"update nation set n_name='x' where %s",
		"delete from nation where %s",
		"select n_nationkey from nation where %s for update",
	} {
		for _, tc := range []struct {
			name, predicate string
			prepared        bool
			wantKey         bool
		}{
			{"json_left", "cast('1' as json)=n_nationkey", false, false},
			{"json_right", "n_nationkey=cast('1' as json)", false, false},
			{"json_single_in", "n_nationkey in (cast('1' as json))", false, false},
			{"json_multiple_in", "n_nationkey in (cast('1' as json),cast('2' as json))", false, false},
			{"prepared_json_left", "cast(? as json)=n_nationkey", true, false},
			{"prepared_json_right", "n_nationkey=cast(? as json)", true, false},
			{"numeric", "n_nationkey=1", false, true},
			{"numeric_in", "n_nationkey in (1,2)", false, true},
			{"explicit_numeric_cast", "n_nationkey=cast(cast('1' as json) as int)", false, true},
			{"independent_numeric_key", "n_nationkey=1 and n_nationkey=cast('1' as json)", false, true},
		} {
			t.Run(fmt.Sprintf("%s/%s", operation, tc.name), func(t *testing.T) {
				statement := fmt.Sprintf(operation, tc.predicate)
				if tc.prepared {
					statement = fmt.Sprintf("prepare json_lock from %q", statement)
				}
				proc := newPlanTestProcess(t)
				logic, err := runOneStmt(NewMockOptimizer(true, proc), t, statement)
				require.NoError(t, err)
				if tc.prepared {
					logic = logic.GetDcl().GetPrepare().Plan
				}
				query := logic.GetQuery()
				require.NotNil(t, query)
				var targets int
				for _, node := range query.Nodes {
					for _, target := range node.LockTargets {
						if target.TableId != query.Nodes[0].TableDef.TblId {
							continue
						}
						targets++
						require.False(t, target.LockTableAtTheEnd, "an unsafe key must not become a whole-table lock")
						if tc.wantKey {
							require.NotNil(t, target.LockRows)
							if payload := target.LockRows.GetVec(); payload != nil {
								decoded := vector.NewVec(types.T_any.ToType())
								defer decoded.Free(proc.Mp())
								require.NoError(t, decoded.UnmarshalBinary(payload.Data))
								require.Equal(t, target.PrimaryColTyp.Id, int32(decoded.GetType().Oid))
								require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](decoded))
							} else {
								require.Equal(t, target.PrimaryColTyp.Id, target.LockRows.Typ.Id)
							}
						} else {
							require.Nil(t, target.LockRows, "mixed comparison values are not physical primary keys")
						}
					}
				}
				require.NotZero(t, targets, "the public statement must reach row locking")
			})
		}
	}
}

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
						predicate := bind(op, args...)
						keys, ok := correlatedPaginationPartitionKeys([]*planpb.Expr{predicate})
						require.False(t, ok)
						require.Nil(t, keys)
						key, ok := correlatedWindowIdentityKey(predicate)
						require.False(t, ok)
						require.Nil(t, key)
						outer, ok := correlatedIdentityOuterKey(predicate)
						require.False(t, ok)
						require.Nil(t, outer)
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
		key, ok := correlatedWindowIdentityKey(bind("=", col(typ), corr(typ)))
		require.True(t, ok)
		require.NotNil(t, key)
		outer, ok := correlatedIdentityOuterKey(bind("=", col(typ), corr(typ)))
		require.True(t, ok)
		require.NotNil(t, outer)
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

func TestJSONNumericEqualityDoesNotFixOnePhysicalValue(t *testing.T) {
	ctx := context.Background()
	for _, typ := range []types.T{types.T_int64, types.T_float32, types.T_decimal128} {
		for _, jsonColumn := range []bool{true, false} {
			for _, reversed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/jsonColumn=%t/reversed=%t", typ, jsonColumn, reversed), func(t *testing.T) {
					columnType, peerType := typ, types.T_json
					if jsonColumn {
						columnType, peerType = peerType, columnType
					}
					column := GetColExpr(columnType.ToType().PlanType(), 10, 0)
					peer, err := appendCastBeforeExpr(ctx, MakePlan2Int64ConstExprWithType(1), peerType.ToType().PlanType())
					require.NoError(t, err)
					args := []*planpb.Expr{column, peer}
					if reversed {
						args[0], args[1] = args[1], args[0]
					}
					equality, err := BindFuncExprImplByPlanExpr(ctx, "=", args)
					require.NoError(t, err)
					require.False(t, filterListHasSingleValueEqualityOnCol([]*planpb.Expr{equality}, 10, 0))

					// A column-to-column dependency must not reintroduce the proof.
					other := GetColExpr(peerType.ToType().PlanType(), 20, 0)
					equality, err = BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{column, other})
					require.NoError(t, err)
					relation := newFullGroupByRelation()
					relation.columns[fullGroupByColumn{10, 0}] = column.Typ
					relation.columns[fullGroupByColumn{20, 0}] = other.Typ
					_, _, ok := relation.equality(equality)
					require.False(t, ok)
				})
			}
		}
	}
	for _, typ := range []types.T{types.T_int64, types.T_json} {
		column := GetColExpr(typ.ToType().PlanType(), 10, 0)
		peer, err := appendCastBeforeExpr(ctx, MakePlan2Int64ConstExprWithType(1), typ.ToType().PlanType())
		require.NoError(t, err)
		equality, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{column, peer})
		require.NoError(t, err)
		require.True(t, filterListHasSingleValueEqualityOnCol([]*planpb.Expr{equality}, 10, 0))
	}
	// Prepared transport normalizes to JSON, but its runtime numeric metadata
	// can select a different comparison domain on each execution.
	for _, name := range []string{function.JsonComparisonParamFunctionName, function.JsonOrderingParamFunctionName} {
		adapter, err := BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{
			{Typ: types.T_text.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
		})
		require.NoError(t, err)
		require.False(t, isMySQLFullGroupBySingleValueExpr(adapter))
	}
	for _, reversed := range []bool{false, true} {
		args := []*planpb.Expr{
			GetColExpr(types.T_json.ToType().PlanType(), 10, 0),
			{Typ: types.T_any.ToType().PlanType(), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
		}
		if reversed {
			args[0], args[1] = args[1], args[0]
		}
		equality, err := BindFuncExprImplByPlanExpr(ctx, "=", args)
		require.NoError(t, err)
		require.False(t, filterListHasSingleValueEqualityOnCol([]*planpb.Expr{equality}, 10, 0))
	}
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
		require.Equal(t, defines.MORPCVersion128, required)
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
