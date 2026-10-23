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

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestPreparedIgnoreAssignmentKeepsRuntimeCast(t *testing.T) {
	tests := []string{
		"prepare stmt from 'insert ignore into constraint_test.emp (empno) values (?)'",
		"prepare stmt from 'update ignore constraint_test.emp set sal = ? where empno = ?'",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			casts := queryFunctionsNamed(resolveQueryPlan(prepared).GetQuery(), "cast_ignore")
			require.NotEmpty(t, casts, "prepared IGNORE assignment must use cast_ignore")
			foundDirectParam := false
			for _, cast := range casts {
				args := cast.GetF().GetArgs()
				if len(args) == 2 && args[0].GetP() != nil {
					foundDirectParam = true
					break
				}
				if len(args) != 3 {
					continue
				}
				_, slot, validRef := numericProgramRefIdentity(args[0])
				source, privateOwner := numericValueProgramSourceOwner(args[0])
				if !validRef || slot != 0 || !privateOwner || source == nil ||
					!numericProgramDefinitionMatchesRef(args[0]) {
					continue
				}
				if _, hasAssignmentParam := preparedNumericValueParamPositions(source)[0]; hasAssignmentParam {
					foundDirectParam = true
					break
				}
			}
			require.True(t, foundDirectParam,
				"prepared IGNORE assignment must retain its value parameter directly or in a validated paired owner")
		})
	}
}

func TestPreparedIgnoreUpsertScalarKindRefresh(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	prepared, err := runOneStmt(mock, t,
		"prepare stmt from 'insert ignore into constraint_test.emp (empno,sal) values (1,0) on duplicate key update sal=(select ?)'")
	require.NoError(t, err)
	template := prepared.GetDcl().GetPrepare().GetPlan()
	original := DeepCopyPlan(template)
	casts := queryFunctionsNamed(template.GetQuery(), "cast_ignore")
	require.NotEmpty(t, casts)
	foundPairedCast := false
	for _, cast := range casts {
		fid, overload := function.DecodeOverloadID(cast.GetF().GetFunc().GetObj())
		args := cast.GetF().GetArgs()
		if fid == function.CAST_IGNORE && overload == 0 && len(args) == 3 &&
			args[0] != nil && args[1] != nil && args[1].GetT() != nil && args[2] != nil &&
			args[2].Typ.Id == int32(types.T_bool) {
			foundPairedCast = true
			require.Equal(t, types.T_decimal64, types.T(args[1].Typ.Id))
			scope, slot, ok := numericProgramRefIdentity(args[0])
			require.True(t, ok)
			require.Equal(t, int64(0), slot)
			kindScope, kindSlot, kindOK := numericProgramRefIdentity(args[2])
			require.True(t, kindOK)
			require.Equal(t, scope, kindScope)
			require.Equal(t, int64(1), kindSlot)
			source, privateOwner := numericValueProgramSourceOwner(args[0])
			require.True(t, privateOwner)
			require.NotNil(t, source)
			require.True(t, numericProgramDefinitionMatchesRef(args[0]))
			require.NotNil(t, source.GetDependent(), "ODKU scalar value must remain a dependent owner")
			require.Equal(t, args[1].Typ.Id, cast.Typ.Id)
		}
	}
	require.True(t, foundPairedCast, "prepared ODKU assignment must retain its optional BOOL source-kind companion")
	bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(context.Background(), template, []any{
		ParamValue{Value: "1.25", IsBinaryProtocol: true, RuntimeType: types.New(types.T_varchar, 4, 0), HasRuntimeType: true},
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.True(t, proto.Equal(original, template), "prepared ODKU template must remain immutable")
	boundCasts := queryFunctionsNamed(bound.GetQuery(), "cast_ignore")
	require.NotEmpty(t, boundCasts)
	for _, cast := range boundCasts {
		fid, overload := function.DecodeOverloadID(cast.GetF().GetFunc().GetObj())
		if fid == function.CAST_IGNORE {
			require.Equal(t, int32(0), overload)
			require.Len(t, cast.GetF().GetArgs(), 3)
			require.Equal(t, int32(types.T_bool), cast.GetF().GetArgs()[2].Typ.Id)
			require.Equal(t, cast.GetF().GetArgs()[1].Typ.Id, cast.Typ.Id)
		}
	}
}

func TestPreparedAssignmentCastOwnerRejectsMalformedEnvelope(t *testing.T) {
	ctx := context.Background()
	target := types.New(types.T_decimal64, 10, 2)
	targetExpr := &planpb.Expr{Typ: makePlan2Type(&target), Expr: &planpb.Expr_T{T: &planpb.TargetType{}}}
	cast, err := BindFuncExprImplByPlanExpr(ctx, "cast_ignore", []*planpb.Expr{
		makePlan2StringConstExprWithType("1.25"), targetExpr,
	})
	require.NoError(t, err)
	cast.GetF().Args = append(cast.GetF().Args, makePlan2BoolConstExprWithType(false))

	for _, tc := range []struct {
		name   string
		mutate func(*planpb.Expr)
	}{
		{
			name: "unknown overload",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Func.Obj = function.EncodeOverloadID(function.CAST_IGNORE, 1)
			},
		},
		{
			name: "non-BOOL source kind",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Args[2] = makePlan2Int64ConstExprWithType(1)
			},
		},
		{
			name: "function ID/name mismatch",
			mutate: func(expr *planpb.Expr) {
				expr.GetF().Func.ObjName = "cast_assign"
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			malformed := DeepCopyExpr(cast)
			tc.mutate(malformed)
			_, err := rebindExplicitPreparedCast(ctx, malformed, malformed.GetF().Args)
			require.Error(t, err)
		})
	}
}

func queryFunctionsNamed(query *planpb.Query, want string) []*planpb.Expr {
	if query == nil {
		return nil
	}
	var result []*planpb.Expr
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		for _, expr := range node.ProjectList {
			result = appendFunctionExpressions(result, expr, want)
		}
		for _, expr := range node.FilterList {
			result = appendFunctionExpressions(result, expr, want)
		}
		for _, expr := range node.OnUpdateExprs {
			result = appendFunctionExpressions(result, expr, want)
		}
		if node.DedupJoinCtx != nil {
			for _, expr := range node.DedupJoinCtx.UpdateColExprList {
				result = appendFunctionExpressions(result, expr, want)
			}
		}
		if node.RowsetData != nil {
			for _, col := range node.RowsetData.Cols {
				for _, row := range col.Data {
					if row != nil {
						result = appendFunctionExpressions(result, row.Expr, want)
					}
				}
			}
		}
	}
	return result
}

func appendFunctionExpressions(result []*planpb.Expr, expr *planpb.Expr, want string) []*planpb.Expr {
	if expr == nil {
		return result
	}
	if function := expr.GetF(); function != nil {
		if function.Func != nil && function.Func.GetObjName() == want {
			result = append(result, expr)
		}
		for _, arg := range function.Args {
			result = appendFunctionExpressions(result, arg, want)
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			result = appendFunctionExpressions(result, item, want)
		}
	}
	return result
}
