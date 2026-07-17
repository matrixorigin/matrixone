// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func collectPlanParamTypes(queryPlan *Plan) []types.T {
	var result []types.T
	query := queryPlan.GetQuery()
	if query == nil {
		return result
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collectExprParamTypes(expr, &result)
		}
		for _, expr := range node.FilterList {
			collectExprParamTypes(expr, &result)
		}
		if rowset := node.RowsetData; rowset != nil {
			for _, col := range rowset.Cols {
				for _, data := range col.Data {
					collectExprParamTypes(data.Expr, &result)
				}
			}
		}
	}
	return result
}

func TestPreparedNumericContextUsesInsertValuesTarget(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "binary arithmetic",
			sql:  "insert into constraint_test.emp (sal) values (? + ?)",
			want: types.T_decimal64,
		},
		{
			name: "mod function",
			sql:  "insert into constraint_test.emp (sal) values (mod(?, ?))",
			want: types.T_decimal64,
		},
		{
			name: "double sibling overrides decimal target",
			sql:  "insert into constraint_test.emp (sal) values ((? + ?) + cast(1 as double))",
			want: types.T_float64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectPlanParamTypes(queryPlan)
			require.Len(t, paramTypes, 2)
			require.Equal(t, test.want, paramTypes[0])
			require.Equal(t, test.want, paramTypes[1])
		})
	}
}

func TestPreparedNumericContextUsesInsertSelectTarget(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       types.T
		paramCount int
	}{
		{
			name:       "direct select",
			sql:        "insert into constraint_test.emp (sal) select ? + ?",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "parenthesized select",
			sql:        "insert into constraint_test.emp (sal) (select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "union select",
			sql:        "insert into constraint_test.emp (sal) (select ? + ? union all select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name:       "double sibling overrides target",
			sql:        "insert into constraint_test.emp (sal) select (? + ?) + cast(1 as double)",
			want:       types.T_float64,
			paramCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(queryPlan)
			require.Len(t, paramTypes, test.paramCount)
			for _, typ := range paramTypes {
				require.Equal(t, test.want, typ)
			}
		})
	}
}

func collectUniquePlanParamTypes(queryPlan *Plan) map[int32]types.T {
	result := make(map[int32]types.T)
	query := queryPlan.GetQuery()
	if query == nil {
		return result
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collectExprParamTypesByPos(expr, result)
		}
		for _, expr := range node.FilterList {
			collectExprParamTypesByPos(expr, result)
		}
		for _, expr := range node.OnUpdateExprs {
			collectExprParamTypesByPos(expr, result)
		}
		if dedup := node.DedupJoinCtx; dedup != nil {
			for _, expr := range dedup.UpdateColExprList {
				collectExprParamTypesByPos(expr, result)
			}
		}
		if rowset := node.RowsetData; rowset != nil {
			for _, col := range rowset.Cols {
				for _, data := range col.Data {
					collectExprParamTypesByPos(data.Expr, result)
				}
			}
		}
	}
	return result
}

func collectExprParamTypesByPos(expr *planpb.Expr, result map[int32]types.T) {
	collectExprEffectiveParamTypes(expr, types.T_any, func(pos int32, typ types.T) {
		result[pos] = typ
	})
}

func TestPreparedNumericContextUsesUpdateTarget(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "update decimal assignment",
			sql:  "update constraint_test.emp set sal = ? + ? where empno = 1",
			want: types.T_decimal64,
		},
		{
			name: "update double sibling overrides target",
			sql:  "update constraint_test.emp set sal = (? + ?) + cast(1 as double) where empno = 1",
			want: types.T_float64,
		},
		{
			name: "on duplicate key update decimal assignment",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = ? + ?",
			want: types.T_decimal64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(queryPlan)
			require.Len(t, paramTypes, 2)
			for _, typ := range paramTypes {
				require.Equal(t, test.want, typ)
			}
		})
	}
}

func collectExprParamTypes(expr *planpb.Expr, result *[]types.T) {
	collectExprEffectiveParamTypes(expr, types.T_any, func(_ int32, typ types.T) {
		*result = append(*result, typ)
	})
}

func collectExprEffectiveParamTypes(expr *planpb.Expr, inherited types.T, collect func(int32, types.T)) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		typ := inherited
		if typ == types.T_any {
			typ = types.T(expr.Typ.Id)
		}
		collect(param.Pos, typ)
		return
	}
	if fn := expr.GetF(); fn != nil {
		childType := inherited
		if fn.Func != nil && fn.Func.ObjName == "cast" {
			childType = types.T(expr.Typ.Id)
		}
		for _, arg := range fn.Args {
			collectExprEffectiveParamTypes(arg, childType, collect)
		}
	}
}
