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

func TestPreparedNumericContextParameterTypes(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "no context defaults to double",
			sql:  "select ? + ?",
			want: types.T_float64,
		},
		{
			name: "cast supplies exact context through integer sibling",
			sql:  "select cast((? + ?) + 1 as decimal(30, 0))",
			want: types.T_decimal128,
		},
		{
			name: "decimal sibling supplies exact context",
			sql:  "select (? + ?) + cast(1 as decimal(20, 2))",
			want: types.T_decimal128,
		},
		{
			name: "double sibling overrides exact cast context",
			sql:  "select cast((? + ?) + cast(1 as double) as decimal(30, 0))",
			want: types.T_float64,
		},
		{
			name: "integer cast context overrides narrower integer sibling",
			sql:  "select cast((? + ?) + N_REGIONKEY as signed) from nation",
			want: types.T_int64,
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

func TestNumericContextLeavesOrdinaryArithmeticOnOriginalPath(t *testing.T) {
	tests := []string{
		"select 1 + 2",
		"select N_REGIONKEY from nation group by N_REGIONKEY having abs(nation.N_REGIONKEY - 1) > 10",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], false)
			require.NoError(t, err)
		})
	}
}

func TestNumericContextDoesNotCrossFunctionBoundary(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), "select ? + abs(?)", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)

	paramTypes := collectPlanParamTypes(queryPlan)
	require.Len(t, paramTypes, 2)
	require.Equal(t, types.T_float64, paramTypes[0])
	require.Equal(t, types.T_int64, paramTypes[1])
}

func TestPreparedNumericContextUsesColumnSiblingType(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "integer column",
			sql:  "select (? + ?) + N_REGIONKEY from nation",
			want: types.T_int32,
		},
		{
			name: "qualified integer column",
			sql:  "select (? + ?) + nation.N_REGIONKEY from nation",
			want: types.T_int32,
		},
		{
			name: "decimal column",
			sql:  "select (? + ?) + p_retailprice from part",
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

			paramTypes := collectPlanParamTypes(queryPlan)
			require.Len(t, paramTypes, 2)
			require.Equal(t, test.want, paramTypes[0])
			require.Equal(t, test.want, paramTypes[1])
		})
	}
}

func TestPreparedNumericContextCoversUnaryAndModFunction(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "context-free mod defaults to double",
			sql:  "select mod(?, ?)",
			want: types.T_float64,
		},
		{
			name: "cast context reaches mod",
			sql:  "select cast(mod(?, ?) as decimal(30, 0))",
			want: types.T_decimal128,
		},
		{
			name: "context-free unary defaults to double",
			sql:  "select -?",
			want: types.T_float64,
		},
		{
			name: "context-free unary plus defaults to double",
			sql:  "select +?",
			want: types.T_float64,
		},
		{
			name: "cast context reaches unary and nested arithmetic",
			sql:  "select cast(-(? + ?) as decimal(30, 0))",
			want: types.T_decimal128,
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
			require.NotEmpty(t, paramTypes)
			for _, typ := range paramTypes {
				require.Equal(t, test.want, typ)
			}
		})
	}
}

func TestPreparedNumericContextCoversBinaryOperators(t *testing.T) {
	tests := []string{
		"select cast(? + ? as decimal(30, 2))",
		"select cast(? - ? as decimal(30, 2))",
		"select cast(? * ? as decimal(30, 2))",
		"select cast(? / ? as decimal(30, 2))",
		"select cast(? div ? as decimal(30, 2))",
		"select cast(? mod ? as decimal(30, 2))",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectPlanParamTypes(queryPlan)
			require.Len(t, paramTypes, 2)
			require.Equal(t, types.T_decimal128, paramTypes[0])
			require.Equal(t, types.T_decimal128, paramTypes[1])
		})
	}
}

func TestNumericContextDoesNotCrossComparisonOrTemporalBoundary(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []types.T
	}{
		{
			name: "comparison",
			sql:  "select ? + cast((? = 1) as signed)",
			want: []types.T{types.T_int64, types.T_int64},
		},
		{
			name: "temporal function",
			sql:  "select ? + year(?)",
			want: []types.T{types.T_float64, types.T_date},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			require.Equal(t, test.want, collectPlanParamTypes(queryPlan))
		})
	}
}

func TestPreparedNumericInspectionPreservesGroupAndAliasState(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(
		optimizer.CurrentContext().GetContext(),
		"select (? + ?) + N_REGIONKEY as numeric_alias from nation group by N_REGIONKEY order by numeric_alias",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	require.Equal(t, []types.T{types.T_int32, types.T_int32}, collectPlanParamTypes(queryPlan))
}

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
