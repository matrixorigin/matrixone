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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
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

func TestPreparedNumericContextMergesExactSiblingTypes(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantType  types.T
		wantWidth int32
		wantScale int32
	}{
		{
			name:     "integer siblings",
			sql:      "select (? + N_REGIONKEY) + cast(0 as signed) from nation",
			wantType: types.T_int64,
		},
		{
			name:     "integer siblings in reverse order",
			sql:      "select (cast(0 as signed) + ?) + N_REGIONKEY from nation",
			wantType: types.T_int64,
		},
		{
			name:      "signed and unsigned bigint siblings",
			sql:       "select (? + cast(1 as signed)) + cast(0 as unsigned)",
			wantType:  types.T_decimal128,
			wantWidth: 38,
		},
		{
			name:      "decimal siblings",
			sql:       "select (? + cast(1 as decimal(10, 2))) + cast(0 as decimal(30, 10))",
			wantType:  types.T_decimal128,
			wantWidth: 30,
			wantScale: 10,
		},
		{
			name:      "decimal siblings in reverse order",
			sql:       "select (? + cast(0 as decimal(30, 10))) + cast(1 as decimal(10, 2))",
			wantType:  types.T_decimal128,
			wantWidth: 30,
			wantScale: 10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			paramTypes := collectPlanParamPlanTypes(queryPlan)
			require.Len(t, paramTypes, 1)
			require.Equal(t, int32(test.wantType), paramTypes[0].Id)
			if test.wantWidth != 0 {
				require.Equal(t, test.wantWidth, paramTypes[0].Width)
			}
			if test.wantScale != 0 {
				require.Equal(t, test.wantScale, paramTypes[0].Scale)
			}
		})
	}
}

func TestPreparedNumericContextUsesCorrelatedColumnType(t *testing.T) {
	for _, sql := range []string{
		"select (select ? + nation.N_REGIONKEY) from nation",
		"select (select ? + N_REGIONKEY) from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			paramTypes := collectPlanParamTypes(queryPlan)
			require.NotEmpty(t, paramTypes)
			for _, typ := range paramTypes {
				require.Equal(t, types.T_int32, typ)
			}
		})
	}
}

func TestNumericColumnTypeScopeLookup(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	parent := NewBindContext(builder, nil)
	parent.binder = NewWhereBinder(builder, parent)
	parentBinding := numericTestBinding("scope_table", "scope_col", types.T_int64.ToType())
	parent.bindingByTable[parentBinding.table] = parentBinding
	parent.bindingByCol["scope_col"] = parentBinding

	middle := NewBindContext(builder, parent)
	child := NewBindContext(builder, middle)
	binder := &baseBinder{builder: builder, ctx: child}

	typ, ok := binder.numericColumnType(tree.NewUnresolvedColName("scope_col"))
	require.True(t, ok)
	require.Equal(t, int32(types.T_int64), typ.Id)

	qualified := tree.NewUnresolvedName(tree.NewCStr("scope_table", 0), tree.NewCStr("scope_col", 0))
	typ, ok = binder.numericColumnType(qualified)
	require.True(t, ok)
	require.Equal(t, int32(types.T_int64), typ.Id)

	localBinding := numericTestBinding("local_table", "scope_col", types.T_int32.ToType())
	child.bindingByCol["scope_col"] = localBinding
	typ, ok = binder.numericColumnType(tree.NewUnresolvedColName("scope_col"))
	require.True(t, ok)
	require.Equal(t, int32(types.T_int32), typ.Id)

	child.bindingByCol["scope_col"] = nil
	_, ok = binder.numericColumnType(tree.NewUnresolvedColName("scope_col"))
	require.False(t, ok)
	delete(child.bindingByCol, "scope_col")

	aliasType := makePlan2Type(typePtrForPlanTest(types.T_int16.ToType()))
	child.aliasMap["alias_col"] = &aliasItem{idx: 0}
	child.projects = []*planpb.Expr{{Typ: aliasType}}
	typ, ok = binder.numericColumnType(tree.NewUnresolvedColName("alias_col"))
	require.True(t, ok)
	require.Equal(t, int32(types.T_int16), typ.Id)

	missingLocalTable := numericTestBinding("scope_table", "other_col", types.T_int32.ToType())
	child.bindingByTable["scope_table"] = missingLocalTable
	typ, ok = binder.numericColumnType(qualified)
	require.True(t, ok)
	require.Equal(t, int32(types.T_int64), typ.Id)

	_, ok = binder.numericColumnType(tree.NewUnresolvedColName("missing_col"))
	require.False(t, ok)
}

func TestPreparedDirectCastKeepsOriginalParameterPath(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantTarget types.T
	}{
		{name: "timestamp", sql: "select cast(? as timestamp(6))", wantTarget: types.T_timestamp},
		{name: "date", sql: "select cast(? as date)", wantTarget: types.T_date},
		{name: "time", sql: "select cast(? as time(6))", wantTarget: types.T_time},
		{name: "char", sql: "select cast(? as char(10))", wantTarget: types.T_char},
		{name: "decimal", sql: "select cast(? as decimal(20, 2))", wantTarget: types.T_decimal128},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			castPaths := collectPlanParamCastPaths(queryPlan)
			require.Len(t, castPaths, 1)
			require.Equal(t, []types.T{test.wantTarget}, castPaths[0])
		})
	}
}

func TestPreparedCastPropagatesContextOnlyIntoArithmetic(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{name: "binary arithmetic", sql: "select cast((? + ?) as char(20))", want: types.T_float64},
		{name: "unary arithmetic", sql: "select cast(-? as decimal(20, 2))", want: types.T_decimal128},
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

func TestPreparedNumericLiteralStrength(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{name: "decimal literal is weak", sql: "select 0.0 + ?", want: types.T_float64},
		{name: "approximate literal is strong", sql: "select 0e0 + ?", want: types.T_float64},
		{name: "integer literal is strong", sql: "select 0 + ?", want: types.T_int64},
		{
			name: "explicit decimal cast is strong",
			sql:  "select cast(0 as decimal(10, 1)) + ?",
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
			require.Len(t, paramTypes, 1)
			require.Equal(t, test.want, paramTypes[0])
		})
	}
}

func numericTestBinding(table, col string, typ types.Type) *Binding {
	planType := makePlan2Type(&typ)
	return &Binding{
		table:       table,
		cols:        []string{col},
		types:       []*planpb.Type{&planType},
		colIdByName: map[string]int32{col: 0},
	}
}

func typePtrForPlanTest(typ types.Type) *types.Type {
	return &typ
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

func collectPlanParamPlanTypes(queryPlan *Plan) []planpb.Type {
	var result []planpb.Type
	query := queryPlan.GetQuery()
	if query == nil {
		return result
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collectExprEffectiveParamPlanTypes(expr, planpb.Type{}, &result)
		}
	}
	return result
}

func collectPlanParamCastPaths(queryPlan *Plan) [][]types.T {
	var result [][]types.T
	query := queryPlan.GetQuery()
	if query == nil {
		return result
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collectExprParamCastPaths(expr, nil, &result)
		}
	}
	return result
}

func collectExprParamCastPaths(expr *planpb.Expr, path []types.T, result *[][]types.T) {
	if expr == nil {
		return
	}
	if expr.GetP() != nil {
		*result = append(*result, append([]types.T(nil), path...))
		return
	}
	if fn := expr.GetF(); fn != nil {
		childPath := path
		if fn.Func != nil && fn.Func.ObjName == "cast" {
			childPath = append(append([]types.T(nil), path...), types.T(expr.Typ.Id))
		}
		for _, arg := range fn.Args {
			collectExprParamCastPaths(arg, childPath, result)
		}
	}
}

func collectExprEffectiveParamPlanTypes(expr *planpb.Expr, inherited planpb.Type, result *[]planpb.Type) {
	if expr == nil {
		return
	}
	if expr.GetP() != nil {
		typ := inherited
		if typ.Id == 0 {
			typ = expr.Typ
		}
		*result = append(*result, typ)
		return
	}
	if fn := expr.GetF(); fn != nil {
		childType := inherited
		if fn.Func != nil && fn.Func.ObjName == "cast" {
			childType = expr.Typ
		}
		for _, arg := range fn.Args {
			collectExprEffectiveParamPlanTypes(arg, childType, result)
		}
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
