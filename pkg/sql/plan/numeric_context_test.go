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
	"context"
	"fmt"
	"math"
	"strconv"
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
			name: "no context remains runtime specialized",
			sql:  "select ? + ?",
			want: types.T_decimal256,
		},
		{
			name: "cast supplies exact context through integer sibling",
			sql:  "select cast((? + ?) + 1 as decimal(30, 0))",
			want: types.T_decimal128,
		},
		{
			name: "decimal sibling supplies exact context",
			sql:  "select (? + ?) + cast(1 as decimal(20, 2))",
			want: types.T_decimal256,
		},
		{
			name: "double sibling overrides exact cast context",
			sql:  "select cast((? + ?) + cast(1 as double) as decimal(30, 0))",
			want: types.T_float64,
		},
		{
			name: "integer result cast does not narrow runtime operands",
			sql:  "select cast((? + ?) + N_REGIONKEY as signed) from nation",
			want: types.T_decimal256,
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
		"select mod(2024, 4)",
		"select mod(mod(2024, 100), 4)",
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

func TestNumericContextModWithoutParametersInPrepareMode(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), "select mod(2024, 4)", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	require.Empty(t, collectPlanParamTypes(queryPlan))
}

func TestNumericContextDoesNotCrossFunctionBoundary(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), "select ? + abs(?)", 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)

	paramTypes := collectPlanParamTypes(queryPlan)
	require.Len(t, paramTypes, 2)
	require.Equal(t, types.T_decimal256, paramTypes[0])
	require.Equal(t, types.T_decimal256, paramTypes[1])
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
			want: types.T_decimal256,
		},
		{
			name: "qualified integer column",
			sql:  "select (? + ?) + nation.N_REGIONKEY from nation",
			want: types.T_decimal256,
		},
		{
			name: "decimal column",
			sql:  "select (? + ?) + p_retailprice from part",
			want: types.T_decimal256,
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

func TestPreparedNumericContextTreatsBitColumnAsUnsignedBigint(t *testing.T) {
	require.True(t, shouldActivateWeakDecimal(nil, typePtrForPlanTest(types.New(types.T_bit, 64, 0))))

	tests := []struct {
		name      string
		sql       string
		want      types.T
		wantWidth int32
		wantScale int32
	}{
		{
			name: "bit only",
			sql:  "select ? + n_regionkey from nation",
			want: types.T_decimal256,
		},
		{
			name: "bit mod function",
			sql:  "select mod(?, n_regionkey) from nation",
			want: types.T_decimal256,
		},
		{
			name:      "bit with weak decimal",
			sql:       "select (? + 0.5) + n_regionkey from nation",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "bit mod function with weak decimal",
			sql:       "select mod(? + 0.5, n_regionkey) from nation",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "bit with weak decimal reverse order",
			sql:       "select n_regionkey + (0.5 + ?) from nation",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			optimizer.ctxt.tables["nation"].Cols[2].Typ = makePlan2Type(typePtrForPlanTest(
				types.New(types.T_bit, 64, 0),
			))
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
			paramTypes := collectPlanParamPlanTypes(queryPlan)
			require.Len(t, paramTypes, 1)
			require.Equal(t, int32(test.want), paramTypes[0].Id)
			if test.wantWidth != 0 {
				require.Equal(t, test.wantWidth, paramTypes[0].Width)
			}
			if test.wantScale != 0 {
				require.Equal(t, test.wantScale, paramTypes[0].Scale)
			}
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
			name: "context-free mod remains runtime specialized",
			sql:  "select mod(?, ?)",
			want: types.T_decimal256,
		},
		{
			name: "cast context reaches mod",
			sql:  "select cast(mod(?, ?) as decimal(30, 0))",
			want: types.T_decimal128,
		},
		{
			name: "nested mod uses integer sibling context",
			sql:  "select mod(mod(?, 100), 4)",
			want: types.T_decimal256,
		},
		{
			name: "context-free unary remains runtime specialized",
			sql:  "select -?",
			want: types.T_decimal256,
		},
		{
			name: "context-free unary plus remains runtime specialized",
			sql:  "select +?",
			want: types.T_decimal256,
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
			want: []types.T{types.T_decimal256, types.T_decimal256},
		},
		{
			name: "temporal function",
			sql:  "select ? + year(?)",
			want: []types.T{types.T_decimal256, types.T_date},
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

func TestPreparedNumericContextPreservesTemporalSubtraction(t *testing.T) {
	tests := []string{
		"select O_ORDERDATE - ? from orders",
		"select ? - O_ORDERDATE from orders",
		"select cast('2024-01-02 03:04:05' as datetime) - ?",
		"select ? - cast('2024-01-02 03:04:05' as datetime)",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)
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
	require.Equal(t, []types.T{types.T_decimal256, types.T_decimal256}, collectPlanParamTypes(queryPlan))
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
			wantType: types.T_decimal256,
		},
		{
			name:     "integer siblings in reverse order",
			sql:      "select (cast(0 as signed) + ?) + N_REGIONKEY from nation",
			wantType: types.T_decimal256,
		},
		{
			name:      "signed and unsigned bigint siblings",
			sql:       "select (? + cast(1 as signed)) + cast(0 as unsigned)",
			wantType:  types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "decimal siblings",
			sql:       "select (? + cast(1 as decimal(10, 2))) + cast(0 as decimal(30, 10))",
			wantType:  types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "decimal siblings in reverse order",
			sql:       "select (? + cast(0 as decimal(30, 10))) + cast(1 as decimal(10, 2))",
			wantType:  types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
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
				require.Equal(t, types.T_decimal256, typ)
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
		name      string
		sql       string
		want      types.T
		wantWidth int32
		wantScale int32
	}{
		{name: "decimal literal preserves dynamic execute values", sql: "select 0.0 + ?", want: types.T_decimal256},
		{name: "approximate literal is strong", sql: "select 0e0 + ?", want: types.T_float64},
		{
			name:      "integer literal preserves dynamic execute values",
			sql:       "select 0 + ?",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "parameter before integer literal preserves dynamic execute values",
			sql:       "select ? + 1",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{name: "null literal remains unknown", sql: "select null + ?", want: types.T_float64},
		{
			name: "explicit decimal cast preserves dynamic execute values",
			sql:  "select cast(0 as decimal(10, 1)) + ?",
			want: types.T_decimal256,
		},
		{
			name:      "weak decimal beats integer sibling in safe decimal domain",
			sql:       "select (? + 0.5) + cast(0 as signed)",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "weak decimal beats integer sibling in reverse order",
			sql:       "select (cast(0 as signed) + ?) + 0.5",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "weak decimal includes unsigned bigint capacity",
			sql:       "select (? + 0.5) + cast(0 as unsigned)",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "integer result cast does not narrow weak decimal arithmetic",
			sql:       "select cast(? + 0.5 as signed)",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name: "approximate outer context beats weak decimal",
			sql:  "select cast(? + 0.5 as double)",
			want: types.T_float64,
		},
		{
			name:      "weak decimal survives unary traversal",
			sql:       "select (-? + 0.5) + cast(0 as signed)",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
		},
		{
			name:      "weak decimal survives mod traversal",
			sql:       "select mod(?, 0.5) + cast(0 as signed)",
			want:      types.T_decimal256,
			wantWidth: 65,
			wantScale: 30,
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
			if test.name == "null literal remains unknown" {
				require.Empty(t, paramTypes, "NULL arithmetic is folded and does not consume the runtime value")
				return
			}
			require.Len(t, paramTypes, 1)
			require.Equal(t, int32(test.want), paramTypes[0].Id)
			if test.wantWidth != 0 {
				require.Equal(t, test.wantWidth, paramTypes[0].Width)
			}
			if test.wantScale != 0 {
				require.Equal(t, test.wantScale, paramTypes[0].Scale)
			}
		})
	}
}

func TestPrepareStatementKeepsDynamicNumericLiteralDomain(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? + 1")
	paramTypes := collectPlanParamTypes(prepare.Plan)
	require.Equal(t, []types.T{types.T_decimal256}, paramTypes)
	require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
	require.NotEmpty(t, prepare.Plan.GetQuery().Nodes)
	root := prepare.Plan.GetQuery().Nodes[prepare.Plan.GetQuery().Steps[0]]
	require.NotEmpty(t, root.ProjectList)
	require.Equal(t, int32(types.T_decimal256), root.ProjectList[0].Typ.Id)
}

func TestPreparedDynamicNumericPlanSpecializesPerExecutionValue(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? + 1")
	require.True(t, HasPreparedDynamicNumericParams(prepare.Plan))

	values := []string{
		"12345678901234567890123456789012345678901234567890123456789012345",
		"-12345678901234567890123456789012345678901234567890123456789012345",
		"0.123456789012345678901234567890",
		"-0.123456789012345678901234567890",
	}
	for _, value := range values {
		t.Run(value, func(t *testing.T) {
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: value, RuntimeType: types.T_decimal256,
			}})
			require.NoError(t, err)
			require.False(t, HasPreparedDynamicNumericParams(specialized))
			paramTypes := collectPlanParamTypes(specialized)
			require.Len(t, paramTypes, 1)
			require.True(t, paramTypes[0].IsDecimal())
		})
	}

	require.True(t, HasPreparedDynamicNumericParams(prepare.Plan), "specialization must not mutate the canonical plan")

	for _, value := range []string{
		"2.5", "9007199254740993", "1e10", "1e-10", "-1e10", "+1.5E+10", ".5e2", "1.e2",
		" 1e10 ", "\t-1e10", "1e-10 ", "1e-10000", "-1e-10000",
	} {
		t.Run("scientific string "+value, func(t *testing.T) {
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: value, RuntimeType: types.T_varchar,
			}})
			require.NoError(t, err)
			root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
			require.NotEmpty(t, root.ProjectList)
			require.True(t, types.T(root.ProjectList[0].Typ.Id).IsFloat())
		})
	}
	for _, value := range []string{"10", "1.25", "-0.5"} {
		t.Run("exact decimal "+value, func(t *testing.T) {
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: value, RuntimeType: types.T_decimal128,
			}})
			require.NoError(t, err)
			root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
			require.NotEmpty(t, root.ProjectList)
			require.True(t, types.T(root.ProjectList[0].Typ.Id).IsDecimal())
		})
	}

	for _, test := range []struct {
		name        string
		value       string
		runtimeType types.T
	}{
		{name: "signed integer", value: "41", runtimeType: types.T_int64},
		{name: "signed max integer", value: strconv.FormatInt(math.MaxInt64, 10), runtimeType: types.T_int64},
		{name: "unsigned max integer", value: strconv.FormatUint(math.MaxUint64, 10), runtimeType: types.T_uint64},
	} {
		t.Run(test.name, func(t *testing.T) {
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: test.value, RuntimeType: test.runtimeType,
			}})
			require.NoError(t, err)
			root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
			resultType := types.T(root.ProjectList[0].Typ.Id)
			if test.runtimeType == types.T_uint64 {
				require.True(t, resultType.IsDecimal(), resultType.String())
			} else {
				require.True(t, resultType.IsInteger(), resultType.String())
			}
		})
	}
	for _, test := range []struct {
		name        string
		value       string
		runtimeType types.T
	}{
		{name: "float32 exponent", value: "1e+10", runtimeType: types.T_float32},
		{name: "float64 small exponent", value: "1e-10", runtimeType: types.T_float64},
		{name: "float64 large exponent", value: "1e+100", runtimeType: types.T_float64},
		{name: "float64 negative exponent", value: "-1e+10", runtimeType: types.T_float64},
		{name: "float64 infinity", value: "+Inf", runtimeType: types.T_float64},
		{name: "float64 not a number", value: "NaN", runtimeType: types.T_float64},
	} {
		t.Run(test.name, func(t *testing.T) {
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: test.value, RuntimeType: test.runtimeType,
			}})
			require.NoError(t, err)
			root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
			require.NotEmpty(t, root.ProjectList)
			require.True(t, types.T(root.ProjectList[0].Typ.Id).IsFloat())
		})
	}

	explicitCast := buildPreparedAggregatePlan(t, "select cast(? as decimal(65, 30)) + 1")
	require.False(t, HasPreparedDynamicNumericParams(explicitCast.Plan),
		"an explicit DECIMAL(65,30) cast is a user contract, not a dynamic marker")
}

func TestPreparedDynamicNumericSpecializationPreservesParamRefs(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? + 1, concat(?, 1)")
	specialized, err := SpecializePreparedNumericPlan(context.Background(), prepare.Plan, []any{
		ParamValue{Value: "2", RuntimeType: types.T_int64},
		ParamValue{Value: "first", RuntimeType: types.T_varchar},
	})
	require.NoError(t, err)
	require.Len(t, collectPlanParamTypes(specialized), 2)
	require.False(t, HasPreparedDynamicNumericParams(specialized))
}

func TestPreparedGenericNumericFunctionsSpecializeRuntimeDomain(t *testing.T) {
	for _, sql := range []string{
		"select abs(?)",
		"select sign(?)",
		"select round(?)",
		"select ceil(?)",
		"select floor(?)",
		"select greatest(?, 10)",
		"select least(?, 10)",
		"select ? > 1",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.True(t, HasPreparedDynamicNumericParams(prepare.Plan))
			for _, runtimeType := range []types.T{types.T_int64, types.T_decimal128, types.T_float64} {
				value := "2.5"
				if runtimeType == types.T_int64 {
					value = "2"
				}
				specialized, err := SpecializePreparedNumericPlan(context.Background(), prepare.Plan, []any{
					ParamValue{Value: value, RuntimeType: runtimeType},
				})
				require.NoError(t, err, runtimeType.String())
				paramTypes := collectPlanParamTypes(specialized)
				require.NotEmpty(t, paramTypes)
				if runtimeType == types.T_decimal128 {
					require.True(t, paramTypes[0].IsDecimal(), paramTypes[0].String())
				}
				if runtimeType == types.T_float64 {
					require.True(t, paramTypes[0].IsFloat(), paramTypes[0].String())
				}
			}
		})
	}
}

func TestPreparedCaseSpecializesConditionAndResultIndependently(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select case when ? > 0 then ? + 1 else 0 end")
	specialized, err := SpecializePreparedNumericPlan(context.Background(), prepare.Plan, []any{
		ParamValue{Value: "1", RuntimeType: types.T_int64},
		ParamValue{Value: "2", RuntimeType: types.T_int64},
	})
	require.NoError(t, err)
	root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
	require.Equal(t, int32(types.T_int64), root.ProjectList[0].Typ.Id)
}

func TestPreparedDynamicNumericDiscoveryCoversWindowDCLAndDDL(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "window", sql: "select sum(? + 1) over () from nation"},
		{name: "set", sql: "set @out = ? + 1"},
		{name: "ctas", sql: "create table prepared_dynamic_ctas as select ? + 1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
				fmt.Sprintf("prepare stmt1 from '%s'", test.sql))
			require.NoError(t, err)
			prepare := logicPlan.GetDcl().GetPrepare()
			require.NotNil(t, prepare)
			require.True(t, HasPreparedDynamicNumericParams(prepare.Plan))
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: strconv.FormatInt(math.MaxInt64, 10), RuntimeType: types.T_int64,
			}})
			require.NoError(t, err)
			require.False(t, HasPreparedDynamicNumericParams(specialized))
		})
	}
}

func TestDeepCopyPreparedCTASPreservesExecutionMetadata(t *testing.T) {
	original := &planpb.Plan{Plan: &planpb.Plan_Ddl{Ddl: &planpb.DataDefinition{
		Definition: &planpb.DataDefinition_CreateTable{CreateTable: &planpb.CreateTable{
			CreateAsSelectSql: "insert into cnorm select ? + 1",
			UpdateFkSqls:      []string{"update fk"},
			FksReferToMe:      []*planpb.ForeignKeyInfo{{Db: "db"}},
			RawSQL:            "create table cnorm as select ? + 1",
		}},
	}}}
	copied := DeepCopyPlan(original)
	create := copied.GetDdl().GetCreateTable()
	require.Equal(t, "insert into cnorm select ? + 1", create.CreateAsSelectSql)
	require.Equal(t, []string{"update fk"}, create.UpdateFkSqls)
	require.Equal(t, "db", create.FksReferToMe[0].Db)
	require.Equal(t, "create table cnorm as select ? + 1", create.RawSQL)
	require.NotSame(t, original.GetDdl().GetCreateTable(), create)
}

func TestPreparedCTASSchemaTracksSpecializedResultType(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare stmt1 from 'create table prepared_ctas_shape as select ? + 1 as v'")
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	specialized, err := SpecializePreparedNumericPlan(context.Background(), prepare.Plan, []any{
		ParamValue{Value: "2", RuntimeType: types.T_int64},
	})
	require.NoError(t, err)
	cols := specialized.GetDdl().GetCreateTable().GetTableDef().GetCols()
	require.NotEmpty(t, cols)
	require.Equal(t, int32(types.T_int64), cols[0].Typ.Id)
	require.True(t, cols[0].Typ.NotNullable)
}

func TestDeepCopyPreparedQueryPreservesExecutionMetadata(t *testing.T) {
	original := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		Nodes: []*planpb.Node{{
			NodeType:    planpb.Node_JOIN,
			SendMsgList: []planpb.MsgHeader{{MsgTag: 11, MsgType: 1}},
			RecvMsgList: []planpb.MsgHeader{{MsgTag: 12, MsgType: 2}},
		}},
	}}}
	copied := DeepCopyPlan(original)
	require.Equal(t, original.GetQuery().Nodes[0].SendMsgList, copied.GetQuery().Nodes[0].SendMsgList)
	require.Equal(t, original.GetQuery().Nodes[0].RecvMsgList, copied.GetQuery().Nodes[0].RecvMsgList)
	require.NotSame(t, original.GetQuery().Nodes[0], copied.GetQuery().Nodes[0])
}

func TestPreparedDynamicNumericRebindsTypeSensitiveParents(t *testing.T) {
	for _, test := range []struct {
		sql    string
		values []any
	}{
		{sql: "select (? + 1) > 0", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select 0 < (? + 1)", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select (? + ?) > 0", values: []any{
			ParamValue{Value: "1", RuntimeType: types.T_int64},
			ParamValue{Value: "2", RuntimeType: types.T_int64},
		}},
		{sql: "select case when (? + 1) > 0 then 1 else 0 end", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select (? + 1) between 0 and 10", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select (? + 1) in (0, 3)", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select n_nationkey from nation where (? + 1) > n_nationkey", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select count(*) from nation having (? + 1) > 0", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
		{sql: "select n.n_nationkey from nation n join region r on (? + 1) > r.r_regionkey", values: []any{ParamValue{Value: "2", RuntimeType: types.T_int64}}},
	} {
		t.Run(test.sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, test.sql)
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, test.values)
			require.NoError(t, err)
			if len(specialized.GetQuery().Steps) > 0 {
				root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
				if len(root.ProjectList) == 1 && root.ProjectList[0].GetF() != nil {
					name := root.ProjectList[0].GetF().GetFunc().GetObjName()
					if name == ">" || name == "<" || name == "between" || name == "in" {
						require.Equal(t, int32(types.T_bool), root.ProjectList[0].Typ.Id)
					}
				}
			}
		})
	}
}

func TestPreparedDynamicNumericCoversEquivalentExactContexts(t *testing.T) {
	for _, sql := range []string{
		"select ? + abs(1)",
		"select ? + coalesce(1, 2)",
		"select ? + cast(1 as signed)",
		"select ? + mod(3, 2)",
		"select ? + n_nationkey from nation",
		"select ? + 0.5",
		"select ? + cast(0.5 as decimal(2, 1))",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			require.True(t, HasPreparedDynamicNumericParams(prepare.Plan))
			_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: "1.5", RuntimeType: types.T_decimal128,
			}})
			require.NoError(t, err)
		})
	}
}

func TestPreparedScalarSubqueryParameterDetection(t *testing.T) {
	for _, test := range []struct {
		sql     string
		dynamic bool
	}{
		{sql: "select (select 1) + 1", dynamic: false},
		{sql: "select (select 1) + ?", dynamic: true},
	} {
		t.Run(test.sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, test.sql)
			require.Equal(t, test.dynamic, HasPreparedDynamicNumericParams(prepare.Plan))
			if test.dynamic {
				_, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
					Value: "1.5", RuntimeType: types.T_decimal128,
				}})
				require.NoError(t, err)
			}
		})
	}
}

func TestPreparedNarrowUnsignedArithmeticWidens(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? + 1")
	for _, test := range []struct {
		value       uint64
		runtimeType types.T
	}{
		{value: math.MaxUint8, runtimeType: types.T_uint8},
		{value: math.MaxUint16, runtimeType: types.T_uint16},
		{value: math.MaxUint32, runtimeType: types.T_uint32},
	} {
		t.Run(test.runtimeType.String(), func(t *testing.T) {
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: strconv.FormatUint(test.value, 10), RuntimeType: test.runtimeType,
			}})
			require.NoError(t, err)
			root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
			require.NotEqual(t, int32(test.runtimeType), root.ProjectList[0].Typ.Id)
		})
	}
}

func TestPreparedUint64MixedNumericDomains(t *testing.T) {
	for _, test := range []struct {
		sql         string
		wantDecimal bool
	}{
		{sql: "select 0 + ?"},
		{sql: "select ? + 0"},
		{sql: "select 0.0 + ?", wantDecimal: true},
		{sql: "select ? + 0.0", wantDecimal: true},
		{sql: "select (? + 0.5) + cast(0 as unsigned)", wantDecimal: true},
		{sql: "select cast(0 as unsigned) + (0.5 + ?)", wantDecimal: true},
	} {
		t.Run(test.sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, test.sql)
			specialized, err := FillValuesOfParamsInPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: strconv.FormatUint(math.MaxUint64, 10), RuntimeType: types.T_uint64,
			}})
			require.NoError(t, err)
			root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
			resultType := types.T(root.ProjectList[0].Typ.Id)
			if test.wantDecimal {
				require.True(t, resultType.IsDecimal(), resultType.String())
			} else {
				require.True(t, resultType.IsDecimal(), resultType.String())
			}
		})
	}
}

func TestPreparedTwoUnknownParametersKeepExactMixedIntegerDomain(t *testing.T) {
	prepare := buildPreparedAggregatePlan(t, "select ? + ?")
	specialized, err := SpecializePreparedNumericPlan(context.Background(), prepare.Plan, []any{
		ParamValue{Value: strconv.FormatUint(math.MaxUint64, 10), RuntimeType: types.T_uint64},
		ParamValue{Value: "-1", RuntimeType: types.T_int64},
	})
	require.NoError(t, err)
	root := specialized.GetQuery().Nodes[specialized.GetQuery().Steps[0]]
	resultType := types.T(root.ProjectList[0].Typ.Id)
	require.True(t, resultType.IsDecimal(), resultType.String())
}

func TestPreparedDynamicNumericRefreshesCrossNodeTypeLineage(t *testing.T) {
	for _, sql := range []string{
		"select x + 1 from (select ? + 1 as x) d",
		"with c as (select ? + 1 as x) select x + 1 from c",
		"select ? + 1 as x union all select 2",
		"select distinct ? + 1 as x",
		"select ? + 1 as x order by x",
		"select ? + 1 as x group by x",
		"select sum(? + 1) + 1 from nation",
		"select sum(? + 1) over () + 1 from nation",
	} {
		t.Run(sql, func(t *testing.T) {
			prepare := buildPreparedAggregatePlan(t, sql)
			specialized, err := SpecializePreparedNumericPlan(context.Background(), prepare.Plan, []any{ParamValue{
				Value: "2", RuntimeType: types.T_int64,
			}})
			require.NoError(t, err)

			producerTypes := make(map[[2]int32]planpb.Type)
			for _, node := range specialized.GetQuery().Nodes {
				recordPreparedNodeOutputTypes(producerTypes, node)
			}
			var checkExpr func(*planpb.Expr)
			checkExpr = func(expr *planpb.Expr) {
				if expr == nil {
					return
				}
				if col := expr.GetCol(); col != nil {
					if want, ok := producerTypes[[2]int32{col.RelPos, col.ColPos}]; ok {
						require.Equal(t, want.Id, expr.Typ.Id)
						require.Equal(t, want.Width, expr.Typ.Width)
						require.Equal(t, want.Scale, expr.Typ.Scale)
					}
				}
				if fn := expr.GetF(); fn != nil {
					for _, arg := range fn.Args {
						checkExpr(arg)
					}
				}
			}
			for _, node := range specialized.GetQuery().Nodes {
				if node.NodeType == planpb.Node_UNION_ALL {
					require.Len(t, node.Children, 2)
					left := specialized.GetQuery().Nodes[node.Children[0]]
					right := specialized.GetQuery().Nodes[node.Children[1]]
					for idx := range node.ProjectList {
						require.Equal(t, node.ProjectList[idx].Typ.Id, left.ProjectList[idx].Typ.Id)
						require.Equal(t, node.ProjectList[idx].Typ.Id, right.ProjectList[idx].Typ.Id)
					}
				}
				if node.NodeType == planpb.Node_WINDOW {
					for _, expr := range node.WinSpecList {
						require.Equal(t, expr.GetW().WindowFunc.Typ.Id, expr.Typ.Id)
					}
				}
				for _, expr := range node.ProjectList {
					checkExpr(expr)
				}
				for _, expr := range node.GroupBy {
					checkExpr(expr)
				}
				for _, expr := range node.AggList {
					checkExpr(expr)
				}
				for _, expr := range node.WinSpecList {
					checkExpr(expr)
				}
				for _, order := range node.OrderBy {
					checkExpr(order.Expr)
				}
			}
		})
	}
}

func TestPreparedFloatRangeHandling(t *testing.T) {
	positiveZero, err := parsePreparedFloat("1e-10000", 64)
	require.NoError(t, err)
	require.Zero(t, positiveZero)
	require.False(t, math.Signbit(positiveZero))
	negativeZero, err := parsePreparedFloat("-1e-10000", 64)
	require.NoError(t, err)
	require.Zero(t, negativeZero)
	require.True(t, math.Signbit(negativeZero))
	require.True(t, math.Signbit(MakePlan2Float64ConstExprWithType(negativeZero).GetLit().GetDval()))
	_, err = parsePreparedFloat("1e10000", 64)
	require.Error(t, err)
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
