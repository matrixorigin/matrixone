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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGenerateSeriesDatetimeLiteralScale(t *testing.T) {
	tests := []struct {
		value string
		want  int32
	}{
		{value: "2020-02-29 23:59:59", want: 0},
		{value: "2020-02-29 23:59:59.1", want: 1},
		{value: "2020-02-29 23:59:59.123", want: 3},
		{value: "2020-02-29 23:59:59.123456", want: 6},
		{value: "2020-02-29 23:59:59.123456789", want: 6},
		{value: "2020-02-29 23:59:59.123+08:00", want: 3},
	}
	for _, test := range tests {
		require.Equal(t, test.want, datetimeLiteralScale(test.value), test.value)
	}
}

func TestPreparedGenerateSeriesParameterInfo(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare series_info from 'select result from generate_series(?,?,?) g'")
	require.NoError(t, err)
	positions, parameterized := PreparedPlanGenerateSeriesParameterInfo(
		prepared.GetDcl().GetPrepare().Plan)
	require.True(t, parameterized)
	require.Equal(t, []int32{0, 1}, positions)

	prepared, err = runOneStmt(NewMockOptimizer(false), t,
		"prepare series_step from 'select result from generate_series(''2020-01-01'',''2020-01-02'',?) g'")
	require.NoError(t, err)
	positions, parameterized = PreparedPlanGenerateSeriesParameterInfo(
		prepared.GetDcl().GetPrepare().Plan)
	require.True(t, parameterized)
	require.Empty(t, positions)
}

func TestGenerateSeriesDatetimeScale(t *testing.T) {
	columnExpr := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: 0,
			}},
		}
	}
	str := MakePlan2StringConstExprWithType

	tests := []struct {
		name  string
		exprs []*planpb.Expr
		want  int32
	}{
		{
			name:  "whole seconds",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00"), str("2020-01-01 00:00:01"), str("1 second")},
			want:  0,
		},
		{
			name:  "fractional endpoint",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00.123"), str("2020-01-01 00:00:01.1"), str("1 second")},
			want:  3,
		},
		{
			name:  "microsecond step",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00"), str("2020-01-01 00:00:01"), str("1 MICROSECOND")},
			want:  6,
		},
		{
			name:  "dynamic string endpoint",
			exprs: []*planpb.Expr{columnExpr(types.T_varchar.ToType()), str("2020-01-01 00:00:01"), str("1 second")},
			want:  6,
		},
		{
			name:  "dynamic step",
			exprs: []*planpb.Expr{str("2020-01-01 00:00:00"), str("2020-01-01 00:00:01"), columnExpr(types.T_varchar.ToType())},
			want:  6,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, generateSeriesDatetimeScale(test.exprs))
		})
	}
}

func TestBindGenerateSeriesArgs(t *testing.T) {
	columnExpr := func(typ types.Type, colPos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ: makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: colPos,
			}},
		}
	}

	t.Run("numeric behavior is unchanged", func(t *testing.T) {
		exprs := []*planpb.Expr{
			MakePlan2Int64ConstExprWithType(1),
			MakePlan2Int64ConstExprWithType(2),
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Same(t, exprs[0], bound[0])
		require.Equal(t, types.T_int64, typ.Oid)
	})

	t.Run("numeric first argument casts later placeholders", func(t *testing.T) {
		markerType := types.T_text.ToType()
		exprs := []*planpb.Expr{
			MakePlan2Int64ConstExprWithType(1),
			{Typ: makePlan2Type(&markerType), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
			{Typ: makePlan2Type(&markerType), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}}},
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Equal(t, types.T_int64, typ.Oid)
		require.Same(t, exprs[0], bound[0])
		for i := 1; i < len(bound); i++ {
			require.Equal(t, types.T_int64, types.T(bound[i].Typ.Id))
			require.Equal(t, "cast", bound[i].GetF().GetFunc().GetObjName())
			require.NotNil(t, bound[i].GetF().Args[0].GetP())
			require.Same(t, exprs[i], bound[i].GetF().Args[0])
		}
	})

	t.Run("string endpoints are cast without modifying input", func(t *testing.T) {
		exprs := []*planpb.Expr{
			MakePlan2StringConstExprWithType("2020-02-29 23:59:59.124356"),
			MakePlan2StringConstExprWithType("2020-02-29 23:59:59.124360"),
			MakePlan2StringConstExprWithType("1 microsecond"),
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Equal(t, types.T_varchar, typ.Oid)
		require.NotSame(t, exprs[0], bound[0])
		require.Same(t, exprs[2], bound[2])
		require.Equal(t, types.T_varchar, types.T(exprs[0].Typ.Id))
		for i := 0; i < 2; i++ {
			require.Equal(t, types.T_datetime, types.T(bound[i].Typ.Id))
			require.Equal(t, int32(6), bound[i].Typ.Scale)
			require.Equal(t, "cast", bound[i].GetF().GetFunc().GetObjName())
		}
	})

	t.Run("typed temporal input retains datetime result", func(t *testing.T) {
		startType := types.T_datetime.ToTypeWithScale(3)
		endType := types.T_datetime.ToTypeWithScale(6)
		exprs := []*planpb.Expr{
			columnExpr(startType, 0),
			columnExpr(endType, 1),
			MakePlan2StringConstExprWithType("1 second"),
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Equal(t, types.T_datetime, typ.Oid)
		require.Equal(t, int32(6), typ.Scale)
		require.NotSame(t, exprs[0], bound[0])
		require.Same(t, exprs[1], bound[1])
		require.Equal(t, "cast", bound[0].GetF().GetFunc().GetObjName())
	})

	t.Run("temporal first argument casts step placeholder", func(t *testing.T) {
		startType := types.T_datetime.ToType()
		markerType := types.T_text.ToType()
		exprs := []*planpb.Expr{
			columnExpr(startType, 0),
			MakePlan2StringConstExprWithType("2020-01-03 00:00:00"),
			{Typ: makePlan2Type(&markerType), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
		}
		bound, typ, err := bindGenerateSeriesArgs(context.Background(), exprs)
		require.NoError(t, err)
		require.Equal(t, types.T_datetime, typ.Oid)
		require.Equal(t, types.T_varchar, types.T(bound[2].Typ.Id))
		require.Equal(t, "cast", bound[2].GetF().GetFunc().GetObjName())
		require.Same(t, exprs[2], bound[2].GetF().Args[0])
	})
}

func TestPreparedGenerateSeriesEndpointDomain(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare gs from 'select min(result), max(result) from generate_series(?,?,?) g'")
	require.NoError(t, err)
	original := prepared.GetDcl().GetPrepare().Plan
	require.True(t, PreparedPlanNeedsRuntimeSpecialization(original))
	findScan := func(p *planpb.Plan) *planpb.Node {
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == planpb.Node_FUNCTION_SCAN && node.TableDef.GetTblFunc().GetName() == "generate_series" {
				return node
			}
		}
		t.Fatal("generate_series scan not found")
		return nil
	}
	require.Equal(t, types.T_int64, types.T(findScan(original).TableDef.Cols[0].Typ.Id))
	values := []any{
		ParamValue{Value: int64(1), SourceType: types.T_int64.ToType(), HasSourceType: true},
		ParamValue{Value: int64(9), SourceType: types.T_int64.ToType(), HasSourceType: true},
		ParamValue{Value: int64(2), SourceType: types.T_int64.ToType(), HasSourceType: true},
	}
	bound, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, values)
	require.NoError(t, err)
	require.True(t, changed)
	scan := findScan(bound)
	require.Equal(t, types.T_int64, types.T(scan.TableDef.Cols[0].Typ.Id))
	for _, arg := range scan.TblFuncExprList {
		require.Equal(t, types.T_int64, types.T(arg.Typ.Id))
	}
	columns := GetResultColumnsFromPlan(bound)
	require.Len(t, columns, 2)
	require.Equal(t, types.T_int64, types.T(columns[0].Typ.Id))
	require.Equal(t, types.T_int64, types.T(columns[1].Typ.Id))
	require.Equal(t, types.T_int64, types.T(findScan(original).TableDef.Cols[0].Typ.Id))

	temporal := []any{
		ParamValue{Value: "2020-01-01 00:00:00", SourceType: types.T_text.ToType(), HasSourceType: true},
		ParamValue{Value: "2020-01-03 00:00:00", SourceType: types.T_text.ToType(), HasSourceType: true},
		ParamValue{Value: "1 day", SourceType: types.T_text.ToType(), HasSourceType: true},
	}
	datePlan, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, temporal)
	require.NoError(t, err)
	require.True(t, changed)
	dateScan := findScan(datePlan)
	require.Equal(t, types.T_varchar, types.T(dateScan.TableDef.Cols[0].Typ.Id))
	for _, arg := range dateScan.TblFuncExprList[:2] {
		require.Equal(t, types.T_datetime, types.T(arg.Typ.Id))
	}
	require.Equal(t, types.T_varchar, types.T(dateScan.TblFuncExprList[2].Typ.Id))
	for _, column := range GetResultColumnsFromPlan(datePlan) {
		require.Equal(t, types.T_varchar, types.T(column.Typ.Id))
	}

	binary := []any{
		ParamValue{Value: "1", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
		ParamValue{Value: "9", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
		ParamValue{Value: "2", RuntimeType: types.T_int64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
	}
	binaryPlan, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, binary)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, types.T_int64, types.T(findScan(binaryPlan).TableDef.Cols[0].Typ.Id))

	unsigned := []any{
		ParamValue{Value: uint64(1), SourceType: types.T_uint64.ToType(), HasSourceType: true},
		ParamValue{Value: uint64(9), SourceType: types.T_uint64.ToType(), HasSourceType: true},
		ParamValue{Value: uint64(2), SourceType: types.T_uint64.ToType(), HasSourceType: true},
	}
	unsignedPlan, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, unsigned)
	require.NoError(t, err)
	require.True(t, changed)
	unsignedScan := findScan(unsignedPlan)
	require.Equal(t, types.T_int64, types.T(unsignedScan.TableDef.Cols[0].Typ.Id))
	for _, arg := range unsignedScan.TblFuncExprList {
		require.Equal(t, types.T_int64, types.T(arg.Typ.Id))
	}

	typedTemporal := []any{
		ParamValue{Value: "2020-01-01 00:00:00", RuntimeType: types.T_datetime.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
		ParamValue{Value: "2020-01-03 00:00:00", RuntimeType: types.T_datetime.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
		ParamValue{Value: "1 day", RuntimeType: types.T_varchar.ToType(), HasRuntimeType: true, IsBinaryProtocol: true},
	}
	typedTemporalPlan, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original, typedTemporal)
	require.NoError(t, err)
	require.True(t, changed)
	typedTemporalScan := findScan(typedTemporalPlan)
	require.Equal(t, types.T_datetime, types.T(typedTemporalScan.TableDef.Cols[0].Typ.Id))
	for _, column := range GetResultColumnsFromPlan(typedTemporalPlan) {
		require.Equal(t, types.T_datetime, types.T(column.Typ.Id))
	}
}

func TestPreparedGenerateSeriesMixedTemporalEndpoint(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare gs_mixed from 'select result from generate_series(?,''2020-01-03 00:00:00'',''1 day'') g'")
	require.NoError(t, err)
	original := prepared.GetDcl().GetPrepare().Plan
	bound, changed, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), original,
		[]any{ParamValue{Value: "2020-01-01 00:00:00", SourceType: types.T_text.ToType(), HasSourceType: true}})
	require.NoError(t, err)
	require.True(t, changed)
	for _, node := range bound.GetQuery().Nodes {
		if node.NodeType != planpb.Node_FUNCTION_SCAN || node.TableDef.GetTblFunc().GetName() != "generate_series" {
			continue
		}
		for _, arg := range node.TblFuncExprList[:2] {
			require.Equal(t, types.T_datetime, types.T(arg.Typ.Id))
		}
		require.Equal(t, types.T_varchar, types.T(node.TableDef.Cols[0].Typ.Id))
		return
	}
	t.Fatal("generate_series scan not found")
}

func TestBuildGenerateSeriesOwnsStableResultSchema(t *testing.T) {
	build := func() *planpb.Node {
		logicPlan, err := runOneStmt(NewMockOptimizer(false), t,
			"select * from generate_series('2020-02-29 23:59:59.124356', '2020-02-29 23:59:59.124360', '1 microsecond') g")
		require.NoError(t, err)
		resultColumns := GetResultColumnsFromPlan(logicPlan)
		require.Len(t, resultColumns, 1)
		require.Equal(t, types.T_varchar, types.T(resultColumns[0].Typ.Id))
		query := resolveQueryPlan(logicPlan).GetQuery()
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_FUNCTION_SCAN && node.GetTableDef().GetTblFunc().GetName() == "generate_series" {
				return node
			}
		}
		t.Fatal("generate_series function scan not found")
		return nil
	}

	first := build()
	second := build()
	require.Len(t, first.TableDef.Cols, 1)
	require.Equal(t, types.T_varchar, types.T(first.TableDef.Cols[0].Typ.Id))
	require.NotSame(t, first.TableDef.Cols[0], second.TableDef.Cols[0])
}

func TestTableFunctionInputDependency(t *testing.T) {
	findFunctionScan := func(query *planpb.Query, name string) *planpb.Node {
		for _, node := range query.Nodes {
			if node.NodeType == planpb.Node_FUNCTION_SCAN &&
				node.GetTableDef().GetTblFunc().GetName() == name {
				return node
			}
		}
		return nil
	}

	findNode := func(query *planpb.Query, nodeType planpb.Node_NodeType) *planpb.Node {
		for _, node := range query.Nodes {
			if node.NodeType == nodeType {
				return node
			}
		}
		return nil
	}
	countNodes := func(query *planpb.Query, nodeType planpb.Node_NodeType) int {
		count := 0
		for _, node := range query.Nodes {
			if node.NodeType == nodeType {
				count++
			}
		}
		return count
	}

	tests := []struct {
		name           string
		sql            string
		functionName   string
		dependsOnInput bool
		apply          bool
		tableScans     int
	}{
		{
			name:           "JOIN right function with literal arguments is a source",
			sql:            "select * from nation n join generate_series(1, 5) g on n.n_nationkey = g.result",
			functionName:   "generate_series",
			dependsOnInput: false,
			tableScans:     1,
		},
		{
			name:           "JOIN right function with scalar-only arguments is a source",
			sql:            "select * from nation n join generate_series(abs(1), 5) g on n.n_nationkey = g.result",
			functionName:   "generate_series",
			dependsOnInput: false,
			tableScans:     1,
		},
		{
			name:           "JOIN right function with left expression is row dependent",
			sql:            "select * from nation n join generate_series(n.n_nationkey, n.n_nationkey + 1) g on n.n_nationkey = g.result",
			functionName:   "generate_series",
			dependsOnInput: true,
			tableScans:     2,
		},
		{
			name: "JOIN chain discovers dependencies in the complete left subtree",
			sql: "select * from nation n join region r on n.n_regionkey = r.r_regionkey " +
				"join generate_series(r.r_regionkey, r.r_regionkey) g on r.r_regionkey = g.result",
			functionName:   "generate_series",
			dependsOnInput: true,
			tableScans:     4,
		},
		{
			name:           "generic table function shares the source rule",
			sql:            "select * from nation n join generate_random_int64(5, 42) g on n.n_nationkey = g.nth",
			functionName:   "generate_random_int64",
			dependsOnInput: false,
			tableScans:     1,
		},
		{
			name:           "unnest with literal input is a source",
			sql:            "select * from nation n join unnest('[1, 2, 3]') u on true",
			functionName:   "unnest",
			dependsOnInput: false,
			tableScans:     1,
		},
		{
			name:           "fulltext JOIN uses input metadata without forcing execution dependency",
			sql:            "select * from nation n join fulltext_index_tokenize('', 1, 'body') f on true",
			functionName:   "fulltext_index_tokenize",
			dependsOnInput: false,
			tableScans:     1,
		},
		{
			name:           "APPLY owns row dependency instead of FUNCTION_SCAN",
			sql:            "select * from nation n cross apply generate_series(n.n_nationkey, n.n_nationkey + 1) g",
			functionName:   "generate_series",
			dependsOnInput: false,
			apply:          true,
			tableScans:     1,
		},
		{
			name:           "APPLY keeps planning input separate from execution child",
			sql:            "select * from nation n cross apply fulltext_index_tokenize('', n.n_nationkey, n.n_name) f",
			functionName:   "fulltext_index_tokenize",
			dependsOnInput: false,
			apply:          true,
			tableScans:     1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)

			query := resolveQueryPlan(logicPlan).GetQuery()
			functionScan := findFunctionScan(query, test.functionName)
			require.NotNil(t, functionScan)
			require.Equal(t, test.dependsOnInput, len(functionScan.Children) > 0)
			require.Equal(t, test.tableScans, countNodes(query, planpb.Node_TABLE_SCAN))
			if test.apply {
				require.NotNil(t, findNode(query, planpb.Node_APPLY))
			}
		})
	}
}

func TestFullTextIndexTokenizeRequiresInputRelation(t *testing.T) {
	_, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"select * from fulltext_index_tokenize('', 1, 'body') f",
	)
	require.ErrorContains(t, err, "fulltext_index_tokenize requires a left input relation")
}
