// Copyright 2022 Matrix Origin
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
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type stubWindowBinder struct {
	bindExprFunc                     func(tree.Expr, int32, bool) (*planpb.Expr, error)
	bindFuncExprFunc                 func(string, []tree.Expr, int32) (*planpb.Expr, error)
	bindPreparedWindowFrameBoundFunc func(tree.Expr, *planpb.Type) (*planpb.Expr, error)
	makeFrameValueFunc               func(tree.Expr, *planpb.Type) (*planpb.Expr, error)
}

func (b *stubWindowBinder) BindExpr(expr tree.Expr, depth int32, isRoot bool) (*planpb.Expr, error) {
	return b.bindExprFunc(expr, depth, isRoot)
}

func (b *stubWindowBinder) bindFuncExprImplByAstExpr(name string, args []tree.Expr, depth int32) (*planpb.Expr, error) {
	return b.bindFuncExprFunc(name, args, depth)
}

func (b *stubWindowBinder) bindPreparedNumericFuncExpr(name string, args []tree.Expr, depth int32) (*planpb.Expr, error) {
	return b.bindFuncExprImplByAstExpr(name, args, depth)
}

func (b *stubWindowBinder) bindPreparedWindowFrameBound(expr tree.Expr, typ *planpb.Type) (*planpb.Expr, error) {
	return b.bindPreparedWindowFrameBoundFunc(expr, typ)
}

func (b *stubWindowBinder) makeFrameConstValue(expr tree.Expr, typ *planpb.Type) (*planpb.Expr, error) {
	return b.makeFrameValueFunc(expr, typ)
}

func (b *stubWindowBinder) GetContext() context.Context {
	return context.Background()
}

func testNumVal(v int64) tree.Expr {
	return tree.NewNumVal(v, strconv.FormatInt(v, 10), false, tree.P_int64)
}

func testWindowFuncExpr(name string, funcType tree.FuncType, ws *tree.WindowSpec, args ...tree.Expr) *tree.FuncExpr {
	return &tree.FuncExpr{
		Func:       tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
		Type:       funcType,
		Exprs:      args,
		WindowSpec: ws,
	}
}

func testLagWindowExpr() *tree.FuncExpr {
	return testWindowFuncExpr(
		"lag",
		tree.FUNC_TYPE_DEFAULT,
		&tree.WindowSpec{
			PartitionBy: tree.Exprs{testNumVal(1)},
			OrderBy: tree.OrderBy{
				tree.NewOrder(testNumVal(1), tree.Descending, tree.NullsLast, false),
			},
		},
		testNumVal(1),
	)
}

func testRangeWindowExpr() *tree.FuncExpr {
	return testWindowFuncExpr(
		"sum",
		tree.FUNC_TYPE_DEFAULT,
		&tree.WindowSpec{
			OrderBy: tree.OrderBy{
				tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false),
			},
			HasFrame: true,
			Frame: &tree.FrameClause{
				Type:   tree.Range,
				HasEnd: true,
				Start: &tree.FrameBound{
					Type: tree.Preceding,
					Expr: testNumVal(1),
				},
				End: &tree.FrameBound{
					Type: tree.Following,
					Expr: testNumVal(2),
				},
			},
		},
		testNumVal(1),
	)
}

func testRowsWindowSpec() *tree.WindowSpec {
	return &tree.WindowSpec{
		HasFrame: true,
		Frame: &tree.FrameClause{
			Type: tree.Rows,
			Start: &tree.FrameBound{
				Type:      tree.Preceding,
				UnBounded: true,
			},
			End: &tree.FrameBound{
				Type: tree.CurrentRow,
			},
		},
	}
}

func testRowNumberWindowExpr() *tree.FuncExpr {
	return testWindowFuncExpr(
		"row_number",
		tree.FUNC_TYPE_DEFAULT,
		&tree.WindowSpec{
			OrderBy: tree.OrderBy{
				tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false),
			},
		},
	)
}

func testScalarFuncExpr(name string, args ...tree.Expr) *tree.FuncExpr {
	return &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
		Type:  tree.FUNC_TYPE_DEFAULT,
		Exprs: args,
	}
}

func testWindowValidationBinder() *stubWindowBinder {
	return &stubWindowBinder{
		bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
			return makePlan2Int64ConstExprWithType(1), nil
		},
		bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
			return makePlan2Int64ConstExprWithType(1), nil
		},
		makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
			return makePlan2Int64ConstExprWithType(1), nil
		},
	}
}

const preparedWindowFrameSQL = "select sum(n_nationkey) over (order by n_nationkey rows between ? preceding and ? following) from nation"

func TestPreparedWindowFrameMarkers(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := parsers.Parse(optimizer.CurrentContext().GetContext(), dialect.MYSQL, preparedWindowFrameSQL, 1)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	window := firstWindowSpec(t, queryPlan)
	requirePreparedRowsFrameParam(t, window.Frame.Start.Val, 1)
	requirePreparedRowsFrameParam(t, window.Frame.End.Val, 2)
}

func TestBuildPlanRejectsNegativeTemporalWindowBounds(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	units := []string{"microsecond", "second", "minute", "hour", "day", "month", "year"}
	for _, unit := range units {
		t.Run(unit, func(t *testing.T) {
			sql := "select sum(a) over (order by cast('2024-01-01' as timestamp) range between interval -1 " + unit + " preceding and current row) from select_test.bind_select"
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmt, false)
			require.ErrorContains(t, err, "frame start or end is negative")
		})
	}
}

func TestNthValueRequiresConstantPositiveOffset(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		{
			name: "constant positive expression",
			sql:  "select nth_value(a, 1 + 1) over (order by a) from select_test.bind_select",
		},
		{
			name:    "zero offset",
			sql:     "select nth_value(a, 0) over (order by a) from select_test.bind_select",
			wantErr: true,
		},
		{
			name:    "column offset",
			sql:     "select nth_value(a, a - 1) over (order by a) from select_test.bind_select",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(ctx, stmt, false)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			require.Equal(t, "Incorrect arguments to nth_value", err.Error())
			moErr, ok := err.(*moerr.Error)
			require.True(t, ok)
			require.Equal(t, moerr.ER_WRONG_ARGUMENTS, moErr.MySQLCode())
			require.Equal(t, moerr.MySQLDefaultSqlState, moErr.SqlState())
		})
	}
}

func TestLagLeadRejectInvalidConstantOffset(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	for _, name := range []string{"lag", "lead"} {
		for _, offset := range []string{
			"-1",
			"0 - 1",
			"-1.5",
			"cast(-1 as decimal)",
			"null",
			"cast(null as signed)",
		} {
			t.Run(name+"/"+offset, func(t *testing.T) {
				sql := "select " + name + "(a, " + offset + ") over (order by a) from select_test.bind_select"
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)

				_, err = BuildPlan(ctx, stmt, false)
				require.EqualError(t, err, "Incorrect arguments to "+name)
				moErr, ok := err.(*moerr.Error)
				require.True(t, ok)
				require.Equal(t, moerr.ER_WRONG_ARGUMENTS, moErr.MySQLCode())
			})
		}

		for _, offset := range []string{"0", "1", "9223372036854775807"} {
			t.Run(name+"/valid/"+offset, func(t *testing.T) {
				sql := "select " + name + "(a, " + offset + ") over (order by a) from select_test.bind_select"
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
				require.NoError(t, err)

				_, err = BuildPlan(ctx, stmt, false)
				require.NoError(t, err)
			})
		}
	}
}

func TestPreparedNthValueAcceptsPositionalOffset(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(
		context.Background(),
		dialect.MYSQL,
		"select nth_value(a, ?) over (order by a) from select_test.bind_select",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(ctx, stmt, true)
	require.NoError(t, err)

	offset := firstWindowSpec(t, queryPlan).WindowFunc.GetF().Args[1]
	require.Equal(t, int32(types.T_text), offset.Typ.Id)
	require.Equal(t, int32(1), offset.GetP().Pos)

	require.NoError(t, NormalizePrepareParamRefs(context.Background(), queryPlan))
	filled, err := FillValuesOfParamsInPlan(context.Background(), queryPlan, []any{int64(2)})
	require.NoError(t, err)
	filledOffset := firstWindowSpec(t, filled).WindowFunc.GetF().Args[1]
	require.Equal(t, int32(types.T_text), filledOffset.Typ.Id)
	require.Equal(t, "2", filledOffset.GetLit().GetSval())

	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "zero", value: int64(0)},
		{name: "negative", value: int64(-1)},
		{name: "null", value: nil},
		{name: "float", value: float64(2.5)},
		{name: "string", value: "2"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := FillValuesOfParamsInPlan(context.Background(), queryPlan, []any{test.value})
			require.Error(t, err)
			require.Equal(t, moerr.ER_WRONG_ARGUMENTS, err.(*moerr.Error).MySQLCode())
		})
	}

	binaryFilled, err := FillValuesOfParamsInPlan(context.Background(), queryPlan, []any{
		ParamValue{
			Value:            "2",
			PrepareParamKind: vector.PrepareParamInteger,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "2", firstWindowSpec(t, binaryFilled).WindowFunc.GetF().Args[1].GetLit().GetSval())

	logicPlan, err := runOneStmt(
		NewMockOptimizer(false),
		t,
		"prepare nth_param from 'select nth_value(a, ?) over (order by a) from select_test.bind_select'",
	)
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
	preparedOffset := firstWindowSpec(t, prepare.Plan).WindowFunc.GetF().Args[1]
	require.Equal(t, int32(types.T_text), preparedOffset.Typ.Id)
	require.NotNil(t, preparedOffset.GetP())
}

func TestPreparedWindowRangeFrameMarkers(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := parsers.Parse(
		optimizer.CurrentContext().GetContext(),
		dialect.MYSQL,
		"select sum(n_nationkey) over (order by n_nationkey range ? preceding) from nation",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	window := firstWindowSpec(t, queryPlan)
	requirePreparedWindowFrameParam(t, window.Frame.Start.Val, types.T_int32, 1)
	require.Equal(t, planpb.FrameBound_CURRENT_ROW, window.Frame.End.Type)
	require.Nil(t, window.Frame.End.Val)
}

func TestPreparedWindowRangeFrameMarkersInBothBounds(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := parsers.Parse(
		optimizer.CurrentContext().GetContext(),
		dialect.MYSQL,
		"select sum(n_nationkey) over (order by n_nationkey range between ? preceding and ? following) from nation",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.NoError(t, err)
	window := firstWindowSpec(t, queryPlan)
	requirePreparedWindowFrameParam(t, window.Frame.Start.Val, types.T_int32, 1)
	requirePreparedWindowFrameParam(t, window.Frame.End.Val, types.T_int32, 2)
}

func TestPreparedWindowRangeFrameMarkerRequiresNumericOrder(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmts, err := parsers.Parse(
		optimizer.CurrentContext().GetContext(),
		dialect.MYSQL,
		"select sum(n_nationkey) over (order by cast('2026-01-01' as date) range ? preceding) from nation",
		1,
	)
	require.NoError(t, err)

	_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], true)
	require.ErrorContains(t, err, "parameterized RANGE frame requires a numeric ORDER BY expression")
}

func TestPreparedWindowIntervalFrameMarkersAreUnsupported(t *testing.T) {
	for _, frameType := range []string{"rows", "range"} {
		t.Run(frameType, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := parsers.Parse(
				optimizer.CurrentContext().GetContext(),
				dialect.MYSQL,
				"select sum(n_nationkey) over (order by n_nationkey "+frameType+" between interval ? day preceding and current row) from nation",
				1,
			)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.ErrorContains(t, err, "prepared parameter markers in interval window frames")
		})
	}
}

func TestPreparedWindowNestedIntervalFrameMarkersAreUnsupported(t *testing.T) {
	for _, frameType := range []string{"rows", "range"} {
		t.Run(frameType, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := parsers.Parse(
				optimizer.CurrentContext().GetContext(),
				dialect.MYSQL,
				"select sum(n_nationkey) over (order by n_nationkey "+frameType+" between interval (? + 1) day preceding and current row) from nation",
				1,
			)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.ErrorContains(t, err, "prepared parameter markers in interval window frames")
		})
	}
}

func TestHasWindowFrameParamTraversesExpressionForms(t *testing.T) {
	param := func() tree.Expr { return tree.NewParamExpr(1) }
	literal := func() tree.Expr { return testNumVal(1) }
	tests := []struct {
		name string
		expr tree.Expr
	}{
		{
			name: "binary",
			expr: tree.NewBinaryExpr(tree.PLUS, param(), literal()),
		},
		{
			name: "comparison",
			expr: tree.NewComparisonExpr(tree.EQUAL, literal(), param()),
		},
		{
			name: "boolean",
			expr: tree.NewAndExpr(literal(), tree.NewNotExpr(param())),
		},
		{
			name: "case",
			expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(literal(), param())}, literal()),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.True(t, hasWindowFrameParam(tc.expr))
		})
	}
}

func TestHasWindowFrameParamRejectsExpressionFormsWithoutParameters(t *testing.T) {
	literal := func() tree.Expr { return testNumVal(1) }
	tests := []struct {
		name string
		expr tree.Expr
	}{
		{name: "literal", expr: literal()},
		{name: "binary", expr: tree.NewBinaryExpr(tree.PLUS, literal(), literal())},
		{name: "unary", expr: &tree.UnaryExpr{Expr: literal()}},
		{name: "comparison", expr: tree.NewComparisonExpr(tree.EQUAL, literal(), literal())},
		{name: "boolean", expr: tree.NewAndExpr(literal(), tree.NewNotExpr(literal()))},
		{name: "xor", expr: &tree.XorExpr{Left: literal(), Right: literal()}},
		{name: "or", expr: &tree.OrExpr{Left: literal(), Right: literal()}},
		{name: "is-null", expr: &tree.IsNullExpr{Expr: literal()}},
		{name: "paren", expr: &tree.ParenExpr{Expr: literal()}},
		{name: "tuple", expr: &tree.Tuple{Exprs: tree.Exprs{literal(), literal()}}},
		{name: "interval", expr: &tree.IntervalExpr{Expr: literal()}},
		{name: "default", expr: &tree.DefaultVal{Expr: literal()}},
		{name: "variable", expr: &tree.VarExpr{Expr: literal()}},
		{name: "case", expr: tree.NewCaseExpr(nil, []*tree.When{tree.NewWhen(literal(), literal())}, literal())},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.False(t, hasWindowFrameParam(tc.expr))
		})
	}

	require.False(t, isPreparedWindowIntervalBound(testScalarFuncExpr("interval", literal())))
}

func firstWindowSpec(t *testing.T, queryPlan *planpb.Plan) *planpb.WindowSpec {
	t.Helper()
	for _, node := range queryPlan.GetQuery().Nodes {
		for _, window := range node.WinSpecList {
			if window.GetW() != nil {
				return window.GetW()
			}
		}
	}
	require.Fail(t, "expected window spec in query plan")
	return nil
}

func allWindowSpecs(queryPlan *planpb.Plan) []*planpb.WindowSpec {
	var result []*planpb.WindowSpec
	for _, node := range queryPlan.GetQuery().Nodes {
		for _, window := range node.WinSpecList {
			if spec := window.GetW(); spec != nil {
				result = append(result, spec)
			}
		}
	}
	return result
}

func queryHasReachableTable(query *planpb.Query, table string) bool {
	visited := make(map[int32]struct{})
	var visit func(int32) bool
	visit = func(nodeID int32) bool {
		if _, ok := visited[nodeID]; ok || nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return false
		}
		visited[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node == nil {
			return false
		}
		if node.NodeType == planpb.Node_TABLE_SCAN && node.GetObjRef().GetObjName() == table {
			return true
		}
		for _, child := range node.Children {
			if visit(child) {
				return true
			}
		}
		return false
	}
	for _, step := range query.Steps {
		if visit(step) {
			return true
		}
	}
	return false
}

func buildNamedWindowPlan(t *testing.T, sql string) (*planpb.Plan, error) {
	t.Helper()
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
	if err != nil {
		return nil, err
	}
	return BuildPlan(NewMockCompilerContext(true), stmt, false)
}

func TestNamedWindowSpecHelpers(t *testing.T) {
	ctx := context.Background()

	require.Nil(t, cloneWindowSpec(nil))
	original := &tree.WindowSpec{
		OrderBy: tree.OrderBy{
			nil,
			tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false),
		},
		Frame: &tree.FrameClause{
			Start: &tree.FrameBound{Expr: testNumVal(2)},
			End:   &tree.FrameBound{Expr: testNumVal(3)},
		},
	}
	cloned := cloneWindowSpec(original)
	require.NotSame(t, original, cloned)
	require.Nil(t, cloned.OrderBy[0])
	require.NotSame(t, original.OrderBy[1], cloned.OrderBy[1])
	require.NotSame(t, original.Frame, cloned.Frame)
	require.NotSame(t, original.Frame.Start, cloned.Frame.Start)
	require.NotSame(t, original.Frame.End, cloned.Frame.End)

	ensureDefaultWindowFrame(nil)
	defaultFrame := &tree.WindowSpec{}
	ensureDefaultWindowFrame(defaultFrame)
	require.Equal(t, tree.Range, defaultFrame.Frame.Type)
	require.True(t, defaultFrame.Frame.Start.UnBounded)
	require.Equal(t, tree.Following, defaultFrame.Frame.End.Type)
	require.True(t, defaultFrame.Frame.End.UnBounded)
	existingFrame := defaultFrame.Frame
	ensureDefaultWindowFrame(defaultFrame)
	require.Same(t, existingFrame, defaultFrame.Frame)

	base := &tree.WindowSpec{
		PartitionBy: tree.Exprs{testNumVal(1)},
		Frame:       existingFrame,
	}
	local := &tree.WindowSpec{RefName: tree.NewCStr("base", 1)}
	inherited, err := inheritWindowSpec(ctx, base, local, "derived", "base")
	require.NoError(t, err)
	require.Len(t, inherited.PartitionBy, 1)
	require.False(t, inherited.HasFrame)
	require.Nil(t, inherited.Frame)
	require.Nil(t, inherited.RefName)

	_, err = resolveNamedWindowDefinitions(ctx, tree.WindowDefinitions{nil})
	require.ErrorContains(t, err, "Invalid named window definition")
	_, err = resolveNamedWindowDefinitions(ctx, tree.WindowDefinitions{{
		Name: tree.NewCStr("derived", 1),
		Spec: &tree.WindowSpec{RefName: tree.NewCStr("missing", 1)},
	}})
	require.ErrorContains(t, err, "Window name 'missing' is not defined")

	resolved, err := resolveWindowSpecReference(ctx, nil, nil)
	require.NoError(t, err)
	require.Nil(t, resolved)
	_, err = resolveWindowSpecReference(ctx, local, nil)
	require.ErrorContains(t, err, "Window name 'base' is not defined")

	named := map[string]*tree.WindowSpec{"base": base}
	referenced, err := resolveWindowSpecReference(ctx, &tree.WindowSpec{
		RefName:        tree.NewCStr("base", 1),
		ReferencedOnly: true,
	}, named)
	require.NoError(t, err)
	require.Nil(t, referenced.RefName)
	require.False(t, referenced.ReferencedOnly)
	require.Len(t, referenced.PartitionBy, 1)

	plainClause := &tree.SelectClause{}
	expanded, orderBy, err := expandNamedWindowReferences(ctx, plainClause, nil)
	require.NoError(t, err)
	require.Same(t, plainClause, expanded)
	require.Nil(t, orderBy)
}

func TestExpandNamedWindowReferencesAcrossClauses(t *testing.T) {
	windowRef := func(name string) *tree.FuncExpr {
		return testWindowFuncExpr(
			"sum",
			tree.FUNC_TYPE_DEFAULT,
			&tree.WindowSpec{
				RefName:        tree.NewCStr(name, 1),
				ReferencedOnly: true,
			},
			testNumVal(1),
		)
	}
	definitions := tree.WindowDefinitions{{
		Name: tree.NewCStr("base", 1),
		Spec: &tree.WindowSpec{PartitionBy: tree.Exprs{testNumVal(1)}},
	}}
	clause := &tree.SelectClause{
		Exprs:   tree.SelectExprs{{Expr: windowRef("base")}},
		Where:   &tree.Where{Expr: windowRef("base")},
		GroupBy: &tree.GroupByClause{GroupByExprsList: []tree.Exprs{{windowRef("base")}}},
		Having:  &tree.Where{Expr: windowRef("base")},
		Windows: definitions,
	}
	inputOrderBy := tree.OrderBy{
		nil,
		tree.NewOrder(windowRef("base"), tree.Ascending, tree.DefaultNullsPosition, false),
	}

	expanded, orderBy, err := expandNamedWindowReferences(context.Background(), clause, inputOrderBy)
	require.NoError(t, err)
	require.NotSame(t, clause, expanded)
	require.Nil(t, expanded.Exprs[0].Expr.(*tree.FuncExpr).WindowSpec.RefName)
	require.Nil(t, expanded.Where.Expr.(*tree.FuncExpr).WindowSpec.RefName)
	require.Nil(t, expanded.GroupBy.GroupByExprsList[0][0].(*tree.FuncExpr).WindowSpec.RefName)
	require.Nil(t, expanded.Having.Expr.(*tree.FuncExpr).WindowSpec.RefName)
	require.Nil(t, orderBy[0])
	require.Nil(t, orderBy[1].Expr.(*tree.FuncExpr).WindowSpec.RefName)
	require.NotSame(t, inputOrderBy[1], orderBy[1])

	badClause := &tree.SelectClause{
		Exprs:   tree.SelectExprs{{Expr: windowRef("missing")}},
		Windows: definitions,
	}
	expanded, orderBy, err = expandNamedWindowReferences(context.Background(), badClause, nil)
	require.ErrorContains(t, err, "Window name 'missing' is not defined")
	require.Nil(t, expanded)
	require.Nil(t, orderBy)
}

func TestBuildPlanNamedWindows(t *testing.T) {
	t.Run("reuse", func(t *testing.T) {
		queryPlan, err := buildNamedWindowPlan(t, `
			select sum(n_nationkey) over win, rank() over win
			from nation
			window win as (partition by n_regionkey order by n_nationkey)`)
		require.NoError(t, err)
		windows := allWindowSpecs(queryPlan)
		require.Len(t, windows, 2)
		for _, window := range windows {
			require.Len(t, window.PartitionBy, 1)
			require.Len(t, window.OrderBy, 1)
			require.Equal(t, planpb.FrameClause_RANGE, window.Frame.Type)
			require.Equal(t, planpb.FrameBound_CURRENT_ROW, window.Frame.End.Type)
		}
	})

	t.Run("forward inheritance", func(t *testing.T) {
		queryPlan, err := buildNamedWindowPlan(t, `
			select sum(n_nationkey) over ordered_win
			from nation
			window ordered_win as (base_win order by n_nationkey rows unbounded preceding),
			       base_win as (partition by n_regionkey)`)
		require.NoError(t, err)
		window := firstWindowSpec(t, queryPlan)
		require.Len(t, window.PartitionBy, 1)
		require.Len(t, window.OrderBy, 1)
		require.Equal(t, planpb.FrameClause_ROWS, window.Frame.Type)
		require.Equal(t, planpb.FrameBound_CURRENT_ROW, window.Frame.End.Type)
	})

	t.Run("bounded rows frame", func(t *testing.T) {
		queryPlan, err := buildNamedWindowPlan(t, `
			select sum(n_nationkey) over framed
			from nation
			window framed as (partition by n_regionkey order by n_nationkey
			                  rows between 1 preceding and current row)`)
		require.NoError(t, err)
		window := firstWindowSpec(t, queryPlan)
		require.Equal(t, planpb.FrameClause_ROWS, window.Frame.Type)
		require.NotNil(t, window.Frame.Start.Val)
		require.Equal(t, planpb.FrameBound_CURRENT_ROW, window.Frame.End.Type)
	})

	t.Run("consumer specific range validation", func(t *testing.T) {
		_, inlineErr := buildNamedWindowPlan(t, `
			select rank() over (order by n_name range 1 preceding)
			from nation`)
		_, namedErr := buildNamedWindowPlan(t, `
			select rank() over win
			from nation
			window win as (order by n_name range 1 preceding)`)
		require.NoError(t, inlineErr)
		require.NoError(t, namedErr)
	})

	t.Run("cte subquery validation is isolated", func(t *testing.T) {
		_, err := buildNamedWindowPlan(t, `
			with c as (select n_nationkey from nation)
			select n_nationkey
			from c
			window win as (order by (select max(n_nationkey) from c))`)
		require.NoError(t, err)
	})
}

func TestBuildPlanDefaultValueWindowFrames(t *testing.T) {
	tests := []struct {
		name         string
		sql          string
		frameType    planpb.FrameClause_FrameType
		endType      planpb.FrameBound_BoundType
		endUnbounded bool
	}{
		{
			name:      "first value uses ordered default frame",
			sql:       "select first_value(n_nationkey) over (order by n_regionkey) from nation",
			frameType: planpb.FrameClause_RANGE,
			endType:   planpb.FrameBound_CURRENT_ROW,
		},
		{
			name:      "last value uses ordered default frame",
			sql:       "select last_value(n_nationkey) over (order by n_regionkey) from nation",
			frameType: planpb.FrameClause_RANGE,
			endType:   planpb.FrameBound_CURRENT_ROW,
		},
		{
			name:      "nth value uses ordered default frame",
			sql:       "select nth_value(n_nationkey, 2) over (order by n_regionkey) from nation",
			frameType: planpb.FrameClause_RANGE,
			endType:   planpb.FrameBound_CURRENT_ROW,
		},
		{
			name:         "lag remains frame independent",
			sql:          "select lag(n_nationkey) over (order by n_regionkey) from nation",
			frameType:    planpb.FrameClause_ROWS,
			endType:      planpb.FrameBound_FOLLOWING,
			endUnbounded: true,
		},
		{
			name:         "lead remains frame independent",
			sql:          "select lead(n_nationkey) over (order by n_regionkey) from nation",
			frameType:    planpb.FrameClause_ROWS,
			endType:      planpb.FrameBound_FOLLOWING,
			endUnbounded: true,
		},
		{
			name: "explicit full frame is preserved",
			sql: `select last_value(n_nationkey) over (
				order by n_regionkey rows between unbounded preceding and unbounded following
			) from nation`,
			frameType:    planpb.FrameClause_ROWS,
			endType:      planpb.FrameBound_FOLLOWING,
			endUnbounded: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			queryPlan, err := buildNamedWindowPlan(t, tc.sql)
			require.NoError(t, err)
			window := firstWindowSpec(t, queryPlan)
			require.Equal(t, tc.frameType, window.Frame.Type)
			require.Equal(t, planpb.FrameBound_PRECEDING, window.Frame.Start.Type)
			require.True(t, window.Frame.Start.UnBounded)
			require.Equal(t, tc.endType, window.Frame.End.Type)
			require.Equal(t, tc.endUnbounded, window.Frame.End.UnBounded)
		})
	}
}

func TestPreparedNamedWindowParameterMetadata(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "used",
			sql:  "select sum(n_nationkey) over w from nation window w as (order by n_nationkey rows ? preceding)",
		},
		{
			name: "unused",
			sql:  "select 1 from nation window w as (order by n_nationkey rows ? preceding)",
		},
		{
			name: "reused by multiple functions",
			sql:  "select sum(n_nationkey) over w, avg(n_nationkey) over w from nation window w as (order by n_nationkey rows ? preceding)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(
				NewMockOptimizer(false),
				t,
				"prepare named_window_param from '"+test.sql+"'",
			)
			require.NoError(t, err)
			prepare := logicPlan.GetDcl().GetPrepare()
			require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
			require.Len(t, prepare.Plan.GetQuery().Params, 1)
			require.Equal(t, int32(0), prepare.Plan.GetQuery().Params[0].GetP().Pos)
		})
	}

	t.Run("nested definitions are globally deduplicated", func(t *testing.T) {
		logicPlan, err := runOneStmt(
			NewMockOptimizer(false),
			t,
			`prepare nested_named_window_param from '
				select 1 from nation
				window outer_w as (order by (
					select 1 from nation
					window inner_w as (order by n_nationkey rows ? preceding)
				))'`,
		)
		require.NoError(t, err)
		prepare := logicPlan.GetDcl().GetPrepare()
		require.Equal(t, []int32{int32(types.T_any)}, prepare.ParamTypes)
		require.Len(t, prepare.Plan.GetQuery().Params, 1)
	})
}

func TestNamedWindowValidationRetainsDependencies(t *testing.T) {
	const sql = "select 1 from nation window unused_w as (order by (select r_name from region limit 1))"

	t.Run("ordinary plan", func(t *testing.T) {
		queryPlan, err := buildNamedWindowPlan(t, sql)
		require.NoError(t, err)
		require.Len(t, queryPlan.GetQuery().GetCatalogDependencies(), 1)
		dependency := queryPlan.GetQuery().GetCatalogDependencies()[0]
		require.Equal(t, "region", dependency.GetObjName())
		require.False(t, queryHasReachableTable(queryPlan.GetQuery(), "region"))
	})

	t.Run("used window control", func(t *testing.T) {
		queryPlan, err := buildNamedWindowPlan(t,
			"select sum(n_nationkey) over unused_w from nation window unused_w as (order by (select r_name from region limit 1))")
		require.NoError(t, err)
		require.Len(t, queryPlan.GetQuery().GetCatalogDependencies(), 1)
		require.Equal(t, "region", queryPlan.GetQuery().GetCatalogDependencies()[0].GetObjName())
	})

	t.Run("prepare schema invalidation", func(t *testing.T) {
		logicPlan, err := runOneStmt(
			NewMockOptimizer(false), t,
			"prepare unused_named_window_dependency from '"+sql+"'",
		)
		require.NoError(t, err)
		prepare := logicPlan.GetDcl().GetPrepare()
		require.Len(t, prepare.GetSchemas(), 2)
		refs := map[string]bool{}
		for _, schema := range prepare.GetSchemas() {
			refs[schema.GetObjName()] = true
		}
		require.Equal(t, map[string]bool{"nation": true, "region": true}, refs)
	})
}

func TestWindowValidationPrivilegeCarriersAreCompactAndDeduplicated(t *testing.T) {
	owner := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	validation := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	wideTable := &planpb.TableDef{
		TableType: "ordinary",
		Cols:      make([]*planpb.ColDef, 1024),
	}
	ordinary := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		ObjRef:   &planpb.ObjectRef{SchemaName: "tpch", ObjName: "region", Obj: 1},
		TableDef: wideTable,
	}
	validation.qry.Nodes = append(validation.qry.Nodes, ordinary)
	for i := 0; i < maxWindowsPerQueryBlock-1; i++ {
		validation.qry.Nodes = append(validation.qry.Nodes, ordinary)
	}
	validation.qry.Nodes = append(validation.qry.Nodes,
		&planpb.Node{
			NodeType: planpb.Node_EXTERNAL_SCAN,
			ObjRef:   &planpb.ObjectRef{SchemaName: "mongodb", ObjName: "events", Obj: 2},
			TableDef: wideTable,
			ExternScan: &planpb.ExternScan{
				Type: int32(planpb.ExternType_MONGODB_TB),
			},
		},
		&planpb.Node{
			NodeType: planpb.Node_FUNCTION_SCAN,
			ObjRef:   &planpb.ObjectRef{SchemaName: "tpch", ObjName: "nation", Obj: 3},
			TableDef: &planpb.TableDef{TableType: "func_table", TblFunc: &planpb.TableFunction{Name: "table_changes"}, Cols: wideTable.Cols},
		},
		&planpb.Node{
			NodeType: planpb.Node_EXTERNAL_SCAN,
			ObjRef:   &planpb.ObjectRef{SchemaName: "external", ObjName: "ignored", Obj: 4},
			ExternScan: &planpb.ExternScan{
				Type: int32(planpb.ExternType_EXTERNAL_TB),
			},
		},
	)

	appendWindowValidationPrivilegeScans(owner, validation)
	require.Len(t, owner.windowValidationScans, 3)

	carriers := make(map[planpb.Node_NodeType]*planpb.Node)
	for _, carrier := range owner.windowValidationScans {
		carriers[carrier.NodeType] = carrier
		require.Empty(t, carrier.GetTableDef().GetCols())
	}
	require.Equal(t, "region", carriers[planpb.Node_TABLE_SCAN].GetObjRef().GetObjName())
	require.Equal(t, int32(planpb.ExternType_MONGODB_TB), carriers[planpb.Node_EXTERNAL_SCAN].GetExternScan().GetType())
	require.Equal(t, "table_changes", carriers[planpb.Node_FUNCTION_SCAN].GetTableDef().GetTblFunc().GetName())

	// A different view path is a distinct authorization context, even for the
	// same relation and snapshot.
	validation.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		ObjRef:      ordinary.ObjRef,
		TableDef:    wideTable,
		OriginViews: []string{"tpch.region_view"},
		DirectView:  "tpch.region_view",
	}}
	appendWindowValidationPrivilegeScans(owner, validation)
	require.Len(t, owner.windowValidationScans, 4)
}

func namedWindowsSQL(prefix string, count int) string {
	var sql strings.Builder
	for i := 0; i < count; i++ {
		if i > 0 {
			sql.WriteString(", ")
		}
		sql.WriteString(prefix)
		sql.WriteString(strconv.Itoa(i))
		sql.WriteString(" as ()")
	}
	return sql.String()
}

func TestNamedWindowLimitPerQueryBlock(t *testing.T) {
	t.Run("127 named windows", func(t *testing.T) {
		_, err := buildNamedWindowPlan(t, "select 1 from nation window "+namedWindowsSQL("w", 127))
		require.NoError(t, err)
	})

	t.Run("128 named windows", func(t *testing.T) {
		_, err := buildNamedWindowPlan(t, "select 1 from nation window "+namedWindowsSQL("w", 128))
		require.Error(t, err)
		moErr, ok := err.(*moerr.Error)
		require.True(t, ok)
		require.Equal(t, moerr.ER_TOO_MANY_WINDOWS, moErr.MySQLCode())
		require.ErrorContains(t, err, "Too many windows in SELECT: 128. Maximum allowed is 127")
	})

	t.Run("named plus implicit", func(t *testing.T) {
		_, err := buildNamedWindowPlan(t,
			"select row_number() over (), rank() over () from nation window "+namedWindowsSQL("w", 125))
		require.NoError(t, err)
		_, err = buildNamedWindowPlan(t,
			"select row_number() over () from nation window "+namedWindowsSQL("w", 126))
		require.NoError(t, err)

		_, err = buildNamedWindowPlan(t,
			"select row_number() over (), rank() over () from nation window "+namedWindowsSQL("w", 126))
		require.ErrorContains(t, err, "Too many windows in SELECT: 128. Maximum allowed is 127")
	})

	t.Run("named references reuse but parenthesized references are implicit", func(t *testing.T) {
		_, err := buildNamedWindowPlan(t,
			"select row_number() over w0 from nation window "+namedWindowsSQL("w", 127))
		require.NoError(t, err)

		_, err = buildNamedWindowPlan(t,
			"select row_number() over (w0) from nation window "+namedWindowsSQL("w", 127))
		require.ErrorContains(t, err, "Too many windows in SELECT: 128. Maximum allowed is 127")
	})

	t.Run("nested selects have independent limits", func(t *testing.T) {
		sql := "select (select 1 from nation window " + namedWindowsSQL("inner_w", 127) +
			") from nation window " + namedWindowsSQL("outer_w", 127)
		_, err := buildNamedWindowPlan(t, sql)
		require.NoError(t, err)
	})
}

func TestSnapshotWindowValidationCTEState(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	declarationCtx := NewBindContext(builder, nil)
	declarationCtx.views = []string{"original_view"}
	ref := &CTERef{
		isRecursive:    true,
		declarationCtx: declarationCtx,
		occurrences:    []cteOccurrence{{rootID: 7, rootTag: 8}},
	}
	ctx.cteByName = map[string]*CTERef{"c": ref}

	restore := snapshotWindowValidationCTEState(ctx)
	ref.isRecursive = false
	ref.occurrences = append(ref.occurrences, cteOccurrence{rootID: 70, rootTag: 80})
	ref.hasNestedRef = true
	ref.hasNestedUse = true
	declarationCtx.views = append(declarationCtx.views, "validation_view")
	restore()

	require.True(t, ref.isRecursive)
	require.Equal(t, []cteOccurrence{{rootID: 7, rootTag: 8}}, ref.occurrences)
	require.False(t, ref.hasNestedRef)
	require.False(t, ref.hasNestedUse)
	require.Equal(t, []string{"original_view"}, declarationCtx.views)
}

func TestBuildPlanRejectsInvalidNamedWindows(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantErr   string
		mysqlCode uint16
	}{
		{
			name:      "unknown",
			sql:       "select sum(n_nationkey) over missing from nation",
			wantErr:   "Window name 'missing' is not defined",
			mysqlCode: moerr.ER_WINDOW_NO_SUCH_WINDOW,
		},
		{
			name:      "duplicate",
			sql:       "select 1 from nation window w as (), w as ()",
			wantErr:   "Window 'w' is defined twice",
			mysqlCode: moerr.ER_WINDOW_DUPLICATE_NAME,
		},
		{
			name:      "cycle",
			sql:       "select 1 from nation window w1 as (w2), w2 as (w1)",
			wantErr:   "circularity",
			mysqlCode: moerr.ER_WINDOW_CIRCULARITY_IN_WINDOW_GRAPH,
		},
		{
			name:      "inherited partition",
			sql:       "select 1 from nation window w1 as (), w2 as (w1 partition by n_regionkey)",
			wantErr:   "cannot define partitioning",
			mysqlCode: moerr.ER_WINDOW_NO_CHILD_PARTITIONING,
		},
		{
			name:      "inherited order",
			sql:       "select 1 from nation window w1 as (order by n_regionkey), w2 as (w1 order by n_nationkey)",
			wantErr:   "Window 'w2' cannot inherit 'w1' since both contain an ORDER BY clause.",
			mysqlCode: moerr.ER_WINDOW_NO_REDEFINE_ORDER_BY,
		},
		{
			name:      "inline inherited order",
			sql:       "select sum(n_nationkey) over (w1 order by n_regionkey) from nation window w1 as (order by n_name)",
			wantErr:   "Window '<unnamed window>' cannot inherit 'w1' since both contain an ORDER BY clause.",
			mysqlCode: moerr.ER_WINDOW_NO_REDEFINE_ORDER_BY,
		},
		{
			name:      "inherited frame",
			sql:       "select 1 from nation window w1 as (rows unbounded preceding), w2 as (w1)",
			wantErr:   "has a frame definition",
			mysqlCode: moerr.ER_WINDOW_NO_INHERIT_FRAME,
		},
		{
			name:    "unused unknown column",
			sql:     "select 1 from nation window w as (partition by no_such_column)",
			wantErr: "no_such_column",
		},
		{
			name:    "unused nested window",
			sql:     "select 1 from nation window w as (partition by row_number() over ())",
			wantErr: "cannot use the window function",
		},
		{
			name:    "unused illegal frame",
			sql:     "select 1 from nation window w as (rows between unbounded following and current row)",
			wantErr: "frame start cannot be UNBOUNDED FOLLOWING",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildNamedWindowPlan(t, test.sql)
			require.ErrorContains(t, err, test.wantErr)
			if test.mysqlCode != 0 {
				moErr := err.(*moerr.Error)
				require.Equal(t, test.mysqlCode, moErr.MySQLCode())
				require.Equal(t, moerr.MySQLDefaultSqlState, moErr.SqlState())
			}
		})
	}
}

func TestResolveNamedWindowDefinitionsReportsFirstDefinitionError(t *testing.T) {
	ctx := context.Background()
	definitions := tree.WindowDefinitions{
		{
			Name: tree.NewCStr("first", 1),
			Spec: &tree.WindowSpec{RefName: tree.NewCStr("MissingFirst", 1)},
		},
		{
			Name: tree.NewCStr("second", 1),
			Spec: &tree.WindowSpec{RefName: tree.NewCStr("missing_second", 1)},
		},
	}

	for range 1000 {
		_, err := resolveNamedWindowDefinitions(ctx, definitions)
		require.ErrorContains(t, err, "Window name 'MissingFirst' is not defined")
		moErr := err.(*moerr.Error)
		require.Equal(t, moerr.ER_WINDOW_NO_SUCH_WINDOW, moErr.MySQLCode())
		require.Equal(t, moerr.MySQLDefaultSqlState, moErr.SqlState())
	}
}

func requirePreparedRowsFrameParam(t *testing.T, expr *planpb.Expr, pos int32) {
	requirePreparedWindowFrameParam(t, expr, types.T_uint64, pos)
}

func requirePreparedWindowFrameParam(t *testing.T, expr *planpb.Expr, typ types.T, pos int32) {
	t.Helper()
	require.Equal(t, int32(typ), expr.Typ.Id)
	cast := expr.GetF()
	require.NotNil(t, cast)
	require.Equal(t, "cast", cast.Func.ObjName)
	require.NotEmpty(t, cast.Args)
	param := cast.Args[0].GetP()
	require.NotNil(t, param)
	require.Equal(t, pos, param.Pos)
}

func TestProjectionAndHavingBinderBindExprOnWindowAlias(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.windowTag = builder.GenNewBindTag()

	windowExpr := testLagWindowExpr()
	astStr := tree.String(windowExpr, dialect.MYSQL)
	bindCtx.windowByAst[astStr] = 0
	bindCtx.windows = []*planpb.Expr{{Typ: planpb.Type{Id: int32(types.T_int64)}}}

	havingBinder := NewHavingBinder(builder, bindCtx)
	havingExpr, err := havingBinder.BindExpr(windowExpr, 0, true)
	require.NoError(t, err)
	require.Equal(t, bindCtx.windowTag, havingExpr.GetCol().RelPos)
	require.Equal(t, int32(0), havingExpr.GetCol().ColPos)

	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)
	projExpr, err := projectionBinder.BindExpr(windowExpr, 0, true)
	require.NoError(t, err)
	require.Equal(t, bindCtx.windowTag, projExpr.GetCol().RelPos)
	require.Equal(t, int32(0), projExpr.GetCol().ColPos)
}

func TestProjectionBinderBindWinFuncCachesWindowExpr(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)
	bindCtx.windowTag = builder.GenNewBindTag()

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

	firstExpr, err := projectionBinder.BindWinFunc("lag", testLagWindowExpr(), 0, true)
	require.NoError(t, err)
	require.Len(t, bindCtx.windows, 1)
	require.Equal(t, bindCtx.windowTag, firstExpr.GetCol().RelPos)
	require.Equal(t, int32(0), firstExpr.GetCol().ColPos)

	windowSpec := bindCtx.windows[0].GetW()
	require.Equal(t, "lag", windowSpec.Name)
	require.Len(t, windowSpec.PartitionBy, 1)
	require.Len(t, windowSpec.OrderBy, 1)
	require.Equal(t, planpb.FrameClause_ROWS, windowSpec.Frame.Type)
	require.True(t, windowSpec.Frame.Start.UnBounded)
	require.True(t, windowSpec.Frame.End.UnBounded)
	require.Equal(t, planpb.OrderBySpec_DESC|planpb.OrderBySpec_NULLS_LAST|planpb.OrderBySpec_INTERNAL, windowSpec.OrderBy[0].Flag)

	secondExpr, err := projectionBinder.BindWinFunc("lag", testLagWindowExpr(), 0, true)
	require.NoError(t, err)
	require.Len(t, bindCtx.windows, 1)
	require.Equal(t, firstExpr.GetCol().RelPos, secondExpr.GetCol().RelPos)
	require.Equal(t, firstExpr.GetCol().ColPos, secondExpr.GetCol().ColPos)
}

func TestProjectionBinderRejectsNestedWindowFuncFromSQL(t *testing.T) {
	builder, bindCtx := genBuilderAndCtx()
	bindCtx.windowTag = builder.GenNewBindTag()

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

	stmts, err := parsers.Parse(
		context.TODO(),
		dialect.MYSQL,
		"select sum(row_number() over (order by a)) over () from select_test.bind_select",
		1,
	)
	require.NoError(t, err)
	selectClause := stmts[0].(*tree.Select).Select.(*tree.SelectClause)

	_, err = projectionBinder.BindExpr(selectClause.Exprs[0].Expr, 0, true)
	require.ErrorContains(t, err, "You cannot use the window function 'row_number' in this context")
}

func TestBuildPlanWindowDependencyValidation(t *testing.T) {
	ctx := NewMockCompilerContext(true)

	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "alias in argument",
			sql:     "select row_number() over () as rn, sum(rn) over () from select_test.bind_select",
			wantErr: "window function result in another window function",
		},
		{
			name:    "alias in partition by",
			sql:     "select row_number() over () as rn, sum(a) over (partition by rn) from select_test.bind_select",
			wantErr: "window function result in another window function",
		},
		{
			name:    "alias in window order by",
			sql:     "select row_number() over () as rn, sum(a) over (order by rn) from select_test.bind_select",
			wantErr: "window function result in another window function",
		},
		{
			name:    "nested window in function-local order by",
			sql:     "select group_concat(a order by row_number() over ()) over () from select_test.bind_select",
			wantErr: "You cannot use the window function 'row_number' in this context",
		},
		{
			name:    "function-local order by is explicit unsupported behavior",
			sql:     "select group_concat(a order by a) over () from select_test.bind_select",
			wantErr: "function-local ORDER BY in window function",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			_, err = BuildPlan(ctx, stmts[0], false)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}

	positiveSQL := []string{
		"select row_number() over (order by a), sum(a) over () from select_test.bind_select",
		"select sum(sum(a)) over () from select_test.bind_select group by b",
		"select sum((select row_number() over ())) over () from select_test.bind_select",
	}
	for _, sql := range positiveSQL {
		stmts, err := parsers.Parse(context.TODO(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		_, err = BuildPlan(ctx, stmts[0], false)
		require.NoError(t, err, sql)
	}
}

func TestHavingBinderBindWinFuncCoversFrameAndGuard(t *testing.T) {
	t.Run("inside aggregate rejects window func", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		bindCtx.windowTag = builder.GenNewBindTag()

		havingBinder := NewHavingBinder(builder, bindCtx)
		havingBinder.insideAgg = true

		_, err := havingBinder.BindWinFunc("lag", testLagWindowExpr(), 0, true)
		require.Error(t, err)
	})

	t.Run("range frame binds frame constants", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		bindCtx.windowTag = builder.GenNewBindTag()

		havingBinder := NewHavingBinder(builder, bindCtx)
		expr, err := havingBinder.BindWinFunc("sum", testRangeWindowExpr(), 0, true)
		require.NoError(t, err)
		require.Equal(t, bindCtx.windowTag, expr.GetCol().RelPos)
		require.Len(t, bindCtx.windows, 1)

		windowSpec := bindCtx.windows[0].GetW()
		require.Equal(t, planpb.FrameClause_RANGE, windowSpec.Frame.Type)
		require.NotNil(t, windowSpec.Frame.Start.Val)
		require.NotNil(t, windowSpec.Frame.End.Val)
	})
}

func TestBindWindowFuncExprValidationAndHelpers(t *testing.T) {
	t.Run("find nested window function across expression shapes", func(t *testing.T) {
		nested := testRowNumberWindowExpr()
		tests := []struct {
			name string
			expr tree.Expr
		}{
			{name: "binary", expr: &tree.BinaryExpr{Left: testNumVal(1), Right: nested}},
			{name: "unary", expr: &tree.UnaryExpr{Expr: nested}},
			{name: "comparison", expr: tree.NewComparisonExpr(tree.EQUAL, testNumVal(1), nested)},
			{name: "and", expr: &tree.AndExpr{Left: testNumVal(1), Right: nested}},
			{name: "xor", expr: &tree.XorExpr{Left: testNumVal(1), Right: nested}},
			{name: "or", expr: &tree.OrExpr{Left: testNumVal(1), Right: nested}},
			{name: "not", expr: &tree.NotExpr{Expr: nested}},
			{name: "is null", expr: &tree.IsNullExpr{Expr: nested}},
			{name: "is not null", expr: &tree.IsNotNullExpr{Expr: nested}},
			{name: "is unknown", expr: &tree.IsUnknownExpr{Expr: nested}},
			{name: "is not unknown", expr: &tree.IsNotUnknownExpr{Expr: nested}},
			{name: "is true", expr: &tree.IsTrueExpr{Expr: nested}},
			{name: "is not true", expr: &tree.IsNotTrueExpr{Expr: nested}},
			{name: "is false", expr: &tree.IsFalseExpr{Expr: nested}},
			{name: "is not false", expr: &tree.IsNotFalseExpr{Expr: nested}},
			{name: "paren", expr: &tree.ParenExpr{Expr: nested}},
			{name: "cast", expr: &tree.CastExpr{Expr: nested}},
			{name: "bit cast", expr: &tree.BitCastExpr{Expr: nested}},
			{name: "tuple", expr: &tree.Tuple{Exprs: tree.Exprs{testNumVal(1), nested}}},
			{name: "range", expr: tree.NewRangeCond(false, testNumVal(1), testNumVal(2), nested)},
			{name: "case expr", expr: &tree.CaseExpr{Expr: nested}},
			{name: "case when", expr: &tree.CaseExpr{Whens: []*tree.When{{Cond: testNumVal(1), Val: nested}}}},
			{name: "case else", expr: &tree.CaseExpr{Whens: []*tree.When{nil}, Else: nested}},
			{name: "interval", expr: &tree.IntervalExpr{Expr: nested}},
			{name: "default", expr: tree.NewDefaultVal(nested)},
			{name: "serial extract", expr: &tree.SerialExtractExpr{SerialExpr: testNumVal(1), IndexExpr: nested}},
			{
				name: "function order by",
				expr: &tree.FuncExpr{
					Func:    tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("group_concat")),
					Type:    tree.FUNC_TYPE_DEFAULT,
					Exprs:   tree.Exprs{testNumVal(1)},
					OrderBy: tree.OrderBy{nil, tree.NewOrder(nested, tree.Ascending, tree.DefaultNullsPosition, false)},
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				name, ok := findNestedWindowFuncName(tc.expr)
				require.True(t, ok)
				require.Equal(t, "row_number", name)
			})
		}

		name, ok := findNestedWindowFuncName(nil)
		require.False(t, ok)
		require.Empty(t, name)

		name, ok = findNestedWindowFuncName(testNumVal(1))
		require.False(t, ok)
		require.Empty(t, name)

		name, ok = findNestedWindowFuncName(&tree.Subquery{})
		require.False(t, ok)
		require.Empty(t, name)

		name, ok = findNestedWindowFuncNameInOrderBy(tree.OrderBy{nil})
		require.False(t, ok)
		require.Empty(t, name)
	})

	t.Run("nested window function argument is rejected", func(t *testing.T) {
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(
			testWindowValidationBinder(),
			ctx,
			"sum",
			testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT, testRowsWindowSpec(), testRowNumberWindowExpr()),
			0,
			true,
		)
		require.ErrorContains(t, err, "You cannot use the window function 'row_number' in this context")
	})

	t.Run("nested window function inside scalar argument is rejected", func(t *testing.T) {
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(
			testWindowValidationBinder(),
			ctx,
			"sum",
			testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT, testRowsWindowSpec(), testScalarFuncExpr("abs", testRowNumberWindowExpr())),
			0,
			true,
		)
		require.ErrorContains(t, err, "You cannot use the window function 'row_number' in this context")
	})

	t.Run("nested window function in window spec is rejected", func(t *testing.T) {
		tests := []struct {
			name string
			ws   *tree.WindowSpec
		}{
			{
				name: "partition by",
				ws: func() *tree.WindowSpec {
					ws := testRowsWindowSpec()
					ws.PartitionBy = tree.Exprs{testRowNumberWindowExpr()}
					return ws
				}(),
			},
			{
				name: "order by",
				ws: func() *tree.WindowSpec {
					ws := testRowsWindowSpec()
					ws.OrderBy = tree.OrderBy{tree.NewOrder(testRowNumberWindowExpr(), tree.Ascending, tree.DefaultNullsPosition, false)}
					return ws
				}(),
			},
			{
				name: "frame start",
				ws: func() *tree.WindowSpec {
					ws := testRowsWindowSpec()
					ws.Frame.Start.Expr = testRowNumberWindowExpr()
					ws.Frame.Start.UnBounded = false
					return ws
				}(),
			},
			{
				name: "frame end",
				ws: func() *tree.WindowSpec {
					ws := testRowsWindowSpec()
					ws.Frame.HasEnd = true
					ws.Frame.End = &tree.FrameBound{
						Type: tree.Following,
						Expr: testRowNumberWindowExpr(),
					}
					return ws
				}(),
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
				_, err := bindWindowFuncExpr(
					testWindowValidationBinder(),
					ctx,
					"sum",
					testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT, tc.ws, testNumVal(1)),
					0,
					true,
				)
				require.ErrorContains(t, err, "You cannot use the window function 'row_number' in this context")
			})
		}
	})

	t.Run("bound window result in frame is rejected", func(t *testing.T) {
		binder := testWindowValidationBinder()
		binder.makeFrameValueFunc = func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
			return &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_int64)},
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 9,
					ColPos: 0,
				}},
			}, nil
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		ws := testRowsWindowSpec()
		ws.Frame.Start.Expr = testNumVal(1)
		ws.Frame.Start.UnBounded = false

		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT, ws, testNumVal(1)),
			0,
			true,
		)
		require.ErrorContains(t, err, "window function result in another window function")
	})

	t.Run("distinct window func is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr(
				"sum",
				tree.FUNC_TYPE_DISTINCT,
				&tree.WindowSpec{
					OrderBy:  tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)},
					HasFrame: true,
					Frame: &tree.FrameClause{
						Type: tree.Rows,
						Start: &tree.FrameBound{
							Type: tree.Preceding,
						},
						End: &tree.FrameBound{
							Type: tree.CurrentRow,
						},
					},
				},
				testNumVal(1),
			),
			0,
			true,
		)
		require.Error(t, err)
	})

	t.Run("groups frame is rejected", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr(
				"sum",
				tree.FUNC_TYPE_DEFAULT,
				&tree.WindowSpec{
					OrderBy:  tree.OrderBy{tree.NewOrder(testNumVal(1), tree.Ascending, tree.DefaultNullsPosition, false)},
					HasFrame: true,
					Frame: &tree.FrameClause{
						Type: tree.Groups,
						Start: &tree.FrameBound{
							Type: tree.Preceding,
						},
						End: &tree.FrameBound{
							Type: tree.CurrentRow,
						},
					},
				},
				testNumVal(1),
			),
			0,
			true,
		)
		require.Error(t, err)
	})

	t.Run("range frame rejects non-numeric order by", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2StringConstExprWithType("x"), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}

		_, err := bindWindowFuncExpr(binder, ctx, "sum", testRangeWindowExpr(), 0, true)
		require.Error(t, err)
	})

	t.Run("range frame without offsets accepts non-numeric order by", func(t *testing.T) {
		binder := &stubWindowBinder{
			bindExprFunc: func(tree.Expr, int32, bool) (*planpb.Expr, error) {
				return makePlan2StringConstExprWithType("x"), nil
			},
			bindFuncExprFunc: func(string, []tree.Expr, int32) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
			makeFrameValueFunc: func(tree.Expr, *planpb.Type) (*planpb.Expr, error) {
				return makePlan2Int64ConstExprWithType(1), nil
			},
		}
		ctx := &BindContext{windowTag: 9, windowByAst: make(map[string]int32)}
		ws := testRangeWindowExpr().WindowSpec
		ws.Frame.Start.Expr = nil
		ws.Frame.Start.UnBounded = true
		ws.Frame.End = &tree.FrameBound{Type: tree.CurrentRow}

		expr, err := bindWindowFuncExpr(
			binder,
			ctx,
			"sum",
			testWindowFuncExpr("sum", tree.FUNC_TYPE_DEFAULT, ws, testNumVal(1)),
			0,
			true,
		)
		require.NoError(t, err)
		require.NotNil(t, expr)
		require.Len(t, ctx.windows, 1)
		require.Equal(t, planpb.FrameClause_RANGE, ctx.windows[0].GetW().Frame.Type)
	})

	t.Run("buildWindowColRefExpr keeps tag and column", func(t *testing.T) {
		expr := buildWindowColRefExpr(&BindContext{windowTag: 17}, planpb.Type{Id: int32(types.T_int64)}, 3)
		require.Equal(t, int32(17), expr.GetCol().RelPos)
		require.Equal(t, int32(3), expr.GetCol().ColPos)
	})
}

func TestWindowFrameConstValueHelpers(t *testing.T) {
	proc := testutil.NewProc(t)

	t.Run("typ nil returns bound expr directly", func(t *testing.T) {
		expected := makePlan2Int64ConstExprWithType(7)
		got, err := makeWindowFrameConstValue(
			func(tree.Expr, int32, bool) (*Expr, error) {
				return expected, nil
			},
			proc,
			context.Background(),
			testNumVal(7),
			nil,
		)
		require.NoError(t, err)
		require.Same(t, expected, got)
	})

	t.Run("typed expr is constant folded", func(t *testing.T) {
		got, err := makeWindowFrameConstValue(
			func(tree.Expr, int32, bool) (*Expr, error) {
				return makePlan2Int64ConstExprWithType(11), nil
			},
			proc,
			context.Background(),
			testNumVal(11),
			&planpb.Type{Id: int32(types.T_int64)},
		)
		require.NoError(t, err)
		require.Equal(t, int64(11), got.GetLit().Value.(*planpb.Literal_I64Val).I64Val)
	})

	t.Run("interval expr is normalized through helper", func(t *testing.T) {
		got, err := makeWindowFrameConstValue(
			func(tree.Expr, int32, bool) (*Expr, error) {
				return &Expr{
					Typ: planpb.Type{Id: int32(types.T_interval)},
					Expr: &planpb.Expr_List{
						List: &planpb.ExprList{
							List: []*planpb.Expr{
								makePlan2StringConstExprWithType("2"),
								makePlan2StringConstExprWithType("day"),
							},
						},
					},
				}, nil
			},
			proc,
			context.Background(),
			testNumVal(1),
			nil,
		)
		require.NoError(t, err)
		require.Equal(t, int64(2), got.GetList().List[0].GetLit().Value.(*planpb.Literal_I64Val).I64Val)
	})

	t.Run("reset interval handles numeric value", func(t *testing.T) {
		expr := &Expr{
			Typ: planpb.Type{Id: int32(types.T_interval)},
			Expr: &planpb.Expr_List{
				List: &planpb.ExprList{
					List: []*planpb.Expr{
						makePlan2Int64ConstExprWithType(3),
						makePlan2StringConstExprWithType("day"),
					},
				},
			},
		}
		got, err := resetWindowIntervalExpr(context.Background(), proc, expr)
		require.NoError(t, err)
		require.Equal(t, int64(3), got.GetList().List[0].GetLit().Value.(*planpb.Literal_I64Val).I64Val)
	})
}

func TestResetWindowIntervalExprRejectsNegativeValues(t *testing.T) {
	proc := testutil.NewProc(t)
	testCases := []struct {
		name  string
		value *planpb.Expr
		unit  string
	}{
		{name: "integer", value: makeInt64ConstForProjection(-1), unit: "DAY"},
		{name: "string", value: makeVarcharConstForProjection("-1"), unit: "MONTH"},
		{name: "float rounds to zero", value: makeFloat64ConstForProjection(-0.0000001), unit: "SECOND"},
		{name: "decimal", value: makeDecimal64ConstForProjection(-0.5, 1), unit: "HOUR"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expr := makeIntervalExprForProjection(tc.value, tc.unit)
			got, err := resetWindowIntervalExpr(context.Background(), proc, expr)
			require.Nil(t, got)
			require.ErrorContains(t, err, "frame start or end is negative")
		})
	}
}

func TestBinderMakeFrameConstValueWrappers(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	bindCtx := NewBindContext(builder, nil)

	havingBinder := NewHavingBinder(builder, bindCtx)
	projectionBinder := NewProjectionBinder(builder, bindCtx, havingBinder)

	projExpr, err := projectionBinder.makeFrameConstValue(testNumVal(5), &planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	require.Equal(t, int64(5), projExpr.GetLit().Value.(*planpb.Literal_I64Val).I64Val)

	havingExpr, err := havingBinder.makeFrameConstValue(testNumVal(6), &planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	require.Equal(t, int64(6), havingExpr.GetLit().Value.(*planpb.Literal_I64Val).I64Val)
}

func TestContainsTagCoversWindowSubAndCorrBranches(t *testing.T) {
	windowExpr := &planpb.Expr{
		Expr: &planpb.Expr_W{
			W: &planpb.WindowSpec{
				WindowFunc: &planpb.Expr{
					Expr: &planpb.Expr_Col{
						Col: &planpb.ColRef{RelPos: 1},
					},
				},
				PartitionBy: []*planpb.Expr{
					{
						Expr: &planpb.Expr_List{
							List: &planpb.ExprList{
								List: []*planpb.Expr{
									{
										Expr: &planpb.Expr_Col{
											Col: &planpb.ColRef{RelPos: 2},
										},
									},
								},
							},
						},
					},
				},
				OrderBy: []*planpb.OrderBySpec{
					{
						Expr: &planpb.Expr{
							Expr: &planpb.Expr_Corr{
								Corr: &planpb.CorrColRef{RelPos: 3},
							},
						},
					},
				},
			},
		},
	}

	require.False(t, containsTag(nil, 1))
	require.True(t, containsTag(windowExpr, 1))
	require.True(t, containsTag(windowExpr, 2))
	require.True(t, containsTag(windowExpr, 3))
	require.False(t, containsTag(&planpb.Expr{Expr: &planpb.Expr_Sub{}}, 1))
	require.True(t, containsTag(&planpb.Expr{
		Expr: &planpb.Expr_Sub{
			Sub: &planpb.SubqueryRef{
				Child: &planpb.Expr{
					Expr: &planpb.Expr_Col{
						Col: &planpb.ColRef{RelPos: 4},
					},
				},
			},
		},
	}, 4))
	require.False(t, containsTag(makePlan2Int64ConstExprWithType(1), 5))
}
