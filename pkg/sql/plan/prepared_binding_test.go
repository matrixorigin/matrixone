// Copyright 2026 Matrix Origin
//
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
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestPreparedDecimalFloatFilterUsesUniqueValueProof(t *testing.T) {
	for _, test := range []struct {
		name   string
		value  string
		column types.Type
		native bool
	}{
		{"integral", "54321", types.New(types.T_decimal64, 12, 2), true},
		{"fractional", "0.1", types.New(types.T_decimal64, 12, 2), true},
		{"between scale points", "0.104", types.New(types.T_decimal64, 12, 2), false},
		{"float collision", "9007199254740992", types.New(types.T_decimal128, 20, 0), false},
		{"executor rounding", "2.9999999999999997e-20", types.New(types.T_decimal128, 20, 20), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := withPreparedSourceBindings(context.Background(),
				[]PreparedSourceBinding{{Position: 0, Type: types.T_float64.ToType()}},
				[]any{ParamValue{Value: test.value, IsBinaryProtocol: true}})
			state := preparedBindingState(ctx)
			state.selectStatement = true
			column := &Expr{Typ: makePlan2Type(&test.column), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
			param := &Expr{Typ: makeSimplePlan2Type(types.T_float64), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			args, err := bindPreparedConsumerArguments(ctx, "=", []*Expr{column, param})
			require.NoError(t, err)
			require.Equal(t, test.native, args[1].Typ.Id == column.Typ.Id)
			require.True(t, state.valueDependent)
		})
	}
}

func TestPreparedNumericPredicateFiltering(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
		values          []string
		native          bool
		integerKey      bool
	}{
		{"or expressions", "c=abs(?) or c=abs(?)", []string{"54321", "54322"}, true, false},
		{"in expressions", "c in (abs(?),abs(?))", []string{"54321", "54322"}, true, false},
		{"between expressions", "c between abs(?) and abs(?)", []string{"54321", "54322"}, true, false},
		{"in markers", "c in (?,?)", []string{"54321", "54322"}, true, false},
		{"between markers", "c between ? and ?", []string{"54321", "54322"}, true, false},
		{"nested boolean", "(c=abs(?) or c=abs(?)) and c>=abs(?)", []string{"54321", "54322", "54320"}, true, false},
		{"mixed unsafe in", "c in (?,?)", []string{"54321", "0.104"}, false, false},
		{"mixed unsafe between", "c between ? and ?", []string{"54321", "54322.104"}, false, false},
		{"decimal exact", "c=cast(? as decimal(38,0))", []string{"9007199254740993"}, true, true},
		{"decimal wide transport", "c=cast(? as decimal(65,0))", []string{"9007199254740993"}, true, true},
		{"abs decimal", "c=abs(cast(? as decimal(38,0)))", []string{"-9007199254740993"}, true, true},
		{"decimal rounded input", "c=cast(? as decimal(38,0))", []string{"9007199254740993.5"}, true, true},
		{"decimal fraction", "c=cast(? as decimal(38,1))", []string{"9007199254740993.5"}, false, true},
		{"decimal signed maximum", "c=cast(? as decimal(38,0))", []string{"9223372036854775807"}, true, true},
		{"decimal signed minimum", "c=cast(? as decimal(38,0))", []string{"-9223372036854775808"}, true, true},
		{"abs signed overflow", "c=abs(cast(? as decimal(38,0)))", []string{"-9223372036854775808"}, false, true},
		{"decimal reversed", "cast(? as decimal(38,0))>=c", []string{"9007199254740993"}, true, true},
		{"decimal in", "c in (cast(? as decimal(38,0)),cast(? as decimal(38,0)))", []string{"9007199254740993", "9007199254740994"}, true, true},
		{"decimal between", "c between cast(? as decimal(38,0)) and cast(? as decimal(38,0))", []string{"9007199254740993", "9007199254740994"}, true, true},
		{"decimal arithmetic", "c=cast(? as decimal(38,0))+1", []string{"9007199254740993"}, true, true},
		{"decimal invalid input", "c=cast(? as decimal(38,0))", []string{"not-a-number"}, false, true},
		{"round default precision", "c=round(?)", []string{"54321.0"}, true, true},
		{"truncate default precision", "c=truncate(?)", []string{"54321.0"}, true, true},
		{"round scalar default", "c=round((select ?))", []string{"54321.0"}, true, true},
		{"round scalar strict lower", "c>round((select ?),0)", []string{"54321.0"}, true, true},
		{"round scalar inclusive lower", "c>=round((select ?),0)", []string{"54321.0"}, true, true},
		{"truncate scalar strict upper", "c<truncate((select ?))", []string{"54321.0"}, true, true},
		{"truncate scalar inclusive upper", "c<=truncate((select ?),0)", []string{"54321.0"}, true, true},
		{"round scalar reversed range", "round((select ?))>c", []string{"54321.0"}, true, true},
		{"round scalar fractional result", "c>=round((select ?))", []string{"54321.5"}, true, true},
		{"round scalar collision fallback", "c>=round((select ?))", []string{"9007199254740992"}, false, true},
		{"round zero precision", "c=round(?,?)", []string{"54321.0", "0"}, true, true},
		{"truncate zero precision", "c=truncate(?,?)", []string{"54321.0", "0"}, true, true},
		{"explicit precision cast", "c=round(?,cast(? as signed))", []string{"54321.0", "0"}, true, true},
		{"round nonzero precision", "c=round(?,?)", []string{"54321.0", "1"}, true, true},
		{"round negative precision", "c=round(?,?)", []string{"54321.0", "-1"}, true, true},
		{"explicit column cast", "cast(c as decimal(5,0))=round(?,0)", []string{"54321.0"}, false, true},
		// ROUND sees the source before the explicit result cast clamps it to
		// 9999. The full expression is safe to lower, but the cast must remain.
		{"explicit value cast", "c=cast(round(?,0) as decimal(4,0))", []string{"54321.0"}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			table := makeExprOptCompositeSortKeyTableDef()
			table.Name, table.TblId = "numeric_filters", 99003
			decimal := types.New(types.T_decimal64, 12, 2)
			table.Cols[2].Typ = makePlan2Type(&decimal)
			if tc.integerKey {
				table.Cols[2].Typ = makeSimplePlan2Type(types.T_int64)
			}
			mock.ctxt.tables[table.Name] = table
			mock.ctxt.objects[table.Name] = &ObjectRef{ObjName: table.Name, Obj: int64(table.TblId)}
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			bindings := make([]PreparedSourceBinding, len(tc.values))
			values := make([]any, len(tc.values))
			for i, value := range tc.values {
				bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_float64.ToType()}
				if tc.integerKey {
					bindings[i].Type = types.T_int64.ToType()
					if i == 0 {
						bindings[i].Type = types.T_varchar.ToType()
					}
				}
				values[i] = ParamValue{Value: value, IsBinaryProtocol: true}
				require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
			}
			proc.SetPrepareParams(params)
			if tc.name == "decimal between" {
				originalCtx := mock.ctxt.GetContext()
				ctx := withPreparedSourceBindings(originalCtx, bindings, values)
				preparedBindingState(ctx).selectStatement = true
				mock.ctxt.SetContext(ctx)
				b := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, false)
				column := GetColExpr(makeSimplePlan2Type(types.T_int64), 0, 0)
				domain := makeSimplePlan2Type(types.T_decimal128)
				domain.Width = 38
				promoted, err := appendCastBeforeExpr(ctx, column, domain)
				require.NoError(t, err)
				args := append(make([]*Expr, 0, 3), promoted)
				for pos := range 2 {
					peer, err := appendCastBeforeExpr(ctx, &Expr{Typ: makeSimplePlan2Type(types.T_varchar), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: int32(pos)}}}, domain)
					require.NoError(t, err)
					args = append(args, peer)
				}
				b.qry.Nodes = []*planpb.Node{{NodeType: planpb.Node_JOIN, OnList: []*Expr{{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "between"}, Args: args}}}}}}
				refs := make(map[[2]int32]int)
				b.countColRefs(0, refs)
				require.Equal(t, 1, refs[[2]int32{0, 0}])
				require.NoError(t, b.rewriteNumericDomainFilters(0, planpb.Node_JOIN))
				clear(refs)
				b.countColRefs(0, refs)
				require.Equal(t, 2, refs[[2]int32{0, 0}], "lowered bounds both reference the key")
				mock.ctxt.SetContext(originalCtx)
			}
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select c from numeric_filters where "+tc.predicate, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			require.True(t, bound.ValueDependent, "runtime value proof must not enter the type-only cache")
			foundScan, columnCast, executableParam := false, false, false
			explicitResultCast := false
			for _, node := range bound.Plan.GetQuery().Nodes {
				if node.NodeType != planpb.Node_TABLE_SCAN || node.TableDef.Name != table.Name {
					continue
				}
				foundScan = true
				for _, filter := range node.FilterList {
					if tc.native {
						// BuildPreparedExecutionPlan deliberately skips scan statistics;
						// assert pruning eligibility here and actual blocks in public QA.
						require.True(t, ExprIsZonemappable(context.Background(), filter), "safe predicate must allow block pruning")
					}
					executableParam = executableParam || function.ContainsParameter(filter)
					require.NoError(t, planpb.VisitExprTree(filter, func(expr *Expr) error {
						if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 &&
							fn.Args[0].GetCol() != nil && fn.Args[0].Typ.Id == table.Cols[2].Typ.Id {
							columnCast = true
						}
						if tc.name == "explicit value cast" && types.T(expr.Typ.Id).IsDecimal() &&
							expr.Typ.Width == 4 && expr.Typ.Scale == 0 {
							if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "cast" {
								_, overload := function.DecodeOverloadID(fn.Func.Obj)
								require.EqualValues(t, 1, overload, "retain the explicit, not implicit, cast")
								require.True(t, function.ContainsParameter(expr))
								explicitResultCast = true
							}
						}
						return nil
					}))
				}
			}
			require.True(t, foundScan)
			require.Equal(t, !tc.native, columnCast, bound.Plan.String())
			require.True(t, executableParam, "proof witnesses must not replace executable parameters")
			if tc.name == "explicit value cast" {
				require.True(t, explicitResultCast, "safe key lowering must still execute the user's narrowing cast")
			}
		})
	}
}

func TestPreparedDomainlessNullUsesConcreteRelationalColumns(t *testing.T) {
	for _, query := range []string{
		"select ? group by 1",
		"select ? union all select ?",
	} {
		for _, binary := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/binary=%v", query, binary), func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				bindings := []PreparedSourceBinding{
					{Position: 0, Type: types.T_any.ToType()},
					{Position: 1, Type: types.T_any.ToType()},
				}
				values := []any{
					ParamValue{IsBinaryProtocol: binary},
					ParamValue{IsBinaryProtocol: binary},
				}
				mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), bindings, values))
				p, err := runOneStmt(mock, t, query)
				require.NoError(t, err)
				for _, node := range p.GetQuery().Nodes {
					for _, expr := range append(append([]*Expr(nil), node.ProjectList...), node.GroupBy...) {
						require.NotEqual(t, int32(types.T_any), expr.Typ.Id, "materialized column or group key")
					}
				}
			})
		}
	}

	ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}}, []any{nil})
	typedNull, err := bindPreparedSource(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, typedNull.GetP(), "typed NULL retains its source domain")
	require.Equal(t, int32(types.T_varchar), typedNull.Typ.Id)
}

func TestPreparedSQLPresentationKeepsDerivedNumericConsumer(t *testing.T) {
	const query = "select x, greatest(x,y) from (select ? as x, ? as y limit 1) d"
	source := types.New(types.T_decimal128, 20, 0)
	bindings := []PreparedSourceBinding{{Position: 0, Type: source}, {Position: 1, Type: source}}
	for _, binary := range []bool{false, true} {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		ctx := withPreparedSourceBindings(context.Background(), bindings, []any{
			ParamValue{Value: "2", IsBinaryProtocol: binary},
			ParamValue{Value: "10", IsBinaryProtocol: binary},
		})
		mock.ctxt.SetContext(ctx)
		p, err := runOneStmt(mock, t, query)
		require.NoError(t, err)
		root := p.GetQuery().Nodes[p.GetQuery().Steps[0]]
		require.Equal(t, int32(types.T_decimal128), root.ProjectList[0].Typ.Id)
		require.Equal(t, int32(types.T_decimal128), root.ProjectList[1].Typ.Id)
		presentPreparedSQLResults(ctx, p.GetQuery())
		if binary {
			require.Equal(t, int32(types.T_decimal128), root.ProjectList[0].Typ.Id)
		} else {
			require.Equal(t, int32(types.T_text), root.ProjectList[0].Typ.Id)
		}
		require.Equal(t, int32(types.T_decimal128), root.ProjectList[1].Typ.Id)
	}
}

func TestPreparedBindingBeforeKeyLowering(t *testing.T) {
	for _, tc := range []struct {
		name, predicate string
		column, source  types.T
		fullKey         bool
	}{
		{"integer", "b=?", types.T_int32, types.T_int32, true},
		{"widening", "b=?", types.T_int64, types.T_int32, true},
		{"narrowing", "b=?", types.T_int32, types.T_int64, false},
		{"fraction", "b=?", types.T_int32, types.T_float64, false},
		{"fraction range", "b>?", types.T_int32, types.T_float64, false},
		{"fraction in", "b in (?)", types.T_int32, types.T_float64, false},
		{"text fraction", "b=?", types.T_int32, types.T_text, false},
		{"text fraction in", "b in (?)", types.T_int32, types.T_text, false},
		{"string number", "b=?", types.T_varchar, types.T_int64, false},
		{"string", "b=?", types.T_varchar, types.T_varchar, true},
		{"explicit cast", "b=cast(? as signed)", types.T_int64, types.T_float64, true},
		{"mixed scalar", "b=? and abs(?)=2", types.T_int32, types.T_int32, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			ctx := &mock.ctxt
			table := makeExprOptCompositeSortKeyTableDef()
			table.Name, table.TblId = "binding_keys", 99001
			table.Cols[0].Typ = makeSimplePlan2Type(types.T_int32)
			table.Cols[1].Typ = makeSimplePlan2Type(tc.column)
			ctx.tables[table.Name] = table
			ctx.objects[table.Name] = &ObjectRef{ObjName: table.Name, Obj: int64(table.TblId)}
			ctx.SetContext(withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
				{Position: 0, Type: types.T_int32.ToType()},
				{Position: 1, Type: tc.source.ToType()},
				{Position: 2, Type: types.T_int32.ToType()},
			}))
			p, err := runOneStmt(mock, t, "select c from binding_keys where a=? and "+tc.predicate)
			require.NoError(t, err)
			fullKey := false
			positions := make(map[int32]bool)
			require.NoError(t, planpb.VisitExpressionsInOwner(p, func(root *Expr) error {
				return planpb.VisitExprTree(root, func(e *Expr) error {
					if param := e.GetP(); param != nil {
						want := types.T_int32
						if param.Pos == 1 {
							want = tc.source
						}
						require.Equal(t, int32(want), e.Typ.Id)
						positions[param.Pos] = true
					}
					fn := e.GetF()
					if fn == nil || fn.Func.GetObjName() != "=" || len(fn.Args) != 2 {
						return nil
					}
					if col := fn.Args[0].GetCol(); col != nil && (col.Name == "binding_keys.__mo_cpkey" || col.Name == "__mo_cpkey" || col.Name == catalog.CPrimaryKeyColName) {
						fullKey = true
						require.True(t, types.T(fn.Args[1].Typ.Id).IsMySQLString())
					}
					return nil
				})
			}))
			require.Equal(t, tc.fullKey, fullKey, p.String())
			require.True(t, positions[0])
			require.True(t, positions[1])
		})
	}
}

func TestPreparedSourceBindingKeepsCurrentValuesAndOrdinals(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
		// Deliberately use a non-identity mapping. Optimizer pruning must not
		// renumber the source parameters or select the neighboring value.
		{Position: 2, Type: types.T_int64.ToType()},
		{Position: 0, Type: types.T_int64.ToType()},
		{Position: 1, Type: types.T_float64.ToType()},
	}))
	p, err := runOneStmt(mock, t, "select ?=cast('02' as char), ?>2147483647, abs(?)")
	require.NoError(t, err)
	query := p.GetQuery()
	project := query.Nodes[query.Steps[0]].ProjectList
	require.Len(t, project, 3)
	before := p.String()
	// Physical lowering must retain exactly the SQL behavior while ensuring
	// an older remote CN only receives TEXT-backed parameter expressions.
	physical := DeepCopyPlan(p)
	require.NoError(t, lowerPreparedSourceTransports(context.Background(), physical))
	require.NoError(t, planpb.VisitExpressionsInOwner(physical, func(root *Expr) error {
		return planpb.VisitExprTree(root, func(expr *Expr) error {
			if expr.GetP() != nil {
				require.Equal(t, int32(types.T_text), expr.Typ.Id)
			}
			return nil
		})
	}))
	project = append(project, physical.GetQuery().Nodes[physical.GetQuery().Steps[0]].ProjectList...)
	for _, tc := range []struct {
		values        []string
		first, second bool
		third         float64
	}{
		{[]string{"2", "-2.5", "2"}, true, false, 2.5},
		{[]string{"2147483648", "-3.25", "3"}, false, true, 3.25},
		{[]string{"-2147483649", "2", "2"}, true, false, 2},
	} {
		func() {
			proc := testutil.NewProcess(t)
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			for _, value := range tc.values {
				require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
			}
			proc.SetPrepareParams(params)
			for i, expression := range project {
				i %= 3
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, expression, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				if i < 2 {
					want := tc.first
					if i == 1 {
						want = tc.second
					}
					require.Equal(t, want, vector.GetFixedAtNoTypeCheck[bool](result, 0))
				} else {
					require.Equal(t, tc.third, vector.GetFixedAtNoTypeCheck[float64](result, 0))
				}
			}
		}()
	}
	require.Equal(t, before, p.String())
}

func TestPreparedTextSourceRemainsText(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
		{Position: 0, Type: types.T_text.ToType()},
		{Position: 1, Type: types.T_text.ToType()},
	}))
	p, err := runOneStmt(mock, t, "select ?, ?=cast(2 as int)")
	require.NoError(t, err)
	physical := DeepCopyPlan(p)
	require.NoError(t, lowerPreparedSourceTransports(context.Background(), physical))
	proc := testutil.NewProcess(t)
	params := vector.NewVec(types.T_text.ToType())
	defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
	require.NoError(t, vector.AppendBytes(params, []byte("你好abc"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("2.5"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	for _, plan := range []*Plan{p, physical} {
		project := plan.GetQuery().Nodes[plan.GetQuery().Steps[0]].ProjectList
		require.Equal(t, int32(types.T_text), project[0].Typ.Id)
		require.NotNil(t, project[0].GetP())
		for i, expr := range project {
			func() {
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				if i == 0 {
					require.Equal(t, "你好abc", result.GetStringAt(0))
				} else {
					require.False(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
				}
			}()
		}
	}
}

func TestPreparedSourceLayoutSurvivesPruning(t *testing.T) {
	for _, sql := range []string{
		"select b from (select ? a, ? b) d",
		"with d as (select ? a, ? b) select b from d",
	} {
		prepared := buildPreparedAggregatePlan(t, sql)
		require.Len(t, prepared.ParamTypes, 2, sql)
		require.Equal(t, []int32{1}, preparedParamPositions(prepared), sql)
	}
}

func TestPreparedSourceTransportDiagnosticOwnership(t *testing.T) {
	// SQL typing does not prove that a TEXT transport value can be decoded.
	// For example, SEND_LONG_DATA can bypass the integer packet decoder.
	for _, tc := range []struct {
		typ        types.T
		valid, bad string
		badSafe    bool
	}{
		{types.T_int64, "12", "12.5", false},
		{types.T_time, "00:00:01", "900:00:00", false},
		// Expression DATE casts return NULL without a warning for lexical
		// errors; conservative classification must still accept that proof.
		{types.T_date, "2024-01-02", "invalid-date", true},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			expr := &Expr{Typ: makeSimplePlan2Type(tc.typ), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			require.True(t, function.MayDiagnoseStatementParameter(expr))
			require.True(t, ContainsStatementInvariantFilterDiagnostic(proc, expr))
			physical, err := makePlan2CastExpr(proc.Ctx,
				&Expr{Typ: makeSimplePlan2Type(types.T_text), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}, expr.Typ)
			require.NoError(t, err)
			require.True(t, function.MayDiagnoseStatementParameter(physical))
			for _, binding := range []struct {
				value      string
				null, safe bool
			}{{tc.valid, false, true}, {tc.bad, false, tc.badSafe}, {"", true, true}, {tc.valid, false, true}} {
				func() {
					params := vector.NewVec(types.T_text.ToType())
					defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
					require.NoError(t, vector.AppendBytes(params, []byte(binding.value), binding.null, proc.Mp()))
					proc.SetPrepareParams(params)
					for _, candidate := range []*Expr{expr, physical} {
						safe, err := ProbeStatementParameterDiagnosticFree(proc, candidate)
						require.NoError(t, err)
						require.Equal(t, binding.safe, safe)
					}
				}()
			}
		})
	}
}

func TestPreparedSourceBindingPreservesComparisonContracts(t *testing.T) {
	for _, tc := range []struct {
		column, source types.T
		comparisonType types.T
		adapter        string
	}{
		{types.T_time, types.T_varchar, types.T_time, "cast"},
		{types.T_json, types.T_int64, types.T_json, function.JsonComparisonParamFunctionName},
		{types.T_decimal64, types.T_float64, types.T_float64, ""},
	} {
		t.Run(tc.column.String(), func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			table := makeExprOptCompositeSortKeyTableDef()
			table.Name, table.TblId = "binding_contract", 99002
			table.Cols[2].Typ = makeSimplePlan2Type(tc.column)
			mock.ctxt.tables[table.Name] = table
			mock.ctxt.objects[table.Name] = &ObjectRef{ObjName: table.Name, Obj: int64(table.TblId)}
			mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
				{Position: 0, Type: tc.source.ToType()},
			}))
			p, err := runOneStmt(mock, t, "select c from binding_contract where c=?")
			require.NoError(t, err)
			comparison := findPlanFunctionExpr(p, "=")
			require.NotNil(t, comparison)
			for _, arg := range comparison.GetF().Args {
				require.Equal(t, int32(tc.comparisonType), arg.Typ.Id)
			}
			if tc.adapter != "" {
				require.NotNil(t, findPlanFunctionExpr(p, tc.adapter))
			}
		})
	}
}

func TestPreparedSourceBindingPreservesUpdateDomains(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	// The generic mock's historical composite definition contains empty
	// component names and no physical key column. Supply catalog-valid key
	// metadata so this test exercises the real UPDATE contract.
	table := DeepCopyTableDef(mock.ctxt.tables["partsupp"], true)
	key := MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
	// Catalog definitions keep the row ID last; UPDATE excludes that slot
	// from its insert-column map.
	rowID := table.Cols[len(table.Cols)-1]
	require.Equal(t, catalog.Row_ID, rowID.Name)
	table.Cols = append(table.Cols[:len(table.Cols)-1], key, rowID)
	table.Pkey = &planpb.PrimaryKeyDef{
		PkeyColName: key.Name, CompPkeyCol: key,
		Names: []string{"ps_partkey", "ps_suppkey"}, Cols: []uint64{0, 1},
	}
	mock.ctxt.tables["partsupp"] = table
	mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
		{Position: 0, Type: types.T_float64.ToType()},
		{Position: 1, Type: types.T_int32.ToType()},
		{Position: 2, Type: types.T_int32.ToType()},
	}))
	p, err := runOneStmt(mock, t, "update partsupp set ps_supplycost=? where ps_partkey=? and ps_suppkey=?")
	require.NoError(t, err)
	writeCast := false
	require.NoError(t, planpb.VisitExpressionsInOwner(p, func(root *Expr) error {
		return planpb.VisitExprTree(root, func(e *Expr) error {
			fn := e.GetF()
			if fn != nil && fn.Func.GetObjName() == "cast" && e.Typ.Id == int32(types.T_decimal64) &&
				len(fn.Args) > 0 && fn.Args[0].GetP() != nil && fn.Args[0].GetP().Pos == 0 {
				require.Equal(t, int32(types.T_float64), fn.Args[0].Typ.Id)
				writeCast = true
			}
			return nil
		})
	}))
	require.True(t, writeCast)
	require.NotNil(t, findPlanFunctionExpr(p, "serial"))
}

func TestPreparedDMLIntegerKeyDomains(t *testing.T) {
	// Sysbench 1.1 normalizes its Lua INT bindings to BIGINT on the wire.
	// A safe value must not widen the indexed INT column. An unsafe value must
	// retain the old comparison, not raise a new narrowing-cast error.
	for _, statement := range []string{
		"update nation set n_comment='updated' where ",
		"delete from nation where ",
	} {
		for _, tc := range []struct {
			name, predicate, value string
			source, column         types.T
			native                 bool
		}{
			{"point", "n_nationkey=?", "54321", types.T_int64, types.T_int32, true},
			{"maximum", "n_nationkey=?", "2147483647", types.T_int64, types.T_int32, true},
			{"minimum", "n_nationkey=?", "-2147483648", types.T_int64, types.T_int32, true},
			{"above maximum", "n_nationkey=?", "2147483648", types.T_int64, types.T_int32, false},
			{"below minimum", "n_nationkey=?", "-2147483649", types.T_int64, types.T_int32, false},
			{"unsigned source", "n_nationkey=?", "54321", types.T_uint64, types.T_int32, false},
			{"unsigned source overflow", "n_nationkey=?", "18446744073709551615", types.T_uint64, types.T_int32, false},
			{"unsigned column", "n_nationkey=?", "4294967295", types.T_int64, types.T_uint32, false},
			{"negative unsigned", "n_nationkey=?", "-1", types.T_int64, types.T_uint32, false},
			{"double collision", "n_nationkey=?", "9007199254740992", types.T_uint64, types.T_int64, false},
			{"strict range", "n_nationkey>?", "7", types.T_int64, types.T_int32, true},
			{"reversed range", "?<=n_nationkey", "7", types.T_int64, types.T_int32, true},
			{"explicit column cast", "cast(n_nationkey as signed)=?", "7", types.T_int64, types.T_int32, false},
		} {
			// The domain matrix belongs to the common binder. DELETE only needs
			// its distinct statement construction checked on each admission path.
			if strings.HasPrefix(statement, "delete") && tc.name != "point" && tc.name != "above maximum" {
				continue
			}
			t.Run(statement+tc.name, func(t *testing.T) {
				mock := NewMockOptimizer(false, newPlanTestProcess(t))
				table := DeepCopyTableDef(mock.ctxt.tables["nation"], true)
				table.Cols[0].Typ = makeSimplePlan2Type(tc.column)
				mock.ctxt.tables["nation"] = table
				proc := mock.ctxt.GetProcess()
				params := vector.NewVec(types.T_text.ToType())
				t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
				require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, proc.Mp()))
				proc.SetPrepareParams(params)
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, statement+tc.predicate, 1)
				require.NoError(t, err)
				defer stmt.Free()
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
					[]PreparedSourceBinding{{Position: 0, Type: tc.source.ToType()}},
					[]any{ParamValue{Value: tc.value, IsBinaryProtocol: true}})
				require.NoError(t, err)
				found, columnCast := false, false
				for _, node := range bound.Plan.GetQuery().Nodes {
					if node.NodeType != planpb.Node_TABLE_SCAN || node.TableDef.Name != "nation" {
						continue
					}
					found = true
					for _, filter := range node.FilterList {
						require.True(t, function.ContainsParameter(filter), "proof may not freeze this execution's value")
						require.NoError(t, planpb.VisitExprTree(filter, func(expr *Expr) error {
							if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 && fn.Args[0].GetCol() != nil {
								columnCast = true
							}
							return nil
						}))
					}
				}
				require.True(t, found)
				require.Equal(t, !tc.native, columnCast, bound.Plan.String())
				if tc.native {
					require.False(t, bound.ValueDependent, "guarded key conversions can reuse a plan")
					require.True(t, bound.DiagnosticFree)
					require.NotEmpty(t, bound.DiagnosticCandidates)
				} else if tc.source == types.T_int64 && tc.column == types.T_int32 && tc.name != "explicit column cast" {
					require.True(t, bound.ValueDependent, "unsafe fallback must not replace a guarded cache entry")
				}
			})
		}
	}
}

func TestPreparedSignedKeyGuardPreservesOtherDependencies(t *testing.T) {
	for _, selectStatement := range []bool{false, true} {
		for _, dependent := range []bool{false, true} {
			ctx := withPreparedSourceBindings(context.Background(),
				[]PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}},
				[]any{ParamValue{Value: "7", IsBinaryProtocol: true, PrepareParamKind: vector.PrepareParamInteger}})
			state := preparedBindingState(ctx)
			state.valueDependent = dependent
			state.selectStatement = selectStatement
			param := &Expr{Typ: makeSimplePlan2Type(types.T_int64), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			// One parameter can be consumed in more than one narrow domain. Each
			// complete cast must survive independently of optimized predicates.
			for _, target := range []types.T{types.T_int32, types.T_int16, types.T_int8} {
				column := &Expr{Typ: makeSimplePlan2Type(target), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
				var converted *Expr
				if selectStatement {
					var admitted bool
					var err error
					converted, admitted, err = bindPreparedIntegerValue(ctx, column, param)
					require.NoError(t, err)
					require.True(t, admitted)
				} else {
					args, err := bindPreparedConsumerArguments(ctx, "=", []*Expr{column, param})
					require.NoError(t, err)
					converted = args[1]
				}
				require.Equal(t, int32(target), converted.Typ.Id)
				require.NotNil(t, converted.GetP(), "admitted consumer uses one transport conversion")
				require.Equal(t, int32(types.T_int64), param.Typ.Id, "source/sibling domain stays unchanged")
				require.Equal(t, types.T_int64, state.bindings[0].Type.Oid)
				require.NotSame(t, param, converted, "sibling source occurrence remains immutable")
				guard := state.diagnosticCandidates[len(state.diagnosticCandidates)-1]
				require.Equal(t, int32(types.T_int64), guard.GetF().Args[0].Typ.Id)
				require.Equal(t, dependent, state.valueDependent)
				require.NotSame(t, converted, state.diagnosticCandidates[len(state.diagnosticCandidates)-1])
			}
			require.Len(t, state.diagnosticCandidates, 3)
			// Revisiting a consumer-local marker must retain the full original
			// source proof as well as another consumer's narrower domain.
			firstGuard := state.diagnosticCandidates[0]
			native := DeepCopyExpr(param)
			native.Typ = makeSimplePlan2Type(types.T_int32)
			column := &Expr{Typ: native.Typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
			_, _, err := bindPreparedIntegerValue(ctx, column, native)
			require.NoError(t, err)
			require.Same(t, firstGuard, state.diagnosticCandidates[0])
			require.Equal(t, int32(types.T_int64), firstGuard.GetF().Args[0].Typ.Id)
			proc := testutil.NewProcess(t)
			params := vector.NewVec(types.T_text.ToType())
			t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()); proc.Free() })
			require.NoError(t, vector.AppendBytes(params, []byte("7"), false, proc.Mp()))
			proc.SetPrepareParams(params)
			for _, tc := range []struct {
				value string
				safe  bool
			}{{"7", true}, {"128", false}, {"32768", false}, {"2147483648", false}, {"-128", true}, {"-129", false}, {"7", true}} {
				require.NoError(t, vector.SetStringAt(params, 0, tc.value, proc.Mp()))
				safe, err := ProbePreparedDiagnosticCandidates(proc, state.diagnosticCandidates)
				require.NoError(t, err)
				require.Equal(t, tc.safe, safe)
			}
		}
	}
}

func TestPreparedIntegerConsumerLoweringProvenance(t *testing.T) {
	for _, value := range []ParamValue{
		{Value: "7", PrepareParamKind: vector.PrepareParamInteger},
		{Value: "7", IsBinaryProtocol: true},
		{Value: "7", IsBinaryProtocol: true, IsBin: true, PrepareParamKind: vector.PrepareParamInteger},
		{Value: "7", IsBinaryProtocol: true, IsBinaryString: true, PrepareParamKind: vector.PrepareParamInteger},
	} {
		ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: types.T_int64.ToType()}}, []any{value})
		param := &Expr{Typ: makeSimplePlan2Type(types.T_int64), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		column := &Expr{Typ: makeSimplePlan2Type(types.T_int32), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
		converted, admitted, err := bindPreparedIntegerValue(ctx, column, param)
		require.NoError(t, err)
		require.True(t, admitted)
		require.NotNil(t, converted.GetF(), "SQL/opaque/unclassified transport retains the source conversion")
		require.Equal(t, int32(types.T_int64), converted.GetF().Args[0].Typ.Id)
	}
}

// Failure on either side must not publish half a narrow range or discard a
// diagnostic/dependency owned by a different consumer in the same statement.
func TestPreparedIntegerBetweenAtomicAdmission(t *testing.T) {
	for _, bounds := range [][]any{{int64(7), int64(2147483648)}, {int64(-2147483649), int64(7)}, {int64(7), int64(8)}} {
		for _, dependent := range []bool{false, true} {
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
				{Position: 0, Type: types.T_int64.ToType()}, {Position: 1, Type: types.T_int64.ToType()},
			}, bounds)
			state := preparedBindingState(ctx)
			state.selectStatement, state.valueDependent = true, dependent
			prior := &Expr{Typ: makeSimplePlan2Type(types.T_int64), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			state.diagnosticCandidates = []*Expr{prior}
			lower, err := bindPreparedSource(ctx, 1)
			require.NoError(t, err)
			upper, err := bindPreparedSource(ctx, 2)
			require.NoError(t, err)
			column := &Expr{Typ: makeSimplePlan2Type(types.T_int32), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
			args := []*Expr{column, lower, upper}
			got, admitted, err := bindPreparedIntegerBetween(ctx, args)
			require.NoError(t, err)
			require.Same(t, prior, state.diagnosticCandidates[0])
			if bounds[1] == int64(8) {
				require.True(t, admitted)
				require.Len(t, state.diagnosticCandidates, 3)
				require.Equal(t, dependent, state.valueDependent)
				require.Equal(t, int32(types.T_int32), got[1].Typ.Id)
				require.Equal(t, int32(types.T_int32), got[2].Typ.Id)
			} else {
				require.False(t, admitted)
				require.Len(t, state.diagnosticCandidates, 1)
				require.True(t, state.valueDependent)
				for i := range args {
					require.Same(t, args[i], got[i])
				}
			}
		}
	}
}

func TestPreparedIntegerAdmissionRejectsUnsupportedDomains(t *testing.T) {
	for _, tc := range []struct {
		source, binding, target types.T
		position                int32
	}{
		{types.T_text, types.T_text, types.T_int32, 0},
		{types.T_float64, types.T_float64, types.T_int32, 0},
		{types.T_uint64, types.T_uint64, types.T_int32, 0},
		{types.T_any, types.T_any, types.T_int32, 0},
		{types.T_int64, types.T_int64, types.T_int64, 0},
		{types.T_int64, types.T_int64, types.T_uint32, 0},
		{types.T_int64, types.T_int64, types.T_float64, 0},
		// A signed expression cannot authorize a different source binding.
		{types.T_int64, types.T_uint64, types.T_int32, 0},
		{types.T_int64, types.T_int32, types.T_int32, 0},
		{types.T_int64, types.T_int64, types.T_int32, 1},
	} {
		t.Run(fmt.Sprintf("%s/%s/%s/pos%d", tc.source, tc.binding, tc.target, tc.position), func(t *testing.T) {
			ctx := withPreparedSourceBindings(context.Background(),
				[]PreparedSourceBinding{{Position: 0, Type: tc.binding.ToType()}}, []any{int64(7)})
			state := preparedBindingState(ctx)
			state.selectStatement, state.valueDependent = true, true
			param, err := bindPreparedSource(ctx, 1)
			require.NoError(t, err)
			if tc.source != tc.binding {
				param.Typ = makeSimplePlan2Type(tc.source)
			}
			param.GetP().Pos = tc.position
			column := &Expr{Typ: makeSimplePlan2Type(tc.target), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}}
			result, admitted, err := bindPreparedIntegerValue(ctx, column, param)
			require.NoError(t, err)
			require.False(t, admitted)
			require.Same(t, param, result)
			require.True(t, state.valueDependent)
			require.Empty(t, state.diagnosticCandidates)
		})
	}
}

func TestPreparedSourceBindingConfigurationConsumers(t *testing.T) {
	t.Run("geometry SRID", func(t *testing.T) {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		for _, tc := range []struct {
			source any
			srid   int64
			width  int32
			fails  bool
		}{
			{"wkb", 4326, 4327, false},
			{"wkb", -1, 0, true},
			{nil, -1, 0, false},
			{"wkb", 3857, 3858, false},
		} {
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
				{Position: 0, Type: types.T_blob.ToType()},
				{Position: 1, Type: types.T_int64.ToType()},
				{Position: 2, Type: types.T_int64.ToType()},
			}, []any{tc.source, tc.srid, int64(9)})
			mock.ctxt.SetContext(ctx)
			p, err := runOneStmt(mock, t, "select st_geomfromwkb(?, ?), ?")
			if tc.fails {
				require.Error(t, err)
				continue
			}
			require.NoError(t, err)
			query := p.GetQuery()
			project := query.Nodes[query.Steps[0]].ProjectList
			require.Equal(t, tc.width, project[0].Typ.Width)
			require.Equal(t, int32(2), project[1].GetP().Pos)
			require.True(t, preparedBindingState(ctx).valueDependent)
		}
	})
	t.Run("series schema", func(t *testing.T) {
		for _, tc := range []struct {
			typ        types.Type
			values     []any
			resultType types.T
			scale      int32
			dependent  bool
		}{
			{types.T_int32.ToType(), []any{int32(1), int32(3), int32(1)}, types.T_int64, 0, false},
			{types.T_varchar.ToType(), []any{"2024-01-01 00:00:00.123", "2024-01-02", "1 second"}, types.T_varchar, 3, true},
			{types.T_datetime.ToTypeWithScale(3), []any{"2024-01-01", "2024-01-02", "1 microsecond"}, types.T_datetime, 6, true},
		} {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			stepType := types.T_varchar.ToType()
			if tc.typ.Oid.IsInteger() {
				stepType = types.T_int32.ToType()
			}
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
				{Position: 0, Type: tc.typ}, {Position: 1, Type: tc.typ}, {Position: 2, Type: stepType},
			}, tc.values)
			mock.ctxt.SetContext(ctx)
			p, err := runOneStmt(mock, t, "select result from generate_series(?,?,?) g")
			require.NoError(t, err)
			found := false
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType != planpb.Node_FUNCTION_SCAN {
					continue
				}
				found = true
				require.Equal(t, int32(tc.resultType), node.TableDef.Cols[0].Typ.Id)
				require.Equal(t, tc.scale, node.TblFuncExprList[0].Typ.Scale)
				require.True(t, function.ContainsParameter(node.TblFuncExprList[0]))
			}
			require.True(t, found)
			require.Equal(t, tc.dependent, preparedBindingState(ctx).valueDependent)
		}
	})
}

func TestPreparedSourceBindingDiagnosticProofIsLocal(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{
		{Position: 0, Type: types.T_varchar.ToType()}, {Position: 1, Type: types.T_int32.ToType()},
	}, []any{"00:00:01", int32(1)})
	mock.ctxt.SetContext(ctx)
	proc := mock.ctxt.GetProcess()
	params := vector.NewVec(types.T_text.ToType())
	defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
	require.NoError(t, vector.AppendBytes(params, []byte("00:00:01"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(params, []byte("1"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	for _, tc := range []struct {
		value string
		safe  bool
	}{{"00:00:01", true}, {"900:00:00", false}, {"00:00:02", true}} {
		require.NoError(t, vector.SetStringAt(params, 0, tc.value, proc.Mp()))
		p, err := runOneStmt(mock, t, "select count(*) from select_test.bind_select a join select_test.bind_select b on a.a=b.a and a.a=hour(time(?)) where a.a=?")
		require.NoError(t, err)
		filters := 0
		for _, node := range p.GetQuery().Nodes {
			if node.NodeType == planpb.Node_TABLE_SCAN {
				filters += len(node.FilterList)
			}
		}
		require.Equal(t, tc.safe, filters > 0, "each builder proves its own binding")
		state := preparedBindingState(ctx)
		require.NotEmpty(t, state.diagnosticCandidates)
		safe, err := ProbePreparedDiagnosticCandidates(proc, state.diagnosticCandidates)
		require.NoError(t, err)
		require.Equal(t, tc.safe, safe, "captured expressions retain current ParamRefs")
	}
}

func TestPreparedSourceBindingStringConsumers(t *testing.T) {
	for _, tc := range []struct {
		name, sql, value string
		typ              types.T
		null, fails      bool
		want             string
	}{
		{"quote number", "select json_quote(?)", "12", types.T_int64, false, false, `"12"`},
		{"quote string", "select json_quote(?)", "abc", types.T_varchar, false, false, `"abc"`},
		{"quote binary", "select json_quote(?)", "abc", types.T_varbinary, false, true, ""},
		{"concat json number", "select concat(?, 'x')", "1.6", types.T_json, false, false, "1.6x"},
		{"concat json string", "select concat(?, 'x')", `"abc"`, types.T_json, false, false, `"abc"x`},
		{"concat json null", "select concat(?, 'x')", "null", types.T_json, false, false, "nullx"},
		{"concat sql null", "select concat(?, 'x')", "", types.T_json, true, false, ""},
		{"concat ws json", "select concat_ws('-', ?, 'x')", "1.6", types.T_json, false, false, "1.6-x"},
		{"quote null", "select json_quote(?)", "", types.T_any, true, false, ""},
		{"regexp text", "select regexp_substr(?, 'a')", "abc", types.T_varchar, false, false, "a"},
		{"regexp binary marker with text pattern", "select regexp_substr(?, 'a')", "abc", types.T_varbinary, false, false, "a"},
		{"regexp null", "select regexp_substr(?, 'a')", "", types.T_any, true, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: tc.typ.ToType()}})
			mock.ctxt.SetContext(ctx)
			p, err := runOneStmt(mock, t, tc.sql)
			if tc.fails && err != nil {
				return
			}
			require.NoError(t, err)
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, proc.Mp()))
			proc.SetPrepareParams(params)
			if tc.typ == types.T_varbinary {
				proc.SetPrepareParamsWithMeta(params, nil, nil, []bool{true})
			}
			q := p.GetQuery()
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, q.Nodes[q.Steps[0]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
			if free != nil {
				defer free()
			}
			if tc.fails {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.null, result.IsNull(0))
			if !tc.null {
				require.Equal(t, tc.want, result.GetStringAt(0))
			}
		})
	}
}

func TestPreparedSourceBindingJSONComparisonExecution(t *testing.T) {
	for _, tc := range []struct {
		name, json, value string
		typ               types.Type
		kind              vector.PrepareParamKind
	}{
		{"integer", "7", "7", types.T_int64.ToType(), vector.PrepareParamInteger},
		{"float", "1.25", "1.25", types.T_float64.ToType(), vector.PrepareParamFloat},
		{"boolean", "true", "true", types.T_bool.ToType(), vector.PrepareParamBoolean},
		{"decimal", "1.25", "1.25", types.New(types.T_decimal64, 3, 2), vector.PrepareParamDecimal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.SetContext(withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: tc.typ}}))
			p, err := runOneStmt(mock, t, "select cast('"+tc.json+"' as json)=?")
			require.NoError(t, err)
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, proc.Mp()))
			proc.SetPrepareParamsWithMeta(params, nil, []vector.PrepareParamKind{tc.kind})
			for _, physical := range []bool{false, true} {
				if physical {
					require.NoError(t, lowerPreparedSourceTransports(proc.Ctx, p))
				}
				q := p.GetQuery()
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, q.Nodes[q.Steps[0]].ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
				if free != nil {
					defer free()
				}
				require.NoError(t, err)
				require.False(t, result.IsNull(0))
				require.True(t, vector.GetFixedAtNoTypeCheck[bool](result, 0))
			}
		})
	}
}

func TestPreparedExecutionPlanConsumerDomains(t *testing.T) {
	for _, tc := range []struct {
		name, sql, value, want   string
		source, numeric, latch   types.Type
		null, dependent, decimal bool
	}{
		{name: "prefix absent", sql: "coalesce(?, cast('7' as decimal(6,2)))", value: "abc", want: "abc", source: types.T_varchar.ToType(), dependent: true},
		{name: "prefix zero", sql: "coalesce(?, cast('7' as decimal(6,2)))", value: "0xx", want: "0.00", source: types.T_varchar.ToType(), dependent: true, decimal: true},
		{name: "prefix decimal", sql: "greatest(?, cast('7' as decimal(6,2)))", value: "12.25xyz", want: "12.25", source: types.T_varchar.ToType(), dependent: true, decimal: true},
		{name: "prefix underflow", sql: "least(?, cast('7' as decimal(6,2)))", value: "1e-999999999999999999999999", want: "0." + strings.Repeat("0", 72), source: types.T_varchar.ToType(), dependent: true, decimal: true},
		{name: "json arithmetic", sql: "? + 0", value: "1.6", want: "1.6", source: types.T_json.ToType()},
		{name: "json arithmetic reversed", sql: "0 + ?", value: "1.6", want: "1.6", source: types.T_json.ToType()},
		{name: "json pair arithmetic", sql: "? + cast('1.6' as json)", value: "1.6", want: "3.2", source: types.T_json.ToType()},
		{name: "json division", sql: "? / 2", value: "1.6", want: "0.8", source: types.T_json.ToType()},
		{name: "json division reversed", sql: "2 / ?", value: "1.6", want: "1.25", source: types.T_json.ToType()},
		{name: "json integer division", sql: "? div 1", value: "1.6", want: "1", source: types.T_json.ToType()},
		{name: "json arithmetic SQL null", sql: "? + 0", source: types.T_json.ToType(), null: true},
		{name: "char fractional text truncates", sql: "char(?)", value: "65.5", want: "A", source: types.T_varchar.ToType(), dependent: true},
		{name: "char suffix", sql: "char(?)", value: "65.5xyz", want: "A", source: types.T_varchar.ToType(), dependent: true},
		{name: "bit count initial", sql: "bit_count(?)", value: "64", want: "7", source: types.T_varchar.ToType()},
		{name: "bit count numeric", sql: "bit_count(?)", value: "64", want: "1", source: types.T_int32.ToType(), latch: types.T_int64.ToType()},
		{name: "bit count latched", sql: "bit_count(?)", value: "65", want: "2", source: types.T_varchar.ToType(), latch: types.T_int64.ToType()},
		{name: "bit count null", sql: "bit_count(?)", null: true, source: types.T_any.ToType(), latch: types.T_int64.ToType()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, proc.Mp()))
			proc.SetPrepareParams(params)
			value := ParamValue{Value: tc.value, SourceType: tc.source, HasSourceType: true, EnableNumericPrefix: true}
			if tc.null {
				value.Value = nil
			}
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select "+tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			previous := mock.ctxt.GetContext()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
				[]PreparedSourceBinding{{Position: 0, Type: tc.source, NumericType: tc.numeric, BitCountType: tc.latch}}, []any{value})
			require.NoError(t, err)
			require.Equal(t, previous, mock.ctxt.GetContext())
			require.Equal(t, tc.dependent, bound.ValueDependent, "%s", bound.Plan.String())
			q := bound.Plan.GetQuery()
			expr := q.Nodes[q.Steps[0]].ProjectList[0]
			if tc.decimal {
				require.True(t, types.T(expr.Typ.Id).IsDecimal())
			}
			require.True(t, function.ContainsParameter(expr), "no value witness may replace an executable parameter")
			textType := types.T_varchar.ToType()
			display, err := makePlan2CastExpr(proc.Ctx, expr, makePlan2Type(&textType))
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, display, []*batch.Batch{batch.EmptyForConstFoldBatch})
			if free != nil {
				defer free()
			}
			require.NoError(t, err)
			require.Equal(t, tc.null, result.IsNull(0))
			if !tc.null {
				require.Equal(t, tc.want, result.GetStringAt(0))
			}
		})
	}
}

func TestPreparedBinarySourceKeepsRuntimeTextWidth(t *testing.T) {
	for _, sql := range []string{"select left(?,1)", "select left(v,1) from (select ? v) s"} {
		t.Run(sql, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			sourceType := types.NewWithCharset(types.T_varbinary, 512, 0, types.CharsetBinary)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
				[]PreparedSourceBinding{{Position: 0, Type: sourceType}},
				[]any{ParamValue{Value: "你好", SourceType: sourceType, HasSourceType: true, RuntimeStringDomain: types.RuntimeStringText}})
			require.NoError(t, err)
			expr := findPlanFunctionExpr(bound.Plan, "left")
			require.NotNil(t, expr)
			require.Equal(t, int32(512), expr.Typ.Width)
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte("你好"), false, proc.Mp()))
			require.NoError(t, params.SetRuntimeStringDomainWithMP(types.RuntimeStringText, proc.Mp()))
			proc.SetPrepareParams(params)
			for _, value := range []string{"你好", "世界", "你好"} {
				func() {
					require.NoError(t, vector.SetStringAt(params, 0, value, proc.Mp()))
					result, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{batch.EmptyForConstFoldBatch})
					if free != nil {
						defer free()
					}
					require.NoError(t, err)
					require.Equal(t, string([]rune(value)[:1]), result.GetStringAt(0))
				}()
			}
		})
	}
}

func TestPreparedRegexpRuntimeTextOverride(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	prepared, err := runOneStmt(mock, t, "prepare domain_override from 'select regexp_substr(left(?, 1), ''a'')'")
	require.NoError(t, err)
	_, _, err = FillValuesOfParamsInPlanWithSpecialization(context.Background(), prepared.GetDcl().GetPrepare().Plan, []any{ParamValue{
		Value: "abc", SourceType: types.T_varbinary.ToType(), HasSourceType: true,
		RuntimeStringDomain: types.RuntimeStringText,
	}})
	require.NoError(t, err, "runtime TEXT overrides static VARBINARY in the parameter vector")
}

func TestPreparedExecutionRegexpDomainModes(t *testing.T) {
	for _, tc := range []struct {
		name, sql   string
		source      types.T
		domain      types.RuntimeStringDomain
		null, fails bool
	}{
		{name: "binary marker with text", sql: "select regexp_substr(?, 'a')", source: types.T_varbinary},
		{name: "text marker with binary", sql: "select regexp_substr(?, cast('a' as varbinary(1)))", source: types.T_varchar, fails: true},
		{name: "nested binary with text", sql: "select regexp_substr(left(?,1), 'a')", source: types.T_varbinary, fails: true},
		{name: "null marker with binary", sql: "select regexp_substr(?, cast('a' as varbinary(1)))", source: types.T_any, null: true, fails: true},
		{name: "text override nested", sql: "select regexp_substr(left(?,1), 'a')", source: types.T_varbinary, domain: types.RuntimeStringText},
		{name: "text override derived", sql: "select regexp_substr(v, cast('a' as varbinary(1))) from (select ? v) s", source: types.T_varbinary, domain: types.RuntimeStringText, fails: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			source := tc.source.ToType()
			value := ParamValue{Value: "abc", SourceType: source, HasSourceType: true, RuntimeStringDomain: tc.domain}
			if tc.null {
				value.Value = nil
			}
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{{Position: 0, Type: source}}, []any{value})
			if tc.fails {
				require.ErrorContains(t, err, "Character set")
				return
			}
			require.NoError(t, err)
			require.False(t, bound.ValueDependent, "compatibility depends only on source metadata")
		})
	}
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select regexp_substr(NULL,cast('a' as varbinary(1)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPreparedExecutionPlan(&mock.ctxt, stmt, nil, nil)
	require.NoError(t, err, "untyped NULL literal has no string domain")
}

func TestPreparedExplainSelectClassification(t *testing.T) {
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"select 1", true}, {"explain select 1", true}, {"explain analyze select 1", true}, {"explain phyplan select 1", true},
		{"insert into t select 1", false}, {"update t set n=1", false}, {"explain update t set n=1", false}, {"explain analyze delete from t", false},
	} {
		stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, tc.sql, 1)
		require.NoError(t, err, tc.sql)
		require.Equal(t, tc.want, preparedUnderlyingSelect(stmts[0]), tc.sql)
		for _, stmt := range stmts {
			stmt.Free()
		}
	}
	require.False(t, preparedUnderlyingSelect(nil))
}

func TestPreparedIntegerComparisonProofDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name, expression, value string
		null, dependent         bool
	}{
		{"warning", "cast(? as double)", "12tail", false, true},
		{"invalid", "cast(? as decimal(38,0))", "invalid", false, true},
		{"null", "cast(? as decimal(38,0))", "", true, true},
		{"volatile", "cast(? as decimal(38,0))+rand()", "12", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), tc.null, proc.Mp()))
			proc.SetPrepareParams(params)
			sink := &loadAssignmentWarningSink{}
			proc.WarningSink = sink
			ctx := withPreparedSourceBindings(mock.ctxt.GetContext(), []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}}, []any{tc.value})
			state := preparedBindingState(ctx)
			state.selectStatement = true
			mock.ctxt.SetContext(ctx)
			p, err := runOneStmt(mock, t, "select "+tc.expression)
			require.NoError(t, err)
			peer := p.GetQuery().Nodes[p.GetQuery().Steps[0]].ProjectList[0]
			column := &Expr{Typ: makeSimplePlan2Type(types.T_int64), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}}
			original, err := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{column, peer})
			require.NoError(t, err)
			state.valueDependent = false
			builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, false)
			rewritten, err := builder.rewritePreparedIntegerComparison(nil, original)
			require.NoError(t, err)
			require.Same(t, original, rewritten, "diagnostics, NULL and volatility retain their execution owner")
			require.Equal(t, tc.dependent, state.valueDependent)
			require.Empty(t, sink.codes, "a proof must not publish warnings")
			if tc.name == "warning" {
				input := batch.NewWithSize(1)
				input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				defer input.Clean(proc.Mp())
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(12), false, proc.Mp()))
				input.SetRowCount(1)
				vec, free, err := colexec.GetReadonlyResultFromExpression(proc, rewritten, []*batch.Batch{input})
				require.NoError(t, err)
				defer free()
				require.True(t, vector.GetFixedAtNoTypeCheck[bool](vec, 0))
				require.Len(t, sink.codes, 1, "runtime still owns the original warning")
			}
		})
	}
}

func TestPreparedSingletonJoinIntegerComparison(t *testing.T) {
	for _, tc := range []struct {
		name, peer, value, query string
		native                   bool
	}{
		{name: "round", peer: "round(x.v,0)", value: "12345.0", native: true},
		{name: "rounded fraction", peer: "round(x.v,0)", value: "12345.5", native: true},
		{name: "negative round", peer: "round(x.v,0)", value: "-12345.5", native: true},
		{name: "decimal", peer: "cast(x.v as decimal(38,0))", value: "9007199254740993", native: true},
		{name: "abs decimal", peer: "abs(cast(x.v as decimal(38,0)))", value: "-9007199254740993", native: true},
		{name: "fraction", peer: "cast(x.v as decimal(38,1))", value: "12345.5", native: false},
		{name: "overflow", peer: "cast(x.v as decimal(38,0))", value: "9223372036854775808", native: false},
		{name: "double collision", peer: "cast(x.v as double)", value: "9007199254740992", native: false},
		{name: "volatile", peer: "cast(x.v as decimal(38,0))+rand()", value: "12", native: false},
		{name: "projected cast", value: "9007199254740993", native: true, query: "select k.c from numeric_join k join (select cast(? as decimal(38,0)) as v) x on k.c=x.v"},
		{name: "nested alias", value: "9007199254740993", native: true, query: "select k.c from numeric_join k join (select v from (select cast(? as decimal(38,0)) as v) y) x on k.c=x.v"},
		{name: "nested arithmetic", value: "9007199254740993", native: true, query: "select k.c from numeric_join k join (select v+0 as v from (select cast(? as decimal(38,0)) as v) y) x on k.c=x.v"},
		{name: "nested abs", value: "-9007199254740993", native: true, query: "select k.c from numeric_join k join (select abs(v) as v from (select cast(? as decimal(38,0)) as v) y) x on k.c=x.v"},
		{name: "CTE", value: "9007199254740993", native: true, query: "with y as (select cast(? as decimal(38,0)) as v), x as (select v+0 as v from y) select k.c from numeric_join k join x on k.c=x.v"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			table := makeExprOptCompositeSortKeyTableDef()
			table.Name, table.TblId = "numeric_join", 99004
			table.Cols[2].Typ = makeSimplePlan2Type(types.T_int64)
			mock.ctxt.tables[table.Name] = table
			mock.ctxt.objects[table.Name] = &ObjectRef{ObjName: table.Name, Obj: int64(table.TblId)}
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, proc.Mp()))
			proc.SetPrepareParams(params)
			query := tc.query
			if query == "" {
				query = "select k.c from numeric_join k join (select ? as v) x on k.c=" + tc.peer
			}
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, query, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}}, []any{ParamValue{Value: tc.value, IsBinaryProtocol: true}})
			require.NoError(t, err)
			found, native, projected := false, false, false
			for _, node := range bound.Plan.GetQuery().Nodes {
				predicates := node.OnList
				if node.NodeType == planpb.Node_FILTER {
					// Volatile residuals can remain above a cross JOIN.
					predicates = node.FilterList
				} else if node.NodeType != planpb.Node_JOIN {
					continue
				}
				for _, on := range predicates {
					fn := on.GetF()
					if fn == nil || len(fn.Args) != 2 {
						continue
					}
					found = true
					for _, arg := range fn.Args {
						if col := arg.GetCol(); col != nil && col.RelPos == 0 && arg.Typ.Id == int32(types.T_int64) {
							native = true
						}
					}
					require.NoError(t, planpb.VisitExprTree(on, func(e *Expr) error {
						if col := e.GetCol(); col != nil && (col.Name == "v" || strings.HasSuffix(col.Name, ".v")) {
							projected = true
						}
						return nil
					}))
				}
			}
			require.True(t, found, bound.Plan.String())
			require.Equal(t, tc.native, native, bound.Plan.String())
			require.True(t, projected, "proof must retain executable projected column")
			if tc.native {
				require.True(t, bound.ValueDependent)
			}
		})
	}
}

func TestSingletonProjectedPeerAdmission(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reject func(*planpb.Node, *planpb.Node, *Expr)
	}{
		{"singleton", nil},
		{"multirow", func(p, v *planpb.Node, e *Expr) { v.RowsetData = &planpb.RowsetData{} }},
		{"table", func(p, v *planpb.Node, e *Expr) { v.TableDef = &TableDef{} }},
		{"project limit", func(p, v *planpb.Node, e *Expr) { p.Limit = DeepCopyExpr(e) }},
		{"project offset", func(p, v *planpb.Node, e *Expr) { p.Offset = DeepCopyExpr(e) }},
		{"project filter", func(p, v *planpb.Node, e *Expr) { p.FilterList = []*Expr{DeepCopyExpr(e)} }},
		{"input filter", func(p, v *planpb.Node, e *Expr) { v.FilterList = []*Expr{DeepCopyExpr(e)} }},
		{"wrong tag", func(p, v *planpb.Node, e *Expr) { p.BindingTags[0]++ }},
		{"wrong position", func(p, v *planpb.Node, e *Expr) { e.GetCol().ColPos = -1 }},
		{"scale mismatch", func(p, v *planpb.Node, e *Expr) { p.ProjectList[0].Typ.Scale++ }},
		{"padding mismatch", func(p, v *planpb.Node, e *Expr) { p.ProjectList[0].Typ.PadSpace = !e.Typ.PadSpace }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			builder := NewQueryBuilder(planpb.Query_SELECT, &mock.ctxt, false, false)
			typ := makeSimplePlan2Type(types.T_int64)
			project := &planpb.Node{NodeType: planpb.Node_PROJECT, BindingTags: []int32{7}, Children: []int32{1}, ProjectList: []*Expr{{Typ: typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}}}
			input := &planpb.Node{NodeType: planpb.Node_VALUE_SCAN}
			peer := &Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 7, ColPos: 0}}}
			if tc.reject != nil {
				tc.reject(project, input, peer)
			}
			builder.qry.Nodes = []*planpb.Node{project, input}
			resolved, ok := builder.singletonProjectedPeerExpression(&planpb.Node{NodeType: planpb.Node_JOIN, Children: []int32{0}}, peer)
			require.Equal(t, tc.reject == nil, ok)
			if ok {
				require.NotNil(t, resolved.GetP())
				require.NotNil(t, peer.GetCol(), "proof must not mutate execution tree")
			}
		})
	}
}

func TestPreparedLowPrecisionFloatMarkerNarrowing(t *testing.T) {
	decimalSource := types.New(types.T_decimal64, 1, 1)
	type param struct {
		value   any
		binding types.Type
	}
	sqlDecimal := param{"0.3", decimalSource}
	for _, tc := range []struct {
		name      string
		predicate string
		params    []param
		binary    bool
		narrowed  int
	}{
		{"equal sql decimal", "f = ?", []param{sqlDecimal}, false, 1},
		{"in sql decimal", "f in (?, 9)", []param{sqlDecimal}, false, 1},
		{"not in sql decimal", "f not in (?, 9)", []param{sqlDecimal}, false, 1},
		{"less equal sql decimal", "f <= ?", []param{sqlDecimal}, false, 1},
		{"between sql decimal", "f between ? and ?", []param{sqlDecimal, {"2", decimalSource}}, false, 2},
		{"equal sql integer", "f = ?", []param{{int64(257), types.T_int64.ToType()}}, false, 1},
		{"in sql decimal and integer", "f in (?, ?)", []param{sqlDecimal, {int64(6), types.T_int64.ToType()}}, false, 2},
		{"in binary double", "f in (?, 9)", []param{{"0.3", types.T_float64.ToType()}}, true, 1},
		{"in binary text", "f in (?, 9)", []param{{"0.3", types.T_varchar.ToType()}}, true, 1},
		{"equal hex float text", "f = ?", []param{{"0x1p0", types.T_varchar.ToType()}}, true, 0},
		{"equal out of range", "f = ?", []param{{"1e300", types.T_float64.ToType()}}, true, 0},
		{"in out of range", "f in (?, 9)", []param{{"1e300", types.T_float64.ToType()}}, true, 0},
		{"in one sql value out of range", "f in (?, ?)", []param{sqlDecimal, {"1e39", types.T_float64.ToType()}}, false, 1},
		{"not in one sql value out of range", "f not in (?, ?)", []param{sqlDecimal, {"1e39", types.T_float64.ToType()}}, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			bindings := make([]PreparedSourceBinding, len(tc.params))
			values := make([]any, len(tc.params))
			for i, p := range tc.params {
				require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(p.value)), false, proc.Mp()))
				bindings[i] = PreparedSourceBinding{Position: int32(i), Type: p.binding}
				value := ParamValue{Value: p.value, IsBinaryProtocol: tc.binary}
				if !tc.binary {
					value.EnableNumericPrefix = true
					value.SourceType, value.HasSourceType = p.binding, true
				}
				values[i] = value
			}
			proc.SetPrepareParams(params)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select id from vecblock_t where "+tc.predicate, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			narrowed, filters := 0, 0
			for _, node := range bound.Plan.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					filters++
					require.NoError(t, planpb.VisitExprTree(filter, func(expr *Expr) error {
						if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "cast" &&
							expr.Typ.Id == int32(types.T_bf16) && function.ContainsParameter(fn.Args[0]) {
							narrowed++
						}
						return nil
					}))
				}
			}
			require.Positive(t, filters)
			require.Equal(t, tc.narrowed, narrowed, bound.Plan.String())
			if tc.narrowed > 0 {
				require.True(t, bound.ValueDependent, "a narrowed marker must not enter the type-only cache")
			}
		})
	}
}

func TestPreparedWideIntegerComparisonKeepsColumn(t *testing.T) {
	for _, predicate := range []string{"val in (?,?)", "val not in (?,?)", "val=?", "?<=val", "val between ? and ?", "val between ? and ? or val between ? and ?"} {
		for _, value := range []int64{math.MinInt32, math.MaxInt32, math.MinInt32 - 1, math.MaxInt32 + 1} {
			t.Run(fmt.Sprintf("%s/%d", predicate, value), func(t *testing.T) {
				mock := NewMockOptimizer(true, newPlanTestProcess(t))
				proc := mock.ctxt.GetProcess()
				stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select id from single_idx_t where "+predicate, 1)
				require.NoError(t, err)
				defer stmt.Free()
				count := tree.ParameterCount(stmt)
				params := vector.NewVec(types.T_text.ToType())
				defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
				bindings := make([]PreparedSourceBinding, count)
				values := make([]any, count)
				for i := range bindings {
					bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_int64.ToType()}
					values[i] = ParamValue{Value: value, IsBinaryProtocol: true}
					require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(value)), false, proc.Mp()))
				}
				proc.SetPrepareParams(params)
				bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
				require.NoError(t, err)
				require.True(t, bound.DiagnosticFree)
				if value >= math.MinInt32 && value <= math.MaxInt32 {
					require.False(t, bound.ValueDependent)
				}
				columnCast := false
				require.NoError(t, planpb.VisitExpressionsInOwner(bound.Plan, func(root *Expr) error {
					return planpb.VisitExprTree(root, func(e *Expr) error {
						if f := e.GetF(); f != nil && f.Func.ObjName == "cast" && len(f.Args) > 0 && f.Args[0].GetCol() != nil {
							columnCast = true
						}
						return nil
					})
				}))
				require.Equal(t, value < math.MinInt32 || value > math.MaxInt32, columnCast)
				if value >= math.MinInt32 && value <= math.MaxInt32 && strings.Contains(predicate, "between") {
					require.NotNil(t, findPlanFunctionExpr(bound.Plan, "between"), "safe closed range keeps its native expression")
				}
				if strings.Contains(predicate, "in") && !strings.Contains(predicate, "between") && value >= math.MinInt32 && value <= math.MaxInt32 {
					require.NotNil(t, findPlanFunctionExpr(bound.Plan, map[bool]string{true: "not_in", false: "in"}[strings.Contains(predicate, "not")]), "safe list keeps one typed IN predicate")
				}
			})
		}
	}
}
