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
		{"round default precision", "c=round(?)", []string{"54321.0"}, true, true},
		{"truncate default precision", "c=truncate(?)", []string{"54321.0"}, true, true},
		{"round scalar default", "c=round((select ?))", []string{"54321.0"}, true, true},
		{"round scalar strict lower", "c>round((select ?),0)", []string{"54321.0"}, true, true},
		{"round scalar inclusive lower", "c>=round((select ?),0)", []string{"54321.0"}, true, true},
		{"truncate scalar strict upper", "c<truncate((select ?))", []string{"54321.0"}, true, true},
		{"truncate scalar inclusive upper", "c<=truncate((select ?),0)", []string{"54321.0"}, true, true},
		{"round scalar reversed range", "round((select ?))>c", []string{"54321.0"}, true, true},
		{"round scalar fractional fallback", "c>=round((select ?))", []string{"54321.5"}, false, true},
		{"round scalar collision fallback", "c>=round((select ?))", []string{"9007199254740992"}, false, true},
		{"round zero precision", "c=round(?,?)", []string{"54321.0", "0"}, true, true},
		{"truncate zero precision", "c=truncate(?,?)", []string{"54321.0", "0"}, true, true},
		{"explicit precision cast", "c=round(?,cast(? as signed))", []string{"54321.0", "0"}, true, true},
		{"round nonzero precision", "c=round(?,?)", []string{"54321.0", "1"}, false, true},
		{"round negative precision", "c=round(?,?)", []string{"54321.0", "-1"}, false, true},
		{"explicit column cast", "cast(c as decimal(5,0))=round(?,0)", []string{"54321.0"}, false, true},
		{"explicit value cast", "c=cast(round(?,0) as decimal(4,0))", []string{"54321.0"}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
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
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select c from numeric_filters where "+tc.predicate, 1)
			require.NoError(t, err)
			defer stmt.Free()
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt, bindings, values)
			require.NoError(t, err)
			require.True(t, bound.ValueDependent, "runtime value proof must not enter the type-only cache")
			foundScan, columnCast, executableParam := false, false, false
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
						return nil
					}))
				}
			}
			require.True(t, foundScan)
			require.Equal(t, !tc.native, columnCast, bound.Plan.String())
			require.True(t, executableParam, "proof witnesses must not replace executable parameters")
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
				mock := NewMockOptimizer(false)
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
		mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(false)
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

func TestPreparedSourceBindingConfigurationConsumers(t *testing.T) {
	t.Run("geometry SRID", func(t *testing.T) {
		mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(false)
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
			mock := NewMockOptimizer(false)
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
	mock := NewMockOptimizer(false)
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, "select regexp_substr(NULL,cast('a' as varbinary(1)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	_, err = BuildPreparedExecutionPlan(&mock.ctxt, stmt, nil, nil)
	require.NoError(t, err, "untyped NULL literal has no string domain")
}

func TestPreparedLowPrecisionFloatMarkerNarrowing(t *testing.T) {
	decimalSource := types.New(types.T_decimal64, 1, 1)
	for _, tc := range []struct {
		name      string
		predicate string
		value     string
		binding   types.Type
		binary    bool
		narrowed  bool
	}{
		{"equal sql decimal", "f = ?", "0.3", decimalSource, false, true},
		{"in sql decimal", "f in (?, 9)", "0.3", decimalSource, false, true},
		{"not in sql decimal", "f not in (?, 9)", "0.3", decimalSource, false, true},
		{"less equal sql decimal", "f <= ?", "0.3", decimalSource, false, true},
		{"in binary double", "f in (?, 9)", "0.3", types.T_float64.ToType(), true, true},
		{"in binary text", "f in (?, 9)", "0.3", types.T_varchar.ToType(), true, true},
		{"equal out of range", "f = ?", "1e300", types.T_float64.ToType(), true, false},
		{"in out of range", "f in (?, 9)", "1e300", types.T_float64.ToType(), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			proc := mock.ctxt.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, proc.Mp()))
			proc.SetPrepareParams(params)
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
				"select id from vecblock_t where "+tc.predicate, 1)
			require.NoError(t, err)
			defer stmt.Free()
			value := ParamValue{Value: tc.value, IsBinaryProtocol: tc.binary}
			if !tc.binary {
				value.EnableNumericPrefix = true
				value.SourceType, value.HasSourceType = tc.binding, true
			}
			bound, err := BuildPreparedExecutionPlan(&mock.ctxt, stmt,
				[]PreparedSourceBinding{{Position: 0, Type: tc.binding}}, []any{value})
			require.NoError(t, err)
			markerNarrowed, filters := false, 0
			for _, node := range bound.Plan.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					filters++
					require.NoError(t, planpb.VisitExprTree(filter, func(expr *Expr) error {
						if fn := expr.GetF(); fn != nil && fn.Func.ObjName == "cast" &&
							expr.Typ.Id == int32(types.T_bf16) && function.ContainsParameter(fn.Args[0]) {
							markerNarrowed = true
						}
						return nil
					}))
				}
			}
			require.Positive(t, filters)
			require.Equal(t, tc.narrowed, markerNarrowed, bound.Plan.String())
			if tc.narrowed {
				require.True(t, bound.ValueDependent, "a narrowed marker must not enter the type-only cache")
			}
		})
	}
}
