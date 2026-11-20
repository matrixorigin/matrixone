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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func groupedNumericFunctions(p *planpb.Plan, name string) []*planpb.Expr {
	var found []*planpb.Expr
	for _, n := range p.GetQuery().Nodes {
		for _, list := range [][]*planpb.Expr{n.ProjectList, n.FilterList, n.OnList, n.GroupBy, n.AggList, n.WinSpecList} {
			for _, expr := range list {
				walkPlanExpr(expr, func(e *planpb.Expr) {
					if f := e.GetF(); f != nil && f.Func.GetObjName() == name {
						found = append(found, e)
					}
				})
			}
		}
	}
	return found
}

func TestGroupedMySQLSpecialNumericContexts(t *testing.T) {
	for _, tc := range []struct {
		name, query, fn string
		want            bool
	}{
		{"enum", "select e + 0 from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"set", "select s + 0 from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"enum cast", "select cast(e as signed) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"set cast", "select cast(s as unsigned) from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"enum decimal cast", "select cast(e as decimal(10,2)) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"row-level cast", "select cast(e as signed) from enum_order_t", moEnumCastValueToIndexFun, false},
		{"derived", "select d.e + 0 from (select e from enum_order_t group by e) d", moEnumCastValueToIndexFun, true},
		{"cte", "with c as (select e from enum_order_t group by e) select e + 0 from c", moEnumCastValueToIndexFun, true},
		{"having", "select e from enum_order_t group by e having e + 0 > 1", moEnumCastValueToIndexFun, true},
		{"enum numeric comparison", "select e = 1 from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"set numeric comparison", "select s < 2 from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"numeric in", "select e in (1,2) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"numeric in null", "select e in (1,null) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"numeric not in null", "select s not in (1,null) from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"numeric no-FROM enum scalar", "select (select e)+0 from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"numeric no-FROM set scalar", "select (select s)+0 from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"cast no-FROM enum scalar", "select cast((select e) as signed) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"lexical no-FROM enum scalar", "select concat((select e), '') from enum_order_t group by e", moEnumCastValueToIndexFun, false},
		{"correlated enum", "select (select e+0) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"correlated set", "select (select s+0) from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"nested correlated enum", "select (select (select e+0)) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"correlated projected enum", "select (select 1 in (select enum_order_t.e)) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"correlated projected set", "select (select 1 in (select enum_order_t.s)) from enum_order_t group by s", moSetCastValueToIndexFun, true},
		{"correlated explicit char", "select (select cast(e as char)+0) from enum_order_t group by e", moEnumCastValueToIndexFun, false},
		{"left subquery", "select e in (select 1) from enum_order_t group by e", moEnumCastValueToIndexFun, true},
		{"right subquery", "select 1 in (select e from enum_order_t group by e)", moEnumCastValueToIndexFun, true},
		{"quantified", "select 1 = any (select e from enum_order_t group by e)", moEnumCastValueToIndexFun, true},
		{"row-level", "select e + 0 from enum_order_t", moEnumCastValueToIndexFun, false},
		{"numeric group key", "select e + 0 from enum_order_t group by e + 0", moEnumCastValueToIndexFun, false},
		{"explicit char", "select cast(e as char) + 0 from enum_order_t group by e", moEnumCastValueToIndexFun, false},
		{"mixed in", "select e in ('low', 1) from enum_order_t group by e", moEnumCastValueToIndexFun, false},
		{"mixed in null", "select e in ('low', 1, null) from enum_order_t group by e", moEnumCastValueToIndexFun, false},
		{"all null in", "select e in (null, null) from enum_order_t group by e", moEnumCastValueToIndexFun, false},
		{"ambiguous", "select e + 0 from enum_duplicate_t group by e", moEnumCastValueToIndexFun, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.query)
			require.NoError(t, err)
			found := groupedNumericFunctions(p, tc.fn)
			if !tc.want {
				require.Empty(t, found)
				return
			}
			require.NotEmpty(t, found)
			if tc.fn == moEnumCastValueToIndexFun {
				_, overload := planfunction.DecodeOverloadID(found[0].GetF().Func.Obj)
				require.Zero(t, overload)
				require.Len(t, found[0].GetF().Args, 2)
			}
		})
	}
}

func TestNoFromSpecialScalarInUsesOneRecovery(t *testing.T) {
	for _, op := range []string{"in", "not in"} {
		t.Run(op, func(t *testing.T) {
			query := "select (select e) " + op + " (1,null) from enum_order_t group by e"
			p, err := runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.NoError(t, err)
			require.Len(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun), 1)

			query = "select (select e) " + op + " (1.5,2,null) from enum_order_t group by e"
			_, err = runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.ErrorContains(t, err, "numeric scalar IN list needs a proven lossless shared type")
		})
	}
}

func TestCorrelatedSpecialInRejectsUnprovedContracts(t *testing.T) {
	for _, tc := range []struct{ query, want string }{
		{"select (select o.e in (2,'x') from enum_order_t i where i.id=o.id) from enum_order_t o group by o.id,o.e", "correlated special IN comparison requires a proven numeric contract"},
		{"select (select 1 in (o.e,3) from enum_order_t i where i.id=o.id) from enum_order_t o group by o.id,o.e", "correlated special IN comparison requires a proven numeric contract"},
		{"select 2 in ((select o.e),3) from enum_order_t o group by o.e", "numeric scalar IN list needs a proven lossless shared type"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			_, err := runOneStmt(newMySQLSpecialOrderMock(), t, tc.query)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestNoFromSpecialScalarTupleInRecovery(t *testing.T) {
	for _, op := range []string{"in", "not in"} {
		t.Run(op, func(t *testing.T) {
			query := "select ((select e), id) " + op + " ((1,1),(2,2)) from enum_order_t group by e,id"
			p, err := runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.NoError(t, err)
			require.NotEmpty(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun))

			for _, wrapped := range []string{"cast((select e) as signed)", "(select e)+0", "0+(select e)"} {
				query = "select (" + wrapped + ", id) " + op + " ((1,1),(2,2)) from enum_order_t group by e,id"
				p, err = runOneStmt(newMySQLSpecialOrderMock(), t, query)
				require.NoError(t, err)
				require.NotEmpty(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun))
			}

			query = "select ((select e), id) " + op + " ((1,1)) from enum_order_t group by e,id"
			p, err = runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.NoError(t, err)
			require.NotEmpty(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun))

			query = "select ((select e), id) " + op + " (('low',1),('mid',2)) from enum_order_t group by e,id"
			p, err = runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.NoError(t, err)
			require.Empty(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun))

			query = "select ((select e), id) " + op + " ((1,1),(2,2)) from enum_order_t"
			p, err = runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.NoError(t, err)
			require.Empty(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun))

			// Each right tuple is written once and must keep its own recovery.
			query = "select (id,0) " + op + " ((1,(select e)+0),(2,0)) from enum_order_t group by e,id"
			p, err = runOneStmt(newMySQLSpecialOrderMock(), t, query)
			require.NoError(t, err)
			require.Len(t, groupedNumericFunctions(p, moEnumCastValueToIndexFun), 1)
		})
	}
}

func TestSpecialColumnDisplayPreservesCorrelationDepth(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, true)
	ctx := NewBindContext(builder, nil)
	columns := []string{"e", "s"}
	typesForColumns := []*planpb.Type{
		{Id: int32(types.T_enum), Enumvalues: "low,high"},
		{Id: int32(types.T_uint64), Enumvalues: "red,green"},
	}
	binding := NewBinding(9, 0, "", "t", 0, columns, nil, typesForColumns, false, nil)
	for _, name := range columns {
		ctx.bindingByCol[name] = binding
	}
	binder := NewProjectionBinder(builder, ctx, NewHavingBinder(builder, ctx))
	for pos, name := range columns {
		for _, depth := range []int32{0, 1, 2} {
			expr, err := binder.baseBindColRef(tree.NewUnresolvedColName(name), depth, false)
			require.NoError(t, err)
			require.NotNil(t, expr.GetF())
			value := expr.GetF().Args[1]
			if depth == 0 {
				require.NotNil(t, value.GetCol())
				require.Equal(t, int32(9), value.GetCol().RelPos)
				require.Equal(t, int32(pos), value.GetCol().ColPos)
			} else {
				require.NotNil(t, value.GetCorr())
				require.Equal(t, &planpb.CorrColRef{RelPos: 9, ColPos: int32(pos), Depth: depth}, value.GetCorr())
			}
		}
	}
}

func TestGroupedNumericCorrelatedProvenance(t *testing.T) {
	outerType := &planpb.Type{Id: int32(types.T_enum), Enumvalues: "outer,other"}
	innerType := &planpb.Type{Id: int32(types.T_enum), Enumvalues: "inner,other"}
	outer := &BindContext{binder: &ProjectionBinder{}, projectTag: 10, mysqlSpecialOrderTypes: map[int32]*planpb.Type{0: outerType}}
	inner := &BindContext{parent: outer, binder: &ProjectionBinder{}, projectTag: 20, mysqlSpecialOrderTypes: map[int32]*planpb.Type{0: innerType}}
	transparent := &BindContext{parent: inner}
	leaf := &BindContext{parent: transparent}
	corr := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{RelPos: 10, ColPos: 0, Depth: 2}}}

	require.Equal(t, outerType, leaf.mysqlSpecialNumericTypeForExpr(corr))
	recovered, changed, err := storedMySQLSpecialNumericExpr(context.Background(), leaf, corr)
	require.NoError(t, err)
	require.True(t, changed)
	foundCorr := false
	walkPlanExpr(recovered, func(e *planpb.Expr) {
		if got := e.GetCorr(); got != nil {
			foundCorr = true
			require.Equal(t, corr.GetCorr(), got)
		}
	})
	require.True(t, foundCorr)
	leaf.projects = []*planpb.Expr{corr}
	require.Equal(t, outerType, leaf.mysqlSpecialNumericTypeForProject(0))
	leaf.mysqlSpecialOrderTypes = map[int32]*planpb.Type{0: nil}
	require.Nil(t, leaf.mysqlSpecialNumericTypeForProject(0))

	// A name shadow or an invalid depth must not borrow another scope's domain.
	corr.GetCorr().Depth = 1
	require.Nil(t, leaf.mysqlSpecialNumericTypeForExpr(corr))
	corr.GetCorr().RelPos = 20
	require.Equal(t, innerType, leaf.mysqlSpecialNumericTypeForExpr(corr))
	corr.GetCorr().Depth = 0
	require.Nil(t, leaf.mysqlSpecialNumericTypeForExpr(corr))
	corr.GetCorr().Depth = 3
	require.Nil(t, leaf.mysqlSpecialNumericTypeForExpr(corr))
}

func TestGroupedNumericFramedEnumPlan(t *testing.T) {
	payload, encoding, err := types.EncodeEnumMembers([]string{"a,b", "c"})
	require.NoError(t, err)
	require.Equal(t, uint32(types.EnumEncodingFramed), encoding)
	mock := newMySQLSpecialOrderMock()
	mock.ctxt.tables["enum_order_t"].Cols[1].Typ.Enumvalues = payload
	mock.ctxt.tables["enum_order_t"].Cols[1].Typ.EnumEncoding = encoding
	p, err := runOneStmt(mock, t, "select e+0 from enum_order_t group by e")
	require.NoError(t, err)
	found := groupedNumericFunctions(p, moEnumCastValueToIndexFun)
	require.NotEmpty(t, found)
	require.Len(t, found[0].GetF().Args, 3)
	_, overload := planfunction.DecodeOverloadID(found[0].GetF().Func.Obj)
	require.Equal(t, int32(1), overload)
	require.Equal(t, encoding, found[0].Typ.EnumEncoding)
}

func TestGroupedEnumNumericExecution(t *testing.T) {
	for _, domain := range []struct {
		name    string
		members []string
	}{{"legacy", []string{"2", "red"}}, {"framed", []string{"a,b", "red"}}} {
		t.Run(domain.name, func(t *testing.T) {
			payload, encoding, err := types.EncodeEnumMembers(domain.members)
			require.NoError(t, err)
			typ := &planpb.Type{Id: int32(types.T_enum), Enumvalues: payload, EnumEncoding: encoding}
			proc := testutil.NewProcess(t)
			t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
			null := makePlan2NullConstExprWithType()
			null.Typ.Id = int32(types.T_varchar)
			for _, tc := range []struct {
				name    string
				display *planpb.Expr
				want    uint16
				isNull  bool
			}{
				{"zero", makePlan2StringConstExprWithType(""), 0, false},
				{"first", makePlan2StringConstExprWithType(domain.members[0]), 1, false},
				{"second", makePlan2StringConstExprWithType("red"), 2, false},
				{"null", null, 0, true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					expr, err := makeMySQLSpecialNumericValue(context.Background(), tc.display, typ)
					require.NoError(t, err)
					require.Equal(t, encoding, expr.Typ.EnumEncoding)
					raw, err := proto.Marshal(expr)
					require.NoError(t, err)
					var decoded planpb.Expr
					require.NoError(t, proto.Unmarshal(raw, &decoded))
					exec, err := colexec.NewExpressionExecutor(proc, &decoded)
					require.NoError(t, err)
					defer exec.Free()
					out, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.Equal(t, tc.isNull, out.GetNulls().Contains(0))
					if !tc.isNull {
						require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[uint16](out)[0])
					}
				})
			}
		})
	}
}

// A subquery may carry hidden projections after its visible result columns.
// Publishing the numeric rewrite must not index or replace those slots.
func TestGroupedNumericSubqueryKeepsHiddenProjection(t *testing.T) {
	display := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 12, 0)
	hidden := MakePlan2Int64ConstExprWithType(42)
	originalResults := []*planpb.Expr{display, hidden}
	subCtx := &BindContext{results: append([]*planpb.Expr(nil), originalResults...), mysqlSpecialOrderTypes: map[int32]*planpb.Type{0: {Id: int32(types.T_enum), Enumvalues: "low,mid"}}}
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, false)
	builder.qry.Nodes = []*planpb.Node{{NodeType: planpb.Node_PROJECT, ProjectList: []*planpb.Expr{display, hidden}}}
	builder.ctxByNode = []*BindContext{subCtx}
	binder := &baseBinder{sysCtx: context.Background(), builder: builder, ctx: &BindContext{}}
	sub := &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{NodeId: 0, RowSize: 1}}}
	left := MakePlan2Int64ConstExprWithType(1)
	got, err := binder.useStoredMySQLSpecialTypesForNumericSubquery(left, sub)
	require.NoError(t, err)
	require.Same(t, left, got)
	require.NotSame(t, display, builder.qry.Nodes[0].ProjectList[0])
	require.Same(t, builder.qry.Nodes[0].ProjectList[0], subCtx.results[0])
	require.Same(t, hidden, builder.qry.Nodes[0].ProjectList[1])
	require.Same(t, hidden, subCtx.results[1])
	require.Same(t, display, originalResults[0])
}

func TestReleasedEnumDefaultReplay(t *testing.T) {
	typ := planpb.Type{Id: int32(types.T_enum), Enumvalues: "2#3:a,b1:c"}
	const releasedFunctionID = 306
	expr := &planpb.Expr{Typ: typ, Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "cast_value_to_index", Obj: planfunction.EncodeOverloadID(releasedFunctionID, 0)},
		Args: []*planpb.Expr{makePlan2StringConstExprWithType(typ.Enumvalues), makePlan2StringConstExprWithType("b1:c")},
	}}}
	data, err := expr.Marshal()
	require.NoError(t, err)
	var stored planpb.Expr
	require.NoError(t, stored.Unmarshal(data))
	col := &planpb.ColDef{Name: "e", Typ: typ, Default: &planpb.Default{Expr: &stored}}
	loaded, err := getDefaultExpr(t.Context(), col)
	require.NoError(t, err)
	require.NotSame(t, &stored, loaded)
	require.Equal(t, expr, loaded)
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	exec, err := colexec.NewExpressionExecutor(proc, loaded)
	require.NoError(t, err)
	defer exec.Free()
	out, err := exec.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
	require.NoError(t, err)
	require.Equal(t, types.Enum(2), vector.MustFixedColWithTypeCheck[types.Enum](out)[0])
}
