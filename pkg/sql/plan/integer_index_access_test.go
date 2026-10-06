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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExactDecimalIntegerDomain(t *testing.T) {
	for _, tc := range []struct {
		value  string
		target types.T
		fits   bool
	}{
		{"0.0", types.T_uint64, true}, {"127.0", types.T_int8, true}, {"128.0", types.T_int8, false},
		{"-128.0", types.T_int8, true}, {"-129.0", types.T_int8, false}, {"-1.0", types.T_uint64, false},
		{"2147483647.0", types.T_int32, true}, {"2147483648.0", types.T_int32, false},
		{"-2147483648.0", types.T_int32, true}, {"-2147483649.0", types.T_int32, false},
		{"9223372036854775807.0", types.T_int64, true}, {"9223372036854775808.0", types.T_int64, false},
		{"-9223372036854775808.0", types.T_int64, true}, {"-9223372036854775809.0", types.T_int64, false},
		{"18446744073709551615.0", types.T_uint64, true}, {"18446744073709551616.0", types.T_uint64, false},
		{"9007199254740993.0", types.T_int64, true}, {"9.000002", types.T_int32, false},
		{"0.00000000000000000000000000000000000001", types.T_int64, false},
		{"9007199254740993.0000000000000000000000000000000000000000", types.T_int64, true},
		{"18446744073709551616.0000000000000000000000000000000000000000", types.T_uint64, false},
	} {
		t.Run(tc.value+tc.target.String(), func(t *testing.T) {
			expr, err := makePlan2DecimalExprWithType(context.Background(), tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.fits, exactDecimalIntegerFits(expr, tc.target))
			col := GetColExpr(pb.Type{Id: int32(tc.target)}, 0, 0)
			bound, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*pb.Expr{col, expr})
			require.NoError(t, err)
			require.Equal(t, tc.fits, bound.GetF().Args[0].GetCol() != nil)
		})
	}
	for _, tc := range []struct {
		oid   types.T
		value string
	}{{types.T_decimal64, "127.0"}, {types.T_decimal128, "-128.0"}} {
		value, scale, err := types.Parse128(tc.value)
		require.NoError(t, err)
		var expr *pb.Expr
		if tc.oid == types.T_decimal64 {
			expr = MakePlan2Decimal64ExprWithType(types.Decimal64(value.B0_63), &pb.Type{Id: int32(tc.oid), Width: 18, Scale: scale})
		} else {
			expr = MakePlan2Decimal128ExprWithType(value, &pb.Type{Id: int32(tc.oid), Width: 38, Scale: scale})
		}
		require.True(t, exactDecimalIntegerFits(expr, types.T_int8))
		expr.Typ.Scale = -1
		require.False(t, exactDecimalIntegerFits(expr, types.T_int8))
	}
}

func TestExactDecimalNativeIntegerFilters(t *testing.T) {
	for _, predicate := range []string{"id=3.0", "3.0=id", "id>=3.0", "3.0<id", "id in (1.0,3.0)", "id between 1.0 and 3.0",
		"id=(select 3.0)", "id<(select 3.0)", "(select 3.0)<id", "id=(select 3)", "id=1.0+2.0", "id<=>(select 3.0)",
		"id in (1.0,(select 3.0))", "id not in (1.0,(select 3.0))", "id between (select 1.0) and (select 3.0)",
		"id=(select 2147483647.0)", "id=(select 3.0000000000000000000000000000000000000000)"} {
		t.Run(predicate, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, "select id from index_hint_t use index() where "+predicate)
			require.NoError(t, err)
			var scan *pb.Node
			for _, n := range query.GetQuery().Nodes {
				if n.NodeType == pb.Node_TABLE_SCAN && !n.IndexScanInfo.IsIndexScan {
					scan = n
				}
			}
			require.NotNil(t, scan)
			require.NotEmpty(t, scan.FilterList)
			for _, filter := range scan.FilterList {
				require.NotNil(t, filter.GetF().Args[0].GetCol(), predicate)
				require.Equal(t, int32(types.T_int32), filter.GetF().Args[0].Typ.Id)
			}
		})
	}
}

func TestNullableUniqueHintCompleteness(t *testing.T) {
	for _, tc := range []struct {
		sql       string
		usesIndex bool
	}{
		{"select id,a,b from index_hint_t force index(uk_ab) order by id", false},
		{"select a,b from index_hint_t force index(uk_ab) order by a", false},
		{"select id from index_hint_t force index(uk_ab) where a is null order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a=1 order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a is not null order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a=1 or b=2 order by id", false},
		{"select id from index_hint_t force index(uk_ab) where a is not null and b is not null order by id", true},
		{"select id from index_hint_t force index(uk_ab) where (a=1 and b=2) or (a=3 and b=4) order by id", true},
		{"select a,count(*) from index_hint_t force index for group by(uk_ab) group by a order by a", false},
		{"select id from index_hint_t force index for join(uk_ab) join nation on id=n_nationkey", false},
		{"select id,a,b from index_hint_t force index(idx_ab) order by id", true},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, tc.sql)
			require.NoError(t, err)
			require.Equal(t, tc.usesIndex, findFirstIndexScanName(query) != "")
		})
	}
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIndexHintChoiceTableForTest(mock)
	for _, c := range mock.ctxt.tables["index_hint_t"].Cols {
		c.Typ.NotNullable = true
		c.Default = nil
	}
	query, err := runOneStmt(mock, t, "select a,b from index_hint_t force index(uk_ab) order by a")
	require.NoError(t, err)
	require.Equal(t, "uk_ab", findFirstIndexScanName(query))
}

func TestSparseUniqueRejectionIsAtomic(t *testing.T) {
	builder := NewQueryBuilder(pb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	tag := builder.genNewBindTag()
	node := &pb.Node{BindingTags: []int32{tag}, TableDef: &pb.TableDef{Cols: []*pb.ColDef{{Name: "k", Typ: pb.Type{Id: int32(types.T_int32)}}}, Name2ColIndex: map[string]int32{"k": 0}, Pkey: &pb.PrimaryKeyDef{PkeyColName: "k"}}}
	index := &pb.IndexDef{Unique: true, Parts: []string{"k"}, IndexName: "uq", IndexTableName: "missing", TableExist: true}
	mapping := make(map[[2]int32]*pb.Expr)
	before := builder.nextBindTag
	id, err := builder.tryHintedCoveringIndexScan(index, node, map[[2]int32]int{{tag, 0}: 1}, mapping)
	require.NoError(t, err)
	require.EqualValues(t, -1, id)
	id, _, err = builder.buildHintedIndexBackfillJoin(index, node)
	require.NoError(t, err)
	require.EqualValues(t, -1, id)
	require.Empty(t, builder.qry.Nodes)
	require.Empty(t, mapping)
	require.Equal(t, before, builder.nextBindTag)
}

func TestExactDecimalPreservesOtherDomains(t *testing.T) {
	for _, predicate := range []string{"id=3.1", "id=2147483648.0", "id=3e0", "cast(id as decimal(12,1))=3.0",
		"id=(select 3.1)", "id=(select 2147483648.0)", "id=(select 3e0)", "cast(id as decimal(12,1))=(select 3.0)"} {
		t.Run(predicate, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			addIndexHintChoiceTableForTest(mock)
			query, err := runOneStmt(mock, t, "select id from index_hint_t use index() where "+predicate)
			require.NoError(t, err)
			casts := 0
			for _, n := range query.GetQuery().Nodes {
				if n.NodeType == pb.Node_TABLE_SCAN {
					casts += countExprFunctionCalls(n.FilterList, "cast")
				}
			}
			require.Positive(t, casts, "unproven and explicitly declared domains retain their column cast")
		})
	}
}

func TestNativeIntegerNormalizationSafety(t *testing.T) {
	ctx := context.Background()
	decimalPeer, err := makePlan2DecimalExprWithType(ctx, "3.0")
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		source types.T
		target pb.Type
		accept bool
	}{
		{"integer widening", types.T_int32, pb.Type{Id: int32(types.T_int64)}, true},
		{"unsigned widening", types.T_uint32, pb.Type{Id: int32(types.T_int64)}, true},
		{"integer narrowing", types.T_int64, pb.Type{Id: int32(types.T_int32)}, false},
		{"signed to unsigned", types.T_int32, pb.Type{Id: int32(types.T_uint64)}, false},
		{"unsigned to signed", types.T_uint64, pb.Type{Id: int32(types.T_int64)}, false},
		{"decimal capacity", types.T_int32, pb.Type{Id: int32(types.T_decimal64), Width: 11, Scale: 1}, true},
		{"decimal too narrow", types.T_int32, pb.Type{Id: int32(types.T_decimal64), Width: 10, Scale: 1}, false},
		{"zero width safe", types.T_int64, pb.Type{Id: int32(types.T_decimal128), Scale: 1}, true},
		{"zero width unsafe", types.T_int64, pb.Type{Id: int32(types.T_decimal64), Scale: 1}, false},
		{"negative scale", types.T_int32, pb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: -1}, false},
		{"physical overflow", types.T_int64, pb.Type{Id: int32(types.T_decimal128), Width: 38, Scale: 20}, false},
		{"approximate", types.T_int32, pb.Type{Id: int32(types.T_float64)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(pb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
			column := GetColExpr(pb.Type{Id: int32(tc.source)}, 0, 0)
			cast, err := appendCastBeforeExpr(ctx, column, tc.target)
			require.NoError(t, err)
			original := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "="}, Args: []*pb.Expr{cast, DeepCopyExpr(decimalPeer)}}}}
			before := DeepCopyExpr(original)
			rewritten := builder.rewriteNativeIntegerComparison(original)
			require.Equal(t, tc.accept, rewritten != original)
			require.Equal(t, before, original, "proof must not modify the candidate")
			if tc.accept {
				require.NotNil(t, rewritten.GetF().Args[0].GetCol())
				require.Equal(t, int32(tc.source), rewritten.GetF().Args[1].Typ.Id)
				require.Same(t, rewritten, builder.rewriteNativeIntegerComparison(rewritten), "idempotent")
			}
		})
	}

	builder := NewQueryBuilder(pb.Query_SELECT, NewMockCompilerContext(true, newPlanTestProcess(t)), false, true)
	column := GetColExpr(pb.Type{Id: int32(types.T_int32)}, 0, 0)
	cast, err := appendCastBeforeExpr(ctx, column, pb.Type{Id: int32(types.T_decimal128), Width: 11, Scale: 1})
	require.NoError(t, err)
	for _, change := range []struct {
		name string
		edit func(*pb.Expr)
	}{
		{"explicit syntax", func(e *pb.Expr) { e.GetF().Args[0].GetF().SyntaxExplicitCast = true }},
		{"private CAST4", func(e *pb.Expr) { e.GetF().Args[0].GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 4) }},
		{"target mismatch", func(e *pb.Expr) { e.GetF().Args[0].GetF().Args[1].Typ.Scale++ }},
		{"fractional peer", func(e *pb.Expr) {
			e.GetF().Args[1], err = makePlan2DecimalExprWithType(ctx, "3.1")
			require.NoError(t, err)
		}},
		{"NULL peer", func(e *pb.Expr) {
			e.GetF().Args[1] = &pb.Expr{Typ: decimalPeer.Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Isnull: true}}}
		}},
	} {
		t.Run(change.name, func(t *testing.T) {
			expr := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "="}, Args: []*pb.Expr{DeepCopyExpr(cast), DeepCopyExpr(decimalPeer)}}}}
			change.edit(expr)
			before := DeepCopyExpr(expr)
			require.Same(t, expr, builder.rewriteNativeIntegerComparison(expr))
			require.Equal(t, before, expr)
		})
	}
	for _, source := range []*pb.Expr{
		{Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}},
		{Expr: &pb.Expr_V{V: &pb.VarRef{Name: "value"}}},
		GetColExpr(pb.Type{Id: int32(types.T_decimal128)}, 1, 0),
		{PreparedNumeric: &pb.PreparedNumericMetadata{ProvisionalResultPeer: true}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_I64Val{I64Val: 3}}}},
	} {
		peer := MakePlan2Decimal64ExprWithType(types.Decimal64(30), &pb.Type{Id: int32(types.T_decimal64), Width: 18, Scale: 1})
		peer.GetLit().Src = source
		expr := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "="}, Args: []*pb.Expr{cast, peer}}}}
		require.Same(t, expr, builder.rewriteNativeIntegerComparison(expr), "dynamic Src is not a static proof")
	}
	fraction, err := makePlan2DecimalExprWithType(ctx, "3.1")
	require.NoError(t, err)
	for _, name := range []string{"in", "not_in", "between"} {
		for _, last := range []*pb.Expr{decimalPeer, fraction, {Typ: decimalPeer.Typ, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Isnull: true}}}} {
			args := []*pb.Expr{DeepCopyExpr(cast), DeepCopyExpr(decimalPeer), DeepCopyExpr(last)}
			if name != "between" {
				args = []*pb.Expr{args[0], {Typ: cast.Typ, Expr: &pb.Expr_List{List: &pb.ExprList{List: args[1:]}}}}
			}
			expr := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: name}, Args: args}}}
			before := DeepCopyExpr(expr)
			require.Equal(t, last == decimalPeer, builder.rewriteNativeIntegerComparison(expr) != expr, name)
			require.Equal(t, before, expr, "list rejection and publication are atomic")
		}
	}
	// The shared pass serves JOIN OnList as well as scan FilterList, without
	// treating the other JOIN column as a constant.
	for _, peer := range []*pb.Expr{decimalPeer, GetColExpr(decimalPeer.Typ, 1, 0)} {
		condition := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Func: &pb.ObjectRef{ObjName: "="}, Args: []*pb.Expr{DeepCopyExpr(cast), DeepCopyExpr(peer)}}}}
		parent, err := BindFuncExprImplByPlanExpr(ctx, "and", []*pb.Expr{condition, MakePlan2BoolConstExprWithType(true)})
		require.NoError(t, err)
		parent.Ndv, parent.Selectivity = 37, .8
		builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_JOIN, OnList: []*pb.Expr{parent}}}
		require.NoError(t, builder.rewriteNumericDomainFilters(0, pb.Node_TABLE_SCAN))
		require.Nil(t, condition.GetF().Args[0].GetCol(), "scan phase must not prove JOINs")
		require.NoError(t, builder.rewriteNumericDomainFilters(0, pb.Node_JOIN))
		require.Equal(t, peer.GetCol() == nil, condition.GetF().Args[0].GetCol() != nil)
		if peer.GetCol() == nil {
			require.Zero(t, parent.Ndv)
			require.Zero(t, parent.Selectivity)
		} else {
			require.Equal(t, float64(37), parent.Ndv)
		}
	}
}

func TestNumericDomainProofCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	compiler.SetContext(ctx)
	b := NewQueryBuilder(pb.Query_SELECT, compiler, false, false)
	require.ErrorIs(t, b.rewriteNumericDomainFilters(0, pb.Node_JOIN), context.Canceled)
}
