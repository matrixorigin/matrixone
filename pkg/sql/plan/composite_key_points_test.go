// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestCompositePointCandidate(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	table := makeExprOptThreePartCompositeSortKeyTableDef()
	delete(table.Name2ColIndex, table.Pkey.PkeyColName)
	table.Cols[3].Name = catalog.CPrimaryKeyColName
	table.Cols[3].Hidden = true
	table.Name2ColIndex[catalog.CPrimaryKeyColName] = 3
	table.Pkey.PkeyColName = catalog.CPrimaryKeyColName
	table.Pkey.CompPkeyCol = table.Cols[3]
	const tag = 10
	eq := func(pos int32, value int64) *pb.Expr {
		return makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, pos, table.Cols[pos].Name), value)
	}
	logical := func(name string, args ...*pb.Expr) *pb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args[:2])
		require.NoError(t, err)
		expr.GetF().Args = args
		return expr
	}
	and := func(args ...*pb.Expr) *pb.Expr { return logical("and", args...) }
	or := func(args ...*pb.Expr) *pb.Expr { return logical("or", args...) }
	point := func(b, c int64) *pb.Expr { return and(eq(1, b), eq(2, c)) }
	first, second := point(2, 3), point(4, 5)
	commuted, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{
		MakePlan2Int64ConstExprWithType(7), makeExprOptInt64Col(tag, 1, "b"),
	})
	require.NoError(t, err)
	foreign := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag+1, 1, "b"), 2)
	rangeFilter := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 2, "c"), 0)
	dynamicLiteral := eq(1, 4)
	dynamicLiteral.GetF().Args[1].GetLit().Src = &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 1}}}
	for _, tc := range []struct {
		name    string
		filters []*pb.Expr
		want    [][]int64
	}{
		{"correlated", []*pb.Expr{eq(0, 1), or(first, second)}, [][]int64{{1, 2, 3}, {1, 4, 5}}},
		{"shared suffix and reversed equality", []*pb.Expr{eq(0, 1), eq(2, 9), or(commuted, eq(1, 8))}, [][]int64{{1, 7, 9}, {1, 8, 9}}},
		{"nested", []*pb.Expr{eq(0, 1), or(first, and(eq(1, 4), or(eq(2, 5), eq(2, 6))))}, [][]int64{{1, 2, 3}, {1, 4, 5}, {1, 4, 6}}},
		{"contradictory branch", []*pb.Expr{eq(0, 1), or(and(eq(0, 9), eq(1, 2), eq(2, 3)), second)}, [][]int64{{1, 2, 3}, {1, 4, 5}}},
		{"incomplete branch", []*pb.Expr{eq(0, 1), or(first, eq(1, 4))}, nil},
		{"foreign column", []*pb.Expr{eq(0, 1), or(first, and(foreign, eq(2, 5)))}, nil},
		{"independent ORs are not multiplied", []*pb.Expr{eq(0, 1), or(eq(1, 2), eq(1, 4)), or(eq(2, 3), eq(2, 5))}, nil},
		{"failed OR does not leak points into later OR", []*pb.Expr{eq(0, 1), or(first, eq(1, 4)), or(point(6, 7), point(8, 9))}, [][]int64{{1, 6, 7}, {1, 8, 9}}},
		{"safe range residual", []*pb.Expr{eq(0, 1), or(first, second), rangeFilter}, [][]int64{{1, 2, 3}, {1, 4, 5}}},
		{"dynamic folded literal", []*pb.Expr{eq(0, 1), or(first, and(dynamicLiteral, eq(2, 5)))}, nil},
		{"existing single point", []*pb.Expr{eq(0, 1), eq(1, 2), eq(2, 3), or(eq(1, 2), eq(1, 4))}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := DeepCopyExprList(tc.filters)
			node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: tc.filters}
			got := compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes)
			if tc.want == nil {
				require.Nil(t, got)
			} else {
				require.NotNil(t, got)
				require.Len(t, got.values, len(tc.want))
				for i := range got.values {
					for j := range got.values[i] {
						require.Equal(t, tc.want[i][j], got.values[i][j].GetLit().GetI64Val())
					}
				}
			}
			for i := range before {
				require.True(t, proto.Equal(before[i], tc.filters[i]), "analysis changed the SQL predicate")
			}
		})
	}

	t.Run("bounded points", func(t *testing.T) {
		arms := make([]*pb.Expr, maxCompositePoints+1)
		for i := range arms {
			arms[i] = point(int64(i), int64(i))
		}
		node := &pb.Node{TableDef: table, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(arms[:maxCompositePoints]...)}}
		candidate := compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes)
		require.Len(t, candidate.values, maxCompositePoints)
		require.GreaterOrEqual(t, candidate.bytes, uint64(maxCompositePoints)*types.DefaultPackerCapacity(), "each serial executor owns a packer")
		nested := arms[0]
		for _, arm := range arms[1:maxCompositePoints] {
			nested = or(nested, arm)
		}
		node.FilterList[1] = nested
		require.Len(t, compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes).values, maxCompositePoints, "the parser can produce a left-deep OR")
		node.FilterList[1] = or(arms...)
		require.Nil(t, compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes))
		node.FilterList = append(node.FilterList, or(point(200, 201), point(202, 203)))
		got := compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes)
		require.Len(t, got.values, 2, "an oversized alternative must not contaminate a later bounded one")
		require.EqualValues(t, 200, got.values[0][1].GetLit().GetI64Val())
		require.EqualValues(t, 202, got.values[1][1].GetLit().GetI64Val())
	})
	t.Run("query budget spans scans", func(t *testing.T) {
		filters := []*pb.Expr{eq(0, 1), or(first, second)}
		firstScan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: filters}
		secondScan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: filters}
		candidate := compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), firstScan, maxCompositePointBytes)
		require.NotNil(t, candidate)
		// Two candidate bodies fit exactly, but the shared budget must also
		// cover metadata. The second scan therefore keeps its baseline filter.
		budget := 2 * candidate.bytes
		capture := &compositePointCapture{remaining: &budget}
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		builder.qry.Nodes = []*pb.Node{firstScan, secondScan}
		capture.collect(builder, 0, firstScan)
		capture.collect(builder, 1, secondScan)
		require.Contains(t, capture.proposals, int32(0))
		require.NotContains(t, capture.proposals, int32(1))
	})
	t.Run("encoded byte budget", func(t *testing.T) {
		folded := makePlan2StringConstExprWithType("ab\x00cd")
		folded.Typ.Width = 1 // A folded function's declared width can understate its value.
		bound, ok := compositePointValueSize(ctx.GetContext(), ctx.GetProcess(), folded)
		require.True(t, ok)
		require.EqualValues(t, 13, bound)
		stringTable := *table
		stringTable.Cols = append([]*pb.ColDef(nil), table.Cols...)
		for i := 0; i < 3; i++ {
			col := *table.Cols[i]
			col.Typ = pb.Type{Id: int32(types.T_varchar), Width: 16, Charset: uint32(types.CharsetUTF8)}
			stringTable.Cols[i] = &col
		}
		stringEq := func(pos int32, value *pb.Expr) *pb.Expr {
			col := &pb.Expr{Typ: stringTable.Cols[pos].Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: tag, ColPos: pos}}}
			expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{col, value})
			require.NoError(t, err)
			// This exercises the post-fold representation used by scan planning.
			expr.GetF().Args[1] = value
			return expr
		}
		arms := make([]*pb.Expr, 50)
		for i := range arms {
			arms[i] = and(stringEq(1, makePlan2StringConstExprWithType("b")), stringEq(2, makePlan2StringConstExprWithType("c")))
		}
		node := &pb.Node{TableDef: &stringTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{stringEq(0, makePlan2StringConstExprWithType("a")), or(arms...)}}
		got := compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes)
		require.NotNil(t, got, "%s", node.FilterList[0].String())
		require.Len(t, got.values, 50)
		node.FilterList[1] = or(arms[:2]...)
		node.FilterList[0] = stringEq(0, makePlan2StringConstExprWithType(strings.Repeat("x", 100_000)))
		nearLimit := compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes)
		require.NotNil(t, nearLimit)
		require.Greater(t, nearLimit.bytes, uint64(maxCompositePointBytes/2))
		require.LessOrEqual(t, nearLimit.bytes, uint64(maxCompositePointBytes))

		// A folded value may be much larger than its declared type width.
		large := makePlan2StringConstExprWithType(strings.Repeat("x", 1<<20))
		large.Typ.Width = 1
		node.FilterList[0] = stringEq(0, large)
		require.Nil(t, compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes), "actual folded bytes exceed the candidate budget")

		padded := stringEq(0, makePlan2StringConstExprWithType("a"))
		padded.GetF().Args[1].Typ.PadSpace = true
		node.FilterList[0] = padded
		require.Nil(t, compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes), "PAD SPACE provenance cannot use raw tuple equality")
		foreignCollation := stringEq(0, makePlan2StringConstExprWithType("a"))
		foreignCollation.GetF().Args[1].Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		node.FilterList[0] = foreignCollation
		require.Nil(t, compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes), "a different comparison domain cannot use the stored bytes")

		param := &pb.Expr{Typ: stringTable.Cols[0].Typ, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
		node.FilterList[0] = stringEq(0, param)
		node.NodeType = pb.Node_TABLE_SCAN
		proc := ctx.GetProcess()
		params := vector.NewVec(types.T_text.ToType())
		defer params.Free(proc.Mp())
		require.NoError(t, vector.AppendBytes(params, []byte("a"), false, proc.Mp()))
		proc.SetPrepareParams(params)
		defer proc.SetPrepareParams(nil)
		original := ctx.GetContext()
		binding := withPreparedSourceBindings(original, []PreparedSourceBinding{{Position: 0, Type: types.T_varchar.ToType()}}, []any{"a"})
		ctx.SetContext(binding)
		defer ctx.SetContext(original)
		budget := uint64(2*compositePointCopies)*types.DefaultPackerCapacity() - 1
		capture := &compositePointCapture{remaining: &budget}
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		capture.collect(builder, 0, node)
		require.Empty(t, capture.proposals)
		require.False(t, preparedBindingState(binding).valueDependent, "an exhausted query budget must not disable plan caching")
		node.FilterList[1] = or(arms...)
		budget = uint64((len(arms)-1)*compositePointCopies) * types.DefaultPackerCapacity()
		capture.collect(builder, 0, node)
		require.Empty(t, capture.proposals)
		require.False(t, preparedBindingState(binding).valueDependent, "an insufficient budget for the known point count must not disable plan caching")
		budget = maxCompositePointBytes
		capture.collect(builder, 0, node)
		require.Contains(t, capture.proposals, int32(0))
		require.True(t, preparedBindingState(binding).valueDependent, "the same bound values are relevant when the candidate fits")
	})
	t.Run("float falls back", func(t *testing.T) {
		floatTable := *table
		floatTable.Cols = append([]*pb.ColDef(nil), table.Cols...)
		floatCol := *table.Cols[1]
		floatCol.Typ.Id = int32(types.T_float64)
		floatTable.Cols[1] = &floatCol
		node := &pb.Node{TableDef: &floatTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(first, second)}}
		require.Nil(t, compositePointCandidate(ctx.GetContext(), ctx.GetProcess(), node, maxCompositePointBytes))
	})

}

func TestCompositePointPreparedValueBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		value     []byte
		null      bool
		castWidth int32
		want      uint64
		wantAdmit bool
	}{
		{name: "raw bytes", value: []byte{'a', 0, 'b'}, want: 9, wantAdmit: true},
		{name: "null", null: true, wantAdmit: true},
		{name: "normal cast fits", value: []byte("abc"), castWidth: 3, want: 9, wantAdmit: true},
		{name: "normal cast exceeds width", value: []byte("abcd"), castWidth: 3},
		{name: "normal cast preserves binary bytes", value: []byte{0xff, 0}, castWidth: 3, want: 7, wantAdmit: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(params, tc.value, tc.null, proc.Mp()))
			proc.SetPrepareParams(params)
			defer proc.SetPrepareParams(nil)
			typ := types.T_varchar.ToType()
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: typ}}, []any{"different from transport"})
			param := &pb.Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
			value := param
			if tc.castWidth > 0 {
				textType := types.T_text.ToType()
				param.Typ = makePlan2Type(&textType)
				target := types.New(types.T_varchar, tc.castWidth, 0)
				var err error
				value, err = appendCastBeforeExpr(ctx, param, makePlan2Type(&target))
				require.NoError(t, err)
				require.True(t, compositePointScalar(value))
				explicit := DeepCopyExpr(value)
				explicit.GetF().SyntaxExplicitCast = true
				require.False(t, compositePointScalar(explicit))
			}
			got, ok := compositePointValueSize(ctx, proc, value)
			require.Equal(t, tc.wantAdmit, ok)
			require.Equal(t, tc.want, got)
			require.True(t, preparedBindingState(ctx).valueDependent)
		})
	}
	t.Run("missing execution binding", func(t *testing.T) {
		proc := newPlanTestProcess(t)
		typ := types.T_varchar.ToType()
		param := &pb.Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
		_, ok := compositePointValueSize(context.Background(), proc, param)
		require.False(t, ok)
		ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: typ}}, []any{"value"})
		_, ok = compositePointValueSize(ctx, proc, param)
		require.False(t, ok)
		require.True(t, preparedBindingState(ctx).valueDependent)
	})
	_, ok := compositePointStringSize(int((maxCompositePointBytes-3)/2) + 1)
	require.False(t, ok)
}

func TestCompositePointRejectsFallibleUnselectedEquality(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  bool
	}{{"abc", true}, {"abcd", false}} {
		t.Run(tc.value, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			require.NoError(t, vector.AppendBytes(params, []byte(tc.value), false, proc.Mp()))
			proc.SetPrepareParams(params)
			defer proc.SetPrepareParams(nil)
			binding := types.T_varchar.ToType()
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: binding}}, []any{tc.value})
			textType := types.T_text.ToType()
			param := &pb.Expr{Typ: makePlan2Type(&textType), Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
			target := types.New(types.T_varchar, 3, 0)
			cast, err := appendCastBeforeExpr(ctx, param, makePlan2Type(&target))
			require.NoError(t, err)
			col := &pb.Expr{Typ: cast.Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 10, ColPos: 0}}}
			eq, err := BindFuncExprImplByPlanExpr(ctx, "=", []*pb.Expr{col, cast})
			require.NoError(t, err)
			a := compositePointAnalysis{ctx: ctx, proc: proc, tag: 10, parts: map[int32]int{0: 0}, types: []pb.Type{col.Typ}}
			_, _, recognized := a.equality(eq)
			require.True(t, recognized)
			require.True(t, a.safePredicate(eq, 0))
			_, safe := a.safeValueSizes()
			require.Equal(t, tc.want, safe)
			require.True(t, preparedBindingState(ctx).valueDependent)
		})
	}
}

func TestCompositePointFallbackPreservesPreparedPlanCache(t *testing.T) {
	const complete = "select payload from index_hint_t where a=? and ((b=? and id=?) or (b=? and id=?))"
	for _, tc := range []struct {
		name, sql      string
		params         int
		oversized      bool
		valueDependent bool
	}{
		{"incomplete key", "select payload from index_hint_t where a=? and ((b=? and payload=1) or (b=? and payload=2))", 3, false, false},
		{"unsafe residual", complete + " and payload+1>0", 5, false, false},
		{"point candidate", complete, 5, false, true},
		{"oversized candidate", complete, 5, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			mock := NewMockOptimizer(true, proc)
			addPrimaryPrefixIndexTableForTest(mock)
			table := mock.ctxt.tables["index_hint_t"]
			for i := range 3 {
				table.Cols[i].Typ = pb.Type{Id: int32(types.T_varchar), Width: 16, NotNullable: true, Charset: uint32(types.CharsetUTF8)}
			}
			table.Pkey.Names = []string{"a", "b", "id"}
			table.Indexes = nil
			statements, err := mysql.Parse(mock.ctxt.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			typ := types.T_varchar.ToType()
			bindings := make([]PreparedSourceBinding, tc.params)
			values := make([]any, tc.params)
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			for i := range bindings {
				value := "abc"
				if i == 0 && tc.oversized {
					value = strings.Repeat("x", 1<<20)
				}
				bindings[i] = PreparedSourceBinding{Position: int32(i), Type: typ}
				values[i] = value
				require.NoError(t, vector.AppendBytes(params, []byte(value), false, proc.Mp()))
			}
			proc.SetPrepareParams(params)
			defer proc.SetPrepareParams(nil)
			result, err := BuildPreparedExecutionPlan(&mock.ctxt, statements[0], bindings, values)
			require.NoError(t, err)
			require.Equal(t, tc.valueDependent, result.ValueDependent)
		})
	}
}

func TestCompositePointProposalRevalidation(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	newProposal := func(t *testing.T) (*QueryBuilder, *compositePointCapture, *pb.Node) {
		t.Helper()
		table := makeExprOptThreePartCompositeSortKeyTableDef()
		delete(table.Name2ColIndex, table.Pkey.PkeyColName)
		table.Cols[3].Name = catalog.CPrimaryKeyColName
		table.Cols[3].Hidden = true
		table.Name2ColIndex[catalog.CPrimaryKeyColName] = 3
		table.Pkey.PkeyColName, table.Pkey.CompPkeyCol = catalog.CPrimaryKeyColName, table.Cols[3]
		const tag = 10
		eq := func(pos int32, value int64) *pb.Expr {
			return makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, pos, table.Cols[pos].Name), value)
		}
		bind := func(name string, args ...*pb.Expr) *pb.Expr {
			out, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args)
			require.NoError(t, err)
			return out
		}
		node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag},
			FilterList: []*pb.Expr{eq(0, 1), bind("or", bind("and", eq(1, 2), eq(2, 3)), bind("and", eq(1, 4), eq(2, 5)))}}
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		builder.qry.Nodes = []*pb.Node{node}
		budget := uint64(maxCompositePointBytes)
		capture := &compositePointCapture{remaining: &budget}
		capture.collect(builder, 0, node)
		require.Contains(t, capture.proposals, int32(0))
		node.FilterList = builder.doMergeFiltersOnCompositeKey(true, table, tag, node.FilterList...)
		capture.snapshot()
		require.True(t, capture.proposals[0].valid(node))
		return builder, capture, node
	}

	t.Run("publish preserves baseline and is idempotent", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		requireFuncNames(t, node.FilterList, "prefix_eq", "or")
		capture.publish(builder, 0)
		requireFuncNames(t, node.FilterList, "prefix_eq", "or", "in")
		capture.publish(builder, 0)
		requireFuncNames(t, node.FilterList, "prefix_eq", "or", "in")
	})
	t.Run("missing witness falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.FilterList = node.FilterList[:1]
		capture.publish(builder, 0)
		require.Len(t, node.FilterList, 1)
	})
	t.Run("late rewrite cannot revive a rejected proof", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.FilterList = node.FilterList[:1]
		capture.revalidate(builder)
		capture.snapshot()
		capture.publish(builder, 0)
		require.NotContains(t, capture.proposals, int32(0))
		require.Len(t, node.FilterList, 1)
	})
	t.Run("owned late rewrite can refresh a valid proof", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		capture.revalidate(builder)
		// Late lowering adjusts selectivity while replacing a logical filter.
		node.FilterList[0].Selectivity = 1
		capture.snapshot()
		capture.publish(builder, 0)
		requireFuncNames(t, node.FilterList, "prefix_eq", "or", "in")
	})
	t.Run("changed binding falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.BindingTags[0]++
		capture.publish(builder, 0)
		require.Len(t, node.FilterList, 2)
	})
	t.Run("changed key metadata falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.TableDef.Cols[1].Typ.Id = int32(types.T_float64)
		capture.publish(builder, 0)
		require.Len(t, node.FilterList, 2)
	})
	t.Run("untracked scan copy falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		copyNode := *node
		builder.qry.Nodes = append(builder.qry.Nodes, &copyNode)
		capture.publish(builder, 1)
		require.Len(t, copyNode.FilterList, 2)
	})
	t.Run("total added filter remains eligible", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		col := makeExprOptInt64Col(10, 1, node.TableDef.Cols[1].Name)
		node.FilterList = append(node.FilterList, makeExprOptBinaryInt64Expr(t, ctx, ">", col, 0))
		capture.publish(builder, 0)
		requireFuncNames(t, node.FilterList, "prefix_eq", "or", ">", "in")
	})
	t.Run("volatile added filter falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		randExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "rand", nil)
		require.NoError(t, err)
		cmp, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), ">", []*pb.Expr{randExpr, makePlan2Float64ConstExprWithType(0)})
		require.NoError(t, err)
		node.FilterList = append(node.FilterList, cmp)
		capture.publish(builder, 0)
		require.Len(t, node.FilterList, 3)
	})
	t.Run("literal source is part of witness identity", func(t *testing.T) {
		a := MakePlan2Int64ConstExprWithType(1)
		a.GetLit().Src = &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 1}}}
		b := DeepCopyExpr(a)
		b.GetLit().Src.GetP().Pos = 2
		require.True(t, exprStructuralEqual(a, b))
		require.False(t, exprWireEqual(a, b))
	})
}
