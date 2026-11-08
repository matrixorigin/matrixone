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
	"strings"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCompositePointCandidate(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	ctx.GetProcessFunc = func() *process.Process { return proc }
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
		{"unproven residual", []*pb.Expr{eq(0, 1), or(first, second), rangeFilter}, nil},
		{"dynamic folded literal", []*pb.Expr{eq(0, 1), or(first, and(dynamicLiteral, eq(2, 5)))}, nil},
		{"existing single point", []*pb.Expr{eq(0, 1), eq(1, 2), eq(2, 3), or(eq(1, 2), eq(1, 4))}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := DeepCopyExprList(tc.filters)
			node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: tc.filters}
			got := compositePointCandidate(node)
			if tc.want == nil {
				require.Nil(t, got)
			} else {
				require.Len(t, got, len(tc.want))
				for i := range got {
					for j := range got[i] {
						require.Equal(t, tc.want[i][j], got[i][j].GetLit().GetI64Val())
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
		require.Len(t, compositePointCandidate(node), maxCompositePoints)
		nested := arms[0]
		for _, arm := range arms[1:maxCompositePoints] {
			nested = or(nested, arm)
		}
		node.FilterList[1] = nested
		require.Len(t, compositePointCandidate(node), maxCompositePoints, "the parser can produce a left-deep OR")
		node.FilterList[1] = or(arms...)
		require.Nil(t, compositePointCandidate(node))
	})
	t.Run("encoded byte budget", func(t *testing.T) {
		folded := makePlan2StringConstExprWithType("ab\x00cd")
		folded.Typ.Width = 1 // A folded function's declared width can understate its value.
		bound, ok := compositePointValueSize(folded)
		require.True(t, ok)
		require.EqualValues(t, 13, bound)
		stringTable := *table
		stringTable.Cols = append([]*pb.ColDef(nil), table.Cols...)
		for i := 0; i < 3; i++ {
			col := *table.Cols[i]
			col.Typ = pb.Type{Id: int32(types.T_varchar), Width: 16}
			stringTable.Cols[i] = &col
		}
		stringEq := func(pos int32) *pb.Expr {
			col := &pb.Expr{Typ: stringTable.Cols[pos].Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: tag, ColPos: pos}}}
			param := &pb.Expr{Typ: pb.Type{Id: int32(types.T_text)}, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: pos + 1}}}
			value, err := appendCastBeforeExpr(ctx.GetContext(), param, pb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen})
			require.NoError(t, err)
			expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{col, value})
			require.NoError(t, err)
			return expr
		}
		arms := make([]*pb.Expr, 55)
		for i := range arms {
			arms[i] = and(stringEq(1), stringEq(2))
		}
		node := &pb.Node{TableDef: &stringTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{stringEq(0), or(arms[:50]...)}}
		require.Len(t, compositePointCandidate(node), 50)
		node.FilterList[1] = or(arms...)
		require.Nil(t, compositePointCandidate(node), "unbounded candidate payload must fall back")

		// Share one folded literal among 50 arms. Its small type width must not
		// hide the much larger tuple data that serialization would duplicate.
		large := makePlan2StringConstExprWithType(strings.Repeat("x", 1<<20))
		large.Typ.Width = 1
		outer := stringEq(0)
		outer.GetF().Args[1] = large
		node.FilterList = []*pb.Expr{outer, or(arms[:50]...)}
		require.Nil(t, compositePointCandidate(node), "actual folded bytes exceed the candidate budget")
	})
	t.Run("float falls back", func(t *testing.T) {
		floatTable := *table
		floatTable.Cols = append([]*pb.ColDef(nil), table.Cols...)
		floatCol := *table.Cols[1]
		floatCol.Typ.Id = int32(types.T_float64)
		floatTable.Cols[1] = &floatCol
		node := &pb.Node{TableDef: &floatTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(first, second)}}
		require.Nil(t, compositePointCandidate(node))
	})

	for _, indexed := range []bool{false, true} {
		t.Run(map[bool]string{false: "without index", true: "with index"}[indexed], func(t *testing.T) {
			testTable := *table
			if indexed {
				testTable.Indexes = []*pb.IndexDef{{IndexName: "idx", TableExist: true}}
			}
			original := []*pb.Expr{eq(0, 1), or(point(2, 3), point(4, 5))}
			before := DeepCopyExprList(original)
			builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
			builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, TableDef: &testTable, BindingTags: []int32{tag}, FilterList: original}}
			deferred := make(map[int32]struct{})
			for range 2 {
				builder.mergeFiltersOnCompositeKey(0, true, deferred)
			}
			node := builder.qry.Nodes[0]
			require.Len(t, node.FilterList, len(before))
			for i := range before {
				require.True(t, proto.Equal(before[i], node.FilterList[i]))
			}
			builder.addCompositePointCandidates(0, deferred)
			builder.mergeFiltersOnCompositeKey(0, false, nil)
			builder.addCompositePointCandidates(0, deferred)
			require.Len(t, node.FilterList, len(before)+1)
			requireCompositeKeyInFilter(t, node.FilterList[len(before)], "in", 3, 2, false)
			require.Len(t, node.BlockFilterList, 1)
			require.NotSame(t, node.FilterList[len(before)], node.BlockFilterList[0])
			for i := range before {
				require.True(t, proto.Equal(before[i], node.FilterList[i]), "the SQL residual must remain intact")
			}
			// DML recalculates scan statistics after createQuery. The inferred
			// key set must not be counted as an independent selectivity.
			node.FilterList[0].Selectivity, node.FilterList[1].Selectivity = 0.4, 0.2
			without := estimateExprSelectivity(colexec.RewriteFilterExprList(node.FilterList[:len(before)]), builder, nil)
			with := estimateExprSelectivity(colexec.RewriteFilterExprList(node.FilterList), builder, nil)
			require.InDelta(t, without, with, 1e-12)
		})
	}
	t.Run("deferred fallback without candidate", func(t *testing.T) {
		testTable := *table
		testTable.Indexes = []*pb.IndexDef{{IndexName: "idx", TableExist: true}}
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, TableDef: &testTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(point(2, 3), point(4, 5))}}}
		builder.mergeFiltersOnCompositeKey(0, true, make(map[int32]struct{}))
		// If candidate construction cannot publish a filter, the existing late
		// index phase must still lower the shared primary-key prefix.
		builder.mergeFiltersOnCompositeKey(0, false, nil)
		requireFuncNames(t, builder.qry.Nodes[0].FilterList, "prefix_eq", "or")
	})
	for _, indexed := range []bool{false, true} {
		name := "unindexed"
		if indexed {
			name = "indexed"
		}
		t.Run("candidate invalidated after deferral/"+name, func(t *testing.T) {
			testTable := *table
			if indexed {
				testTable.Indexes = []*pb.IndexDef{{IndexName: "idx", TableExist: true}}
			}
			builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
			builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, TableDef: &testTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(point(2, 3), point(4, 5))}}}
			deferred := make(map[int32]struct{})
			builder.mergeFiltersOnCompositeKey(0, true, deferred)
			require.Contains(t, deferred, int32(0))
			// Join planning can deduce an equality between two columns. It is
			// safe SQL, but outside the candidate analyzer's admitted predicates.
			col1 := makeExprOptInt64Col(tag, 1, table.Cols[1].Name)
			col2 := makeExprOptInt64Col(tag, 2, table.Cols[2].Name)
			joinFact, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{col1, col2})
			require.NoError(t, err)
			node := builder.qry.Nodes[0]
			node.FilterList = append(node.FilterList, joinFact)
			require.True(t, builder.addCompositePointCandidates(0, deferred))
			if indexed {
				// Logical key columns remain available until index selection.
				requireNoFuncNames(t, node.FilterList, "prefix_eq")
				builder.mergeFiltersOnCompositeKey(0, false, nil)
			}
			requireFuncNames(t, node.FilterList, "prefix_eq", "or", "=")
		})
	}
	t.Run("block filtering disabled", func(t *testing.T) {
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		builder.optimizerHints = &OptimizerHints{blockFilter: 2}
		builder.qry.Nodes = []*pb.Node{{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(point(2, 3), point(4, 5))}}}
		builder.addCompositePointCandidates(0, nil)
		require.Len(t, builder.qry.Nodes[0].FilterList, 3)
		require.Empty(t, builder.qry.Nodes[0].BlockFilterList)
	})
}

func TestCompositePointJoinDerivedPredicateKeepsPrefix(t *testing.T) {
	mock := NewMockOptimizer(true)
	proc := mock.ctxt.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	mock.ctxt.GetProcessFunc = func() *process.Process { return proc }
	addPrimaryPrefixIndexTableForTest(mock)
	mock.ctxt.tables["index_hint_t"].Indexes = nil
	query, err := runOneStmt(mock, t, `select t.id from index_hint_t t join index_hint_t u
		on t.b=u.b and t.id=u.b where t.a=1 and
		((t.id=1 and t.b=1) or (t.id=2 and t.b=2))`)
	require.NoError(t, err)
	var prefix, derived bool
	for _, node := range query.GetQuery().Nodes {
		if node.NodeType != pb.Node_TABLE_SCAN {
			continue
		}
		for _, filter := range node.FilterList {
			fn := filter.GetF()
			if fn == nil || fn.Func == nil || len(fn.Args) != 2 {
				continue
			}
			left, right := fn.Args[0].GetCol(), fn.Args[1].GetCol()
			if left != nil && strings.HasPrefix(left.Name, "t.") {
				prefix = prefix || fn.Func.ObjName == "prefix_eq" && strings.Contains(left.Name, catalog.CPrimaryKeyColName)
				derived = derived || fn.Func.ObjName == "=" && right != nil && strings.HasPrefix(right.Name, "t.")
			}
		}
	}
	require.True(t, derived, "the join must actually derive the unsupported scan predicate")
	require.True(t, prefix, "the base scan must retain its primary-key prefix independently of runtime filters")
}

func TestCompositeKeyLoweringKeepsFloatEquality(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	proc := ctx.GetProcess()
	t.Cleanup(func() { proc.Free(); proc.GetFileService().Close(proc.Ctx) })
	ctx.GetProcessFunc = func() *process.Process { return proc }
	for _, floatPos := range []int32{0, 1} {
		t.Run(map[int32]string{0: "leading", 1: "after prefix"}[floatPos], func(t *testing.T) {
			table := makeExprOptThreePartCompositeSortKeyTableDef()
			table.Cols[floatPos].Typ = pb.Type{Id: int32(types.T_float64)}
			builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
			tag := builder.genNewBindTag()
			var filters []*pb.Expr
			for pos := int32(0); pos < 3; pos++ {
				if pos == floatPos {
					col := &pb.Expr{Typ: table.Cols[pos].Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: tag, ColPos: pos}}}
					expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{col, makePlan2Float64ConstExprWithType(0)})
					require.NoError(t, err)
					filters = append(filters, expr)
				} else {
					filters = append(filters, makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, pos, table.Cols[pos].Name), int64(pos)))
				}
			}
			result := builder.doMergeFiltersOnCompositeKey(true, table, tag, filters...)
			require.Len(t, result, 3)
			if floatPos == 0 {
				require.Same(t, filters[0], result[0])
				requireNoFuncNames(t, result, "prefix_eq")
			} else {
				require.Equal(t, "prefix_eq", result[0].GetF().Func.ObjName)
				require.Same(t, filters[1], result[1])
			}
		})
	}
}
