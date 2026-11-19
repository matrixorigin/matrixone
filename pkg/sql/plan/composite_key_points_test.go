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
	"errors"
	"strings"
	"testing"
	"time"

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
			got := compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes)
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
		candidate := compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes)
		require.Len(t, candidate.values, maxCompositePoints)
		charge, ok := candidate.candidateBytes(ctx.GetContext(), ctx.GetProcess(), maxCompositePointBytes-candidate.shapeBytes)
		require.True(t, ok)
		require.GreaterOrEqual(t, charge, uint64(maxCompositePoints*compositePointCopies)*types.DefaultPackerCapacity(), "each serial executor owns a packer")
		nested := arms[0]
		for _, arm := range arms[1:maxCompositePoints] {
			nested = or(nested, arm)
		}
		node.FilterList[1] = nested
		require.Len(t, compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes).values, maxCompositePoints, "the parser can produce a left-deep OR")
		node.FilterList[1] = or(arms...)
		require.Nil(t, compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes))
		node.FilterList = append(node.FilterList, or(point(200, 201), point(202, 203)))
		got := compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes)
		require.Len(t, got.values, 2, "an oversized alternative must not contaminate a later bounded one")
		require.EqualValues(t, 200, got.values[0][1].GetLit().GetI64Val())
		require.EqualValues(t, 202, got.values[1][1].GetLit().GetI64Val())
	})
	t.Run("query budget spans scans", func(t *testing.T) {
		filters := []*pb.Expr{eq(0, 1), or(first, second)}
		firstScan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: filters}
		secondScan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: filters}
		candidate := compositePointCandidate(ctx.GetContext(), firstScan, maxCompositePointBytes)
		require.NotNil(t, candidate)
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		builder.qry.Nodes = []*pb.Node{firstScan, secondScan}
		probeBudget := uint64(maxCompositePointBytes)
		probe := &compositePointCapture{remaining: &probeBudget}
		probe.collect(builder, 0, firstScan)
		require.Contains(t, probe.proposals, int32(0))
		// Reserve enough for the first frozen shape and metadata, but less
		// than the minimum packers required by a second scan.
		consumed := uint64(maxCompositePointBytes) - probeBudget
		budget := consumed + 2*compositePointCopies*types.DefaultPackerCapacity() - 1
		capture := &compositePointCapture{remaining: &budget}
		capture.collect(builder, 0, firstScan)
		capture.collect(builder, 1, secondScan)
		require.Contains(t, capture.proposals, int32(0))
		require.NotContains(t, capture.proposals, int32(1))

		budget = maxCompositePointBytes
		capture = &compositePointCapture{remaining: &budget}
		capture.collect(builder, 0, firstScan)
		capture.collect(builder, 1, secondScan)
		capture.snapshot()
		require.Len(t, capture.proposals, 2)
		before := budget
		require.True(t, capture.proposals[0].valid(firstScan, &budget))
		require.Less(t, budget, before, "comparison buffers share the query budget")
		afterFirst := budget
		require.True(t, capture.proposals[0].valid(firstScan, &budget))
		require.Equal(t, afterFirst, budget, "the same temporary reservation is not charged twice")
		require.True(t, capture.proposals[1].valid(secondScan, &budget))
		require.Less(t, budget, afterFirst, "a second scan reserves its own comparison work")
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
		got := compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes)
		require.NotNil(t, got, "%s", node.FilterList[0].String())
		require.Len(t, got.values, 50)
		node.FilterList[1] = or(arms[:2]...)
		node.FilterList[0] = stringEq(0, makePlan2StringConstExprWithType(strings.Repeat("x", 100_000)))
		nearLimit := compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes)
		require.NotNil(t, nearLimit)
		charge, ok := nearLimit.candidateBytes(ctx.GetContext(), ctx.GetProcess(), maxCompositePointBytes-nearLimit.shapeBytes)
		require.True(t, ok)
		require.Greater(t, charge, uint64(maxCompositePointBytes/2))
		require.LessOrEqual(t, charge, uint64(maxCompositePointBytes)-nearLimit.shapeBytes)

		// A folded value may be much larger than its declared type width.
		large := makePlan2StringConstExprWithType(strings.Repeat("x", 1<<20))
		large.Typ.Width = 1
		node.FilterList[0] = stringEq(0, large)
		largeCandidate := compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes)
		require.NotNil(t, largeCandidate)
		_, ok = largeCandidate.candidateBytes(ctx.GetContext(), ctx.GetProcess(), maxCompositePointBytes-largeCandidate.shapeBytes)
		require.False(t, ok, "actual folded bytes exceed the candidate budget")

		padded := stringEq(0, makePlan2StringConstExprWithType("a"))
		padded.GetF().Args[1].Typ.PadSpace = true
		node.FilterList[0] = padded
		require.Nil(t, compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes), "PAD SPACE provenance cannot use raw tuple equality")
		foreignCollation := stringEq(0, makePlan2StringConstExprWithType("a"))
		foreignCollation.GetF().Args[1].Typ.Charset = uint32(types.CharsetUTF8MB4UnicodeCI)
		node.FilterList[0] = foreignCollation
		require.Nil(t, compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes), "a different comparison domain cannot use the stored bytes")

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
		require.False(t, preparedBindingState(binding).valueDependent, "capturing a shape must not inspect parameter values")
		builder.qry.Nodes = []*pb.Node{node}
		capture.snapshot()
		proposal := capture.proposals[0]
		require.True(t, proposal.valid(node, &budget))
		budget = uint64(len(arms)*compositePointCopies)*types.DefaultPackerCapacity() - 1
		require.NoError(t, capture.publish(builder, 0))
		require.False(t, preparedBindingState(binding).valueDependent, "a late value-independent budget rejection retains cache eligibility")
		require.Len(t, node.FilterList, 2)

		budget = maxCompositePointBytes
		capture = &compositePointCapture{remaining: &budget}
		require.NoError(t, vector.SetBytesAt(params, 0, []byte(strings.Repeat("x", 1<<20)), proc.Mp()))
		capture.collect(builder, 0, node)
		capture.snapshot()
		require.NoError(t, capture.publish(builder, 0))
		require.True(t, preparedBindingState(binding).valueDependent, "a rejection after reading bound bytes must prevent stale fallback caching")
		require.Len(t, node.FilterList, 2)

		preparedBindingState(binding).valueDependent = false
		require.NoError(t, vector.SetBytesAt(params, 0, []byte("a"), proc.Mp()))
		budget = maxCompositePointBytes
		capture = &compositePointCapture{remaining: &budget}
		capture.collect(builder, 0, node)
		capture.snapshot()
		require.NoError(t, capture.publish(builder, 0))
		require.True(t, preparedBindingState(binding).valueDependent, "the bound values are relevant when a surviving candidate fits")

		t.Run("shared budget and prepared admission", func(t *testing.T) {
			// Both scans reserve their frozen shapes, witnesses, and temporary
			// comparison buffers before the first scan consumes its point charge.
			setup := func(initial uint64) (*QueryBuilder, *compositePointCapture, *pb.Node, *pb.Node, *uint64) {
				t.Helper()
				literalScan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: &stringTable, BindingTags: []int32{tag},
					FilterList: []*pb.Expr{stringEq(0, makePlan2StringConstExprWithType("a")), or(arms[:2]...)}}
				preparedScan := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: &stringTable, BindingTags: []int32{tag},
					FilterList: []*pb.Expr{stringEq(0, param), or(arms[:2]...)}}
				builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
				builder.qry.Nodes = []*pb.Node{literalScan, preparedScan, {NodeType: pb.Node_JOIN, Children: []int32{0, 1}}}
				remaining := initial
				capture := &compositePointCapture{remaining: &remaining}
				capture.collect(builder, 0, literalScan)
				capture.collect(builder, 1, preparedScan)
				capture.snapshot()
				require.Len(t, capture.proposals, 2)
				beforeComparison := remaining
				capture.revalidate(builder)
				require.Len(t, capture.proposals, 2)
				require.Less(t, remaining, beforeComparison, "both scans reserve comparison work")
				require.Positive(t, capture.proposals[0].comparisonBytes)
				require.Positive(t, capture.proposals[1].comparisonBytes)
				return builder, capture, literalScan, preparedScan, &remaining
			}

			preparedBindingState(binding).valueDependent = false
			_, probe, _, _, remaining := setup(maxCompositePointBytes)
			firstCharge, ok := probe.proposals[0].shape.candidateBytes(binding, proc, *remaining)
			require.True(t, ok)
			require.False(t, preparedBindingState(binding).valueDependent, "the first scan uses literals only")
			reserved := uint64(maxCompositePointBytes) - *remaining
			minimum := uint64(2*compositePointCopies) * types.DefaultPackerCapacity()

			for _, tc := range []struct {
				name      string
				remaining uint64
				value     string
				dependent bool
				admitted  bool
			}{
				{name: "below minimum", remaining: minimum - 1, value: strings.Repeat("x", 1<<20)},
				{name: "oversized after inspection", remaining: minimum, value: strings.Repeat("x", 1<<20), dependent: true},
				{name: "small with space", remaining: maxCompositePointBytes - reserved - firstCharge, value: "a", dependent: true, admitted: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					preparedBindingState(binding).valueDependent = false
					require.NoError(t, vector.SetBytesAt(params, 0, []byte(tc.value), proc.Mp()))
					builder, capture, firstScan, secondScan, left := setup(reserved + firstCharge + tc.remaining)
					require.NoError(t, capture.publish(builder, 2))
					require.Len(t, firstScan.FilterList, 3, "the first scan consumes its point charge")
					require.Equal(t, tc.dependent, preparedBindingState(binding).valueDependent)
					if tc.admitted {
						require.Len(t, secondScan.FilterList, 3)
					} else {
						require.Len(t, secondScan.FilterList, 2)
						require.Equal(t, tc.remaining, *left, "the second scan rejected without consuming point charge")
					}
				})
			}
		})
	})
	t.Run("float falls back", func(t *testing.T) {
		floatTable := *table
		floatTable.Cols = append([]*pb.ColDef(nil), table.Cols...)
		floatCol := *table.Cols[1]
		floatCol.Typ.Id = int32(types.T_float64)
		floatTable.Cols[1] = &floatCol
		node := &pb.Node{TableDef: &floatTable, BindingTags: []int32{tag}, FilterList: []*pb.Expr{eq(0, 1), or(first, second)}}
		require.Nil(t, compositePointCandidate(ctx.GetContext(), node, maxCompositePointBytes))
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

func TestCompositePointSerialBindingPreservesPreparedState(t *testing.T) {
	for _, tc := range []struct {
		name           string
		implicitCast   bool
		invalidType    bool
		valueDependent bool
	}{
		{name: "parameter"},
		{name: "implicit cast", implicitCast: true, valueDependent: true},
		{name: "invalid metadata", invalidType: true, valueDependent: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := types.T_varchar.ToType()
			ctx := withPreparedSourceBindings(context.Background(), []PreparedSourceBinding{{Position: 0, Type: typ}}, []any{
				ParamValue{Value: "abc", SourceType: typ, HasSourceType: true, EnableNumericPrefix: true},
			})
			param := &pb.Expr{Typ: makePlan2Type(&typ), Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
			value := param
			if tc.implicitCast {
				textType := types.T_text.ToType()
				param.Typ = makePlan2Type(&textType)
				target := types.New(types.T_varchar, 16, 0)
				var err error
				value, err = appendCastBeforeExpr(ctx, param, makePlan2Type(&target))
				require.NoError(t, err)
				require.True(t, compositePointScalar(value))
			}
			if tc.invalidType {
				value.Typ.Charset = 256
			}
			state := preparedBindingState(ctx)
			state.selectStatement = true
			state.valueDependent = tc.valueDependent
			state.diagnosticFree = false
			diagnostic := MakePlan2Int64ConstExprWithType(9)
			state.diagnosticCandidates = []*pb.Expr{diagnostic}
			state.decimalFloatProofs = []PreparedDecimalFloatProof{{position: 7}}
			temporal := &PreparedTemporalTypes{}
			state.temporalTypes, state.temporalTypesDirty = temporal, true
			field := &preparedFieldTypeBinding{dirty: true}
			state.fieldTypes = field
			state.fieldReturnSources = map[*pb.Expr]preparedFieldReturnSource{diagnostic: {}}

			_, err := BindFuncExprImplByPlanExpr(ctx, "serial", []*pb.Expr{value, MakePlan2Int64ConstExprWithType(1)})
			if tc.invalidType {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.valueDependent, state.valueDependent)
			require.False(t, state.diagnosticFree)
			require.Equal(t, []*pb.Expr{diagnostic}, state.diagnosticCandidates)
			require.Equal(t, []PreparedDecimalFloatProof{{position: 7}}, state.decimalFloatProofs)
			require.Same(t, temporal, state.temporalTypes)
			require.True(t, state.temporalTypesDirty)
			require.Same(t, field, state.fieldTypes)
			require.True(t, field.dirty)
			require.Equal(t, map[*pb.Expr]preparedFieldReturnSource{diagnostic: {}}, state.fieldReturnSources)
		})
	}
}

func TestCompositePointFrozenSources(t *testing.T) {
	for _, change := range []string{"none", "selected ordinal", "unselected ordinal", "unselected cast", "unselected overwidth", "literal provenance"} {
		t.Run(change, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			table := makeExprOptThreePartCompositeSortKeyTableDef()
			delete(table.Name2ColIndex, table.Pkey.PkeyColName)
			table.Cols[3].Name = catalog.CPrimaryKeyColName
			table.Cols[3].Hidden = true
			table.Name2ColIndex[catalog.CPrimaryKeyColName] = 3
			table.Pkey.PkeyColName, table.Pkey.CompPkeyCol = catalog.CPrimaryKeyColName, table.Cols[3]
			for i := range 3 {
				table.Cols[i].Typ = pb.Type{Id: int32(types.T_varchar), Width: 16, Charset: uint32(types.CharsetUTF8)}
			}
			const tag = 10
			typ := table.Cols[0].Typ
			param := func(pos int32) *pb.Expr {
				return &pb.Expr{Typ: typ, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: pos}}}
			}
			unselectedParam := param(0)
			textType := types.T_text.ToType()
			unselectedParam.Typ = makePlan2Type(&textType)
			cast, err := appendCastBeforeExpr(ctx.GetContext(), unselectedParam, typ)
			require.NoError(t, err)
			require.True(t, compositePointScalar(cast))
			selectedParam := param(1)
			literal := makePlan2StringConstExprWithType("a")
			eq := func(pos int32, value *pb.Expr) *pb.Expr {
				col := &pb.Expr{Typ: table.Cols[pos].Typ, Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: tag, ColPos: pos}}}
				expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*pb.Expr{col, value})
				require.NoError(t, err)
				expr.GetF().Args[1] = value
				return expr
			}
			bind := func(name string, args ...*pb.Expr) *pb.Expr {
				expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args)
				require.NoError(t, err)
				return expr
			}
			node := &pb.Node{NodeType: pb.Node_TABLE_SCAN, TableDef: table, BindingTags: []int32{tag}, FilterList: []*pb.Expr{
				eq(0, literal), eq(1, makePlan2StringConstExprWithType("b")),
				bind("or", bind("and", eq(1, cast), eq(2, selectedParam)), bind("and", eq(1, param(2)), eq(2, param(3)))),
			}}
			proc := ctx.GetProcess()
			params := vector.NewVec(types.T_text.ToType())
			defer params.Free(proc.Mp())
			bindings := make([]PreparedSourceBinding, 4)
			values := make([]any, 4)
			for i := range bindings {
				bindings[i] = PreparedSourceBinding{Position: int32(i), Type: types.T_varchar.ToType()}
				values[i] = "b"
				require.NoError(t, vector.AppendBytes(params, []byte("b"), false, proc.Mp()))
			}
			proc.SetPrepareParams(params)
			defer proc.SetPrepareParams(nil)
			ctx.SetContext(withPreparedSourceBindings(ctx.GetContext(), bindings, values))
			builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
			builder.qry.Nodes = []*pb.Node{node}
			budget := uint64(maxCompositePointBytes)
			capture := &compositePointCapture{remaining: &budget}
			capture.collect(builder, 0, node)
			require.Contains(t, capture.proposals, int32(0))
			capture.snapshot()
			proposal := capture.proposals[0]
			require.True(t, proposal.valid(node, capture.remaining))
			frozen := make([][]byte, 0, len(proposal.shape.checked))
			for _, value := range proposal.shape.checked {
				wire, err := value.Marshal()
				require.NoError(t, err)
				frozen = append(frozen, wire)
			}
			switch change {
			case "selected ordinal":
				selectedParam.GetP().Pos = 3
			case "unselected ordinal":
				unselectedParam.GetP().Pos = 2
			case "unselected cast":
				cast.Typ.Width++
			case "unselected overwidth":
				require.NoError(t, vector.SetBytesAt(params, 0, []byte(strings.Repeat("x", 17)), proc.Mp()))
			case "literal provenance":
				literal.GetLit().Src = param(0)
			}
			for i, value := range proposal.shape.checked {
				wire, err := value.Marshal()
				require.NoError(t, err)
				require.Equal(t, frozen[i], wire, "frozen source must not alias the scan")
			}
			require.NoError(t, capture.publish(builder, 0))
			if change == "none" {
				requireFuncNames(t, node.FilterList, "=", "=", "or", "in")
				require.True(t, preparedBindingState(ctx.GetContext()).valueDependent)
			} else if change == "unselected overwidth" {
				require.Len(t, node.FilterList, 3, "a fallible unselected cast must keep the baseline scan")
				require.True(t, preparedBindingState(ctx.GetContext()).valueDependent)
			} else {
				require.Len(t, node.FilterList, 3, "a changed witness must keep the baseline scan")
				require.False(t, preparedBindingState(ctx.GetContext()).valueDependent)
			}
		})
	}
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
		nullLast       bool
		indexed        bool
		valueDependent bool
	}{
		{name: "incomplete key", sql: "select payload from index_hint_t where a=? and ((b=? and payload=1) or (b=? and payload=2))", params: 3},
		{name: "unsafe residual", sql: complete + " and payload+1>0", params: 5},
		{name: "point candidate", sql: complete, params: 5, valueDependent: true},
		{name: "oversized candidate", sql: complete, params: 5, oversized: true, valueDependent: true},
		{name: "forced secondary", sql: "select a,b from index_hint_t force index(uk_ab) where a=? and b='abc' and ((b=? and id=?) or (b=? and id=?))", params: 5, indexed: true},
		{name: "automatic secondary", sql: "select a,b from index_hint_t where a=? and b='abc' and ((b=? and id=?) or (b=? and id=?))", params: 5, indexed: true},
		{name: "forced secondary oversized", sql: "select a,b from index_hint_t force index(uk_ab) where a=? and b='abc' and ((b=? and id=?) or (b=? and id=?))", params: 5, indexed: true, oversized: true},
		{name: "forced secondary null", sql: "select a,b from index_hint_t force index(uk_ab) where a=? and b='abc' and ((b=? and id=?) or (b=? and id=?))", params: 5, indexed: true, nullLast: true},
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
			if !tc.indexed {
				table.Indexes = nil
			}
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
				isNull := tc.nullLast && i == len(bindings)-1
				if !isNull {
					values[i] = value
				}
				require.NoError(t, vector.AppendBytes(params, []byte(value), isNull, proc.Mp()))
			}
			proc.SetPrepareParams(params)
			defer proc.SetPrepareParams(nil)
			result, err := BuildPreparedExecutionPlan(&mock.ctxt, statements[0], bindings, values)
			require.NoError(t, err)
			require.Equal(t, tc.valueDependent, result.ValueDependent)
			if tc.indexed {
				require.True(t, result.DiagnosticFree)
				require.Equal(t, "uk_ab", findFirstIndexScanName(result.Plan))
				for _, node := range result.Plan.GetQuery().Nodes {
					for _, filter := range node.FilterList {
						fn := filter.GetF()
						if fn == nil || fn.Func == nil || fn.Func.ObjName != "in" || len(fn.Args) != 2 {
							continue
						}
						col := fn.Args[0].GetCol()
						require.False(t, col != nil && strings.HasSuffix(col.Name, catalog.CPrimaryKeyColName), "an eliminated proposal must not publish points")
					}
				}
			}
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
			ObjRef: &pb.ObjectRef{Db: 1, Obj: 2}, ScanSnapshot: &pb.Snapshot{Tenant: &pb.SnapshotTenant{TenantID: 3}},
			FilterList: []*pb.Expr{eq(0, 1), bind("or", bind("and", eq(1, 2), eq(2, 3)), bind("and", eq(1, 4), eq(2, 5)))}}
		builder := NewQueryBuilder(pb.Query_SELECT, ctx, false, false)
		builder.qry.Nodes = []*pb.Node{node}
		budget := uint64(maxCompositePointBytes)
		capture := &compositePointCapture{remaining: &budget}
		capture.collect(builder, 0, node)
		require.Contains(t, capture.proposals, int32(0))
		node.FilterList = builder.doMergeFiltersOnCompositeKey(true, table, tag, node.FilterList...)
		capture.snapshot()
		require.True(t, capture.proposals[0].valid(node, capture.remaining))
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
	t.Run("block filtering disabled", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		builder.optimizerHints = &OptimizerHints{blockFilter: 2}
		require.NoError(t, capture.publish(builder, 0))
		requireFuncNames(t, node.FilterList, "prefix_eq", "or", "in")
		require.Empty(t, node.BlockFilterList)
	})
	t.Run("missing witness falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.FilterList = node.FilterList[:1]
		capture.publish(builder, 0)
		require.Len(t, node.FilterList, 1)
	})
	t.Run("duplicate witnesses require distinct matches", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.FilterList = append(node.FilterList, DeepCopyExpr(node.FilterList[0]))
		capture.snapshot()
		node.FilterList = node.FilterList[:2]
		require.NoError(t, capture.publish(builder, 0))
		require.Len(t, node.FilterList, 2)
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
	t.Run("changed relation falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.ObjRef.Obj++
		require.NoError(t, capture.publish(builder, 0))
		require.Len(t, node.FilterList, 2)
	})
	t.Run("changed snapshot falls back", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.ScanSnapshot.Tenant.TenantID++
		require.NoError(t, capture.publish(builder, 0))
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
	t.Run("late cancellation propagates", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		original := ctx.GetContext()
		cancelled, cancel := context.WithCancelCause(original)
		cause := errors.New("planning stopped")
		cancel(cause)
		ctx.SetContext(cancelled)
		defer ctx.SetContext(original)
		require.ErrorIs(t, capture.publish(builder, 0), cause)
		require.Len(t, node.FilterList, 2)
	})
	t.Run("late deadline propagates", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		original := ctx.GetContext()
		expired, cancel := context.WithDeadline(original, time.Unix(1, 0))
		defer cancel()
		ctx.SetContext(expired)
		defer ctx.SetContext(original)
		require.ErrorIs(t, capture.publish(builder, 0), context.DeadlineExceeded)
		require.Len(t, node.FilterList, 2)
	})
	t.Run("cancellation after rejected proof propagates", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		node.FilterList = node.FilterList[:1]
		capture.revalidate(builder)
		require.Empty(t, capture.proposals)
		original := ctx.GetContext()
		cancelled, cancel := context.WithCancelCause(original)
		cause := errors.New("planning stopped after proof")
		cancel(cause)
		ctx.SetContext(cancelled)
		defer ctx.SetContext(original)
		require.ErrorIs(t, capture.publish(builder, 0), cause)
	})
	t.Run("ordinary bind failure keeps the fallback", func(t *testing.T) {
		builder, capture, node := newProposal(t)
		// An empty tuple cannot be produced by valid key analysis; it makes
		// the optional SERIAL binder fail without changing the scan witness.
		capture.proposals[0].shape.values[0] = nil
		original := ctx.GetContext()
		binding := withPreparedSourceBindings(original, nil)
		state := preparedBindingState(binding)
		state.valueDependent = true
		diagnostic := MakePlan2Int64ConstExprWithType(9)
		state.diagnosticCandidates = []*pb.Expr{diagnostic}
		ctx.SetContext(binding)
		defer ctx.SetContext(original)
		require.NoError(t, capture.publish(builder, 0))
		require.Len(t, node.FilterList, 2)
		require.True(t, state.valueDependent)
		require.Equal(t, []*pb.Expr{diagnostic}, state.diagnosticCandidates)
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
