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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestBitCountDerivedSubqueryProjectionBinding(t *testing.T) {
	for _, tc := range []struct {
		name    string
		selects string
		want    int32
	}{
		{
			name: "correlated projection materializes",
			selects: `select cast(e.sal as double) x,
				(select max(i.sal) from constraint_test.emp i where i.empno=e.empno) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "row-dependent wrapper materializes",
			selects: `select cast(e.sal as double) x,
				e.sal+(select max(i.sal) from constraint_test.emp i) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "volatile wrapper materializes",
			selects: `select cast(e.sal as double) x,
				rand()+(select max(i.sal) from constraint_test.emp i) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "uncorrelated subquery remains mergeable",
			selects: `select cast(e.sal as double) x,
				(select max(i.sal) from constraint_test.emp i) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "tableless scalar remains reducible",
			selects: `select cast(e.sal as double) x, (select rand()) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "WHERE-only subquery remains mergeable",
			selects: `select cast(e.sal as double) x from constraint_test.emp e
				where exists (select 1 from constraint_test.emp i where i.empno=e.empno)`,
			want: 16,
		},
		{
			name: "direct IN with literal RHS reduces",
			selects: `select cast(e.sal as double) x, e.empno in (select 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with NULL RHS reduces",
			selects: `select cast(e.sal as double) x, e.empno in (select null) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with volatile RHS reduces",
			selects: `select cast(e.sal as double) x, e.empno in (select rand()) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces nested scalar RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select (select 1)) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN retains nested physical scalar RHS",
			selects: `select cast(e.sal as double) x,
				e.empno in (select (select (select max(i.empno) from constraint_test.emp i))) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN keeps wrapped scalar RHS subquery",
			selects: `select cast(e.sal as double) x, e.empno in (select abs((select 1))) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN retains subquery through GROUP BY ordinal",
			selects: `select cast(e.sal as double) x,
				e.empno in (select abs((select 1)) group by 1) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN keeps filtered empty RHS subquery",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where false) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN reduces true-filtered RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1=1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces constant-true comparison filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 2>1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with pure string predicate",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 'a' like 'a') z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with pure numeric function",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where abs(-1) = 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with volatile filter retains subquery",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where rand() < 0.5) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN with numeric true filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with null test true filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1 is not null) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with boolean true filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1=1 or 1=2) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with NULL filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where null) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN reduces DISTINCT RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select distinct 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces grouped RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 group by 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces one-row VALUES RHS",
			selects: `select cast(e.sal as double) x, e.empno in (values row(1)) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN retains multirow VALUES RHS",
			selects: `select cast(e.sal as double) x, e.empno in (values row(1), row(2)) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "NOT IN remains a subquery",
			selects: `select cast(e.sal as double) x, e.empno not in (select 1) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "wrapped IN remains a subquery",
			selects: `select cast(e.sal as double) x, (e.empno in (select 1)) + 0 z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "equality ANY reduces like IN",
			selects: `select cast(e.sal as double) x, e.empno = any (select 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "non-equality ANY remains a subquery",
			selects: `select cast(e.sal as double) x, e.empno > any (select 1) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			queryPlan, err := runOneStmt(optimizer, t, "select bit_count(x) from ("+tc.selects+") s")
			require.NoError(t, err)
			fn := findPlanFunctionExpr(queryPlan, "bit_count")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, tc.want, overload)
		})
	}
}

func TestBitCountPredicateFoldUsesBindingContext(t *testing.T) {
	proc := newPlanTestProcess(t)
	compCtx := NewEmptyCompilerContext(proc)
	builder := &QueryBuilder{compCtx: compCtx}
	oldStatement, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = oldStatement

	text := makePlan2StringConstExprWithType("a")
	text.Typ.Charset = uint32(types.CharsetUTF8)
	like, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "like", []*Expr{text, DeepCopyExpr(text)})
	require.NoError(t, err)
	ctx := &BindContext{whereFilters: []*Expr{like}}
	require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx))
	require.NotNil(t, like.GetF(), "speculative folding must leave the bound predicate intact")

	compCtx.SetContext(oldStatement)
	require.False(t, bitCountWhereIsEffectivelyTrue(builder, ctx))
}

func TestBitCountPredicateProofHandlesLongPredicatesAndBoundedStrings(t *testing.T) {
	proc := newPlanTestProcess(t)
	builder := &QueryBuilder{compCtx: NewEmptyCompilerContext(proc)}

	filters := make([]*pb.Expr, 12)
	for i := range filters {
		text := makePlan2StringConstExprWithType(string(rune('a' + i)))
		filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "like", []*pb.Expr{text, DeepCopyExpr(text)})
		require.NoError(t, err)
		filters[i] = filter
	}

	ctx := &BindContext{}
	ctx.setWhereFilters(filters)
	require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "distinct eligible conjuncts must not share an arbitrary node cutoff")
	require.True(t, ctx.bitCountWhereProofKnown)
	require.True(t, ctx.bitCountWhereProofValue)
	require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "repeat probes should use the cached scope proof")
	proofContext := builder.GetContext()
	cancelledContext, cancel := context.WithCancel(proofContext)
	cancel()
	builder.compCtx.SetContext(cancelledContext)
	require.False(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "a cached proof must not bypass the current request cancellation")
	builder.compCtx.SetContext(proofContext)
	require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "restoring the active request context may reuse the proof")

	longDisjunction := DeepCopyExpr(filters[0])
	for _, filter := range filters[1:] {
		var err error
		longDisjunction, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "or", []*pb.Expr{longDisjunction, DeepCopyExpr(filter)})
		require.NoError(t, err)
	}
	ctx.setWhereFilters([]*pb.Expr{longDisjunction})
	require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "one large pure boolean tree must be admitted")
	ctx.setWhereFilters(append(append([]*pb.Expr(nil), filters...), makePlan2BoolConstExprWithType(false)))
	require.False(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "a false suffix must retain the materialization boundary")
	null := &pb.Expr{Typ: pb.Type{Id: int32(types.T_bool)}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Isnull: true}}}
	ctx.setWhereFilters(append(append([]*pb.Expr(nil), filters...), null))
	require.False(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "a NULL suffix must retain the materialization boundary")

	makeLengthPredicate := func(name string, size, want int64) *pb.Expr {
		space, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "space", []*pb.Expr{makePlan2Int64ConstExprWithType(size)})
		require.NoError(t, err)
		length, err := BindFuncExprImplByPlanExpr(builder.GetContext(), name, []*pb.Expr{space})
		require.NoError(t, err)
		predicate, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*pb.Expr{length, makePlan2Int64ConstExprWithType(want)})
		require.NoError(t, err)
		return predicate
	}
	ctx.setWhereFilters([]*pb.Expr{makeLengthPredicate("length", 65536, 65536)})
	require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "bounded 64 KiB string output should fold")
	largeSpace, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "space", []*pb.Expr{makePlan2Int64ConstExprWithType(64 << 20)})
	require.NoError(t, err)
	budget := &speculativeConstantFoldBudget{ctx: builder.GetContext()}
	require.False(t, isSpeculativeConstantFoldSafe(largeSpace, budget), "the allocation guard must reject a raw 64 MiB string")
	for _, name := range []string{"length", "char_length", "character_length", "octet_length", "bit_length"} {
		t.Run(name, func(t *testing.T) {
			const size = 64 << 20
			want := int64(size)
			if name == "bit_length" {
				want *= 8
			}
			predicate := makeLengthPredicate(name, size, want)
			ctx.setWhereFilters([]*pb.Expr{predicate})
			require.True(t, bitCountWhereIsEffectivelyTrue(builder, ctx), "SPACE length should fold symbolically")
			require.NotNil(t, predicate.GetF().Args[0].GetF().Args[0].GetF(), "the proof must leave the runtime predicate intact")
		})
	}
}

func TestBitCountStringMeasureBypassesAllocationBudget(t *testing.T) {
	builder := &QueryBuilder{compCtx: NewEmptyCompilerContext(newPlanTestProcess(t))}
	bindCtx := builder.GetContext()
	bind := func(name string, args ...*pb.Expr) *pb.Expr {
		t.Helper()
		expr, err := BindFuncExprImplByPlanExpr(bindCtx, name, args)
		require.NoError(t, err)
		return expr
	}
	const count = maxSpeculativeFoldValueBytes + 1
	intLit := makePlan2Int64ConstExprWithType
	strLit := makePlan2StringConstExprWithType
	producer := bind("concat", bind("repeat", strLit("é"), intLit(count)), strLit("👍"))
	require.False(t, isSpeculativeConstantFoldSafe(producer, &speculativeConstantFoldBudget{ctx: bindCtx}))
	for _, tc := range []struct {
		name string
		want int64
	}{
		{"length", 2*count + 4},
		{"octet_length", 2*count + 4},
		{"character_length", count + 1},
		{"bit_length", (2*count + 4) * 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			length := bind(tc.name, DeepCopyExpr(producer))
			proof := &BindContext{}
			for _, want := range []int64{tc.want, tc.want - 1} {
				predicate := bind("=", DeepCopyExpr(length), intLit(want))
				proof.setWhereFilters([]*pb.Expr{predicate})
				require.Equal(t, want == tc.want, bitCountWhereIsEffectivelyTrue(builder, proof))
				require.NotNil(t, predicate.GetF().Args[0].GetF(), "proof must preserve the original expression")
			}
		})
	}

	// An out-of-range unsigned input must not wrap to a negative count
	// during the proof and then become positive through unary minus.
	unsigned := makePlan2Uint64ConstExprWithType(^uint64(count - 1))
	signed, err := makePlan2CastExpr(bindCtx, unsigned, makePlan2Type(&types.Type{Oid: types.T_int64}))
	require.NoError(t, err)
	outOfRangeCast := bind("length", bind("repeat", strLit("a"), bind("unary_minus", signed)))
	foldConstantStringLengths(outOfRangeCast, bindCtx)
	require.NotNil(t, outOfRangeCast.GetF(), "proof must leave the checked cast to normal execution")

	oversize := bind("length", bind("repeat", strLit("aa"), intLit(1<<62)))
	foldConstantStringLengths(oversize, bindCtx)
	require.NotNil(t, oversize.GetF(), "the execution limit must still apply")
	cancelled, cancel := context.WithCancel(bindCtx)
	cancel()
	length := bind("length", producer)
	foldConstantStringLengths(length, cancelled)
	require.NotNil(t, length.GetF(), "cancelled proof must not rewrite")
}

func TestBitCountReverseProofPreservesStringDomains(t *testing.T) {
	builder := &QueryBuilder{compCtx: NewEmptyCompilerContext(newPlanTestProcess(t))}
	ctx := builder.GetContext()
	bind := func(name string, args ...*pb.Expr) *pb.Expr {
		t.Helper()
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	text := makePlan2StringConstExprWithType
	integer := makePlan2Int64ConstExprWithType
	const n = maxSpeculativeFoldValueBytes + 1
	large := bind("repeat", text("é"), integer(n))
	for _, tc := range []struct {
		metric string
		want   int64
	}{
		{"length", n * 2},
		{"character_length", n},
		{"bit_length", n * 16},
	} {
		t.Run(tc.metric, func(t *testing.T) {
			metric := bind(tc.metric, bind("reverse", DeepCopyExpr(large)))
			proof := &BindContext{}
			proof.setWhereFilters([]*pb.Expr{bind("=", metric, integer(tc.want))})
			require.True(t, bitCountWhereIsEffectivelyTrue(builder, proof))
			require.NotNil(t, metric.GetF(), "keep the execution predicate intact")
		})
	}

	// Text reversal can expand invalid bytes when it re-encodes runes.
	invalid := text(string([]byte{0xff}))
	metric := bind("length", bind("reverse", DeepCopyExpr(invalid)))
	foldConstantStringLengths(metric, ctx)
	require.NotNil(t, metric.GetF())

	legacyBinary := DeepCopyExpr(invalid)
	legacyBinary.GetLit().IsBin = true
	metric = bind("length", bind("reverse", legacyBinary))
	foldConstantStringLengths(metric, ctx)
	require.NotNil(t, metric.GetLit())
	require.Equal(t, int64(1), metric.GetLit().GetI64Val())

	staticBinary := text("é")
	staticBinary.Typ = makePlan2Type(&types.Type{Oid: types.T_blob, Charset: types.CharsetBinary})
	metric = bind("character_length", bind("reverse", staticBinary))
	foldConstantStringLengths(metric, ctx)
	require.NotNil(t, metric.GetLit())
	require.Equal(t, int64(2), metric.GetLit().GetI64Val())

	right := bind("right", DeepCopyExpr(invalid), integer(2))
	bound, safe := speculativeConstantFoldSafety(right, &speculativeConstantFoldBudget{ctx: ctx})
	require.True(t, safe)
	require.False(t, bound.validUTF8, "RIGHT can return the original invalid bytes")

	// The allocation check must apply the same text expansion bound for
	// operators that re-encode a selected rune range.
	const repeat = maxSpeculativeFoldValueBytes / 2
	for name, args := range map[string][]*pb.Expr{
		"reverse": nil, "substring": {integer(1)}, "left": {integer(repeat)}, "right": {integer(repeat)},
	} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				value *pb.Expr
				safe  bool
			}{
				{invalid, false},
				{text("a"), true},
			} {
				source := bind("repeat", DeepCopyExpr(tc.value), integer(repeat))
				expr := bind(name, append([]*pb.Expr{source}, args...)...)
				require.Equal(t, tc.safe, isSpeculativeConstantFoldSafe(expr, &speculativeConstantFoldBudget{ctx: ctx}))
			}
		})
	}
}

func TestBitCountMeasuresConstantStringSlices(t *testing.T) {
	builder := &QueryBuilder{compCtx: NewEmptyCompilerContext(newPlanTestProcess(t))}
	ctx := builder.GetContext()
	bind := func(name string, args ...*pb.Expr) *pb.Expr {
		t.Helper()
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	integer := makePlan2Int64ConstExprWithType
	text := makePlan2StringConstExprWithType
	const n = maxSpeculativeFoldValueBytes + 1
	space := bind("space", integer(n))
	uniform := bind("repeat", text("éø"), integer(n/2))
	mixed := bind("repeat", text("é😀"), integer(n/2))
	binary := text("é😀")
	binary.Typ = makePlan2Type(&types.Type{Oid: types.T_blob, Charset: types.CharsetBinary})
	for _, tc := range []struct {
		name    string
		metric  string
		value   *pb.Expr
		want    int64
		refused bool
	}{
		{name: "whole prefix", metric: "length", value: bind("left", DeepCopyExpr(space), integer(n)), want: n},
		{name: "suffix", metric: "length", value: bind("right", DeepCopyExpr(space), integer(17)), want: 17},
		{name: "negative start", metric: "octet_length", value: bind("mid", DeepCopyExpr(space), integer(-3), integer(9)), want: 3},
		{name: "extreme start", metric: "length", value: bind("substr", DeepCopyExpr(space), integer(-1<<63)), want: 0},
		{name: "reversed uniform runes", metric: "bit_length", value: bind("substring", bind("reverse", uniform), integer(2), integer(3)), want: 48},
		{name: "whole mixed-width value", metric: "length", value: bind("substring", DeepCopyExpr(mixed), integer(1)), want: 6 * (n / 2)},
		{name: "mixed-width characters", metric: "character_length", value: bind("substring", DeepCopyExpr(mixed), integer(-5), integer(4)), want: 4},
		{name: "partial mixed-width bytes unknown", metric: "length", value: bind("substring", mixed, integer(2), integer(3)), refused: true},
		{name: "binary bytes", metric: "character_length", value: bind("right", binary, integer(2)), want: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := bind(tc.metric, tc.value)
			foldConstantStringLengths(expr, ctx)
			if tc.refused {
				require.NotNil(t, expr.GetF(), "an upper bound must not become a byte count")
			} else {
				require.NotNil(t, expr.GetLit())
				require.Equal(t, tc.want, expr.GetLit().GetI64Val())
			}
		})
	}
}

func TestBitCountConditionalBinding(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	queryPlan, err := runOneStmt(optimizer, t, `select
		bit_count(case when e.empno=1 then cast(e.sal as double)+0 else cast(e.sal as double) end)
		from constraint_test.emp e`)
	require.NoError(t, err)
	selector := findPlanFunctionExpr(queryPlan, "case")
	require.NotNil(t, selector)
	require.Equal(t, int32(types.T_uint64), selector.Typ.Id)
	for i, want := range []int32{function.BitCountFloat64ExpressionOverload, 16} {
		arg := selector.GetF().GetArgs()[i+1]
		require.NotNil(t, arg.GetF())
		fid, overload := function.DecodeOverloadID(arg.GetF().GetFunc().GetObj())
		require.Equal(t, int32(function.BIT_COUNT), fid)
		require.Equal(t, want, overload)
	}
}

func TestBitCountMergedSelectorFilterReachesJoinInput(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	ctx := optimizer.CurrentContext().(*MockCompilerContext)
	table := ctx.tables["emp"]
	for _, col := range table.Cols {
		if col.Name == "sal" {
			// FLOAT field conversion has no row diagnostic and can be pushed
			// below the join. DOUBLE retains the normal diagnostic barrier.
			col.Typ = pb.Type{Id: int32(types.T_float32)}
		}
	}
	ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
	p, err := runOneStmt(optimizer, t, `select s.empno
		from (select e.empno, if(e.empno=1, e.sal, cast(e.sal as float)) as x
		      from constraint_test.emp e cross join constraint_test.emp d) s
		where bit_count(s.x)=2`)
	require.NoError(t, err)
	query := p.GetQuery()
	join := findNodeUnder(query, query.Steps[len(query.Steps)-1], pb.Node_JOIN)
	require.NotNil(t, join)
	filtered := 0
	for _, child := range join.Children {
		scan := findNodeUnder(query, child, pb.Node_TABLE_SCAN)
		require.NotNil(t, scan)
		for _, filter := range scan.FilterList {
			if exprCallsFunc(filter, "bit_count") {
				filtered++
			}
		}
	}
	require.Equal(t, 1, filtered, "the merged selector predicate should filter one scan before the cross join")
}

func TestBitCountCorrelatedScalarProjectionBinding(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want int32
	}{
		{
			name: "direct scalar field",
			sql:  `select bit_count((select e.sal)) from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct scalar explicit cast",
			sql:  `select bit_count((select cast(e.sal as double))) from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "reducible scalar does not materialize sibling projection",
			sql:  `select bit_count(x) from (select cast(e.sal as double) x, (select e.sal) z from constraint_test.emp e) s`,
			want: 16,
		},
		{
			name: "unreduced scalar field",
			sql:  `select bit_count((select i.sal from constraint_test.emp i limit 1))`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "IN subquery retains its dependent left operand",
			sql:  `select bit_count(x) from (select cast(e.sal as double) x, e.sal in (select i.sal from constraint_test.emp i) z from constraint_test.emp e) s`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "uncorrelated tableless scalar remains reducible",
			sql:  `select bit_count((select rand()))`,
			want: function.BitCountFloat64ExpressionOverload,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			salPos, ok := tableColumnPosition(table, "sal")
			require.True(t, ok)
			table.Cols[salPos].Typ.Id = int32(types.T_float64)
			queryPlan, err := runOneStmt(optimizer, t, tc.sql)
			require.NoError(t, err)
			fn := findPlanFunctionExpr(queryPlan, "bit_count")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, tc.want, overload)
		})
	}
}

func TestTableDumpReleasedBitCountBindings(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	const ddl = "create table t (d double not null, g bigint generated always as " +
		"(bit_count(cast(d as double)) + bit_count(cast(-d as double))) stored)"
	current := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, ddl))
	released := DeepCopyTableDef(current, true)
	var calls []*pb.Function
	require.NoError(t, pb.VisitExprTree(released.Cols[1].GeneratedCol.Expr, func(e *pb.Expr) error {
		if fn := e.GetF(); fn != nil && fn.Func.ObjName == "bit_count" {
			fid, overload := function.DecodeOverloadID(fn.Func.Obj)
			require.Equal(t, int32(function.BIT_COUNT), fid)
			require.Equal(t, int32(16), overload)
			// 4.2.8 stored BIT_COUNT overload 9 over CAST overload 0.
			fn.Func.Obj = fn.Func.Obj&^0xffffffff | 9
			cast := fn.Args[0].GetF()
			require.Equal(t, "cast", cast.Func.ObjName)
			cast.Func.Obj &^= 0xffffffff
			cast.SyntaxExplicitCast = false
			calls = append(calls, fn)
		}
		return nil
	}))
	require.Len(t, calls, 2)

	sensitive, err := AnalyzeTableDumpBindings(compiler, released, released)
	require.NoError(t, err)
	require.True(t, sensitive, "DUMP must carry the released bound expression")
	sensitive, err = AnalyzeTableDumpBindings(compiler, current, released)
	require.NoError(t, err)
	require.True(t, sensitive)
	calls[0].Func.Obj = calls[0].Func.Obj&^0xffffffff | 16
	calls[0].Args[0].GetF().Func.Obj |= 1
	_, err = AnalyzeTableDumpBindings(compiler, current, released)
	require.ErrorContains(t, err, "cannot verify bound generated expression", "mixed modes have no SQL binding")
}
