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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// enumNumericMockContext rebinds the mock table single_idx_t (columns id,
// val and rowid) to the requested column types, so the planner binds against
// real column references instead of folded constants. The pattern follows
// async_datalink_test.go: mutate the mock TableDef through ctx.tables before
// BuildPlan.
func enumNumericMockContext(t *testing.T, idType, valType planpb.Type) *MockCompilerContext {
	t.Helper()
	ctx := NewMockOptimizer(false, newPlanTestProcess(t)).CurrentContext().(*MockCompilerContext)
	table := ctx.tables["single_idx_t"]
	require.NotNil(t, table, "mock schema table single_idx_t must exist")
	table.Cols[0].Typ = idType
	table.Cols[1].Typ = valType
	table.Indexes = nil
	return ctx
}

func buildPlanForEnumNumericQuery(t *testing.T, ctx *MockCompilerContext, sql string) *planpb.Query {
	t.Helper()
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err, "%s", sql)
	defer stmts[0].Free()
	p, err := BuildPlan(ctx, stmts[0], false, nil)
	require.NoError(t, err, "%s", sql)
	return p.GetQuery()
}

// enumNumericProjectNode returns the projection node BuildPlan produced for
// these single-table selects. q.Steps[0] is the last pipeline step (the
// PROJECT node here), the same convention bit_integer_arguments_test.go uses
// to select bound expressions; selecting the step node instead of
// aggregating every node's ProjectList keeps the assertions tied to the real
// plan shape rather than an assumed node count.
func enumNumericProjectNode(t *testing.T, q *planpb.Query) *planpb.Node {
	t.Helper()
	require.NotEmpty(t, q.Steps, "query must have a step list")
	node := q.Nodes[q.Steps[0]]
	require.Equal(t, planpb.Node_PROJECT, node.NodeType,
		"the top step of these selects must be the projection node")
	return node
}

// enumNumericFilterNodes returns the nodes BuildPlan actually attached
// predicates to. Round-5 plan evidence shows BuildPlan retains a WHERE
// clause on more than one consumer — the TABLE_SCAN node keeps the pushed
// down predicates and the FILTER node keeps them too — with an AND bound
// as separate FilterList roots rather than one shared root. Selecting
// every node with a non-empty FilterList keeps the test aligned with the
// real plan structure; the downstream assertions verify each retained
// consumer and each root instead of assuming a single-node shape.
func enumNumericFilterNodes(q *planpb.Query) []*planpb.Node {
	var nodes []*planpb.Node
	for _, node := range q.Nodes {
		if len(node.FilterList) > 0 {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// requireUint16OperandPair asserts that a bound binary operator holds both
// operands in one executable uint16 domain. On the unfixed revision the
// ENUM column reference kept T_enum here, so `id = val` bound mismatched
// vectors that panicked in the comparison kernel ("casting SMALLINT
// UNSIGNED vector to []types.Enum"), while `id < val` and the arithmetic
// forms failed to bind at all ("bad value [ENUM SMALLINT UNSIGNED]").
func requireUint16OperandPair(t *testing.T, expr *planpb.Expr, context string) {
	t.Helper()
	f := expr.GetF()
	require.NotNil(t, f, "%s", context)
	require.Len(t, f.Args, 2, "%s", context)
	require.Equal(t, int32(types.T_uint16), f.Args[0].Typ.Id, "%s left operand", context)
	require.Equal(t, int32(types.T_uint16), f.Args[1].Typ.Id, "%s right operand", context)
}

// countCastsToUint16 counts implicit cast function nodes materialized into
// the uint16 domain inside the expression tree (makePlan2CastExpr builds
// implicit operator casts as "cast" function nodes carrying the cast target
// as the expression type).
func countCastsToUint16(exprs ...*planpb.Expr) int {
	count := 0
	var visit func(*planpb.Expr)
	visit = func(e *planpb.Expr) {
		if e == nil {
			return
		}
		if f := e.GetF(); f != nil {
			if f.Func.GetObjName() == "cast" && e.Typ.Id == int32(types.T_uint16) {
				count++
			}
			for _, arg := range f.Args {
				visit(arg)
			}
		}
	}
	for _, e := range exprs {
		visit(e)
	}
	return count
}

// countUint16OperandPairs counts binary operators whose two operands are both
// bound in the uint16 domain. Cast nodes themselves never match: their first
// argument is the still-unconverted source operand.
func countUint16OperandPairs(expr *planpb.Expr) int {
	count := 0
	var visit func(*planpb.Expr)
	visit = func(e *planpb.Expr) {
		if e == nil {
			return
		}
		if f := e.GetF(); f != nil {
			if len(f.Args) == 2 &&
				f.Args[0].Typ.Id == int32(types.T_uint16) &&
				f.Args[1].Typ.Id == int32(types.T_uint16) {
				count++
			}
			for _, arg := range f.Args {
				visit(arg)
			}
		}
	}
	visit(expr)
	return count
}

// TestEnumSmallintUnsignedPlanCoercion walks the issue #31901 repro through
// the real planner against a mock ENUM column (id) and SMALLINT UNSIGNED
// column (val):
//
//	create table t (e enum('a','b','c'), s smallint unsigned, ...);
//	select e = s, s = e, e <=> s from t;
//	select * from t where e = s and e < s;
//	select e + s, s * e, e - s, e % s from t;
//
// Every form must build one executable uint16 domain with a real implicit
// cast of the ENUM side, in both SELECT and WHERE positions and both operand
// orders.
func TestEnumSmallintUnsignedPlanCoercion(t *testing.T) {
	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b,c"}
	u16Type := planpb.Type{Id: int32(types.T_uint16)}

	t.Run("select both orders and null-safe equality", func(t *testing.T) {
		ctx := enumNumericMockContext(t, enumType, u16Type)
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select id = val, val = id, id <=> val from constraint_test.single_idx_t")
		projects := enumNumericProjectNode(t, q).ProjectList
		require.Len(t, projects, 3)
		for i, expr := range projects {
			context := fmt.Sprintf("projection %d", i)
			requireUint16OperandPair(t, expr, context)
			require.Equal(t, int32(types.T_bool), expr.Typ.Id, "%s result type", context)
			require.Equal(t, 1, countCastsToUint16(expr),
				"%s must cast exactly the ENUM operand into uint16", context)
		}
	})

	t.Run("where filter keeps both novel operators executable", func(t *testing.T) {
		ctx := enumNumericMockContext(t, enumType, u16Type)
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select * from constraint_test.single_idx_t where id = val and id < val")
		filterNodes := enumNumericFilterNodes(q)
		require.NotEmpty(t, filterNodes,
			"the WHERE clause must stay attached to at least one plan node")
		// Round-5 plan evidence: BuildPlan retains this WHERE on both the
		// TABLE_SCAN node (pushed down) and the FILTER node, and binds the
		// AND as two separate FilterList roots (= and <) on each. Verify
		// every retained consumer instead of assuming one node with one
		// shared root: each filter-bearing node must carry both operators,
		// each with the ENUM operand cast exactly once into uint16 and
		// both operands held in the uint16 domain.
		for i, node := range filterNodes {
			context := fmt.Sprintf("filter node %d (%s)", i, node.NodeType)
			require.Len(t, node.FilterList, 2,
				"%s must carry the = and < predicates as separate roots", context)
			seen := map[string]bool{}
			for j, root := range node.FilterList {
				rootContext := fmt.Sprintf("%s root %d", context, j)
				f := root.GetF()
				require.NotNil(t, f, "%s", rootContext)
				seen[f.Func.GetObjName()] = true
				require.Equal(t, 1, countCastsToUint16(root),
					"%s must cast exactly the ENUM operand into uint16", rootContext)
				require.Equal(t, 1, countUint16OperandPairs(root),
					"%s must hold both operands in uint16", rootContext)
			}
			require.True(t, seen["="], "%s must keep the equality predicate", context)
			require.True(t, seen["<"], "%s must keep the ordering predicate", context)
			require.Len(t, seen, 2, "%s must not gain unexpected operators", context)
		}
	})

	t.Run("arithmetic projections keep the ordinal domain", func(t *testing.T) {
		ctx := enumNumericMockContext(t, enumType, u16Type)
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select id + val, val * id, id - val, id % val from constraint_test.single_idx_t")
		projects := enumNumericProjectNode(t, q).ProjectList
		require.Len(t, projects, 4)
		for i, expr := range projects {
			context := fmt.Sprintf("projection %d", i)
			requireUint16OperandPair(t, expr, context)
			require.Equal(t, 1, countCastsToUint16(expr),
				"%s must cast exactly the ENUM operand into uint16", context)
		}
		// -, * and % share the same overload-0 result contract as + (their
		// retType closures return the first target parameter outside the
		// decimal and date/datetime/year special cases), so all four
		// arithmetic forms contract to the uint16 ordinal domain.
		require.Equal(t, int32(types.T_uint16), projects[0].Typ.Id, "id + val result domain")
		require.Equal(t, int32(types.T_uint16), projects[1].Typ.Id, "val * id result domain")
		require.Equal(t, int32(types.T_uint16), projects[2].Typ.Id, "id - val result domain")
		require.Equal(t, int32(types.T_uint16), projects[3].Typ.Id, "id %% val result domain")
	})

	t.Run("enum to enum different member sets and orders keep label comparison", func(t *testing.T) {
		// The planner binds ENUM/ENUM comparisons in the VARCHAR label
		// domain: attempt-1 RED evidence (overlay baseline) showed both
		// operands bound as T_varchar, not raw T_enum sentinels. The
		// preservation contract is that these comparisons stay in the
		// string domain with no uint16-domain casts, for a different
		// member set and for the same members in a different order.
		for _, members := range []string{"x,y", "c,b,a"} {
			ctx := enumNumericMockContext(t, enumType,
				planpb.Type{Id: int32(types.T_enum), Enumvalues: members})
			q := buildPlanForEnumNumericQuery(t, ctx,
				"select id = val from constraint_test.single_idx_t")
			projects := enumNumericProjectNode(t, q).ProjectList
			require.Len(t, projects, 1, "%s", members)
			expr := projects[0]
			f := expr.GetF()
			require.NotNil(t, f, "%s", members)
			require.Len(t, f.Args, 2, "%s", members)
			require.Equal(t, int32(types.T_varchar), f.Args[0].Typ.Id,
				"%s left operand stays in the label domain", members)
			require.Equal(t, int32(types.T_varchar), f.Args[1].Typ.Id,
				"%s right operand stays in the label domain", members)
			require.Equal(t, 0, countCastsToUint16(expr),
				"ENUM/ENUM comparison must not be forced into the numeric domain (%s)", members)
		}
	})

	t.Run("plain smallint unsigned pair stays cast free", func(t *testing.T) {
		ctx := enumNumericMockContext(t, u16Type, u16Type)
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select id = val from constraint_test.single_idx_t")
		projects := enumNumericProjectNode(t, q).ProjectList
		require.Len(t, projects, 1)
		expr := projects[0]
		requireUint16OperandPair(t, expr, "plain uint16 pair")
		require.Equal(t, 0, countCastsToUint16(expr),
			"an equal-type uint16 pair must not gain a useless cast")
	})

	t.Run("enum with tinyint unsigned keeps its widening route", func(t *testing.T) {
		ctx := enumNumericMockContext(t, enumType, planpb.Type{Id: int32(types.T_uint8)})
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select id = val from constraint_test.single_idx_t")
		projects := enumNumericProjectNode(t, q).ProjectList
		require.Len(t, projects, 1)
		expr := projects[0]
		requireUint16OperandPair(t, expr, "enum to tinyint unsigned pair")
		require.GreaterOrEqual(t, countCastsToUint16(expr), 1,
			"the pre-existing widening route must keep casting both operands to uint16")
	})

	t.Run("enum projection keeps display type and member metadata", func(t *testing.T) {
		// The planner display-normalizes a projected ENUM column into the
		// VARCHAR domain (attempt-1 RED evidence: plain `select id` projected
		// as T_varchar). The preservation contract here is output-side and
		// separate from the mutated mock input: the projection stays in the
		// display string domain, gains no numeric uint16 cast, and whenever
		// the binding retains the raw enum column reference inside the
		// display wrapper, that reference keeps the declared member list.
		ctx := enumNumericMockContext(t,
			planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b,c"}, u16Type)
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select id from constraint_test.single_idx_t")
		projects := enumNumericProjectNode(t, q).ProjectList
		require.Len(t, projects, 1)
		require.Equal(t, int32(types.T_varchar), projects[0].Typ.Id,
			"the projected enum column is display-normalized to varchar")
		require.Equal(t, 0, countCastsToUint16(projects[0]),
			"the projection must not be forced into the numeric domain")
		var inner *planpb.Expr
		var collect func(e *planpb.Expr)
		collect = func(e *planpb.Expr) {
			if e == nil || inner != nil {
				return
			}
			if col := e.GetCol(); col != nil && e.Typ.Id == int32(types.T_enum) {
				inner = e
				return
			}
			if fn := e.GetF(); fn != nil {
				for _, arg := range fn.Args {
					collect(arg)
				}
			}
		}
		collect(projects[0])
		require.NotNil(t, inner,
			"the display-normalized projection must retain the raw enum column reference")
		require.Equal(t, "a,b,c", inner.Typ.Enumvalues,
			"the retained raw enum reference keeps its member metadata")
	})

	t.Run("enum with varchar column keeps its label-comparison route", func(t *testing.T) {
		// The ENUM-vs-string comparison binds in the VARCHAR label domain
		// (attempt-1 RED evidence: both operands T_varchar on the overlay
		// baseline), via the planner's display normalization of ENUM
		// operands. The uint16 mixed-pair guard cannot touch this route:
		// it forces the diagonal only when the non-ENUM operand normalizes
		// exactly to uint16. Assert the string route with no numeric casts
		// so any regression into the numeric domain fails loudly here.
		ctx := enumNumericMockContext(t, enumType,
			planpb.Type{Id: int32(types.T_varchar), Width: 16})
		q := buildPlanForEnumNumericQuery(t, ctx,
			"select id = val from constraint_test.single_idx_t")
		projects := enumNumericProjectNode(t, q).ProjectList
		require.Len(t, projects, 1)
		expr := projects[0]
		f := expr.GetF()
		require.NotNil(t, f)
		require.Len(t, f.Args, 2)
		require.Equal(t, int32(types.T_varchar), f.Args[0].Typ.Id,
			"enum to varchar comparison stays in the label domain (left)")
		require.Equal(t, int32(types.T_varchar), f.Args[1].Typ.Id,
			"enum to varchar comparison stays in the label domain (right)")
		require.Equal(t, 0, countCastsToUint16(expr),
			"the string comparison route must not gain uint16 casts")
	})
}

// enumUint16BoolRow is one row of an expected boolean result: NULL rows keep
// their null flag, non-null rows carry the compared value.
type enumUint16BoolRow struct {
	null  bool
	value bool
}

// enumUint16ValueRow is one row of an expected numeric result.
type enumUint16ValueRow struct {
	null  bool
	value uint16
}

// enumNumericInputBatches maps the real input batch onto the relation
// position the binder used in the bound expressions. These single-table
// queries reference exactly one source relation; reading relPos and ColPos
// out of the bound expressions (instead of assuming position zero or a
// guessed node count) keeps the evaluation aligned with the actual
// BuildPlan structure, and a different binding fails loudly instead of
// being misread.
func enumNumericInputBatches(t *testing.T, input *batch.Batch, exprs ...*planpb.Expr) []*batch.Batch {
	t.Helper()
	relPos := int32(-1)
	maxColPos := int32(-1)
	var collect func(e *planpb.Expr)
	collect = func(e *planpb.Expr) {
		if e == nil {
			return
		}
		if col := e.GetCol(); col != nil {
			require.GreaterOrEqual(t, col.ColPos, int32(0), "column positions must be real")
			if col.ColPos > maxColPos {
				maxColPos = col.ColPos
			}
			if relPos == -1 {
				relPos = col.RelPos
			} else {
				require.Equal(t, relPos, col.RelPos, "all references must share one relation")
			}
		}
		if f := e.GetF(); f != nil {
			for _, arg := range f.Args {
				collect(arg)
			}
		}
	}
	for _, e := range exprs {
		collect(e)
	}
	require.GreaterOrEqual(t, relPos, int32(0), "expression must reference a real column")
	require.LessOrEqual(t, maxColPos, int32(1), "only the enum and uint16 columns are bound here")
	require.Len(t, input.Vecs, int(maxColPos+1), "batch columns must cover the referenced positions")
	batches := make([]*batch.Batch, relPos+1)
	batches[relPos] = input
	return batches
}

// requireBoundEnumUint16Bool evaluates a binder-bound boolean expression
// over the mapped input batch through the same
// colexec.NewExpressionExecutor pattern as bit_integer_arguments_test.go,
// asserting the value bitmap and the NULL bitmap row by row.
func requireBoundEnumUint16Bool(t *testing.T, proc *process.Process, batches []*batch.Batch, expr *planpb.Expr, want []enumUint16BoolRow) {
	t.Helper()
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, batches, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_bool, result.GetType().Oid,
		"the bound comparison must evaluate to a bool-domain result vector")
	values := vector.MustFixedColNoTypeCheck[bool](result)
	require.Len(t, values, len(want))
	for i, w := range want {
		if w.null {
			require.True(t, result.IsNull(uint64(i)), "row %d must be NULL", i)
			continue
		}
		require.False(t, result.IsNull(uint64(i)), "row %d must be non-NULL", i)
		require.Equal(t, w.value, values[i], "row %d", i)
	}
}

// requireBoundEnumUint16Values evaluates a binder-bound uint16-domain
// expression and asserts value and NULL bitmaps row by row.
func requireBoundEnumUint16Values(t *testing.T, proc *process.Process, batches []*batch.Batch, expr *planpb.Expr, want []enumUint16ValueRow) {
	t.Helper()
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, batches, nil)
	require.NoError(t, err)
	require.Equal(t, types.T_uint16, result.GetType().Oid,
		"the bound arithmetic must evaluate to a uint16-domain result vector")
	values := vector.MustFixedColNoTypeCheck[uint16](result)
	require.Len(t, values, len(want))
	for i, w := range want {
		if w.null {
			require.True(t, result.IsNull(uint64(i)), "row %d must be NULL", i)
			continue
		}
		require.False(t, result.IsNull(uint64(i)), "row %d must be non-NULL", i)
		require.Equal(t, w.value, values[i], "row %d", i)
	}
}

// TestEnumSmallintUnsignedBoundExpressionExecution closes the consumer gap
// of the structural subtests above: they prove cast placement, operand
// domains and plan shape, but never evaluate a bound expression, so they
// cannot show that a mixed types.Enum + uint16 batch really flows through
// the implicit binder cast into the comparison and arithmetic kernels
// without the pre-fix panic ("casting SMALLINT UNSIGNED vector to
// []types.Enum"). This test evaluates the BuildPlan-bound expressions
// themselves through the same colexec.NewExpressionExecutor pattern as
// bit_integer_arguments_test.go, over real non-constant column vectors.
//
// Rows follow the issue contract plus a NULL mix, with 'a'->1, 'b'->2 and
// 'c'->3: the ('b', 2) row must give =:1 in both directions, <=>:1, <:0,
// +:4 and *:4. SELECT covers all five operators (= <=> < + *) in both
// operand orders; WHERE covers the equality and ordering directions
// (id = val / val = id, id < val / val < id). Strict operators keep
// three-valued semantics over the NULL rows; <=> never returns NULL and
// returns true for the NULL/NULL row.
func TestEnumSmallintUnsignedBoundExpressionExecution(t *testing.T) {
	proc := testutil.NewProcess(t)

	// (enum ordinal, enum NULL, uint16 value, uint16 NULL).
	rows := []struct {
		enumValue types.Enum
		enumNull  bool
		uintValue uint16
		uintNull  bool
	}{
		{2, false, 2, false}, // 'b' vs 2: the issue contract row
		{0, true, 2, false},  // NULL enum vs 2
		{3, false, 2, false}, // 'c' vs 2
		{1, false, 1, false}, // 'a' vs 1
		{1, false, 3, false}, // 'a' vs 3: the true forward ordering row
		{2, false, 0, true},  // 'b' vs NULL
		{0, true, 0, true},   // NULL vs NULL: strict operators NULL, nullsafe true
	}
	// wantEqualBoth serves both = orders; wantPlus/wantMul serve both operand
	// orders of + and *. wantLessThan (id < val) and wantGreaterThan
	// (val < id) are distinct non-commutative expectations: the (3, 2) row
	// is false forward and true reversed, the (1, 3) row the opposite, so a
	// swapped binding cannot satisfy both directions at once.
	wantEqualBoth := []enumUint16BoolRow{
		{value: true}, {null: true}, {value: false}, {value: true}, {value: false}, {null: true}, {null: true},
	}
	wantNullSafe := []enumUint16BoolRow{
		{value: true}, {value: false}, {value: false}, {value: true}, {value: false}, {value: false}, {value: true},
	}
	wantLessThan := []enumUint16BoolRow{
		{value: false}, {null: true}, {value: false}, {value: false}, {value: true}, {null: true}, {null: true},
	}
	wantGreaterThan := []enumUint16BoolRow{
		{value: false}, {null: true}, {value: true}, {value: false}, {value: false}, {null: true}, {null: true},
	}
	wantPlus := []enumUint16ValueRow{
		{value: 4}, {null: true}, {value: 5}, {value: 2}, {value: 4}, {null: true}, {null: true},
	}
	wantMul := []enumUint16ValueRow{
		{value: 4}, {null: true}, {value: 6}, {value: 1}, {value: 3}, {null: true}, {null: true},
	}

	// The input relation is a real column vector pair: a non-constant
	// types.Enum batch and a non-constant uint16 batch, with NULL bitmaps.
	input := batch.NewWithSize(2)
	defer input.Clean(proc.Mp())
	input.Vecs[0] = vector.NewVec(types.T_enum.ToType())
	input.Vecs[1] = vector.NewVec(types.T_uint16.ToType())
	enumValues := make([]types.Enum, len(rows))
	enumNulls := make([]bool, len(rows))
	uintValues := make([]uint16, len(rows))
	uintNulls := make([]bool, len(rows))
	for i, row := range rows {
		enumValues[i], enumNulls[i] = row.enumValue, row.enumNull
		uintValues[i], uintNulls[i] = row.uintValue, row.uintNull
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[0], enumValues, enumNulls, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(input.Vecs[1], uintValues, uintNulls, proc.Mp()))
	input.SetRowCount(len(rows))

	enumType := planpb.Type{Id: int32(types.T_enum), Enumvalues: "a,b,c"}
	u16Type := planpb.Type{Id: int32(types.T_uint16)}
	ctx := enumNumericMockContext(t, enumType, u16Type)

	// SELECT output columns in both operand orders, bound by the real
	// optimizer for the real table: =, <=>, <, + and * each appear as
	// id <op> val and val <op> id.
	q := buildPlanForEnumNumericQuery(t, ctx,
		"select id = val, val = id, id <=> val, val <=> id, id < val, val < id, id + val, val + id, id * val, val * id from constraint_test.single_idx_t")
	projects := enumNumericProjectNode(t, q).ProjectList
	require.Len(t, projects, 10)
	selectBatches := enumNumericInputBatches(t, input, projects...)
	requireBoundEnumUint16Bool(t, proc, selectBatches, projects[0], wantEqualBoth)
	requireBoundEnumUint16Bool(t, proc, selectBatches, projects[1], wantEqualBoth)
	requireBoundEnumUint16Bool(t, proc, selectBatches, projects[2], wantNullSafe)
	requireBoundEnumUint16Bool(t, proc, selectBatches, projects[3], wantNullSafe)
	requireBoundEnumUint16Bool(t, proc, selectBatches, projects[4], wantLessThan)
	requireBoundEnumUint16Bool(t, proc, selectBatches, projects[5], wantGreaterThan)
	requireBoundEnumUint16Values(t, proc, selectBatches, projects[6], wantPlus)
	requireBoundEnumUint16Values(t, proc, selectBatches, projects[7], wantPlus)
	requireBoundEnumUint16Values(t, proc, selectBatches, projects[8], wantMul)
	requireBoundEnumUint16Values(t, proc, selectBatches, projects[9], wantMul)

	// WHERE predicates in both operand orders — the issue's exact WHERE id = val
	// and WHERE val = id forms, plus both ordering directions — evaluated as
	// the same bound filters the scan node would push down.
	for _, tc := range []struct {
		sql  string
		want []enumUint16BoolRow
	}{
		{"select * from constraint_test.single_idx_t where id = val", wantEqualBoth},
		{"select * from constraint_test.single_idx_t where val = id", wantEqualBoth},
		{"select * from constraint_test.single_idx_t where id < val", wantLessThan},
		{"select * from constraint_test.single_idx_t where val < id", wantGreaterThan},
	} {
		q := buildPlanForEnumNumericQuery(t, ctx, tc.sql)
		filterNodes := enumNumericFilterNodes(q)
		require.NotEmpty(t, filterNodes, "%s", tc.sql)
		// The single-operator WHERE is retained on every filter-bearing
		// node (scan pushdown plus the filter node), each holding it as a
		// separate FilterList root. Evaluate every retained root so a
		// swapped or half-bound consumer cannot hide behind the first
		// node: both = orders and both < directions must produce the
		// exact three-valued expectation on every consumer.
		for i, node := range filterNodes {
			require.Len(t, node.FilterList, 1, "%s filter node %d", tc.sql, i)
			filterBatches := enumNumericInputBatches(t, input, node.FilterList...)
			requireBoundEnumUint16Bool(t, proc, filterBatches, node.FilterList[0], tc.want)
		}
	}
}
