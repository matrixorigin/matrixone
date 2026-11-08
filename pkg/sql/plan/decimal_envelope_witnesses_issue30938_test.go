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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// This file pins design revision 29's cell (xxi) (c) non-MOD
// storage-envelope witnesses (F-ENVELOPE-COVERAGE, decision log 74 (iii))
// and the cell (xxi) (e) memoized-walk determinism witnesses
// (F-MEMOIZED-WALK, decision log 74 (v)) for issue #30938. The (c)
// families assert CTAS and view records persist the parent's own
// inference-derived tags for the section-4.1 add/subtract, divide and
// PERCENTILE_DISC envelope mirrors and for a genuine-wide value merge; all
// expected tags are derived from the shared typing owners
// (decimalAddSubReturnType's min(maxIntegerDigits+scale+1, 65), the
// truncate rule's integral-digits derivation, PercentileDiscReturnType's
// value-argument selection and the merge common-domain rule), never from
// observed candidate behavior. The (e) families drive the statement-scoped
// evaluator directly and count its memo misses (build_ddl.go's evalCount,
// incremented on memo miss only).

// TestDecimalAddChainStorageBoundaryIssue30938 is cell (xxi) (c-add): the
// capped-consumer addition chain TRUNCATE(a*b, 0) + d over DECIMAL(38,10)
// operands with d DECIMAL(39,0) persists the parent's own derived tag
// (54,0) — the product publishes (65,12) on the parent, the truncate
// rule's integral-digits derivation yields (53,0), and
// decimalAddSubReturnType's maximum-integral-plus-carry rule over (53,0)
// and (39,0) yields min(53+0+1, 65) = (54,0) — never the widened
// derivation's (65,0) head. CTAS and view records alike.
func TestDecimalAddChainStorageBoundaryIssue30938(t *testing.T) {
	ctas := buildModBoundaryCTAS(t,
		"create table m30938a1 as select truncate(a*b, 0) + d as v from m30938s")
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 54, 0,
		"(c-add): the capped addition chain's CTAS record persists the parent's (54,0)")
	view := buildModBoundaryCTAS(t,
		"create view m30938av1 as select truncate(a*b, 0) + d as v from m30938s")
	requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 54, 0,
		"(c-add): the capped addition chain's view record persists the parent's (54,0)")
}

// TestDecimalPercentileDiscStorageBoundaryIssue30938 is cell (xxi)
// (c-percentile): a CTAS or view over PERCENTILE_DISC(0.5) WITHIN GROUP
// (ORDER BY a*b) over DECIMAL(38,0) operands persists the parent's own
// (65,0) — the value-selecting aggregate returns its ORDER BY argument
// verbatim (PercentileDiscReturnType), and the parent's product publishes
// (65,0) — never the widened product's (76,0). CTAS and view records
// alike.
func TestDecimalPercentileDiscStorageBoundaryIssue30938(t *testing.T) {
	ctas := buildModBoundaryCTAS(t,
		"create table m30938a2 as select percentile_disc(0.5) within group (order by a*b) as v from m30938w")
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 65, 0,
		"(c-percentile): the CTAS record persists the parent's (65,0)")
	view := buildModBoundaryCTAS(t,
		"create view m30938av2 as select percentile_disc(0.5) within group (order by a*b) as v from m30938w")
	requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 65, 0,
		"(c-percentile): the view record persists the parent's (65,0)")
}

// TestDecimalMergeGenuineWideStorageBoundaryIssue30938 is cell (xxi) (c)
// genuine-wide control: a value merge of a widened product with a genuine
// 66-digit literal persists the parent's own merge-derived (66,0) — the
// parent's merge common over the product's (65,0) and the literal's
// (66,0) yields (66,0), a parent-genuine inferred tag above the 65
// declaration cap that the boundary retains exactly — never the widened
// head's (76,0). CTAS and view records alike.
func TestDecimalMergeGenuineWideStorageBoundaryIssue30938(t *testing.T) {
	wide := "999999999999999999999999999999999999999999999999999999999999999999"
	require.Len(t, wide, 66, "the genuine-wide literal carries 66 digits")
	ctas := buildModBoundaryCTAS(t,
		"create table m30938a3 as select coalesce(a*b, "+wide+") as v from m30938w")
	requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 66, 0,
		"(c genuine-wide control): the CTAS record persists the parent's (66,0)")
	view := buildModBoundaryCTAS(t,
		"create view m30938av3 as select coalesce(a*b, "+wide+") as v from m30938w")
	requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
		types.T_decimal256, 66, 0,
		"(c genuine-wide control): the view record persists the parent's (66,0)")
}

// driveStorageBoundaryColumns evaluates the statement-scoped storage
// boundary over every output column of a plain SELECT plan exactly the
// way the CTAS column loop shares one evaluator per statement (design
// §4.3): one ctasStorageDomain, the result column node, one
// storageBoundary call per projected output expression. It returns the
// evaluator (for its memo-miss count) and the per-column results.
func driveStorageBoundaryColumns(t *testing.T, built *pb.Plan) (*ctasStorageDomain, []pb.Type) {
	t.Helper()
	query := built.GetQuery()
	require.NotNil(t, query)
	main := resultColumnNode(query)
	require.NotNil(t, main)
	d := newCtasStorageBoundaryDomain(nil, nil, query)
	results := make([]pb.Type, 0, len(main.ProjectList))
	for _, expr := range main.ProjectList {
		typ, err := d.storageBoundary(expr.Typ, expr, main)
		require.NoError(t, err)
		results = append(results, typ)
	}
	return d, results
}

// TestDecimalStorageBoundaryMemoizedDiamondIssue30938 is cell (xxi) (e)
// shared-diamond: a producer slot referenced again — within one output
// column and across every output column of the statement — is evaluated
// at most once (design §4.3's one-shot statement-scoped memo), so three
// output columns carrying five references to the same shared CTE slot
// cost exactly one slot evaluation, and the shared evaluation derives the
// parent's own (65,0) boundary for the widened product column.
func TestDecimalStorageBoundaryMemoizedDiamondIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock,
		"create table m30938w(a decimal(38,0), b decimal(38,0), d decimal(39,0))")
	built, err := runOneStmt(mock, t,
		"with c as (select a*b as v from m30938w) "+
			"select v as p, coalesce(v, v) as q, greatest(v, v) as r from c")
	require.NoError(t, err)
	d, results := driveStorageBoundaryColumns(t, built)
	require.Len(t, results, 3)
	require.Equal(t, 1, d.evalCount,
		"(e) shared-diamond: five references to one producer slot evaluate it once")
	requireModBoundaryTyp(t, results[0], types.T_decimal256, 65, 0,
		"(e) shared-diamond: the shared evaluation derives the parent's (65,0)")
}

// TestDecimalStorageBoundaryRecursiveAnchorSeedIssue30938 is cell (xxi)
// (e) anchor-only recursion: a recursive CTE's union slot envelope IS its
// anchor slot's envelope (the parent types recursive columns from the
// anchor member and coerces every recursive member to the anchor's
// projection types), so no recursive-member slot is envelope-evaluated at
// all — two output references to the recursive column cost exactly one
// anchor-seeded slot evaluation, and the boundary derives the parent's
// (65,0) for the widened product anchor.
func TestDecimalStorageBoundaryRecursiveAnchorSeedIssue30938(t *testing.T) {
	proc := newPlanTestProcess(t)
	t.Cleanup(proc.Free)
	mock := NewMockOptimizer(false, proc)
	registerModBoundaryTable(t, mock,
		"create table m30938w(a decimal(38,0), b decimal(38,0), d decimal(39,0))")
	built, err := runOneStmt(mock, t,
		"with recursive c(n, v) as "+
			"(select 1, a*b from m30938w union all select n+1, v from c where n < 3) "+
			"select v as p, v as q from c")
	require.NoError(t, err)
	d, results := driveStorageBoundaryColumns(t, built)
	require.Len(t, results, 2)
	require.Equal(t, 1, d.evalCount,
		"(e) anchor-only recursion: zero recursive-member slots are evaluated")
	requireModBoundaryTyp(t, results[0], types.T_decimal256, 65, 0,
		"(e) anchor-only recursion: the anchor seed derives the parent's (65,0)")
	require.Equal(t, results[0], results[1],
		"(e) anchor-only recursion: the duplicated reference reads the memo")
}

// TestDecimalNameConstStorageBoundaryIssue30938 pins the NAME_CONST
// storage-envelope repair (issue #30938 pre-PR review 1793732446352):
// NAME_CONST's
// first argument is a non-value label — validateNameConstArgs requires a
// literal and the function's own publication returns parameters[1]
// verbatim — so only the second argument determines the persisted
// precision and scale. Merging the label's digits into the storage
// envelope persisted DECIMAL(11,2) for a nine-digit label over a
// DECIMAL(4,2) value where the parent persists the value argument's own
// DECIMAL(4,2). The value argument is always a literal (or a literal
// cast/unary form), so no multiplication provenance can ride it. The
// expected tags derive from the base owners (the name_const retType
// returning parameters[1] and the base CTAS column type passthrough),
// never from observed candidate behavior.
func TestDecimalNameConstStorageBoundaryIssue30938(t *testing.T) {
	// A numeric label contributes no integral digits: CTAS and view
	// records persist the value argument's own DECIMAL(4,2).
	t.Run("numeric label", func(t *testing.T) {
		ctas := buildModBoundaryCTAS(t,
			"create table m30938n1 as select name_const(123456789, 12.34) as v")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 4, 2,
			"numeric label: the CTAS record persists the value argument's (4,2)")
		view := buildModBoundaryCTAS(t,
			"create view m30938nv1 as select name_const(123456789, 12.34) as v")
		requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 4, 2,
			"numeric label: the view record persists the value argument's (4,2)")
	})
	// A numeric-looking string label is still a label: its digits never
	// reach the value domain.
	t.Run("numeric-looking string label", func(t *testing.T) {
		ctas := buildModBoundaryCTAS(t,
			"create table m30938n2 as select name_const('999999999', 12.34) as v")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 4, 2,
			"numeric-looking label: the CTAS record persists the value argument's (4,2)")
		view := buildModBoundaryCTAS(t,
			"create view m30938nv2 as select name_const('999999999', 12.34) as v")
		requireModBoundaryTyp(t, view.GetDdl().GetCreateView().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 4, 2,
			"numeric-looking label: the view record persists the value argument's (4,2)")
	})
	// Control: a non-numeric label carries no decimal domain on any
	// engine; the record keeps the value argument's (4,2).
	t.Run("non-numeric label control", func(t *testing.T) {
		ctas := buildModBoundaryCTAS(t,
			"create table m30938n3 as select name_const('report', 12.34) as v")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 4, 2,
			"non-numeric label control: the CTAS record persists (4,2)")
	})
	// A wider literal value keeps its own (8,2); the nine-digit label
	// must not raise the persisted width to (11,2).
	t.Run("wider literal value", func(t *testing.T) {
		ctas := buildModBoundaryCTAS(t,
			"create table m30938n4 as select name_const(123456789, 123456.78) as v")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 8, 2,
			"wider literal value: the CTAS record persists the value argument's (8,2)")
	})
	// A signed literal value keeps its (4,2) through the transparent
	// unary_minus wrapper under the value-argument branch.
	t.Run("signed literal value", func(t *testing.T) {
		ctas := buildModBoundaryCTAS(t,
			"create table m30938n5 as select name_const(123456789, -12.34) as v")
		requireModBoundaryTyp(t, ctas.GetDdl().GetCreateTable().GetTableDef().GetCols()[0].GetTyp(),
			types.T_decimal64, 4, 2,
			"signed literal value: the CTAS record persists the value argument's (4,2)")
	})
}
