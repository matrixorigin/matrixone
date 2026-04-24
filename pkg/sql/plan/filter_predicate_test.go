// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Test fixture: a 4-column scan node with INCLUDE columns at positions
// 1 ("price", float32) and 2 ("cat", int64). Position 0 is "id" (pk),
// position 3 is "other" (non-INCLUDE).
const testScanTag int32 = 99

func newFilterTestScanNode() *plan.Node {
	return &plan.Node{
		BindingTags: []int32{testScanTag},
		TableDef: &plan.TableDef{
			Name: "products",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "price", Typ: plan.Type{Id: int32(types.T_float32)}},
				{Name: "cat", Typ: plan.Type{Id: int32(types.T_int64)}},
				{Name: "other", Typ: plan.Type{Id: int32(types.T_int64)}},
			},
		},
	}
}

// Helper builders ---------------------------------------------------------

func colExpr(name string, colPos int32, oid types.T) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(oid)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{
			RelPos: testScanTag,
			ColPos: colPos,
			Name:   name,
		}},
	}
}

func i64Lit(v int64) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}},
	}
}

func f32Lit(v float32) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_float32)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: v}}},
	}
}

func sLit(v string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_varchar)},
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: v}}},
	}
}

func fnExpr(name string, args ...*plan.Expr) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &ObjectRef{ObjName: name},
			Args: args,
		}},
	}
}

// Tests -------------------------------------------------------------------

func TestBuildFilterPredicateJSON_NoFilters(t *testing.T) {
	js, ser, res, err := buildFilterPredicateJSON(nil, newFilterTestScanNode(), []string{"price", "cat"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_NoIncludeColumns(t *testing.T) {
	filters := []*plan.Expr{fnExpr("=", colExpr("price", 1, types.T_float32), f32Lit(5))}
	js, ser, res, err := buildFilterPredicateJSON(filters, newFilterTestScanNode(), nil, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, filters, res, "filters with no INCLUDE cols flow through as residual unchanged")
}

func TestBuildFilterPredicateJSON_NilScanNode(t *testing.T) {
	filters := []*plan.Expr{fnExpr("=", colExpr("price", 1, types.T_float32), f32Lit(5))}
	js, ser, res, err := buildFilterPredicateJSON(filters, nil, []string{"price"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, filters, res)
}

func TestBuildFilterPredicateJSON_AllComparisonOps(t *testing.T) {
	scan := newFilterTestScanNode()
	cases := []struct {
		op      string
		wantOp  string
		wantStr string // expected JSON for a single predicate
	}{
		{"=", "=", `[{"col":0,"op":"=","val":5}]`},
		{"!=", "!=", `[{"col":0,"op":"!=","val":5}]`},
		{"<>", "!=", `[{"col":0,"op":"!=","val":5}]`},
		{"<", "<", `[{"col":0,"op":"<","val":5}]`},
		{"<=", "<=", `[{"col":0,"op":"<=","val":5}]`},
		{">", ">", `[{"col":0,"op":">","val":5}]`},
		{">=", ">=", `[{"col":0,"op":">=","val":5}]`},
	}
	for _, tc := range cases {
		t.Run(tc.op, func(t *testing.T) {
			filters := []*plan.Expr{fnExpr(tc.op, colExpr("price", 1, types.T_float32), i64Lit(5))}
			js, ser, res, err := buildFilterPredicateJSON(filters, scan, []string{"price", "cat"}, "")
			require.NoError(t, err)
			require.JSONEq(t, tc.wantStr, js, "op=%s", tc.op)
			require.Len(t, ser, 1)
			require.Empty(t, res)
		})
	}
}

func TestBuildFilterPredicateJSON_FlippedComparison(t *testing.T) {
	// 5 < price  →  price > 5  (op flipped, column on left in the JSON)
	scan := newFilterTestScanNode()
	filters := []*plan.Expr{fnExpr("<", i64Lit(5), colExpr("price", 1, types.T_float32))}
	js, ser, res, err := buildFilterPredicateJSON(filters, scan, []string{"price"}, "")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":0,"op":">","val":5}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_AndDecomposition(t *testing.T) {
	// price >= 5.0 AND cat = 10  →  two predicates, implicit AND on C++ side
	scan := newFilterTestScanNode()
	left := fnExpr(">=", colExpr("price", 1, types.T_float32), f32Lit(5.0))
	right := fnExpr("=", colExpr("cat", 2, types.T_int64), i64Lit(10))
	andExpr := fnExpr("and", left, right)

	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{andExpr}, scan, []string{"price", "cat"}, "")
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"col":0,"op":">=","val":5},{"col":1,"op":"=","val":10}]`, js)
	require.Len(t, ser, 1, "the single AND expression got serialized as a unit")
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_AndWithUnserializableArm(t *testing.T) {
	// AND where one arm references a non-INCLUDE column → whole AND becomes
	// residual (we don't half-peel an AND; the C++ side wouldn't know which
	// arm was applied vs deferred).
	scan := newFilterTestScanNode()
	good := fnExpr("=", colExpr("price", 1, types.T_float32), f32Lit(5))
	bad := fnExpr("=", colExpr("other", 3, types.T_int64), i64Lit(7))
	andExpr := fnExpr("and", good, bad)

	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{andExpr}, scan, []string{"price", "cat"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, []*plan.Expr{andExpr}, res)
}

func TestBuildFilterPredicateJSON_Between(t *testing.T) {
	scan := newFilterTestScanNode()
	bw := fnExpr("between", colExpr("price", 1, types.T_float32), i64Lit(1), i64Lit(10))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{bw}, scan, []string{"price"}, "")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":0,"op":"between","lo":1,"hi":10}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_InWithFlatArgs(t *testing.T) {
	// IN where each value is a separate trailing arg: (col, lit0, lit1, lit2)
	scan := newFilterTestScanNode()
	in := fnExpr("in",
		colExpr("cat", 2, types.T_int64),
		i64Lit(100), i64Lit(200), i64Lit(300))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{in}, scan, []string{"price", "cat"}, "")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":1,"op":"in","vals":[100,200,300]}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_InWithExprList(t *testing.T) {
	// IN with the (col, Expr_List{...}) shape some planner phases produce.
	scan := newFilterTestScanNode()
	listExpr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_tuple)},
		Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			i64Lit(1), i64Lit(2),
		}}},
	}
	in := fnExpr("in", colExpr("cat", 2, types.T_int64), listExpr)
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{in}, scan, []string{"price", "cat"}, "")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":1,"op":"in","vals":[1,2]}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_IsNullVariants(t *testing.T) {
	scan := newFilterTestScanNode()
	cases := []struct {
		fnName string
		wantOp string
	}{
		{"is_null", "is_null"},
		{"isnull", "is_null"},
		{"is_not_null", "is_not_null"},
		{"isnotnull", "is_not_null"},
	}
	for _, tc := range cases {
		t.Run(tc.fnName, func(t *testing.T) {
			f := fnExpr(tc.fnName, colExpr("price", 1, types.T_float32))
			js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, []string{"price"}, "")
			require.NoError(t, err)
			require.JSONEq(t, `[{"col":0,"op":"`+tc.wantOp+`"}]`, js)
			require.Len(t, ser, 1)
			require.Empty(t, res)
		})
	}
}

func TestBuildFilterPredicateJSON_MixedIncludeAndResidual(t *testing.T) {
	// price <= 100 (INCLUDE)  +  other > 3 (non-INCLUDE)  +  cat = 5 (INCLUDE)
	// Expect: 2 peeled into JSON, 1 residual stays on FilterList.
	scan := newFilterTestScanNode()
	peelable1 := fnExpr("<=", colExpr("price", 1, types.T_float32), i64Lit(100))
	residualOne := fnExpr(">", colExpr("other", 3, types.T_int64), i64Lit(3))
	peelable2 := fnExpr("=", colExpr("cat", 2, types.T_int64), i64Lit(5))

	js, ser, res, err := buildFilterPredicateJSON(
		[]*plan.Expr{peelable1, residualOne, peelable2},
		scan, []string{"price", "cat"}, "")
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"col":0,"op":"<=","val":100},{"col":1,"op":"=","val":5}]`, js)
	require.Equal(t, []*plan.Expr{peelable1, peelable2}, ser)
	require.Equal(t, []*plan.Expr{residualOne}, res)
}

func TestBuildFilterPredicateJSON_StringLiteralFallsThrough(t *testing.T) {
	// VARCHAR literals aren't yet hashable in the planner — predicate stays
	// as residual rather than emitting a JSON entry the C++ side can't
	// interpret against a hashed column.
	scan := newFilterTestScanNode()
	f := fnExpr("=", colExpr("price", 1, types.T_float32), sLit("xyz"))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, []string{"price"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, []*plan.Expr{f}, res)
}

func TestBuildFilterPredicateJSON_UnsupportedOpFallsThrough(t *testing.T) {
	// LIKE isn't on the C++ op_from_string list — stays residual.
	scan := newFilterTestScanNode()
	f := fnExpr("like", colExpr("price", 1, types.T_float32), sLit("5%"))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, []string{"price"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, []*plan.Expr{f}, res)
}

func TestBuildFilterPredicateJSON_ColumnNotInIncludeList(t *testing.T) {
	// price is INCLUDE but the predicate references "other" which is not.
	scan := newFilterTestScanNode()
	f := fnExpr("=", colExpr("other", 3, types.T_int64), i64Lit(7))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, []string{"price"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, []*plan.Expr{f}, res)
}

// PK pushdown via __mo_pk_host_id -------------------------------------------

func TestBuildFilterPredicateJSON_PKComparison(t *testing.T) {
	// id >= 100 with pkColName="id" → routes to the PK virtual column,
	// emitting col=-1 (sentinel that wraps to kHostIdColIdx on the C++ side).
	scan := newFilterTestScanNode()
	f := fnExpr(">=", colExpr("id", 0, types.T_int64), i64Lit(100))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, nil, "id")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":-1,"op":">=","val":100}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_PKIn(t *testing.T) {
	// id IN (1,2,3) is the common case we want working end-to-end.
	scan := newFilterTestScanNode()
	in := fnExpr("in", colExpr("id", 0, types.T_int64),
		i64Lit(1), i64Lit(2), i64Lit(3))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{in}, scan, nil, "id")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":-1,"op":"in","vals":[1,2,3]}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_PKBetween(t *testing.T) {
	scan := newFilterTestScanNode()
	bw := fnExpr("between", colExpr("id", 0, types.T_int64), i64Lit(10), i64Lit(20))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{bw}, scan, nil, "id")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":-1,"op":"between","lo":10,"hi":20}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_PKAndIncludeMixed(t *testing.T) {
	// id = 50 (PK) AND price < 10 (INCLUDE) → both peeled, PK uses col=-1.
	scan := newFilterTestScanNode()
	pkF := fnExpr("=", colExpr("id", 0, types.T_int64), i64Lit(50))
	incF := fnExpr("<", colExpr("price", 1, types.T_float32), i64Lit(10))
	js, ser, res, err := buildFilterPredicateJSON(
		[]*plan.Expr{pkF, incF}, scan, []string{"price", "cat"}, "id")
	require.NoError(t, err)
	require.JSONEq(t,
		`[{"col":-1,"op":"=","val":50},{"col":0,"op":"<","val":10}]`, js)
	require.Len(t, ser, 2)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_PKDisabledWhenNameEmpty(t *testing.T) {
	// Empty pkColName disables PK routing; predicate on "id" falls to
	// residual because "id" is not in the INCLUDE list.
	scan := newFilterTestScanNode()
	f := fnExpr("=", colExpr("id", 0, types.T_int64), i64Lit(50))
	js, ser, res, err := buildFilterPredicateJSON(
		[]*plan.Expr{f}, scan, []string{"price"}, "")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, []*plan.Expr{f}, res)
}

func TestBuildFilterPredicateJSON_PKTakesPrecedenceOverIncludeList(t *testing.T) {
	// Defense-in-depth: build_ddl.go's validateIncludeColumns rejects the PK
	// in the INCLUDE list at CREATE INDEX time, so this state should not
	// occur in practice. If it ever does (pre-migration indexes, tests),
	// the planner still prefers the PK path over the duplicate FilterStore
	// column — cheaper, and keeps the emitted JSON canonical.
	scan := newFilterTestScanNode()
	f := fnExpr("=", colExpr("id", 0, types.T_int64), i64Lit(7))
	js, ser, res, err := buildFilterPredicateJSON(
		[]*plan.Expr{f}, scan, []string{"id", "price"}, "id")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":-1,"op":"=","val":7}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_PKIsNotNull(t *testing.T) {
	// IS_NULL/IS_NOT_NULL on PK is legal syntactically; the C++ side
	// short-circuits (PKs are non-nullable). We still emit the predicate.
	scan := newFilterTestScanNode()
	f := fnExpr("is_not_null", colExpr("id", 0, types.T_int64))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, nil, "id")
	require.NoError(t, err)
	require.JSONEq(t, `[{"col":-1,"op":"is_not_null"}]`, js)
	require.Len(t, ser, 1)
	require.Empty(t, res)
}

func TestBuildFilterPredicateJSON_PKVarcharLiteralFallsThrough(t *testing.T) {
	// VARCHAR literal on PK is not representable as a JSON number — stays
	// residual (no regression vs today).
	scan := newFilterTestScanNode()
	f := fnExpr("=", colExpr("id", 0, types.T_int64), sLit("abc"))
	js, ser, res, err := buildFilterPredicateJSON([]*plan.Expr{f}, scan, nil, "id")
	require.NoError(t, err)
	require.Equal(t, "", js)
	require.Empty(t, ser)
	require.Equal(t, []*plan.Expr{f}, res)
}

// parseIncludedColumnsFromParams ---------------------------------------------

func TestParseIncludedColumnsFromParams(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"missing_key", `{"lists":"10"}`, nil},
		{"single", `{"included_columns":"price"}`, []string{"price"}},
		{"multi", `{"included_columns":"price,category_id"}`, []string{"price", "category_id"}},
		{"trims_spaces", `{"included_columns":" price , category_id "}`, []string{"price", "category_id"}},
		{"drops_empties", `{"included_columns":"price,,cat,"}`, []string{"price", "cat"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseIncludedColumnsFromParams(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
