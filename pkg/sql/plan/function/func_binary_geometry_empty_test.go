// Copyright 2021 - 2026 Matrix Origin
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

package function

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// MySQL's object-shape contract for the spatial relation functions documents
// that, unless noted otherwise, they return NULL when any geometry argument is
// empty. ST_Equals is the documented exception; ST_Intersects and ST_Disjoint
// follow the general contract, so a valid empty operand makes the row SQL NULL
// while a NULL predicate result stays distinguishable from FALSE.

func runStRelationEmptyCase(
	t *testing.T,
	info string,
	fn executeLogicOfOverload,
	left, right []string,
	wanted []bool,
	wantedNulls []bool,
) {
	t.Helper()
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_geometry.ToType(), left, make([]bool, len(left))),
		NewFunctionTestInput(types.T_geometry.ToType(), right, make([]bool, len(right))),
	}
	expect := NewFunctionTestResult(types.T_bool.ToType(), false, wanted, wantedNulls)
	fcTC := NewFunctionTestCase(proc, inputs, expect, fn)
	s, errInfo := fcTC.RunAndFree()
	require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", info, errInfo))
}

// TestStIntersectsEmptyOperandIsNull covers the issue-33196 reproductions: an
// empty point or an all-empty collection on either side yields NULL, while
// regular rows of the same vector keep their TRUE/FALSE results and a
// collection with a non-empty member stays on the regular predicate path.
func TestStIntersectsEmptyOperandIsNull(t *testing.T) {
	one := []string{"POINT(0 0)"}
	empty := []string{"POINT EMPTY"}
	t.Run("right operand empty", func(t *testing.T) {
		runStRelationEmptyCase(t, "intersects right empty", StIntersects, one, empty,
			[]bool{false}, []bool{true})
	})
	t.Run("left operand empty", func(t *testing.T) {
		runStRelationEmptyCase(t, "intersects left empty", StIntersects, empty, one,
			[]bool{false}, []bool{true})
	})
	t.Run("both operands empty", func(t *testing.T) {
		runStRelationEmptyCase(t, "intersects both empty", StIntersects, empty, empty,
			[]bool{false}, []bool{true})
	})
	t.Run("empty collections", func(t *testing.T) {
		runStRelationEmptyCase(t, "intersects geometrycollection empty", StIntersects,
			one, []string{"GEOMETRYCOLLECTION EMPTY"}, []bool{false}, []bool{true})
		runStRelationEmptyCase(t, "intersects multipoint empty", StIntersects,
			one, []string{"MULTIPOINT EMPTY"}, []bool{false}, []bool{true})
		runStRelationEmptyCase(t, "intersects all-empty collection", StIntersects,
			one, []string{"GEOMETRYCOLLECTION(POINT EMPTY)"}, []bool{false}, []bool{true})
		runStRelationEmptyCase(t, "intersects nested all-empty collection", StIntersects,
			one, []string{"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT EMPTY),MULTIPOINT EMPTY)"},
			[]bool{false}, []bool{true})
	})
	t.Run("mixed rows keep predicate results", func(t *testing.T) {
		runStRelationEmptyCase(t, "intersects mixed rows", StIntersects,
			[]string{"POINT(0 0)", "POINT EMPTY", "POINT(2 2)", "POINT EMPTY"},
			[]string{"POINT(0 0)", "POINT(0 0)", "POINT(1 1)", "POINT EMPTY"},
			[]bool{true, false, false, false},
			[]bool{false, true, false, true})
	})
	// A collection that still has a non-empty member is not empty: it keeps
	// its member-wise evaluation, which still rejects empty members exactly as
	// before this change (issue 33198 territory, deliberately untouched here).
	// The pre-fix behavior on exact main is the same 20301 error.
	t.Run("collection with an empty member keeps its evaluation path", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_geometry.ToType(),
				[]string{"GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 1))"}, []bool{false}),
			NewFunctionTestInput(types.T_geometry.ToType(), one, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{false})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects)
		succeed, info := fcTC.RunAndFree()
		require.False(t, succeed, "legacy member-wise evaluation retains issue-33198 behavior")
		require.Contains(t, info, "invalid point payload")
	})
	// A valid empty operand must not swallow a malformed operand on the other
	// side: both operands run the established decode before the emptiness
	// result, mirroring ST_DISTANCE's two-sided ordering. The malformed payload
	// is a vector value that reaches the actual evaluator, not a failed
	// constructor.
	t.Run("malformed paired with valid empty keeps its error", func(t *testing.T) {
		runStRelationErrorCase(t, "intersects malformed right with empty left", StIntersects,
			[]string{"POINT EMPTY"}, []string{"GEOMETRYCOLLECTION("})
		runStRelationErrorCase(t, "intersects malformed left with empty right", StIntersects,
			[]string{"GEOMETRYCOLLECTION("}, []string{"POINT EMPTY"})
	})
}

// TestStDisjointEmptyOperandIsNull mirrors the ST_Intersects coverage for the
// sibling predicate from the same issue: valid empty operands yield NULL, and
// regular disjoint/intersecting rows keep their results.
func TestStDisjointEmptyOperandIsNull(t *testing.T) {
	one := []string{"POINT(0 0)"}
	other := []string{"POINT(1 1)"}
	empty := []string{"POINT EMPTY"}
	t.Run("right operand empty", func(t *testing.T) {
		runStRelationEmptyCase(t, "disjoint right empty", StDisjoint, one, empty,
			[]bool{false}, []bool{true})
	})
	t.Run("left operand empty", func(t *testing.T) {
		runStRelationEmptyCase(t, "disjoint left empty", StDisjoint, empty, one,
			[]bool{false}, []bool{true})
	})
	t.Run("both operands empty", func(t *testing.T) {
		runStRelationEmptyCase(t, "disjoint both empty", StDisjoint, empty, empty,
			[]bool{false}, []bool{true})
	})
	t.Run("empty collections", func(t *testing.T) {
		runStRelationEmptyCase(t, "disjoint all-empty collection", StDisjoint,
			one, []string{"GEOMETRYCOLLECTION(POINT EMPTY)"}, []bool{false}, []bool{true})
	})
	t.Run("mixed rows keep predicate results", func(t *testing.T) {
		runStRelationEmptyCase(t, "disjoint mixed rows", StDisjoint,
			[]string{"POINT(0 0)", "POINT EMPTY", "POINT(2 2)", "POINT EMPTY"},
			[]string{"POINT(0 0)", "POINT(0 0)", "POINT(1 1)", "POINT EMPTY"},
			[]bool{false, false, true, false},
			[]bool{false, true, false, true})
	})
	// A collection that still has a non-empty member keeps its member-wise
	// evaluation and its issue-33198 behavior: still the same 20301 error as
	// on exact main.
	t.Run("collection with an empty member keeps its evaluation path", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(types.T_geometry.ToType(),
				[]string{"GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 1))"}, []bool{false}),
			NewFunctionTestInput(types.T_geometry.ToType(), other, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, []bool{false})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StDisjoint)
		succeed, info := fcTC.RunAndFree()
		require.False(t, succeed, "legacy member-wise evaluation retains issue-33198 behavior")
		require.Contains(t, info, "invalid point payload")
	})
	// Mirror of the ST_Intersects paired-validation proof for the sibling
	// predicate: a malformed operand errors in both operand orders rather
	// than collapsing into NULL against a valid empty.
	t.Run("malformed paired with valid empty keeps its error", func(t *testing.T) {
		runStRelationErrorCase(t, "disjoint malformed right with empty left", StDisjoint,
			[]string{"POINT EMPTY"}, []string{"GEOMETRYCOLLECTION("})
		runStRelationErrorCase(t, "disjoint malformed left with empty right", StDisjoint,
			[]string{"GEOMETRYCOLLECTION("}, []string{"POINT EMPTY"})
	})
}

func runStRelationSridCase(
	t *testing.T,
	info string,
	fn executeLogicOfOverload,
	leftType, rightType types.Type,
	leftWKT, rightWKT []string,
	wanted []bool,
	wantedNulls []bool,
	wantError bool,
) {
	t.Helper()
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(leftType, leftWKT, make([]bool, len(leftWKT))),
		NewFunctionTestInput(rightType, rightWKT, make([]bool, len(rightWKT))),
	}
	expect := NewFunctionTestResult(types.T_bool.ToType(), false, wanted, wantedNulls)
	fcTC := NewFunctionTestCase(proc, inputs, expect, fn)
	s, errInfo := fcTC.RunAndFree()
	if wantError {
		require.False(t, s, fmt.Sprintf("case is '%s', expected an error", info))
	} else {
		require.True(t, s, fmt.Sprintf("case is '%s', err info is '%s'", info, errInfo))
	}
}

// TestStRelationEmptyWithSRID keeps the SRID behavior stable: WGS 84 empty
// operands still yield NULL (the emptiness decision happens before the
// geodetic projection), regular WGS 84 pairs keep their predicate results, and
// a mismatched pair still fails the established SRID guard instead of NULLing.
func TestStRelationEmptyWithSRID(t *testing.T) {
	geom0 := types.T_geometry.ToType()
	geom4326 := types.T_geometry.ToType()
	geom4326.Width = 4326

	t.Run("wgs84 empty is null", func(t *testing.T) {
		runStRelationSridCase(t, "intersects 4326 empty", StIntersects,
			geom4326, geom4326,
			[]string{"POINT(1 1)"}, []string{"POINT EMPTY"},
			[]bool{false}, []bool{true}, false)
		runStRelationSridCase(t, "disjoint 4326 empty", StDisjoint,
			geom4326, geom4326,
			[]string{"POINT EMPTY"}, []string{"POINT(1 1)"},
			[]bool{false}, []bool{true}, false)
	})
	t.Run("wgs84 regular pairs keep results", func(t *testing.T) {
		runStRelationSridCase(t, "intersects 4326 regular", StIntersects,
			geom4326, geom4326,
			[]string{"LINESTRING(0 0,2 2)"}, []string{"POINT(1 1)"},
			[]bool{true}, []bool{false}, false)
		runStRelationSridCase(t, "disjoint 4326 regular", StDisjoint,
			geom4326, geom4326,
			[]string{"LINESTRING(0 0,2 2)"}, []string{"POINT(5 5)"},
			[]bool{true}, []bool{false}, false)
	})
	t.Run("mismatched srids still fail the guard", func(t *testing.T) {
		runStRelationSridCase(t, "intersects different srids", StIntersects,
			geom0, geom4326,
			[]string{"POINT(0 0)"}, []string{"POINT(1 1)"},
			[]bool{false}, []bool{false}, true)
		runStRelationSridCase(t, "disjoint different srids", StDisjoint,
			geom0, geom4326,
			[]string{"POINT(0 0)"}, []string{"POINT(1 1)"},
			[]bool{false}, []bool{false}, true)
	})
}

// runStRelationErrorCase asserts that a malformed vector payload paired with
// any other operand keeps failing through the actual predicate evaluator.
func runStRelationErrorCase(
	t *testing.T,
	info string,
	fn executeLogicOfOverload,
	left, right []string,
) {
	t.Helper()
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_geometry.ToType(), left, make([]bool, len(left))),
		NewFunctionTestInput(types.T_geometry.ToType(), right, make([]bool, len(right))),
	}
	expect := NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false}, make([]bool, 1))
	fcTC := NewFunctionTestCase(proc, inputs, expect, fn)
	succeed, errInfo := fcTC.RunAndFree()
	require.False(t, succeed, fmt.Sprintf("case is '%s', expected the malformed pairing to error", info))
	require.Contains(t, errInfo, "invalid geometry payload")
}

// TestStRelationEmptySelectionAndConstants validates the folded evaluation
// paths: both-constant pairs are fetched and evaluated once and broadcast,
// fully masked batches perform no evaluation, one-sided constants are fetched
// and decoded once, and partial masks plus NULL vectors keep their row
// semantics — with selected malformed errors intact in every shape.
func TestStRelationEmptySelectionAndConstants(t *testing.T) {
	geom := types.T_geometry.ToType()

	constBroadcast := func(t *testing.T, fn executeLogicOfOverload, leftWKT, rightWKT []string, wanted []bool, wantedNulls []bool, wantErr bool) {
		t.Helper()
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestConstInput(geom, leftWKT, make([]bool, len(leftWKT))),
			NewFunctionTestConstInput(geom, rightWKT, make([]bool, len(rightWKT))),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, wanted, wantedNulls)
		fcTC := NewFunctionTestCase(proc, inputs, expect, fn)
		s, info := fcTC.RunAndFree()
		if wantErr {
			require.False(t, s)
		} else {
			require.True(t, s, info)
		}
	}

	t.Run("both const regular pair broadcast", func(t *testing.T) {
		constBroadcast(t, StIntersects,
			[]string{"POINT(0 0)", "POINT(0 0)"}, []string{"POINT(0 0)"},
			[]bool{true, true}, []bool{false, false}, false)
	})
	t.Run("both const empty pair yields null rows", func(t *testing.T) {
		constBroadcast(t, StDisjoint,
			[]string{"POINT(0 0)", "POINT(0 0)"}, []string{"POINT EMPTY"},
			[]bool{false, false}, []bool{true, true}, false)
	})
	t.Run("both const malformed pair still errors", func(t *testing.T) {
		constBroadcast(t, StIntersects,
			[]string{"GEOMETRYCOLLECTION("}, []string{"POINT(0 0)"},
			[]bool{false}, []bool{false}, true)
	})

	t.Run("one side const regular evaluates mixed rows", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestConstInput(geom,
				[]string{"POINT(0 0)", "POINT(0 0)", "POINT(0 0)"},
				[]bool{false, false, false}),
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT EMPTY", "POINT(1 1)"},
				[]bool{false, false, false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false}, []bool{false, true, false})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects)
		s, info := fcTC.RunAndFree()
		require.True(t, s, info)
	})
	t.Run("const empty with malformed row keeps error", func(t *testing.T) {
		// The constant EMPTY left operand must still decode/validate the
		// varying right operand: a malformed row errors instead of turning
		// into NULL.
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestConstInput(geom, []string{"POINT EMPTY"}, []bool{false}),
			NewFunctionTestInput(geom, []string{"GEOMETRYCOLLECTION("}, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false}, []bool{false})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StDisjoint)
		s, info := fcTC.RunAndFree()
		require.False(t, s)
		require.Contains(t, info, "invalid geometry payload")
	})
	t.Run("const malformed with flat rows keeps error", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT(1 1)"}, []bool{false, false}),
			NewFunctionTestConstInput(geom, []string{"GEOMETRYCOLLECTION("}, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false}, []bool{false, false})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects)
		s, info := fcTC.RunAndFree()
		require.False(t, s)
		require.Contains(t, info, "invalid geometry payload")
	})

	t.Run("partial arbitrary mask keeps selected row semantics", func(t *testing.T) {
		// Rows 0/1 are masked off; only row 2 is evaluated. SelectList carries
		// the inverted sense: false marks a row as skipped.
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT(0 0)", "POINT(0 0)"},
				[]bool{false, false, false}),
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT EMPTY", "POINT(0 0)"},
				[]bool{false, false, false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false, true}, []bool{true, true, false})
		mask := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, true}}
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects).WithSelectList(mask)
		s, info := fcTC.RunAndFree()
		require.True(t, s, info)
	})
	t.Run("masked rows do not swallow a selected malformed error", func(t *testing.T) {
		// Only the malformed row 2 is evaluated; the mask must not hide it.
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT(0 0)", "GEOMETRYCOLLECTION("},
				[]bool{false, false, false}),
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT(0 0)", "POINT(0 0)"},
				[]bool{false, false, false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false, false}, []bool{false, false, false})
		mask := &FunctionSelectList{AnyNull: true, SelectList: []bool{false, false, true}}
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects).WithSelectList(mask)
		s, info := fcTC.RunAndFree()
		require.False(t, s)
		require.Contains(t, info, "invalid geometry payload")
	})
	t.Run("fully masked batch performs no evaluation", func(t *testing.T) {
		// AllNull mask: even malformed payloads stay unevaluated and the batch
		// is all NULL, like the replaced template's fast path.
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(geom,
				[]string{"GEOMETRYCOLLECTION(", "POINT(0 0)"}, []bool{false, false}),
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "GEOMETRYCOLLECTION("}, []bool{false, false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, false}, []bool{true, true})
		mask := &FunctionSelectList{AllNull: true}
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects).WithSelectList(mask)
		s, info := fcTC.RunAndFree()
		require.True(t, s, info)
	})
	t.Run("nullable flat vectors keep null semantics", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(geom,
				[]string{"POINT(0 0)", "POINT(0 0)", "POINT(1 1)"},
				[]bool{false, true, false}),
			NewFunctionTestInput(geom,
				[]string{"POINT(1 1)", "POINT(0 0)", "POINT(1 1)"},
				[]bool{false, false, true}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false}, []bool{false, true, true})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StDisjoint)
		s, info := fcTC.RunAndFree()
		require.True(t, s, info)
	})
}

// TestStRelationLegacyTextInvalidOperand covers legacy materialized text
// payloads that reach the predicate evaluator as raw vector values: the
// payload encode path keeps the raw text when a WKT string is not parseable,
// so a vector can genuinely carry "POINT(nope 0)" or a pseudo-empty "POINT()".
// Invalid coordinates and the pseudo-empty shape keep their original errors
// when paired with a valid EMPTY in either operand order — flat, one-sided
// constant, and both-constant modes — instead of collapsing into the
// empty-NULL result. Valid forms, including FLOAT32-coordinate WKB
// (GEOMETRY32) regular and empty payloads, keep their established results.
func TestStRelationLegacyTextInvalidOperand(t *testing.T) {
	geom := types.T_geometry.ToType()

	for _, fn := range []struct {
		name string
		fn   executeLogicOfOverload
	}{
		{name: "intersects", fn: StIntersects},
		{name: "disjoint", fn: StDisjoint},
	} {
		t.Run("invalid coordinate with valid empty keeps its error ("+fn.name+")", func(t *testing.T) {
			// "POINT(nope 0)" stays raw legacy text in the vector while the
			// valid POINT EMPTY operand is stored WKB; both orders must error.
			runStRelationErrorCase(t, "invalid coordinates right of empty "+fn.name, fn.fn,
				[]string{"POINT EMPTY"}, []string{"POINT(nope 0)"})
			runStRelationErrorCase(t, "invalid coordinates left of empty "+fn.name, fn.fn,
				[]string{"POINT(nope 0)"}, []string{"POINT EMPTY"})
		})
		t.Run("pseudo-empty POINT() keeps its error ("+fn.name+")", func(t *testing.T) {
			// POINT() is not a valid POINT even though a paren-shape check
			// classifies it as empty; it must error in every pairing.
			runStRelationErrorCase(t, "pseudo empty with empty "+fn.name, fn.fn,
				[]string{"POINT()"}, []string{"POINT EMPTY"})
			runStRelationErrorCase(t, "empty with pseudo empty "+fn.name, fn.fn,
				[]string{"POINT EMPTY"}, []string{"POINT()"})
			runStRelationErrorCase(t, "pseudo empty with nonempty "+fn.name, fn.fn,
				[]string{"POINT()"}, []string{"POINT(0 0)"})
			runStRelationErrorCase(t, "nonempty with pseudo empty "+fn.name, fn.fn,
				[]string{"POINT(0 0)"}, []string{"POINT()"})
		})
		t.Run("invalid coordinate with valid nonempty keeps the kernel kind ("+fn.name+")", func(t *testing.T) {
			// Error-kind control: the ordinary both-nonempty pairing goes
			// through the predicate kernel, whose original kind for invalid
			// point coordinates is "invalid point payload".
			for _, order := range []struct {
				left, right string
			}{
				{left: "POINT(nope 0)", right: "POINT(0 0)"},
				{left: "POINT(0 0)", right: "POINT(nope 0)"},
			} {
				proc := testutil.NewProcess(t)
				inputs := []FunctionTestInput{
					NewFunctionTestInput(geom, []string{order.left}, []bool{false}),
					NewFunctionTestInput(geom, []string{order.right}, []bool{false}),
				}
				expect := NewFunctionTestResult(types.T_bool.ToType(), false,
					[]bool{false}, []bool{false})
				fcTC := NewFunctionTestCase(proc, inputs, expect, fn.fn)
				succeed, errInfo := fcTC.RunAndFree()
				require.False(t, succeed)
				require.Contains(t, errInfo, "invalid point payload")
			}
		})
		t.Run("constant modes validate the constant once and still error ("+fn.name+")", func(t *testing.T) {
			consts := []struct {
				name   string
				inputs []FunctionTestInput
			}{
				{name: "const empty with flat invalid coordinate", inputs: []FunctionTestInput{
					NewFunctionTestConstInput(geom, []string{"POINT EMPTY"}, []bool{false}),
					NewFunctionTestInput(geom, []string{"POINT(nope 0)"}, []bool{false}),
				}},
				{name: "flat empty with const invalid coordinate", inputs: []FunctionTestInput{
					NewFunctionTestInput(geom, []string{"POINT EMPTY"}, []bool{false}),
					NewFunctionTestConstInput(geom, []string{"POINT(nope 0)"}, []bool{false}),
				}},
				{name: "both const empty with invalid coordinate", inputs: []FunctionTestInput{
					NewFunctionTestConstInput(geom, []string{"POINT EMPTY"}, []bool{false}),
					NewFunctionTestConstInput(geom, []string{"POINT(nope 0)"}, []bool{false}),
				}},
			}
			for _, tc := range consts {
				proc := testutil.NewProcess(t)
				expect := NewFunctionTestResult(types.T_bool.ToType(), false,
					[]bool{false}, []bool{false})
				fcTC := NewFunctionTestCase(proc, tc.inputs, expect, fn.fn)
				succeed, errInfo := fcTC.RunAndFree()
				require.False(t, succeed, tc.name)
				require.Contains(t, errInfo, "invalid geometry payload")
			}
		})
	}

	t.Run("geometry32 float32 WKB keeps regular results and empty NULLs", func(t *testing.T) {
		g32 := func(wkt string) string {
			t.Helper()
			g, err := geo.ParseWKT(wkt)
			require.NoError(t, err)
			out, err := geo.WriteWKBFloat32(g)
			require.NoError(t, err)
			return string(out)
		}
		geom32 := types.T_geometry32.ToType()
		pair := func(fn executeLogicOfOverload, a, b string, want bool, wantNull bool) {
			t.Helper()
			proc := testutil.NewProcess(t)
			inputs := []FunctionTestInput{
				NewFunctionTestInput(geom32, []string{a}, []bool{false}),
				NewFunctionTestInput(geom32, []string{b}, []bool{false}),
			}
			expect := NewFunctionTestResult(types.T_bool.ToType(), false,
				[]bool{want}, []bool{wantNull})
			fcTC := NewFunctionTestCase(proc, inputs, expect, fn)
			s, info := fcTC.RunAndFree()
			require.True(t, s, info)
		}
		pair(StIntersects, g32("POINT(0 0)"), g32("POINT(0 0)"), true, false)
		pair(StIntersects, g32("POINT(0 0)"), g32("POINT(1 1)"), false, false)
		pair(StDisjoint, g32("POINT(0 0)"), g32("POINT(1 1)"), true, false)
		pair(StIntersects, g32("POINT(0 0)"), g32("POINT EMPTY"), false, true)
		pair(StDisjoint, g32("POINT EMPTY"), g32("POINT(0 0)"), false, true)
	})
}

// TestStRelationBoundedWkbEmptinessScan pins the relation-predicate emptiness
// classification on native WKB: each evaluated operand is decoded once and
// classified on the decoded values, with no WKB-to-WKT round trip and no
// per-child re-decode of a whole collection. The catalog asserts parity with
// the established text-classification helper on identical payloads —
// verdicts and error classes — across regular shapes, every "<TYPE> EMPTY"
// form, all-empty and mixed collections, member-empty MULTI* shapes,
// multi-member all-empty collections, a long empty prefix with a late live
// child, and GEOMETRY32 float32 WKB. The predicate rows prove the row-level
// outcomes: a 256-member all-empty collection becomes SQL NULL through the
// single-decode walk, and a late live child keeps the unchanged nonempty
// predicate behavior.
func TestStRelationBoundedWkbEmptinessScan(t *testing.T) {
	wkbOf := func(wkt string) string {
		t.Helper()
		g, err := geo.ParseWKT(wkt)
		require.NoError(t, err)
		return string(geo.WriteWKB(g))
	}
	wkb32Of := func(wkt string) string {
		t.Helper()
		g, err := geo.ParseWKT(wkt)
		require.NoError(t, err)
		out, err := geo.WriteWKBFloat32(g)
		require.NoError(t, err)
		return string(out)
	}
	allEmptyCollection := func(count int) string {
		return "GEOMETRYCOLLECTION(" + strings.Repeat("POINT EMPTY,", count-1) + "POINT EMPTY)"
	}
	lateLiveCollection := func(count int) string {
		return "GEOMETRYCOLLECTION(" + strings.Repeat("POINT EMPTY,", count-1) + "POINT(9 9))"
	}

	// parity compares the once-decoded classification with the established
	// helper on the same payload bytes: identical verdicts, and identical
	// error presence/class whenever either errors.
	parity := func(t *testing.T, payload string) {
		t.Helper()
		once, onceErr := geometryPayloadEmptinessOnce([]byte(payload))
		ref, refErr := geometryDistancePayloadEmpty([]byte(payload))
		if onceErr != nil || refErr != nil {
			require.Error(t, onceErr, "new classification must error exactly when the established one does")
			require.Error(t, refErr, "established classification must error exactly when the new one does")
			require.Contains(t, onceErr.Error(), "invalid geometry payload")
			require.Contains(t, refErr.Error(), "invalid geometry payload")
			return
		}
		require.Equal(t, ref, once)
	}

	t.Run("parity catalog", func(t *testing.T) {
		for _, wkt := range []string{
			"POINT(0 0)", "LINESTRING(0 0,1 1)", "POLYGON((0 0,1 0,1 1,0 1,0 0))",
			"MULTIPOINT((0 0),(1 1))", "MULTILINESTRING((0 0,1 1),(2 2,3 3))",
			"MULTIPOLYGON(((0 0,1 0,1 1,0 1,0 0)))",
			"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
			"POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY",
			"MULTIPOINT EMPTY", "MULTILINESTRING EMPTY", "MULTIPOLYGON EMPTY",
			"GEOMETRYCOLLECTION EMPTY",
			"GEOMETRYCOLLECTION(POINT EMPTY)",
			"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT EMPTY),MULTIPOINT EMPTY)",
			"GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 1))",
			"MULTIPOINT(EMPTY)", "MULTIPOINT(EMPTY,EMPTY)", "MULTIPOINT(EMPTY,(0 0))",
			"MULTILINESTRING(EMPTY)", "MULTIPOLYGON(EMPTY)",
		} {
			parity(t, wkbOf(wkt))
		}
	})
	t.Run("large multi-member collections", func(t *testing.T) {
		parity(t, wkbOf(allEmptyCollection(256)))
		parity(t, wkbOf(lateLiveCollection(256)))
	})
	t.Run("geometry32 float32 WKB parity", func(t *testing.T) {
		parity(t, wkb32Of("POINT(0 0)"))
		parity(t, wkb32Of("POINT EMPTY"))
		parity(t, wkb32Of("GEOMETRYCOLLECTION EMPTY"))
		parity(t, wkb32Of(allEmptyCollection(128)))
	})

	t.Run("predicate rows", func(t *testing.T) {
		geom := types.T_geometry.ToType()

		// A 256-member all-empty collection yields SQL NULL through the
		// single-decode walk; the predicate kernel is never entered.
		proc := testutil.NewProcess(t)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(geom, []string{"POINT(0 0)"}, []bool{false}),
			NewFunctionTestInput(geom, []string{wkbOf(allEmptyCollection(256))}, []bool{false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false}, []bool{true})
		fcTC := NewFunctionTestCase(proc, inputs, expect, StIntersects)
		s, info := fcTC.RunAndFree()
		require.True(t, s, info)

		// A late live child after a long empty prefix keeps the unchanged
		// nonempty behavior: the collection is not empty, so the member-wise
		// predicate evaluation runs and keeps its established result.
		proc = testutil.NewProcess(t)
		inputs = []FunctionTestInput{
			NewFunctionTestInput(geom, []string{"POINT(9 9)"}, []bool{false}),
			NewFunctionTestInput(geom, []string{wkbOf(lateLiveCollection(256))}, []bool{false}),
		}
		expect = NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false}, []bool{false})
		fcTC = NewFunctionTestCase(proc, inputs, expect, StIntersects)
		succeed, errInfo := fcTC.RunAndFree()
		require.False(t, succeed)
		require.Contains(t, errInfo, "invalid point payload")
	})
}
