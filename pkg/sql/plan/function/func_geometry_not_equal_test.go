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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// geometryStorageWKB encodes a WKT geometry the way geometry columns and
// geometry-returning functions store it: bare WKB with float64 coordinates for
// GEOMETRY and float32 coordinates for GEOMETRY32.
func geometryStorageWKB(t *testing.T, wkt string, f32 bool) string {
	t.Helper()
	g, err := geo.ParseWKTForSerialization(wkt)
	require.NoError(t, err)
	if f32 {
		out, err := geo.WriteWKBFloat32(g)
		require.NoError(t, err)
		return string(out)
	}
	return string(geo.WriteWKB(g))
}

var geometryNotEqualWKT = struct {
	pointA    string
	pointB    string
	longLineA string
	longLineB string
}{
	"POINT(1 2)",
	"POINT(3 4)",
	"LINESTRING(0 0,1 1,2 3,4 5,6 7,8 9,10 11,11 12,12 13,13 14,14 15,15 16,16 17,17 18,18 19)",
	"LINESTRING(0 0,1 1,2 3,4 5,6 7,8 9,10 11,11 12,12 13,13 14,14 15,15 16,16 17,17 18,18 20)",
}

// geometryNotEqualStorageCases builds the row set shared by the inequality
// tests: two different points, two equal points (short payloads), two
// different long linestrings, two equal long linestrings, a NULL left value
// and a NULL right value.
func geometryNotEqualStorageCases(t *testing.T, f32 bool) (left, right []string, leftNull, rightNull []bool) {
	w := func(wkt string) string { return geometryStorageWKB(t, wkt, f32) }
	return []string{w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.longLineA), w(geometryNotEqualWKT.longLineA), w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.pointA)},
		[]string{w(geometryNotEqualWKT.pointB), w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.longLineB), w(geometryNotEqualWKT.longLineA), w(geometryNotEqualWKT.pointB), w(geometryNotEqualWKT.pointB)},
		[]bool{false, false, false, false, false, true},
		[]bool{false, false, false, false, true, false}
}

// TestGeometryNotEqualComplementsEquality pins <>/!= on GEOMETRY and
// GEOMETRY32: ordinary inequality is the complement of ordinary equality on
// non-NULL values and propagates ordinary NULLs. Every non-NULL row is
// cross-checked against the existing equality oracle so the two operators
// cannot drift apart.
func TestGeometryNotEqualComplementsEquality(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, st := range []struct {
		name string
		oid  types.T
		f32  bool
	}{
		{"GEOMETRY", types.T_geometry, false},
		{"GEOMETRY32", types.T_geometry32, true},
	} {
		typ := st.oid.ToType()
		left, right, leftNull, rightNull := geometryNotEqualStorageCases(t, st.f32)
		inputs := []FunctionTestInput{
			NewFunctionTestInput(typ, left, leftNull),
			NewFunctionTestInput(typ, right, rightNull),
		}
		wantNotEqual := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, true, false, false, false},
			[]bool{false, false, false, false, true, true})
		fc := NewFunctionTestCase(proc, inputs, wantNotEqual, notEqualFn)
		ok, info := fc.RunAndFree()
		require.True(t, ok, st.name+": "+info)

		// rows 4/5 carry the same ordinary-NULL propagation on the equality side.
		wantEqual := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{false, true, false, true, false, false},
			[]bool{false, false, false, false, true, true})
		fc = NewFunctionTestCase(proc, inputs, wantEqual, equalFn)
		ok, info = fc.RunAndFree()
		require.True(t, ok, st.name+": "+info)
	}
}

// TestGeometryNotEqualSelectedRows pins that masked rows are reported NULL
// instead of being evaluated by the batched template, for geometry payloads on
// both storage formats.
func TestGeometryNotEqualSelectedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, st := range []struct {
		name string
		oid  types.T
		f32  bool
	}{
		{"GEOMETRY", types.T_geometry, false},
		{"GEOMETRY32", types.T_geometry32, true},
	} {
		typ := st.oid.ToType()
		w := func(wkt string) string { return geometryStorageWKB(t, wkt, st.f32) }
		inputs := []FunctionTestInput{
			NewFunctionTestInput(typ, []string{w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.pointB)}, []bool{false, false}),
			NewFunctionTestInput(typ, []string{w(geometryNotEqualWKT.pointB), w(geometryNotEqualWKT.pointB)}, []bool{false, false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false},
			[]bool{false, true})
		fc := NewFunctionTestCase(proc, inputs, expect, notEqualFn).
			WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}})
		ok, info := fc.RunAndFree()
		require.True(t, ok, st.name+": "+info)
	}
}

// TestGeometryEqualityOracleControls keeps the admission rules and the
// null-safe equality oracle frozen while their inequality complement is fixed:
// equality-grade comparisons (both spellings of != share them) stay admitted,
// ordering comparisons stay rejected, and <=> keeps its NULL-safe semantics.
func TestGeometryEqualityOracleControls(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, st := range []struct {
		name string
		oid  types.T
		f32  bool
	}{
		{"GEOMETRY", types.T_geometry, false},
		{"GEOMETRY32", types.T_geometry32, true},
	} {
		typ := st.oid.ToType()
		require.True(t, equalAndNotEqualOperatorSupports(typ, typ), st.name)
		require.False(t, otherCompareOperatorSupports(typ, typ), st.name)

		w := func(wkt string) string { return geometryStorageWKB(t, wkt, st.f32) }
		inputs := []FunctionTestInput{
			NewFunctionTestInput(typ,
				[]string{w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.pointA), ""},
				[]bool{false, false, false, true}),
			NewFunctionTestInput(typ,
				[]string{w(geometryNotEqualWKT.pointA), w(geometryNotEqualWKT.pointB), w(geometryNotEqualWKT.pointB), ""},
				[]bool{false, false, false, true}),
		}
		// <=> : equal values, different values, NULL vs value, NULL vs NULL.
		wantNullSafe := NewFunctionTestResult(types.T_bool.ToType(), false,
			[]bool{true, false, false, true},
			[]bool{false, false, false, false})
		fc := NewFunctionTestCase(proc, inputs, wantNullSafe, nullSafeEqualFn)
		ok, info := fc.RunAndFree()
		require.True(t, ok, st.name+": "+info)
	}
}

// TestGeometryComparisonVarlenaStorageClasses pins ordinary =/<> over the two
// reachable GEOMETRY varlena storage classes and both geometry storage types:
// point payloads (21B float64 / 13B float32 WKB) fit the 23-byte inline
// capacity, so an inline-only vector keeps a nil area and equality's no-area
// fast path is live; one long payload makes the vector area-backed. The
// equality expectations are independent cells, and the inequality rows must
// be their exact complement, including ordinary NULL propagation.
func TestGeometryComparisonVarlenaStorageClasses(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, st := range []struct {
		name string
		oid  types.T
		f32  bool
	}{
		{"GEOMETRY", types.T_geometry, false},
		{"GEOMETRY32", types.T_geometry32, true},
	} {
		typ := st.oid.ToType()
		pointA := geometryStorageWKB(t, geometryNotEqualWKT.pointA, st.f32)
		pointB := geometryStorageWKB(t, geometryNotEqualWKT.pointB, st.f32)
		longA := geometryStorageWKB(t, geometryNotEqualWKT.longLineA, st.f32)
		longB := geometryStorageWKB(t, geometryNotEqualWKT.longLineB, st.f32)
		pointAb, pointBb := []byte(pointA), []byte(pointB)
		longAb, longBb := []byte(longA), []byte(longB)

		// Class reachability: both point encodings fit inline storage, both
		// linestrings are external payloads.
		require.LessOrEqual(t, len(pointA), types.VarlenaInlineSize, st.name+"/pointA")
		require.LessOrEqual(t, len(pointB), types.VarlenaInlineSize, st.name+"/pointB")
		require.Greater(t, len(longA), types.VarlenaInlineSize, st.name+"/longA")
		require.Greater(t, len(longB), types.VarlenaInlineSize, st.name+"/longB")
		// equalFn's fast path is reachable exactly when both areas are nil and
		// no select list masks rows; geometry collation keeps that live.
		require.False(t, types.IsUnicodeCollation(typ.Charset), st.name+"/collation")

		storageCase := func(shape string,
			leftBytes, rightBytes [][]byte, leftNull, rightNull []uint64,
			wantEq, wantNe []bool, wantNulls []bool,
			wantLeftArea, wantRightArea bool, wantLeftAreaLen, wantRightAreaLen int,
		) {
			rows := len(leftBytes)
			for _, op := range []struct {
				name string
				fn   executeLogicOfOverload
				want []bool
			}{
				{"notEqual", notEqualFn, wantNe},
				{"equal", equalFn, wantEq},
			} {
				// Per-case scoped call: every allocation gets its own immediate
				// deferred free at acquisition, so a Fatalf-capable shape
				// assertion or a panic inside Run can never bypass cleanup.
				// FunctionTestCase.Free is intentionally never called here:
				// one owner per resource, no double free, and the defers are
				// scoped to this call instead of accumulating over the loop.
				func() {
					mp := proc.Mp()
					lv := testutil.MakeVarlenaVector(leftBytes, leftNull, typ, mp)
					defer lv.Free(mp)
					rv := testutil.MakeVarlenaVector(rightBytes, rightNull, typ, mp)
					defer rv.Free(mp)
					require.Equal(t, rows, lv.Length(), st.name+"/"+shape+"/left rows")
					require.Equal(t, rows, rv.Length(), st.name+"/"+shape+"/right rows")
					if wantLeftArea {
						require.NotNil(t, lv.GetArea(), st.name+"/"+shape+"/left area")
						require.Equal(t, wantLeftAreaLen, len(lv.GetArea()), st.name+"/"+shape+"/left area bytes")
					} else {
						require.Nil(t, lv.GetArea(), st.name+"/"+shape+"/left no-area inline")
					}
					if wantRightArea {
						require.NotNil(t, rv.GetArea(), st.name+"/"+shape+"/right area")
						require.Equal(t, wantRightAreaLen, len(rv.GetArea()), st.name+"/"+shape+"/right area bytes")
					} else {
						require.Nil(t, rv.GetArea(), st.name+"/"+shape+"/right no-area inline")
					}
					fc := FunctionTestCase{proc: proc, parameters: []*vector.Vector{lv, rv}}
					fc.result = vector.NewFunctionResultWrapper(types.T_bool.ToType(), mp)
					defer func() { fc.result.Free() }()
					fc.fnLength = rows
					fc.expected = NewFunctionTestResult(types.T_bool.ToType(), false, op.want, wantNulls)
					fc.fn = op.fn
					ok, info := fc.Run()
					require.True(t, ok, st.name+"/"+shape+"/"+op.name+": "+info)
				}()
			}
		}

		// rows: (A,A) equal, (A,B) different, (NULL,B), (A,NULL).
		wantEq := []bool{true, false, false, false}
		wantNe := []bool{false, true, false, false}
		wantNulls := []bool{false, false, true, true}

		// Inline-only, no area on either side: equality runs its fast path and
		// the inequality must stay its complement.
		storageCase("inline-only",
			[][]byte{pointAb, pointAb, pointAb, pointAb},
			[][]byte{pointAb, pointBb, pointBb, pointBb},
			[]uint64{2}, []uint64{3},
			wantEq, wantNe, wantNulls,
			false, false, 0, 0)

		// External-only: every payload lives in the area.
		storageCase("external-only",
			[][]byte{longAb, longAb, longAb, longAb},
			[][]byte{longAb, longBb, longBb, longBb},
			[]uint64{2}, []uint64{3},
			wantEq, wantNe, wantNulls,
			true, true, 4*len(longA), 4*len(longB))

		// Mixed: inline rows share the vector with area-backed rows; the area
		// holds exactly the two long payloads.
		storageCase("mixed",
			[][]byte{pointAb, longAb, longAb, pointAb},
			[][]byte{pointAb, longBb, pointBb, longAb},
			nil, nil,
			[]bool{true, false, false, false},
			[]bool{false, true, true, true},
			[]bool{false, false, false, false},
			true, true, 2*len(longA), len(longB)+len(longA))

		// A const geometry operand and a const NULL operand against a column.
		for _, cc := range []struct {
			name     string
			value    string
			valueNil bool
			wantEq   []bool
			wantNe   []bool
			wantNull []bool
		}{
			{"const-value", pointA, false, []bool{true, false}, []bool{false, true}, []bool{false, false}},
			{"const-null", "", true, []bool{false, false}, []bool{false, false}, []bool{true, true}},
		} {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(typ, []string{pointA, pointB}, []bool{false, false}),
				NewFunctionTestConstInput(typ, []string{cc.value}, []bool{cc.valueNil}),
			}
			for _, op := range []struct {
				name string
				fn   executeLogicOfOverload
				want []bool
			}{
				{"notEqual", notEqualFn, cc.wantNe},
				{"equal", equalFn, cc.wantEq},
			} {
				expect := NewFunctionTestResult(types.T_bool.ToType(), false, op.want, cc.wantNull)
				tc := NewFunctionTestCase(proc, inputs, expect, op.fn)
				ok, info := tc.RunAndFree()
				require.True(t, ok, st.name+"/"+cc.name+"/"+op.name+": "+info)
			}
		}

		// Selected rows on an inline-only fixture: masked rows report NULL and
		// the evaluated row keeps the =/<> complement.
		{
			inputs := []FunctionTestInput{
				NewFunctionTestInput(typ, []string{pointA, pointA}, []bool{false, false}),
				NewFunctionTestInput(typ, []string{pointA, pointB}, []bool{false, false}),
			}
			for _, op := range []struct {
				name string
				fn   executeLogicOfOverload
				want []bool
			}{
				{"notEqual", notEqualFn, []bool{false, true}},
				{"equal", equalFn, []bool{true, false}},
			} {
				expect := NewFunctionTestResult(types.T_bool.ToType(), false, op.want, []bool{false, true})
				fc := NewFunctionTestCase(proc, inputs, expect, op.fn).
					WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{true, false}})
				ok, info := fc.RunAndFree()
				require.True(t, ok, st.name+"/inline-selected-rows/"+op.name+": "+info)
			}
		}
	}
}
