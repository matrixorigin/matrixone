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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestCastGeometryToSubtype(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name      string
		subtype   string
		payload   string
		wantErr   bool
		wantValue string
	}{
		{
			name:      "POINT matches POINT column",
			subtype:   "POINT",
			payload:   "POINT(1 2)",
			wantErr:   false,
			wantValue: "POINT(1 2)",
		},
		{
			name:      "LINESTRING matches LINESTRING column",
			subtype:   "LINESTRING",
			payload:   "LINESTRING(0 0, 1 1)",
			wantErr:   false,
			wantValue: "LINESTRING(0 0, 1 1)",
		},
		{
			name:      "POLYGON matches POLYGON column",
			subtype:   "POLYGON",
			payload:   "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
			wantErr:   false,
			wantValue: "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))",
		},
		{
			name:      "MULTIPOINT matches MULTIPOINT column",
			subtype:   "MULTIPOINT",
			payload:   "MULTIPOINT((0 0), (1 1))",
			wantErr:   false,
			wantValue: "MULTIPOINT((0 0), (1 1))",
		},
		{
			name:      "MULTILINESTRING matches MULTILINESTRING column",
			subtype:   "MULTILINESTRING",
			payload:   "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
			wantErr:   false,
			wantValue: "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
		},
		{
			name:      "MULTIPOLYGON matches MULTIPOLYGON column",
			subtype:   "MULTIPOLYGON",
			payload:   "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))",
			wantErr:   false,
			wantValue: "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))",
		},
		{
			name:      "GEOMETRYCOLLECTION matches GEOMETRYCOLLECTION column",
			subtype:   "GEOMETRYCOLLECTION",
			payload:   "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))",
			wantErr:   false,
			wantValue: "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1))",
		},
		{
			name:    "LINESTRING rejected in POINT column",
			subtype: "POINT",
			payload: "LINESTRING(0 0, 1 1)",
			wantErr: true,
		},
		{
			name:    "POINT rejected in POLYGON column",
			subtype: "POLYGON",
			payload: "POINT(1 2)",
			wantErr: true,
		},
		{
			name:      "any subtype allowed in GEOMETRY column",
			subtype:   "GEOMETRY",
			payload:   "LINESTRING(0 0, 1 1)",
			wantErr:   false,
			wantValue: "LINESTRING(0 0, 1 1)",
		},
		{
			name:      "any subtype allowed when column has no subtype constraint",
			subtype:   "",
			payload:   "POLYGON((0 0, 1 0, 1 1, 0 0))",
			wantErr:   false,
			wantValue: "POLYGON((0 0, 1 0, 1 1, 0 0))",
		},
		{
			name:      "POINT EMPTY accepted in POINT column",
			subtype:   "POINT",
			payload:   "POINT EMPTY",
			wantErr:   false,
			wantValue: "POINT EMPTY",
		},
		{
			name:      "GEOMETRYCOLLECTION EMPTY accepted",
			subtype:   "GEOMETRYCOLLECTION",
			payload:   "GEOMETRYCOLLECTION EMPTY",
			wantErr:   false,
			wantValue: "GEOMETRYCOLLECTION EMPTY",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.subtype}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			if tc.wantErr {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			} else {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.wantValue}, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			}
		})
	}
}

func TestCastGeometryToSubtypeSRID(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name      string
		subtype   string
		payload   string
		wantErr   bool
		wantValue string
	}{
		{
			name:    "SRID mismatch rejected",
			subtype: "POINT;SRID=4326",
			payload: "SRID=0;POINT(1 2)",
			wantErr: true,
		},
		{
			name:      "SRID matches",
			subtype:   "POINT;SRID=4326",
			payload:   "SRID=4326;POINT(1 2)",
			wantErr:   false,
			wantValue: "SRID=4326;POINT(1 2)",
		},
		{
			name:    "no SRID in value rejected when column requires SRID",
			subtype: "POINT;SRID=4326",
			payload: "POINT(1 2)",
			wantErr: true,
		},
		{
			name:    "value has SRID but column expects different",
			subtype: ";SRID=4326",
			payload: "SRID=3857;POINT(1 2)",
			wantErr: true,
		},
		{
			name:      "no column SRID allows any value SRID",
			subtype:   "POINT",
			payload:   "SRID=4326;POINT(1 2)",
			wantErr:   false,
			wantValue: "SRID=4326;POINT(1 2)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.subtype}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			if tc.wantErr {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			} else {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.wantValue}, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			}
		})
	}
}

func TestCastGeometryToSubtypeRejectMalformed(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name    string
		subtype string
		payload string
	}{
		{name: "unclosed paren", subtype: "POINT", payload: "POINT("},
		{name: "empty payload", subtype: "POINT", payload: ""},
		{name: "invalid type name", subtype: "POINT", payload: "FOOBAR(1 2)"},
		{name: "non-finite Inf", subtype: "POINT", payload: "POINT(Inf 0)"},
		{name: "non-finite NaN", subtype: "POINT", payload: "POINT(NaN 0)"},
		{name: "linestring too few points", subtype: "LINESTRING", payload: "LINESTRING(0 0)"},
		{name: "polygon ring too few points", subtype: "POLYGON", payload: "POLYGON((0 0, 1 0))"},
		{name: "malformed SRID prefix", subtype: "POINT", payload: "SRID=;POINT(1 2)"},
		{name: "GEOMETRY wrapper rejected in POINT column", subtype: "POINT", payload: "GEOMETRY(LINESTRING(0 0, 1 1))"},
		{name: "three coords in point", subtype: "POINT", payload: "POINT(1 2 3)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.subtype}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
			tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
			succeed, info := tcc.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCastGeometryToSubtypeNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{""}, []bool{true}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true})
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestCastGeometryToSubtypeSRIDAssign(t *testing.T) {
	proc := testutil.NewProcess(t)
	// value SRID=0, column SRID=0 => should pass and assign SRID
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT;SRID=0"}, nil),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, nil),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"SRID=0;POINT(1 2)"}, nil)
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestCastGeometryToSubtypeMultiPointValidation(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name    string
		subtype string
		payload string
		wantErr bool
		want    string
	}{
		{
			name:    "multipoint with bare coords",
			subtype: "MULTIPOINT",
			payload: "MULTIPOINT(0 0, 1 1, 2 2)",
			wantErr: false,
			want:    "MULTIPOINT(0 0, 1 1, 2 2)",
		},
		{
			name:    "multipoint empty content",
			subtype: "MULTIPOINT",
			payload: "MULTIPOINT EMPTY",
			wantErr: false,
			want:    "MULTIPOINT EMPTY",
		},
		{
			name:    "multipoint with nested POINT keyword",
			subtype: "MULTIPOINT",
			payload: "MULTIPOINT(POINT(0 0), POINT(1 1))",
			wantErr: false,
			want:    "MULTIPOINT(POINT(0 0), POINT(1 1))",
		},
		{
			name:    "multipoint with parenthesized coords",
			subtype: "MULTIPOINT",
			payload: "MULTIPOINT((0 0), (1 1))",
			wantErr: false,
			want:    "MULTIPOINT((0 0), (1 1))",
		},
		{
			name:    "multipoint with invalid nested type",
			subtype: "MULTIPOINT",
			payload: "MULTIPOINT(LINESTRING(0 0, 1 1))",
			wantErr: true,
		},
		{
			name:    "multipoint with malformed parens",
			subtype: "MULTIPOINT",
			payload: "MULTIPOINT((0 0)",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{tc.subtype}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			if tc.wantErr {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			} else {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.want}, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			}
		})
	}
}

func TestCastGeometryToSubtypeMultiLineStringValidation(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name    string
		payload string
		wantErr bool
		want    string
	}{
		{
			name:    "valid multilinestring",
			payload: "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
			wantErr: false,
			want:    "MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
		},
		{
			name:    "multilinestring empty",
			payload: "MULTILINESTRING EMPTY",
			wantErr: false,
			want:    "MULTILINESTRING EMPTY",
		},
		{
			name:    "multilinestring missing inner parens",
			payload: "MULTILINESTRING(0 0, 1 1)",
			wantErr: true,
		},
		{
			name:    "multilinestring inner too few points",
			payload: "MULTILINESTRING((0 0))",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"MULTILINESTRING"}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			if tc.wantErr {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			} else {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.want}, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			}
		})
	}
}

func TestCastGeometryToSubtypeMultiPolygonValidation(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name    string
		payload string
		wantErr bool
		want    string
	}{
		{
			name:    "valid multipolygon",
			payload: "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
			wantErr: false,
			want:    "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))",
		},
		{
			name:    "multipolygon empty",
			payload: "MULTIPOLYGON EMPTY",
			wantErr: false,
			want:    "MULTIPOLYGON EMPTY",
		},
		{
			name:    "multipolygon missing inner parens",
			payload: "MULTIPOLYGON((0 0, 1 0, 1 1, 0 0))",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"MULTIPOLYGON"}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			if tc.wantErr {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			} else {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.want}, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			}
		})
	}
}

func TestCastGeometryToSubtypeGeometryCollectionValidation(t *testing.T) {
	proc := testutil.NewProcess(t)

	cases := []struct {
		name    string
		payload string
		wantErr bool
		want    string
	}{
		{
			name:    "nested collection",
			payload: "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)))",
			wantErr: false,
			want:    "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)))",
		},
		{
			name:    "collection with multiple types",
			payload: "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1), POLYGON((0 0, 1 0, 1 1, 0 0)))",
			wantErr: false,
			want:    "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1), POLYGON((0 0, 1 0, 1 1, 0 0)))",
		},
		{
			name:    "empty collection",
			payload: "GEOMETRYCOLLECTION EMPTY",
			wantErr: false,
			want:    "GEOMETRYCOLLECTION EMPTY",
		},
		{
			name:    "collection with invalid member",
			payload: "GEOMETRYCOLLECTION(FOOBAR(1 2))",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inputs := []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"GEOMETRYCOLLECTION"}, nil),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{tc.payload}, nil),
			}
			if tc.wantErr {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			} else {
				expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.want}, nil)
				tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
				succeed, info := tcc.Run()
				require.True(t, succeed, info)
			}
		})
	}
}

func TestCastGeometryToSubtypePointLimit(t *testing.T) {
	proc := testutil.NewProcess(t)
	// Build a linestring with many points to test the point count validation.
	// defaultMaxPointsInGeometry is 65536; we use a much smaller one via a
	// long linestring that exceeds what a reasonable limit would allow.
	// Since we can't easily set max_points_in_geometry via process variable in
	// test, we just test a valid large geometry passes (exercising the count path).
	payload := "LINESTRING(0 0, 1 1, 2 2, 3 3, 4 4, 5 5, 6 6, 7 7, 8 8, 9 9)"
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"LINESTRING"}, nil),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{payload}, nil),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{payload}, nil)
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestValidateGeometryHelpers(t *testing.T) {
	t.Run("encodeGeometryPayload without SRID", func(t *testing.T) {
		result := encodeGeometryPayload("POINT(1 2)", 0, false)
		require.Equal(t, "POINT(1 2)", string(result))
	})

	t.Run("encodeGeometryPayload with SRID", func(t *testing.T) {
		result := encodeGeometryPayload("POINT(1 2)", 4326, true)
		require.Equal(t, "SRID=4326;POINT(1 2)", string(result))
	})

	t.Run("decodeGeometryPayload with SRID", func(t *testing.T) {
		wkt, srid, defined, err := decodeGeometryPayload([]byte("SRID=4326;POINT(1 2)"))
		require.NoError(t, err)
		require.Equal(t, "POINT(1 2)", wkt)
		require.Equal(t, uint32(4326), srid)
		require.True(t, defined)
	})

	t.Run("decodeGeometryPayload without SRID", func(t *testing.T) {
		wkt, srid, defined, err := decodeGeometryPayload([]byte("POINT(1 2)"))
		require.NoError(t, err)
		require.Equal(t, "POINT(1 2)", wkt)
		require.Equal(t, uint32(0), srid)
		require.False(t, defined)
	})

	t.Run("decodeGeometryPayload malformed SRID", func(t *testing.T) {
		_, _, _, err := decodeGeometryPayload([]byte("SRID=abc;POINT(1 2)"))
		require.Error(t, err)
	})

	t.Run("decodeGeometryPayload SRID no semicolon", func(t *testing.T) {
		_, _, _, err := decodeGeometryPayload([]byte("SRID=4326"))
		require.Error(t, err)
	})

	t.Run("decodeGeometryPayload SRID empty WKT", func(t *testing.T) {
		_, _, _, err := decodeGeometryPayload([]byte("SRID=4326; "))
		require.Error(t, err)
	})

	t.Run("geometryTypeNameFromText valid types", func(t *testing.T) {
		for _, name := range []string{"POINT(1 2)", "LINESTRING(0 0, 1 1)", "POLYGON((0 0, 1 0, 1 1, 0 0))",
			"MULTIPOINT(0 0)", "MULTILINESTRING((0 0, 1 1))", "MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))",
			"GEOMETRYCOLLECTION(POINT(0 0))"} {
			_, err := geometryTypeNameFromText(name)
			require.NoError(t, err, "should parse: %s", name)
		}
	})

	t.Run("geometryTypeNameFromText EMPTY variants", func(t *testing.T) {
		for _, name := range []string{"POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY",
			"MULTIPOINT EMPTY", "MULTILINESTRING EMPTY", "MULTIPOLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY"} {
			typeName, err := geometryTypeNameFromText(name)
			require.NoError(t, err, "should parse: %s", name)
			require.NotEmpty(t, typeName)
		}
	})

	t.Run("geometryTypeNameFromText invalid EMPTY", func(t *testing.T) {
		_, err := geometryTypeNameFromText("FOOBAR EMPTY")
		require.Error(t, err)
	})

	t.Run("splitTopLevelGeometryItemsStrict unbalanced parens", func(t *testing.T) {
		_, err := splitTopLevelGeometryItemsStrict("(0 0, (1 1)", "err")
		require.Error(t, err)
	})

	t.Run("splitTopLevelGeometryItemsStrict empty item", func(t *testing.T) {
		_, err := splitTopLevelGeometryItemsStrict("0 0,,1 1", "err")
		require.Error(t, err)
	})

	t.Run("splitTopLevelGeometryItemsStrict trailing comma", func(t *testing.T) {
		_, err := splitTopLevelGeometryItemsStrict("0 0, 1 1, ", "err")
		require.Error(t, err)
	})

	t.Run("validateFiniteCoordinatesInGeometryText valid", func(t *testing.T) {
		err := validateFiniteCoordinatesInGeometryText("POINT(1.5 -2.3)")
		require.NoError(t, err)
	})

	t.Run("validateFiniteCoordinatesInGeometryText with non-numeric tokens", func(t *testing.T) {
		// Non-numeric tokens like type names are not floats and not Inf/NaN, so they pass
		err := validateFiniteCoordinatesInGeometryText("POINT(1 2)")
		require.NoError(t, err)
	})

	t.Run("maxPointsInGeometryLimit nil proc", func(t *testing.T) {
		limit := maxPointsInGeometryLimit(nil)
		require.Equal(t, defaultMaxPointsInGeometry, limit)
	})

	t.Run("maxPointsInGeometryLimit with int64 variable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
			if varName == "max_points_in_geometry" {
				return int64(100), nil
			}
			return nil, nil
		})
		limit := maxPointsInGeometryLimit(proc)
		require.Equal(t, int64(100), limit)
	})

	t.Run("maxPointsInGeometryLimit with int32 variable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
			if varName == "max_points_in_geometry" {
				return int32(200), nil
			}
			return nil, nil
		})
		limit := maxPointsInGeometryLimit(proc)
		require.Equal(t, int64(200), limit)
	})

	t.Run("maxPointsInGeometryLimit with uint64 variable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
			if varName == "max_points_in_geometry" {
				return uint64(300), nil
			}
			return nil, nil
		})
		limit := maxPointsInGeometryLimit(proc)
		require.Equal(t, int64(300), limit)
	})

	t.Run("maxPointsInGeometryLimit with uint32 variable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
			if varName == "max_points_in_geometry" {
				return uint32(400), nil
			}
			return nil, nil
		})
		limit := maxPointsInGeometryLimit(proc)
		require.Equal(t, int64(400), limit)
	})

	t.Run("maxPointsInGeometryLimit with int variable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
			if varName == "max_points_in_geometry" {
				return int(500), nil
			}
			return nil, nil
		})
		limit := maxPointsInGeometryLimit(proc)
		require.Equal(t, int64(500), limit)
	})

	t.Run("maxPointsInGeometryLimit with uint variable", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
			if varName == "max_points_in_geometry" {
				return uint(600), nil
			}
			return nil, nil
		})
		limit := maxPointsInGeometryLimit(proc)
		require.Equal(t, int64(600), limit)
	})

	t.Run("validateGeometryTextForStorage exceeds point limit", func(t *testing.T) {
		err := validateGeometryTextForStorage("LINESTRING(0 0, 1 1, 2 2)", 2)
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds max_points_in_geometry")
	})

	t.Run("validateGeometryTextForStorage zero limit skips count", func(t *testing.T) {
		err := validateGeometryTextForStorage("LINESTRING(0 0, 1 1, 2 2)", 0)
		require.NoError(t, err)
	})

	t.Run("validateGenericGeometryTextContent valid", func(t *testing.T) {
		err := validateGenericGeometryTextContent("POINT(1 2)", 0)
		require.NoError(t, err)
	})

	t.Run("validateGenericGeometryTextContent multiple items rejected", func(t *testing.T) {
		err := validateGenericGeometryTextContent("POINT(1 2), POINT(3 4)", 0)
		require.Error(t, err)
	})

	t.Run("validateGeometryCollectionTextContent exceeds nesting depth", func(t *testing.T) {
		err := validateGeometryCollectionTextContent("POINT(0 0)", maxGeometryCollectionNestingDepth)
		require.Error(t, err)
		require.Contains(t, err.Error(), "nesting depth")
	})

	t.Run("multiLineStringPointCountFromTextContent empty", func(t *testing.T) {
		count, err := multiLineStringPointCountFromTextContent("")
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("multiLineStringPointCountFromTextContent valid", func(t *testing.T) {
		count, err := multiLineStringPointCountFromTextContent("(0 0, 1 1), (2 2, 3 3, 4 4)")
		require.NoError(t, err)
		require.Equal(t, int64(5), count)
	})

	t.Run("multiLineStringPointCountFromTextContent malformed", func(t *testing.T) {
		_, err := multiLineStringPointCountFromTextContent("0 0, 1 1")
		require.Error(t, err)
	})

	t.Run("multiPolygonPointCountFromTextContent empty", func(t *testing.T) {
		count, err := multiPolygonPointCountFromTextContent("")
		require.NoError(t, err)
		require.Equal(t, int64(0), count)
	})

	t.Run("multiPolygonPointCountFromTextContent valid", func(t *testing.T) {
		count, err := multiPolygonPointCountFromTextContent("((0 0, 1 0, 1 1, 0 0))")
		require.NoError(t, err)
		require.Equal(t, int64(4), count)
	})

	t.Run("multiPolygonPointCountFromTextContent malformed", func(t *testing.T) {
		_, err := multiPolygonPointCountFromTextContent("0 0, 1 0")
		require.Error(t, err)
	})

	t.Run("genericGeometryPointCountFromTextContent valid", func(t *testing.T) {
		count, err := genericGeometryPointCountFromTextContent("POINT(1 2)", 0)
		require.NoError(t, err)
		require.Equal(t, int64(1), count)
	})

	t.Run("genericGeometryPointCountFromTextContent multiple items", func(t *testing.T) {
		_, err := genericGeometryPointCountFromTextContent("POINT(1 2), POINT(3 4)", 0)
		require.Error(t, err)
	})
}
