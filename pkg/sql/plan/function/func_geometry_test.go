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
