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

func TestCastGeometryToSubtypeRejectSRIDMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT;SRID=4326"}, nil),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"SRID=0;POINT(1 2)"}, nil),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestCastGeometryToSubtypeRejectMalformedStructure(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, nil),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT("}, nil),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestCastGeometryToSubtypeRejectNonFinite(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, nil),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(Inf 0)"}, nil),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}

func TestCastGeometryToSubtypeRejectGeometryWrapper(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, nil),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"GEOMETRY(LINESTRING(0 0, 1 1))"}, nil),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), true, nil, nil)
	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.True(t, succeed, info)
}
