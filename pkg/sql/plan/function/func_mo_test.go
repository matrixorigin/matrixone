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

	testCases := []tcTemp{
		{
			info: "point fits point column",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 2)"}, []bool{false}),
		},
		{
			info: "generic geometry column accepts point",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"GEOMETRY"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{false}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 2)"}, []bool{false}),
		},
		{
			info: "null payload propagates",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
				NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1 2)"}, []bool{true}),
			},
			expect: NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			tcc := NewFunctionTestCase(proc, tc.inputs, tc.expect, CastGeometryToSubtype)
			succeed, info := tcc.Run()
			require.True(t, succeed, info)
		})
	}
}

func TestCastGeometryToSubtypeRejectMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"LINESTRING(0 0,1 1)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "cannot store LINESTRING in POINT column")
}

func TestCastGeometryToSubtypeRejectSRIDMismatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT;SRID=4326"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"SRID=0;POINT(1 2)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "The SRID of the geometry does not match the SRID of the column")
}

func TestCastGeometryToSubtypeRejectSRIDMismatchGenericGeometryLabel(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"SRID=4326"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"SRID=0;POINT(1 2)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "column 'GEOMETRY'")
	require.Contains(t, info, "SRID of the column is 4326")
}

func TestCastGeometryToSubtypeRejectNonFinite(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(NaN 1)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "invalid geometry payload")
}

func TestCastGeometryToSubtypeRejectMalformedStructure(t *testing.T) {
	proc := testutil.NewProcess(t)
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"POINT"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"POINT(1"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "invalid geometry payload")
}

func TestCastGeometryToSubtypeRejectTooManyPoints(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(varName string, isSystemVar, isGlobalVar bool) (interface{}, error) {
		if varName == "max_points_in_geometry" {
			return int64(3), nil
		}
		return nil, nil
	})
	inputs := []FunctionTestInput{
		NewFunctionTestInput(types.T_varchar.ToType(), []string{"LINESTRING"}, []bool{false}),
		NewFunctionTestInput(types.T_geometry.ToType(), []string{"LINESTRING(0 0,1 1,2 2,3 3)"}, []bool{false}),
	}
	expect := NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{false})

	tcc := NewFunctionTestCase(proc, inputs, expect, CastGeometryToSubtype)
	succeed, info := tcc.Run()
	require.False(t, succeed)
	require.Contains(t, info, "max_points_in_geometry=3")
}
