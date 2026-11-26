// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

// #33196: ST_Intersects and ST_Disjoint return NULL when either geometry is
// empty instead of failing with "invalid point payload".
func TestStIntersectsDisjointEmptyGeometryIsNull33196(t *testing.T) {
	proc := testutil.NewProcess(t)
	left := []string{
		"POINT(0 0)",
		"POINT EMPTY",
		"POLYGON((0 0,2 0,2 2,0 2,0 0))",
		"LINESTRING EMPTY",
		"GEOMETRYCOLLECTION EMPTY",
		"MULTIPOINT EMPTY",
		"GEOMETRYCOLLECTION(POINT EMPTY)",
		"POINT EMPTY",
		"POINT(0 0)",
		"POINT(0 0)",
	}
	right := []string{
		"POINT EMPTY",
		"POINT(0 0)",
		"POINT EMPTY",
		"POINT(0 0)",
		"POINT(0 0)",
		"POINT(0 0)",
		"POINT(0 0)",
		"POINT EMPTY",
		"POINT(0 0)",
		"POINT(1 1)",
	}
	nulls := []bool{true, true, true, true, true, true, true, true, false, false}
	for _, tc := range []struct {
		name string
		fn   func([]FunctionTestInput, FunctionTestResult) FunctionTestCase
		want []bool
	}{
		{"st_intersects", func(in []FunctionTestInput, out FunctionTestResult) FunctionTestCase {
			return NewFunctionTestCase(proc, in, out, StIntersects)
		}, []bool{false, false, false, false, false, false, false, false, true, false}},
		{"st_disjoint", func(in []FunctionTestInput, out FunctionTestResult) FunctionTestCase {
			return NewFunctionTestCase(proc, in, out, StDisjoint)
		}, []bool{false, false, false, false, false, false, false, false, false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := tc.fn(
				[]FunctionTestInput{
					NewFunctionTestInput(types.T_geometry.ToType(), left, nil),
					NewFunctionTestInput(types.T_geometry.ToType(), right, nil),
				},
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, nulls))
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}
