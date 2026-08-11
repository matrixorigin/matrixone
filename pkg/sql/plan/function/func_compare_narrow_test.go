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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// TestNarrowArrayCompareSupports covers the narrow-vector (bf16/f16/int8/uint8)
// branches of the comparison-operator type-support gates.
func TestNarrowArrayCompareSupports(t *testing.T) {
	for _, oid := range []types.T{
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
	} {
		typ := oid.ToType()
		require.True(t, equalAndNotEqualOperatorSupports(typ, typ), oid.String())
		require.True(t, otherCompareOperatorSupports(typ, typ), oid.String())
	}
}

type narrowCompareFn = func([]*vector.Vector, vector.FunctionResultWrapper, *process.Process, int, *FunctionSelectList) error

// runNarrowCompareOps drives every comparison operator over three rows — (b,a),
// (a,b), (a,a) with a < b elementwise — and checks each operator's narrow-vector
// branch against the expected boolean pattern.
func runNarrowCompareOps[T types.ArrayElement](t *testing.T, proc *process.Process, oid types.T, a, b []T) {
	ops := []struct {
		name string
		fn   narrowCompareFn
		exp  []bool // results for rows (b?a), (a?b), (a?a)
	}{
		{"equal", equalFn, []bool{false, false, true}},
		{"notEqual", notEqualFn, []bool{true, true, false}},
		{"greatThan", greatThanFn, []bool{true, false, false}},
		{"greatEqual", greatEqualFn, []bool{true, false, true}},
		{"lessThan", lessThanFn, []bool{false, true, false}},
		{"lessEqual", lessEqualFn, []bool{false, true, true}},
	}
	for _, op := range ops {
		inputs := []FunctionTestInput{
			NewFunctionTestInput(oid.ToType(), [][]T{b, a, a}, []bool{false, false, false}),
			NewFunctionTestInput(oid.ToType(), [][]T{a, b, a}, []bool{false, false, false}),
		}
		expect := NewFunctionTestResult(types.T_bool.ToType(), false, op.exp, []bool{false, false, false})
		fc := NewFunctionTestCase(proc, inputs, expect, op.fn)
		ok, info := fc.Run()
		require.True(t, ok, info, oid.String()+"/"+op.name)
	}
}

// TestNarrowArrayCompareOps covers the narrow-vector branches of every comparison
// operator (=, <>, >, >=, <, <=) for bf16/f16/int8/uint8.
func TestNarrowArrayCompareOps(t *testing.T) {
	proc := testutil.NewProcess(t)
	runNarrowCompareOps(t, proc, types.T_array_bf16,
		types.Float32ToBF16Slice([]float32{1, 2}), types.Float32ToBF16Slice([]float32{3, 4}))
	runNarrowCompareOps(t, proc, types.T_array_float16,
		types.Float32ToFloat16Slice([]float32{1, 2}), types.Float32ToFloat16Slice([]float32{3, 4}))
	runNarrowCompareOps(t, proc, types.T_array_int8, []int8{1, 2}, []int8{3, 4})
	runNarrowCompareOps(t, proc, types.T_array_uint8, []uint8{1, 2}, []uint8{3, 4})
}
