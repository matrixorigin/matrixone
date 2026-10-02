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
	"github.com/stretchr/testify/require"
)

// TestLowPrecisionFloatComparisonKernels checks the comparison operators on two operands
// of the same bf16, float16, float8 or float4 type, compared by value: -0 equals +0, and
// a NULL row yields NULL except for <=>.
func TestLowPrecisionFloatComparisonKernels(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	// rows: (1, 2), (2, 1), (+0, -0), (1.5, 1.5), (NULL, 1)
	nulls := []bool{false, false, false, false, true}
	inputsFor := func(oid types.T) []FunctionTestInput {
		switch oid {
		case types.T_bf16:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.BF16{types.BF16FromFloat32(1), types.BF16FromFloat32(2), 0, types.BF16FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.BF16{types.BF16FromFloat32(2), types.BF16FromFloat32(1), 0x8000, types.BF16FromFloat32(1.5), types.BF16FromFloat32(1)}, nil),
			}
		case types.T_float16:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.Float16{types.Float16FromFloat32(1), types.Float16FromFloat32(2), 0, types.Float16FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.Float16{types.Float16FromFloat32(2), types.Float16FromFloat32(1), 0x8000, types.Float16FromFloat32(1.5), types.Float16FromFloat32(1)}, nil),
			}
		case types.T_float8:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.Float8{types.Float8FromFloat32(1), types.Float8FromFloat32(2), 0, types.Float8FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.Float8{types.Float8FromFloat32(2), types.Float8FromFloat32(1), 0x80, types.Float8FromFloat32(1.5), types.Float8FromFloat32(1)}, nil),
			}
		default:
			return []FunctionTestInput{
				NewFunctionTestInput(oid.ToType(), []types.Float4{types.Float4FromFloat32(1), types.Float4FromFloat32(2), 0, types.Float4FromFloat32(1.5), 0}, nulls),
				NewFunctionTestInput(oid.ToType(), []types.Float4{types.Float4FromFloat32(2), types.Float4FromFloat32(1), 0x08, types.Float4FromFloat32(1.5), types.Float4FromFloat32(1)}, nil),
			}
		}
	}
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		for _, test := range []struct {
			name      string
			fn        fEvalFn
			want      []bool
			wantNulls []bool
		}{
			{"equal", equalFn, []bool{false, false, true, true, false}, nulls},
			{"null safe equal", nullSafeEqualFn, []bool{false, false, true, true, false}, nil},
			{"not equal", notEqualFn, []bool{true, true, false, false, false}, nulls},
			{"greater than", greatThanFn, []bool{false, true, false, false, false}, nulls},
			{"greater equal", greatEqualFn, []bool{false, true, true, true, false}, nulls},
			{"less than", lessThanFn, []bool{true, false, false, false, false}, nulls},
			{"less equal", lessEqualFn, []bool{true, false, true, true, false}, nulls},
		} {
			expect := NewFunctionTestResult(types.T_bool.ToType(), false, test.want, test.wantNulls)
			testCase := NewFunctionTestCase(proc, inputsFor(oid), expect, test.fn)
			ok, info := testCase.Run()
			require.True(t, ok, "%s %s: %s", oid, test.name, info)
		}
	}
}

// TestCastNullToLowPrecisionFloat checks CAST(NULL AS bf16/float16/float8/float4).
func TestCastNullToLowPrecisionFloat(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, oid := range []types.T{types.T_bf16, types.T_float16, types.T_float8, types.T_float4} {
		nullVec := vector.NewConstNull(types.T_any.ToType(), 2, proc.Mp())
		target := vector.NewConstNull(oid.ToType(), 2, proc.Mp())
		result := vector.NewFunctionResultWrapper(oid.ToType(), proc.Mp())
		require.NoError(t, result.PreExtendAndReset(2))
		require.NoError(t, NewCast([]*vector.Vector{nullVec, target}, result, proc, 2, nil), oid.String())
		out := result.GetResultVector()
		require.Equal(t, oid, out.GetType().Oid)
		require.True(t, out.IsNull(0) && out.IsNull(1), oid.String())
		result.Free()
		nullVec.Free(proc.Mp())
		target.Free(proc.Mp())
	}
}
