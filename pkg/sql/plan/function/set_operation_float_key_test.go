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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSetOperationFloatKey(t *testing.T) {
	negative32 := math.Float32frombits(1 << 31)
	negative64 := math.Float64frombits(1 << 63)
	for _, tc := range []struct {
		name  string
		input FunctionTestInput
		want  []uint64
		nulls []bool
		fn    executeLogicOfOverload
	}{
		{
			name: "float32",
			input: NewFunctionTestInput(types.T_float32.ToType(), []float32{
				negative32, 0, negative32, 1.25, -1.25,
				float32(math.Inf(1)), float32(math.Inf(-1)),
				math.Float32frombits(0x7fc00001), math.Float32frombits(0xffc00002), 0,
			}, []bool{false, false, false, false, false, false, false, false, false, true}),
			want:  []uint64{1 << 31, 0, 1 << 31, 0x3fa00000, 0xbfa00000, 0x7f800000, 0xff800000, 0x7fc00000, 0x7fc00000, 0},
			nulls: []bool{false, false, false, false, false, false, false, false, false, true},
			fn:    setOperationFloat32Key,
		},
		{
			name: "float64",
			input: NewFunctionTestInput(types.T_float64.ToType(), []float64{
				negative64, 0, negative64, 1.25, -1.25, math.Inf(1), math.Inf(-1),
				math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0xfff8000000000002), 0,
			}, []bool{false, false, false, false, false, false, false, false, false, true}),
			want:  []uint64{1 << 63, 0, 1 << 63, 0x3ff4000000000000, 0xbff4000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000000000, 0x7ff8000000000000, 0},
			nulls: []bool{false, false, false, false, false, false, false, false, false, true},
			fn:    setOperationFloat64Key,
		},
		{
			name:  "scaled-float32",
			input: NewFunctionTestInput(types.New(types.T_float32, 8, 2), []float32{negative32, 0, 1.231, 1.234, 0.004, -0.004}, nil),
			want:  []uint64{1 << 31, 0, uint64(math.Float32bits(1.23)), uint64(math.Float32bits(1.23)), 0, 0},
			fn:    setOperationFloat32Key,
		},
		{
			name:  "constant-float32",
			input: NewFunctionTestConstInput(types.T_float32.ToType(), []float32{negative32, negative32}, nil),
			want:  []uint64{1 << 31, 1 << 31},
			fn:    setOperationFloat32Key,
		},
		{
			name:  "constant-float64-null",
			input: NewFunctionTestConstInput(types.T_float64.ToType(), []float64{0, 0}, []bool{true, true}),
			want:  []uint64{0, 0},
			nulls: []bool{true, true},
			fn:    setOperationFloat64Key,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			fc := NewFunctionTestCase(proc, []FunctionTestInput{tc.input},
				NewFunctionTestResult(types.T_uint64.ToType(), false, tc.want, tc.nulls), tc.fn)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
	for _, typ := range []types.T{types.T_float32, types.T_float64} {
		got, err := GetFunctionByName(t.Context(), "set_operation_float_key", []types.Type{typ.ToType()})
		require.NoError(t, err)
		require.Equal(t, types.T_uint64, got.GetReturnType().Oid)
	}
	for _, typ := range []types.T{types.T_int64, types.T_decimal64, types.T_varchar} {
		_, err := GetFunctionByName(t.Context(), "set_operation_float_key", []types.Type{typ.ToType()})
		require.Error(t, err, "physical keys must not add implicit floating casts")
	}
}

func TestSetOperationFloatKeySelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, selection := range []*FunctionSelectList{
		{AnyNull: true, SelectList: []bool{false, true}}, {AllNull: true},
	} {
		fc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestConstInput(types.T_float64.ToType(), []float64{math.Copysign(0, -1), math.Copysign(0, -1)}, nil)},
			NewFunctionTestResult(types.T_uint64.ToType(), false, []uint64{0, 1 << 63}, []bool{true, selection.AllNull}),
			setOperationFloat64Key).WithSelectList(selection)
		ok, info := fc.RunAndFree()
		require.True(t, ok, info)
	}
	require.Zero(t, proc.Mp().CurrNB())
}
