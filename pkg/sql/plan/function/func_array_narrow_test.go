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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// summation, l1_norm and l2_norm take the narrow vector types directly and return DOUBLE;
// subvector slices them and keeps their type (#31666).
func TestNarrowArrayReductionsAndSubvector(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	bf16 := func(v ...float32) []types.BF16 { return types.Float32ToBF16Slice(v) }
	f16 := func(v ...float32) []types.Float16 { return types.Float32ToFloat16Slice(v) }
	i64 := func(v int64) FunctionTestInput { return NewFunctionTestInput(types.T_int64.ToType(), []int64{v}, nil) }
	nulls := []bool{false, false, true}
	double := types.T_float64.ToType()

	type tc struct {
		name, function string
		inputs         []FunctionTestInput
		expected       FunctionTestResult
	}
	var cases []tc
	reductions := []struct {
		typ               types.T
		in                any
		summation, l1, l2 []float64
	}{
		{types.T_array_bf16, [][]types.BF16{bf16(3, 4, 0), bf16(-1.5, 2, 0.25), bf16(1)},
			[]float64{7, 0.75, 0}, []float64{7, 3.75, 0}, []float64{5, math.Sqrt(6.3125), 0}},
		{types.T_array_float16, [][]types.Float16{f16(3, 4, 0), f16(-1.5, 2, 0.25), f16(1)},
			[]float64{7, 0.75, 0}, []float64{7, 3.75, 0}, []float64{5, math.Sqrt(6.3125), 0}},
		{types.T_array_int8, [][]int8{{3, 4, 0}, {-128, 127, 0}, {1}},
			[]float64{7, -1, 0}, []float64{7, 255, 0}, []float64{5, math.Sqrt(32513), 0}},
		{types.T_array_uint8, [][]uint8{{3, 4, 0}, {255, 255, 0}, {1}},
			[]float64{7, 510, 0}, []float64{7, 510, 0}, []float64{5, math.Sqrt(130050), 0}},
	}
	for _, r := range reductions {
		for _, f := range []struct {
			name string
			want []float64
		}{{"summation", r.summation}, {"l1_norm", r.l1}, {"l2_norm", r.l2}} {
			cases = append(cases, tc{
				name: f.name + "/" + r.typ.String(), function: f.name,
				inputs:   []FunctionTestInput{NewFunctionTestInput(r.typ.ToType(), r.in, nulls)},
				expected: NewFunctionTestResult(double, false, f.want, nulls),
			})
		}
	}

	slices := []struct {
		typ                                types.T
		in, from2, last, first2, mid, none any
	}{
		{types.T_array_bf16, [][]types.BF16{bf16(1.5, -2, 3)},
			[][]types.BF16{bf16(-2, 3)}, [][]types.BF16{bf16(3)}, [][]types.BF16{bf16(1.5, -2)}, [][]types.BF16{bf16(-2)}, [][]types.BF16{{}}},
		{types.T_array_float16, [][]types.Float16{f16(1.5, -2, 3)},
			[][]types.Float16{f16(-2, 3)}, [][]types.Float16{f16(3)}, [][]types.Float16{f16(1.5, -2)}, [][]types.Float16{f16(-2)}, [][]types.Float16{{}}},
		{types.T_array_int8, [][]int8{{-128, 0, 127}},
			[][]int8{{0, 127}}, [][]int8{{127}}, [][]int8{{-128, 0}}, [][]int8{{0}}, [][]int8{{}}},
		{types.T_array_uint8, [][]uint8{{0, 128, 255}},
			[][]uint8{{128, 255}}, [][]uint8{{255}}, [][]uint8{{0, 128}}, [][]uint8{{128}}, [][]uint8{{}}},
	}
	for _, s := range slices {
		vec := func() FunctionTestInput { return NewFunctionTestInput(s.typ.ToType(), s.in, nil) }
		for _, c := range []struct {
			name string
			args []FunctionTestInput
			want any
		}{
			{"start2", []FunctionTestInput{vec(), i64(2)}, s.from2},
			{"start-1", []FunctionTestInput{vec(), i64(-1)}, s.last},
			{"start0", []FunctionTestInput{vec(), i64(0)}, s.none},
			{"start1_len2", []FunctionTestInput{vec(), i64(1), i64(2)}, s.first2},
			{"start-2_len1", []FunctionTestInput{vec(), i64(-2), i64(1)}, s.mid},
			{"start4_len1", []FunctionTestInput{vec(), i64(4), i64(1)}, s.none},
			{"start2_lenMax", []FunctionTestInput{vec(), i64(2), i64(math.MaxInt64)}, s.from2},
			{"startMin", []FunctionTestInput{vec(), i64(math.MinInt64)}, s.none},
			{"startMin_len1", []FunctionTestInput{vec(), i64(math.MinInt64), i64(1)}, s.none},
		} {
			cases = append(cases, tc{
				name: "subvector/" + s.typ.String() + "/" + c.name, function: "subvector",
				inputs:   c.args,
				expected: NewFunctionTestResult(s.typ.ToType(), false, c.want, nil),
			})
		}
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			args := make([]types.Type, len(c.inputs))
			for i := range c.inputs {
				args[i] = c.inputs[i].typ
			}
			resolved, err := GetFunctionByName(proc.Ctx, c.function, args)
			require.NoError(t, err)
			_, cast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, cast, "the narrow type is taken directly")
			require.Equal(t, c.expected.typ.Oid, resolved.GetReturnType().Oid)
			registered, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
			require.NoError(t, err)
			execute, _, _, _ := registered.GetExecuteMethod()
			fc := NewFunctionTestCase(proc, c.inputs, c.expected, execute)
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
}
