// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

// Share the unchanged process configuration; each serial case owns fresh vectors.
func TestArrayMath(t *testing.T) {
	proc := newMemoryFunctionTestProcess(t)
	cases := []struct {
		name, function string
		inputs         []FunctionTestInput
		expected       FunctionTestResult
		errorText      string
	}{
		{
			name: "abs/f32/mixed_sign_zero", function: "abs",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{-4, 9999999, -99999}, {0, -25, 49}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{4, 9999999, 99999}, {0, 25, 49}},
				nil),
		},
		{
			name: "abs/f64/mixed_sign_zero", function: "abs",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{-4, 9999999, -99999}, {0, -25, 49}, {-16777217}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{4, 9999999, 99999}, {0, 25, 49}, {16777217}},
				nil),
		},
		{
			name: "normalize_l2/f32/signed_fractional_null", function: "normalize_l2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{
						{},
						{1, 2, 3, 4},
						{-1, 2, 3, 4},
						{10, 3.333333333333333, 4, 5},
						{1, 2, 3.6666666666666665, 4.666666666666666}},
					[]bool{true, false, false, false, false}),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{},
					{0.18257418, 0.36514837, 0.5477226, 0.73029673},
					{-0.18257418, 0.36514837, 0.5477226, 0.73029673},
					{0.8108108, 0.27027026, 0.32432434, 0.4054054},
					{0.1576765, 0.315353, 0.5781472, 0.73582363},
				},
				[]bool{true, false, false, false, false}),
		},
		{
			name: "normalize_l2/f64/signed_fractional_null", function: "normalize_l2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{
						{},
						{1, 2, 3, 4},
						{-1, 2, 3, 4},
						{10, 3.333333333333333, 4, 5},
						{1, 2, 3.6666666666666665, 4.666666666666666},
					},
					[]bool{true, false, false, false, false}),
			},
			expected: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{
					{},
					{0.18257418583505536, 0.3651483716701107, 0.5477225575051661, 0.7302967433402214},
					{-0.18257418583505536, 0.3651483716701107, 0.5477225575051661, 0.7302967433402214},
					{0.8108108108108107, 0.27027027027027023, 0.3243243243243243, 0.4054054054054054},
					{0.15767649936829103, 0.31535299873658207, 0.5781471643504004, 0.7358236637186913},
				},
				[]bool{true, false, false, false, false}),
		},
		{
			name: "normalize_l2/int8/widen_signed", function: "normalize_l2",
			// int8 input normalizes to a unit vector, which cannot be represented
			// as int8 — the result must widen to vecf32 (not round back to int8).
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_int8.ToType(),
					[][]int8{{1, 2, 3, 4}, {-1, 2, 3, 4}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{0.18257418, 0.36514837, 0.5477226, 0.73029673},
					{-0.18257418, 0.36514837, 0.5477226, 0.73029673},
				},
				nil),
		},
		{
			name: "normalize_l2/uint8/widen_scaled_zero_component", function: "normalize_l2",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_uint8.ToType(),
					[][]uint8{{0, 1, 2, 3}, {10, 20, 30, 40}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{
					{0, 0.26726124, 0.5345225, 0.80178374},
					{0.18257418, 0.36514837, 0.5477226, 0.73029673},
				},
				nil),
		},
		{
			name: "summation/f32/positive_signed", function: "summation",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {4, 5, 6}, {3, -4}, {16777216, 1, -16777216}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15, -1, 1},
				nil),
		},
		{
			name: "summation/f64/positive_signed", function: "summation",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {4, 5, 6}, {3, -4}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15, -1},
				nil),
		},
		{
			name: "l1_norm/f32/positive_signed", function: "l1_norm",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {4, 5, 6}, {3, -4}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15, 7},
				nil),
		},
		{
			name: "l1_norm/f64/positive_signed", function: "l1_norm",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {4, 5, 6}, {3, -4}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{6, 15, 7},
				nil),
		},
		{
			name: "l2_norm/f32/double_norm", function: "l2_norm",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{1, 2, 3}, {4, 5, 6}},
					nil),
			},
			// l2_norm is a DOUBLE function accumulated in float64 (#29083), so a VECF32 whose
			// elements are exactly representable (1..6) yields the SAME value as the VECF64 case,
			// not the older, less accurate float32-reduced result.
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3.741657386773941, 8.774964387392124},
				nil),
		},
		{
			name: "l2_norm/f64/double_norm", function: "l2_norm",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{1, 2, 3}, {4, 5, 6}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{3.741657386773941, 8.774964387392124},
				nil),
		},
		{
			name: "subvector/f32/2args/start_pos1", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				nil),
		},
		{
			name: "subvector/f32/2args/start_pos2", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{2, 3}},
				nil),
		},
		{
			name: "subvector/f32/2args/start_pos3", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{3}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{3}},
				nil),
		},
		{
			name: "subvector/f32/2args/start_neg1", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{-1}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{3}},
				nil),
		},
		{
			name: "subvector/f32/2args/start_neg2", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{-2}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{2, 3}},
				nil),
		},
		{
			name: "subvector/f32/2args/start_neg3", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{-3}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				nil),
		},
		{
			name: "subvector/f32/2args/start_zero", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{0}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{}},
				nil),
		},
		{
			name: "subvector/f64/2args/start_pos1", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{1, 2, 3}},
				nil),
		},
		{
			name: "subvector/f32/3args/start_pos1_len1", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1}},
				nil),
		},
		{
			name: "subvector/f32/3args/start_pos1_len2", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2}},
				nil),
		},
		{
			name: "subvector/f32/3args/start_pos1_len3", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{3}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				nil),
		},
		{
			name: "subvector/f32/3args/start_pos1_len4", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{4}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2, 3}},
				nil),
		},
		{
			name: "subvector/f32/3args/start_neg2_len2", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{-2}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{2, 3}},
				nil),
		},
		{
			name: "subvector/f32/3args/start_neg3_len2", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{-3}, nil),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{2}, nil),
			},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false,
				[][]float32{{1, 2}},
				nil),
		},
		{
			name: "sqrt/f32/squares_zero", function: "sqrt",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{4, 9, 16}, {0, 25, 49}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				//NOTE: SQRT(vecf32) --> vecf64
				[][]float64{{2, 3, 4}, {0, 5, 7}},
				nil),
		},
		{
			name: "sqrt/f64/squares_zero", function: "sqrt",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(),
					[][]float64{{4, 9, 16}, {0, 25, 49}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{2, 3, 4}, {0, 5, 7}},
				nil),
		},
		{
			name: "sqrt/f32/negative_element_null", function: "sqrt",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(),
					[][]float32{{4, -9, 16}, {4, 9, 16}},
					nil),
			},
			expected: NewFunctionTestResult(types.T_array_float64.ToType(), false,
				[][]float64{{0, 0, 0}, {2, 3, 4}},
				[]bool{true, false}),
		},
		{
			name: "inner_product/f32/negated_dot", function: "inner_product",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}, {1, 2}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}, {-3, 5}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-14, -77, -7},
				nil),
		},
		{
			name: "inner_product/f64/negated_dot", function: "inner_product",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}, {1, 2}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}, {-3, 5}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{-14, -77, -7},
				nil),
		},
		{
			name: "cosine_similarity/f32/similarity", function: "cosine_similarity",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 1},
				nil),
		},
		{
			name: "cosine_similarity/f64/similarity", function: "cosine_similarity",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}, {1, 0}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}, {-1, 0}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{1, 1, -1},
				nil),
		},
		{
			name: "l1_distance/f32/manhattan", function: "l1_distance",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {40, 50, 60}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{54, 135},
				nil),
		},
		{
			name: "l1_distance/f64/manhattan", function: "l1_distance",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {40, 50, 60}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{54, 135},
				nil),
		},
		{
			name: "l2_distance/f32/euclidean_f32_domain", function: "l2_distance",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {40, 50, 60}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{33.6749153137207, 78.97467803955078},
				nil),
		},
		{
			name: "l2_distance/f64/euclidean_f32_domain", function: "l2_distance",
			// Vector distances are a float32 domain (#29040 / #29050), so a float64 base rounds to
			// float32 -- not the old exact-f64 33.67491648096547 / 78.9746794865291.
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {40, 50, 60}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{float64(float32(33.67491648096547)), float64(float32(78.9746794865291))},
				nil),
		},
		{
			name: "cosine_distance/f32/cosine_f32_domain", function: "cosine_distance",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {4, 5, 6}, {0, 0}}, nil),
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{10, 20, 30}, {5, 6, 7}, {1, 0}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0.0003542540071066469, 1},
				nil),
		},
		{
			name: "cosine_distance/f64/cosine_f32_domain", function: "cosine_distance",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{10, 20, 30}, {5, 6, 7}}, nil),
			},
			expected: NewFunctionTestResult(types.T_float64.ToType(), false,
				[]float64{0, 0.0003542540112345671},
				nil),
		},
		{name: "subvector/f64/3args/const_start2_len1", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float64.ToType(), [][]float64{{1, 2, 3}, {4, 5, 6}}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{2}, nil),
				NewFunctionTestConstInput(types.T_int64.ToType(), []int64{1}, nil),
			}, expected: NewFunctionTestResult(types.T_array_float64.ToType(), false, [][]float64{{2}, {5}}, nil)},
		{name: "subvector/f32/3args/null_argument_union", function: "subvector",
			inputs: []FunctionTestInput{
				NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2}, {1, 2}, {1, 2}, {1, 2}}, []bool{true, false, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1, 1}, []bool{false, true, false, false}),
				NewFunctionTestInput(types.T_int64.ToType(), []int64{1, 1, 1, 1}, []bool{false, false, true, false}),
			}, expected: NewFunctionTestResult(types.T_array_float32.ToType(), false, [][]float32{nil, nil, nil, {1}}, []bool{true, true, true, false})},
		{name: "normalize_l2/f32/empty_vector_rejected", function: "normalize_l2",
			inputs:   []FunctionTestInput{NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{}}, nil)},
			expected: NewFunctionTestResult(types.T_array_float32.ToType(), false, nil, nil), errorText: "cannot normalize empty vector"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(func() {
				require.Zero(t, proc.Mp().CurrNB())
				bytes, objects := proc.Mp().OnHeapOutstanding()
				require.Zero(t, bytes)
				require.Zero(t, objects)
			})
			args := make([]types.Type, len(tc.inputs))
			for i := range tc.inputs {
				args[i] = tc.inputs[i].typ
			}
			resolved, err := GetFunctionByName(proc.Ctx, tc.function, args)
			require.NoError(t, err)
			_, cast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, cast)
			require.Equal(t, tc.expected.typ, resolved.GetReturnType())
			registered, err := GetFunctionById(proc.Ctx, resolved.GetEncodedOverloadID())
			require.NoError(t, err)
			execute, _, cleanup, retained := registered.GetExecuteMethod()
			require.Nil(t, cleanup)
			require.Nil(t, retained)
			fc := NewFunctionTestCase(proc, tc.inputs, tc.expected, execute)
			t.Cleanup(fc.Free)
			if tc.errorText != "" {
				_, err := fc.DebugRun()
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal), "unexpected error: %v", err)
				require.EqualError(t, err, "internal error: "+tc.errorText)
				return
			}
			ok, info := fc.RunAndFree()
			require.True(t, ok, info)
		})
	}
	for _, tc := range []struct {
		name string
		args []types.Type
	}{
		{"one_argument", []types.Type{types.T_array_float32.ToType()}},
		{"four_arguments", []types.Type{types.T_array_float32.ToType(), types.T_int64.ToType(), types.T_int64.ToType(), types.T_int64.ToType()}},
	} {
		t.Run("subvector/arity/"+tc.name, func(t *testing.T) {
			_, err := GetFunctionByName(proc.Ctx, "subvector", tc.args)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidArg), "unexpected error: %v", err)
		})
	}
	for _, oid := range []types.T{types.T_array_int8, types.T_array_uint8} {
		t.Run("normalize_l2/"+oid.String()+"/return_metadata_dim4", func(t *testing.T) {
			resolved, err := GetFunctionByName(proc.Ctx, "normalize_l2", []types.Type{types.New(oid, 4, 0)})
			require.NoError(t, err)
			_, cast := resolved.ShouldDoImplicitTypeCast()
			require.False(t, cast)
			require.Equal(t, types.New(types.T_array_float32, 4, 0), resolved.GetReturnType())
		})
	}

}
