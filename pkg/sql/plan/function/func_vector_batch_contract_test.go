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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

// cosine_similarity must answer identically whether or not an operand is constant. It has no
// batch path: the batch kernels compute cosine DISTANCE, and recovering similarity as 1-distance
// loses what the subtraction cancels -- for [1e-6,1] against [1,0] that recovery lands about 116k
// float32 ULP away from the kernel's own answer. The zero-vector contract differs too:
// cosine_distance returns 1 by convention where cosine_similarity rejects.
func TestCosineSimilarityIsConstnessIndependent(t *testing.T) {
	proc := testutil.NewProcess(t)

	// The precision case: constant operand and column operand must give the same value.
	const want = 1e-06
	for _, tc := range []struct {
		name  string
		input []FunctionTestInput
	}{
		{"const", []FunctionTestInput{
			NewFunctionTestConstInput(types.T_array_float32.ToType(), [][]float32{{1, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1e-6, 1}}, []bool{false}),
		}},
		{"column", []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1e-6, 1}}, []bool{false}),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewFunctionTestCase(proc, tc.input,
				NewFunctionTestResult(types.T_float64.ToType(), false, []float64{want}, []bool{false}),
				CosineSimilarityArray[float32])
			ok, info := c.Run()
			require.True(t, ok, info)
		})
	}

	// A zero-magnitude vector is rejected with either operand constant.
	constZero := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_array_float32.ToType(), [][]float32{{0, 0, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {0, 0, 0}}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), true, []float64{0, 0}, []bool{false, false}),
		CosineSimilarityArray[float32])
	s, info := constZero.Run()
	require.True(t, s, info)

	// Ordinary vectors still answer.
	constOk := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{1, 2, 3}, {2, 4, 6}}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), false, []float64{1, 1}, []bool{false, false}),
		CosineSimilarityArray[float32])
	s, info = constOk.Run()
	require.True(t, s, info)
}

// The const-vs-column L2 fast path must agree with the per-row path on a distance whose square
// leaves float32. Both kernels accumulate the square in float32, so neither can answer the pair:
// the batch kernel reaches +Inf and hands the row to the per-row kernel, which raises the
// canonical error. Making an operand constant must not change what SQL returns.
func TestL2DistanceBatchRejectsSquaredOverflowLikeScalar(t *testing.T) {
	proc := testutil.NewProcess(t)

	batch := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestConstInput(types.T_array_float32.ToType(), [][]float32{{0, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{2e19, 2e19}}, []bool{false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), true, []float64{}, []bool{}),
		L2DistanceArray[float32])
	s, info := batch.Run()
	require.True(t, s, info)

	perRow := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{0, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{2e19, 2e19}}, []bool{false}),
		},
		NewFunctionTestResult(types.T_float64.ToType(), true, []float64{}, []bool{}),
		L2DistanceArray[float32])
	s, info = perRow.Run()
	require.True(t, s, info)

	// An ordinary pair takes the batch path and both forms answer identically.
	for _, tc := range []struct {
		name  string
		input []FunctionTestInput
	}{
		{"const", []FunctionTestInput{
			NewFunctionTestConstInput(types.T_array_float32.ToType(), [][]float32{{0, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{3, 4}}, []bool{false}),
		}},
		{"column", []FunctionTestInput{
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{0, 0}}, []bool{false}),
			NewFunctionTestInput(types.T_array_float32.ToType(), [][]float32{{3, 4}}, []bool{false}),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := NewFunctionTestCase(proc, tc.input,
				NewFunctionTestResult(types.T_float64.ToType(), false, []float64{5}, []bool{false}),
				L2DistanceArray[float32])
			ok, info := c.Run()
			require.True(t, ok, info)
		})
	}
}

// normalize_l2 leaves its output buffer untouched when it rejects a vector, and that buffer is
// taken from a sync.Pool and resized without being zeroed. A discarded error would therefore emit
// an earlier row's vector as this row's result, so the error must reach the caller.
func TestNormalizeL2RejectsInsteadOfEmittingStaleBuffer(t *testing.T) {
	proc := testutil.NewProcess(t)

	// Row 0 normalizes and fills the pooled buffer; row 1's squared norm overflows float64.
	f64 := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_array_float64.ToType(),
				[][]float64{{3, 4}, {1e308, 1e308}}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_array_float64.ToType(), true, [][]float64{}, []bool{}),
		NormalizeL2Array[float64])
	s, info := f64.Run()
	require.True(t, s, info)

	// Underflow is rejected the same way. Only float64 reaches either bound: the norm accumulates
	// in float64, and no float32 element squares outside that domain.
	under := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_array_float64.ToType(),
				[][]float64{{3, 4}, {1e-300, 1e-300}}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_array_float64.ToType(), true, [][]float64{}, []bool{}),
		NormalizeL2Array[float64])
	s, info = under.Run()
	require.True(t, s, info)

	// Ordinary rows still normalize, and a null row stays null.
	ok := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(types.T_array_float32.ToType(),
				[][]float32{{3, 4}, {}, {0, 0}}, []bool{false, true, false}),
		},
		NewFunctionTestResult(types.T_array_float32.ToType(), false,
			[][]float32{{0.6, 0.8}, {}, {0, 0}}, []bool{false, true, false}),
		NormalizeL2Array[float32])
	s, info = ok.Run()
	require.True(t, s, info)
}
