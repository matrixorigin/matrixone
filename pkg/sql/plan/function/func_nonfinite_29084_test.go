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
	"encoding/base64"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestRejectNonFiniteVectorElems guards the shared finite check that closes both #29084 bypasses.
// It must reject NaN/Inf for every float element type, pass finite vectors, and skip integer targets
// (which clamp and can never be non-finite).
func TestRejectNonFiniteVectorElems(t *testing.T) {
	inf := float32(math.Inf(1))
	nan := float32(math.NaN())

	// float32
	require.Error(t, rejectNonFiniteVectorElems([]float32{1, inf}))
	require.Error(t, rejectNonFiniteVectorElems([]float32{nan, 1}))
	require.NoError(t, rejectNonFiniteVectorElems([]float32{1, -2, 3}))

	// float64
	require.Error(t, rejectNonFiniteVectorElems([]float64{math.Inf(-1), 1}))
	require.Error(t, rejectNonFiniteVectorElems([]float64{1, math.NaN()}))
	require.NoError(t, rejectNonFiniteVectorElems([]float64{1e300, -1e300}))

	// Float16 / BF16: non-finite is only reachable after narrowing, so build it that way.
	require.Error(t, rejectNonFiniteVectorElems(types.FromFloat32Array[types.Float16]([]float32{inf})))
	require.NoError(t, rejectNonFiniteVectorElems(types.FromFloat32Array[types.Float16]([]float32{1.5})))
	require.Error(t, rejectNonFiniteVectorElems(types.FromFloat32Array[types.BF16]([]float32{nan})))
	require.NoError(t, rejectNonFiniteVectorElems(types.FromFloat32Array[types.BF16]([]float32{1.5})))

	// int8 / uint8 targets clamp and are skipped -- even a value derived from an overflow.
	require.NoError(t, rejectNonFiniteVectorElems([]int8{-128, 127}))
	require.NoError(t, rejectNonFiniteVectorElems([]uint8{0, 255}))
}

// TestRejectNonFiniteVectorElemsF16NoAlloc guards the #29084 P2 optimization: the F16/BF16 finite
// check must widen each element in place (e.ToFloat32()) rather than materializing a whole []float32
// per row (types.ToFloat32Array), which added 4*dimension bytes/row on batch CAST / VEC*_FROM_BASE64.
func TestRejectNonFiniteVectorElemsF16NoAlloc(t *testing.T) {
	f16 := types.FromFloat32Array[types.Float16](make([]float32, 1536))
	bf16 := types.FromFloat32Array[types.BF16](make([]float32, 1536))
	require.Zero(t, testing.AllocsPerRun(100, func() { _ = rejectNonFiniteVectorElems(f16) }),
		"F16 finite check must not allocate a widened slice (#29084)")
	require.Zero(t, testing.AllocsPerRun(100, func() { _ = rejectNonFiniteVectorElems(bf16) }),
		"BF16 finite check must not allocate a widened slice (#29084)")
}

// TestCastArrayNarrowingRejectsNonFinite guards #29084 bypass 1: a vector-to-vector CAST that
// narrows a finite source to +/-Inf must be rejected, not persist Infinity. Mirrors the direct text
// cast, which already rejects the same value.
func TestCastArrayNarrowingRejectsNonFinite(t *testing.T) {
	proc := testutil.NewProcess(t)
	vecf32 := func(w int32) types.Type { return types.New(types.T_array_float32, w, 0) }
	vecf64 := func(w int32) types.Type { return types.New(types.T_array_float64, w, 0) }

	t.Run("vecf64->vecf32 overflow rejected", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(vecf64(2), [][]float64{{1e300, -1e300}}, []bool{false}),
				NewFunctionTestInput(vecf32(2), [][]float32{}, []bool{}),
			},
			NewFunctionTestResult(vecf32(2), true, nil, nil), NewCast)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})

	t.Run("vecf64->vecf32 finite allowed", func(t *testing.T) {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{
				NewFunctionTestInput(vecf64(2), [][]float64{{3e38, -3e38}}, []bool{false}),
				NewFunctionTestInput(vecf32(2), [][]float32{}, []bool{}),
			},
			NewFunctionTestResult(vecf32(2), false, [][]float32{{3e38, -3e38}}, []bool{false}), NewCast)
		ok, info := tc.Run()
		require.True(t, ok, info)
	})
}

// TestVecFromBase64RejectsNonFinite guards #29084 bypass 2: VEC*_FROM_BASE64 decodes raw IEEE-754
// bytes, so a NaN/Inf bit pattern would otherwise land straight in a vector column. It must be
// rejected; a finite payload still decodes. Base64 strings are the issue's exact reproduction values.
func TestVecFromBase64RejectsNonFinite(t *testing.T) {
	proc := testutil.NewProcess(t)
	f32vec := types.T_array_float32.ToType()

	// +Inf (AACAfw==) and NaN (AADAfw==) float32 payloads are rejected.
	for _, b64 := range []string{"AACAfw==", "AADAfw=="} {
		tc := NewFunctionTestCase(proc,
			[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{b64}, []bool{false})},
			NewFunctionTestResult(f32vec, true, nil, nil), VecFromBase64[float32])
		ok, info := tc.Run()
		require.True(t, ok, info)
	}

	// A finite payload (2.0 -> AAAAQA==) still decodes.
	tc := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{"AAAAQA=="}, []bool{false})},
		NewFunctionTestResult(f32vec, false, [][]float32{{2}}, []bool{false}), VecFromBase64[float32])
	ok, info := tc.Run()
	require.True(t, ok, info)

	// float64: a +Inf payload is rejected, but a finite value that overflows float32 (1e300) must
	// still decode -- the finite check runs in float64's native precision, not via a float32 narrow.
	f64vec := types.T_array_float64.ToType()
	infB64 := base64.StdEncoding.EncodeToString(types.ArrayToBytes([]float64{math.Inf(1)}))
	tcInf64 := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{infB64}, []bool{false})},
		NewFunctionTestResult(f64vec, true, nil, nil), VecFromBase64[float64])
	ok, info = tcInf64.Run()
	require.True(t, ok, info)

	bigB64 := base64.StdEncoding.EncodeToString(types.ArrayToBytes([]float64{1e300, -1e300}))
	tcBig64 := NewFunctionTestCase(proc,
		[]FunctionTestInput{NewFunctionTestInput(types.T_varchar.ToType(), []string{bigB64}, []bool{false})},
		NewFunctionTestResult(f64vec, false, [][]float64{{1e300, -1e300}}, []bool{false}), VecFromBase64[float64])
	ok, info = tcBig64.Run()
	require.True(t, ok, info)
}
