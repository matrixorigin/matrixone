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

package hnsw

import (
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestCheckStorable(t *testing.T) {
	require.NoError(t, CheckStorable([]float32{1, 65504}, usearch.F16))
	require.Error(t, CheckStorable([]float32{1, 100000}, usearch.F16))
	require.NoError(t, CheckStorable([]float32{1, 100000}, usearch.BF16))
	require.Error(t, CheckStorable([]float32{1, math.MaxFloat32}, usearch.BF16))
	require.NoError(t, CheckStorable([]float64{1, 1e30}, usearch.F32))
	require.Error(t, CheckStorable([]float64{1, 1e300}, usearch.F32))
	require.Error(t, CheckStorable([]float64{1, 1e300}, usearch.I8))
	// an int8/uint8 range maps any finite float32 vector
	require.NoError(t, CheckStorable([]float32{-1e30, 1e30}, usearch.I8))
	require.NoError(t, CheckStorable([]float32{-1e30, 1e30}, usearch.U8))
	require.NoError(t, CheckStorable([]int8{-128, 127}, usearch.I8))
	code := func(err error) uint16 {
		c, ok := vectorindex.UnstorableCode(err)
		require.True(t, ok, "%v", err)
		return c
	}
	require.Equal(t, moerr.ErrIndexVectorNonFinite, code(CheckStorable([]float32{1, float32(math.NaN())}, usearch.F32)))
	require.Equal(t, moerr.ErrIndexVectorDimension, code(CheckStorable([]float32{}, usearch.F32)))
	require.Equal(t, moerr.ErrIndexVectorOutOfRange, code(CheckStorable([]float32{1, 100000}, usearch.F16)))
	// same verdict as ToStorage, and the same detail
	same := func(want, got error, msgAndArgs ...any) {
		if want == nil {
			require.NoError(t, got, msgAndArgs...)
			return
		}
		require.Equal(t, moerr.ErrIndexVectorOutOfRange, code(got), msgAndArgs...)
		detail := strings.TrimPrefix(got.Error(), moerr.MessageOf(moerr.ErrIndexVectorOutOfRange)+": ")
		require.True(t, strings.HasPrefix(detail, "vector element "), msgAndArgs...)
		require.True(t, strings.HasSuffix(want.Error(), ", "+detail), msgAndArgs...)
	}

	// CheckStorable rejects exactly the vectors ToStorage rejects, with the same error, on both
	// sides of every overflow boundary
	var f32s []float32
	for _, b := range []float32{f16Overflow, bf16Overflow, math.MaxFloat32} {
		for _, x := range []float32{b, math.Nextafter32(b, 0), math.Nextafter32(b, float32(math.Inf(1)))} {
			f32s = append(f32s, x, -x)
		}
	}
	f32s = append(f32s, 65504, 1, 0)
	for _, kind := range []usearch.Quantization{usearch.F32, usearch.F16, usearch.BF16, usearch.I8, usearch.U8} {
		bounds := QuantizeBounds{Min: -1, Max: 1, Set: true}
		for _, x := range f32s {
			if math.IsInf(float64(x), 0) {
				continue
			}
			v := []float32{1, x}
			_, _, want := ToStorage(v, kind, bounds)
			same(want, CheckStorable(v, kind), "%v as %v", x, kind)
		}
		// a vecf64 element near the float32 limit, rounding either way
		for _, x := range []float64{math.MaxFloat32, math.MaxFloat32 * (1 + 0x1p-25), math.MaxFloat32 * (1 + 0x1p-23), 65519.999, 65520.001} {
			for _, e := range []float64{x, -x} {
				v := []float64{1, e}
				_, _, want := ToStorage(v, kind, bounds)
				same(want, CheckStorable(v, kind), "%v as %v", e, kind)
			}
		}
	}
}
