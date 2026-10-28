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
	"testing"

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
	require.Error(t, CheckStorable([]float32{1, float32(math.NaN())}, usearch.F32))
	require.Error(t, CheckStorable([]float32{}, usearch.F32))

	// CheckStorable rejects exactly the vectors ToStorage rejects
	for _, v := range [][]float32{{1, 2}, {70000, 1}, {65504, -65504}, {65520, 0}} {
		_, _, err := ToStorage(v, usearch.F16, QuantizeBounds{})
		require.Equal(t, err != nil, CheckStorable(v, usearch.F16) != nil, "%v", v)
	}
}
