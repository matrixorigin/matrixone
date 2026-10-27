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
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestStorageQuantization(t *testing.T) {
	for _, tc := range []struct {
		quant string
		base  types.T
		want  usearch.Quantization
	}{
		{"", types.T_array_float32, usearch.F32},
		{"", types.T_array_float64, usearch.F64},
		{"float32", types.T_array_float64, usearch.F32},
		{"FLOAT16", types.T_array_float32, usearch.F16},
		{"bf16", types.T_array_float32, usearch.BF16},
		{"float64", types.T_array_float64, usearch.F64},
		{"int8", types.T_array_float32, usearch.I8},
		{"uint8", types.T_array_float64, usearch.U8},
	} {
		got, err := StorageQuantization(tc.quant, int32(tc.base))
		require.NoError(t, err, tc.quant)
		require.Equal(t, tc.want, got, tc.quant)
	}
	_, err := StorageQuantization("int4", int32(types.T_array_float32))
	require.Error(t, err)

}

// ToStorage hands usearch the element type it stores, converted by MO.
func TestToStorage(t *testing.T) {
	_, v, err := ToStorage([]float32{1.5, -2}, usearch.F32, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []float32{1.5, -2}, v)
	_, v, err = ToStorage([]float64{1.5, -2}, usearch.F32, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []float32{1.5, -2}, v)
	_, v, err = ToStorage([]float32{1.5, -2}, usearch.F64, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []float64{1.5, -2}, v)
	_, v, err = ToStorage([]float32{1.5, -2}, usearch.F16, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []types.Float16{types.Float16FromFloat32(1.5), types.Float16FromFloat32(-2)}, v)
	_, v, err = ToStorage([]float64{1.5, -2}, usearch.BF16, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []types.BF16{types.BF16FromFloat32(1.5), types.BF16FromFloat32(-2)}, v)
	_, _, err = ToStorage([]float32{}, usearch.F32, QuantizeBounds{})
	require.Error(t, err)

	// a finite element the stored type cannot hold is rejected
	_, _, err = ToStorage([]float32{1, 1e5}, usearch.F16, QuantizeBounds{})
	require.Error(t, err)
	_, _, err = ToStorage([]float64{1, 1e39}, usearch.F32, QuantizeBounds{})
	require.Error(t, err)
	_, _, err = ToStorage([]float64{1, 1e39}, usearch.BF16, QuantizeBounds{})
	require.Error(t, err)
	_, _, err = ToStorage([]float32{65504}, usearch.F16, QuantizeBounds{})
	require.NoError(t, err)

	// int8/uint8 map the trained range onto the integer range, and need it
	_, _, err = ToStorage([]float32{0.5}, usearch.I8, QuantizeBounds{})
	require.Error(t, err)
	bounds := QuantizeBounds{Min: -1, Max: 1, Set: true}
	_, v, err = ToStorage([]float32{-1, 1}, usearch.I8, bounds)
	require.NoError(t, err)
	require.Equal(t, []int8{-128, 127}, v)
	_, v, err = ToStorage([]float64{-1, 1}, usearch.U8, bounds)
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 255}, v)
}

// A model storing quantized elements is built from and searched with base-type vectors:
// every vector finds itself, and the top 10 agree with brute force.
func TestQuantizedModelSearch(t *testing.T) {
	testQuantizedModelSearch[float32](t, usearch.F16, usearch.L2sq, false)
	testQuantizedModelSearch[float32](t, usearch.BF16, usearch.L2sq, false)
	testQuantizedModelSearch[float32](t, usearch.I8, usearch.L2sq, false)
	testQuantizedModelSearch[float32](t, usearch.U8, usearch.L2sq, false)
	testQuantizedModelSearch[float64](t, usearch.I8, usearch.L2sq, false)
	testQuantizedModelSearch[float64](t, usearch.F32, usearch.L2sq, false)
}

func testQuantizedModelSearch[T types.RealNumbers](t *testing.T, storage usearch.Quantization, metricKind usearch.Metric, normalize bool) {
	const dimensions = 32
	const count = 1000
	const limit = 10

	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(dimensions)}
	cfg.Usearch.Quantization = storage
	cfg.Usearch.Metric = metricKind
	model, err := NewHnswModelForBuild[T]("q", cfg, 1, count, t.TempDir())
	require.NoError(t, err)
	defer model.Destroy()

	vectors := make([][]T, count)
	state := uint32(7)
	for i := range vectors {
		vectors[i] = make([]T, dimensions)
		var norm float64
		for d := range vectors[i] {
			state = state*1664525 + 1013904223
			v := float64(state>>8)/float64(1<<24) - 0.5
			vectors[i][d] = T(v)
			norm += v * v
		}
		if normalize {
			for d := range vectors[i] {
				vectors[i][d] = T(float64(vectors[i][d]) / math.Sqrt(norm))
			}
		}
	}
	if NeedsQuantizeBounds(storage) {
		model.Bounds = TrainQuantizeBounds(vectors)
	}
	for i := range vectors {
		require.NoError(t, model.Add(int64(i), vectors[i]))
	}

	distance := func(a, b []T) float64 {
		var dot, na, nb, l2 float64
		for d := range a {
			x, y := float64(a[d]), float64(b[d])
			dot += x * y
			na += x * x
			nb += y * y
			l2 += (x - y) * (x - y)
		}
		if metricKind == usearch.Cosine {
			return 1 - dot/math.Sqrt(na*nb)
		}
		return l2
	}

	var hits, total int
	for q := 0; q < count; q += 37 {
		keys, dists, err := model.Search(vectors[q], limit)
		require.NoError(t, err)
		require.Equal(t, usearch.Key(q), keys[0], "storage %v: a vector must find itself", storage)
		if metricKind == usearch.L2sq {
			// distances are reported in the units of the base column
			for j, k := range keys {
				want := distance(vectors[q], vectors[k])
				require.InDelta(t, want, float64(dists[j]), 0.05*want+0.01, "storage %v distance", storage)
			}
		}

		ids := make([]int, count)
		for i := range ids {
			ids[i] = i
		}
		sort.Slice(ids, func(a, b int) bool {
			return distance(vectors[q], vectors[ids[a]]) < distance(vectors[q], vectors[ids[b]])
		})
		truth := map[usearch.Key]bool{}
		for _, id := range ids[:limit] {
			truth[usearch.Key(id)] = true
		}
		for _, k := range keys {
			if truth[k] {
				hits++
			}
		}
		total += limit
	}
	require.GreaterOrEqual(t, float64(hits)/float64(total), 0.9, "storage %v recall@%d", storage, limit)
}

// Vectors of vecf16/vecbf16/vecint8/vecuint8 columns are widened to float32; an int8/uint8
// column keeps its values, mapped with the range of its type.
func TestNarrowColumns(t *testing.T) {
	for _, v := range []any{
		[]types.Float16{types.Float16FromFloat32(1.5), types.Float16FromFloat32(-2)},
		[]types.BF16{types.BF16FromFloat32(1.5), types.BF16FromFloat32(-2)},
	} {
		got, ok := WidenToFloat32(v)
		require.True(t, ok)
		require.Equal(t, []float32{1.5, -2}, got)
	}
	got, ok := WidenToFloat32([]int8{-128, 127})
	require.True(t, ok)
	require.Equal(t, []float32{-128, 127}, got)
	_, ok = WidenToFloat32([]float32{1})
	require.False(t, ok)

	got, ok = BytesToFloat32(int32(types.T_array_uint8), []byte{0, 255})
	require.True(t, ok)
	require.Equal(t, []float32{0, 255}, got)
	_, ok = BytesToFloat32(int32(types.T_array_float32), types.ArrayToBytes([]float32{1}))
	require.False(t, ok)

	_, v, err := ToStorage([]float32{-128, 0, 127}, usearch.I8, BaseBounds(int32(types.T_array_int8)))
	require.NoError(t, err)
	require.Equal(t, []int8{-128, 0, 127}, v, "a vecint8 column is stored as it is")
	_, v, err = ToStorage([]float32{0, 7, 255}, usearch.U8, BaseBounds(int32(types.T_array_uint8)))
	require.NoError(t, err)
	require.Equal(t, []uint8{0, 7, 255}, v, "a vecuint8 column is stored as it is")
	require.False(t, BaseBounds(int32(types.T_array_float16)).Set)

	for typ, want := range map[types.T]usearch.Quantization{
		types.T_array_float16: usearch.F16, types.T_array_bf16: usearch.BF16,
		types.T_array_int8: usearch.I8, types.T_array_uint8: usearch.U8,
	} {
		got, err := StorageQuantization("", int32(typ))
		require.NoError(t, err)
		require.Equal(t, want, got, typ.String())
	}
}
