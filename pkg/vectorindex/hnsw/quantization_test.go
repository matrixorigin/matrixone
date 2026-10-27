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
	"unsafe"

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

// A narrow vector already of the stored type is used as it is; otherwise it is converted by
// value.
func TestToStorageNarrow(t *testing.T) {
	f16 := types.Float32ToFloat16Slice([]float32{1.5, -2})
	ptr, v, err := ToStorage(f16, usearch.F16, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, unsafe.Pointer(&f16[0]), ptr, "no copy")
	require.Equal(t, f16, v)
	bf16 := []types.BF16{types.BF16FromFloat32(1.5), types.BF16FromFloat32(-2)}
	ptr, _, err = ToStorage(bf16, usearch.BF16, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, unsafe.Pointer(&bf16[0]), ptr)

	_, v, err = ToStorage(f16, usearch.F32, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []float32{1.5, -2}, v, "a float16 element converts by value, not by its bits")
	_, v, err = ToStorage(bf16, usearch.F64, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, []float64{1.5, -2}, v)
	_, v, err = ToStorage(bf16, usearch.F16, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, f16, v)
	_, v, err = ToStorage(f16, usearch.I8, QuantizeBounds{Min: -2, Max: 2, Set: true})
	require.NoError(t, err)
	_, want, err := ToStorage([]float32{1.5, -2}, usearch.I8, QuantizeBounds{Min: -2, Max: 2, Set: true})
	require.NoError(t, err)
	require.Equal(t, want, v)

	// a vecint8/vecuint8 vector mapped with its own type range is used as it is
	i8 := []int8{-128, 5, 127}
	ptr, v, err = ToStorage(i8, usearch.I8, BaseBounds(int32(types.T_array_int8)))
	require.NoError(t, err)
	require.Equal(t, unsafe.Pointer(&i8[0]), ptr)
	require.Equal(t, i8, v)
	u8 := []uint8{0, 5, 255}
	ptr, _, err = ToStorage(u8, usearch.U8, BaseBounds(int32(types.T_array_uint8)))
	require.NoError(t, err)
	require.Equal(t, unsafe.Pointer(&u8[0]), ptr)
	// with another range it is mapped, and as a float stored type it converts by value
	_, v, err = ToStorage(i8, usearch.I8, QuantizeBounds{Min: -128, Max: 0, Set: true})
	require.NoError(t, err)
	_, want, err = ToStorage([]float32{-128, 5, 127}, usearch.I8, QuantizeBounds{Min: -128, Max: 0, Set: true})
	require.NoError(t, err)
	require.Equal(t, want, v)
	require.NotEqual(t, i8, v)
	_, v, err = ToStorage(u8, usearch.F16, QuantizeBounds{})
	require.NoError(t, err)
	require.Equal(t, types.Float32ToFloat16Slice([]float32{0, 5, 255}), v)

	require.Equal(t, TrainQuantizeBounds([][]float32{{1.5, -2}}), TrainQuantizeBounds([][]types.Float16{f16}))
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
	model, err := NewHnswModelForBuild[T]("q", cfg, 1, count, 0, t.TempDir())
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

// A model of a narrow element type is built from and searched with vectors of that type:
// every vector finds itself.
func TestNarrowModelSearch(t *testing.T) {
	f16 := func(x float64) types.Float16 { return types.Float16FromFloat32(float32(x)) }
	bf16 := func(x float64) types.BF16 { return types.BF16FromFloat32(float32(x)) }
	i8 := func(x float64) int8 { return int8(x * 40) }
	u8 := func(x float64) uint8 { return uint8(x*40 + 128) }
	testNarrowModelSearch(t, usearch.F16, QuantizeBounds{}, f16)
	testNarrowModelSearch(t, usearch.I8, QuantizeBounds{Min: -3, Max: 3, Set: true}, f16)
	testNarrowModelSearch(t, usearch.BF16, QuantizeBounds{}, bf16)
	testNarrowModelSearch(t, usearch.F32, QuantizeBounds{}, bf16)
	testNarrowModelSearch(t, usearch.I8, BaseBounds(int32(types.T_array_int8)), i8)
	testNarrowModelSearch(t, usearch.U8, BaseBounds(int32(types.T_array_uint8)), u8)
	testNarrowModelSearch(t, usearch.F16, QuantizeBounds{}, u8)
}

func testNarrowModelSearch[T types.ArrayElement](t *testing.T, storage usearch.Quantization, bounds QuantizeBounds, conv func(float64) T) {
	const dimensions = 16
	const count = 300
	cfg := vectorindex.IndexConfig{Type: "hnsw", Usearch: usearch.DefaultConfig(dimensions)}
	cfg.Usearch.Quantization = storage
	cfg.Usearch.Metric = usearch.L2sq
	model, err := NewHnswModelForBuild[T]("narrow", cfg, 1, count, 0, t.TempDir())
	require.NoError(t, err)
	defer model.Destroy()
	model.Bounds = bounds

	vectors := make([][]T, count)
	state := uint32(11)
	for i := range vectors {
		vectors[i] = make([]T, dimensions)
		for d := range vectors[i] {
			state = state*1664525 + 1013904223
			vectors[i][d] = conv(float64(state>>8)/float64(1<<24)*6 - 3)
		}
		require.NoError(t, model.Add(int64(i), vectors[i]))
	}
	for i, v := range vectors {
		keys, distances, err := model.Search(v, 1)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		require.EqualValues(t, i, keys[0], "%T stored as %v", v, storage)
		require.InDelta(t, 0, distances[0], 1e-3)
	}
}
