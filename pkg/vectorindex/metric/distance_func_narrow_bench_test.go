// Copyright 2023 Matrix Origin
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

package metric

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// Compares the narrow element types (bf16/f16/int8) against f32/f64 on the
// realistic workload: distance computed from the raw stored bytes (so the cost
// includes each type's decode — free reinterpret for f32/f64/int8, an
// upcast-to-float32 for bf16/f16). At dim=1024 the per-vector load is:
//   f64 8KB · f32 4KB · bf16/f16 2KB · int8 1KB
// so int8 should win on bandwidth + integer ops; bf16/f16 trade half the
// bandwidth for the fp32 upcast.
//
// Run: go test ./pkg/vectorindex/metric/ -run x -bench Benchmark_NarrowVsFloat -benchmem

const (
	narrowBenchDim  = 1024
	narrowBenchPool = 256
)

func benchF32Pool(n, dim int) [][]float32 {
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, dim)
		for j := range v {
			v[j] = float32(rand.Float64()*16 - 8) // [-8, 8)
		}
		out[i] = v
	}
	return out
}

func benchInt8Pool(n, dim int) [][]int8 {
	out := make([][]int8, n)
	for i := range out {
		v := make([]int8, dim)
		for j := range v {
			v[j] = int8(rand.Intn(255) - 127) // [-127, 127]
		}
		out[i] = v
	}
	return out
}

func toBytesPool[T types.ArrayElement](vecs [][]T) [][]byte {
	out := make([][]byte, len(vecs))
	for i, v := range vecs {
		out[i] = append([]byte(nil), types.ArrayToBytes(v)...)
	}
	return out
}

func Benchmark_NarrowVsFloat(b *testing.B) {
	dim, n := narrowBenchDim, narrowBenchPool

	f32 := benchF32Pool(n, dim)
	f64 := make([][]float64, n)
	bf16 := make([][]types.BF16, n)
	f16 := make([][]types.Float16, n)
	for i, v := range f32 {
		d := make([]float64, dim)
		for j, x := range v {
			d[j] = float64(x)
		}
		f64[i] = d
		bf16[i] = types.Float32ToBF16Slice(v)
		f16[i] = types.Float32ToFloat16Slice(v)
	}
	i8 := benchInt8Pool(n, dim)

	f64b := toBytesPool(f64)
	f32b := toBytesPool(f32)
	bf16b := toBytesPool(bf16)
	f16b := toBytesPool(f16)
	i8b := toBytesPool(i8)

	metrics := []struct {
		name string
		mt   MetricType
	}{
		{"L2sq", Metric_L2sqDistance},
		{"InnerProduct", Metric_InnerProduct},
		{"Cosine", Metric_CosineDistance},
	}

	for _, m := range metrics {
		b.Run(m.name, func(b *testing.B) {
			b.Run("f64", func(b *testing.B) { benchFloatFromBytes[float64](b, m.mt, f64b) })
			b.Run("f32", func(b *testing.B) { benchFloatFromBytes[float32](b, m.mt, f32b) })
			b.Run("bf16", func(b *testing.B) { benchNarrowFromBytes[types.BF16](b, m.mt, bf16b) })
			b.Run("f16", func(b *testing.B) { benchNarrowFromBytes[types.Float16](b, m.mt, f16b) })
			b.Run("int8", func(b *testing.B) { benchNarrowFromBytes[int8](b, m.mt, i8b) })
		})
	}
}

func benchFloatFromBytes[T float32 | float64](b *testing.B, m MetricType, pool [][]byte) {
	fn, err := ResolveDistanceFn[T, T](m)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := types.BytesToArray[T](pool[i%len(pool)])
		c := types.BytesToArray[T](pool[(i+1)%len(pool)])
		_, _ = fn(a, c)
	}
}

func benchNarrowFromBytes[T types.ArrayElement](b *testing.B, m MetricType, pool [][]byte) {
	fn, err := ResolveDistanceFn[T, float32](m)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := types.BytesToArray[T](pool[i%len(pool)])
		c := types.BytesToArray[T](pool[(i+1)%len(pool)])
		_, _ = fn(a, c)
	}
}
