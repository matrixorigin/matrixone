//go:build amd64 && go1.26 && goexperiment.simd

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

package metric

import (
	"math/rand"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var avxUpperSink float64

// scalarAfterKernel is scalar float32 work of the kind that follows a distance kernel.
func scalarAfterKernel(v []float32, s float32) float32 {
	var a float32
	for _, x := range v {
		a += x * s
	}
	return a
}

// BenchmarkScalarAfterSIMDKernel times a SIMD distance kernel followed by scalar float code.
func BenchmarkScalarAfterSIMDKernel(b *testing.B) {
	const dim = 768
	r := rand.New(rand.NewSource(1))
	f1, f2 := make([]float32, dim), make([]float32, dim)
	h1, h2 := make([]types.BF16, dim), make([]types.BF16, dim)
	i1, i2 := make([]int8, dim), make([]int8, dim)
	for i := range f1 {
		f1[i], f2[i] = float32(r.NormFloat64()), float32(r.NormFloat64())
		h1[i], h2[i] = types.BF16FromFloat32(f1[i]), types.BF16FromFloat32(f2[i])
		i1[i], i2[i] = int8(r.Intn(255)-127), int8(r.Intn(255)-127)
	}
	cell1, _ := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, f1)
	cell2, _ := types.AppendBlockScaled(nil, types.BlockScaledMXFP8, f2)
	c1, _ := types.ParseBlockScaledCell(cell1)
	c2, _ := types.ParseBlockScaledCell(cell2)
	d1, d2 := make([]float32, dim), make([]float32, dim)

	b.Run("vecf32-ip/kernel-only", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := InnerProduct(f1, f2)
			avxUpperSink += float64(d)
		}
	})
	b.Run("vecf32-ip/then-scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := InnerProduct(f1, f2)
			avxUpperSink += float64(d + scalarAfterKernel(f1, d))
		}
	})
	b.Run("bf16-l2sq-avx512/then-scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := l2sqBF16SIMD(h1, h2)
			avxUpperSink += d + float64(scalarAfterKernel(f1, float32(d)))
		}
	})
	b.Run("bf16-l2sq-avx2/then-scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := l2sqBF16AVX2(h1, h2)
			avxUpperSink += float64(d) + float64(scalarAfterKernel(f1, float32(d)))
		}
	})
	b.Run("bf16-ip-avx2/then-scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := innerProductBF16AVX2(h1, h2)
			avxUpperSink += d + float64(scalarAfterKernel(f1, float32(d)))
		}
	})
	b.Run("bf16-ip-avx2/then-bf16-decode", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := innerProductBF16AVX2(h1, h2)
			for j := range d1 {
				d1[j] = h1[j].ToFloat32() * float32(d)
			}
			avxUpperSink += float64(d1[0])
		}
	})
	b.Run("int8-cosine/then-scalar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			d, _ := cosineDistanceInt8SIMD(i1, i2)
			avxUpperSink += float64(d) + float64(scalarAfterKernel(f1, float32(d)))
		}
	})
	b.Run("vecf8-dequant-then-ip", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			c1.Dequantize(d1)
			c2.Dequantize(d2)
			d, _ := InnerProduct(d1, d2)
			avxUpperSink += float64(d)
		}
	})
}
