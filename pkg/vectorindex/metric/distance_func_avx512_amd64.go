//go:build amd64 && go1.26 && goexperiment.simd

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

import "simd/archsimd"

// Keep SIMD locals behind a non-inlined boundary: debug builds may initialize
// them in the function prologue, before any source-level CPU or length guard.
// Callers must check hasAVX512 and their existing block-size threshold first.
// Only scalar prefix results cross back to the baseline entrypoints, which
// retain their original accumulation order, tails and error handling.

//go:noinline
func l2DistanceSqFloat32AVX512(a, b []float32) (float32, int) {
	n := len(a)
	i := 0
	acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
	for i <= n-64 {
		as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
		d0 := archsimd.LoadFloat32x16Slice(as[0:16]).Sub(archsimd.LoadFloat32x16Slice(bs[0:16]))
		d1 := archsimd.LoadFloat32x16Slice(as[16:32]).Sub(archsimd.LoadFloat32x16Slice(bs[16:32]))
		d2 := archsimd.LoadFloat32x16Slice(as[32:48]).Sub(archsimd.LoadFloat32x16Slice(bs[32:48]))
		d3 := archsimd.LoadFloat32x16Slice(as[48:64]).Sub(archsimd.LoadFloat32x16Slice(bs[48:64]))

		acc0 = d0.MulAdd(d0, acc0)
		acc1 = d1.MulAdd(d1, acc1)
		acc2 = d2.MulAdd(d2, acc2)
		acc3 = d3.MulAdd(d3, acc3)
		i += 64
	}
	return sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3))), i
}

//go:noinline
func innerProductFloat32AVX512(a, b []float32) (float32, int) {
	n := len(a)
	i := 0
	acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
	for i <= n-64 {
		as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
		acc0 = archsimd.LoadFloat32x16Slice(as[0:16]).MulAdd(archsimd.LoadFloat32x16Slice(bs[0:16]), acc0)
		acc1 = archsimd.LoadFloat32x16Slice(as[16:32]).MulAdd(archsimd.LoadFloat32x16Slice(bs[16:32]), acc1)
		acc2 = archsimd.LoadFloat32x16Slice(as[32:48]).MulAdd(archsimd.LoadFloat32x16Slice(bs[32:48]), acc2)
		acc3 = archsimd.LoadFloat32x16Slice(as[48:64]).MulAdd(archsimd.LoadFloat32x16Slice(bs[48:64]), acc3)
		i += 64
	}
	return sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3))), i
}

//go:noinline
func l2DistanceSqFloat64AVX512(a, b []float64) (float64, int) {
	n := len(a)
	i := 0
	acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
	for i <= n-32 {
		as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
		d0 := archsimd.LoadFloat64x8Slice(as[0:8]).Sub(archsimd.LoadFloat64x8Slice(bs[0:8]))
		d1 := archsimd.LoadFloat64x8Slice(as[8:16]).Sub(archsimd.LoadFloat64x8Slice(bs[8:16]))
		d2 := archsimd.LoadFloat64x8Slice(as[16:24]).Sub(archsimd.LoadFloat64x8Slice(bs[16:24]))
		d3 := archsimd.LoadFloat64x8Slice(as[24:32]).Sub(archsimd.LoadFloat64x8Slice(bs[24:32]))
		acc0 = d0.MulAdd(d0, acc0)
		acc1 = d1.MulAdd(d1, acc1)
		acc2 = d2.MulAdd(d2, acc2)
		acc3 = d3.MulAdd(d3, acc3)
		i += 32
	}
	return sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3))), i
}

//go:noinline
func innerProductFloat64AVX512(a, b []float64) (float64, int) {
	n := len(a)
	i := 0
	acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
	for i <= n-32 {
		as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
		acc0 = archsimd.LoadFloat64x8Slice(as[0:8]).MulAdd(archsimd.LoadFloat64x8Slice(bs[0:8]), acc0)
		acc1 = archsimd.LoadFloat64x8Slice(as[8:16]).MulAdd(archsimd.LoadFloat64x8Slice(bs[8:16]), acc1)
		acc2 = archsimd.LoadFloat64x8Slice(as[16:24]).MulAdd(archsimd.LoadFloat64x8Slice(bs[16:24]), acc2)
		acc3 = archsimd.LoadFloat64x8Slice(as[24:32]).MulAdd(archsimd.LoadFloat64x8Slice(bs[24:32]), acc3)
		i += 32
	}
	return sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3))), i
}

//go:noinline
func l1DistanceFloat32AVX512(a, b []float32) (float32, int) {
	n := len(a)
	i := 0
	acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
	for i <= n-64 {
		as, bs := a[i:i+64:i+64], b[i:i+64:i+64]
		acc0 = acc0.Add(archsimd.LoadFloat32x16Slice(as[0:16]).Sub(archsimd.LoadFloat32x16Slice(bs[0:16])).Max(archsimd.LoadFloat32x16Slice(bs[0:16]).Sub(archsimd.LoadFloat32x16Slice(as[0:16]))))
		acc1 = acc1.Add(archsimd.LoadFloat32x16Slice(as[16:32]).Sub(archsimd.LoadFloat32x16Slice(bs[16:32])).Max(archsimd.LoadFloat32x16Slice(bs[16:32]).Sub(archsimd.LoadFloat32x16Slice(as[16:32]))))
		acc2 = acc2.Add(archsimd.LoadFloat32x16Slice(as[32:48]).Sub(archsimd.LoadFloat32x16Slice(bs[32:48])).Max(archsimd.LoadFloat32x16Slice(bs[32:48]).Sub(archsimd.LoadFloat32x16Slice(as[32:48]))))
		acc3 = acc3.Add(archsimd.LoadFloat32x16Slice(as[48:64]).Sub(archsimd.LoadFloat32x16Slice(bs[48:64])).Max(archsimd.LoadFloat32x16Slice(bs[48:64]).Sub(archsimd.LoadFloat32x16Slice(as[48:64]))))
		i += 64
	}
	return sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3))), i
}

//go:noinline
func l1DistanceFloat64AVX512(a, b []float64) (float64, int) {
	n := len(a)
	i := 0
	acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
	for i <= n-32 {
		as, bs := a[i:i+32:i+32], b[i:i+32:i+32]
		acc0 = acc0.Add(archsimd.LoadFloat64x8Slice(as[0:8]).Sub(archsimd.LoadFloat64x8Slice(bs[0:8])).Max(archsimd.LoadFloat64x8Slice(bs[0:8]).Sub(archsimd.LoadFloat64x8Slice(as[0:8]))))
		acc1 = acc1.Add(archsimd.LoadFloat64x8Slice(as[8:16]).Sub(archsimd.LoadFloat64x8Slice(bs[8:16])).Max(archsimd.LoadFloat64x8Slice(bs[8:16]).Sub(archsimd.LoadFloat64x8Slice(as[8:16]))))
		acc2 = acc2.Add(archsimd.LoadFloat64x8Slice(as[16:24]).Sub(archsimd.LoadFloat64x8Slice(bs[16:24])).Max(archsimd.LoadFloat64x8Slice(bs[16:24]).Sub(archsimd.LoadFloat64x8Slice(as[16:24]))))
		acc3 = acc3.Add(archsimd.LoadFloat64x8Slice(as[24:32]).Sub(archsimd.LoadFloat64x8Slice(bs[24:32])).Max(archsimd.LoadFloat64x8Slice(bs[24:32]).Sub(archsimd.LoadFloat64x8Slice(as[24:32]))))
		i += 32
	}
	return sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3))), i
}

//go:noinline
func cosineAccumFloat32AVX512(a, b []float32) (float32, float32, float32, int) {
	n := len(a)
	i := 0
	accD, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
	for i <= n-16 {
		va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
		accD = va.MulAdd(vb, accD)
		accA = va.MulAdd(va, accA)
		accB = vb.MulAdd(vb, accB)
		i += 16
	}
	return sumF32x16(accD), sumF32x16(accA), sumF32x16(accB), i
}

//go:noinline
func cosineAccumFloat64AVX512(a, b []float64) (float64, float64, float64, int) {
	n := len(a)
	i := 0
	accD, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
	for i <= n-8 {
		va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
		accD = va.MulAdd(vb, accD)
		accA = va.MulAdd(va, accA)
		accB = vb.MulAdd(vb, accB)
		i += 8
	}
	return sumF64x8(accD), sumF64x8(accA), sumF64x8(accB), i
}
