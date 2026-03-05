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

import (
	"math"
	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	dist, err := L2DistanceSq(v1, v2)
	if err != nil {
		return dist, err
	}

	return T(math.Sqrt(float64(dist))), nil
}

func SumFloat32x16(v archsimd.Float32x16) float32 {
	var arr [16]float32
	v.Store(&arr)
	s0 := (arr[0] + arr[1]) + (arr[2] + arr[3])
	s1 := (arr[4] + arr[5]) + (arr[6] + arr[7])
	s2 := (arr[8] + arr[9]) + (arr[10] + arr[11])
	s3 := (arr[12] + arr[13]) + (arr[14] + arr[15])
	return (s0 + s1) + (s2 + s3)
}

func SumFloat32x8(v archsimd.Float32x8) float32 {
	var arr [8]float32
	v.Store(&arr)
	s0 := (arr[0] + arr[1]) + (arr[2] + arr[3])
	s1 := (arr[4] + arr[5]) + (arr[6] + arr[7])
	return s0 + s1
}

func SumFloat32x4(v archsimd.Float32x4) float32 {
	var arr [4]float32
	v.Store(&arr)
	return (arr[0] + arr[1]) + (arr[2] + arr[3])
}

func SumFloat64x8(v archsimd.Float64x8) float64 {
	var arr [8]float64
	v.Store(&arr)
	s0 := (arr[0] + arr[1]) + (arr[2] + arr[3])
	s1 := (arr[4] + arr[5]) + (arr[6] + arr[7])
	return s0 + s1
}

func SumFloat64x4(v archsimd.Float64x4) float64 {
	var arr [4]float64
	v.Store(&arr)
	return (arr[0] + arr[1]) + (arr[2] + arr[3])
}

func SumFloat64x2(v archsimd.Float64x2) float64 {
	var arr [2]float64
	v.Store(&arr)
	return arr[0] + arr[1]
}

func L2DistanceSqFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sumSq float32
	i := 0
	n := len(a)

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-128 {
			v0a := archsimd.LoadFloat32x16Slice(a[i : i+16])
			v0b := archsimd.LoadFloat32x16Slice(b[i : i+16])
			v1a := archsimd.LoadFloat32x16Slice(a[i+16 : i+32])
			v1b := archsimd.LoadFloat32x16Slice(b[i+16 : i+32])
			v2a := archsimd.LoadFloat32x16Slice(a[i+32 : i+48])
			v2b := archsimd.LoadFloat32x16Slice(b[i+32 : i+48])
			v3a := archsimd.LoadFloat32x16Slice(a[i+48 : i+64])
			v3b := archsimd.LoadFloat32x16Slice(b[i+48 : i+64])
			v4a := archsimd.LoadFloat32x16Slice(a[i+64 : i+80])
			v4b := archsimd.LoadFloat32x16Slice(b[i+64 : i+80])
			v5a := archsimd.LoadFloat32x16Slice(a[i+80 : i+96])
			v5b := archsimd.LoadFloat32x16Slice(b[i+80 : i+96])
			v6a := archsimd.LoadFloat32x16Slice(a[i+96 : i+112])
			v6b := archsimd.LoadFloat32x16Slice(b[i+96 : i+112])
			v7a := archsimd.LoadFloat32x16Slice(a[i+112 : i+128])
			v7b := archsimd.LoadFloat32x16Slice(b[i+112 : i+128])

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)
			d4, d5, d6, d7 := v4a.Sub(v4b), v5a.Sub(v5b), v6a.Sub(v6b), v7a.Sub(v7b)

			acc0, acc1 = d0.MulAdd(d0, acc0), d1.MulAdd(d1, acc1)
			acc2, acc3 = d2.MulAdd(d2, acc2), d3.MulAdd(d3, acc3)
			acc4, acc5 = d4.MulAdd(d4, acc4), d5.MulAdd(d5, acc5)
			acc6, acc7 = d6.MulAdd(d6, acc6), d7.MulAdd(d7, acc7)
			i += 128
		}
		for i <= n-16 {
			va := archsimd.LoadFloat32x16Slice(a[i : i+16])
			vb := archsimd.LoadFloat32x16Slice(b[i : i+16])
			d := va.Sub(vb)
			acc0 = d.MulAdd(d, acc0)
			i += 16
		}
		s0 := acc0.Add(acc1)
		s1 := acc2.Add(acc3)
		s2 := acc4.Add(acc5)
		s3 := acc6.Add(acc7)
		res := s0.Add(s1).Add(s2.Add(s3))
		sumSq += SumFloat32x16(res)
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-8 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat32x8Slice(a[i+8:i+16]), archsimd.LoadFloat32x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat32x8Slice(a[i+16:i+24]), archsimd.LoadFloat32x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat32x8Slice(a[i+24:i+32]), archsimd.LoadFloat32x8Slice(b[i+24:i+32])

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)
			acc0, acc1 = acc0.Add(d0.Mul(d0)), acc1.Add(d1.Mul(d1))
			acc2, acc3 = acc2.Add(d2.Mul(d2)), acc3.Add(d3.Mul(d3))
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			d := va.Sub(vb)
			acc0 = acc0.Add(d.Mul(d))
			i += 8
		}
		sumSq += SumFloat32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sumSq += diff * diff
	}
	return sumSq, nil
}

func L2DistanceSqFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sumSq float64
	i := 0
	n := len(a)

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		acc4, acc5, acc6, acc7 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-64 {
			v0a, v0b := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat64x8Slice(a[i+8:i+16]), archsimd.LoadFloat64x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat64x8Slice(a[i+16:i+24]), archsimd.LoadFloat64x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat64x8Slice(a[i+24:i+32]), archsimd.LoadFloat64x8Slice(b[i+24:i+32])
			v4a, v4b := archsimd.LoadFloat64x8Slice(a[i+32:i+40]), archsimd.LoadFloat64x8Slice(b[i+32:i+40])
			v5a, v5b := archsimd.LoadFloat64x8Slice(a[i+40:i+48]), archsimd.LoadFloat64x8Slice(b[i+40:i+48])
			v6a, v6b := archsimd.LoadFloat64x8Slice(a[i+48:i+56]), archsimd.LoadFloat64x8Slice(b[i+48:i+56])
			v7a, v7b := archsimd.LoadFloat64x8Slice(a[i+56:i+64]), archsimd.LoadFloat64x8Slice(b[i+56:i+64])

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)
			d4, d5, d6, d7 := v4a.Sub(v4b), v5a.Sub(v5b), v6a.Sub(v6b), v7a.Sub(v7b)

			acc0, acc1 = d0.MulAdd(d0, acc0), d1.MulAdd(d1, acc1)
			acc2, acc3 = d2.MulAdd(d2, acc2), d3.MulAdd(d3, acc3)
			acc4, acc5 = d4.MulAdd(d4, acc4), d5.MulAdd(d5, acc5)
			acc6, acc7 = d6.MulAdd(d6, acc6), d7.MulAdd(d7, acc7)
			i += 64
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			d := va.Sub(vb)
			acc0 = d.MulAdd(d, acc0)
			i += 8
		}
		s0, s1, s2, s3 := acc0.Add(acc1), acc2.Add(acc3), acc4.Add(acc5), acc6.Add(acc7)
		res := s0.Add(s1).Add(s2.Add(s3))
		sumSq += SumFloat64x8(res)
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-4 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			v0a, v0b := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			v1a, v1b := archsimd.LoadFloat64x4Slice(a[i+4:i+8]), archsimd.LoadFloat64x4Slice(b[i+4:i+8])
			v2a, v2b := archsimd.LoadFloat64x4Slice(a[i+8:i+12]), archsimd.LoadFloat64x4Slice(b[i+8:i+12])
			v3a, v3b := archsimd.LoadFloat64x4Slice(a[i+12:i+16]), archsimd.LoadFloat64x4Slice(b[i+12:i+16])

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)
			acc0, acc1 = acc0.Add(d0.Mul(d0)), acc1.Add(d1.Mul(d1) )
			acc2, acc3 = acc2.Add(d2.Mul(d2)), acc3.Add(d3.Mul(d3) )
			i += 16
		}
		for i <= n-4 {
			va, vb := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			d := va.Sub(vb)
			acc0 = acc0.Add(d.Mul(d))
			i += 4
		}
		sumSq += SumFloat64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sumSq += diff * diff
	}
	return sumSq, nil
}

func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		ret, err := L2DistanceSqFloat32(any(p).([]float32), any(q).([]float32))
		return T(ret), err
	case []float64:
		ret, err := L2DistanceSqFloat64(any(p).([]float64), any(q).([]float64))
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func L1DistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n, sum, i := len(a), float32(0), 0

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			v0a, v0b := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			v1a, v1b := archsimd.LoadFloat32x16Slice(a[i+16:i+32]), archsimd.LoadFloat32x16Slice(b[i+16:i+32])
			v2a, v2b := archsimd.LoadFloat32x16Slice(a[i+32:i+48]), archsimd.LoadFloat32x16Slice(b[i+32:i+48])
			v3a, v3b := archsimd.LoadFloat32x16Slice(a[i+48:i+64]), archsimd.LoadFloat32x16Slice(b[i+48:i+64])

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 64
		}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 16
		}
		sum += SumFloat32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-8 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat32x8Slice(a[i+8:i+16]), archsimd.LoadFloat32x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat32x8Slice(a[i+16:i+24]), archsimd.LoadFloat32x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat32x8Slice(a[i+24:i+32]), archsimd.LoadFloat32x8Slice(b[i+24:i+32])

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 8
		}
		sum += SumFloat32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		val := a[i] - b[i]
		if val < 0 { val = -val }
		sum += val
	}
	return sum, nil
}

func L1DistanceFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n, total, i := len(a), float64(0), 0

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat64x8Slice(a[i+8:i+16]), archsimd.LoadFloat64x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat64x8Slice(a[i+16:i+24]), archsimd.LoadFloat64x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat64x8Slice(a[i+24:i+32]), archsimd.LoadFloat64x8Slice(b[i+24:i+32])

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 8
		}
		total += SumFloat64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-4 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			v0a, v0b := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			v1a, v1b := archsimd.LoadFloat64x4Slice(a[i+4:i+8]), archsimd.LoadFloat64x4Slice(b[i+4:i+8])
			v2a, v2b := archsimd.LoadFloat64x4Slice(a[i+8:i+12]), archsimd.LoadFloat64x4Slice(b[i+8:i+12])
			v3a, v3b := archsimd.LoadFloat64x4Slice(a[i+12:i+16]), archsimd.LoadFloat64x4Slice(b[i+12:i+16])

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 16
		}
		for i <= n-4 {
			va, vb := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 4
		}
		total += SumFloat64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		val := a[i] - b[i]
		if val < 0 { val = -val }
		total += val
	}
	return total, nil
}

func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		ret, err := L1DistanceFloat32(any(p).([]float32), any(q).([]float32))
		return T(ret), err
	case []float64:
		ret, err := L1DistanceFloat64(any(p).([]float64), any(q).([]float64))
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func InnerProductFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n, total, i := len(a), float32(0), 0

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-128 {
			v0a, v0b := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			v1a, v1b := archsimd.LoadFloat32x16Slice(a[i+16:i+32]), archsimd.LoadFloat32x16Slice(b[i+16:i+32])
			v2a, v2b := archsimd.LoadFloat32x16Slice(a[i+32:i+48]), archsimd.LoadFloat32x16Slice(b[i+32:i+48])
			v3a, v3b := archsimd.LoadFloat32x16Slice(a[i+48:i+64]), archsimd.LoadFloat32x16Slice(b[i+48:i+64])
			v4a, v4b := archsimd.LoadFloat32x16Slice(a[i+64:i+80]), archsimd.LoadFloat32x16Slice(b[i+64:i+80])
			v5a, v5b := archsimd.LoadFloat32x16Slice(a[i+80:i+96]), archsimd.LoadFloat32x16Slice(b[i+80:i+96])
			v6a, v6b := archsimd.LoadFloat32x16Slice(a[i+96:i+112]), archsimd.LoadFloat32x16Slice(b[i+96:i+112])
			v7a, v7b := archsimd.LoadFloat32x16Slice(a[i+112:i+128]), archsimd.LoadFloat32x16Slice(b[i+112:i+128])

			acc0, acc1 = v0a.MulAdd(v0b, acc0), v1a.MulAdd(v1b, acc1)
			acc2, acc3 = v2a.MulAdd(v2b, acc2), v3a.MulAdd(v3b, acc3)
			acc4, acc5 = v4a.MulAdd(v4b, acc4), v5a.MulAdd(v5b, acc5)
			acc6, acc7 = v6a.MulAdd(v6b, acc6), v7a.MulAdd(v7b, acc7)
			i += 128
		}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			acc0 = va.MulAdd(vb, acc0)
			i += 16
		}
		s0, s1, s2, s3 := acc0.Add(acc1), acc2.Add(acc3), acc4.Add(acc5), acc6.Add(acc7)
		total += SumFloat32x16(s0.Add(s1).Add(s2.Add(s3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-8 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat32x8Slice(a[i+8:i+16]), archsimd.LoadFloat32x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat32x8Slice(a[i+16:i+24]), archsimd.LoadFloat32x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat32x8Slice(a[i+24:i+32]), archsimd.LoadFloat32x8Slice(b[i+24:i+32])

			acc0, acc1 = acc0.Add(v0a.Mul(v0b)), acc1.Add(v1a.Mul(v1b))
			acc2, acc3 = acc2.Add(v2a.Mul(v2b)), acc3.Add(v3a.Mul(v3b))
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			acc0 = acc0.Add(va.Mul(vb))
			i += 8
		}
		total += SumFloat32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	return -total, nil
}

func InnerProductFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n, total, i := len(a), float64(0), 0

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		acc4, acc5, acc6, acc7 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-64 {
			v0a, v0b := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat64x8Slice(a[i+8:i+16]), archsimd.LoadFloat64x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat64x8Slice(a[i+16:i+24]), archsimd.LoadFloat64x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat64x8Slice(a[i+24:i+32]), archsimd.LoadFloat64x8Slice(b[i+24:i+32])
			v4a, v4b := archsimd.LoadFloat64x8Slice(a[i+32:i+40]), archsimd.LoadFloat64x8Slice(b[i+32:i+40])
			v5a, v5b := archsimd.LoadFloat64x8Slice(a[i+40:i+48]), archsimd.LoadFloat64x8Slice(b[i+40:i+48])
			v6a, v6b := archsimd.LoadFloat64x8Slice(a[i+48:i+56]), archsimd.LoadFloat64x8Slice(b[i+48:i+56])
			v7a, v7b := archsimd.LoadFloat64x8Slice(a[i+56:i+64]), archsimd.LoadFloat64x8Slice(b[i+56:i+64])

			acc0, acc1 = v0a.MulAdd(v0b, acc0), v1a.MulAdd(v1b, acc1)
			acc2, acc3 = v2a.MulAdd(v2b, acc2), v3a.MulAdd(v3b, acc3)
			acc4, acc5 = v4a.MulAdd(v4b, acc4), v5a.MulAdd(v5b, acc5)
			acc6, acc7 = v6a.MulAdd(v6b, acc6), v7a.MulAdd(v7b, acc7)
			i += 64
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			acc0 = va.MulAdd(vb, acc0)
			i += 8
		}
		s0, s1, s2, s3 := acc0.Add(acc1), acc2.Add(acc3), acc4.Add(acc5), acc6.Add(acc7)
		total += SumFloat64x8(s0.Add(s1).Add(s2.Add(s3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-4 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			v0a, v0b := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			v1a, v1b := archsimd.LoadFloat64x4Slice(a[i+4:i+8]), archsimd.LoadFloat64x4Slice(b[i+4:i+8])
			v2a, v2b := archsimd.LoadFloat64x4Slice(a[i+8:i+12]), archsimd.LoadFloat64x4Slice(b[i+8:i+12])
			v3a, v3b := archsimd.LoadFloat64x4Slice(a[i+12:i+16]), archsimd.LoadFloat64x4Slice(b[i+12:i+16])

			acc0, acc1 = acc0.Add(v0a.Mul(v0b)), acc1.Add(v1a.Mul(v1b))
			acc2, acc3 = acc2.Add(v2a.Mul(v2b)), acc3.Add(v3a.Mul(v3b))
			i += 16
		}
		for i <= n-4 {
			va, vb := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			acc0 = acc0.Add(va.Mul(vb))
			i += 4
		}
		total += SumFloat64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	return -total, nil
}

func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		ret, err := InnerProductFloat32(any(p).([]float32), any(q).([]float32))
		return T(ret), err
	case []float64:
		ret, err := InnerProductFloat64(any(p).([]float64), any(q).([]float64))
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func CosineDistanceF32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float32
	i, n := 0, len(a)

	if archsimd.X86.AVX512() {
		accDot0, accA0, accB0 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		accDot1, accA1, accB1 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		accDot2, accA2, accB2 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		accDot3, accA3, accB3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			v0a, v0b := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			v1a, v1b := archsimd.LoadFloat32x16Slice(a[i+16:i+32]), archsimd.LoadFloat32x16Slice(b[i+16:i+32])
			v2a, v2b := archsimd.LoadFloat32x16Slice(a[i+32:i+48]), archsimd.LoadFloat32x16Slice(b[i+32:i+48])
			v3a, v3b := archsimd.LoadFloat32x16Slice(a[i+48:i+64]), archsimd.LoadFloat32x16Slice(b[i+48:i+64])

			accDot0, accA0, accB0 = v0a.MulAdd(v0b, accDot0), v0a.MulAdd(v0a, accA0), v0b.MulAdd(v0b, accB0)
			accDot1, accA1, accB1 = v1a.MulAdd(v1b, accDot1), v1a.MulAdd(v1a, accA1), v1b.MulAdd(v1b, accB1)
			accDot2, accA2, accB2 = v2a.MulAdd(v2b, accDot2), v2a.MulAdd(v2a, accA2), v2b.MulAdd(v2b, accB2)
			accDot3, accA3, accB3 = v3a.MulAdd(v3b, accDot3), v3a.MulAdd(v3a, accA3), v3b.MulAdd(v3b, accB3)
			i += 64
		}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			accDot0, accA0, accB0 = va.MulAdd(vb, accDot0), va.MulAdd(va, accA0), vb.MulAdd(vb, accB0)
			i += 16
		}
		dot += SumFloat32x16(accDot0.Add(accDot1).Add(accDot2.Add(accDot3)))
		normA += SumFloat32x16(accA0.Add(accA1).Add(accA2.Add(accA3)))
		normB += SumFloat32x16(accB0.Add(accB1).Add(accB2.Add(accB3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-8 {
		accDot0, accA0, accB0 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		accDot1, accA1, accB1 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-16 {
			v0a, v0b := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat32x8Slice(a[i+8:i+16]), archsimd.LoadFloat32x8Slice(b[i+8:i+16])
			accDot0, accA0, accB0 = accDot0.Add(v0a.Mul(v0b)), accA0.Add(v0a.Mul(v0a)), accB0.Add(v0b.Mul(v0b))
			accDot1, accA1, accB1 = accDot1.Add(v1a.Mul(v1b)), accA1.Add(v1a.Mul(v1a)), accB1.Add(v1b.Mul(v1b))
			i += 16
		}
		if i <= n-8 {
			va, vb := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			accDot0, accA0, accB0 = accDot0.Add(va.Mul(vb)), accA0.Add(va.Mul(va)), accB0.Add(vb.Mul(vb))
			i += 8
		}
		dot += SumFloat32x8(accDot0.Add(accDot1))
		normA += SumFloat32x8(accA0.Add(accA1))
		normB += SumFloat32x8(accB0.Add(accB1))
	}

	for ; i < n; i++ {
		dot, normA, normB = dot + a[i]*b[i], normA + a[i]*a[i], normB + b[i]*b[i]
	}

	denominator := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if denominator == 0 { return 1.0, nil }
	similarity := float64(dot) / denominator
	return float32(1.0 - similarity), nil
}

func CosineDistanceF64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float64
	i, n := 0, len(a)

	if archsimd.X86.AVX512() {
		accDot0, accA0, accB0 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		accDot1, accA1, accB1 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		accDot2, accA2, accB2 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		accDot3, accA3, accB3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat64x8Slice(a[i+8:i+16]), archsimd.LoadFloat64x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat64x8Slice(a[i+16:i+24]), archsimd.LoadFloat64x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat64x8Slice(a[i+24:i+32]), archsimd.LoadFloat64x8Slice(b[i+24:i+32])

			accDot0, accA0, accB0 = v0a.MulAdd(v0b, accDot0), v0a.MulAdd(v0a, accA0), v0b.MulAdd(v0b, accB0)
			accDot1, accA1, accB1 = v1a.MulAdd(v1b, accDot1), v1a.MulAdd(v1a, accA1), v1b.MulAdd(v1b, accB1)
			accDot2, accA2, accB2 = v2a.MulAdd(v2b, accDot2), v2a.MulAdd(v2a, accA2), v2b.MulAdd(v2b, accB2)
			accDot3, accA3, accB3 = v3a.MulAdd(v3b, accDot3), v3a.MulAdd(v3a, accA3), v3b.MulAdd(v3b, accB3)
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			accDot0, accA0, accB0 = va.MulAdd(vb, accDot0), va.MulAdd(va, accA0), vb.MulAdd(vb, accB0)
			i += 8
		}
		dot += SumFloat64x8(accDot0.Add(accDot1).Add(accDot2.Add(accDot3)))
		normA += SumFloat64x8(accA0.Add(accA1).Add(accA2.Add(accA3)))
		normB += SumFloat64x8(accB0.Add(accB1).Add(accB2.Add(accB3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-4 {
		accDot0, accA0, accB0 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		accDot1, accA1, accB1 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-8 {
			v0a, v0b := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			v1a, v1b := archsimd.LoadFloat64x4Slice(a[i+4:i+8]), archsimd.LoadFloat64x4Slice(b[i+4:i+8])
			accDot0, accA0, accB0 = accDot0.Add(v0a.Mul(v0b)), accA0.Add(v0a.Mul(v0a)), accB0.Add(v0b.Mul(v0b))
			accDot1, accA1, accB1 = accDot1.Add(v1a.Mul(v1b)), accA1.Add(v1a.Mul(v1a)), accB1.Add(v1b.Mul(v1b))
			i += 8
		}
		if i <= n-4 {
			va, vb := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			accDot0, accA0, accB0 = accDot0.Add(va.Mul(vb)), accA0.Add(va.Mul(va)), accB0.Add(vb.Mul(vb))
			i += 4
		}
		dot += SumFloat64x4(accDot0.Add(accDot1))
		normA += SumFloat64x4(accA0.Add(accA1))
		normB += SumFloat64x4(accB0.Add(accB1))
	}

	for ; i < n; i++ {
		dot, normA, normB = dot + a[i]*b[i], normA + a[i]*a[i], normB + b[i]*b[i]
	}

	denominator := math.Sqrt(normA) * math.Sqrt(normB)
	if denominator == 0 { return 1.0, nil }
	similarity := dot / denominator
	return 1.0 - similarity, nil
}

func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		ret, err := CosineDistanceF32(any(p).([]float32), any(q).([]float32))
		return T(ret), err
	case []float64:
		ret, err := CosineDistanceF64(any(p).([]float64), any(q).([]float64))
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func CosineSimilarityF32(a, b []float32) (float32, error) {
	if len(a) == 0 { return 0, nil }
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float32
	i, n := 0, len(a)

	if archsimd.X86.AVX512() {
		accDot0, accA0, accB0 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		accDot1, accA1, accB1 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		accDot2, accA2, accB2 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		accDot3, accA3, accB3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			v0a, v0b := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			v1a, v1b := archsimd.LoadFloat32x16Slice(a[i+16:i+32]), archsimd.LoadFloat32x16Slice(b[i+16:i+32])
			v2a, v2b := archsimd.LoadFloat32x16Slice(a[i+32:i+48]), archsimd.LoadFloat32x16Slice(b[i+32:i+48])
			v3a, v3b := archsimd.LoadFloat32x16Slice(a[i+48:i+64]), archsimd.LoadFloat32x16Slice(b[i+48:i+64])

			accDot0, accA0, accB0 = v0a.MulAdd(v0b, accDot0), v0a.MulAdd(v0a, accA0), v0b.MulAdd(v0b, accB0)
			accDot1, accA1, accB1 = v1a.MulAdd(v1b, accDot1), v1a.MulAdd(v1a, accA1), v1b.MulAdd(v1b, accB1)
			accDot2, accA2, accB2 = v2a.MulAdd(v2b, accDot2), v2a.MulAdd(v2a, accA2), v2b.MulAdd(v2b, accB2)
			accDot3, accA3, accB3 = v3a.MulAdd(v3b, accDot3), v3a.MulAdd(v3a, accA3), v3b.MulAdd(v3b, accB3)
			i += 64
		}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			accDot0, accA0, accB0 = va.MulAdd(vb, accDot0), va.MulAdd(va, accA0), vb.MulAdd(vb, accB0)
			i += 16
		}
		dot += SumFloat32x16(accDot0.Add(accDot1).Add(accDot2.Add(accDot3)))
		normA += SumFloat32x16(accA0.Add(accA1).Add(accA2.Add(accA3)))
		normB += SumFloat32x16(accB0.Add(accB1).Add(accB2.Add(accB3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-8 {
		accDot0, accA0, accB0 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		accDot1, accA1, accB1 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-16 {
			v0a, v0b := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat32x8Slice(a[i+8:i+16]), archsimd.LoadFloat32x8Slice(b[i+8:i+16])
			accDot0, accA0, accB0 = accDot0.Add(v0a.Mul(v0b)), accA0.Add(v0a.Mul(v0a)), accB0.Add(v0b.Mul(v0b))
			accDot1, accA1, accB1 = accDot1.Add(v1a.Mul(v1b)), accA1.Add(v1a.Mul(v1a)), accB1.Add(v1b.Mul(v1b))
			i += 16
		}
		if i <= n-8 {
			va, vb := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			accDot0, accA0, accB0 = accDot0.Add(va.Mul(vb)), accA0.Add(va.Mul(va)), accB0.Add(vb.Mul(vb))
			i += 8
		}
		dot += SumFloat32x8(accDot0.Add(accDot1))
		normA += SumFloat32x8(accA0.Add(accA1))
		normB += SumFloat32x8(accB0.Add(accB1))
	}

	for ; i < n; i++ {
		dot, normA, normB = dot + a[i]*b[i], normA + a[i]*a[i], normB + b[i]*b[i]
	}

	denominator := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if denominator == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	similarity := float64(dot) / denominator
	return float32(similarity), nil
}

func CosineSimilarityF64(a, b []float64) (float64, error) {
	if len(a) == 0 { return 0, nil }
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float64
	i, n := 0, len(a)

	if archsimd.X86.AVX512() {
		accDot0, accA0, accB0 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		accDot1, accA1, accB1 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		accDot2, accA2, accB2 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		accDot3, accA3, accB3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat64x8Slice(a[i+8:i+16]), archsimd.LoadFloat64x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat64x8Slice(a[i+16:i+24]), archsimd.LoadFloat64x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat64x8Slice(a[i+24:i+32]), archsimd.LoadFloat64x8Slice(b[i+24:i+32])

			accDot0, accA0, accB0 = v0a.MulAdd(v0b, accDot0), v0a.MulAdd(v0a, accA0), v0b.MulAdd(v0b, accB0)
			accDot1, accA1, accB1 = v1a.MulAdd(v1b, accDot1), v1a.MulAdd(v1a, accA1), v1b.MulAdd(v1b, accB1)
			accDot2, accA2, accB2 = v2a.MulAdd(v2b, accDot2), v2a.MulAdd(v2a, accA2), v2b.MulAdd(v2b, accB2)
			accDot3, accA3, accB3 = v3a.MulAdd(v3b, accDot3), v3a.MulAdd(v3a, accA3), v3b.MulAdd(v3b, accB3)
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			accDot0, accA0, accB0 = va.MulAdd(vb, accDot0), va.MulAdd(va, accA0), vb.MulAdd(vb, accB0)
			i += 8
		}
		dot += SumFloat64x8(accDot0.Add(accDot1).Add(accDot2.Add(accDot3)))
		normA += SumFloat64x8(accA0.Add(accA1).Add(accA2.Add(accA3)))
		normB += SumFloat64x8(accB0.Add(accB1).Add(accB2.Add(accB3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-4 {
		accDot0, accA0, accB0 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		accDot1, accA1, accB1 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-8 {
			v0a, v0b := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			v1a, v1b := archsimd.LoadFloat64x4Slice(a[i+4:i+8]), archsimd.LoadFloat64x4Slice(b[i+4:i+8])
			accDot0, accA0, accB0 = accDot0.Add(v0a.Mul(v0b)), accA0.Add(v0a.Mul(v0a)), accB0.Add(v0b.Mul(v0b))
			accDot1, accA1, accB1 = accDot1.Add(v1a.Mul(v1b)), accA1.Add(v1a.Mul(v1a)), accB1.Add(v1b.Mul(v1b))
			i += 8
		}
		if i <= n-4 {
			va, vb := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			accDot0, accA0, accB0 = accDot0.Add(va.Mul(vb)), accA0.Add(va.Mul(va)), accB0.Add(vb.Mul(vb))
			i += 4
		}
		dot += SumFloat64x4(accDot0.Add(accDot1))
		normA += SumFloat64x4(accA0.Add(accA1))
		normB += SumFloat64x4(accB0.Add(accB1))
	}

	for ; i < n; i++ {
		dot, normA, normB = dot + a[i]*b[i], normA + a[i]*a[i], normB + b[i]*b[i]
	}

	denominator := math.Sqrt(normA) * math.Sqrt(normB)
	if denominator == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	similarity := dot / denominator
	return similarity, nil
}

func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		ret, err := CosineSimilarityF32(any(p).([]float32), any(q).([]float32))
		return T(ret), err
	case []float64:
		ret, err := CosineSimilarityF64(any(p).([]float64), any(q).([]float64))
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func SphericalDistanceFloat32(a, b []float32) (float32, error) {
	if len(a) != len(b) {
		return float32(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n, total, i := len(a), float32(0), 0

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		acc4, acc5, acc6, acc7 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-128 {
			v0a, v0b := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			v1a, v1b := archsimd.LoadFloat32x16Slice(a[i+16:i+32]), archsimd.LoadFloat32x16Slice(b[i+16:i+32])
			v2a, v2b := archsimd.LoadFloat32x16Slice(a[i+32:i+48]), archsimd.LoadFloat32x16Slice(b[i+32:i+48])
			v3a, v3b := archsimd.LoadFloat32x16Slice(a[i+48:i+64]), archsimd.LoadFloat32x16Slice(b[i+48:i+64])
			v4a, v4b := archsimd.LoadFloat32x16Slice(a[i+64:i+80]), archsimd.LoadFloat32x16Slice(b[i+64:i+80])
			v5a, v5b := archsimd.LoadFloat32x16Slice(a[i+80:i+96]), archsimd.LoadFloat32x16Slice(b[i+80:i+96])
			v6a, v6b := archsimd.LoadFloat32x16Slice(a[i+96:i+112]), archsimd.LoadFloat32x16Slice(b[i+96:i+112])
			v7a, v7b := archsimd.LoadFloat32x16Slice(a[i+112:i+128]), archsimd.LoadFloat32x16Slice(b[i+112:i+128])

			acc0, acc1 = v0a.MulAdd(v0b, acc0), v1a.MulAdd(v1b, acc1)
			acc2, acc3 = v2a.MulAdd(v2b, acc2), v3a.MulAdd(v3b, acc3)
			acc4, acc5 = v4a.MulAdd(v4b, acc4), v5a.MulAdd(v5b, acc5)
			acc6, acc7 = v6a.MulAdd(v6b, acc6), v7a.MulAdd(v7b, acc7)
			i += 128
		}
		for i <= n-16 {
			va, vb := archsimd.LoadFloat32x16Slice(a[i:i+16]), archsimd.LoadFloat32x16Slice(b[i:i+16])
			acc0 = va.MulAdd(vb, acc0)
			i += 16
		}
		s0, s1, s2, s3 := acc0.Add(acc1), acc2.Add(acc3), acc4.Add(acc5), acc6.Add(acc7)
		total += SumFloat32x16(s0.Add(s1).Add(s2.Add(s3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-8 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			v0a, v0b := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat32x8Slice(a[i+8:i+16]), archsimd.LoadFloat32x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat32x8Slice(a[i+16:i+24]), archsimd.LoadFloat32x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat32x8Slice(a[i+24:i+32]), archsimd.LoadFloat32x8Slice(b[i+24:i+32])

			acc0, acc1 = acc0.Add(v0a.Mul(v0b)), acc1.Add(v1a.Mul(v1b))
			acc2, acc3 = acc2.Add(v2a.Mul(v2b)), acc3.Add(v3a.Mul(v3b))
			i += 32
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat32x8Slice(a[i:i+8]), archsimd.LoadFloat32x8Slice(b[i:i+8])
			acc0 = acc0.Add(va.Mul(vb))
			i += 8
		}
		total += SumFloat32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	if total > 1.0 { total = 1.0 } else if total < -1.0 { total = -1.0 }
	theta := math.Acos(float64(total))
	return float32(theta / math.Pi), nil
}

func SphericalDistanceFloat64(a, b []float64) (float64, error) {
	if len(a) != len(b) {
		return float64(0), moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	n, total, i := len(a), float64(0), 0

	if archsimd.X86.AVX512() {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		acc4, acc5, acc6, acc7 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-64 {
			v0a, v0b := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			v1a, v1b := archsimd.LoadFloat64x8Slice(a[i+8:i+16]), archsimd.LoadFloat64x8Slice(b[i+8:i+16])
			v2a, v2b := archsimd.LoadFloat64x8Slice(a[i+16:i+24]), archsimd.LoadFloat64x8Slice(b[i+16:i+24])
			v3a, v3b := archsimd.LoadFloat64x8Slice(a[i+24:i+32]), archsimd.LoadFloat64x8Slice(b[i+24:i+32])
			v4a, v4b := archsimd.LoadFloat64x8Slice(a[i+32:i+40]), archsimd.LoadFloat64x8Slice(b[i+32:i+40])
			v5a, v5b := archsimd.LoadFloat64x8Slice(a[i+40:i+48]), archsimd.LoadFloat64x8Slice(b[i+40:i+48])
			v6a, v6b := archsimd.LoadFloat64x8Slice(a[i+48:i+56]), archsimd.LoadFloat64x8Slice(b[i+48:i+56])
			v7a, v7b := archsimd.LoadFloat64x8Slice(a[i+56:i+64]), archsimd.LoadFloat64x8Slice(b[i+56:i+64])

			acc0, acc1 = v0a.MulAdd(v0b, acc0), v1a.MulAdd(v1b, acc1)
			acc2, acc3 = v2a.MulAdd(v2b, acc2), v3a.MulAdd(v3b, acc3)
			acc4, acc5 = v4a.MulAdd(v4b, acc4), v5a.MulAdd(v5b, acc5)
			acc6, acc7 = v6a.MulAdd(v6b, acc6), v7a.MulAdd(v7b, acc7)
			i += 64
		}
		for i <= n-8 {
			va, vb := archsimd.LoadFloat64x8Slice(a[i:i+8]), archsimd.LoadFloat64x8Slice(b[i:i+8])
			acc0 = va.MulAdd(vb, acc0)
			i += 8
		}
		s0, s1, s2, s3 := acc0.Add(acc1), acc2.Add(acc3), acc4.Add(acc5), acc6.Add(acc7)
		total += SumFloat64x8(s0.Add(s1).Add(s2.Add(s3)))
	}

	if (archsimd.X86.AVX2() || archsimd.X86.AVX()) && i <= n-4 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			v0a, v0b := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			v1a, v1b := archsimd.LoadFloat64x4Slice(a[i+4:i+8]), archsimd.LoadFloat64x4Slice(b[i+4:i+8])
			v2a, v2b := archsimd.LoadFloat64x4Slice(a[i+8:i+12]), archsimd.LoadFloat64x4Slice(b[i+8:i+12])
			v3a, v3b := archsimd.LoadFloat64x4Slice(a[i+12:i+16]), archsimd.LoadFloat64x4Slice(b[i+12:i+16])

			acc0, acc1 = acc0.Add(v0a.Mul(v0b)), acc1.Add(v1a.Mul(v1b))
			acc2, acc3 = acc2.Add(v2a.Mul(v2b)), acc3.Add(v3a.Mul(v3b))
			i += 16
		}
		for i <= n-4 {
			va, vb := archsimd.LoadFloat64x4Slice(a[i:i+4]), archsimd.LoadFloat64x4Slice(b[i:i+4])
			acc0 = acc0.Add(va.Mul(vb))
			i += 4
		}
		total += SumFloat64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	if total > 1.0 { total = 1.0 } else if total < -1.0 { total = -1.0 }
	theta := math.Acos(total)
	return theta / math.Pi, nil
}

func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {
	switch any(p).(type) {
	case []float32:
		ret, err := SphericalDistanceFloat32(any(p).([]float32), any(q).([]float32))
		return T(ret), err
	case []float64:
		ret, err := SphericalDistanceFloat64(any(p).([]float64), any(q).([]float64))
		return T(ret), err
	default:
		return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
	}
}

func NormalizeL2[T types.RealNumbers](v1 []T, normalized []T) error {
	if len(v1) == 0 { return moerr.NewInternalErrorNoCtx("cannot normalize empty vector") }
	var sumSquares float64
	for _, val := range v1 { sumSquares += float64(val) * float64(val) }
	norm := math.Sqrt(sumSquares)
	if norm == 0 { copy(normalized, v1); return nil }
	for i, val := range v1 { normalized[i] = T(float64(val) / norm) }
	return nil
}

func ScaleInPlace[T types.RealNumbers](v []T, scale T) {
	for i := range v { v[i] *= scale }
}
