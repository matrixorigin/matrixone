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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

var (
	hasAVX512 = archsimd.X86.AVX512()
	hasAVX2   = archsimd.X86.AVX2() || archsimd.X86.AVX()
)

func L2Distance[T types.RealNumbers](v1, v2 []T) (T, error) {
	if pf32, ok := any(v1).([]float32); ok {
		dist, err := L2DistanceSqFloat32(pf32, any(v2).([]float32))
		if err != nil {
			return 0, err
		}
		return T(math.Sqrt(float64(dist))), nil
	}
	if pf64, ok := any(v1).([]float64); ok {
		dist, err := L2DistanceSqFloat64(pf64, any(v2).([]float64))
		if err != nil {
			return 0, err
		}
		return T(math.Sqrt(dist)), nil
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

// Inlineable sum helpers
func sumF32x16(v archsimd.Float32x16) float32 {
	var a [16]float32
	v.Store(&a)
	return (a[0] + a[1] + a[2] + a[3]) + (a[4] + a[5] + a[6] + a[7]) +
		(a[8] + a[9] + a[10] + a[11]) + (a[12] + a[13] + a[14] + a[15])
}

func sumF32x8(v archsimd.Float32x8) float32 {
	var a [8]float32
	v.Store(&a)
	return (a[0] + a[1] + a[2] + a[3]) + (a[4] + a[5] + a[6] + a[7])
}

func sumF64x8(v archsimd.Float64x8) float64 {
	var a [8]float64
	v.Store(&a)
	return (a[0] + a[1] + a[2] + a[3]) + (a[4] + a[5] + a[6] + a[7])
}

func sumF64x4(v archsimd.Float64x4) float64 {
	var a [4]float64
	v.Store(&a)
	return (a[0] + a[1]) + (a[2] + a[3])
}

func L2DistanceSqFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum float32
	i := 0

	if n >= 16 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			_ = a[i+63]
			_ = b[i+63]
			v0a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+16])))
			v1b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+16])))
			v2a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+32])))
			v2b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+32])))
			v3a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+48])))
			v3b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+48])))

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)

			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 64
		}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			va := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			d := va.Sub(vb)
			acc0 = d.MulAdd(d, acc0)
			i += 16
		}
		sum += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 8 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+24])))

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)

			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			d := va.Sub(vb)
			acc0 = d.MulAdd(d, acc0)
			i += 8
		}
		sum += sumF32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum, nil
}

func L2DistanceSqFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum float64
	i := 0

	if n >= 8 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+24])))

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)

			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			d := va.Sub(vb)
			acc0 = d.MulAdd(d, acc0)
			i += 8
		}
		sum += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 4 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			v0a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+4])))
			v1b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+4])))
			v2a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+8])))
			v2b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+8])))
			v3a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+12])))
			v3b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+12])))

			d0, d1, d2, d3 := v0a.Sub(v0b), v1a.Sub(v1b), v2a.Sub(v2b), v3a.Sub(v3b)

			acc0 = d0.MulAdd(d0, acc0)
			acc1 = d1.MulAdd(d1, acc1)
			acc2 = d2.MulAdd(d2, acc2)
			acc3 = d3.MulAdd(d3, acc3)
			i += 16
		}
		for i <= n-4 {
			_ = a[i+3]
			_ = b[i+3]
			va := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			d := va.Sub(vb)
			acc0 = d.MulAdd(d, acc0)
			i += 4
		}
		sum += sumF64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		diff := a[i] - b[i]
		sum += diff * diff
	}
	return sum, nil
}

func L2DistanceSq[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := L2DistanceSqFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := L2DistanceSqFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func L1DistanceFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum float32
	i := 0

	if n >= 16 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			_ = a[i+63]
			_ = b[i+63]
			v0a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+16])))
			v1b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+16])))
			v2a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+32])))
			v2b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+32])))
			v3a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+48])))
			v3b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+48])))

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 64
		}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			va := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 16
		}
		sum += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 8 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+24])))

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 8
		}
		sum += sumF32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		val := a[i] - b[i]
		if val < 0 { val = -val }
		sum += val
	}
	return sum, nil
}

func L1DistanceFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var sum float64
	i := 0

	if n >= 8 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+24])))

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 8
		}
		sum += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 4 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			v0a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+4])))
			v1b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+4])))
			v2a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+8])))
			v2b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+8])))
			v3a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+12])))
			v3b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+12])))

			acc0 = acc0.Add(v0a.Sub(v0b).Max(v0b.Sub(v0a)))
			acc1 = acc1.Add(v1a.Sub(v1b).Max(v1b.Sub(v1a)))
			acc2 = acc2.Add(v2a.Sub(v2b).Max(v2b.Sub(v2a)))
			acc3 = acc3.Add(v3a.Sub(v3b).Max(v3b.Sub(v3a)))
			i += 16
		}
		for i <= n-4 {
			_ = a[i+3]
			_ = b[i+3]
			va := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			acc0 = acc0.Add(va.Sub(vb).Max(vb.Sub(va)))
			i += 4
		}
		sum += sumF64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ {
		val := a[i] - b[i]
		if val < 0 { val = -val }
		sum += val
	}
	return sum, nil
}

func L1Distance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := L1DistanceFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := L1DistanceFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func InnerProductFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var total float32
	i := 0

	if n >= 16 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			_ = a[i+63]
			_ = b[i+63]
			v0a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+16])))
			v1b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+16])))
			v2a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+32])))
			v2b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+32])))
			v3a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+48])))
			v3b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+48])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 64
		}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			va := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 16
		}
		total += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 8 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+24])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 8
		}
		total += sumF32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	return -total, nil
}

func InnerProductFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var total float64
	i := 0

	if n >= 8 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+24])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 8
		}
		total += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 4 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			v0a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+4])))
			v1b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+4])))
			v2a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+8])))
			v2b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+8])))
			v3a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+12])))
			v3b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+12])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 16
		}
		for i <= n-4 {
			_ = a[i+3]
			_ = b[i+3]
			va := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 4
		}
		total += sumF64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	return -total, nil
}

func InnerProduct[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := InnerProductFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := InnerProductFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func CosineDistanceF32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float32
	i := 0

	if n >= 16 && hasAVX512 {
		accD, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			va := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 16
		}
		dot, normA, normB = sumF32x16(accD), sumF32x16(accA), sumF32x16(accB)
	} else if n >= 8 && hasAVX2 {
		accD, accA, accB := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 8
		}
		dot, normA, normB = sumF32x8(accD), sumF32x8(accA), sumF32x8(accB)
	}

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if denominator == 0 { return 1.0, nil }
	similarity := float64(dot) / denominator
	return float32(1.0 - similarity), nil
}

func CosineDistanceF64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float64
	i := 0

	if n >= 8 && hasAVX512 {
		accD, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 8
		}
		dot, normA, normB = sumF64x8(accD), sumF64x8(accA), sumF64x8(accB)
	} else if n >= 4 && hasAVX2 {
		accD, accA, accB := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-4 {
			_ = a[i+3]
			_ = b[i+3]
			va := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 4
		}
		dot, normA, normB = sumF64x4(accD), sumF64x4(accA), sumF64x4(accB)
	}

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(normA) * math.Sqrt(normB)
	if denominator == 0 { return 1.0, nil }
	similarity := dot / denominator
	return 1.0 - similarity, nil
}

func CosineDistance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := CosineDistanceF32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := CosineDistanceF64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func CosineSimilarityF32(a, b []float32) (float32, error) {
	n := len(a)
	if n == 0 { return 0, nil }
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float32
	i := 0

	if n >= 16 && hasAVX512 {
		accD, accA, accB := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			va := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 16
		}
		dot, normA, normB = sumF32x16(accD), sumF32x16(accA), sumF32x16(accB)
	} else if n >= 8 && hasAVX2 {
		accD, accA, accB := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 8
		}
		dot, normA, normB = sumF32x8(accD), sumF32x8(accA), sumF32x8(accB)
	}

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(float64(normA)) * math.Sqrt(float64(normB))
	if denominator == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	similarity := float64(dot) / denominator
	return float32(similarity), nil
}

func CosineSimilarityF64(a, b []float64) (float64, error) {
	n := len(a)
	if n == 0 { return 0, nil }
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension mismatch")
	}

	var dot, normA, normB float64
	i := 0

	if n >= 8 && hasAVX512 {
		accD, accA, accB := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 8
		}
		dot, normA, normB = sumF64x8(accD), sumF64x8(accA), sumF64x8(accB)
	} else if n >= 4 && hasAVX2 {
		accD, accA, accB := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-4 {
			_ = a[i+3]
			_ = b[i+3]
			va := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			accD = va.MulAdd(vb, accD)
			accA = va.MulAdd(va, accA)
			accB = vb.MulAdd(vb, accB)
			i += 4
		}
		dot, normA, normB = sumF64x4(accD), sumF64x4(accA), sumF64x4(accB)
	}

	for ; i < n; i++ {
		dot += a[i] * b[i]
		normA += a[i] * a[i]
		normB += b[i] * b[i]
	}

	denominator := math.Sqrt(normA) * math.Sqrt(normB)
	if denominator == 0 {
		return 0, moerr.NewInternalErrorNoCtx("cosine similarity: one of the vector is zero")
	}
	similarity := dot / denominator
	return similarity, nil
}

func CosineSimilarity[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := CosineSimilarityF32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := CosineSimilarityF64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
}

func SphericalDistanceFloat32(a, b []float32) (float32, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var total float32
	i := 0

	if n >= 16 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}, archsimd.Float32x16{}
		for i <= n-64 {
			_ = a[i+63]
			_ = b[i+63]
			v0a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+16])))
			v1b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+16])))
			v2a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+32])))
			v2b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+32])))
			v3a := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i+48])))
			v3b := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i+48])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 64
		}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			va := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x16((*[16]float32)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 16
		}
		total += sumF32x16(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 8 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}, archsimd.Float32x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i+24])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat32x8((*[8]float32)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 8
		}
		total += sumF32x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	if total > 1.0 { total = 1.0 } else if total < -1.0 { total = -1.0 }
	theta := math.Acos(float64(total))
	return float32(theta / math.Pi), nil
}

func SphericalDistanceFloat64(a, b []float64) (float64, error) {
	n := len(a)
	if n != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}

	var total float64
	i := 0

	if n >= 8 && hasAVX512 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}, archsimd.Float64x8{}
		for i <= n-32 {
			_ = a[i+31]
			_ = b[i+31]
			v0a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+8])))
			v1b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+8])))
			v2a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+16])))
			v2b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+16])))
			v3a := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i+24])))
			v3b := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i+24])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 32
		}
		for i <= n-8 {
			_ = a[i+7]
			_ = b[i+7]
			va := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x8((*[8]float64)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 8
		}
		total += sumF64x8(acc0.Add(acc1).Add(acc2.Add(acc3)))
	} else if n >= 4 && hasAVX2 {
		acc0, acc1, acc2, acc3 := archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}, archsimd.Float64x4{}
		for i <= n-16 {
			_ = a[i+15]
			_ = b[i+15]
			v0a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			v0b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			v1a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+4])))
			v1b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+4])))
			v2a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+8])))
			v2b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+8])))
			v3a := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i+12])))
			v3b := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i+12])))

			acc0 = v0a.MulAdd(v0b, acc0)
			acc1 = v1a.MulAdd(v1b, acc1)
			acc2 = v2a.MulAdd(v2b, acc2)
			acc3 = v3a.MulAdd(v3b, acc3)
			i += 16
		}
		for i <= n-4 {
			_ = a[i+3]
			_ = b[i+3]
			va := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&a[i])))
			vb := archsimd.LoadFloat64x4((*[4]float64)(unsafe.Pointer(&b[i])))
			acc0 = va.MulAdd(vb, acc0)
			i += 4
		}
		total += sumF64x4(acc0.Add(acc1).Add(acc2.Add(acc3)))
	}

	for ; i < n; i++ { total += a[i] * b[i] }
	if total > 1.0 { total = 1.0 } else if total < -1.0 { total = -1.0 }
	theta := math.Acos(total)
	return theta / math.Pi, nil
}

func SphericalDistance[T types.RealNumbers](p, q []T) (T, error) {
	if pf32, ok := any(p).([]float32); ok {
		res, err := SphericalDistanceFloat32(pf32, any(q).([]float32))
		return T(res), err
	}
	if pf64, ok := any(p).([]float64); ok {
		res, err := SphericalDistanceFloat64(pf64, any(q).([]float64))
		return T(res), err
	}
	return 0, moerr.NewInternalErrorNoCtx("vector type not supported")
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
