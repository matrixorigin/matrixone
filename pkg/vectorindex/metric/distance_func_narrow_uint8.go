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
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Pure-Go INTEGER (int64-accumulated) distance kernels for vecuint8 ([]uint8),
// mirroring the vecint8 kernels. uint8 values are promoted to int32/int64 before
// arithmetic so the math is identical to int8 (per-element |d|<=255 -> d*d<=65025,
// a*b in [0,65025]); int64 accumulation never overflows at MaxArrayDimension.
//
// The kernel function pointers are swappable so a future
// distance_func_narrow_uint8_amd64.go can drop in SIMD impls via init(), exactly
// as int8 does — there is no SIMD variant yet, so they point at the Go kernels.
var (
	uint8L2sqFn   = l2sqUint8
	uint8IPFn     = innerProductUint8
	uint8CosineFn = cosineDistanceUint8
	uint8L1Fn     = l1DistanceUint8
)

func resolveUint8Kernel(metric MetricType) (func(a, b []uint8) (float64, error), error) {
	switch metric {
	case Metric_L2Distance, Metric_L2sqDistance:
		return uint8L2sqFn, nil
	case Metric_InnerProduct:
		return uint8IPFn, nil
	case Metric_CosineDistance:
		return uint8CosineFn, nil
	case Metric_L1Distance:
		return uint8L1Fn, nil
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid distance type")
	}
}

func l2sqUint8(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum int64
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		d0 := int32(aa[0]) - int32(bb[0])
		d1 := int32(aa[1]) - int32(bb[1])
		d2 := int32(aa[2]) - int32(bb[2])
		d3 := int32(aa[3]) - int32(bb[3])
		d4 := int32(aa[4]) - int32(bb[4])
		d5 := int32(aa[5]) - int32(bb[5])
		d6 := int32(aa[6]) - int32(bb[6])
		d7 := int32(aa[7]) - int32(bb[7])
		sum += int64(d0*d0+d1*d1) + int64(d2*d2+d3*d3) + int64(d4*d4+d5*d5) + int64(d6*d6+d7*d7)
	}
	for ; i < n; i++ {
		d := int32(a[i]) - int32(b[i])
		sum += int64(d * d)
	}
	return float64(sum), nil
}

func innerProductUint8(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum int64
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		sum += int64(int32(aa[0])*int32(bb[0])+int32(aa[1])*int32(bb[1])) +
			int64(int32(aa[2])*int32(bb[2])+int32(aa[3])*int32(bb[3])) +
			int64(int32(aa[4])*int32(bb[4])+int32(aa[5])*int32(bb[5])) +
			int64(int32(aa[6])*int32(bb[6])+int32(aa[7])*int32(bb[7]))
	}
	for ; i < n; i++ {
		sum += int64(int32(a[i]) * int32(b[i]))
	}
	// matches metric.InnerProduct: returns -dot
	return float64(-sum), nil
}

func l1DistanceUint8(a, b []uint8) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var sum int64
	n := len(a)
	i := 0
	abs := func(x int32) int32 {
		if x < 0 {
			return -x
		}
		return x
	}
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		sum += int64(abs(int32(aa[0])-int32(bb[0]))+abs(int32(aa[1])-int32(bb[1]))) +
			int64(abs(int32(aa[2])-int32(bb[2]))+abs(int32(aa[3])-int32(bb[3]))) +
			int64(abs(int32(aa[4])-int32(bb[4]))+abs(int32(aa[5])-int32(bb[5]))) +
			int64(abs(int32(aa[6])-int32(bb[6]))+abs(int32(aa[7])-int32(bb[7])))
	}
	for ; i < n; i++ {
		sum += int64(abs(int32(a[i]) - int32(b[i])))
	}
	return float64(sum), nil
}

func cosineDistanceUint8(a, b []uint8) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	var dot, na2, nb2 int64
	n := len(a)
	i := 0
	for ; i <= n-8; i += 8 {
		aa := a[i : i+8 : i+8]
		bb := b[i : i+8 : i+8]
		for k := 0; k < 8; k++ {
			ai := int64(aa[k])
			bi := int64(bb[k])
			dot += ai * bi
			na2 += ai * ai
			nb2 += bi * bi
		}
	}
	for ; i < n; i++ {
		ai := int64(a[i])
		bi := int64(b[i])
		dot += ai * bi
		na2 += ai * ai
		nb2 += bi * bi
	}
	// matches metric.CosineDistance: denominator 0 -> distance 1.0
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return cosineDistClamped(float64(dot), denom), nil
}
