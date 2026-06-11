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

// AVX-512 SIMD distance kernels for vecbf16 (types.BF16).
//
// bf16 is the high 16 bits of an IEEE float32, so the decode bf16->f32 is a pure
// bit op: value<<16. Go's archsimd has no bf16 type and no AVX512BF16 detector
// (and the native VDPBF16PS is therefore unreachable from Go), but we don't need
// it: load the raw bf16 bytes as Uint32x16 (32 bf16 per load), split the even and
// odd 16-bit halves into two Float32x16 vectors via one shift + one and-mask +
// AsFloat32x16 bitcast, then reuse the existing AVX-512 float32 reduction
// (sumF32x16 / hasAVX512 from distance_func_amd64.go — same package + build tag).
//
// The pure-Go kernels in distance_func_narrow.go stay the fallback (non-AVX512
// CPUs) and the equivalence oracle; init() only swaps the selection vars when
// hasAVX512 is true.

package metric

import (
	"math"
	"unsafe"

	"simd/archsimd"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

func init() {
	if hasAVX512 {
		bf16L2sqFn = l2sqBF16SIMD
		bf16IPFn = innerProductBF16SIMD
		bf16CosineFn = cosineDistanceBF16SIMD
		bf16L1Fn = l1DistanceBF16SIMD
	}
}

// bf16AsU32 reinterprets a []types.BF16 (uint16-backed) as []uint32 viewing its
// first len/2 even-aligned pairs. x86 tolerates the unaligned load; the stored
// bf16 bytes originate from an 8-aligned []byte (BytesToArray), so in practice
// the start is 4-aligned.
func bf16AsU32(s []types.BF16) []uint32 {
	if len(s) < 2 {
		return nil
	}
	return unsafe.Slice((*uint32)(unsafe.Pointer(unsafe.SliceData(s))), len(s)/2)
}

func l2sqBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x16(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		dE := ua.ShiftAllLeft(16).AsFloat32x16().Sub(ub.ShiftAllLeft(16).AsFloat32x16())
		dO := ua.And(hi).AsFloat32x16().Sub(ub.And(hi).AsFloat32x16())
		acc0 = dE.MulAdd(dE, acc0)
		acc1 = dO.MulAdd(dO, acc1)
	}
	sum := sumF32x16(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		sum += d * d
	}
	return float64(sum), nil
}

func innerProductBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x16(0xFFFF0000)
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		acc0 = ua.ShiftAllLeft(16).AsFloat32x16().MulAdd(ub.ShiftAllLeft(16).AsFloat32x16(), acc0)
		acc1 = ua.And(hi).AsFloat32x16().MulAdd(ub.And(hi).AsFloat32x16(), acc1)
	}
	sum := sumF32x16(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		sum += a[i].ToFloat32() * b[i].ToFloat32()
	}
	return float64(-sum), nil
}

func l1DistanceBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x16(0xFFFF0000)
	absMask := archsimd.BroadcastUint32x16(0x7FFFFFFF)
	acc0, acc1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		dE := ua.ShiftAllLeft(16).AsFloat32x16().Sub(ub.ShiftAllLeft(16).AsFloat32x16())
		dO := ua.And(hi).AsFloat32x16().Sub(ub.And(hi).AsFloat32x16())
		acc0 = acc0.Add(dE.AsUint32x16().And(absMask).AsFloat32x16())
		acc1 = acc1.Add(dO.AsUint32x16().And(absMask).AsFloat32x16())
	}
	sum := sumF32x16(acc0.Add(acc1))
	for i := j * 2; i < n; i++ {
		d := a[i].ToFloat32() - b[i].ToFloat32()
		if d < 0 {
			d = -d
		}
		sum += d
	}
	return float64(sum), nil
}

func cosineDistanceBF16SIMD(a, b []types.BF16) (float64, error) {
	if len(a) == 0 {
		return 0, nil
	}
	if len(a) != len(b) {
		return 0, moerr.NewInternalErrorNoCtx("vector dimension not matched")
	}
	n := len(a)
	au, bu := bf16AsU32(a), bf16AsU32(b)
	hi := archsimd.BroadcastUint32x16(0xFFFF0000)
	dot0, dot1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	na0, na1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	nb0, nb1 := archsimd.Float32x16{}, archsimd.Float32x16{}
	np, j := len(au), 0
	for ; j <= np-16; j += 16 {
		ua := archsimd.LoadUint32x16Slice(au[j : j+16])
		ub := archsimd.LoadUint32x16Slice(bu[j : j+16])
		aE := ua.ShiftAllLeft(16).AsFloat32x16()
		aO := ua.And(hi).AsFloat32x16()
		bE := ub.ShiftAllLeft(16).AsFloat32x16()
		bO := ub.And(hi).AsFloat32x16()
		dot0 = aE.MulAdd(bE, dot0)
		dot1 = aO.MulAdd(bO, dot1)
		na0 = aE.MulAdd(aE, na0)
		na1 = aO.MulAdd(aO, na1)
		nb0 = bE.MulAdd(bE, nb0)
		nb1 = bO.MulAdd(bO, nb1)
	}
	dot := sumF32x16(dot0.Add(dot1))
	na2 := sumF32x16(na0.Add(na1))
	nb2 := sumF32x16(nb0.Add(nb1))
	for i := j * 2; i < n; i++ {
		ai, bi := a[i].ToFloat32(), b[i].ToFloat32()
		dot += ai * bi
		na2 += ai * ai
		nb2 += bi * bi
	}
	denom := math.Sqrt(float64(na2)) * math.Sqrt(float64(nb2))
	if denom == 0 {
		return 1.0, nil
	}
	return 1.0 - float64(dot)/denom, nil
}
