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

//go:build goexperiment.simd && amd64

package simdkernels

import (
	"math/bits"
	"simd/archsimd"
	"unsafe"

	"golang.org/x/sys/cpu"
)

// init swaps the dispatcher function pointers from the package-default
// scalar implementations to the best available SIMD variant on the host.
//
// Order of preference: AVX-512 (Int64x8, 8 lanes) > AVX2 (Int64x4, 4 lanes).
// archsimd.X86.AVX512() requires the bundled F+CD+BW+DQ+VL set, exposed via
// cpu.X86.HasAVX512.
func init() {
	switch {
	case cpu.X86.HasAVX512:
		D64AddUnchecked = avx512D64AddUnchecked
		D64SubUnchecked = avx512D64SubUnchecked
		D64AddChecked = avx512D64AddChecked
		D64SubChecked = avx512D64SubChecked
		D64AddScalarUnchecked = avx512D64AddScalarUnchecked
		D64AddScalarChecked = avx512D64AddScalarChecked
		D64SubScalarUnchecked = avx512D64SubScalarUnchecked
		D64SubScalarChecked = avx512D64SubScalarChecked
		D64ScalarSubUnchecked = avx512D64ScalarSubUnchecked
		D64ScalarSubChecked = avx512D64ScalarSubChecked
		D64SumReduceToD128 = avx512D64SumReduceToD128
	case cpu.X86.HasAVX2:
		D64AddUnchecked = avx2D64AddUnchecked
		D64SubUnchecked = avx2D64SubUnchecked
		D64AddChecked = avx2D64AddChecked
		D64SubChecked = avx2D64SubChecked
		D64AddScalarUnchecked = avx2D64AddScalarUnchecked
		D64AddScalarChecked = avx2D64AddScalarChecked
		D64SubScalarUnchecked = avx2D64SubScalarUnchecked
		D64SubScalarChecked = avx2D64SubScalarChecked
		D64ScalarSubUnchecked = avx2D64ScalarSubUnchecked
		D64ScalarSubChecked = avx2D64ScalarSubChecked
		D64SumReduceToD128 = avx2D64SumReduceToD128
	}
}

// ---------------------------------------------------------------------------
// AVX2 path: 4-lane Int64x4 vectors. Main loop processes 16 elements
// (4× Int64x4 = 128 B) per iteration to hide the 4-cycle VPADDQ latency on
// Zen 3 / Skylake; cleanup is a 4-wide loop, then a scalar tail.
// ---------------------------------------------------------------------------

func avx2D64AddUnchecked(a, b, r []uint64) {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	i := 0
	for ; i+16 <= n; i += 16 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		a2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+64)))
		a3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+96)))
		b0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))
		b2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+64)))
		b3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+96)))
		a0.Add(b0).Store((*[4]int64)(unsafe.Add(pr, off)))
		a1.Add(b1).Store((*[4]int64)(unsafe.Add(pr, off+32)))
		a2.Add(b2).Store((*[4]int64)(unsafe.Add(pr, off+64)))
		a3.Add(b3).Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		av.Add(bv).Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = a[i] + b[i]
	}
}

func avx2D64SubUnchecked(a, b, r []uint64) {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	i := 0
	for ; i+16 <= n; i += 16 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		a2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+64)))
		a3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+96)))
		b0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))
		b2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+64)))
		b3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+96)))
		a0.Sub(b0).Store((*[4]int64)(unsafe.Add(pr, off)))
		a1.Sub(b1).Store((*[4]int64)(unsafe.Add(pr, off+32)))
		a2.Sub(b2).Store((*[4]int64)(unsafe.Add(pr, off+64)))
		a3.Sub(b3).Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		av.Sub(bv).Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = a[i] - b[i]
	}
}

// avx2D64AddChecked accumulates per-lane signed-overflow predicates into a
// vector OR; if any MSB is set at the end, it falls back to a scalar rescan
// (d64FirstOverflow) to find the first offending index. For the common
// "no overflow" case the cost is one OR + one Xor + one AndNot per 4 elems.
//
// Predicate: overflow iff sign(a)==sign(b) && sign(a)!=sign(r).
//
//	⇔ ((a^r) &^ (a^b)) < 0   (MSB set)
func avx2D64AddChecked(a, b, r []uint64) int {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		rv := av.Add(bv)
		ofAcc = ofAcc.Or(av.Xor(rv).AndNot(av.Xor(bv)))
		rv.Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai + bi
		r[i] = uint64(ri)
		if (ai^ri)&^(ai^bi) < 0 {
			if vecOverflow {
				return d64FirstOverflow(a, b, vecEnd, false)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64FirstOverflow(a, b, vecEnd, false)
}

// avx2D64SubChecked: overflow iff sign(a)!=sign(b) && sign(a)!=sign(r).
//
//	⇔ ((a^r) & (a^b)) < 0
func avx2D64SubChecked(a, b, r []uint64) int {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		rv := av.Sub(bv)
		ofAcc = ofAcc.Or(av.Xor(rv).And(av.Xor(bv)))
		rv.Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai - bi
		r[i] = uint64(ri)
		if (ai^ri)&(ai^bi) < 0 {
			if vecOverflow {
				return d64FirstOverflow(a, b, vecEnd, true)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64FirstOverflow(a, b, vecEnd, true)
}

// ---------------------------------------------------------------------------
// AVX-512 path: 8-lane Int64x8 vectors (ZMM, 64 B). Main loop processes 32
// elements per iteration (4× Int64x8 = 256 B) to keep the 4-cycle VPADDQ
// pipe full. Cleanup is 8-wide, then scalar tail.
// ---------------------------------------------------------------------------

func avx512D64AddUnchecked(a, b, r []uint64) {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	i := 0
	for ; i+32 <= n; i += 32 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		a2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+128)))
		a3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+192)))
		b0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))
		b2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+128)))
		b3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+192)))
		a0.Add(b0).Store((*[8]int64)(unsafe.Add(pr, off)))
		a1.Add(b1).Store((*[8]int64)(unsafe.Add(pr, off+64)))
		a2.Add(b2).Store((*[8]int64)(unsafe.Add(pr, off+128)))
		a3.Add(b3).Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		av.Add(bv).Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = a[i] + b[i]
	}
}

func avx512D64SubUnchecked(a, b, r []uint64) {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	i := 0
	for ; i+32 <= n; i += 32 {
		off := uintptr(i) * 8
		a0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		a2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+128)))
		a3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+192)))
		b0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))
		b2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+128)))
		b3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+192)))
		a0.Sub(b0).Store((*[8]int64)(unsafe.Add(pr, off)))
		a1.Sub(b1).Store((*[8]int64)(unsafe.Add(pr, off+64)))
		a2.Sub(b2).Store((*[8]int64)(unsafe.Add(pr, off+128)))
		a3.Sub(b3).Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		av.Sub(bv).Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = a[i] - b[i]
	}
}

func avx512D64AddChecked(a, b, r []uint64) int {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		rv := av.Add(bv)
		ofAcc = ofAcc.Or(av.Xor(rv).AndNot(av.Xor(bv)))
		rv.Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai + bi
		r[i] = uint64(ri)
		if (ai^ri)&^(ai^bi) < 0 {
			if vecOverflow {
				return d64FirstOverflow(a, b, vecEnd, false)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64FirstOverflow(a, b, vecEnd, false)
}

func avx512D64SubChecked(a, b, r []uint64) int {
	n := len(r)
	if n == 0 || len(a) < n || len(b) < n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		rv := av.Sub(bv)
		ofAcc = ofAcc.Or(av.Xor(rv).And(av.Xor(bv)))
		rv.Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		ai, bi := int64(a[i]), int64(b[i])
		ri := ai - bi
		r[i] = uint64(ri)
		if (ai^ri)&(ai^bi) < 0 {
			if vecOverflow {
				return d64FirstOverflow(a, b, vecEnd, true)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64FirstOverflow(a, b, vecEnd, true)
}

// ---------------------------------------------------------------------------
// Scalar-broadcast SIMD variants. Pattern: broadcast the scalar into a
// vector once outside the main loop, then fuse loads of the column with
// vector add/sub against the broadcast register.
//
// AVX2: 4× Int64x4 unroll = 16 elems/iter, then 4-wide cleanup, scalar tail.
// AVX-512: 4× Int64x8 unroll = 32 elems/iter, then 8-wide cleanup, scalar tail.
// ---------------------------------------------------------------------------

// AVX2 — Add scalar broadcast.

func avx2D64AddScalarUnchecked(s uint64, v, r []uint64) {
	n := len(r)
	if n == 0 || len(v) < n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x4(int64(s))

	i := 0
	for ; i+16 <= n; i += 16 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))
		sv.Add(v0).Store((*[4]int64)(unsafe.Add(pr, off)))
		sv.Add(v1).Store((*[4]int64)(unsafe.Add(pr, off+32)))
		sv.Add(v2).Store((*[4]int64)(unsafe.Add(pr, off+64)))
		sv.Add(v3).Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		sv.Add(vv).Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = s + v[i]
	}
}

func avx2D64SubScalarUnchecked(v []uint64, s uint64, r []uint64) {
	n := len(r)
	if n == 0 || len(v) < n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x4(int64(s))

	i := 0
	for ; i+16 <= n; i += 16 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))
		v0.Sub(sv).Store((*[4]int64)(unsafe.Add(pr, off)))
		v1.Sub(sv).Store((*[4]int64)(unsafe.Add(pr, off+32)))
		v2.Sub(sv).Store((*[4]int64)(unsafe.Add(pr, off+64)))
		v3.Sub(sv).Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		vv.Sub(sv).Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = v[i] - s
	}
}

func avx2D64ScalarSubUnchecked(s uint64, v, r []uint64) {
	n := len(r)
	if n == 0 || len(v) < n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x4(int64(s))

	i := 0
	for ; i+16 <= n; i += 16 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))
		sv.Sub(v0).Store((*[4]int64)(unsafe.Add(pr, off)))
		sv.Sub(v1).Store((*[4]int64)(unsafe.Add(pr, off+32)))
		sv.Sub(v2).Store((*[4]int64)(unsafe.Add(pr, off+64)))
		sv.Sub(v3).Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		sv.Sub(vv).Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = s - v[i]
	}
}

// Checked broadcast variants follow the same accumulate-mask pattern as the
// vector+vector ones. The overflow predicate is identical to the scalar
// reference because broadcasting the scalar gives the same per-lane test.

func avx2D64AddScalarChecked(s uint64, v, r []uint64) int {
	n := len(r)
	if n == 0 || len(v) < n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x4(int64(s))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		rv := sv.Add(vv)
		ofAcc = ofAcc.Or(sv.Xor(rv).AndNot(sv.Xor(vv)))
		rv.Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	si := int64(s)
	for ; i < n; i++ {
		vi := int64(v[i])
		ri := si + vi
		r[i] = uint64(ri)
		if (si^ri)&^(si^vi) < 0 {
			if vecOverflow {
				return d64ScalarFirstOverflow(s, v, vecEnd, 0)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64ScalarFirstOverflow(s, v, vecEnd, 0)
}

func avx2D64SubScalarChecked(v []uint64, s uint64, r []uint64) int {
	n := len(r)
	if n == 0 || len(v) < n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x4(int64(s))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		rv := vv.Sub(sv)
		ofAcc = ofAcc.Or(vv.Xor(rv).And(vv.Xor(sv)))
		rv.Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	si := int64(s)
	for ; i < n; i++ {
		vi := int64(v[i])
		ri := vi - si
		r[i] = uint64(ri)
		if (vi^ri)&(vi^si) < 0 {
			if vecOverflow {
				return d64ScalarFirstOverflow(s, v, vecEnd, 1)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64ScalarFirstOverflow(s, v, vecEnd, 1)
}

func avx2D64ScalarSubChecked(s uint64, v, r []uint64) int {
	n := len(r)
	if n == 0 || len(v) < n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x4(int64(s))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		rv := sv.Sub(vv)
		ofAcc = ofAcc.Or(sv.Xor(rv).And(sv.Xor(vv)))
		rv.Store((*[4]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	si := int64(s)
	for ; i < n; i++ {
		vi := int64(v[i])
		ri := si - vi
		r[i] = uint64(ri)
		if (si^ri)&(si^vi) < 0 {
			if vecOverflow {
				return d64ScalarFirstOverflow(s, v, vecEnd, 2)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64ScalarFirstOverflow(s, v, vecEnd, 2)
}

// AVX-512 broadcast variants — Int64x8 (8 lanes), main loop 4× unrolled.

func avx512D64AddScalarUnchecked(s uint64, v, r []uint64) {
	n := len(r)
	if n == 0 || len(v) < n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x8(int64(s))

	i := 0
	for ; i+32 <= n; i += 32 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))
		sv.Add(v0).Store((*[8]int64)(unsafe.Add(pr, off)))
		sv.Add(v1).Store((*[8]int64)(unsafe.Add(pr, off+64)))
		sv.Add(v2).Store((*[8]int64)(unsafe.Add(pr, off+128)))
		sv.Add(v3).Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		sv.Add(vv).Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = s + v[i]
	}
}

func avx512D64SubScalarUnchecked(v []uint64, s uint64, r []uint64) {
	n := len(r)
	if n == 0 || len(v) < n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x8(int64(s))

	i := 0
	for ; i+32 <= n; i += 32 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))
		v0.Sub(sv).Store((*[8]int64)(unsafe.Add(pr, off)))
		v1.Sub(sv).Store((*[8]int64)(unsafe.Add(pr, off+64)))
		v2.Sub(sv).Store((*[8]int64)(unsafe.Add(pr, off+128)))
		v3.Sub(sv).Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		vv.Sub(sv).Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = v[i] - s
	}
}

func avx512D64ScalarSubUnchecked(s uint64, v, r []uint64) {
	n := len(r)
	if n == 0 || len(v) < n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x8(int64(s))

	i := 0
	for ; i+32 <= n; i += 32 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))
		sv.Sub(v0).Store((*[8]int64)(unsafe.Add(pr, off)))
		sv.Sub(v1).Store((*[8]int64)(unsafe.Add(pr, off+64)))
		sv.Sub(v2).Store((*[8]int64)(unsafe.Add(pr, off+128)))
		sv.Sub(v3).Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		sv.Sub(vv).Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		r[i] = s - v[i]
	}
}

func avx512D64AddScalarChecked(s uint64, v, r []uint64) int {
	n := len(r)
	if n == 0 || len(v) < n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x8(int64(s))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		rv := sv.Add(vv)
		ofAcc = ofAcc.Or(sv.Xor(rv).AndNot(sv.Xor(vv)))
		rv.Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	si := int64(s)
	for ; i < n; i++ {
		vi := int64(v[i])
		ri := si + vi
		r[i] = uint64(ri)
		if (si^ri)&^(si^vi) < 0 {
			if vecOverflow {
				return d64ScalarFirstOverflow(s, v, vecEnd, 0)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64ScalarFirstOverflow(s, v, vecEnd, 0)
}

func avx512D64SubScalarChecked(v []uint64, s uint64, r []uint64) int {
	n := len(r)
	if n == 0 || len(v) < n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x8(int64(s))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		rv := vv.Sub(sv)
		ofAcc = ofAcc.Or(vv.Xor(rv).And(vv.Xor(sv)))
		rv.Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	si := int64(s)
	for ; i < n; i++ {
		vi := int64(v[i])
		ri := vi - si
		r[i] = uint64(ri)
		if (vi^ri)&(vi^si) < 0 {
			if vecOverflow {
				return d64ScalarFirstOverflow(s, v, vecEnd, 1)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64ScalarFirstOverflow(s, v, vecEnd, 1)
}

func avx512D64ScalarSubChecked(s uint64, v, r []uint64) int {
	n := len(r)
	if n == 0 || len(v) < n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sv := archsimd.BroadcastInt64x8(int64(s))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		vv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		rv := sv.Sub(vv)
		ofAcc = ofAcc.Or(sv.Xor(rv).And(sv.Xor(vv)))
		rv.Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	si := int64(s)
	for ; i < n; i++ {
		vi := int64(v[i])
		ri := si - vi
		r[i] = uint64(ri)
		if (si^ri)&(si^vi) < 0 {
			if vecOverflow {
				return d64ScalarFirstOverflow(s, v, vecEnd, 2)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d64ScalarFirstOverflow(s, v, vecEnd, 2)
}

// avx2D64SumReduceToD128 sums signed Decimal64 values into a 128-bit total.
// Uses K=8 inner iterations per lane: per-lane partial sum stays bounded
// because |Decimal64| < 10^18 ≈ 2^59.79, so 8 such values fit in int63.
func avx2D64SumReduceToD128(v []uint64) (lo, hi uint64) {
	n := len(v)
	if n == 0 {
		return
	}
	pv := unsafe.Pointer(&v[0])
	const K = 8
	const blk = 4 * K // 32 elements per outer iteration
	i := 0
	for ; i+blk <= n; i += blk {
		acc := archsimd.BroadcastInt64x4(0)
		for k := 0; k < K; k++ {
			off := uintptr(i+4*k) * 8
			x := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
			acc = acc.Add(x)
		}
		var buf [4]int64
		acc.Store(&buf)
		for j := 0; j < 4; j++ {
			x := buf[j]
			sx := uint64(x >> 63)
			var c uint64
			lo, c = bits.Add64(lo, uint64(x), 0)
			hi, _ = bits.Add64(hi, sx, c)
		}
	}
	for ; i < n; i++ {
		x := int64(v[i])
		sx := uint64(x >> 63)
		var c uint64
		lo, c = bits.Add64(lo, uint64(x), 0)
		hi, _ = bits.Add64(hi, sx, c)
	}
	return
}

// avx512D64SumReduceToD128: same chunked partial-sum strategy with 8-wide
// lanes. K=8 inner iters → 64 elements per outer iter.
func avx512D64SumReduceToD128(v []uint64) (lo, hi uint64) {
	n := len(v)
	if n == 0 {
		return
	}
	pv := unsafe.Pointer(&v[0])
	const K = 8
	const blk = 8 * K
	i := 0
	for ; i+blk <= n; i += blk {
		acc := archsimd.BroadcastInt64x8(0)
		for k := 0; k < K; k++ {
			off := uintptr(i+8*k) * 8
			x := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
			acc = acc.Add(x)
		}
		var buf [8]int64
		acc.Store(&buf)
		for j := 0; j < 8; j++ {
			x := buf[j]
			sx := uint64(x >> 63)
			var c uint64
			lo, c = bits.Add64(lo, uint64(x), 0)
			hi, _ = bits.Add64(hi, sx, c)
		}
	}
	for ; i < n; i++ {
		x := int64(v[i])
		sx := uint64(x >> 63)
		var c uint64
		lo, c = bits.Add64(lo, uint64(x), 0)
		hi, _ = bits.Add64(hi, sx, c)
	}
	return
}
