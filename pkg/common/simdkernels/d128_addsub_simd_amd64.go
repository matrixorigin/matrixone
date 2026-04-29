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

// signBit128 is used to convert unsigned int64 compares to the available
// signed Less by flipping every lane's MSB. Stored as the int64 value
// -1<<63, which has only the sign bit set (writing 1<<63 directly would
// overflow an untyped int constant).
const signBit128 int64 = -1 << 63

func init() {
	switch {
	case cpu.X86.HasAVX512:
		D128AddUnchecked = avx512D128AddUnchecked
		D128SubUnchecked = avx512D128SubUnchecked
		D128AddChecked = avx512D128AddChecked
		D128SubChecked = avx512D128SubChecked
		D128AddScalarUnchecked = avx512D128AddScalarUnchecked
		D128SubScalarUnchecked = avx512D128SubScalarUnchecked
		D128ScalarSubUnchecked = avx512D128ScalarSubUnchecked
		D128AddScalarChecked = avx512D128AddScalarChecked
		D128SubScalarChecked = avx512D128SubScalarChecked
		D128ScalarSubChecked = avx512D128ScalarSubChecked
		D128SumReduce = avx512D128SumReduce
	case cpu.X86.HasAVX2:
		D128AddUnchecked = avx2D128AddUnchecked
		D128SubUnchecked = avx2D128SubUnchecked
		D128AddChecked = avx2D128AddChecked
		D128SubChecked = avx2D128SubChecked
		D128AddScalarUnchecked = avx2D128AddScalarUnchecked
		D128SubScalarUnchecked = avx2D128SubScalarUnchecked
		D128ScalarSubUnchecked = avx2D128ScalarSubUnchecked
		D128AddScalarChecked = avx2D128AddScalarChecked
		D128SubScalarChecked = avx2D128SubScalarChecked
		D128ScalarSubChecked = avx2D128ScalarSubChecked
		D128SumReduce = avx2D128SumReduce
	}
}

// ---------------------------------------------------------------------------
// AVX2 path: each Decimal128 = 2 uint64 = 16 B. We process 4 elements per
// kernel iteration (= 64 B per input). Layout in two Int64x4 loads:
//
//	vec0 = [a0.lo, a0.hi, a1.lo, a1.hi]
//	vec1 = [a2.lo, a2.hi, a3.lo, a3.hi]
//
// InterleaveLoGrouped/HiGrouped (VPUNPCKLQDQ/VPUNPCKHQDQ) split into:
//
//	los = [a0.lo, a2.lo, a1.lo, a3.lo]   (= permutation [0,2,1,3] of a*.lo)
//	his = [a0.hi, a2.hi, a1.hi, a3.hi]   (same permutation of a*.hi)
//
// Both operands and the result use the same permutation, so reinterleaving
// (los, his) with the same two instructions restores the original order.
// Carry: SIMD has no unsigned int64 compare; flip the MSB on both inputs
// and use signed Less. Carry mask is -1 in overflowing lanes, so
// `hi - carryMask` is `hi + 1` exactly where carry is set.
// ---------------------------------------------------------------------------

// avx2D128AddCarry computes [los, his] = [aLo, aHi] + [bLo, bHi] (128-bit
// across lanes). aLo/bLo/aHi/bHi are already in the deinterleaved permuted
// form. Used by both Unchecked and Checked variants.
//
//go:nosplit
func avx2D128AddCarry(aLo, aHi, bLo, bHi, sb archsimd.Int64x4) (rLo, rHi archsimd.Int64x4) {
	rLo = aLo.Add(bLo)
	carryMask := rLo.Xor(sb).Less(aLo.Xor(sb)).ToInt64x4()
	rHi = aHi.Add(bHi).Sub(carryMask)
	return
}

//go:nosplit
func avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb archsimd.Int64x4) (rLo, rHi archsimd.Int64x4) {
	rLo = aLo.Sub(bLo)
	// Borrow iff aLo < bLo (unsigned). borrowMask is -1 per borrowing lane,
	// and `hi + borrowMask` equals `hi - 1` there.
	borrowMask := aLo.Xor(sb).Less(bLo.Xor(sb)).ToInt64x4()
	rHi = aHi.Sub(bHi).Add(borrowMask)
	return
}

func avx2D128AddUnchecked(a, b, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		b0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx2D128AddCarry(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		lo := a[j] + b[j]
		var c uint64
		if lo < a[j] {
			c = 1
		}
		r[j] = lo
		r[j+1] = a[j+1] + b[j+1] + c
	}
}

func avx2D128SubUnchecked(a, b, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		b0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		var br uint64
		if a[j] < b[j] {
			br = 1
		}
		r[j] = a[j] - b[j]
		r[j+1] = a[j+1] - b[j+1] - br
	}
}

func avx2D128AddChecked(a, b, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		b0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx2D128AddCarry(aLo, aHi, bLo, bHi, sb)
		// 128-bit add overflow predicate is the same as 64-bit, evaluated on
		// the high words after carry propagation.
		ofAcc = ofAcc.Or(aHi.Xor(rHi).AndNot(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 1
		aLo, aHi := a[j], a[j+1]
		bLo, bHi := b[j], b[j+1]
		lo := aLo + bLo
		var c uint64
		if lo < aLo {
			c = 1
		}
		hi := aHi + bHi + c
		r[j] = lo
		r[j+1] = hi
		ah, bh, rh := int64(aHi), int64(bHi), int64(hi)
		if (ah^rh)&^(ah^bh) < 0 {
			if vecOverflow {
				return d128FirstOverflow(a, b, vecEnd, false)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128FirstOverflow(a, b, vecEnd, false)
}

func avx2D128SubChecked(a, b, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		b0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).And(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 1
		aLo, aHi := a[j], a[j+1]
		bLo, bHi := b[j], b[j+1]
		var br uint64
		if aLo < bLo {
			br = 1
		}
		lo := aLo - bLo
		hi := aHi - bHi - br
		r[j] = lo
		r[j+1] = hi
		ah, bh, rh := int64(aHi), int64(bHi), int64(hi)
		if (ah^rh)&(ah^bh) < 0 {
			if vecOverflow {
				return d128FirstOverflow(a, b, vecEnd, true)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128FirstOverflow(a, b, vecEnd, true)
}

// ---------------------------------------------------------------------------
// AVX-512 path: Int64x8 = 8 lanes = 4 D128 elements per vector. Processes 8
// elements per kernel iteration (two Int64x8 = 128 B per input). Same
// deinterleave / carry-propagate / reinterleave pattern as AVX2; the only
// changes are the lane width and a wider scalar tail.
// ---------------------------------------------------------------------------

//go:nosplit
func avx512D128AddCarry(aLo, aHi, bLo, bHi, sb archsimd.Int64x8) (rLo, rHi archsimd.Int64x8) {
	rLo = aLo.Add(bLo)
	carryMask := rLo.Xor(sb).Less(aLo.Xor(sb)).ToInt64x8()
	rHi = aHi.Add(bHi).Sub(carryMask)
	return
}

//go:nosplit
func avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb archsimd.Int64x8) (rLo, rHi archsimd.Int64x8) {
	rLo = aLo.Sub(bLo)
	borrowMask := aLo.Xor(sb).Less(bLo.Xor(sb)).ToInt64x8()
	rHi = aHi.Sub(bHi).Add(borrowMask)
	return
}

func avx512D128AddUnchecked(a, b, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		b0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx512D128AddCarry(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	for ; i < n; i++ {
		j := i << 1
		lo := a[j] + b[j]
		var c uint64
		if lo < a[j] {
			c = 1
		}
		r[j] = lo
		r[j+1] = a[j+1] + b[j+1] + c
	}
}

func avx512D128SubUnchecked(a, b, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		b0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	for ; i < n; i++ {
		j := i << 1
		var br uint64
		if a[j] < b[j] {
			br = 1
		}
		r[j] = a[j] - b[j]
		r[j+1] = a[j+1] - b[j+1] - br
	}
}

func avx512D128AddChecked(a, b, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		b0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx512D128AddCarry(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).AndNot(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 1
		aLo, aHi := a[j], a[j+1]
		bLo, bHi := b[j], b[j+1]
		lo := aLo + bLo
		var c uint64
		if lo < aLo {
			c = 1
		}
		hi := aHi + bHi + c
		r[j] = lo
		r[j+1] = hi
		ah, bh, rh := int64(aHi), int64(bHi), int64(hi)
		if (ah^rh)&^(ah^bh) < 0 {
			if vecOverflow {
				return d128FirstOverflow(a, b, vecEnd, false)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128FirstOverflow(a, b, vecEnd, false)
}

func avx512D128SubChecked(a, b, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(a) < 2*n || len(b) < 2*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		a0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		a1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		b0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		b1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))

		aLo := a0.InterleaveLoGrouped(a1)
		aHi := a0.InterleaveHiGrouped(a1)
		bLo := b0.InterleaveLoGrouped(b1)
		bHi := b0.InterleaveHiGrouped(b1)

		rLo, rHi := avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).And(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 1
		aLo, aHi := a[j], a[j+1]
		bLo, bHi := b[j], b[j+1]
		var br uint64
		if aLo < bLo {
			br = 1
		}
		lo := aLo - bLo
		hi := aHi - bHi - br
		r[j] = lo
		r[j+1] = hi
		ah, bh, rh := int64(aHi), int64(bHi), int64(hi)
		if (ah^rh)&(ah^bh) < 0 {
			if vecOverflow {
				return d128FirstOverflow(a, b, vecEnd, true)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128FirstOverflow(a, b, vecEnd, true)
}

// ---------------------------------------------------------------------------
// AVX2 broadcast variants. Scalar (slo, shi) is broadcast once outside the
// loop; only the vector operand is loaded each iteration. Layout/permutation
// notes from the vec+vec path apply identically — the broadcast vectors are
// uniform, so deinterleaving them is a no-op (same value in every lane).
// ---------------------------------------------------------------------------

func avx2D128AddScalarUnchecked(slo, shi uint64, v, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	bLo := archsimd.BroadcastInt64x4(int64(slo))
	bHi := archsimd.BroadcastInt64x4(int64(shi))

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx2D128AddCarry(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		lo := slo + v[j]
		var c uint64
		if lo < slo {
			c = 1
		}
		r[j] = lo
		r[j+1] = shi + v[j+1] + c
	}
}

func avx2D128SubScalarUnchecked(v []uint64, slo, shi uint64, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	bLo := archsimd.BroadcastInt64x4(int64(slo))
	bHi := archsimd.BroadcastInt64x4(int64(shi))

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		var br uint64
		if v[j] < slo {
			br = 1
		}
		r[j] = v[j] - slo
		r[j+1] = v[j+1] - shi - br
	}
}

func avx2D128ScalarSubUnchecked(slo, shi uint64, v, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	aLo := archsimd.BroadcastInt64x4(int64(slo))
	aHi := archsimd.BroadcastInt64x4(int64(shi))

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))

		bLo := v0.InterleaveLoGrouped(v1)
		bHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		var br uint64
		if slo < v[j] {
			br = 1
		}
		r[j] = slo - v[j]
		r[j+1] = shi - v[j+1] - br
	}
}

func avx2D128AddScalarChecked(slo, shi uint64, v, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	bLo := archsimd.BroadcastInt64x4(int64(slo))
	bHi := archsimd.BroadcastInt64x4(int64(shi))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx2D128AddCarry(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).AndNot(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(shi)
	for ; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		lo := slo + vLo
		var c uint64
		if lo < slo {
			c = 1
		}
		hi := shi + vHi + c
		r[j] = lo
		r[j+1] = hi
		vh, rh := int64(vHi), int64(hi)
		if (sh^rh)&^(sh^vh) < 0 {
			if vecOverflow {
				return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 0)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 0)
}

func avx2D128SubScalarChecked(v []uint64, slo, shi uint64, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	bLo := archsimd.BroadcastInt64x4(int64(slo))
	bHi := archsimd.BroadcastInt64x4(int64(shi))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).And(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(shi)
	for ; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		var br uint64
		if vLo < slo {
			br = 1
		}
		lo := vLo - slo
		hi := vHi - shi - br
		r[j] = lo
		r[j+1] = hi
		vh, rh := int64(vHi), int64(hi)
		if (vh^rh)&(vh^sh) < 0 {
			if vecOverflow {
				return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 1)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 1)
}

func avx2D128ScalarSubChecked(slo, shi uint64, v, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	aLo := archsimd.BroadcastInt64x4(int64(slo))
	aHi := archsimd.BroadcastInt64x4(int64(shi))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))

		bLo := v0.InterleaveLoGrouped(v1)
		bHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx2D128SubBorrow(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).And(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pr, off)))
		r1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x4(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(shi)
	for ; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		var br uint64
		if slo < vLo {
			br = 1
		}
		lo := slo - vLo
		hi := shi - vHi - br
		r[j] = lo
		r[j+1] = hi
		vh, rh := int64(vHi), int64(hi)
		if (sh^rh)&(sh^vh) < 0 {
			if vecOverflow {
				return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 2)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 2)
}

// ---------------------------------------------------------------------------
// AVX-512 broadcast variants. Same structure as AVX2 with Int64x8 lanes
// (8 D128 elements per kernel iteration).
// ---------------------------------------------------------------------------

func avx512D128AddScalarUnchecked(slo, shi uint64, v, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	bLo := archsimd.BroadcastInt64x8(int64(slo))
	bHi := archsimd.BroadcastInt64x8(int64(shi))

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx512D128AddCarry(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	for ; i < n; i++ {
		j := i << 1
		lo := slo + v[j]
		var c uint64
		if lo < slo {
			c = 1
		}
		r[j] = lo
		r[j+1] = shi + v[j+1] + c
	}
}

func avx512D128SubScalarUnchecked(v []uint64, slo, shi uint64, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	bLo := archsimd.BroadcastInt64x8(int64(slo))
	bHi := archsimd.BroadcastInt64x8(int64(shi))

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	for ; i < n; i++ {
		j := i << 1
		var br uint64
		if v[j] < slo {
			br = 1
		}
		r[j] = v[j] - slo
		r[j+1] = v[j+1] - shi - br
	}
}

func avx512D128ScalarSubUnchecked(slo, shi uint64, v, r []uint64) {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	aLo := archsimd.BroadcastInt64x8(int64(slo))
	aHi := archsimd.BroadcastInt64x8(int64(shi))

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))

		bLo := v0.InterleaveLoGrouped(v1)
		bHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb)

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	for ; i < n; i++ {
		j := i << 1
		var br uint64
		if slo < v[j] {
			br = 1
		}
		r[j] = slo - v[j]
		r[j+1] = shi - v[j+1] - br
	}
}

func avx512D128AddScalarChecked(slo, shi uint64, v, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	bLo := archsimd.BroadcastInt64x8(int64(slo))
	bHi := archsimd.BroadcastInt64x8(int64(shi))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx512D128AddCarry(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).AndNot(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(shi)
	for ; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		lo := slo + vLo
		var c uint64
		if lo < slo {
			c = 1
		}
		hi := shi + vHi + c
		r[j] = lo
		r[j+1] = hi
		vh, rh := int64(vHi), int64(hi)
		if (sh^rh)&^(sh^vh) < 0 {
			if vecOverflow {
				return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 0)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 0)
}

func avx512D128SubScalarChecked(v []uint64, slo, shi uint64, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	bLo := archsimd.BroadcastInt64x8(int64(slo))
	bHi := archsimd.BroadcastInt64x8(int64(shi))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))

		aLo := v0.InterleaveLoGrouped(v1)
		aHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).And(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(shi)
	for ; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		var br uint64
		if vLo < slo {
			br = 1
		}
		lo := vLo - slo
		hi := vHi - shi - br
		r[j] = lo
		r[j+1] = hi
		vh, rh := int64(vHi), int64(hi)
		if (vh^rh)&(vh^sh) < 0 {
			if vecOverflow {
				return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 1)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 1)
}

func avx512D128ScalarSubChecked(slo, shi uint64, v, r []uint64) int {
	n := len(r) / 2
	if n == 0 || len(v) < 2*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	aLo := archsimd.BroadcastInt64x8(int64(slo))
	aHi := archsimd.BroadcastInt64x8(int64(shi))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))

		bLo := v0.InterleaveLoGrouped(v1)
		bHi := v0.InterleaveHiGrouped(v1)

		rLo, rHi := avx512D128SubBorrow(aLo, aHi, bLo, bHi, sb)
		ofAcc = ofAcc.Or(aHi.Xor(rHi).And(aHi.Xor(bHi)))

		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pr, off)))
		r1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
	}
	vecEnd := i

	zero := archsimd.BroadcastInt64x8(0)
	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(shi)
	for ; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		var br uint64
		if slo < vLo {
			br = 1
		}
		lo := slo - vLo
		hi := shi - vHi - br
		r[j] = lo
		r[j+1] = hi
		vh, rh := int64(vHi), int64(hi)
		if (sh^rh)&(sh^vh) < 0 {
			if vecOverflow {
				return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 2)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d128ScalarFirstOverflow(slo, shi, v, vecEnd, 2)
}

// avx2D128SumReduce sums a slice of Decimal128 values laid out as
// [lo, hi, lo, hi, ...] and returns the 128-bit total (lo, hi).
// Wraps on overflow (mod 2^128).
func avx2D128SumReduce(v []uint64) (uint64, uint64) {
	n := len(v) >> 1
	if n == 0 {
		return 0, 0
	}
	pv := unsafe.Pointer(&v[0])
	sb := archsimd.BroadcastInt64x4(signBit128)

	zero := archsimd.BroadcastInt64x4(0)
	accLo := zero
	accHi := zero

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		bLo := v0.InterleaveLoGrouped(v1)
		bHi := v0.InterleaveHiGrouped(v1)

		newLo := accLo.Add(bLo)
		// Carry mask: -1 in lanes where unsigned newLo < accLo (wrap-around).
		carryMask := newLo.Xor(sb).Less(accLo.Xor(sb)).ToInt64x4()
		accHi = accHi.Add(bHi).Sub(carryMask)
		accLo = newLo
	}

	// Horizontal reduce of 4 partial 128-bit sums.
	var loBuf, hiBuf [4]int64
	accLo.Store(&loBuf)
	accHi.Store(&hiBuf)
	var totLo, totHi uint64
	for k := 0; k < 4; k++ {
		var c uint64
		totLo, c = bits.Add64(totLo, uint64(loBuf[k]), 0)
		totHi, _ = bits.Add64(totHi, uint64(hiBuf[k]), c)
	}

	// Tail (n%4 elements).
	for ; i < n; i++ {
		j := i << 1
		var c uint64
		totLo, c = bits.Add64(totLo, v[j], 0)
		totHi, _ = bits.Add64(totHi, v[j+1], c)
	}
	return totLo, totHi
}

// avx512D128SumReduce: same as AVX2 but 8 elements per iteration.
func avx512D128SumReduce(v []uint64) (uint64, uint64) {
	n := len(v) >> 1
	if n == 0 {
		return 0, 0
	}
	pv := unsafe.Pointer(&v[0])
	sb := archsimd.BroadcastInt64x8(signBit128)

	zero := archsimd.BroadcastInt64x8(0)
	accLo := zero
	accHi := zero

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		bLo := v0.InterleaveLoGrouped(v1)
		bHi := v0.InterleaveHiGrouped(v1)

		newLo := accLo.Add(bLo)
		carryMask := newLo.Xor(sb).Less(accLo.Xor(sb)).ToInt64x8()
		accHi = accHi.Add(bHi).Sub(carryMask)
		accLo = newLo
	}

	var loBuf, hiBuf [8]int64
	accLo.Store(&loBuf)
	accHi.Store(&hiBuf)
	var totLo, totHi uint64
	for k := 0; k < 8; k++ {
		var c uint64
		totLo, c = bits.Add64(totLo, uint64(loBuf[k]), 0)
		totHi, _ = bits.Add64(totHi, uint64(hiBuf[k]), c)
	}

	for ; i < n; i++ {
		j := i << 1
		var c uint64
		totLo, c = bits.Add64(totLo, v[j], 0)
		totHi, _ = bits.Add64(totHi, v[j+1], c)
	}
	return totLo, totHi
}
