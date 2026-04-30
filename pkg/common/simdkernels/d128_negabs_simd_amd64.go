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

// d128_negabs_simd_amd64.go: SIMD batch negate / abs for Decimal128.
//
// Per-element semantics: rLo:rHi = (~lo:~hi) + m, where m is 1 (Negate) or
// the sign bit of hi (Abs). Implemented via the conditional-negate idiom:
//
//	mask  = -m              (all-ones if m == 1, else zero)
//	loBar = lo XOR mask      // ~lo when negating, lo otherwise
//	hiBar = hi XOR mask
//	rLo   = loBar - mask     // adds 1 (or 0) without an explicit branch
//	carry = rLo wraps        // i.e., loBar < rLo unsigned ⇔ mask=-1 AND lo=0
//	rHi   = hiBar - carry    // adds 1 in carrying lanes; XOR already did ~
//
// Crucially rHi must NOT subtract `mask` again: the XOR has already produced
// ~hi when negating, and the +1 for two's complement only propagates from lo
// via the carry. Subtracting mask twice would over-add 1 on every negated
// lane.
//
// 4 elements per AVX2 iteration (8 q-words = 64 B), 8 per AVX-512 iteration
// (16 q-words = 128 B). Layout deinterleave/reinterleave reuses the same
// VPUNPCK pattern as d128_addsub.

func init() {
	switch {
	case cpu.X86.HasAVX512:
		D128Negate = avx512D128Negate
		D128Abs = avx512D128Abs
	case cpu.X86.HasAVX2:
		D128Negate = avx2D128Negate
		D128Abs = avx2D128Abs
	}
}

// ---------------------------------------------------------------------------
// AVX2 (Int64x4) implementation
// ---------------------------------------------------------------------------

//go:nosplit
func avx2D128NegBody(lo, hi, mask, sb archsimd.Int64x4) (rLo, rHi archsimd.Int64x4) {
	loBar := lo.Xor(mask)
	hiBar := hi.Xor(mask)
	rLo = loBar.Sub(mask)
	carry := rLo.Xor(sb).Less(loBar.Xor(sb)).ToInt64x4()
	rHi = hiBar.Sub(carry)
	return
}

func avx2D128Negate(src, dst []uint64) {
	n := len(dst) / 2
	if n == 0 || len(src) < 2*n {
		return
	}
	ps, pd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	mask := archsimd.BroadcastInt64x4(-1) // unconditional negate

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		s0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off)))
		s1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+32)))
		lo := s0.InterleaveLoGrouped(s1)
		hi := s0.InterleaveHiGrouped(s1)
		rLo, rHi := avx2D128NegBody(lo, hi, mask, sb)
		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pd, off)))
		r1.Store((*[4]int64)(unsafe.Add(pd, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		lo, c := bits.Add64(^src[j], 1, 0)
		hi, _ := bits.Add64(^src[j+1], 0, c)
		dst[j] = lo
		dst[j+1] = hi
	}
}

func avx2D128Abs(src, dst []uint64) {
	n := len(dst) / 2
	if n == 0 || len(src) < 2*n {
		return
	}
	ps, pd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 16
		s0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off)))
		s1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+32)))
		lo := s0.InterleaveLoGrouped(s1)
		hi := s0.InterleaveHiGrouped(s1)
		// mask = -1 in lanes where hi < 0 (signed), 0 elsewhere.
		mask := hi.Less(zero).ToInt64x4()
		rLo, rHi := avx2D128NegBody(lo, hi, mask, sb)
		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pd, off)))
		r1.Store((*[4]int64)(unsafe.Add(pd, off+32)))
	}
	for ; i < n; i++ {
		j := i << 1
		lo, hi := src[j], src[j+1]
		sign := uint64(int64(hi) >> 63)
		lo ^= sign
		hi ^= sign
		var c uint64
		lo, c = bits.Add64(lo, sign&1, 0)
		hi, _ = bits.Add64(hi, 0, c)
		dst[j] = lo
		dst[j+1] = hi
	}
}

// ---------------------------------------------------------------------------
// AVX-512 (Int64x8) implementation
// ---------------------------------------------------------------------------

//go:nosplit
func avx512D128NegBody(lo, hi, mask archsimd.Int64x8) (rLo, rHi archsimd.Int64x8) {
	loBar := lo.Xor(mask)
	hiBar := hi.Xor(mask)
	rLo = loBar.Sub(mask)
	// Native unsigned compare on AVX-512.
	carry := rLo.AsUint64x8().Less(loBar.AsUint64x8()).ToInt64x8()
	rHi = hiBar.Sub(carry)
	return
}

func avx512D128Negate(src, dst []uint64) {
	n := len(dst) / 2
	if n == 0 || len(src) < 2*n {
		return
	}
	ps, pd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	mask := archsimd.BroadcastInt64x8(-1)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		s0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(ps, off)))
		s1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(ps, off+64)))
		lo := s0.InterleaveLoGrouped(s1)
		hi := s0.InterleaveHiGrouped(s1)
		rLo, rHi := avx512D128NegBody(lo, hi, mask)
		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pd, off)))
		r1.Store((*[8]int64)(unsafe.Add(pd, off+64)))
	}
	// AVX2 path handles 4-elem chunks; reuse for 4..7 remainder.
	if r := n - i; r >= 4 {
		sb := archsimd.BroadcastInt64x4(signBit128)
		mask4 := archsimd.BroadcastInt64x4(-1)
		off := uintptr(i) * 16
		s0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off)))
		s1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+32)))
		lo := s0.InterleaveLoGrouped(s1)
		hi := s0.InterleaveHiGrouped(s1)
		rLo, rHi := avx2D128NegBody(lo, hi, mask4, sb)
		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pd, off)))
		r1.Store((*[4]int64)(unsafe.Add(pd, off+32)))
		i += 4
	}
	for ; i < n; i++ {
		j := i << 1
		lo, c := bits.Add64(^src[j], 1, 0)
		hi, _ := bits.Add64(^src[j+1], 0, c)
		dst[j] = lo
		dst[j+1] = hi
	}
}

func avx512D128Abs(src, dst []uint64) {
	n := len(dst) / 2
	if n == 0 || len(src) < 2*n {
		return
	}
	ps, pd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	zero := archsimd.BroadcastInt64x8(0)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 16
		s0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(ps, off)))
		s1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(ps, off+64)))
		lo := s0.InterleaveLoGrouped(s1)
		hi := s0.InterleaveHiGrouped(s1)
		mask := hi.Less(zero).ToInt64x8()
		rLo, rHi := avx512D128NegBody(lo, hi, mask)
		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[8]int64)(unsafe.Add(pd, off)))
		r1.Store((*[8]int64)(unsafe.Add(pd, off+64)))
	}
	if r := n - i; r >= 4 {
		sb := archsimd.BroadcastInt64x4(signBit128)
		zero4 := archsimd.BroadcastInt64x4(0)
		off := uintptr(i) * 16
		s0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off)))
		s1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+32)))
		lo := s0.InterleaveLoGrouped(s1)
		hi := s0.InterleaveHiGrouped(s1)
		mask := hi.Less(zero4).ToInt64x4()
		rLo, rHi := avx2D128NegBody(lo, hi, mask, sb)
		r0 := rLo.InterleaveLoGrouped(rHi)
		r1 := rLo.InterleaveHiGrouped(rHi)
		r0.Store((*[4]int64)(unsafe.Add(pd, off)))
		r1.Store((*[4]int64)(unsafe.Add(pd, off+32)))
		i += 4
	}
	for ; i < n; i++ {
		j := i << 1
		lo, hi := src[j], src[j+1]
		sign := uint64(int64(hi) >> 63)
		lo ^= sign
		hi ^= sign
		var c uint64
		lo, c = bits.Add64(lo, sign&1, 0)
		hi, _ = bits.Add64(hi, 0, c)
		dst[j] = lo
		dst[j+1] = hi
	}
}
