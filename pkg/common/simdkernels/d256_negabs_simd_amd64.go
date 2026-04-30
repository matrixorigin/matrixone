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

// d256_negabs_simd_amd64.go: SIMD batch negate / abs for Decimal256.
//
// Same conditional-negate idiom as d128_negabs but with 4 stages of carry
// propagation across the 4 words of each Decimal256:
//
//	mask = -m              (-1 if negating this lane, 0 otherwise)
//	wBar = w XOR mask
//	stage 0: r0 = wBar0 - mask        // mask supplies the +1 only when negating
//	stage k (k>0): rk = wBark - cIn   // cIn is 0 or -1 carry from stage k-1
//	carry out of any stage k: rk wraps unsigned ⇔ rk <_unsigned wBar_k
//	top stage: drop cOut.
//
// Layout: each Decimal256 is 4 q-words = 32 B. Process 4 elements per AVX2
// iter (= 16 q-words = 128 B), 8 per AVX-512 iter. Reuse transpose4x4 from
// d256_addsub_simd_amd64.go for AoS↔SoA conversion.

func init() {
	// AVX-512 D256 transpose would need a custom 8×4 ConcatPermute layout
	// and an inverse for the writeback; deferred. AVX2 path runs on AVX-512
	// hosts as well (still 4 elements per iter).
	if cpu.X86.HasAVX2 {
		D256Negate = avx2D256Negate
		D256Abs = avx2D256Abs
	}
}

// ---------------------------------------------------------------------------
// AVX2 (Int64x4) implementation
// ---------------------------------------------------------------------------

//go:nosplit
func avx2D256NegStage(w, mask, cIn, sb archsimd.Int64x4) (r, cOut archsimd.Int64x4) {
	wBar := w.Xor(mask)
	r = wBar.Sub(cIn)
	cOut = r.Xor(sb).Less(wBar.Xor(sb)).ToInt64x4()
	return
}

//go:nosplit
func avx2D256NegStageNoOut(w, mask, cIn archsimd.Int64x4) archsimd.Int64x4 {
	return w.Xor(mask).Sub(cIn)
}

func avx2D256NegabsCore(src, dst []uint64, abs bool) {
	n := len(dst) / 4
	if n == 0 || len(src) < 4*n {
		return
	}
	ps, pd := unsafe.Pointer(&src[0]), unsafe.Pointer(&dst[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	allOnes := archsimd.BroadcastInt64x4(-1)
	zero := archsimd.BroadcastInt64x4(0)

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(ps, off+96)))

		w0, w1, w2, w3 := transpose4x4(v0, v1, v2, v3)
		var mask archsimd.Int64x4
		if abs {
			mask = w3.Less(zero).ToInt64x4()
		} else {
			mask = allOnes
		}

		r0, c0 := avx2D256NegStage(w0, mask, mask, sb)
		r1, c1 := avx2D256NegStage(w1, mask, c0, sb)
		r2, c2 := avx2D256NegStage(w2, mask, c1, sb)
		r3 := avx2D256NegStageNoOut(w3, mask, c2)

		rv0, rv1, rv2, rv3 := transpose4x4(r0, r1, r2, r3)
		rv0.Store((*[4]int64)(unsafe.Add(pd, off)))
		rv1.Store((*[4]int64)(unsafe.Add(pd, off+32)))
		rv2.Store((*[4]int64)(unsafe.Add(pd, off+64)))
		rv3.Store((*[4]int64)(unsafe.Add(pd, off+96)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, w1, w2, w3 := src[j], src[j+1], src[j+2], src[j+3]
		var sign uint64
		if abs {
			sign = uint64(int64(w3) >> 63)
		} else {
			sign = ^uint64(0)
		}
		w0 ^= sign
		w1 ^= sign
		w2 ^= sign
		w3 ^= sign
		var c uint64
		w0, c = bits.Add64(w0, sign&1, 0)
		w1, c = bits.Add64(w1, 0, c)
		w2, c = bits.Add64(w2, 0, c)
		w3, _ = bits.Add64(w3, 0, c)
		dst[j] = w0
		dst[j+1] = w1
		dst[j+2] = w2
		dst[j+3] = w3
	}
}

func avx2D256Negate(src, dst []uint64) { avx2D256NegabsCore(src, dst, false) }
func avx2D256Abs(src, dst []uint64)    { avx2D256NegabsCore(src, dst, true) }
