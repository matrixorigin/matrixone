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
	"simd/archsimd"
	"unsafe"

	"golang.org/x/sys/cpu"
)

// D64Scale: per-lane signed 64-bit value × constant uint64 factor f.
//
// Algorithm (per lane, matches scalarD64ScaleUnchecked):
//
//	mask = v >>arith 63               // 0 or -1
//	abs  = (v XOR mask) - mask        // |v| as Uint64
//	prod = abs * f                    // truncating low-64 product
//	out  = (prod XOR mask) - mask     // restore sign
//
// The unchecked path assumes the caller has prescanned that abs ≤ MaxInt64/f
// for every lane, so the truncating mul cannot overflow.
//
// Only AVX-512 is enabled. AVX-512DQ provides VPMULLQ which gives one fused
// 64×64→low-64 multiply per Int64x8 lane. AVX2 lacks VPMULLQ; emulating it
// via 32×32 partial products (3 VPMULUDQ + shifts + adds per 4 lanes) is
// roughly 5× slower than scalar IMUL64 on Zen 3, so we fall back to scalar
// on AVX2-only hosts.

func init() {
	if cpu.X86.HasAVX512 {
		D64ScaleUnchecked = avx512D64ScaleUnchecked
	}
}

// ---------------------------------------------------------------------------
// AVX-512 path — Int64x8.Mul (VPMULLQ from AVX-512DQ).
// One real multiply per 8 lanes. Main loop unrolls 32 lanes per iter.
// ---------------------------------------------------------------------------

func avx512D64ScaleUnchecked(vec, rs []uint64, f uint64) {
	n := len(rs)
	if n == 0 || len(vec) < n {
		return
	}
	pv, pr := unsafe.Pointer(&vec[0]), unsafe.Pointer(&rs[0])

	fv := archsimd.BroadcastInt64x8(int64(f))
	zero := archsimd.BroadcastInt64x8(0)

	i := 0
	for ; i+32 <= n; i += 32 {
		off := uintptr(i) * 8
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))
		m0 := zero.Greater(v0).ToInt64x8()
		m1 := zero.Greater(v1).ToInt64x8()
		m2 := zero.Greater(v2).ToInt64x8()
		m3 := zero.Greater(v3).ToInt64x8()
		v0.Xor(m0).Sub(m0).Mul(fv).Xor(m0).Sub(m0).Store((*[8]int64)(unsafe.Add(pr, off)))
		v1.Xor(m1).Sub(m1).Mul(fv).Xor(m1).Sub(m1).Store((*[8]int64)(unsafe.Add(pr, off+64)))
		v2.Xor(m2).Sub(m2).Mul(fv).Xor(m2).Sub(m2).Store((*[8]int64)(unsafe.Add(pr, off+128)))
		v3.Xor(m3).Sub(m3).Mul(fv).Xor(m3).Sub(m3).Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		v := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		m := zero.Greater(v).ToInt64x8()
		v.Xor(m).Sub(m).Mul(fv).Xor(m).Sub(m).Store((*[8]int64)(unsafe.Add(pr, off)))
	}
	for ; i < n; i++ {
		signBit := vec[i] >> 63
		mask := -signBit
		abs := (vec[i] ^ mask) + signBit
		rs[i] = (abs*f ^ mask) + signBit
	}
}
