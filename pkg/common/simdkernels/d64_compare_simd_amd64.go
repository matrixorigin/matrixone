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

// AVX2 was tried (4-elem and 8-elem interleaved batches) but consistently
// loses ~13-24% to scalar on Zen 3 because:
//   - scalar `cmp/sete/store` runs at ~1.2 cyc/elem (near retirement-width
//     limit on a 4-wide OoO core);
//   - AVX2 has no native qword->byte gather (VPMOVQB is AVX-512 only),
//     so the post-compare bool packing on AVX2 (vector store -> 4 byte
//     loads -> shift/OR -> store) adds a serial dependency chain that
//     the OoO scheduler cannot parallelize away.
// Therefore AVX2 is intentionally NOT registered. Scalar stays as default
// on AVX2-only hardware. AVX-512 uses VPMOVQB (TruncateToInt8) which
// performs the byte gather natively in one instruction; this path is
// expected to win on Intel SPR / Zen 5 but cannot be benched on Zen 3.

func init() {
	if cpu.X86.HasAVX512 {
		D64Eq = avx512D64Eq
		D64Lt = avx512D64Lt
	}
}

// AVX-512 path: 8 D64 elements per inner step (Int64x8). Compare ->
// AND with broadcast(1) puts a 0/1 byte in the low byte of each qword.
// VPMOVQB (TruncateToInt8) then gathers the 8 low bytes into the low
// 8 bytes of an Int8x16. We extract that as a single uint64 and emit
// one 8-byte unsafe store.

//go:nosplit
func storeBools8(v archsimd.Int64x8, one archsimd.Int64x8, out []bool, i int) {
	packed := v.And(one).TruncateToInt8().AsInt64x2().GetElem(0)
	*(*uint64)(unsafe.Pointer(&out[i])) = uint64(packed)
}

func avx512D64Eq(a, b []uint64, out []bool) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	pa, pb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	one := archsimd.BroadcastInt64x8(1)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		storeBools8(av.Equal(bv).ToInt64x8(), one, out, i)
	}
	for ; i < n; i++ {
		out[i] = a[i] == b[i]
	}
}

func avx512D64Lt(a, b []uint64, out []bool) {
	n := len(a)
	if n == 0 || len(b) < n || len(out) < n {
		return
	}
	pa, pb := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0])
	one := archsimd.BroadcastInt64x8(1)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 8
		av := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		bv := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		storeBools8(av.Less(bv).ToInt64x8(), one, out, i)
	}
	for ; i < n; i++ {
		out[i] = int64(a[i]) < int64(b[i])
	}
}
