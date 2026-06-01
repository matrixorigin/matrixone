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

package simdkernels

import "math/bits"

// Decimal128 add/sub on slices of uint64 with the matrixone Decimal128
// in-memory layout (lo, hi pair per element). The slices have length 2*N
// where N is the element count; element i occupies indices 2i (lo) and
// 2i+1 (hi). Hi is interpreted as int64 for the signed-overflow predicate.
//
// Operands and result are assumed to share the same scale.
//
// Two variants per operator (mirroring D64*):
//
//	*Unchecked — wraps on overflow, no detection.
//	*Checked   — returns the first overflowing element index, or -1 if none.
//
// The exported variables are dispatchers; their default values are the
// scalar reference implementations and may be replaced at init time on
// amd64 when AVX2 / AVX-512 are detected (see d128_addsub_simd_amd64.go).

var (
	D128AddUnchecked func(a, b, r []uint64)     = scalarD128AddUnchecked
	D128SubUnchecked func(a, b, r []uint64)     = scalarD128SubUnchecked
	D128AddChecked   func(a, b, r []uint64) int = scalarD128AddChecked
	D128SubChecked   func(a, b, r []uint64) int = scalarD128SubChecked

	// Scalar-broadcast variants: scalar is passed as two uint64s
	// (slo = low 64 bits, shi = high 64 bits), matching the
	// types.Decimal128 in-memory layout (B0_63, B64_127).
	D128AddScalarUnchecked func(slo, shi uint64, v, r []uint64)              = scalarD128AddScalarUnchecked
	D128AddScalarChecked   func(slo, shi uint64, v, r []uint64) int          = scalarD128AddScalarChecked
	D128SubScalarUnchecked func(v []uint64, slo, shi uint64, r []uint64)     = scalarD128SubScalarUnchecked
	D128SubScalarChecked   func(v []uint64, slo, shi uint64, r []uint64) int = scalarD128SubScalarChecked
	D128ScalarSubUnchecked func(slo, shi uint64, v, r []uint64)              = scalarD128ScalarSubUnchecked
	D128ScalarSubChecked   func(slo, shi uint64, v, r []uint64) int          = scalarD128ScalarSubChecked

	// D128SumReduce sums a contiguous slice of Decimal128 values and returns
	// the 128-bit total as (lo, hi). Wraps on overflow (mod 2^128).
	D128SumReduce func(v []uint64) (lo, hi uint64) = scalarD128SumReduce
)

func scalarD128AddUnchecked(a, b, r []uint64) {
	n := len(r) / 2
	if len(a) < 2*n || len(b) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo, c := bits.Add64(a[j], b[j], 0)
		hi, _ := bits.Add64(a[j+1], b[j+1], c)
		r[j] = lo
		r[j+1] = hi
	}
}

func scalarD128SubUnchecked(a, b, r []uint64) {
	n := len(r) / 2
	if len(a) < 2*n || len(b) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo, br := bits.Sub64(a[j], b[j], 0)
		hi, _ := bits.Sub64(a[j+1], b[j+1], br)
		r[j] = lo
		r[j+1] = hi
	}
}

// d128 signed overflow on add: same as 64-bit, evaluated on the high half.
//
//	signX == signY && signX != signR     ⇔     ((aHi^rHi) &^ (aHi^bHi)) < 0
func scalarD128AddChecked(a, b, r []uint64) int {
	n := len(r) / 2
	if len(a) < 2*n || len(b) < 2*n {
		return -1
	}
	first := -1
	for i := 0; i < n; i++ {
		j := i << 1
		aLo, aHi := a[j], a[j+1]
		bLo, bHi := b[j], b[j+1]
		lo, c := bits.Add64(aLo, bLo, 0)
		hi, _ := bits.Add64(aHi, bHi, c)
		r[j] = lo
		r[j+1] = hi
		if first < 0 {
			ah, bh, rh := int64(aHi), int64(bHi), int64(hi)
			if (ah^rh)&^(ah^bh) < 0 {
				first = i
			}
		}
	}
	return first
}

// d128 signed overflow on sub:
//
//	signX != signY && signX != signR     ⇔     ((aHi^rHi) & (aHi^bHi)) < 0
func scalarD128SubChecked(a, b, r []uint64) int {
	n := len(r) / 2
	if len(a) < 2*n || len(b) < 2*n {
		return -1
	}
	first := -1
	for i := 0; i < n; i++ {
		j := i << 1
		aLo, aHi := a[j], a[j+1]
		bLo, bHi := b[j], b[j+1]
		lo, br := bits.Sub64(aLo, bLo, 0)
		hi, _ := bits.Sub64(aHi, bHi, br)
		r[j] = lo
		r[j+1] = hi
		if first < 0 {
			ah, bh, rh := int64(aHi), int64(bHi), int64(hi)
			if (ah^rh)&(ah^bh) < 0 {
				first = i
			}
		}
	}
	return first
}

// d128FirstOverflow rescans the first end elements (each = 2 uint64) for
// the first overflow. Used by SIMD checked variants when their accumulated
// mask reports overflow but the scalar tail did not see one.
func d128FirstOverflow(a, b []uint64, end int, sub bool) int {
	if sub {
		for i := 0; i < end; i++ {
			j := i << 1
			lo, br := bits.Sub64(a[j], b[j], 0)
			hi, _ := bits.Sub64(a[j+1], b[j+1], br)
			_ = lo
			ah, bh, rh := int64(a[j+1]), int64(b[j+1]), int64(hi)
			if (ah^rh)&(ah^bh) < 0 {
				return i
			}
		}
		return -1
	}
	for i := 0; i < end; i++ {
		j := i << 1
		lo, c := bits.Add64(a[j], b[j], 0)
		hi, _ := bits.Add64(a[j+1], b[j+1], c)
		_ = lo
		ah, bh, rh := int64(a[j+1]), int64(b[j+1]), int64(hi)
		if (ah^rh)&^(ah^bh) < 0 {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Scalar-broadcast reference implementations.
// ---------------------------------------------------------------------------

func scalarD128AddScalarUnchecked(slo, shi uint64, v, r []uint64) {
	n := len(r) / 2
	if len(v) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo, c := bits.Add64(slo, v[j], 0)
		hi, _ := bits.Add64(shi, v[j+1], c)
		r[j] = lo
		r[j+1] = hi
	}
}

func scalarD128SubScalarUnchecked(v []uint64, slo, shi uint64, r []uint64) {
	n := len(r) / 2
	if len(v) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo, br := bits.Sub64(v[j], slo, 0)
		hi, _ := bits.Sub64(v[j+1], shi, br)
		r[j] = lo
		r[j+1] = hi
	}
}

func scalarD128ScalarSubUnchecked(slo, shi uint64, v, r []uint64) {
	n := len(r) / 2
	if len(v) < 2*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 1
		lo, br := bits.Sub64(slo, v[j], 0)
		hi, _ := bits.Sub64(shi, v[j+1], br)
		r[j] = lo
		r[j+1] = hi
	}
}

func scalarD128AddScalarChecked(slo, shi uint64, v, r []uint64) int {
	n := len(r) / 2
	if len(v) < 2*n {
		return -1
	}
	first := -1
	sh := int64(shi)
	for i := 0; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		lo, c := bits.Add64(slo, vLo, 0)
		hi, _ := bits.Add64(shi, vHi, c)
		r[j] = lo
		r[j+1] = hi
		if first < 0 {
			vh, rh := int64(vHi), int64(hi)
			if (sh^rh)&^(sh^vh) < 0 {
				first = i
			}
		}
	}
	return first
}

func scalarD128SubScalarChecked(v []uint64, slo, shi uint64, r []uint64) int {
	n := len(r) / 2
	if len(v) < 2*n {
		return -1
	}
	first := -1
	sh := int64(shi)
	for i := 0; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		lo, br := bits.Sub64(vLo, slo, 0)
		hi, _ := bits.Sub64(vHi, shi, br)
		r[j] = lo
		r[j+1] = hi
		if first < 0 {
			vh, rh := int64(vHi), int64(hi)
			if (vh^rh)&(vh^sh) < 0 {
				first = i
			}
		}
	}
	return first
}

func scalarD128ScalarSubChecked(slo, shi uint64, v, r []uint64) int {
	n := len(r) / 2
	if len(v) < 2*n {
		return -1
	}
	first := -1
	sh := int64(shi)
	for i := 0; i < n; i++ {
		j := i << 1
		vLo, vHi := v[j], v[j+1]
		lo, br := bits.Sub64(slo, vLo, 0)
		hi, _ := bits.Sub64(shi, vHi, br)
		r[j] = lo
		r[j+1] = hi
		if first < 0 {
			vh, rh := int64(vHi), int64(hi)
			if (sh^rh)&(sh^vh) < 0 {
				first = i
			}
		}
	}
	return first
}

// d128ScalarFirstOverflow rescans first end elements for the first overflow
// in scalar-broadcast operations. kind: 0=AddScalar, 1=SubScalar (v-s),
// 2=ScalarSub (s-v).
func d128ScalarFirstOverflow(slo, shi uint64, v []uint64, end int, kind int) int {
	sh := int64(shi)
	switch kind {
	case 0:
		for i := 0; i < end; i++ {
			j := i << 1
			vLo, vHi := v[j], v[j+1]
			_, c := bits.Add64(slo, vLo, 0)
			hi, _ := bits.Add64(shi, vHi, c)
			vh, rh := int64(vHi), int64(hi)
			if (sh^rh)&^(sh^vh) < 0 {
				return i
			}
		}
	case 1:
		for i := 0; i < end; i++ {
			j := i << 1
			vLo, vHi := v[j], v[j+1]
			_, br := bits.Sub64(vLo, slo, 0)
			hi, _ := bits.Sub64(vHi, shi, br)
			vh, rh := int64(vHi), int64(hi)
			if (vh^rh)&(vh^sh) < 0 {
				return i
			}
		}
	case 2:
		for i := 0; i < end; i++ {
			j := i << 1
			vLo, vHi := v[j], v[j+1]
			_, br := bits.Sub64(slo, vLo, 0)
			hi, _ := bits.Sub64(shi, vHi, br)
			vh, rh := int64(vHi), int64(hi)
			if (sh^rh)&(sh^vh) < 0 {
				return i
			}
		}
	}
	return -1
}

func scalarD128SumReduce(v []uint64) (lo, hi uint64) {
	n := len(v) >> 1
	for i := 0; i < n; i++ {
		j := i << 1
		var c uint64
		lo, c = bits.Add64(lo, v[j], 0)
		hi, _ = bits.Add64(hi, v[j+1], c)
	}
	return
}
