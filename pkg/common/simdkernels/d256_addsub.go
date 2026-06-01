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

// Decimal256 add/sub on uint64 slices with the matrixone Decimal256 layout
// (4 uint64 per element, lo→hi at indices 4i..4i+3). The top word (slot
// 4i+3) is interpreted as int64 for the signed-overflow predicate.
//
// API mirrors D64*/D128*: dispatcher pairs (Unchecked, Checked); the
// dispatcher defaults to the scalar reference and is replaced at init time
// on amd64 with AVX2 / AVX-512 (see d256_addsub_simd_amd64.go).

var (
	D256AddUnchecked func(a, b, r []uint64)     = scalarD256AddUnchecked
	D256SubUnchecked func(a, b, r []uint64)     = scalarD256SubUnchecked
	D256AddChecked   func(a, b, r []uint64) int = scalarD256AddChecked
	D256SubChecked   func(a, b, r []uint64) int = scalarD256SubChecked

	D256AddScalarUnchecked func(s0, s1, s2, s3 uint64, v, r []uint64)              = scalarD256AddScalarUnchecked
	D256AddScalarChecked   func(s0, s1, s2, s3 uint64, v, r []uint64) int          = scalarD256AddScalarChecked
	D256SubScalarUnchecked func(v []uint64, s0, s1, s2, s3 uint64, r []uint64)     = scalarD256SubScalarUnchecked
	D256SubScalarChecked   func(v []uint64, s0, s1, s2, s3 uint64, r []uint64) int = scalarD256SubScalarChecked
	D256ScalarSubUnchecked func(s0, s1, s2, s3 uint64, v, r []uint64)              = scalarD256ScalarSubUnchecked
	D256ScalarSubChecked   func(s0, s1, s2, s3 uint64, v, r []uint64) int          = scalarD256ScalarSubChecked
)

func scalarD256AddUnchecked(a, b, r []uint64) {
	n := len(r) / 4
	if len(a) < 4*n || len(b) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0, c := bits.Add64(a[j], b[j], 0)
		w1, c := bits.Add64(a[j+1], b[j+1], c)
		w2, c := bits.Add64(a[j+2], b[j+2], c)
		w3, _ := bits.Add64(a[j+3], b[j+3], c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func scalarD256SubUnchecked(a, b, r []uint64) {
	n := len(r) / 4
	if len(a) < 4*n || len(b) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0, br := bits.Sub64(a[j], b[j], 0)
		w1, br := bits.Sub64(a[j+1], b[j+1], br)
		w2, br := bits.Sub64(a[j+2], b[j+2], br)
		w3, _ := bits.Sub64(a[j+3], b[j+3], br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func scalarD256AddChecked(a, b, r []uint64) int {
	n := len(r) / 4
	if len(a) < 4*n || len(b) < 4*n {
		return -1
	}
	first := -1
	for i := 0; i < n; i++ {
		j := i << 2
		aHi := a[j+3]
		bHi := b[j+3]
		w0, c := bits.Add64(a[j], b[j], 0)
		w1, c := bits.Add64(a[j+1], b[j+1], c)
		w2, c := bits.Add64(a[j+2], b[j+2], c)
		w3, _ := bits.Add64(aHi, bHi, c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		if first < 0 {
			ah, bh, rh := int64(aHi), int64(bHi), int64(w3)
			if (ah^rh)&^(ah^bh) < 0 {
				first = i
			}
		}
	}
	return first
}

func scalarD256SubChecked(a, b, r []uint64) int {
	n := len(r) / 4
	if len(a) < 4*n || len(b) < 4*n {
		return -1
	}
	first := -1
	for i := 0; i < n; i++ {
		j := i << 2
		aHi := a[j+3]
		bHi := b[j+3]
		w0, br := bits.Sub64(a[j], b[j], 0)
		w1, br := bits.Sub64(a[j+1], b[j+1], br)
		w2, br := bits.Sub64(a[j+2], b[j+2], br)
		w3, _ := bits.Sub64(aHi, bHi, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		if first < 0 {
			ah, bh, rh := int64(aHi), int64(bHi), int64(w3)
			if (ah^rh)&(ah^bh) < 0 {
				first = i
			}
		}
	}
	return first
}

// d256FirstOverflow rescans the first end elements (each = 4 uint64) for
// the first overflow. Used by SIMD checked variants when the accumulated
// mask reports overflow but the scalar tail did not see one.
func d256FirstOverflow(a, b []uint64, end int, sub bool) int {
	if sub {
		for i := 0; i < end; i++ {
			j := i << 2
			_, br := bits.Sub64(a[j], b[j], 0)
			_, br = bits.Sub64(a[j+1], b[j+1], br)
			_, br = bits.Sub64(a[j+2], b[j+2], br)
			w3, _ := bits.Sub64(a[j+3], b[j+3], br)
			ah, bh, rh := int64(a[j+3]), int64(b[j+3]), int64(w3)
			if (ah^rh)&(ah^bh) < 0 {
				return i
			}
		}
		return -1
	}
	for i := 0; i < end; i++ {
		j := i << 2
		_, c := bits.Add64(a[j], b[j], 0)
		_, c = bits.Add64(a[j+1], b[j+1], c)
		_, c = bits.Add64(a[j+2], b[j+2], c)
		w3, _ := bits.Add64(a[j+3], b[j+3], c)
		ah, bh, rh := int64(a[j+3]), int64(b[j+3]), int64(w3)
		if (ah^rh)&^(ah^bh) < 0 {
			return i
		}
	}
	return -1
}

// ---------------------------------------------------------------------------
// Scalar-broadcast reference implementations (Decimal256 = 4 uint64/elem).
// ---------------------------------------------------------------------------

func scalarD256AddScalarUnchecked(s0, s1, s2, s3 uint64, v, r []uint64) {
	n := len(r) / 4
	if len(v) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0, c := bits.Add64(s0, v[j], 0)
		w1, c := bits.Add64(s1, v[j+1], c)
		w2, c := bits.Add64(s2, v[j+2], c)
		w3, _ := bits.Add64(s3, v[j+3], c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func scalarD256SubScalarUnchecked(v []uint64, s0, s1, s2, s3 uint64, r []uint64) {
	n := len(r) / 4
	if len(v) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0, br := bits.Sub64(v[j], s0, 0)
		w1, br := bits.Sub64(v[j+1], s1, br)
		w2, br := bits.Sub64(v[j+2], s2, br)
		w3, _ := bits.Sub64(v[j+3], s3, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func scalarD256ScalarSubUnchecked(s0, s1, s2, s3 uint64, v, r []uint64) {
	n := len(r) / 4
	if len(v) < 4*n {
		return
	}
	for i := 0; i < n; i++ {
		j := i << 2
		w0, br := bits.Sub64(s0, v[j], 0)
		w1, br := bits.Sub64(s1, v[j+1], br)
		w2, br := bits.Sub64(s2, v[j+2], br)
		w3, _ := bits.Sub64(s3, v[j+3], br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func scalarD256AddScalarChecked(s0, s1, s2, s3 uint64, v, r []uint64) int {
	n := len(r) / 4
	if len(v) < 4*n {
		return -1
	}
	first := -1
	sh := int64(s3)
	for i := 0; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, c := bits.Add64(s0, v[j], 0)
		w1, c := bits.Add64(s1, v[j+1], c)
		w2, c := bits.Add64(s2, v[j+2], c)
		w3, _ := bits.Add64(s3, vHi, c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		if first < 0 {
			vh, rh := int64(vHi), int64(w3)
			if (sh^rh)&^(sh^vh) < 0 {
				first = i
			}
		}
	}
	return first
}

func scalarD256SubScalarChecked(v []uint64, s0, s1, s2, s3 uint64, r []uint64) int {
	n := len(r) / 4
	if len(v) < 4*n {
		return -1
	}
	first := -1
	sh := int64(s3)
	for i := 0; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, br := bits.Sub64(v[j], s0, 0)
		w1, br := bits.Sub64(v[j+1], s1, br)
		w2, br := bits.Sub64(v[j+2], s2, br)
		w3, _ := bits.Sub64(vHi, s3, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		if first < 0 {
			vh, rh := int64(vHi), int64(w3)
			if (vh^rh)&(vh^sh) < 0 {
				first = i
			}
		}
	}
	return first
}

func scalarD256ScalarSubChecked(s0, s1, s2, s3 uint64, v, r []uint64) int {
	n := len(r) / 4
	if len(v) < 4*n {
		return -1
	}
	first := -1
	sh := int64(s3)
	for i := 0; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, br := bits.Sub64(s0, v[j], 0)
		w1, br := bits.Sub64(s1, v[j+1], br)
		w2, br := bits.Sub64(s2, v[j+2], br)
		w3, _ := bits.Sub64(s3, vHi, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		if first < 0 {
			vh, rh := int64(vHi), int64(w3)
			if (sh^rh)&(sh^vh) < 0 {
				first = i
			}
		}
	}
	return first
}

// d256ScalarFirstOverflow: rescan first end elements for first overflow in
// scalar-broadcast ops. kind: 0=AddScalar, 1=SubScalar (v-s), 2=ScalarSub (s-v).
func d256ScalarFirstOverflow(s0, s1, s2, s3 uint64, v []uint64, end int, kind int) int {
	sh := int64(s3)
	switch kind {
	case 0:
		for i := 0; i < end; i++ {
			j := i << 2
			_, c := bits.Add64(s0, v[j], 0)
			_, c = bits.Add64(s1, v[j+1], c)
			_, c = bits.Add64(s2, v[j+2], c)
			w3, _ := bits.Add64(s3, v[j+3], c)
			vh, rh := int64(v[j+3]), int64(w3)
			if (sh^rh)&^(sh^vh) < 0 {
				return i
			}
		}
	case 1:
		for i := 0; i < end; i++ {
			j := i << 2
			_, br := bits.Sub64(v[j], s0, 0)
			_, br = bits.Sub64(v[j+1], s1, br)
			_, br = bits.Sub64(v[j+2], s2, br)
			w3, _ := bits.Sub64(v[j+3], s3, br)
			vh, rh := int64(v[j+3]), int64(w3)
			if (vh^rh)&(vh^sh) < 0 {
				return i
			}
		}
	case 2:
		for i := 0; i < end; i++ {
			j := i << 2
			_, br := bits.Sub64(s0, v[j], 0)
			_, br = bits.Sub64(s1, v[j+1], br)
			_, br = bits.Sub64(s2, v[j+2], br)
			w3, _ := bits.Sub64(s3, v[j+3], br)
			vh, rh := int64(v[j+3]), int64(w3)
			if (sh^rh)&(sh^vh) < 0 {
				return i
			}
		}
	}
	return -1
}
