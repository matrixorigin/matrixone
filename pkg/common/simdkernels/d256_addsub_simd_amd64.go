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

// On AVX-512 hosts we use the avx512 path (8 elements/iter); otherwise on
// AVX2 hosts we use the avx2 path (4 elements/iter); else scalar.
func init() {
	switch {
	case cpu.X86.HasAVX512:
		D256AddUnchecked = avx512D256AddUnchecked
		D256SubUnchecked = avx512D256SubUnchecked
		D256AddChecked = avx512D256AddChecked
		D256SubChecked = avx512D256SubChecked
		D256AddScalarUnchecked = avx512D256AddScalarUnchecked
		D256SubScalarUnchecked = avx512D256SubScalarUnchecked
		D256ScalarSubUnchecked = avx512D256ScalarSubUnchecked
		D256AddScalarChecked = avx512D256AddScalarChecked
		D256SubScalarChecked = avx512D256SubScalarChecked
		D256ScalarSubChecked = avx512D256ScalarSubChecked
	case cpu.X86.HasAVX2:
		D256AddUnchecked = avx2D256AddUnchecked
		D256SubUnchecked = avx2D256SubUnchecked
		D256AddChecked = avx2D256AddChecked
		D256SubChecked = avx2D256SubChecked
		D256AddScalarUnchecked = avx2D256AddScalarUnchecked
		D256SubScalarUnchecked = avx2D256SubScalarUnchecked
		D256ScalarSubUnchecked = avx2D256ScalarSubUnchecked
		D256AddScalarChecked = avx2D256AddScalarChecked
		D256SubScalarChecked = avx2D256SubScalarChecked
		D256ScalarSubChecked = avx2D256ScalarSubChecked
	}
}

// transpose4x4 turns the column-major load layout [v0=elem0, v1=elem1,
// v2=elem2, v3=elem3] (each Int64x4 = one Decimal256, lanes = words) into
// the row-major layout where Wk[i] = element_i.word_k. The operation is
// involutory: applying it again restores the original order, which we use
// to write the result back.
//
// Implemented as the standard 4×4 int64 transpose with 4 unpacks + 4
// VPERM2I128 (Select128FromPair). All operations are AVX2.
//
//go:nosplit
func transpose4x4(v0, v1, v2, v3 archsimd.Int64x4) (W0, W1, W2, W3 archsimd.Int64x4) {
	t0 := v0.InterleaveLoGrouped(v1) // [v0[0], v1[0], v0[2], v1[2]]
	t1 := v0.InterleaveHiGrouped(v1) // [v0[1], v1[1], v0[3], v1[3]]
	t2 := v2.InterleaveLoGrouped(v3) // [v2[0], v3[0], v2[2], v3[2]]
	t3 := v2.InterleaveHiGrouped(v3) // [v2[1], v3[1], v2[3], v3[3]]
	// VPERM2I128 imm encoding: arg(lo)/arg(hi) selects which 128-bit half:
	//   0 = x.lo, 1 = x.hi, 2 = y.lo, 3 = y.hi.
	W0 = t0.Select128FromPair(0, 2, t2) // [v0[0], v1[0], v2[0], v3[0]]
	W2 = t0.Select128FromPair(1, 3, t2) // [v0[2], v1[2], v2[2], v3[2]]
	W1 = t1.Select128FromPair(0, 2, t3) // [v0[1], v1[1], v2[1], v3[1]]
	W3 = t1.Select128FromPair(1, 3, t3) // [v0[3], v1[3], v2[3], v3[3]]
	return
}

// addCarryStage performs one stage of the multi-word add: r = a + b + cIn,
// returning r and the carry-out vector (each lane = 0 or -1).
//
// cIn is encoded as a vector with values 0 or -1 (so subtracting it adds 0
// or 1). cOut is the OR of two contributing wraps:
//   - the bare `a + b` wrap  (always possible, regardless of cIn)
//   - the `(a+b) - cIn` wrap (only when cIn = -1 and a+b = MaxU64)
//
// The (s == -1) & cIn alternative formulation was tried but regressed by ~5%
// because addCarryStage already shares (s^sb) between csA and csB via CSE,
// so only one XOR is saved while an extra constant broadcast and AND are
// added. The asymmetry vs subBorrowStage reflects that addCarry's CSE is
// already optimal whereas subBorrow's was not.
//
// Sign-bit-flipped Less is used because AVX2 has no unsigned int64 compare.
//
//go:nosplit
func addCarryStage(a, b, cIn, sb archsimd.Int64x4) (r, cOut archsimd.Int64x4) {
	s := a.Add(b)
	csA := s.Xor(sb).Less(a.Xor(sb)).ToInt64x4()
	r = s.Sub(cIn)
	csB := r.Xor(sb).Less(s.Xor(sb)).ToInt64x4()
	cOut = csA.Or(csB)
	return
}

// addCarryStageNoOut is addCarryStage without computing the carry-out
// (used for the topmost word in unchecked add).
//
//go:nosplit
func addCarryStageNoOut(a, b, cIn archsimd.Int64x4) archsimd.Int64x4 {
	return a.Add(b).Sub(cIn)
}

// subBorrowStage performs one stage of the multi-word sub: r = a - b - bIn.
//
// bIn is encoded as a vector with values 0 or -1 (so adding it subtracts 0
// or 1). bOut is the OR of:
//   - bare `a - b` borrow: a < b unsigned     ⇔ (a^sb) < (b^sb) signed
//   - `(a-b) + bIn` borrow: happens iff s = 0 AND bIn = -1
//
// The second term is computed as (s == 0) & bIn, which avoids materializing
// `s^sb` and `r^sb`. Compared to the symmetric `(s^sb) < (r^sb)` form, this
// drops one XOR per stage and (more importantly) shortens the live-range
// chain so the register allocator does not spill across the carry chain.
//
//go:nosplit
func subBorrowStage(a, b, bIn, sb, zero archsimd.Int64x4) (r, bOut archsimd.Int64x4) {
	s := a.Sub(b)
	bsA := a.Xor(sb).Less(b.Xor(sb)).ToInt64x4()
	r = s.Add(bIn)
	bsB := s.Equal(zero).ToInt64x4().And(bIn)
	bOut = bsA.Or(bsB)
	return
}

//go:nosplit
func subBorrowStageNoOut(a, b, bIn archsimd.Int64x4) archsimd.Int64x4 {
	return a.Sub(b).Add(bIn)
}

func avx2D256AddUnchecked(a, b, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		aV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+64)))
		aV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+96)))
		bV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))
		bV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+64)))
		bV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose4x4(bV0, bV1, bV2, bV3)

		rW0, c0 := addCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := addCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := addCarryStage(aW2, bW2, c1, sb)
		rW3 := addCarryStageNoOut(aW3, bW3, c2)

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, c := addU64(a[j], b[j], 0)
		w1, c := addU64(a[j+1], b[j+1], c)
		w2, c := addU64(a[j+2], b[j+2], c)
		w3, _ := addU64(a[j+3], b[j+3], c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx2D256SubUnchecked(a, b, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		aV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+64)))
		aV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+96)))
		bV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))
		bV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+64)))
		bV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose4x4(bV0, bV1, bV2, bV3)

		rW0, b0 := subBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := subBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := subBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := subBorrowStageNoOut(aW3, bW3, b2)

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, br := subU64(a[j], b[j], 0)
		w1, br := subU64(a[j+1], b[j+1], br)
		w2, br := subU64(a[j+2], b[j+2], br)
		w3, _ := subU64(a[j+3], b[j+3], br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx2D256AddChecked(a, b, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		aV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+64)))
		aV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+96)))
		bV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))
		bV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+64)))
		bV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose4x4(bV0, bV1, bV2, bV3)

		rW0, c0 := addCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := addCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := addCarryStage(aW2, bW2, c1, sb)
		rW3 := addCarryStageNoOut(aW3, bW3, c2)

		ofAcc = ofAcc.Or(aW3.Xor(rW3).AndNot(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 2
		aHi := a[j+3]
		bHi := b[j+3]
		w0, c := addU64(a[j], b[j], 0)
		w1, c := addU64(a[j+1], b[j+1], c)
		w2, c := addU64(a[j+2], b[j+2], c)
		w3, _ := addU64(aHi, bHi, c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		ah, bh, rh := int64(aHi), int64(bHi), int64(w3)
		if (ah^rh)&^(ah^bh) < 0 {
			if vecOverflow {
				return d256FirstOverflow(a, b, vecEnd, false)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256FirstOverflow(a, b, vecEnd, false)
}

func avx2D256SubChecked(a, b, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+32)))
		aV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+64)))
		aV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pa, off+96)))
		bV0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+32)))
		bV2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+64)))
		bV3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pb, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose4x4(bV0, bV1, bV2, bV3)

		rW0, b0 := subBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := subBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := subBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := subBorrowStageNoOut(aW3, bW3, b2)

		ofAcc = ofAcc.Or(aW3.Xor(rW3).And(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 2
		aHi := a[j+3]
		bHi := b[j+3]
		w0, br := subU64(a[j], b[j], 0)
		w1, br := subU64(a[j+1], b[j+1], br)
		w2, br := subU64(a[j+2], b[j+2], br)
		w3, _ := subU64(aHi, bHi, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		ah, bh, rh := int64(aHi), int64(bHi), int64(w3)
		if (ah^rh)&(ah^bh) < 0 {
			if vecOverflow {
				return d256FirstOverflow(a, b, vecEnd, true)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256FirstOverflow(a, b, vecEnd, true)
}

// addU64 / subU64 are local thin wrappers around math/bits to keep the
// scalar tail concise; they inline at call sites.
//
//go:nosplit
func addU64(x, y, c uint64) (uint64, uint64) {
	s := x + y
	c2 := uint64(0)
	if s < x {
		c2 = 1
	}
	r := s + c
	c3 := uint64(0)
	if r < s {
		c3 = 1
	}
	return r, c2 | c3
}

//go:nosplit
func subU64(x, y, br uint64) (uint64, uint64) {
	d := x - y
	b1 := uint64(0)
	if x < y {
		b1 = 1
	}
	r := d - br
	b2 := uint64(0)
	if d < br {
		b2 = 1
	}
	return r, b1 | b2
}

// ---------------------------------------------------------------------------
// AVX-512 path: each Decimal256 = 4 uint64 = 32 B. We process 8 elements per
// kernel iteration (= 256 B per input), loaded as 4 Int64x8 vectors.
//
// Layout per loaded vector (lo→hi inside each 4-word element):
//   V0 = [e0w0,e0w1,e0w2,e0w3, e1w0,e1w1,e1w2,e1w3]
//   V1 = [e2w0,e2w1,e2w2,e2w3, e3w0,e3w1,e3w2,e3w3]
//   V2 = [e4*..]  V3 = [e6*..]
//
// We transpose to per-word vectors W0..W3 each holding the same word from
// 8 elements, run the 4-stage carry/borrow chain, and inverse-transpose
// back. Both transposes are 8 ConcatPermute (VPERMI2Q) instructions
// arranged as two 4-permute stages.
// ---------------------------------------------------------------------------

// AVX-512 transpose index vectors. They are package-scope so that
// LoadUint64x8 hoists to a single broadcast/load that the compiler can
// keep in a register across the loop body.
var (
	avx512D256IdxFwdW0W2 = [8]uint64{0, 4, 8, 12, 2, 6, 10, 14}
	avx512D256IdxFwdW1W3 = [8]uint64{1, 5, 9, 13, 3, 7, 11, 15}
	avx512D256IdxLoHalf  = [8]uint64{0, 1, 2, 3, 8, 9, 10, 11}
	avx512D256IdxHiHalf  = [8]uint64{4, 5, 6, 7, 12, 13, 14, 15}
	avx512D256IdxInvV0   = [8]uint64{0, 8, 4, 12, 1, 9, 5, 13}
	avx512D256IdxInvV1   = [8]uint64{2, 10, 6, 14, 3, 11, 7, 15}
)

// transpose8x4Forward: V0..V3 (each = 2 D256 elements in element-natural
// layout) → W0..W3 (each = one word from all 8 elements, in element order).
//
//go:nosplit
func transpose8x4Forward(v0, v1, v2, v3 archsimd.Int64x8) (w0, w1, w2, w3 archsimd.Int64x8) {
	idxW02 := archsimd.LoadUint64x8(&avx512D256IdxFwdW0W2)
	idxW13 := archsimd.LoadUint64x8(&avx512D256IdxFwdW1W3)
	idxLo := archsimd.LoadUint64x8(&avx512D256IdxLoHalf)
	idxHi := archsimd.LoadUint64x8(&avx512D256IdxHiHalf)

	// Stage 1: gather (w0,w2) and (w1,w3) within each pair-of-elements vector.
	q0 := v0.ConcatPermute(v1, idxW02) // [e0w0,e1w0,e2w0,e3w0, e0w2,e1w2,e2w2,e3w2]
	q1 := v0.ConcatPermute(v1, idxW13) // [e0w1,e1w1,e2w1,e3w1, e0w3,e1w3,e2w3,e3w3]
	q2 := v2.ConcatPermute(v3, idxW02) // [e4w0..e7w0, e4w2..e7w2]
	q3 := v2.ConcatPermute(v3, idxW13) // [e4w1..e7w1, e4w3..e7w3]

	// Stage 2: combine the lo/hi halves across pair groups.
	w0 = q0.ConcatPermute(q2, idxLo) // [e0w0..e7w0]
	w2 = q0.ConcatPermute(q2, idxHi) // [e0w2..e7w2]
	w1 = q1.ConcatPermute(q3, idxLo) // [e0w1..e7w1]
	w3 = q1.ConcatPermute(q3, idxHi) // [e0w3..e7w3]
	return
}

// transpose8x4Inverse: W0..W3 → V0..V3 (the inverse of transpose8x4Forward).
//
//go:nosplit
func transpose8x4Inverse(w0, w1, w2, w3 archsimd.Int64x8) (v0, v1, v2, v3 archsimd.Int64x8) {
	idxLo := archsimd.LoadUint64x8(&avx512D256IdxLoHalf)
	idxHi := archsimd.LoadUint64x8(&avx512D256IdxHiHalf)
	idxV0 := archsimd.LoadUint64x8(&avx512D256IdxInvV0)
	idxV1 := archsimd.LoadUint64x8(&avx512D256IdxInvV1)

	// Stage 1 (inverse of forward stage 2): split per-word vectors back into
	// pair-of-elements groups.
	q0 := w0.ConcatPermute(w2, idxLo) // [e0w0..e3w0, e0w2..e3w2]
	q2 := w0.ConcatPermute(w2, idxHi) // [e4w0..e7w0, e4w2..e7w2]
	q1 := w1.ConcatPermute(w3, idxLo) // [e0w1..e3w1, e0w3..e3w3]
	q3 := w1.ConcatPermute(w3, idxHi) // [e4w1..e7w1, e4w3..e7w3]

	// Stage 2 (inverse of forward stage 1): re-interleave words back into
	// element-natural layout.
	v0 = q0.ConcatPermute(q1, idxV0) // [e0w0..e0w3, e1w0..e1w3]
	v1 = q0.ConcatPermute(q1, idxV1) // [e2w0..e2w3, e3w0..e3w3]
	v2 = q2.ConcatPermute(q3, idxV0) // [e4w0..e4w3, e5w0..e5w3]
	v3 = q2.ConcatPermute(q3, idxV1) // [e6w0..e6w3, e7w0..e7w3]
	return
}

// avx512AddCarryStage and friends mirror the AVX2 helpers but on Int64x8.
//
//go:nosplit
func avx512AddCarryStage(a, b, cIn, sb archsimd.Int64x8) (r, cOut archsimd.Int64x8) {
	s := a.Add(b)
	csA := s.Xor(sb).Less(a.Xor(sb)).ToInt64x8()
	r = s.Sub(cIn)
	csB := r.Xor(sb).Less(s.Xor(sb)).ToInt64x8()
	cOut = csA.Or(csB)
	return
}

//go:nosplit
func avx512AddCarryStageNoOut(a, b, cIn archsimd.Int64x8) archsimd.Int64x8 {
	return a.Add(b).Sub(cIn)
}

//go:nosplit
func avx512SubBorrowStage(a, b, bIn, sb, zero archsimd.Int64x8) (r, bOut archsimd.Int64x8) {
	s := a.Sub(b)
	bsA := a.Xor(sb).Less(b.Xor(sb)).ToInt64x8()
	r = s.Add(bIn)
	bsB := s.Equal(zero).ToInt64x8().And(bIn)
	bOut = bsA.Or(bsB)
	return
}

//go:nosplit
func avx512SubBorrowStageNoOut(a, b, bIn archsimd.Int64x8) archsimd.Int64x8 {
	return a.Sub(b).Add(bIn)
}

func avx512D256AddUnchecked(a, b, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		aV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+128)))
		aV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+192)))
		bV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))
		bV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+128)))
		bV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose8x4Forward(bV0, bV1, bV2, bV3)

		rW0, c0 := avx512AddCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := avx512AddCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := avx512AddCarryStage(aW2, bW2, c1, sb)
		rW3 := avx512AddCarryStageNoOut(aW3, bW3, c2)

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, c := addU64(a[j], b[j], 0)
		w1, c := addU64(a[j+1], b[j+1], c)
		w2, c := addU64(a[j+2], b[j+2], c)
		w3, _ := addU64(a[j+3], b[j+3], c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx512D256SubUnchecked(a, b, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		aV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+128)))
		aV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+192)))
		bV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))
		bV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+128)))
		bV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose8x4Forward(bV0, bV1, bV2, bV3)

		rW0, b0 := avx512SubBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := avx512SubBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := avx512SubBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := avx512SubBorrowStageNoOut(aW3, bW3, b2)

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, br := subU64(a[j], b[j], 0)
		w1, br := subU64(a[j+1], b[j+1], br)
		w2, br := subU64(a[j+2], b[j+2], br)
		w3, _ := subU64(a[j+3], b[j+3], br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx512D256AddChecked(a, b, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		aV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+128)))
		aV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+192)))
		bV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))
		bV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+128)))
		bV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose8x4Forward(bV0, bV1, bV2, bV3)

		rW0, c0 := avx512AddCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := avx512AddCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := avx512AddCarryStage(aW2, bW2, c1, sb)
		rW3 := avx512AddCarryStageNoOut(aW3, bW3, c2)

		ofAcc = ofAcc.Or(aW3.Xor(rW3).AndNot(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 2
		aHi := a[j+3]
		bHi := b[j+3]
		w0, c := addU64(a[j], b[j], 0)
		w1, c := addU64(a[j+1], b[j+1], c)
		w2, c := addU64(a[j+2], b[j+2], c)
		w3, _ := addU64(aHi, bHi, c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		ah, bh, rh := int64(aHi), int64(bHi), int64(w3)
		if (ah^rh)&^(ah^bh) < 0 {
			if vecOverflow {
				return d256FirstOverflow(a, b, vecEnd, false)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256FirstOverflow(a, b, vecEnd, false)
}

func avx512D256SubChecked(a, b, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(a) < 4*n || len(b) < 4*n {
		return -1
	}
	pa, pb, pr := unsafe.Pointer(&a[0]), unsafe.Pointer(&b[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		aV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off)))
		aV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+64)))
		aV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+128)))
		aV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pa, off+192)))
		bV0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off)))
		bV1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+64)))
		bV2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+128)))
		bV3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pb, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(aV0, aV1, aV2, aV3)
		bW0, bW1, bW2, bW3 := transpose8x4Forward(bV0, bV1, bV2, bV3)

		rW0, b0 := avx512SubBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := avx512SubBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := avx512SubBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := avx512SubBorrowStageNoOut(aW3, bW3, b2)

		ofAcc = ofAcc.Or(aW3.Xor(rW3).And(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	for ; i < n; i++ {
		j := i << 2
		aHi := a[j+3]
		bHi := b[j+3]
		w0, br := subU64(a[j], b[j], 0)
		w1, br := subU64(a[j+1], b[j+1], br)
		w2, br := subU64(a[j+2], b[j+2], br)
		w3, _ := subU64(aHi, bHi, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		ah, bh, rh := int64(aHi), int64(bHi), int64(w3)
		if (ah^rh)&(ah^bh) < 0 {
			if vecOverflow {
				return d256FirstOverflow(a, b, vecEnd, true)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256FirstOverflow(a, b, vecEnd, true)
}

// ---------------------------------------------------------------------------
// AVX2 D256 broadcast variants. The scalar's 4 words become 4 uniform Int64x4
// vectors — no load/transpose needed for the scalar operand.
// ---------------------------------------------------------------------------

func avx2D256AddScalarUnchecked(s0, s1, s2, s3 uint64, v, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)
	bW0 := archsimd.BroadcastInt64x4(int64(s0))
	bW1 := archsimd.BroadcastInt64x4(int64(s1))
	bW2 := archsimd.BroadcastInt64x4(int64(s2))
	bW3 := archsimd.BroadcastInt64x4(int64(s3))

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(v0, v1, v2, v3)

		rW0, c0 := addCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := addCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := addCarryStage(aW2, bW2, c1, sb)
		rW3 := addCarryStageNoOut(aW3, bW3, c2)

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, c := addU64(s0, v[j], 0)
		w1, c := addU64(s1, v[j+1], c)
		w2, c := addU64(s2, v[j+2], c)
		w3, _ := addU64(s3, v[j+3], c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx2D256SubScalarUnchecked(v []uint64, s0, s1, s2, s3 uint64, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)
	bW0 := archsimd.BroadcastInt64x4(int64(s0))
	bW1 := archsimd.BroadcastInt64x4(int64(s1))
	bW2 := archsimd.BroadcastInt64x4(int64(s2))
	bW3 := archsimd.BroadcastInt64x4(int64(s3))

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(v0, v1, v2, v3)

		rW0, b0 := subBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := subBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := subBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := subBorrowStageNoOut(aW3, bW3, b2)

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, br := subU64(v[j], s0, 0)
		w1, br := subU64(v[j+1], s1, br)
		w2, br := subU64(v[j+2], s2, br)
		w3, _ := subU64(v[j+3], s3, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx2D256ScalarSubUnchecked(s0, s1, s2, s3 uint64, v, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)
	aW0 := archsimd.BroadcastInt64x4(int64(s0))
	aW1 := archsimd.BroadcastInt64x4(int64(s1))
	aW2 := archsimd.BroadcastInt64x4(int64(s2))
	aW3 := archsimd.BroadcastInt64x4(int64(s3))

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))

		bW0, bW1, bW2, bW3 := transpose4x4(v0, v1, v2, v3)

		rW0, b0 := subBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := subBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := subBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := subBorrowStageNoOut(aW3, bW3, b2)

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, br := subU64(s0, v[j], 0)
		w1, br := subU64(s1, v[j+1], br)
		w2, br := subU64(s2, v[j+2], br)
		w3, _ := subU64(s3, v[j+3], br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx2D256AddScalarChecked(s0, s1, s2, s3 uint64, v, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)
	bW0 := archsimd.BroadcastInt64x4(int64(s0))
	bW1 := archsimd.BroadcastInt64x4(int64(s1))
	bW2 := archsimd.BroadcastInt64x4(int64(s2))
	bW3 := archsimd.BroadcastInt64x4(int64(s3))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(v0, v1, v2, v3)

		rW0, c0 := addCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := addCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := addCarryStage(aW2, bW2, c1, sb)
		rW3 := addCarryStageNoOut(aW3, bW3, c2)
		ofAcc = ofAcc.Or(aW3.Xor(rW3).AndNot(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(s3)
	for ; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, c := addU64(s0, v[j], 0)
		w1, c := addU64(s1, v[j+1], c)
		w2, c := addU64(s2, v[j+2], c)
		w3, _ := addU64(s3, vHi, c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		vh, rh := int64(vHi), int64(w3)
		if (sh^rh)&^(sh^vh) < 0 {
			if vecOverflow {
				return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 0)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 0)
}

func avx2D256SubScalarChecked(v []uint64, s0, s1, s2, s3 uint64, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)
	bW0 := archsimd.BroadcastInt64x4(int64(s0))
	bW1 := archsimd.BroadcastInt64x4(int64(s1))
	bW2 := archsimd.BroadcastInt64x4(int64(s2))
	bW3 := archsimd.BroadcastInt64x4(int64(s3))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))

		aW0, aW1, aW2, aW3 := transpose4x4(v0, v1, v2, v3)

		rW0, b0 := subBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := subBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := subBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := subBorrowStageNoOut(aW3, bW3, b2)
		ofAcc = ofAcc.Or(aW3.Xor(rW3).And(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(s3)
	for ; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, br := subU64(v[j], s0, 0)
		w1, br := subU64(v[j+1], s1, br)
		w2, br := subU64(v[j+2], s2, br)
		w3, _ := subU64(vHi, s3, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		vh, rh := int64(vHi), int64(w3)
		if (vh^rh)&(vh^sh) < 0 {
			if vecOverflow {
				return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 1)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 1)
}

func avx2D256ScalarSubChecked(s0, s1, s2, s3 uint64, v, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x4(signBit128)
	zero := archsimd.BroadcastInt64x4(0)
	aW0 := archsimd.BroadcastInt64x4(int64(s0))
	aW1 := archsimd.BroadcastInt64x4(int64(s1))
	aW2 := archsimd.BroadcastInt64x4(int64(s2))
	aW3 := archsimd.BroadcastInt64x4(int64(s3))

	var ofAcc archsimd.Int64x4

	i := 0
	for ; i+4 <= n; i += 4 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+32)))
		v2 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+64)))
		v3 := archsimd.LoadInt64x4((*[4]int64)(unsafe.Add(pv, off+96)))

		bW0, bW1, bW2, bW3 := transpose4x4(v0, v1, v2, v3)

		rW0, b0 := subBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := subBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := subBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := subBorrowStageNoOut(aW3, bW3, b2)
		ofAcc = ofAcc.Or(aW3.Xor(rW3).And(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose4x4(rW0, rW1, rW2, rW3)
		rV0.Store((*[4]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[4]int64)(unsafe.Add(pr, off+32)))
		rV2.Store((*[4]int64)(unsafe.Add(pr, off+64)))
		rV3.Store((*[4]int64)(unsafe.Add(pr, off+96)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(s3)
	for ; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, br := subU64(s0, v[j], 0)
		w1, br := subU64(s1, v[j+1], br)
		w2, br := subU64(s2, v[j+2], br)
		w3, _ := subU64(s3, vHi, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		vh, rh := int64(vHi), int64(w3)
		if (sh^rh)&(sh^vh) < 0 {
			if vecOverflow {
				return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 2)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 2)
}

// ---------------------------------------------------------------------------
// AVX-512 D256 broadcast variants. Same skeleton but Int64x8 and 8 elems/iter.
// ---------------------------------------------------------------------------

func avx512D256AddScalarUnchecked(s0, s1, s2, s3 uint64, v, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)
	bW0 := archsimd.BroadcastInt64x8(int64(s0))
	bW1 := archsimd.BroadcastInt64x8(int64(s1))
	bW2 := archsimd.BroadcastInt64x8(int64(s2))
	bW3 := archsimd.BroadcastInt64x8(int64(s3))

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(v0, v1, v2, v3)

		rW0, c0 := avx512AddCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := avx512AddCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := avx512AddCarryStage(aW2, bW2, c1, sb)
		rW3 := avx512AddCarryStageNoOut(aW3, bW3, c2)

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, c := addU64(s0, v[j], 0)
		w1, c := addU64(s1, v[j+1], c)
		w2, c := addU64(s2, v[j+2], c)
		w3, _ := addU64(s3, v[j+3], c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx512D256SubScalarUnchecked(v []uint64, s0, s1, s2, s3 uint64, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)
	bW0 := archsimd.BroadcastInt64x8(int64(s0))
	bW1 := archsimd.BroadcastInt64x8(int64(s1))
	bW2 := archsimd.BroadcastInt64x8(int64(s2))
	bW3 := archsimd.BroadcastInt64x8(int64(s3))

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(v0, v1, v2, v3)

		rW0, b0 := avx512SubBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := avx512SubBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := avx512SubBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := avx512SubBorrowStageNoOut(aW3, bW3, b2)

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, br := subU64(v[j], s0, 0)
		w1, br := subU64(v[j+1], s1, br)
		w2, br := subU64(v[j+2], s2, br)
		w3, _ := subU64(v[j+3], s3, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx512D256ScalarSubUnchecked(s0, s1, s2, s3 uint64, v, r []uint64) {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)
	aW0 := archsimd.BroadcastInt64x8(int64(s0))
	aW1 := archsimd.BroadcastInt64x8(int64(s1))
	aW2 := archsimd.BroadcastInt64x8(int64(s2))
	aW3 := archsimd.BroadcastInt64x8(int64(s3))

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))

		bW0, bW1, bW2, bW3 := transpose8x4Forward(v0, v1, v2, v3)

		rW0, b0 := avx512SubBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := avx512SubBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := avx512SubBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := avx512SubBorrowStageNoOut(aW3, bW3, b2)

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	for ; i < n; i++ {
		j := i << 2
		w0, br := subU64(s0, v[j], 0)
		w1, br := subU64(s1, v[j+1], br)
		w2, br := subU64(s2, v[j+2], br)
		w3, _ := subU64(s3, v[j+3], br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
	}
}

func avx512D256AddScalarChecked(s0, s1, s2, s3 uint64, v, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)
	bW0 := archsimd.BroadcastInt64x8(int64(s0))
	bW1 := archsimd.BroadcastInt64x8(int64(s1))
	bW2 := archsimd.BroadcastInt64x8(int64(s2))
	bW3 := archsimd.BroadcastInt64x8(int64(s3))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(v0, v1, v2, v3)

		rW0, c0 := avx512AddCarryStage(aW0, bW0, zero, sb)
		rW1, c1 := avx512AddCarryStage(aW1, bW1, c0, sb)
		rW2, c2 := avx512AddCarryStage(aW2, bW2, c1, sb)
		rW3 := avx512AddCarryStageNoOut(aW3, bW3, c2)
		ofAcc = ofAcc.Or(aW3.Xor(rW3).AndNot(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(s3)
	for ; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, c := addU64(s0, v[j], 0)
		w1, c := addU64(s1, v[j+1], c)
		w2, c := addU64(s2, v[j+2], c)
		w3, _ := addU64(s3, vHi, c)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		vh, rh := int64(vHi), int64(w3)
		if (sh^rh)&^(sh^vh) < 0 {
			if vecOverflow {
				return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 0)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 0)
}

func avx512D256SubScalarChecked(v []uint64, s0, s1, s2, s3 uint64, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)
	bW0 := archsimd.BroadcastInt64x8(int64(s0))
	bW1 := archsimd.BroadcastInt64x8(int64(s1))
	bW2 := archsimd.BroadcastInt64x8(int64(s2))
	bW3 := archsimd.BroadcastInt64x8(int64(s3))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))

		aW0, aW1, aW2, aW3 := transpose8x4Forward(v0, v1, v2, v3)

		rW0, b0 := avx512SubBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := avx512SubBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := avx512SubBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := avx512SubBorrowStageNoOut(aW3, bW3, b2)
		ofAcc = ofAcc.Or(aW3.Xor(rW3).And(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(s3)
	for ; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, br := subU64(v[j], s0, 0)
		w1, br := subU64(v[j+1], s1, br)
		w2, br := subU64(v[j+2], s2, br)
		w3, _ := subU64(vHi, s3, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		vh, rh := int64(vHi), int64(w3)
		if (vh^rh)&(vh^sh) < 0 {
			if vecOverflow {
				return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 1)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 1)
}

func avx512D256ScalarSubChecked(s0, s1, s2, s3 uint64, v, r []uint64) int {
	n := len(r) / 4
	if n == 0 || len(v) < 4*n {
		return -1
	}
	pv, pr := unsafe.Pointer(&v[0]), unsafe.Pointer(&r[0])
	sb := archsimd.BroadcastInt64x8(signBit128)
	zero := archsimd.BroadcastInt64x8(0)
	aW0 := archsimd.BroadcastInt64x8(int64(s0))
	aW1 := archsimd.BroadcastInt64x8(int64(s1))
	aW2 := archsimd.BroadcastInt64x8(int64(s2))
	aW3 := archsimd.BroadcastInt64x8(int64(s3))

	var ofAcc archsimd.Int64x8

	i := 0
	for ; i+8 <= n; i += 8 {
		off := uintptr(i) * 32
		v0 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off)))
		v1 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+64)))
		v2 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+128)))
		v3 := archsimd.LoadInt64x8((*[8]int64)(unsafe.Add(pv, off+192)))

		bW0, bW1, bW2, bW3 := transpose8x4Forward(v0, v1, v2, v3)

		rW0, b0 := avx512SubBorrowStage(aW0, bW0, zero, sb, zero)
		rW1, b1 := avx512SubBorrowStage(aW1, bW1, b0, sb, zero)
		rW2, b2 := avx512SubBorrowStage(aW2, bW2, b1, sb, zero)
		rW3 := avx512SubBorrowStageNoOut(aW3, bW3, b2)
		ofAcc = ofAcc.Or(aW3.Xor(rW3).And(aW3.Xor(bW3)))

		rV0, rV1, rV2, rV3 := transpose8x4Inverse(rW0, rW1, rW2, rW3)
		rV0.Store((*[8]int64)(unsafe.Add(pr, off)))
		rV1.Store((*[8]int64)(unsafe.Add(pr, off+64)))
		rV2.Store((*[8]int64)(unsafe.Add(pr, off+128)))
		rV3.Store((*[8]int64)(unsafe.Add(pr, off+192)))
	}
	vecEnd := i

	vecOverflow := uint64(ofAcc.Less(zero).ToBits()) != 0

	sh := int64(s3)
	for ; i < n; i++ {
		j := i << 2
		vHi := v[j+3]
		w0, br := subU64(s0, v[j], 0)
		w1, br := subU64(s1, v[j+1], br)
		w2, br := subU64(s2, v[j+2], br)
		w3, _ := subU64(s3, vHi, br)
		r[j], r[j+1], r[j+2], r[j+3] = w0, w1, w2, w3
		vh, rh := int64(vHi), int64(w3)
		if (sh^rh)&(sh^vh) < 0 {
			if vecOverflow {
				return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 2)
			}
			return i
		}
	}
	if !vecOverflow {
		return -1
	}
	return d256ScalarFirstOverflow(s0, s1, s2, s3, v, vecEnd, 2)
}
