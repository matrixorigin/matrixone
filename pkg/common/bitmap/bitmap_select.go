// +gobra

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

package bitmap

import (
	"math/bits"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// Row-selection primitives (issue #33181). A selection is a bitmap over a
// batch's physical rows; these read and build one a word at a time.
//
// This file is part of the Gobra-verified core (see gobra/README.md). It is
// kept apart from bitmap.go so that verifying bitmap.go's members does not
// load it, and new members do not change existing proofs' context.
// gobra/verify.sh verifies this file's members with bitmap.go loaded.

// Word returns rows 64i..64i+63 of n as one word: bit b is row 64i+b. Rows
// outside [0, Len) read as clear. Callers that visit the set rows of a
// selection loop over words with bits.TrailingZeros64 rather than calling
// Contains or an iterator per row.
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 ==> bits.Bit(res, b) == HasS(n.ISeq(), n.Len(), 64*i+b)
// @ decreases
func (n *Bitmap) Word(i int) (res uint64) {
	return n.bitsFrom(64 * int64(i))
}

// NumWords returns the number of words that cover n's rows, ceil(Len/64).
// @ requires acc(n.Inv(), 1/4)
// @ ensures acc(n.Inv(), 1/4) && n.ISeq() == old(n.ISeq()) && n.Len() == old(n.Len())
// @ ensures res == int((n.Len()+63)/64)
// @ decreases
func (n *Bitmap) NumWords() (res int) {
	return int((n.Len() + 63) / 64)
}

// depositBits places the low bits of s, in order, at the set bits of w: the
// set bit b of w is kept when bit PcUpTo(w, b) of s, its rank among w's set
// bits, is set.
// @ ensures forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 ==> bits.Bit(res, b) == (bits.Bit(w, b) && bits.Bit(s, bits.PcUpTo(w, b)))
// @ decreases
func depositBits(w, s uint64) (res uint64) {
	// @ pcBitBelow(w)
	// @ pcMonoAll(w, 64)
	if s == ^uint64(0)>>uint(64-bits.OnesCount64(w)) {
		// Every set bit of w has a rank below Pc(w), and s has exactly the
		// low Pc(w) bits set.
		return w
	}
	// @ ghost s0 := s
	// @ ghost p := 0
	// @ ghost j := 0
	rest := w
	// @ invariant 0 <= p && p <= 64 && j == bits.PcUpTo(w, p)
	// @ invariant forall k int :: {bits.Bit(rest, k)} 0 <= k && k < 64 ==> bits.Bit(rest, k) == (bits.Bit(w, k) && k >= p)
	// @ invariant forall k int :: {bits.Bit(s, k)} 0 <= k && k < 64 ==> bits.Bit(s, k) == (k+j < 64 && bits.Bit(s0, k+j))
	// @ invariant forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 ==> bits.Bit(res, b) == (b < p && bits.Bit(w, b) && bits.Bit(s0, bits.PcUpTo(w, b)))
	// @ decreases 64 - p
	for rest != 0 {
		t := bits.TrailingZeros64(rest)
		// @ assert forall k int :: {bits.Bit(w, k)} p <= k && k < t ==> bits.Bit(rest, k) == bits.Bit(w, k) && !bits.Bit(rest, k)
		// @ assert bits.Bit(rest, t) && t >= p && bits.Bit(w, t)
		// @ pcFlat(w, p, t)
		// @ assert bits.PcUpTo(w, t+1) == j+1
		if s&(uint64(1)<<0) != 0 {
			res |= uint64(1) << uint(t)
		}
		s >>= 1
		rest &^= uint64(1) << uint(t)
		// @ p = t + 1
		// @ j = j + 1
	}
	// @ assert forall k int :: {bits.Bit(w, k)} 0 <= k && k < 64 && k >= p ==> !bits.Bit(rest, k)
	return res
}

// InitWithBools makes n a bitmap of len(values) rows in which row r is set
// when values[r] is true and r is not in nulls. nulls may be nil. n's words
// are reused when they are large enough.
// @ requires n.Raw() && acc(values, 1/2)
// @ requires nulls != nil ==> acc(nulls.Inv(), 1/4) && nulls != n
// @ ensures acc(values, 1/2) && forall q int :: {&values[q]} 0 <= q && q < len(values) ==> values[q] == old(values[q])
// @ ensures nulls != nil ==> acc(nulls.Inv(), 1/4)
// @ ensures err == nil ==> n.Inv() && n.Len() == int64(len(values))
// @ ensures err == nil ==> forall r int :: {n.Has(r)} n.Has(r) == (0 <= r && r < len(values) && values[r] && !(nulls != nil && nulls.Has(r)))
// @ ensures err != nil ==> n.Raw()
// @ decreases
func (n *Bitmap) InitWithBools(values []bool, nulls *Bitmap, mp *mpool.MPool) (err error) {
	if err = n.InitWithSize(int64(len(values)), mp); err != nil {
		return err
	}
	// @ ghost nb := int64(len(values))
	// @ ghost if nulls != nil {
	// @ 	nulls.rowFacts()
	// @ 	nulls.EmptyIsNone()
	// @ }
	// @ ghost var nseq seq[uint64]
	// @ ghost var nlen int64
	// @ ghost if nulls != nil {
	// @ 	nseq = nulls.ISeq()
	// @ 	nlen = nulls.Len()
	// @ }
	hasNulls := nulls != nil && !nulls.IsEmpty()
	count := 0
	words := (len(values) + 63) / 64
	// @ ghost if nulls != nil && !hasNulls {
	// @ 	assert nulls.IsEmpty()
	// @ 	assert forall r int :: {nulls.Has(r)} 0 <= r && r < int(nulls.Len()) ==> !nulls.Has(r)
	// @ 	assert forall r int :: {nulls.Has(r)} nulls.Has(r) == HasS(nulls.ISeq(), nulls.Len(), r)
	// @ 	assert forall r int :: {nulls.Has(r)} !(0 <= r && r < int(nulls.Len())) ==> !HasS(nulls.ISeq(), nulls.Len(), r)
	// @ 	assert forall r int :: {nulls.Has(r)} !nulls.Has(r)
	// @ }
	// @ unfold n.Inv()
	// @ invariant n.Mem() && n.NB() == nb && words == len(n.Seq()) && acc(values, 1/2) && (forall q int :: {&values[q]} 0 <= q && q < len(values) ==> values[q] == old(values[q]))
	// @ invariant nulls != nil ==> acc(nulls.Inv(), 1/4) && nulls.ISeq() == nseq && nulls.Len() == nlen
	// @ invariant nulls != nil ==> forall r int :: {nulls.Has(r)} nulls.Has(r) == HasS(nulls.ISeq(), nulls.Len(), r)
	// @ invariant !hasNulls ==> nulls == nil || forall r int :: {nulls.Has(r)} !nulls.Has(r)
	// @ invariant 0 <= i && i <= len(n.Seq()) && count == SumPc(n.Seq(), i)
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} 0 <= r && r < 64*i ==>
	// @ 	HasS(n.Seq(), n.NB(), r) == (r < len(values) && old(values[r]) && !(nulls != nil && nulls.Has(r)))
	// @ decreases len(n.Seq()) - i
	for i := 0; i < words; i++ {
		base := 64 * i
		end := base + 64
		if end > len(values) {
			end = len(values)
		}
		var word uint64
		// @ invariant acc(values, 1/2) && (forall q int :: {&values[q]} 0 <= q && q < len(values) ==> values[q] == old(values[q])) && base <= r && r <= end && end <= len(values) && end-base <= 64
		// @ invariant forall k int :: {bits.Bit(word, k)} 0 <= k && k < 64 ==> bits.Bit(word, k) == (k < r-base && old(values[base+k]))
		// @ decreases end - r
		for r := base; r < end; r++ {
			var b uint64
			if values[r] {
				b = 1
			}
			word |= b << uint(r-base)
		}
		if hasNulls {
			word &^= nulls.bitsFrom(int64(base))
		}
		// @ assert forall k int :: {bits.Bit(word, k)} 0 <= k && k < 64 ==>
		// @ 	bits.Bit(word, k) == (k < end-base && old(values[base+k]) && !(nulls != nil && nulls.Has(base+k)))
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		// @ assert TailClear(s0, n.numBits)
		// @ assert forall r int :: {HasS(s0, n.numBits, r)} 0 <= r && r < 64*i ==>
		// @ 	HasS(s0, n.numBits, r) == (r < len(values) && old(values[r]) && !(nulls != nil && nulls.Has(r)))
		n.data[i] = word
		count += bits.OnesCount64(word)
		// @ assert seq(n.data) == s0[i = word]
		// @ sumUpdate(s0, i, word, i)
		// @ assert SumPc(seq(n.data), i+1) == SumPc(seq(n.data), i) + bits.Pc(word)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} 0 <= r && r < 64*i ==> r/64 != i && HasS(seq(n.data), n.numBits, r) == HasS(s0, n.numBits, r)
		// @ assert forall r int :: {HasS(seq(n.data), n.numBits, r)} 64*i <= r && r < 64*i+64 ==> r/64 == i && r%64 == r-base &&
		// @ 	HasS(seq(n.data), n.numBits, r) == (r < len(values) && old(values[r]) && !(nulls != nil && nulls.Has(r)))
		// @ assert i == words-1 && nb%64 != 0 ==> end-base == int(nb%64)
		// @ ghost L := len(s0)
		// @ ghost if n.numBits%64 != 0 && L > 0 {
		// @ 	if i == L-1 {
		// @ 		assert seq(n.data)[L-1] == word
		// @ 		assert forall k int :: {bits.Bit(seq(n.data)[L-1], k)} int(n.numBits%64) <= k && k < 64 ==> !bits.Bit(word, k) && !bits.Bit(seq(n.data)[L-1], k)
		// @ 	} else {
		// @ 		assert seq(n.data)[L-1] == s0[L-1]
		// @ 	}
		// @ }
		// @ assert TailClear(seq(n.data), n.numBits)
		// @ fold n.Mem()
	}
	// @ unfold n.Mem()
	n.popCount = int64(count)
	// @ fold n.Mem()
	// @ fold n.Inv()
	return nil
}

// InitDeposit makes n a bitmap of base.Len() rows in which row r is set when
// r is in base and sel holds rank(r), the number of base rows below r. It
// maps a selection over base's rows (for example the live rows of a block)
// back to base's positions. Rows of sel at or beyond base.Count() are
// ignored. Neither base nor sel may be n.
// @ requires n.Raw() && acc(base.Inv(), 1/4) && acc(sel.Inv(), 1/4) && base != n && sel != n
// @ ensures acc(base.Inv(), 1/4) && base.ISeq() == old(base.ISeq()) && base.Len() == old(base.Len())
// @ ensures acc(sel.Inv(), 1/4) && sel.ISeq() == old(sel.ISeq()) && sel.Len() == old(sel.Len())
// @ ensures err == nil ==> n.Inv() && n.Len() == base.Len()
// @ ensures err == nil ==> forall r int :: {n.Has(r)} n.Has(r) == (base.Has(r) && sel.Has(CntS(base.ISeq(), base.Len(), r)))
// @ ensures err != nil ==> n.Raw()
// @ decreases
func (n *Bitmap) InitDeposit(base, sel *Bitmap, mp *mpool.MPool) (err error) {
	if err = n.InitWithSize(base.Len(), mp); err != nil {
		return err
	}
	// @ base.rowFacts()
	// @ sel.rowFacts()
	// @ ghost bseq := base.ISeq()
	// @ ghost blen := base.Len()
	// @ ghost sseq := sel.ISeq()
	// @ ghost slen := sel.Len()
	words := int((base.Len() + 63) / 64)
	var rank int64
	count := 0
	// @ unfold n.Inv()
	// @ invariant n.Mem() && n.NB() == blen && words == len(n.Seq()) && words == len(bseq)
	// @ invariant acc(base.Inv(), 1/4) && base.ISeq() == bseq && base.Len() == blen
	// @ invariant acc(sel.Inv(), 1/4) && sel.ISeq() == sseq && sel.Len() == slen
	// @ invariant forall r int :: {base.Has(r)} base.Has(r) == HasS(bseq, blen, r)
	// @ invariant forall r int :: {sel.Has(r)} sel.Has(r) == HasS(sseq, slen, r)
	// @ invariant TailClear(bseq, blen) && 0 <= blen && len(bseq) == int((blen+63)/64)
	// @ invariant forall j int :: {CntS(bseq, blen, 64*j)} 0 <= j && j <= len(bseq) ==> CntS(bseq, blen, 64*j) == SumPc(bseq, j)
	// @ invariant 0 <= i && i <= words && count == SumPc(n.Seq(), i) && int(rank) == SumPc(bseq, i)
	// @ invariant forall r int :: {HasS(n.Seq(), n.NB(), r)} 0 <= r && r < 64*i ==>
	// @ 	HasS(n.Seq(), n.NB(), r) == (HasS(bseq, blen, r) && HasS(sseq, slen, CntS(bseq, blen, r)))
	// @ decreases words - i
	for i := 0; i < words; i++ {
		w := base.Word(i)
		// @ assert forall b int :: {bits.Bit(bseq[i], b)} 0 <= b && b < 64 ==> bits.Bit(w, b) == HasS(bseq, blen, 64*i+b) && (64*i+b)/64 == i && (64*i+b)%64 == b
		// @ assert forall b int :: {bits.Bit(bseq[i], b)} 0 <= b && b < 64 && 64*i+b >= int(blen) ==> !bits.Bit(bseq[i], b)
		// @ bits.BitsEq(w, bseq[i])
		k := bits.OnesCount64(w)
		var word, s uint64
		if k != 0 {
			s = sel.bitsFrom(rank)
			if k < 64 {
				s &= ^uint64(0) >> uint(64-k)
			}
			word = depositBits(w, s)
		}
		// @ assert forall b int :: {bits.Bit(s, b)} 0 <= b && b < 64 ==> bits.Bit(s, b) == (b < bits.Pc(bseq[i]) && HasS(sseq, slen, int(rank)+b))
		// @ ghost if k == 0 {
		// @ 	pcBitBelow(w)
		// @ 	pcMonoAll(w, 64)
		// @ 	assert forall b int :: {bits.Bit(word, b)} 0 <= b && b < 64 ==> !bits.Bit(w, b)
		// @ }
		// @ depositWordRows(bseq, sseq, blen, slen, i, int(rank), s, word)
		// @ unfold n.Mem()
		// @ ghost s0 := seq(n.data)
		n.data[i] = word
		count += bits.OnesCount64(word)
		rank += int64(k)
		// @ ghost sn := seq(n.data)
		// @ depositStep(s0, sn, bseq, sseq, n.numBits, blen, slen, i, word)
		// @ assert SumPc(bseq, i+1) == SumPc(bseq, i)+bits.Pc(bseq[i])
		// @ fold n.Mem()
		// @ assert n.Seq() == sn && n.NB() == blen
	}
	// @ unfold n.Mem()
	n.popCount = int64(count)
	// @ fold n.Mem()
	// @ fold n.Inv()
	return nil
}

// KeepFirstN clears every set row of n after the first k, so that n keeps
// its k lowest set rows. It does nothing when n has at most k set rows.
// @ requires n.Inv() && 0 <= k
// @ ensures n.Inv() && n.Len() == old(n.Len())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == (old(n.Has(r)) && CntS(old(n.ISeq()), old(n.Len()), r) < k)
// @ decreases
func (n *Bitmap) KeepFirstN(k int) {
	if k < 0 {
		panic("negative row count")
	}
	// @ n.rowFacts()
	// @ n.CountIsRows()
	// @ ghost s := n.ISeq()
	// @ ghost nb := n.Len()
	if int64(k) >= int64(n.Count()) {
		// @ cntMonoAll(s, nb, int(nb))
		// @ assert n.Count() == CntS(s, nb, int(nb))
		// @ assert forall r int :: {n.Has(r)} n.Has(r) ==> 0 <= r && r < int(nb) && CntS(s, nb, r+1) == CntS(s, nb, r)+1 && CntS(s, nb, r+1) <= CntS(s, nb, int(nb))
		return
	}
	n.mustWrite()
	words := n.Size() / 8
	remain := k
	i := 0
	// @ invariant acc(n.Inv(), 1/2) && n.ISeq() == s && n.Len() == nb && words == len(s)
	// @ invariant 0 <= i && i <= words && 0 <= remain && remain == k-SumPc(s, i)
	// @ decreases words - i
	for i < words {
		c := bits.OnesCount64( /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[i])
		// @ assert c == bits.Pc(s[i]) && SumPc(s, i+1) == SumPc(s, i)+c
		if remain < c {
			break
		}
		remain -= c
		i++
	}
	// @ ghost if i == words {
	// @ 	cntWords(s, nb, words)
	// @ 	cntBeyond(s, nb, 64*words)
	// @ }
	// @ assert i < words && remain < bits.Pc(s[i])
	w := /*@ unfolding acc(n.Inv(), 1/2) in unfolding acc(n.Mem(), 1/2) in @*/ n.data[i]
	kept := keepLowBits(w, remain)
	// @ unfold n.Inv()
	// @ unfold n.Mem()
	// @ assert seq(n.data) == s && n.numBits == nb
	// @ assert TailClear(s, nb)
	n.data[i] = kept
	// @ assert seq(n.data) == s[i = kept]
	// @ ghost if nb%64 != 0 && i == words-1 {
	// @ 	assert forall b int :: {bits.Bit(seq(n.data)[words-1], b)} int(nb%64) <= b && b < 64 ==> !bits.Bit(s[i], b) && !bits.Bit(kept, b)
	// @ } else {
	// @ 	assert words > 0 && i != words-1 ==> seq(n.data)[words-1] == s[words-1]
	// @ }
	// @ assert TailClear(seq(n.data), n.numBits)
	// @ fold n.Mem()
	j := i + 1
	// @ invariant n.Mem() && n.NB() == nb && len(n.Seq()) == words && i+1 <= j && j <= words
	// @ invariant forall x int :: {n.Seq()[x]} 0 <= x && x < words ==> n.Seq()[x] == (x < i ? s[x] : (x == i ? kept : (x < j ? 0 : s[x])))
	// @ decreases words - j
	for j < words {
		// @ unfold n.Mem()
		// @ ghost s2 := seq(n.data)
		// @ assert TailClear(s2, n.numBits)
		n.data[j] = 0
		// @ assert seq(n.data) == s2[j = 0]
		// @ ghost if j != words-1 {
		// @ 	assert seq(n.data)[words-1] == s2[words-1]
		// @ }
		// @ assert TailClear(seq(n.data), n.numBits)
		// @ fold n.Mem()
		j++
	}
	// @ unfold n.Mem()
	// @ ghost sf := seq(n.data)
	// @ keepFirstRows(s, sf, nb, i, kept, k)
	n.popCount = int64(k)
	// @ fold n.Mem()
	// @ fold n.Inv()
	// @ n.rowFacts()
}

// keepLowBits returns the r lowest set bits of w.
// @ requires 0 <= r && r < bits.Pc(w)
// @ ensures forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 ==> bits.Bit(res, b) == (bits.Bit(w, b) && bits.PcUpTo(w, b) < r)
// @ ensures bits.Pc(res) == r
// @ decreases
func keepLowBits(w uint64, r int) (res uint64) {
	// @ ghost w0 := w
	// @ ghost p := 0
	// @ pcMonoAll(w0, 64)
	remain := r
	// @ invariant 0 <= p && p <= 64 && 0 <= remain && remain == r-bits.PcUpTo(w0, p)
	// @ invariant forall b int :: {bits.Bit(w, b)} 0 <= b && b < 64 ==> bits.Bit(w, b) == (bits.Bit(w0, b) && b >= p)
	// @ invariant forall b int :: {bits.Bit(res, b)} 0 <= b && b < 64 ==> bits.Bit(res, b) == (bits.Bit(w0, b) && b < p)
	// @ decreases remain
	for remain > 0 {
		// @ ghost if w == 0 {
		// @ 	assert forall b int :: {bits.Bit(w0, b)} p <= b && b < 64 ==> bits.Bit(w, b) == (bits.Bit(w0, b) && b >= p) && !bits.Bit(w, b) && !bits.Bit(w0, b)
		// @ 	pcFlat(w0, p, 64)
		// @ 	assert false
		// @ }
		t := bits.TrailingZeros64(w)
		// @ assert forall b int :: {bits.Bit(w0, b)} p <= b && b < t ==> bits.Bit(w, b) == (bits.Bit(w0, b) && b >= p) && !bits.Bit(w, b) && !bits.Bit(w0, b)
		// @ assert bits.Bit(w, t) && t >= p && bits.Bit(w0, t)
		// @ pcFlat(w0, p, t)
		// @ assert bits.PcUpTo(w0, t+1) == bits.PcUpTo(w0, p)+1
		res |= uint64(1) << uint(t)
		w &^= uint64(1) << uint(t)
		remain--
		// @ p = t + 1
	}
	// @ assert bits.PcUpTo(w0, p) == r
	// @ assert forall b int :: {bits.Bit(res, b)} p <= b && b < 64 ==> !bits.Bit(res, b)
	// @ pcFlat(res, p, 64)
	// @ pcSame(res, w0, p)
	return res
}
