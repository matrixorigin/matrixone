// +gobra

// Copyright 2021 Matrix Origin
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

// Package nulls wraps a bitmap.Bitmap that marks the NULL rows of a column.
package nulls

// This file is the Gobra-verified core of Nulls; run gobra/verify.sh. The
// `// @` and `/*@ @*/` comments are Gobra specifications and ghost code; Go
// ignores them. The proof uses only the specifications of the bitmap
// package, whose core is verified separately.
//
// A non-nil *Nulls is valid when its bitmap keeps bitmap.Inv. Most functions
// accept a nil *Nulls and treat it as having no rows; the specifications say
// which ones need a non-nil argument. Has(r) means row r is NULL.

import (
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
)

type Nulls struct {
	np bitmap.Bitmap
}

// @ ghost
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ decreases
// @ pure func (nsp *Nulls) Has(r int) bool {
// @ 	return nsp != nil && nsp.np.Has(r)
// @ }

// @ ghost
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ ensures 0 <= res
// @ decreases
// @ pure func (nsp *Nulls) NLen() (res int64) {
// @ 	return nsp == nil ? 0 : nsp.np.Len()
// @ }

// countNull is the number of the first m entries of s that are NULL rows.
// @ ghost
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ requires 0 <= m && m <= len(s)
// @ decreases m
// @ pure func countNull(nsp *Nulls, s seq[int64], m int) int {
// @ 	return m == 0 ? 0 : countNull(nsp, s, m-1) + ((s[m-1] >= 0 && nsp.Has(int(s[m-1]))) ? 1 : 0)
// @ }

// @ ghost
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/4)
// @ requires 0 <= m && m <= len(s)
// @ requires forall r int :: {nsp.Has(r)} !nsp.Has(r)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/4) && nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
// @ ensures countNull(nsp, s, m) == 0
// @ decreases m
// @ func countNullZero(nsp *Nulls, s seq[int64], m int) {
// @ 	if m > 0 {
// @ 		assert s[m-1] >= 0 ==> !nsp.Has(int(s[m-1]))
// @ 		countNullZero(nsp, s, m-1)
// @ 	}
// @ }

// emptyIsNone turns IsEmpty into "no row is NULL".
// @ ghost
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/2)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/2)
// @ ensures nsp != nil ==> nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
// @ ensures old(nsp.IsEmpty()) ==> forall r int :: {nsp.Has(r)} !nsp.Has(r)
// @ decreases
// @ func (nsp *Nulls) emptyIsNone() {
// @ 	if nsp != nil {
// @ 		nsp.np.EmptyIsNone()
// @ 	}
// @ }

// Clone returns a deep copy, or nil for nil.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/2)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/2) && nsp.NLen() == old(nsp.NLen())
// @ ensures forall r int :: {nsp.Has(r)} nsp.Has(r) == old(nsp.Has(r))
// @ ensures (res == nil) == (nsp == nil)
// @ ensures res != nil ==> res.np.Inv() && res.NLen() == nsp.NLen()
// @ ensures res != nil ==> forall r int :: {res.Has(r)} res.Has(r) == nsp.Has(r)
// @ decreases
func (nsp *Nulls) Clone() (res *Nulls) {
	if nsp == nil {
		return nil
	}
	var n /*@@@*/ Nulls
	n.InitWith(nsp)
	return &n
}

// InitWith makes nsp a copy of n. It ignores the old contents of nsp.
// @ requires acc(nsp) && n != nil && acc(n.np.Inv(), 1/2)
// @ ensures nsp.np.Inv() && acc(n.np.Inv(), 1/2) && n.NLen() == old(n.NLen())
// @ ensures forall r int :: {n.Has(r)} n.Has(r) == old(n.Has(r))
// @ ensures nsp.NLen() == n.NLen() && forall r int :: {nsp.Has(r)} nsp.Has(r) == n.Has(r)
// @ decreases
func (nsp *Nulls) InitWith(n *Nulls) {
	nsp.np.InitWith(&n.np)
}

// InitWithSize makes nsp an all-clear bitmap of size rows. It ignores the old
// contents of nsp.
// @ requires acc(nsp) && 0 <= size
// @ ensures nsp.np.Inv() && nsp.NLen() == int64(size)
// @ ensures forall r int :: {nsp.Has(r)} !nsp.Has(r)
// @ decreases
func (nsp *Nulls) InitWithSize(size int) {
	nsp.np.InitWithSize(int64(size))
}

// @ requires 0 <= size
// @ ensures res != nil && res.np.Inv() && res.NLen() == int64(size)
// @ ensures forall r int :: {res.Has(r)} !res.Has(r)
// @ decreases
func NewWithSize(size int) (res *Nulls) {
	var n /*@@@*/ Nulls
	n.InitWithSize(size)
	return &n
}

// Reset makes nsp empty with length 0 and drops its storage. It ignores the
// old contents of nsp.
// @ requires acc(nsp)
// @ ensures nsp.np.Inv() && nsp.NLen() == 0
// @ ensures forall r int :: {nsp.Has(r)} !nsp.Has(r)
// @ decreases
func (nsp *Nulls) Reset() {
	nsp.np.Reset()
}

// Clear unsets every row and keeps the length.
// @ requires nsp != nil && nsp.np.Inv()
// @ ensures nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
// @ ensures forall r int :: {nsp.Has(r)} !nsp.Has(r)
// @ decreases
func (nsp *Nulls) Clear() {
	nsp.np.Clear()
}

// @ requires nsp != nil
// @ ensures res == &nsp.np
// @ decreases
// @ pure
func (nsp *Nulls) GetBitmap() (res *bitmap.Bitmap) {
	return &nsp.np
}

// Or performs union operation on Nulls nsp,m and store the result in r.
// r keeps its own rows, except that when nsp and m are both empty, r is
// reset to length 0. r may alias nsp or m.
// @ requires r != nil && r.np.Inv()
// @ requires nsp != nil && nsp != r ==> acc(nsp.np.Inv(), 1/2)
// @ requires m != nil && m != r ==> acc(m.np.Inv(), 1/2)
// @ ensures r.np.Inv()
// @ ensures nsp != nil && nsp != r ==> acc(nsp.np.Inv(), 1/2)
// @ ensures m != nil && m != r ==> acc(m.np.Inv(), 1/2)
// @ ensures nsp != r ==> forall x int :: {nsp.Has(x)} nsp.Has(x) == old(nsp.Has(x))
// @ ensures m != r ==> forall x int :: {m.Has(x)} m.Has(x) == old(m.Has(x))
// @ ensures old(nsp.IsEmpty()) && old(m.IsEmpty()) ==> r.NLen() == 0 && forall x int :: {r.Has(x)} !r.Has(x)
// @ ensures !(old(nsp.IsEmpty()) && old(m.IsEmpty())) ==>
// @ 	forall x int :: {r.Has(x)} r.Has(x) == (old(r.Has(x)) || old(nsp.Has(x)) || old(m.Has(x)))
// @ decreases
func Or(nsp, m, r *Nulls) {
	if nsp.IsEmpty() && m.IsEmpty() {
		// @ r.np.Release()
		r.Reset()
		return
	}

	if nsp != nil {
		orBitmapInto(r, nsp.GetBitmap())
	}
	if m != nil {
		orBitmapInto(r, m.GetBitmap())
	}
}

// orBitmapInto sets in dst every row set in src. src may be dst's own bitmap.
// @ requires dst != nil && dst.np.Inv()
// @ requires src != nil && src != &dst.np ==> acc(src.Inv(), 1/2)
// @ ensures dst.np.Inv() && dst.NLen() >= old(dst.NLen())
// @ ensures src != nil && src != &dst.np ==> acc(src.Inv(), 1/2)
// @ ensures src != nil && src != &dst.np ==> forall x int :: {src.Has(x)} src.Has(x) == old(src.Has(x))
// @ ensures forall x int :: {dst.Has(x)} dst.Has(x) == (old(dst.Has(x)) || (src != nil && src != &dst.np && old(src.Has(x))))
// @ decreases
func orBitmapInto(dst *Nulls, src *bitmap.Bitmap) {
	if src == nil || src == &dst.np {
		return
	}
	if src.IsEmpty() {
		// @ src.EmptyIsNone()
		return
	}
	dst.np.Or(src)
}

// Build makes nsp a bitmap of size rows with rows set. It ignores the old
// contents of nsp. The length grows past size if a row needs it.
// @ requires acc(nsp) && 0 <= size && (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2))
// @ ensures nsp.np.Inv() && (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2)) && nsp.NLen() >= int64(size)
// @ ensures forall r int :: {nsp.Has(r)} nsp.Has(r) == bitmap.InRowsU(seq(rows), len(rows), r)
// @ requires forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> 0 <= rows[k]
// @ decreases
func (nsp *Nulls) Build(size int, rows ...uint64) {
	nsp.InitWithSize(size)
	nsp.addRows(rows)
}

// @ requires 0 <= size && (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2))
// @ ensures res != nil && res.np.Inv() && (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2)) && res.NLen() >= int64(size)
// @ ensures forall r int :: {res.Has(r)} res.Has(r) == bitmap.InRowsU(seq(rows), len(rows), r)
// @ requires forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> 0 <= rows[k]
// @ decreases
func Build(size int, rows ...uint64) (res *Nulls) {
	var n /*@@@*/ Nulls
	n.InitWithSize(size)
	n.addRows(rows)
	return &n
}

// Size estimates the memory usage of the Nulls.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ ensures 0 <= res
// @ decreases
func Size(nsp *Nulls) (res int) {
	if nsp == nil {
		return 0
	}
	return nsp.np.Size()
}

// TryExpand grows nsp to at least size rows; new rows are clear.
// @ requires nsp != nil && nsp.np.Inv()
// @ ensures nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen()) && nsp.NLen() >= int64(size)
// @ ensures forall r int :: {nsp.Has(r)} nsp.Has(r) == old(nsp.Has(r))
// @ decreases
func TryExpand(nsp *Nulls, size int) {
	nsp.np.TryExpandWithSize(size)
}

// Contains returns true if the integer is contained in the Nulls
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ requires 0 <= row
// @ ensures res == nsp.Has(int(row))
// @ decreases
// @ pure
func (nsp *Nulls) Contains(row uint64) (res bool) {
	return nsp != nil && nsp.np.Contains(row)
}

// Add sets rows, growing the length to cover the largest one. rows need not
// be sorted.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen())
// @ ensures (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) || bitmap.InRowsU(seq(sels), len(sels), r))
// @ requires forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> 0 <= sels[k]
// @ decreases
func (nsp *Nulls) Add(sels ...uint64) {
	nsp.addRows(sels)
}

// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen())
// @ ensures (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) || bitmap.InRowsU(seq(sels), len(sels), r))
// @ requires forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> 0 <= sels[k]
// @ decreases
func (nsp *Nulls) addRows(sels []uint64) {
	if nsp == nil || len(sels) == 0 {
		return
	}
	maxRow := sels[0]
	// @ invariant (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2)) && 1 <= i && i <= len(sels)
	// @ invariant forall k int :: {sels[k]} 0 <= k && k < i ==> sels[k] <= maxRow
	// @ invariant forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> 0 <= sels[k]
	// @ decreases len(sels) - i
	for i := 1; i < len(sels); i++ {
		if sels[i] > maxRow {
			maxRow = sels[i]
		}
	}
	nsp.np.TryExpandWithSize(int(maxRow) + 1)
	// @ assert forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> sels[k] <= maxRow
	nsp.np.AddMany(sels)
}

// AddRange sets rows [start, end).
// @ requires nsp != nil ==> nsp.np.Inv()
// @ ensures nsp != nil ==> nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen())
// @ ensures nsp != nil ==> forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) || (int(start) <= r && r < int(end)))
// @ requires 0 <= start && 0 <= end
// @ decreases
func (nsp *Nulls) AddRange(start, end uint64) {
	if nsp != nil {
		nsp.np.TryExpandWithSize(int(end))
		nsp.np.AddRange(start, end)
	}
}

// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
// @ ensures (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) && !bitmap.InRowsU(seq(sels), len(sels), r))
// @ requires forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> 0 <= sels[k]
// @ decreases
func (nsp *Nulls) Del(sels ...uint64) {
	if nsp != nil {
		// @ invariant nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
		// @ invariant (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2)) && 0 <= i && i <= len(sels)
		// @ invariant forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> 0 <= sels[k]
		// @ invariant forall r int :: {nsp.np.Has(r)} nsp.np.Has(r) == (old(nsp.np.Has(r)) && !bitmap.InRowsU(seq(sels), i, r))
		// @ decreases len(sels) - i
		for i := 0; i < len(sels); i++ {
			nsp.np.Remove(sels[i])
		}
	}
}

// DelI64 is Del for int64 rows; negative rows are ignored.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2))
// @ ensures nsp != nil ==> nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
// @ ensures (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2))
// @ ensures nsp != nil ==> forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) && !InRowsI(seq(rows), len(rows), r))
// @ decreases
func (nsp *Nulls) DelI64(rows ...int64) {
	if nsp != nil {
		// @ invariant nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
		// @ invariant (forall k int :: {&rows[k]} 0 <= k && k < len(rows) ==> acc(&rows[k], 1/2)) && 0 <= i && i <= len(rows)
		// @ invariant forall r int :: {nsp.np.Has(r)} nsp.np.Has(r) == (old(nsp.np.Has(r)) && !InRowsI(seq(rows), i, r))
		// @ decreases len(rows) - i
		for i := 0; i < len(rows); i++ {
			if row := rows[i]; row >= 0 {
				nsp.np.Remove(uint64(row))
			}
		}
	}
}

// InRowsI holds when r is one of the first m entries of s.
// @ ghost
// @ requires 0 <= m && m <= len(s)
// @ decreases m
// @ pure func InRowsI(s seq[int64], m int, r int) bool {
// @ 	return m > 0 && (int(s[m-1]) == r || InRowsI(s, m-1, r))
// @ }

// FilterCount returns the number of entries of sels that are NULL rows.
// Negative entries never are.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/2)
// @ requires (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/2) && nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
// @ ensures (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures res == countNull(nsp, seq(sels), len(sels))
// @ decreases
func FilterCount(nsp *Nulls, sels []int64) (res int) {
	if nsp.IsEmpty() || len(sels) == 0 {
		// @ ghost if nsp.IsEmpty() {
		// @ 	nsp.emptyIsNone()
		// @ 	countNullZero(nsp, seq(sels), len(sels))
		// @ }
		return 0
	}
	var count int
	// @ invariant acc(nsp.np.Inv(), 1/2) && (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2)) && 0 <= i && i <= len(sels)
	// @ invariant nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
	// @ invariant count == countNull(nsp, seq(sels), i)
	// @ decreases len(sels) - i
	for i := 0; i < len(sels); i++ {
		if sel := sels[i]; sel >= 0 && nsp.np.Contains(uint64(sel)) {
			count++
		}
	}
	return count
}

// RemoveRange unsets rows [start, end).
// @ requires nsp != nil && nsp.np.Inv()
// @ ensures nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
// @ ensures forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) && !(int(start) <= r && r < int(end)))
// @ requires 0 <= start && 0 <= end
// @ decreases
func RemoveRange(nsp *Nulls, start, end uint64) {
	if nsp.np.IsEmpty() {
		// @ nsp.emptyIsNone()
		return
	}
	nsp.np.RemoveRange(start, end)
}

// Range makes b the rows [start, end) of nsp, shifted down by bias: row i of
// b is row i+bias of nsp. b gets length end-bias. If nsp has no rows, b is
// left unchanged.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/2)
// @ requires b != nil && b != nsp && b.np.Inv()
// @ requires 0 <= bias && bias <= start && bias <= end
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/2) && nsp.NLen() == old(nsp.NLen())
// @ ensures forall r int :: {nsp.Has(r)} nsp.Has(r) == old(nsp.Has(r))
// @ ensures b.np.Inv()
// @ ensures old(nsp.IsEmpty()) ==> b.NLen() == old(b.NLen()) && forall i int :: {b.Has(i)} b.Has(i) == old(b.Has(i))
// @ ensures !old(nsp.IsEmpty()) ==> b.NLen() == int64(end-bias)
// @ ensures !old(nsp.IsEmpty()) ==> forall i int :: {b.Has(i)} b.Has(i) == (int(start-bias) <= i && i < int(end-bias) && nsp.Has(i+int(bias)))
// @ decreases
func Range(nsp *Nulls, start, end, bias uint64, b *Nulls) {
	if nsp.IsEmpty() {
		return
	}

	// @ b.np.Release()
	b.np.InitWithSize(int64(end - bias))
	// @ ghost start0 := start
	// @ invariant acc(nsp.np.Inv(), 1/2) && b.np.Inv() && b.NLen() == int64(end-bias)
	// @ invariant nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
	// @ invariant forall r int :: {nsp.np.Has(r)} nsp.np.Has(r) == old(nsp.np.Has(r))
	// @ invariant start0 <= start && (start0 <= end ==> start <= end)
	// @ invariant forall i int :: {b.np.Has(i)} b.np.Has(i) == (int(start0-bias) <= i && i < int(start-bias) && nsp.np.Has(i+int(bias)))
	// @ decreases end - start
	for ; start < end; start++ {
		if nsp.Contains(start) {
			b.np.Add(start - bias)
		}
	}
}

// Filter keeps the rows named by sels.
//
// When negate is false, row i becomes old row sels[i]; sels may be in any
// order and entries outside the bitmap read as clear. The length becomes
// len(sels).
//
// When negate is true, sels must be strictly increasing and non-negative and
// names the rows to remove; the kept rows are packed at the front, in place.
// The length is unchanged.
//
// A Nulls with no rows is left unchanged.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ requires negate ==> bitmap.SortedRows(seq(sels))
// @ ensures nsp != nil ==> nsp.np.Inv()
// @ ensures (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures old(nsp.IsEmpty()) ==> nsp.NLen() == old(nsp.NLen()) && forall i int :: {nsp.Has(i)} nsp.Has(i) == old(nsp.Has(i))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> nsp.NLen() == int64(len(sels))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall i int :: {nsp.Has(i)} 0 <= i && i < len(sels) ==>
// @ 	nsp.Has(i) == old(sels[i] >= 0 && nsp.Has(int(sels[i])))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall i int :: {nsp.Has(i)} len(sels) <= i ==> !nsp.Has(i)
// @ ensures !old(nsp.IsEmpty()) && negate ==> nsp.NLen() == old(nsp.NLen())
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall s int :: {old(nsp.Has(s))} 0 <= s && s < int(old(nsp.NLen())) &&
// @ 	bitmap.Rank(seq(sels), int64(s+1)) == bitmap.Rank(seq(sels), int64(s)) ==>
// @ 	nsp.Has(s-bitmap.Rank(seq(sels), int64(s))) == old(nsp.Has(s))
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall i int :: {nsp.Has(i)} int(old(nsp.NLen()))-bitmap.Rank(seq(sels), old(nsp.NLen())) <= i ==> !nsp.Has(i)
// @ decreases
func Filter(nsp *Nulls, sels []int64, negate bool) {
	if nsp.IsEmpty() {
		return
	}
	if negate {
		nsp.np.RemapOrderedUnchecked(sels, true)
		return
	}

	var bm /*@@@*/ bitmap.Bitmap
	bm.InitWithSize(int64(len(sels)))
	// @ invariant nsp.np.Inv() && bm.Inv() && bm.Len() == int64(len(sels))
	// @ invariant (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2)) && 0 <= i && i <= len(sels)
	// @ invariant nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
	// @ invariant forall r int :: {nsp.np.Has(r)} nsp.np.Has(r) == old(nsp.np.Has(r))
	// @ invariant forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> sels[k] == old(sels[k])
	// @ invariant forall j int :: {bm.Has(j)} bm.Has(j) == (0 <= j && j < i && sels[j] >= 0 && nsp.np.Has(int(sels[j])))
	// @ decreases len(sels) - i
	for i := 0; i < len(sels); i++ {
		if sel := sels[i]; sel >= 0 && nsp.np.Contains(uint64(sel)) {
			bm.Add(uint64(i))
		}
	}
	nsp.np.Swap(&bm)
}

// FilterInPlaceOrdered is Filter for strictly increasing, non-negative sels,
// the contract of Vector.Shrink. It rewrites the bitmap in place.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2)) && bitmap.SortedRows(seq(sels))
// @ ensures nsp != nil ==> nsp.np.Inv()
// @ ensures (forall k int :: {&sels[k]} 0 <= k && k < len(sels) ==> acc(&sels[k], 1/2))
// @ ensures old(nsp.IsEmpty()) ==> nsp.NLen() == old(nsp.NLen()) && forall i int :: {nsp.Has(i)} nsp.Has(i) == old(nsp.Has(i))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> nsp.NLen() == int64(len(sels))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall i int :: {nsp.Has(i)} 0 <= i && i < len(sels) ==>
// @ 	nsp.Has(i) == old(sels[i] >= 0 && nsp.Has(int(sels[i])))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall i int :: {nsp.Has(i)} len(sels) <= i ==> !nsp.Has(i)
// @ ensures !old(nsp.IsEmpty()) && negate ==> nsp.NLen() == old(nsp.NLen())
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall s int :: {old(nsp.Has(s))} 0 <= s && s < int(old(nsp.NLen())) &&
// @ 	bitmap.Rank(seq(sels), int64(s+1)) == bitmap.Rank(seq(sels), int64(s)) ==>
// @ 	nsp.Has(s-bitmap.Rank(seq(sels), int64(s))) == old(nsp.Has(s))
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall i int :: {nsp.Has(i)} int(old(nsp.NLen()))-bitmap.Rank(seq(sels), old(nsp.NLen())) <= i ==> !nsp.Has(i)
// @ decreases
func FilterInPlaceOrdered(nsp *Nulls, sels []int64, negate bool) {
	if nsp.IsEmpty() {
		return
	}
	// @ assert forall i int :: {sels[i]} 0 <= i && i < len(sels) ==> seq(sels)[i] == sels[i] && sels[i] >= 0
	nsp.np.RemapOrderedUnchecked(sels, negate)
	if !negate {
		// RemapOrdered leaves the length at min(Len, len(sels)); Filter's
		// length is len(sels), and the extra rows are clear.
		nsp.np.TryExpandWithSize(len(sels))
	}
}

// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires sels != nil && acc(sels.Inv(), 1/2)
// @ ensures nsp != nil ==> nsp.np.Inv()
// @ ensures acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq())
// @ ensures old(nsp.IsEmpty()) ==> nsp.NLen() == old(nsp.NLen()) && forall i int :: {nsp.Has(i)} nsp.Has(i) == old(nsp.Has(i))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> nsp.NLen() == int64(old(sels.Count()))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall r int :: {sels.Has(r)} sels.Has(r) &&
// @ 	sels.CntBelow(r) < int(old(nsp.NLen())) && sels.CntBelow(r) < old(sels.Count()) ==>
// @ 	nsp.Has(sels.CntBelow(r)) == old(nsp.Has(r))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall i int :: {nsp.Has(i)} (int(old(nsp.NLen())) <= i || old(sels.Count()) <= i) ==> !nsp.Has(i)
// @ ensures !old(nsp.IsEmpty()) && negate ==> nsp.NLen() == old(nsp.NLen())
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall s int :: {old(nsp.Has(s))} 0 <= s && s < int(old(nsp.NLen())) && !sels.Has(s) ==>
// @ 	nsp.Has(s-bitmap.RankOff(sels.ISeq(), sels.Len(), 0, s)) == old(nsp.Has(s))
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall i int :: {nsp.Has(i)} int(old(nsp.NLen()))-bitmap.RankOff(sels.ISeq(), sels.Len(), 0, int(old(nsp.NLen()))) <= i ==> !nsp.Has(i)
// @ decreases
func FilterByMask(nsp *Nulls, sels *bitmap.Bitmap, negate bool) {
	FilterByMaskWithOffset(nsp, sels, negate, 0)
}

// FilterByMaskWithOffset is Filter for a selection bitmap: the selected rows
// are r+offset for the set rows r of sels, in increasing order. The selection
// bitmap is relative to a window of the owning vector, while the null bitmap
// remains in the full vector's row domain. When negate is false the length
// becomes sels.Count(); when negate is true it is unchanged. The bitmap is
// rewritten in place.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires sels != nil && acc(sels.Inv(), 1/2) && 0 <= offset
// @ ensures nsp != nil ==> nsp.np.Inv()
// @ ensures acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq())
// @ ensures old(nsp.IsEmpty()) ==> nsp.NLen() == old(nsp.NLen()) && forall i int :: {nsp.Has(i)} nsp.Has(i) == old(nsp.Has(i))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> nsp.NLen() == int64(old(sels.Count()))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall r int :: {sels.Has(r)} sels.Has(r) &&
// @ 	sels.CntBelow(r) < int(old(nsp.NLen())) && sels.CntBelow(r) < old(sels.Count()) ==>
// @ 	nsp.Has(sels.CntBelow(r)) == old(nsp.Has(r+int(offset)))
// @ ensures !old(nsp.IsEmpty()) && !negate ==> forall i int :: {nsp.Has(i)} (int(old(nsp.NLen())) <= i || old(sels.Count()) <= i) ==> !nsp.Has(i)
// @ ensures !old(nsp.IsEmpty()) && negate ==> nsp.NLen() == old(nsp.NLen())
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall s int :: {old(nsp.Has(s))} 0 <= s && s < int(old(nsp.NLen())) && !(s >= int(offset) && sels.Has(s-int(offset))) ==>
// @ 	nsp.Has(s-bitmap.RankOff(sels.ISeq(), sels.Len(), int(offset), s)) == old(nsp.Has(s))
// @ ensures !old(nsp.IsEmpty()) && negate ==> forall i int :: {nsp.Has(i)} int(old(nsp.NLen()))-bitmap.RankOff(sels.ISeq(), sels.Len(), int(offset), int(old(nsp.NLen()))) <= i ==> !nsp.Has(i)
// @ decreases
func FilterByMaskWithOffset(nsp *Nulls, sels *bitmap.Bitmap, negate bool, offset uint64) {
	if nsp.IsEmpty() {
		return
	}
	// @ sels.CountIsRows()
	nsp.np.RemapMaskOrderedWithOffset(sels, negate, offset)
	// @ sels.CountIsRows()
	if !negate {
		// The remap leaves the length at min(Len, sels.Count()); the
		// result has sels.Count() rows, and the extra rows are clear.
		nsp.np.TryExpandWithSize(sels.Count())
	}
}

// FilterByMaskInPlace is FilterByMask; it never needed scratch space.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires sels != nil && acc(sels.Inv(), 1/2)
// @ ensures nsp != nil ==> nsp.np.Inv()
// @ ensures acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq())
// @ decreases
func FilterByMaskInPlace(nsp *Nulls, sels *bitmap.Bitmap, negate bool) {
	FilterByMaskWithOffset(nsp, sels, negate, 0)
}

// FilterByMaskInPlaceWithOffset is FilterByMaskWithOffset.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ requires sels != nil && acc(sels.Inv(), 1/2) && 0 <= offset
// @ ensures nsp != nil ==> nsp.np.Inv()
// @ ensures acc(sels.Inv(), 1/2) && sels.Len() == old(sels.Len()) && sels.ISeq() == old(sels.ISeq())
// @ decreases
func FilterByMaskInPlaceWithOffset(nsp *Nulls, sels *bitmap.Bitmap, negate bool, offset uint64) {
	FilterByMaskWithOffset(nsp, sels, negate, offset)
}

// Any reports whether at least one row is set. It is exactly !IsEmpty().
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ decreases
// @ pure
func (nsp *Nulls) Any() bool {
	return !nsp.IsEmpty()
}

// IsEmpty reports whether no row is set. A nil *Nulls is empty. emptyIsNone
// proves it means no row is NULL.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), _)
// @ decreases
// @ pure
func (nsp *Nulls) IsEmpty() bool {
	return nsp == nil || nsp.np.IsEmpty()
}

// Set sets row, growing the length to cover it.
// @ requires nsp != nil && nsp.np.Inv()
// @ ensures nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen()) && nsp.NLen() > int64(row)
// @ ensures forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) || r == int(row))
// @ requires 0 <= row
// @ decreases
func (nsp *Nulls) Set(row uint64) {
	nsp.np.TryExpandWithSize(int(row) + 1)
	nsp.np.Add(row)
}

// Call it unset to match set.   Clear or reset are taken.
// @ requires nsp != nil ==> nsp.np.Inv()
// @ ensures nsp != nil ==> nsp.np.Inv() && nsp.NLen() == old(nsp.NLen())
// @ ensures nsp != nil ==> forall r int :: {nsp.Has(r)} nsp.Has(r) == (old(nsp.Has(r)) && r != int(row))
// @ requires 0 <= row
// @ decreases
func (nsp *Nulls) Unset(row uint64) {
	if nsp != nil {
		nsp.np.Remove(row)
	}
}

// Count returns the number of set rows.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/4)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/4)
// @ ensures res == (nsp == nil ? 0 : nsp.np.Count())
// @ decreases
func (nsp *Nulls) Count() (res int) {
	if nsp == nil {
		return 0
	}
	return nsp.np.Count()
}

// CountRange returns the number of NULL rows in [start, end).
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/2)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/2) && nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
// @ ensures nsp == nil || start >= end ==> res == 0
// @ ensures nsp != nil && start < end ==> res ==
// @ 	nsp.np.CntBelow(int(end) < int(nsp.np.Len()) ? int(end) : int(nsp.np.Len())) -
// @ 	nsp.np.CntBelow(int(start) < int(nsp.np.Len()) ? int(start) : int(nsp.np.Len()))
// @ requires 0 <= start && 0 <= end
// @ decreases
func (nsp *Nulls) CountRange(start, end uint64) (res int) {
	if nsp == nil || start >= end {
		return 0
	}
	return nsp.np.CountRange(start, end)
}

// Len returns the logical bitmap length.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/4)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/4) && nsp.np.Len() == old(nsp.np.Len()) && nsp.np.ISeq() == old(nsp.np.ISeq())
// @ ensures res == nsp.NLen()
// @ decreases
func (nsp *Nulls) Len() (res int64) {
	if nsp == nil {
		return 0
	}
	return nsp.np.Len()
}

// OrBitmap sets in nsp every row set in m. m may be nsp's own bitmap.
// @ requires nsp != nil && nsp.np.Inv()
// @ requires m != nil && m != &nsp.np ==> acc(m.Inv(), 1/2)
// @ ensures nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen())
// @ ensures m != nil && m != &nsp.np ==> acc(m.Inv(), 1/2)
// @ ensures forall x int :: {nsp.Has(x)} nsp.Has(x) == (old(nsp.Has(x)) || (m != nil && m != &nsp.np && old(m.Has(x))))
// @ decreases
func (nsp *Nulls) OrBitmap(m *bitmap.Bitmap) {
	orBitmapInto(nsp, m)
}

// Or the m Nulls into nsp. m may be nsp.
// @ requires nsp != nil && nsp.np.Inv()
// @ requires m != nil && m != nsp ==> acc(m.np.Inv(), 1/2)
// @ ensures nsp.np.Inv() && nsp.NLen() >= old(nsp.NLen())
// @ ensures m != nil && m != nsp ==> acc(m.np.Inv(), 1/2) && forall x int :: {m.Has(x)} m.Has(x) == old(m.Has(x))
// @ ensures forall x int :: {nsp.Has(x)} nsp.Has(x) == (old(nsp.Has(x)) || (m != nsp && old(m.Has(x))))
// @ decreases
func (nsp *Nulls) Or(m *Nulls) {
	if m != nil {
		orBitmapInto(nsp, m.GetBitmap())
	}
}

// IsSame reports whether nsp and m are the same pointer or hold the same data
// words. Equal words mean the same rows; the converse fails when the lengths
// need a different number of words, and a nil Nulls is never the same as a
// non-nil one.
// @ requires nsp != nil ==> acc(nsp.np.Inv(), 1/4)
// @ requires m != nil ==> acc(m.np.Inv(), 1/4)
// @ ensures nsp != nil ==> acc(nsp.np.Inv(), 1/4)
// @ ensures m != nil ==> acc(m.np.Inv(), 1/4)
// @ ensures res ==> forall r int :: {nsp.Has(r)} {m.Has(r)} nsp.Has(r) == m.Has(r)
// @ decreases
func (nsp *Nulls) IsSame(m *Nulls) (res bool) {
	if nsp == m {
		return true
	}
	if nsp == nil || m == nil {
		return false
	}

	return nsp.GetBitmap().IsSame(m.GetBitmap())
}
