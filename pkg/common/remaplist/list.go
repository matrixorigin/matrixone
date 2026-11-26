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

// Package remaplist holds List, the one type for row lists that reorder or
// repeat rows: sort permutations, gathers with repeats, and bucket splits. A
// selection that only keeps rows in order is a bitmap instead. See
// docs/design/20261124-row-selection-bitmap-remaplist.md, "Design: RemapList".
package remaplist

import (
	"math"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
)

// List maps output rows to source rows: output row i is source row Rows()[i].
// Rows may be in any order and may repeat.
//
// A List follows the bitmap ownership rule: its owner calls Init once with
// its pool, refills it per batch (Reset, Identity, Append, Repeat), and calls
// Free once. The rows live in one block from the pool, which grows only when
// a batch needs more room than any before it.
type List struct {
	rows []int64
	offs []int64 // bucket boundaries of the last BuildBuckets, else empty
	mp   *mpool.MPool
}

// Init sets the pool and reserves room for capacity rows.
func (r *List) Init(mp *mpool.MPool, capacity int) error {
	r.Free()
	r.mp = mp
	return r.reserve(capacity)
}

// Free returns the rows to the pool. The List keeps its pool and can be
// refilled. Free is idempotent.
func (r *List) Free() {
	if r.mp != nil && cap(r.rows) > 0 {
		r.mp.Free(int64Bytes(r.rows[:1]))
	}
	r.rows = nil
	r.freeOffs()
}

// Reset sets the length to zero and keeps the capacity.
func (r *List) Reset() {
	r.rows = r.rows[:0]
	r.offs = r.offs[:0]
}

// Len is the number of output rows.
func (r *List) Len() int {
	return len(r.rows)
}

// Rows returns the source row of every output row. The slice is owned by the
// List and stays valid until the next call that can grow it (Identity,
// Append, Repeat, Init) or Free. Callers may reorder it in place, which is how
// a sort builds a permutation from Identity.
func (r *List) Rows() []int64 {
	return r.rows
}

// Identity sets the List to 0, 1, ..., n-1: the starting point for a sort.
func (r *List) Identity(n int) error {
	if n < 0 {
		return moerr.NewInternalErrorNoCtxf("remaplist: Identity(%d)", n)
	}
	r.rows = r.rows[:0]
	r.offs = r.offs[:0]
	if err := r.reserve(n); err != nil {
		return err
	}
	r.rows = r.rows[:n]
	for i := range r.rows {
		r.rows[i] = int64(i)
	}
	return nil
}

// Append adds one output row taken from source row row.
func (r *List) Append(row int64) error {
	if err := r.reserve(len(r.rows) + 1); err != nil {
		return err
	}
	r.rows = append(r.rows, row)
	return nil
}

// Repeat adds n output rows, each taken from source row row.
func (r *List) Repeat(row int64, n int) error {
	if n <= 0 {
		return nil
	}
	if err := r.reserve(len(r.rows) + n); err != nil {
		return err
	}
	at := len(r.rows)
	r.rows = r.rows[:at+n]
	for i := at; i < at+n; i++ {
		r.rows[i] = row
	}
	return nil
}

// reserve grows the block so that it holds at least n rows. The rows already
// in the List are kept.
func (r *List) reserve(n int) error {
	if n <= cap(r.rows) {
		return nil
	}
	if r.mp == nil {
		return errNoInit
	}
	if n > math.MaxInt/8 {
		return moerr.NewInternalErrorNoCtxf("remaplist: %d rows do not fit in memory", n)
	}
	// Grow geometrically so that a run of Appends is amortized.
	if c := 2 * cap(r.rows); c > n {
		n = min(c, math.MaxInt/8)
	}
	b, err := r.mp.Alloc(n*8, true)
	if err != nil {
		return err
	}
	rows := unsafe.Slice((*int64)(unsafe.Pointer(unsafe.SliceData(b))), n)[:len(r.rows)]
	copy(rows, r.rows)
	r.Free()
	r.rows = rows
	return nil
}

var errNoInit = moerr.NewInternalErrorNoCtx("remaplist: List used before Init")

func int64Bytes(rows []int64) []byte {
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(rows))), cap(rows)*8)
}
