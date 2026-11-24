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

package nulls

// This file holds the Nulls operations outside the Gobra-verified core in
// nulls.go: raw pointers, serialization, string and array conversions,
// closures and generics. They use only the bitmap's own (trusted or
// verified) operations and must keep its invariant.

import (
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"golang.org/x/exp/constraints"
)

// Ownership. A Nulls owns its bitmap words through the bitmap's pool, the
// pool of the vector it belongs to. Free returns them. A Nulls without a
// pool is read-only: it references words it does not own (a decoded
// buffer, or the bitmap a view was taken from), and every write panics.

// Init gives an empty Nulls the pool it allocates from.
func (nsp *Nulls) Init(mp *mpool.MPool) {
	nsp.np.Init(mp)
}

// MPool returns the pool that owns nsp's words, or nil when read-only.
func (nsp *Nulls) MPool() *mpool.MPool {
	if nsp == nil {
		return nil
	}
	return nsp.np.MPool()
}

// IsReadOnly reports whether nsp has no pool.
func (nsp *Nulls) IsReadOnly() bool {
	return nsp == nil || nsp.np.IsReadOnly()
}

// Free returns nsp's words to its pool and leaves it empty with the same
// pool. A read-only Nulls only drops its reference.
func (nsp *Nulls) Free() {
	if nsp != nil {
		nsp.np.Free()
	}
}

// InitView makes nsp a read-only view of rows [start, end) of src: row r of
// nsp is row start+r of src. It shares src's words and allocates nothing;
// it is valid only while src is unchanged and alive.
func (nsp *Nulls) InitView(src *Nulls, start, end int) {
	var bm *bitmap.Bitmap
	if src != nil {
		bm = &src.np
	}
	nsp.np.InitView(bm, int64(start), int64(end))
}

// tryAddRows is addRows returning the allocation error instead of
// panicking.
func (nsp *Nulls) tryAddRows(rows []uint64) error {
	if len(rows) == 0 {
		return nil
	}
	maxRow := rows[0]
	for _, row := range rows[1:] {
		maxRow = max(maxRow, row)
	}
	if err := nsp.np.TryExpandWithSize(int(maxRow) + 1); err != nil {
		return err
	}
	nsp.np.AddMany(rows)
	return nil
}

// mustOK panics with err. The in-place writers (Add, Set, AddRange, Or)
// have no error result; they can fail only when growing the bitmap from its
// pool, or when the Nulls is read-only.
func mustOK(err error) {
	if err != nil {
		panic(err)
	}
}

func Ptr(nsp *Nulls) *uint64 {
	if nsp == nil {
		return nil
	}
	return nsp.GetBitmap().Ptr()
}

func String(nsp *Nulls) string {
	if nsp.IsEmpty() {
		return "[]"
	}
	return fmt.Sprintf("%v", nsp.ToArray())
}

func (nsp *Nulls) Show() ([]byte, error) {
	if nsp.IsEmpty() {
		return nil, nil
	}
	return nsp.np.Marshal(), nil
}

func (nsp *Nulls) MarshalSize() int {
	if nsp.IsEmpty() {
		return 0
	}
	return nsp.np.MarshalSize()
}

func (nsp *Nulls) MarshalTo(w io.Writer) error {
	if nsp.IsEmpty() {
		return nil
	}
	return nsp.np.MarshalTo(w)
}

// ShowV1 in version 1, bitmap is v1
func (nsp *Nulls) ShowV1() ([]byte, error) {
	if nsp.IsEmpty() {
		return nil, nil
	}
	return nsp.np.MarshalV1(), nil
}

// Read decodes data into nsp: a Nulls with a pool copies the words into
// it, one without references data. Empty data leaves nsp unchanged.
func (nsp *Nulls) Read(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return nsp.np.Unmarshal(data)
}

func (nsp *Nulls) ReadNoCopy(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopy(data)
	return nil
}

func (nsp *Nulls) ReadNoCopyV1(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	nsp.np.UnmarshalNoCopyV1(data)
	return nil
}

func (nsp *Nulls) ToArray() []uint64 {
	if nsp.IsEmpty() {
		return []uint64{}
	}
	return nsp.np.ToArray()
}

func (nsp *Nulls) ToI64Array() []int64 {
	if nsp.IsEmpty() {
		return []int64{}
	}
	return nsp.np.ToI64Array(nil)
}

func (nsp *Nulls) Foreach(fn func(uint64) bool) {
	if nsp.IsEmpty() {
		return
	}
	itr := nsp.np.IteratorValue()
	for itr.HasNext() {
		row := itr.Next()
		if !fn(row) {
			break
		}
	}
}

func (nsp *Nulls) String() string {
	if nsp.IsEmpty() {
		return fmt.Sprintf("%v", []uint64{})
	}
	return nsp.np.String()
}

func ToArray[T constraints.Integer](nsp *Nulls) []T {
	if nsp.IsEmpty() {
		return []T{}
	}
	ret := make([]T, 0, nsp.Count())
	nsp.Foreach(func(row uint64) bool {
		ret = append(ret, T(row))
		return true
	})
	return ret
}
