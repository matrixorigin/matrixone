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

package approxcd

import (
	"fmt"

	hll "github.com/axiomhq/hyperloglog"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewApproxCountDistinct(typ types.Type) *ApproxCountDistinctRing {
	return &ApproxCountDistinctRing{
		Typ: typ,
		Sk:  hll.New(),
	}
}

// impl Ring interface
var _ ring.Ring = (*ApproxCountDistinctRing)(nil)

func (r *ApproxCountDistinctRing) String() string {
	return fmt.Sprintf("hll-ring now estimate %v", r.Sk.Estimate())
}

func (r *ApproxCountDistinctRing) Free(m *mheap.Mheap) {
	r.Sk = nil
}

func (r *ApproxCountDistinctRing) Count() int {
	return 1
}

func (r *ApproxCountDistinctRing) Size() int {
	return 1 << 13
}

// Create a ring with same type using interface
func (r *ApproxCountDistinctRing) Dup() ring.Ring {
	return NewApproxCountDistinct(r.Typ)
}

func (r *ApproxCountDistinctRing) Type() types.Type {
	return r.Typ
}

func (r *ApproxCountDistinctRing) SetLength(n int) {}

func (r *ApproxCountDistinctRing) Shrink(sels []int64) {}

func (r *ApproxCountDistinctRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *ApproxCountDistinctRing) Grow(m *mheap.Mheap) error {
	return nil
}

func (r *ApproxCountDistinctRing) Grows(size int, m *mheap.Mheap) error {
	return nil
}

func (r *ApproxCountDistinctRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	panic("not support")
}

func (r *ApproxCountDistinctRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	panic("not support")
}

func (r *ApproxCountDistinctRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	iter := newBytesIterSolo(vec)
	iter.Foreach(func(data []byte) { r.Sk.Insert(data) })
}

func (r *ApproxCountDistinctRing) Add(a interface{}, x, y int64) {
	ar := a.(*ApproxCountDistinctRing)
	r.Sk.Merge(ar.Sk)
}

func (r *ApproxCountDistinctRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	panic("not support")
}

func (r *ApproxCountDistinctRing) Mul(a interface{}, x, y, z int64) {
	panic("not support")
}

func (r *ApproxCountDistinctRing) Eval(_ []int64) *vector.Vector {
	defer func() {
		r.Sk = nil
	}()
	res := make([]uint64, 1)
	res[0] = r.Sk.Estimate()
	return &vector.Vector{
		Nsp:  new(nulls.Nulls),
		Data: encoding.EncodeUint64Slice(res),
		Col:  res,
		Or:   false,
		Typ:  types.Type{Oid: types.T_uint64, Size: 8},
	}
}
