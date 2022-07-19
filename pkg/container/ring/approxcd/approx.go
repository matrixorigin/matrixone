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
	}
}

// impl Ring interface
var _ ring.Ring = (*ApproxCountDistinctRing)(nil)

func (r *ApproxCountDistinctRing) String() string {
	return fmt.Sprintf("hll-ring(%d sketches)", len(r.Sk))
}

func (r *ApproxCountDistinctRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Sk = nil
	}
}

func (r *ApproxCountDistinctRing) Count() int {
	return len(r.Vs)
}

func (r *ApproxCountDistinctRing) Size() int {
	return cap(r.Da)
}

// Dup creates a ring with same type using interface
func (r *ApproxCountDistinctRing) Dup() ring.Ring {
	return NewApproxCountDistinct(r.Typ)
}

func (r *ApproxCountDistinctRing) Type() types.Type {
	return r.Typ
}

func (r *ApproxCountDistinctRing) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Sk = r.Sk[:n]
}

func (r *ApproxCountDistinctRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Sk[i] = r.Sk[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Sk = r.Sk[:len(sels)]
}

func (r *ApproxCountDistinctRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *ApproxCountDistinctRing) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 8*8)
		if err != nil {
			return err
		}
		r.Da = data
		r.Vs = encoding.DecodeUint64Slice(data)
	} else if n+1 >= cap(r.Vs) {
		r.Da = r.Da[:n]
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeUint64Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n+1)*8]
	r.Vs[n] = 0
	r.Sk = append(r.Sk, hll.New())
	return nil
}

func (r *ApproxCountDistinctRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Vs = encoding.DecodeUint64Slice(data)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeUint64Slice(data)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	for i := 0; i < size; i++ {
		r.Sk = append(r.Sk, hll.New())
	}
	return nil
}

func (r *ApproxCountDistinctRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	iter := newBytesIterSolo(vec)
	if sel_data := iter.Nth(sel); sel_data != nil {
		r.Sk[i].Insert(sel_data)
	}
}

func (r *ApproxCountDistinctRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	iter := newBytesIterSolo(vec)
	for i := range os {
		dest := vps[i] - 1
		sel := int64(i) + start
		if sel_data := iter.Nth(sel); sel_data != nil {
			r.Sk[dest].Insert(sel_data)
		}
	}
}

func (r *ApproxCountDistinctRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	iter := newBytesIterSolo(vec)
	iter.Foreach(func(data []byte) { r.Sk[i].Insert(data) })
}

func (r *ApproxCountDistinctRing) Add(a interface{}, x, y int64) {
	ar := a.(*ApproxCountDistinctRing)
	if err := r.Sk[x].Merge(ar.Sk[y]); err != nil {
		panic(err)
	}
}

func (r *ApproxCountDistinctRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*ApproxCountDistinctRing)
	for i := range os {
		dest := vps[i] - 1
		src := int64(i) + start
		if err := r.Sk[dest].Merge(ar.Sk[src]); err != nil {
			panic(err)
		}
	}
}

func (r *ApproxCountDistinctRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*ApproxCountDistinctRing)
	if err := r.Sk[x].Merge(ar.Sk[y]); err != nil {
		panic(err)
	}
}

func (r *ApproxCountDistinctRing) Eval(_ []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Sk = nil
	}()
	for i, sk := range r.Sk {
		r.Vs[i] = sk.Estimate()
	}
	return &vector.Vector{
		Nsp:  new(nulls.Nulls),
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  types.Type{Oid: types.T_uint64, Size: 8},
	}
}
