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

package sum

import (
	"fmt"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/ring"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vm/mheap"
)

func NewInt(typ types.Type) *IntRing {
	return &IntRing{Typ: typ}
}

func (r *IntRing) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *IntRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *IntRing) Count() int {
	return len(r.Vs)
}

func (r *IntRing) Size() int {
	return cap(r.Da)
}

func (r *IntRing) Dup() ring.Ring {
	return &IntRing{
		Typ: r.Typ,
	}
}

func (r *IntRing) Type() types.Type {
	return r.Typ
}

func (r *IntRing) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
}

func (r *IntRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *IntRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *IntRing) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Vs = encoding.DecodeInt64Slice(data)
	} else if n+1 >= cap(r.Vs) {
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt64Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Vs[n] = 0
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *IntRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		r.Vs[i] += int64(vec.Col.([]int8)[sel]) * z
	case types.T_int16:
		r.Vs[i] += int64(vec.Col.([]int16)[sel]) * z
	case types.T_int32:
		r.Vs[i] += int64(vec.Col.([]int32)[sel]) * z
	case types.T_int64:
		r.Vs[i] += int64(vec.Col.([]int64)[sel]) * z
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i]++
	}
}

func (r *IntRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]uint8)
		for j, v := range vs {
			r.Vs[i] += int64(v) * zs[j]
		}
	case types.T_int16:
		vs := vec.Col.([]uint16)
		for j, v := range vs {
			r.Vs[i] += int64(v) * zs[j]
		}
	case types.T_int32:
		vs := vec.Col.([]uint32)
		for j, v := range vs {
			r.Vs[i] += int64(v) * zs[j]
		}
	case types.T_int64:
		vs := vec.Col.([]uint64)
		for j, v := range vs {
			r.Vs[i] += int64(v) * zs[j]
		}

	}
	r.Ns[i] += int64(nulls.Length(vec.Nsp))
}

func (r *IntRing) Add(a interface{}, x, y int64) {
	ar := a.(*IntRing)
	r.Vs[x] += ar.Vs[y]
	r.Ns[x] += ar.Ns[y]
}

func (r *IntRing) Mul(x, z int64) {
	r.Ns[x] *= z
	r.Vs[x] *= z
}

func (r *IntRing) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.Ns[i] == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  types.Type{Oid: types.T_int64, Size: 8},
	}
}
