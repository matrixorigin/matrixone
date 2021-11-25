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
	"matrixone/pkg/vm/mheap"
	"unsafe"
)

func NewUint(typ types.Type) *UIntRing {
	return &UIntRing{Typ: typ}
}

func (r *UIntRing) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *UIntRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *UIntRing) Count() int {
	return len(r.Vs)
}

func (r *UIntRing) Size() int {
	return cap(r.Da)
}

func (r *UIntRing) Dup() ring.Ring {
	return &UIntRing{
		Typ: r.Typ,
	}
}

func (r *UIntRing) Type() types.Type {
	return r.Typ
}

func (r *UIntRing) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
}

func (r *UIntRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *UIntRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *UIntRing) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Vs = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), cap(data)/8)[:len(data)/8]
	} else if n+1 > cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = unsafe.Slice((*uint64)(unsafe.Pointer(&data[0])), cap(data)/8)[:len(data)/8]
	}
	r.Vs = r.Vs[:n+1]
	r.Vs[n] = 0
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *UIntRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_uint8:
		r.Vs[i] += uint64(vec.Col.([]uint8)[sel]) * uint64(z)
	case types.T_uint16:
		r.Vs[i] += uint64(vec.Col.([]uint16)[sel]) * uint64(z)
	case types.T_uint32:
		r.Vs[i] += uint64(vec.Col.([]uint32)[sel]) * uint64(z)
	case types.T_uint64:
		r.Vs[i] += uint64(vec.Col.([]uint64)[sel]) * uint64(z)
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i]++
	}
}

func (r *UIntRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for j, v := range vs {
			r.Vs[i] += uint64(v) * uint64(zs[j])
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for j, v := range vs {
			r.Vs[i] += uint64(v) * uint64(zs[j])
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for j, v := range vs {
			r.Vs[i] += uint64(v) * uint64(zs[j])
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for j, v := range vs {
			r.Vs[i] += uint64(v) * uint64(zs[j])
		}

	}
	r.Ns[i] += int64(nulls.Length(vec.Nsp))
}

func (r *UIntRing) Add(a interface{}, x, y int64) {
	ar := a.(*UIntRing)
	r.Vs[x] += ar.Vs[y]
	r.Ns[x] += ar.Ns[y]
}

func (r *UIntRing) Mul(x, z int64) {
	r.Ns[x] *= z
	r.Vs[x] *= uint64(z)
}

func (r *UIntRing) Eval(zs []int64) *vector.Vector {
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
		Typ:  types.Type{Oid: types.T_uint64, Size: 8},
	}
}
