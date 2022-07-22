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

package max

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

var Decimal128Size = encoding.Decimal128Size

func NewDecimal128(typ types.Type) *Decimal128Ring {
	return &Decimal128Ring{
		Typ: typ,
	}
}

func (r *Decimal128Ring) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *Decimal128Ring) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *Decimal128Ring) Count() int {
	return len(r.Vs)
}

func (r *Decimal128Ring) Size() int {
	return cap(r.Da)
}

func (r *Decimal128Ring) Dup() ring.Ring {
	return &Decimal128Ring{
		Typ: r.Typ,
	}
}

func (r *Decimal128Ring) Type() types.Type {
	return r.Typ
}

func (r *Decimal128Ring) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
	r.Es = r.Es[:n]
}

func (r *Decimal128Ring) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
		r.Es[i] = r.Es[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
	r.Es = r.Es[:len(sels)]
}

func (r *Decimal128Ring) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *Decimal128Ring) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(8*Decimal128Size))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Es = make([]bool, 0, 8)
		r.Vs = encoding.DecodeDecimal128Slice(data)
	} else if n+1 >= cap(r.Vs) {
		r.Da = r.Da[:n*Decimal128Size]
		data, err := mheap.Grow(m, r.Da, int64((n+1)*Decimal128Size))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeDecimal128Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n+1)*16]
	r.Vs[n] = types.Decimal128Min
	r.Ns = append(r.Ns, 0)
	r.Es = append(r.Es, true)
	return nil
}

func (r *Decimal128Ring) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*Decimal128Size))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Es = make([]bool, 0, size)
		r.Vs = encoding.DecodeDecimal128Slice(data)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*Decimal128Size]
		data, err := mheap.Grow(m, r.Da, int64((n+size)*Decimal128Size))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeDecimal128Slice(data)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*16]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
		r.Es = append(r.Es, true)
		r.Vs[i+n] = types.Decimal128Min
	}
	return nil
}

func (r *Decimal128Ring) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if v := vec.Col.([]types.Decimal128)[sel]; r.Es[i] || types.CompareDecimal128Decimal128Aligned(v, r.Vs[i]) == 1 {
		r.Vs[i] = v
		r.Es[i] = false
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
	}
}

func (r *Decimal128Ring) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]types.Decimal128)
	for i := range os {
		j := vps[i] - 1
		if types.CompareDecimal128Decimal128Aligned(vs[int64(i)+start], r.Vs[j]) == 1 {
			r.Vs[j] = vs[int64(i)+start]
			r.Es[j] = false
		}
	}
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.Ns[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (r *Decimal128Ring) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]types.Decimal128)
	for _, v := range vs {
		if r.Es[i] || types.CompareDecimal128Decimal128Aligned(v, r.Vs[i]) == 1 {
			r.Vs[i] = v
			r.Es[i] = false
		}
	}
	if nulls.Any(vec.Nsp) {
		for j := range vs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.Ns[i] += zs[j]
			}
		}
	}
}

func (r *Decimal128Ring) Add(a interface{}, x, y int64) {
	ar := a.(*Decimal128Ring)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	if r.Es[x] || types.CompareDecimal128Decimal128Aligned(ar.Vs[y], r.Vs[x]) == 1 {
		r.Es[x] = false
		r.Vs[x] = ar.Vs[y]
	}
	r.Ns[x] += ar.Ns[y]
}

func (r *Decimal128Ring) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*Decimal128Ring)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	for i := range os {
		j := vps[i] - 1
		if r.Es[j] || types.CompareDecimal128Decimal128Aligned(ar.Vs[int64(i)+start], r.Vs[j]) == 1 {
			r.Es[j] = false
			r.Vs[j] = ar.Vs[int64(i)+start]
		}
		r.Ns[j] += ar.Ns[int64(i)+start]
	}
}

func (r *Decimal128Ring) Mul(a interface{}, x, y, z int64) {
	ar := a.(*Decimal128Ring)
	if r.Es[x] || types.CompareDecimal128Decimal128Aligned(ar.Vs[y], r.Vs[x]) == 1 {
		r.Es[x] = false
		r.Vs[x] = ar.Vs[y]
	}
	r.Ns[x] += ar.Ns[y] * z
}

func (r *Decimal128Ring) Eval(zs []int64) *vector.Vector {
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
		Typ:  r.Typ,
	}
}
