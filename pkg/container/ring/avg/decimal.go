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

// the average aggregation function for decimal types(decimal64, decimal128)
// the avg function for decimal types has result of type decimal128, precision 38, and the result's scale is the same as the original column's scale

package avg

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

func NewDecimalRing(typ types.Type) *DecimalRing {
	return &DecimalRing{Typ: typ}
}

func (r *DecimalRing) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *DecimalRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *DecimalRing) Count() int {
	return len(r.Vs)
}

func (r *DecimalRing) Size() int {
	return cap(r.Da)
}

func (r *DecimalRing) Dup() ring.Ring {
	return &DecimalRing{
		Typ: r.Typ,
	}
}

func (r *DecimalRing) Type() types.Type {
	return r.Typ
}

func (r *DecimalRing) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
}

func (r *DecimalRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *DecimalRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *DecimalRing) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 64*2)
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
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
	r.Da = r.Da[:(n+1)*Decimal128Size]
	r.Vs[n] = types.Decimal128_Zero
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *DecimalRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*Decimal128Size))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
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
	r.Da = r.Da[:(n+size)*Decimal128Size]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
	}
	return nil
}

//
// XXX What the hell is this z?
//
func (r *DecimalRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_decimal64:
		tmp64 := types.Decimal64Int64Mul(vec.Col.([]types.Decimal64)[sel], z)
		tmp128 := types.Decimal128_FromDecimal64(tmp64)
		r.Vs[i] = types.Decimal128AddAligned(r.Vs[i], tmp128)
	case types.T_decimal128:
		tmp := types.Decimal128Int64Mul(vec.Col.([]types.Decimal128)[sel], z)
		r.Vs[i] = types.Decimal128AddAligned(r.Vs[i], tmp)
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
	}
}

func (r *DecimalRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_decimal64:
		vs := vec.Col.([]types.Decimal64)
		for i := range os {
			tmp := vs[int64(i)+start].MulInt64(zs[int64(i)+start])
			tmp128 := types.Decimal128_FromDecimal64(tmp)
			r.Vs[vps[i]-1] = types.Decimal128AddAligned(r.Vs[vps[i]-1], tmp128)
		}
	case types.T_decimal128:
		vs := vec.Col.([]types.Decimal128)
		for i := range os {
			tmp := types.Decimal128Int64Mul(vs[int64(i)+start], zs[int64(i)+start])
			r.Vs[vps[i]-1] = types.Decimal128AddAligned(r.Vs[vps[i]-1], tmp)
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

func (r *DecimalRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_decimal64:
		vs := vec.Col.([]types.Decimal64)
		for j, v := range vs {
			tmp := v.MulInt64(zs[j])
			r.Vs[i] = r.Vs[i].AddDecimal64(tmp)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.Ns[i] += zs[j]
				}
			}
		}
	case types.T_decimal128:
		vs := vec.Col.([]types.Decimal128)
		for j, v := range vs {
			tmp := types.Decimal128Int64Mul(v, zs[j])
			r.Vs[i] = types.Decimal128AddAligned(r.Vs[i], tmp)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.Ns[i] += zs[j]
				}
			}
		}
	}
}

func (r *DecimalRing) Add(a interface{}, x, y int64) {
	ar := a.(*DecimalRing)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	r.Vs[x] = types.Decimal128AddAligned(r.Vs[x], ar.Vs[y])
	r.Ns[x] += ar.Ns[y]
}

func (r *DecimalRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*DecimalRing)
	if r.Typ.Width == 0 && ar.Typ.Width != 0 {
		r.Typ = ar.Typ
	}
	for i := range os {
		r.Vs[vps[i]-1] = types.Decimal128AddAligned(r.Vs[vps[i]-1], ar.Vs[int64(i)+start])
		r.Ns[vps[i]-1] += ar.Ns[int64(i)+start]
	}
}

// Mul r[x] += a[y] * z
func (r *DecimalRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*DecimalRing)
	r.Ns[x] += ar.Ns[y] * z
	tmp := types.Decimal128Int64Mul(ar.Vs[y], z)
	r.Vs[x] = types.Decimal128AddAligned(r.Vs[x], tmp)
}

func (r *DecimalRing) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if n := z - r.Ns[i]; n == 0 {
			nulls.Add(nsp, uint64(i))
		} else {
			r.Vs[i] = types.Decimal128Int64Div(r.Vs[i], n)
		}
	}
	// the DecimalRing can have two types, but the result vector's type is Decimal128
	resultTyp := types.Type{Oid: types.T_decimal128, Size: 16, Width: 38, Scale: r.Typ.Scale}

	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  resultTyp,
	}
}
