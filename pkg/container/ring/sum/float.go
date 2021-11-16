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

func NewFloat(typ types.Type) *FloatRing {
	return &FloatRing{Typ: typ}
}

func (r *FloatRing) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *FloatRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *FloatRing) Count() int {
	return len(r.Vs)
}

func (r *FloatRing) Size() int {
	return len(r.Vs) * 8
}

func (r *FloatRing) Dup() ring.Ring {
	return &FloatRing{
		Typ: r.Typ,
	}
}

func (r *FloatRing) Type() types.Type {
	return r.Typ
}

func (r *FloatRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *FloatRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *FloatRing) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 64)
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Vs = encoding.DecodeFloat64Slice(data)
	} else if n+1 >= cap(r.Vs) {
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeFloat64Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Vs[n] = 0
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *FloatRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		r.Vs[i] += float64(vec.Col.([]float32)[sel]) * float64(z)
	case types.T_float64:
		r.Vs[i] += float64(vec.Col.([]float64)[sel]) * float64(z)
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i]++
	}
}

func (r *FloatRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vs := vec.Col.([]float32)
		for j, v := range vs {
			r.Vs[i] += float64(v) * float64(zs[j])
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for j, v := range vs {
			r.Vs[i] += v * float64(zs[j])
		}
	}
	r.Ns[i] += int64(nulls.Length(vec.Nsp))
}

func (r *FloatRing) Add(a interface{}, x, y int64) {
	ar := a.(*FloatRing)
	r.Vs[x] += ar.Vs[y]
	r.Ns[x] += ar.Ns[y]
}

func (r *FloatRing) Mul(x, z int64) {
	r.Ns[x] *= z
	r.Vs[x] *= float64(z)
}

func (r *FloatRing) Eval(zs []int64) *vector.Vector {
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
		Typ:  types.Type{Oid: types.T_float64, Size: 8},
	}
}
