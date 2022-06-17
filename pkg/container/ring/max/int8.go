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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewInt8(typ types.Type) *Int8Ring {
	return &Int8Ring{Typ: typ}
}

func (r *Int8Ring) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *Int8Ring) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *Int8Ring) Count() int {
	return len(r.Vs)
}

func (r *Int8Ring) Size() int {
	return cap(r.Da)
}

func (r *Int8Ring) Dup() ring.Ring {
	return &Int8Ring{
		Typ: r.Typ,
	}
}

func (r *Int8Ring) Type() types.Type {
	return r.Typ
}

func (r *Int8Ring) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
}

func (r *Int8Ring) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *Int8Ring) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *Int8Ring) Grow(m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, 8)
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, 8)
		r.Vs = encoding.DecodeInt8Slice(data)
	} else if n+1 >= cap(r.Vs) {
		r.Da = r.Da[:n]
		data, err := mheap.Grow(m, r.Da, int64(n+1))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt8Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n + 1)]
	r.Vs[n] = math.MinInt8
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *Int8Ring) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Vs = encoding.DecodeInt8Slice(data)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n]
		data, err := mheap.Grow(m, r.Da, int64(n+size))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt8Slice(data)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n + size)]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
		r.Vs[i+n] = math.MinInt8
	}
	return nil
}

func (r *Int8Ring) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if v := vec.Col.([]int8)[sel]; v > r.Vs[i] {
		r.Vs[i] = v
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
	}
}

func (r *Int8Ring) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]int8)
	for i := range os {
		j := vps[i] - 1
		if vs[int64(i)+start] > r.Vs[j] {
			r.Vs[j] = vs[int64(i)+start]
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

func (r *Int8Ring) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]int8)
	for _, v := range vs {
		if v > r.Vs[i] {
			r.Vs[i] = v
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

func (r *Int8Ring) Add(a interface{}, x, y int64) {
	ar := a.(*Int8Ring)
	if r.Vs[x] < ar.Vs[y] {
		r.Vs[x] = ar.Vs[y]
	}
	r.Ns[x] += ar.Ns[y]
}

func (r *Int8Ring) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*Int8Ring)
	for i := range os {
		j := vps[i] - 1
		if ar.Vs[int64(i)+start] > r.Vs[j] {
			r.Vs[j] = ar.Vs[int64(i)+start]
		}
		r.Ns[j] += ar.Ns[int64(i)+start]
	}
}

func (r *Int8Ring) Mul(a interface{}, x, y, z int64) {
	ar := a.(*Int8Ring)
	if ar.Vs[y] > r.Vs[x] {
		r.Vs[x] = ar.Vs[y]
	}
	r.Ns[x] += ar.Ns[y] * z
}

func (r *Int8Ring) Eval(zs []int64) *vector.Vector {
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
