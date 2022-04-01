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

package bit_or

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewUInt8(typ types.Type) *UInt8Ring {
	return &UInt8Ring{Typ: typ}
}

func (r *UInt8Ring) String() string {
	return fmt.Sprintf("%v-%v", r.Values, r.NullCounts)
}

func (r *UInt8Ring) Free(m *mheap.Mheap) {
	if r.Data != nil {
		mheap.Free(m, r.Data)
		r.Data = nil
		r.Values = nil
		r.NullCounts = nil
	}
}

func (r *UInt8Ring) Count() int {
	return len(r.Values)
}

func (r *UInt8Ring) Size() int {
	return cap(r.Data)
}

func (r *UInt8Ring) Dup() ring.Ring {
	return &UInt8Ring{
		Typ: r.Typ,
	}
}

func (r *UInt8Ring) Type() types.Type {
	return r.Typ
}

func (r *UInt8Ring) SetLength(n int) {
	r.Values = r.Values[:n]
	r.NullCounts = r.NullCounts[:n]
}

func (r *UInt8Ring) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Values[i] = r.Values[sel]
		r.NullCounts[i] = r.NullCounts[sel]
	}
	r.Values = r.Values[:len(sels)]
	r.NullCounts = r.NullCounts[:len(sels)]
}

func (r *UInt8Ring) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *UInt8Ring) Grow(m *mheap.Mheap) error {
	n := len(r.Values)
	if n == 0 {
		data, err := mheap.Alloc(m, 8)
		if err != nil {
			return err
		}
		r.Data = data
		r.NullCounts = make([]int64, 0, 8)
		r.Values = encoding.DecodeUint8Slice(data)
	} else if n+1 >= cap(r.Values) {
		r.Data = r.Data[:n]
		data, err := mheap.Grow(m, r.Data, int64(n+1))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint8Slice(data)
	}
	r.Values = r.Values[:n+1]
	r.Values[n] = 0
	r.NullCounts = append(r.NullCounts, 0)
	return nil
}

func (r *UInt8Ring) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Values)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size))
		if err != nil {
			return err
		}
		r.Data = data
		r.NullCounts = make([]int64, 0, size)
		r.Values = encoding.DecodeUint8Slice(data)
	} else if n+size >= cap(r.Values) {
		r.Data = r.Data[:n]
		data, err := mheap.Grow(m, r.Data, int64(n+size))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint8Slice(data)
	}
	r.Values = r.Values[:n+size]
	for i := 0; i < size; i++ {
		r.NullCounts = append(r.NullCounts, 0)
	}
	return nil
}

func (r *UInt8Ring) Fill(i int64, sel, z int64, vec *vector.Vector) {
	v := vec.Col.([]uint8)[sel]
	r.Values[i] |= v
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.NullCounts[i] += z
	}
}

func (r *UInt8Ring) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]uint8)
	for i := range os {
		j := vps[i] - 1
		r.Values[j] |= vs[int64(i)+start]
	}
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.NullCounts[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (r *UInt8Ring) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	vs := vec.Col.([]uint8)
	for _, v := range vs {
			r.Values[i] |= v
	}
	if nulls.Any(vec.Nsp) {
		for j := range vs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.NullCounts[i] += zs[j]
			}
		}
	}
}

func (r *UInt8Ring) Add(a interface{}, x, y int64) {
	ar := a.(*UInt8Ring)
	r.Values[x] |= ar.Values[y]
	r.NullCounts[x] += ar.NullCounts[y]
}

func (r *UInt8Ring) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*UInt8Ring)
	for i := range os {
		j := vps[i] - 1
		r.Values[j] |= ar.Values[int64(i)+start]
		r.NullCounts[j] += ar.NullCounts[int64(i)+start]
	}
}

func (r *UInt8Ring) Mul(a interface{}, x, y, z int64) {
	ar := a.(*UInt8Ring)
	r.Values[x] |= ar.Values[y]
	r.NullCounts[x] += ar.NullCounts[y] * z
}

func (r *UInt8Ring) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Data = nil
		r.Values = nil
		r.NullCounts = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if z-r.NullCounts[i] == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Data,
		Col:  r.Values,
		Or:   false,
		Typ:  r.Typ,
	}
}
