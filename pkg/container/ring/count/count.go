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

package count

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewCount(typ types.Type) *CountRing {
	return &CountRing{Typ: typ}
}

func (r *CountRing) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *CountRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *CountRing) Count() int {
	return len(r.Vs)
}

func (r *CountRing) Size() int {
	return cap(r.Da)
}

func (r *CountRing) Dup() ring.Ring {
	return &CountRing{
		Typ: r.Typ,
	}
}

func (r *CountRing) Type() types.Type {
	return r.Typ
}

func (r *CountRing) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
}

func (r *CountRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
}

func (r *CountRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *CountRing) Grow(m *mheap.Mheap) error {
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
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt64Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n+1)*8]
	r.Vs[n] = 0
	r.Ns = append(r.Ns, 0)
	return nil
}

func (r *CountRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Vs = encoding.DecodeInt64Slice(data)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt64Slice(data)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
	}
	return nil
}

func (r *CountRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
	} else {
		r.Vs[i] += z
	}
}

func (r *CountRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.Ns[vps[i]-1] += zs[int64(i)+start]
			} else {
				r.Vs[vps[i]-1] += zs[int64(i)+start]
			}
		}
	} else {
		for i := range os {
			r.Vs[vps[i]-1] += zs[int64(i)+start]
		}
	}
}

func (r *CountRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		for j, z := range zs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.Ns[i] += z
			} else {
				r.Vs[i] += z
			}
		}
	} else {
		for _, z := range zs {
			r.Vs[i] += z
		}
	}
}

func (r *CountRing) Add(a interface{}, x, y int64) {
	ar := a.(*CountRing)
	r.Vs[x] += ar.Vs[y]
	r.Ns[x] += ar.Ns[y]
}

func (r *CountRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*CountRing)
	for i := range os {
		r.Vs[vps[i]-1] += ar.Vs[int64(i)+start]
		r.Ns[vps[i]-1] += ar.Ns[int64(i)+start]
	}
}

// Mul r[x] += a[y] * z
func (r *CountRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*CountRing)
	r.Vs[x] += ar.Vs[y] * z
	r.Ns[x] += ar.Ns[y] * z
}

func (r *CountRing) Eval(_ []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}()
	nsp := new(nulls.Nulls)
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  types.Type{Oid: types.T_int64, Size: 8},
	}
}

func NewDistinctCount(typ types.Type) *DistCountRing {
	return &DistCountRing{Typ: typ}
}

func (r *DistCountRing) String() string {
	return fmt.Sprintf("%v-%v", r.Vs, r.Ns)
}

func (r *DistCountRing) Free(m *mheap.Mheap) {
	if r.Da != nil {
		mheap.Free(m, r.Da)
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}
}

func (r *DistCountRing) Count() int {
	return len(r.Vs)
}

func (r *DistCountRing) Size() int {
	return cap(r.Da)
}

func (r *DistCountRing) Dup() ring.Ring {
	return &DistCountRing{
		Typ: r.Typ,
	}
}

func (r *DistCountRing) Type() types.Type {
	return r.Typ
}

func (r *DistCountRing) SetLength(n int) {
	r.Vs = r.Vs[:n]
	r.Ns = r.Ns[:n]
	r.Ms = r.Ms[:n]
}

func (r *DistCountRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Vs[i] = r.Vs[sel]
		r.Ns[i] = r.Ns[sel]
		r.Ms[i] = r.Ms[sel]
	}
	r.Vs = r.Vs[:len(sels)]
	r.Ns = r.Ns[:len(sels)]
	r.Ms = r.Ms[:len(sels)]
}

func (r *DistCountRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *DistCountRing) Grow(m *mheap.Mheap) error {
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
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt64Slice(data)
	}
	r.Vs = r.Vs[:n+1]
	r.Da = r.Da[:(n+1)*8]
	r.Vs[n] = 0
	r.Ns = append(r.Ns, 0)
	r.Ms = append(r.Ms, make(map[any]uint8))
	return nil
}

func (r *DistCountRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Vs)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Da = data
		r.Ns = make([]int64, 0, size)
		r.Vs = encoding.DecodeInt64Slice(data)
	} else if n+size >= cap(r.Vs) {
		r.Da = r.Da[:n*8]
		data, err := mheap.Grow(m, r.Da, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Da)
		r.Da = data
		r.Vs = encoding.DecodeInt64Slice(data)
	}
	r.Vs = r.Vs[:n+size]
	r.Da = r.Da[:(n+size)*8]
	for i := 0; i < size; i++ {
		r.Ns = append(r.Ns, 0)
		r.Ms = append(r.Ms, make(map[any]uint8))
	}
	return nil
}

func (r *DistCountRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.Ns[i] += z
	} else {
		if insertIntoMap(r.Ms[i], getValue(vec, sel)) {
			r.Vs[i] += z
		}
	}
}

func (r *DistCountRing) BatchFill(start int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(start)+uint64(i)) {
				r.Ns[vps[i]-1] += zs[int64(i)+start]
			} else {
				if insertIntoMap(r.Ms[vps[i]-1], getValue(vec, int64(i)+start)) {
					r.Vs[vps[i]-1] += zs[int64(i)+start]
				}
			}
		}
	} else {
		for i := range os {
			if insertIntoMap(r.Ms[vps[i]-1], getValue(vec, int64(i)+start)) {
				r.Vs[vps[i]-1] += zs[int64(i)+start]
			}
		}
	}
}

func (r *DistCountRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	if nulls.Any(vec.Nsp) {
		for j, z := range zs {
			if nulls.Contains(vec.Nsp, uint64(j)) {
				r.Ns[i] += z
			} else {
				if insertIntoMap(r.Ms[i], getValue(vec, int64(j))) {
					r.Vs[i] += z
				}
			}
		}
	} else {
		for j, z := range zs {
			if insertIntoMap(r.Ms[i], getValue(vec, int64(j))) {
				r.Vs[i] += z
			}
		}
	}
}

func (r *DistCountRing) Add(a interface{}, x, y int64) {
	ar := a.(*DistCountRing)
	if insertIntoMap(r.Ms[x], ar.Vs[y]) {
		r.Vs[x] += ar.Vs[y]
	}
	r.Ns[x] += ar.Ns[y]
}

func (r *DistCountRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*DistCountRing)
	for i := range os {
		if insertIntoMap(r.Ms[vps[i]-1], ar.Vs[int64(i)+start]) {
			r.Vs[vps[i]-1] += ar.Vs[int64(i)+start]
		}
		r.Ns[vps[i]-1] += ar.Ns[int64(i)+start]
	}
}

// Mul r[x] += a[y] * z
func (r *DistCountRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*DistCountRing)
	r.Vs[x] += ar.Vs[y] * z
	r.Ns[x] += ar.Ns[y] * z
}

func (r *DistCountRing) Eval(_ []int64) *vector.Vector {
	defer func() {
		r.Da = nil
		r.Vs = nil
		r.Ns = nil
	}()
	nsp := new(nulls.Nulls)
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Da,
		Col:  r.Vs,
		Or:   false,
		Typ:  types.Type{Oid: types.T_int64, Size: 8},
	}
}

func insertIntoMap(mp map[any]uint8, v any) bool {
	if _, ok := mp[v]; ok {
		return false
	}
	mp[v] = 0
	return true
}

func getValue(vec *vector.Vector, sel int64) any {
	switch vec.Typ.Oid {
	case types.T_bool:
		return vec.Col.([]bool)[sel]
	case types.T_int8:
		return vec.Col.([]int8)[sel]
	case types.T_int16:
		return vec.Col.([]int16)[sel]
	case types.T_int32:
		return vec.Col.([]int32)[sel]
	case types.T_int64:
		return vec.Col.([]int64)[sel]
	case types.T_uint8:
		return vec.Col.([]uint8)[sel]
	case types.T_uint16:
		return vec.Col.([]uint16)[sel]
	case types.T_uint32:
		return vec.Col.([]uint32)[sel]
	case types.T_uint64:
		return vec.Col.([]uint64)[sel]
	case types.T_float32:
		return vec.Col.([]float32)[sel]
	case types.T_float64:
		return vec.Col.([]float64)[sel]
	case types.T_date:
		return vec.Col.([]types.Date)[sel]
	case types.T_datetime:
		return vec.Col.([]types.Datetime)[sel]
	case types.T_timestamp:
		return vec.Col.([]types.Timestamp)[sel]
	case types.T_decimal64:
		return vec.Col.([]types.Decimal64)[sel]
	case types.T_decimal128:
		return vec.Col.([]types.Decimal128)[sel]
	case types.T_char, types.T_varchar, types.T_json:
		vs := vec.Col.(*types.Bytes)
		return string(vs.Get(sel))
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.SetLength", vec.Typ))
	}
}
