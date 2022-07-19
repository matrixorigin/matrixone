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

package bitxor

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func NewBitXor(typ types.Type) *BitXorRing {
	return &BitXorRing{Typ: typ}
}

func (r *BitXorRing) String() string {
	return fmt.Sprintf("%v-%v", r.Values, r.NullCounts)
}

func (r *BitXorRing) Free(m *mheap.Mheap) {
	if r.Data != nil {
		mheap.Free(m, r.Data)
		r.Data = nil
		r.Values = nil
		r.NullCounts = nil
	}
}

func (r *BitXorRing) Count() int {
	return len(r.Values)
}

func (r *BitXorRing) Size() int {
	return cap(r.Data)
}

// Dup the error is because that BitXorRing hasn't implemented the Ring interface
func (r *BitXorRing) Dup() ring.Ring {
	return &BitXorRing{
		Typ: r.Typ,
	}
}

func (r *BitXorRing) Type() types.Type {
	return r.Typ
}

func (r *BitXorRing) SetLength(len int) {
	r.Values = r.Values[:len]
	r.NullCounts = r.NullCounts[:len]
}

// Shrink The sels must be sorted,otherwise the implement below is wrong
func (r *BitXorRing) Shrink(sels []int64) {
	for i, sel := range sels {
		r.Values[i] = r.Values[sel]
		r.NullCounts[i] = r.NullCounts[sel]
	}
	r.Values = r.Values[:len(sels)]
	r.NullCounts = r.NullCounts[:len(sels)]
}

// func (r *BitXorRing) Grow(m *mheap.Mheap) error {
// 	n := len(r.Values)
// 	if n == 0 {
// 		data, err := mheap.Alloc(m, 8*8)
// 		if err != nil {
// 			return err
// 		}
// 		r.Data = data
// 		r.Values = encoding.DecodeUint64Slice(data)
// 		r.NullCounts = make([]int64, 0, 8)
// 	} else if n+1 >= cap(r.Values) {
// 		r.Data = r.Data[:n*8]
// 		data, err := mheap.Grow(m, r.Data, int64(n+1)*8)
// 		if err != nil {
// 			return err
// 		}
// 		mheap.Free(m, r.Data)
// 		r.Data = data
// 		r.Values = encoding.DecodeUint64Slice(data)
// 	}
// 	r.Values = r.Values[:n+1]
// 	r.Values[n] = 0
// 	r.NullCounts = append(r.NullCounts, 0)
// 	return nil
// }

func (r *BitXorRing) Grow(m *mheap.Mheap) error {
	return r.Grows(1, m)
}

func (r *BitXorRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.Values)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
		r.NullCounts = make([]int64, 0, size)
	} else if n+size >= cap(r.Values) {
		r.Data = r.Data[:n*8]
		data, err := mheap.Grow(m, r.Data, int64(n+size)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
	}
	r.Data = r.Data[:(n+1)*8]
	r.Values = r.Values[:n+size]
	for i := 0; i < size; i++ {
		r.Values[n+i] = 0
		r.NullCounts = append(r.NullCounts, 0)
	}
	return nil
}

func (r BitXorRing) Fill(i int64, sel, z int64, vec *vector.Vector) {
	isOdd := 0
	if z%2 == 1 {
		isOdd = 1
	}
	switch vec.Typ.Oid {
	case types.T_int8:
		r.Values[i] ^= uint64(vec.Col.([]int8)[sel]) * uint64(isOdd)
	case types.T_int16:
		r.Values[i] ^= uint64(vec.Col.([]int16)[sel]) * uint64(isOdd)
	case types.T_int32:
		r.Values[i] ^= uint64(vec.Col.([]int32)[sel]) * uint64(isOdd)
	case types.T_int64:
		r.Values[i] ^= uint64(vec.Col.([]int64)[sel]) * uint64(isOdd)
	case types.T_uint8:
		r.Values[i] ^= uint64(vec.Col.([]uint8)[sel]) * uint64(isOdd)
	case types.T_uint16:
		r.Values[i] ^= uint64(vec.Col.([]uint16)[sel]) * uint64(isOdd)
	case types.T_uint32:
		r.Values[i] ^= uint64(vec.Col.([]uint32)[sel]) * uint64(isOdd)
	case types.T_uint64:
		r.Values[i] ^= uint64(vec.Col.([]uint64)[sel]) * uint64(isOdd)
	case types.T_float32:
		r.Values[i] ^= uint64(vec.Col.([]float32)[sel]) * uint64(isOdd)
	case types.T_float64:
		r.Values[i] ^= uint64(vec.Col.([]float64)[sel]) * uint64(isOdd)
	}
	if nulls.Contains(vec.Nsp, uint64(sel)) {
		r.NullCounts[i] += z
	}
}

func (r *BitXorRing) BatchFill(offset int64, os []uint8, vps []uint64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for i := range os {
			isOdd := 0
			if zs[int64(i)+offset]%2 == 1 {
				isOdd = 1
			}
			r.Values[vps[i]-1] ^= uint64(vs[int64(i)+offset]) * uint64(isOdd)
		}
	}

	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(offset)+uint64(i)) {
				r.NullCounts[vps[i]-1] += zs[int64(i)+offset]
			}
		}
	}
}

func (r *BitXorRing) BulkFill(i int64, zs []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_int8:
		vs := vec.Col.([]int8)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_int16:
		vs := vec.Col.([]int16)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_int32:
		vs := vec.Col.([]int32)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_int64:
		vs := vec.Col.([]int64)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_uint8:
		vs := vec.Col.([]uint8)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_uint16:
		vs := vec.Col.([]uint16)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_uint32:
		vs := vec.Col.([]uint32)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_uint64:
		vs := vec.Col.([]uint64)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_float32:
		vs := vec.Col.([]float32)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}
	case types.T_float64:
		vs := vec.Col.([]float64)
		for j, v := range vs {
			isOdd := 0
			if zs[j]%2 == 1 {
				isOdd = 1
			}
			r.Values[i] ^= uint64(v) * uint64(isOdd)
		}
		if nulls.Any(vec.Nsp) {
			for j := range vs {
				if nulls.Contains(vec.Nsp, uint64(j)) {
					r.NullCounts[i] += zs[j]
				}
			}
		}

	}
}

func (r *BitXorRing) Add(a interface{}, x, y int64) {
	ar := a.(*BitXorRing)
	r.Values[x] ^= ar.Values[y]
	r.NullCounts[x] += ar.NullCounts[y]
}

func (r *BitXorRing) BatchAdd(a interface{}, start int64, os []uint8, vps []uint64) {
	ar := a.(*BitXorRing)
	for i := range os {
		r.Values[vps[i]-1] ^= ar.Values[int64(i)+start]
		r.NullCounts[vps[i]-1] += ar.NullCounts[int64(i)+start]
	}
}

func (r *BitXorRing) Mul(a interface{}, x, y, z int64) {
	ar := a.(*BitXorRing)
	isOdd := 0
	if z%2 == 1 {
		isOdd = 1
	}
	r.Values[x] ^= ar.Values[y] * uint64(isOdd)
	r.NullCounts[x] += ar.NullCounts[y] * z
}

func (r *BitXorRing) Eval(zs []int64) *vector.Vector {
	defer func() {
		r.Data = nil
		r.Values = nil
		r.NullCounts = nil
	}()
	nsp := new(nulls.Nulls)
	for i, z := range zs {
		if n := z - r.NullCounts[i]; n == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}
	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Data,
		Col:  r.Values,
		Or:   false,
		Typ:  types.Type{Oid: types.T_uint64, Size: 8},
	}
}

// Shuffle not used now.
func (r *BitXorRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}
