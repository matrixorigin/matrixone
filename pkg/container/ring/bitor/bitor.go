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
package bitor

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type BitOrRing struct {
	Typ        types.Type // vec value type
	Data       []byte
	Values     []uint64 // value
	NullCounts []int64  // group to record number of the null value
}

func NewBitOr(typ types.Type) *BitOrRing {
	return &BitOrRing{Typ: typ}
}

func (r *BitOrRing) String() string {
	return fmt.Sprintf("%v-%v", r.Values, r.NullCounts)
}

func (r *BitOrRing) Free(m *mheap.Mheap) {
	if r.Data != nil {
		mheap.Free(m, r.Data)
		r.Data = nil
		r.NullCounts = nil
		r.Values = nil
	}
}

func (r *BitOrRing) Count() int {
	return len(r.Values)
}

func (r *BitOrRing) Size() int {
	return cap(r.Data)
}

func (r *BitOrRing) Dup() ring.Ring {
	return &BitOrRing{
		Typ: r.Typ,
	}
}

func (r *BitOrRing) Type() types.Type {
	return r.Typ
}

func (r *BitOrRing) SetLength(n int) {
	r.Values = r.Values[:n]
	r.NullCounts = r.NullCounts[:n]
}

func (r *BitOrRing) Shrink(selectedIndexes []int64) {
	for i, idx := range selectedIndexes {
		r.Values[i] = r.Values[idx]
		r.NullCounts[i] = r.NullCounts[idx]
	}
	r.Values = r.Values[:len(selectedIndexes)]
	r.NullCounts = r.NullCounts[:len(selectedIndexes)]
}

func (r *BitOrRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *BitOrRing) Grow(m *mheap.Mheap) error {
	n := len(r.Values)

	if n == 0 {
		data, err := mheap.Alloc(m, 8*8)
		if err != nil {
			return err
		}
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
		r.NullCounts = make([]int64, 0, 8)
	} else if n+1 > cap(r.Values) {
		r.Data = r.Data[:n*8]
		data, err := mheap.Grow(m, r.Data, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
	}
	r.Values = r.Values[:n+1]
	r.Data = r.Data[:(n+1)*8]
	r.Values[n] = 0
	r.NullCounts = append(r.NullCounts, 0)
	return nil
}

func (r *BitOrRing) Grows(size int, m *mheap.Mheap) error {
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
		data, err := mheap.Grow(m, r.Data, int64((n+size)*8))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.Values = encoding.DecodeUint64Slice(data)
	}
	r.Values = r.Values[:n+size]
	r.Data = r.Data[:(n+size)*8]

	for i := 0; i < size; i++ {
		r.Values[n+i] = 0
	}

	for i := 0; i < size; i++ {
		r.NullCounts = append(r.NullCounts, 0)
	}
	return nil
}

// Fill update Uint64Ring by a row
func (r *BitOrRing) Fill(idxOfGroup, idxOfRow, cntOfRow int64, vec *vector.Vector) {
	var rowData uint64
	switch vec.Typ.Oid {
	case types.T_float32:
		rowData = uint64(vec.Col.([]float32)[idxOfRow])
	case types.T_float64:
		rowData = uint64(vec.Col.([]float64)[idxOfRow])
	case types.T_int8:
		rowData = uint64(vec.Col.([]int8)[idxOfRow])
	case types.T_int16:
		rowData = uint64(vec.Col.([]int16)[idxOfRow])
	case types.T_int32:
		rowData = uint64(vec.Col.([]int32)[idxOfRow])
	case types.T_int64:
		rowData = uint64(vec.Col.([]int64)[idxOfRow])
	case types.T_uint8:
		rowData = uint64(vec.Col.([]uint8)[idxOfRow])
	case types.T_uint16:
		rowData = uint64(vec.Col.([]uint16)[idxOfRow])
	case types.T_uint32:
		rowData = uint64(vec.Col.([]uint32)[idxOfRow])
	case types.T_uint64:
		rowData = uint64(vec.Col.([]uint64)[idxOfRow])
	}
	r.Values[idxOfGroup] |= rowData // update Values of this group

	if nulls.Contains(vec.Nsp, uint64(idxOfRow)) {
		r.NullCounts[idxOfGroup] += cntOfRow
	}
}

// BulkFill update ring by a whole vector
func (r *BitOrRing) BulkFill(idxOfGroup int64, cntOfRows []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vecCol := vec.Col.([]float32)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_float64:
		vecCol := vec.Col.([]float64)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int8:
		vecCol := vec.Col.([]int8)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int16:
		vecCol := vec.Col.([]int16)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int32:
		vecCol := vec.Col.([]int32)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int64:
		vecCol := vec.Col.([]int64)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint8:
		vecCol := vec.Col.([]uint8)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint16:
		vecCol := vec.Col.([]uint16)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint32:
		vecCol := vec.Col.([]uint32)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint64:
		vecCol := vec.Col.([]uint64)
		for _, val := range vecCol {
			r.Values[idxOfGroup] |= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCounts[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	}
}

func (r *BitOrRing) BatchFill(offset int64, os []uint8, vps []uint64, cntOfRows []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vecCol := vec.Col.([]float32)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_float64:
		vecCol := vec.Col.([]float64)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int8:
		vecCol := vec.Col.([]int8)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int16:
		vecCol := vec.Col.([]int16)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int32:
		vecCol := vec.Col.([]int32)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int64:
		vecCol := vec.Col.([]int64)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint8:
		vecCol := vec.Col.([]uint8)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint16:
		vecCol := vec.Col.([]uint16)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint32:
		vecCol := vec.Col.([]uint32)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint64:
		vecCol := vec.Col.([]uint64)
		for i := range os {
			r.Values[vps[i]-1] |= uint64(vecCol[offset+int64(i)])
		}
	}

	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(offset)+uint64(i)) {
				r.NullCounts[vps[i]-1] += cntOfRows[int64(i)+offset]
			}
		}
	}
}

func (r *BitOrRing) Add(ring interface{}, x, y int64) {
	ringData := ring.(*BitOrRing)
	r.Values[x] |= ringData.Values[y]
	r.NullCounts[x] += ringData.NullCounts[y]
}

func (r *BitOrRing) BatchAdd(ring interface{}, start int64, os []uint8, vps []uint64) {
	ringData := ring.(*BitOrRing)
	for i := range os {
		r.Values[vps[i]-1] |= ringData.Values[int64(i)+start]
		r.NullCounts[vps[i]-1] += ringData.NullCounts[int64(i)+start]
	}
}

func (r *BitOrRing) Mul(ring interface{}, x, y, z int64) {
	ringData := ring.(*BitOrRing)
	r.Values[x] |= ringData.Values[y]
	r.NullCounts[x] += ringData.NullCounts[y] * z
}

func (r *BitOrRing) Eval(cntOfRows []int64) *vector.Vector {
	defer func() {
		r.Data = nil
		r.NullCounts = nil
		r.Values = nil
	}()

	nsp := new(nulls.Nulls)

	for i, cnt := range cntOfRows {
		if cnt-r.NullCounts[i] == 0 {
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
