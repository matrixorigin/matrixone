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
package bitand

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/ring"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

type NumericRing struct {
	Typ types.Type // vec value type
	// some attributes for pre-computation
	Data         []byte
	BitAndResult []uint64 // group's pre-computation result of bit_and and has the same address as Data field
	// Values  [][]int64 // value of groups
	NullCnt []int64 // group to record number of the null value
}

func NewNumeric(typ types.Type) *NumericRing {
	return &NumericRing{Typ: typ}
}

func (r *NumericRing) String() string {
	return fmt.Sprintf("%v-%v", r.BitAndResult, r.NullCnt)
}

func (r *NumericRing) Free(m *mheap.Mheap) {
	if r.Data != nil {
		mheap.Free(m, r.Data)
		r.Data = nil
		r.NullCnt = nil
		r.BitAndResult = nil
	}
}

func (r *NumericRing) Count() int {
	return len(r.BitAndResult)
}

func (r *NumericRing) Size() int {
	return cap(r.Data)
}

func (r *NumericRing) Dup() ring.Ring {
	return &NumericRing{
		Typ: r.Typ,
	}
}

func (r *NumericRing) Type() types.Type {
	return r.Typ
}

func (r *NumericRing) SetLength(n int) {
	r.BitAndResult = r.BitAndResult[:n]
	r.NullCnt = r.NullCnt[:n]
}

func (r *NumericRing) Shrink(selectedIndexes []int64) {
	for i, idx := range selectedIndexes {
		r.BitAndResult[i] = r.BitAndResult[idx]
		r.NullCnt[i] = r.NullCnt[idx]
	}
	r.BitAndResult = r.BitAndResult[:len(selectedIndexes)]
	r.NullCnt = r.NullCnt[:len(selectedIndexes)]
}

func (r *NumericRing) Shuffle(_ []int64, _ *mheap.Mheap) error {
	return nil
}

func (r *NumericRing) Grow(m *mheap.Mheap) error {
	n := len(r.BitAndResult)

	if n == 0 {
		data, err := mheap.Alloc(m, 8*8)
		if err != nil {
			return err
		}
		r.Data = data
		r.BitAndResult = encoding.DecodeUint64Slice(data)
		r.NullCnt = make([]int64, 0, 8)
	} else if n+1 > cap(r.BitAndResult) {
		r.Data = r.Data[:n*8]
		data, err := mheap.Grow(m, r.Data, int64(n+1)*8)
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.BitAndResult = encoding.DecodeUint64Slice(data)
	}
	r.BitAndResult = r.BitAndResult[:n+1]
	r.Data = r.Data[:(n+1)*8]
	r.BitAndResult[n] = ^uint64(0) // UINT64_MAX
	r.NullCnt = append(r.NullCnt, 0)
	return nil
}

func (r *NumericRing) Grows(size int, m *mheap.Mheap) error {
	n := len(r.BitAndResult)
	if n == 0 {
		data, err := mheap.Alloc(m, int64(size*8))
		if err != nil {
			return err
		}
		r.Data = data
		r.BitAndResult = encoding.DecodeUint64Slice(data)
		r.NullCnt = make([]int64, 0, size)
	} else if n+size >= cap(r.BitAndResult) {
		r.Data = r.Data[:n*8]
		data, err := mheap.Grow(m, r.Data, int64((n+size)*8))
		if err != nil {
			return err
		}
		mheap.Free(m, r.Data)
		r.Data = data
		r.BitAndResult = encoding.DecodeUint64Slice(data)
	}
	r.BitAndResult = r.BitAndResult[:n+size]
	r.Data = r.Data[:(n+size)*8]

	// set all empty to ^uint64(0)
	for i := 0; i < size; i++ {
		r.BitAndResult[n+i] = ^uint64(0)
	}

	for i := 0; i < size; i++ {
		r.NullCnt = append(r.NullCnt, 0)
	}
	return nil
}

// Fill update NumericRing by a row
func (r *NumericRing) Fill(idxOfGroup, idxOfRow, cntOfRow int64, vec *vector.Vector) {
	var rowData uint64
	switch vec.Typ.Oid {
	// case types.T_char, types.T_varchar: // currently not support
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
	r.BitAndResult[idxOfGroup] &= rowData // update BitAndResult of this group

	if nulls.Contains(vec.Nsp, uint64(idxOfRow)) {
		r.NullCnt[idxOfGroup] += cntOfRow
	}
}

// BulkFill update ring by a whole vector
func (r *NumericRing) BulkFill(idxOfGroup int64, cntOfRows []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vecCol := vec.Col.([]float32)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_float64:
		vecCol := vec.Col.([]float64)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int8:
		vecCol := vec.Col.([]int8)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int16:
		vecCol := vec.Col.([]int16)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int32:
		vecCol := vec.Col.([]int32)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_int64:
		vecCol := vec.Col.([]int64)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint8:
		vecCol := vec.Col.([]uint8)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint16:
		vecCol := vec.Col.([]uint16)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint32:
		vecCol := vec.Col.([]uint32)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	case types.T_uint64:
		vecCol := vec.Col.([]uint64)
		for _, val := range vecCol {
			r.BitAndResult[idxOfGroup] &= uint64(val)
		}
		if nulls.Any(vec.Nsp) {
			for i := range vecCol {
				if nulls.Contains(vec.Nsp, uint64(i)) {
					r.NullCnt[idxOfGroup] += cntOfRows[i]
				}
			}
		}
	}
}

func (r *NumericRing) BatchFill(offset int64, os []uint8, vps []uint64, cntOfRows []int64, vec *vector.Vector) {
	switch vec.Typ.Oid {
	case types.T_float32:
		vecCol := vec.Col.([]float32)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_float64:
		vecCol := vec.Col.([]float64)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int8:
		vecCol := vec.Col.([]int8)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int16:
		vecCol := vec.Col.([]int16)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int32:
		vecCol := vec.Col.([]int32)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_int64:
		vecCol := vec.Col.([]int64)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint8:
		vecCol := vec.Col.([]uint8)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint16:
		vecCol := vec.Col.([]uint16)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint32:
		vecCol := vec.Col.([]uint32)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	case types.T_uint64:
		vecCol := vec.Col.([]uint64)
		for i := range os {
			r.BitAndResult[vps[i]-1] &= uint64(vecCol[offset+int64(i)])
		}
	}

	if nulls.Any(vec.Nsp) {
		for i := range os {
			if nulls.Contains(vec.Nsp, uint64(offset)+uint64(i)) {
				r.NullCnt[vps[i]-1] += cntOfRows[int64(i)+offset]
			}
		}
	}
}

func (r *NumericRing) Add(ring interface{}, x, y int64) {
	ringData := ring.(*NumericRing)
	r.BitAndResult[x] &= ringData.BitAndResult[y]
	r.NullCnt[x] += ringData.NullCnt[y]
}

func (r *NumericRing) BatchAdd(ring interface{}, start int64, os []uint8, vps []uint64) {
	ringData := ring.(*NumericRing)
	for i := range os {
		r.BitAndResult[vps[i]-1] &= ringData.BitAndResult[int64(i)+start]
		r.NullCnt[vps[i]-1] += ringData.NullCnt[int64(i)+start]
	}
}

func (r *NumericRing) Mul(ring interface{}, x, y, z int64) {
	ringData := ring.(*NumericRing)
	r.BitAndResult[x] &= ringData.BitAndResult[y]
	r.NullCnt[x] += ringData.NullCnt[y] * z
}

func (r *NumericRing) Eval(cntOfRows []int64) *vector.Vector {
	defer func() {
		r.Data = nil
		r.NullCnt = nil
		r.BitAndResult = nil
	}()

	nsp := new(nulls.Nulls)

	for i, cnt := range cntOfRows {
		if cnt-r.NullCnt[i] == 0 {
			nulls.Add(nsp, uint64(i))
		}
	}

	return &vector.Vector{
		Nsp:  nsp,
		Data: r.Data,
		Col:  r.BitAndResult,
		Or:   false,
		Typ:  types.Type{Oid: types.T_uint64, Size: 8},
	}
}
