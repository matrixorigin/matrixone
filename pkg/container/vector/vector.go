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

package vector

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/vectorize/shuffle"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
)

func GetStrVectorValues(v *Vector) ([]byte, []uint32, []uint32) {
	if v.IsConst {
		col := v.Col.(*types.Bytes)
		data := make([]byte, 0, len(col.Data)*v.Length)
		os := make([]uint32, v.Length)
		ns := make([]uint32, v.Length)
		o := uint32(0)
		for i := 0; i < v.Length; i++ {
			os[i] = o
			ns[i] = uint32(len(col.Data))
			o += uint32(len(col.Data))
			data = append(data, col.Data...)
		}
		return data, os, ns
	}
	vs := v.Col.(*types.Bytes)
	return vs.Data, vs.Offsets, vs.Lengths
}

func GetFixedVectorValues[T any](v *Vector, sz int) []T {
	if v.IsConst {
		vs := make([]T, v.Length)
		addr := reflect.ValueOf(v.Col).Index(0).Addr().UnsafePointer()
		data := unsafe.Slice((*byte)(addr), sz)
		val := encoding.DecodeFixedSlice[T](data, sz)[0]
		for i := range vs {
			vs[i] = val
		}
		return vs
	}
	return DecodeFixedCol[T](v, sz)
}

func GenericVectorValues[T any](v *Vector) []T {
	return v.Col.([]T)
}

func GetColumn[T any](v *Vector) []T {
	return v.Col.([]T)
}

func GetStrColumn(v *Vector) *types.Bytes {
	return v.Col.(*types.Bytes)
}

// Count return the number of rows in the vector
func (v *Vector) Count() int {
	return Length(v)
}

func (v *Vector) Size() int {
	return len(v.Data)
}

func (v *Vector) GetType() types.Type {
	return v.Typ
}

func (v *Vector) GetNulls() *nulls.Nulls {
	return v.Nsp
}

func (v *Vector) GetString(i int64) []byte {
	return v.Col.(*types.Bytes).Get(i)
}

func (v *Vector) FillDefaultValue() {
	if !nulls.Any(v.Nsp) || len(v.Data) == 0 {
		return
	}
	switch v.Typ.Oid {
	case types.T_bool:
		fillDefaultValue[bool](v)
	case types.T_int8:
		fillDefaultValue[int8](v)
	case types.T_int16:
		fillDefaultValue[int16](v)
	case types.T_int32:
		fillDefaultValue[int32](v)
	case types.T_int64:
		fillDefaultValue[int64](v)
	case types.T_uint8:
		fillDefaultValue[uint8](v)
	case types.T_uint16:
		fillDefaultValue[uint16](v)
	case types.T_uint32:
		fillDefaultValue[uint32](v)
	case types.T_uint64:
		fillDefaultValue[uint64](v)
	case types.T_float32:
		fillDefaultValue[uint64](v)
	case types.T_float64:
		fillDefaultValue[float64](v)
	case types.T_date:
		fillDefaultValue[types.Date](v)
	case types.T_datetime:
		fillDefaultValue[types.Datetime](v)
	case types.T_timestamp:
		fillDefaultValue[types.Timestamp](v)
	case types.T_decimal64:
		fillDefaultValue[types.Decimal64](v)
	case types.T_decimal128:
		fillDefaultValue[types.Decimal128](v)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		col := v.Col.(*types.Bytes)
		rows := v.Nsp.Np.ToArray()
		for _, row := range rows {
			col.Offsets[row] = 0
			col.Lengths[row] = 0
		}
	}
}

func (v *Vector) ToConst(row int) *Vector {
	if v.IsConst {
		return v
	}
	switch v.Typ.Oid {
	case types.T_bool:
		return toConstVector[bool](v, row)
	case types.T_int8:
		return toConstVector[int8](v, row)
	case types.T_int16:
		return toConstVector[int16](v, row)
	case types.T_int32:
		return toConstVector[int32](v, row)
	case types.T_int64:
		return toConstVector[int64](v, row)
	case types.T_uint8:
		return toConstVector[uint8](v, row)
	case types.T_uint16:
		return toConstVector[uint16](v, row)
	case types.T_uint32:
		return toConstVector[uint32](v, row)
	case types.T_uint64:
		return toConstVector[uint64](v, row)
	case types.T_float32:
		return toConstVector[float32](v, row)
	case types.T_float64:
		return toConstVector[float64](v, row)
	case types.T_date:
		return toConstVector[types.Date](v, row)
	case types.T_datetime:
		return toConstVector[types.Datetime](v, row)
	case types.T_timestamp:
		return toConstVector[types.Timestamp](v, row)
	case types.T_decimal64:
		return toConstVector[types.Decimal64](v, row)
	case types.T_decimal128:
		return toConstVector[types.Decimal128](v, row)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		col := v.Col.(*types.Bytes)
		src := col.Data[col.Offsets[row] : col.Offsets[row]+col.Lengths[row]]
		data := make([]byte, len(src))
		copy(data, src)
		return &Vector{
			IsConst: true,
			Typ:     v.Typ,
			Col: &types.Bytes{
				Data:    data,
				Offsets: []uint32{0},
				Lengths: []uint32{col.Lengths[row]},
			},
		}
	}
	return nil
}

func (v *Vector) ConstExpand(m *mheap.Mheap) *Vector {
	if !v.IsConst {
		return v
	}
	if v.IsScalarNull() {
		var i uint64
		l := uint64(v.Length)
		temp := make([]uint64, v.Length)
		for i = 0; i < l; i++ {
			temp[i] = i
		}
		nulls.Add(v.Nsp, temp...)
		return v
	}
	switch v.Typ.Oid {
	case types.T_bool:
		expandVector[bool](v, 1, m)
	case types.T_int8:
		expandVector[int8](v, 1, m)
	case types.T_int16:
		expandVector[int16](v, 2, m)
	case types.T_int32:
		expandVector[int32](v, 4, m)
	case types.T_int64:
		expandVector[int64](v, 8, m)
	case types.T_uint8:
		expandVector[uint8](v, 1, m)
	case types.T_uint16:
		expandVector[uint16](v, 2, m)
	case types.T_uint32:
		expandVector[uint32](v, 4, m)
	case types.T_uint64:
		expandVector[uint64](v, 8, m)
	case types.T_float32:
		expandVector[float32](v, 4, m)
	case types.T_float64:
		expandVector[float64](v, 8, m)
	case types.T_date:
		expandVector[types.Date](v, 4, m)
	case types.T_datetime:
		expandVector[types.Datetime](v, 8, m)
	case types.T_timestamp:
		expandVector[types.Timestamp](v, 8, m)
	case types.T_decimal64:
		expandVector[types.Decimal64](v, 8, m)
	case types.T_decimal128:
		expandVector[types.Decimal128](v, 16, m)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		col := v.Col.(*types.Bytes)
		if nulls.Any(v.Nsp) {
			col.Offsets = col.Offsets[:0]
			col.Lengths = col.Lengths[:0]
			for i := 0; i < v.Length; i++ {
				nulls.Add(v.Nsp, uint64(i))
				col.Offsets = append(col.Offsets, 0)
				col.Lengths = append(col.Lengths, 0)
			}
		} else {
			data, err := mheap.Alloc(m, int64(v.Length*len(col.Data)))
			if err != nil {
				return nil
			}
			data = data[:0]
			o := uint32(0)
			os := make([]uint32, v.Length)
			ns := make([]uint32, v.Length)
			for i := 0; i < v.Length; i++ {
				os[i] = o
				ns[i] = uint32(len(col.Data))
				o += uint32(len(col.Data))
				data = append(data, col.Data...)
			}
			col.Data = data
			col.Offsets = os
			col.Lengths = ns
		}
	}
	v.IsConst = false
	return v
}

func (v *Vector) TryExpandNulls(n int) {
	nulls.TryExpand(v.Nsp, n)
}

func fillDefaultValue[T any](v *Vector) {
	var dv T

	col := v.Col.([]T)
	rows := v.Nsp.Np.ToArray()
	for _, row := range rows {
		col[row] = dv
	}
	v.Col = col
}

func toConstVector[T any](v *Vector, row int) *Vector {
	nsp := new(nulls.Nulls)
	if nulls.Contains(v.Nsp, uint64(row)) {
		nulls.Add(nsp, 0)
	}
	return &Vector{
		Nsp:     nsp,
		IsConst: true,
		Typ:     v.Typ,
		Col:     []T{v.Col.([]T)[row]},
	}
}

func expandVector[T any](v *Vector, sz int, m *mheap.Mheap) *Vector {
	data, err := mheap.Alloc(m, int64(v.Length*sz))
	if err != nil {
		return nil
	}
	vs := encoding.DecodeFixedSlice[T](data, sz)
	if nulls.Any(v.Nsp) {
		for i := 0; i < v.Length; i++ {
			nulls.Add(v.Nsp, uint64(i))
		}
	} else {
		val := v.Col.([]T)[0]
		for i := 0; i < v.Length; i++ {
			vs[i] = val
		}
	}
	v.Col = vs
	v.Data = data[:len(vs)*sz]
	return v
}

func DecodeFixedCol[T any](v *Vector, sz int) []T {
	return encoding.DecodeFixedSlice[T](v.Data, sz)
}

func NewWithData(typ types.Type, data []byte, col interface{}, nsp *nulls.Nulls) *Vector {
	return &Vector{
		Nsp:  nsp,
		Col:  col,
		Typ:  typ,
		Data: data,
	}
}

func New(typ types.Type) *Vector {
	switch typ.Oid {
	case types.T_any:
		return &Vector{
			Typ: typ,
			Col: nil,
			Nsp: &nulls.Nulls{},
		}
	case types.T_bool:
		return &Vector{
			Typ: typ,
			Col: []bool{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_int8:
		return &Vector{
			Typ: typ,
			Col: []int8{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_int16:
		return &Vector{
			Typ: typ,
			Col: []int16{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_int32:
		return &Vector{
			Typ: typ,
			Col: []int32{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_int64:
		return &Vector{
			Typ: typ,
			Col: []int64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_uint8:
		return &Vector{
			Typ: typ,
			Col: []uint8{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_uint16:
		return &Vector{
			Typ: typ,
			Col: []uint16{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_uint32:
		return &Vector{
			Typ: typ,
			Col: []uint32{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_uint64:
		return &Vector{
			Typ: typ,
			Col: []uint64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_float32:
		return &Vector{
			Typ: typ,
			Col: []float32{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_float64:
		return &Vector{
			Typ: typ,
			Col: []float64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_date:
		return &Vector{
			Typ: typ,
			Col: []types.Date{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_datetime:
		return &Vector{
			Typ: typ,
			Col: []types.Datetime{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_timestamp:
		return &Vector{
			Typ: typ,
			Col: []types.Timestamp{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_sel:
		return &Vector{
			Typ: typ,
			Col: []int64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_tuple:
		return &Vector{
			Typ: typ,
			Nsp: &nulls.Nulls{},
			Col: [][]interface{}{},
		}
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		return &Vector{
			Typ: typ,
			Col: &types.Bytes{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_decimal64:
		return &Vector{
			Typ: typ,
			Col: []types.Decimal64{},
			Nsp: &nulls.Nulls{},
		}
	case types.T_decimal128:
		return &Vector{
			Typ: typ,
			Col: []types.Decimal128{},
			Nsp: &nulls.Nulls{},
		}
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.New", typ))
	}
}

func NewConst(typ types.Type, length int) *Vector {
	v := New(typ)
	v.IsConst = true
	v.initConst(typ)
	v.Length = length
	return v
}

func NewConstNull(typ types.Type, length int) *Vector {
	v := New(typ)
	v.IsConst = true
	v.initConst(typ)
	nulls.Add(v.Nsp, 0)
	v.Length = length
	return v
}

func (v *Vector) initConst(typ types.Type) {
	switch typ.Oid {
	case types.T_bool:
		v.Col = []bool{false}
	case types.T_int8:
		v.Col = []int8{0}
	case types.T_int16:
		v.Col = []int16{0}
	case types.T_int32:
		v.Col = []int32{0}
	case types.T_int64:
		v.Col = []int64{0}
	case types.T_uint8:
		v.Col = []uint8{0}
	case types.T_uint16:
		v.Col = []uint16{0}
	case types.T_uint32:
		v.Col = []uint32{0}
	case types.T_uint64:
		v.Col = []uint64{0}
	case types.T_float32:
		v.Col = []float32{0}
	case types.T_float64:
		v.Col = []float64{0}
	case types.T_date:
		v.Col = make([]types.Date, 1)
	case types.T_datetime:
		v.Col = make([]types.Datetime, 1)
	case types.T_timestamp:
		v.Col = make([]types.Timestamp, 1)
	case types.T_decimal64:
		v.Col = make([]types.Decimal64, 1)
	case types.T_decimal128:
		v.Col = make([]types.Decimal128, 1)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		v.Col = &types.Bytes{
			Offsets: []uint32{0},
			Lengths: []uint32{0},
			Data:    []byte{},
		}
	}
}

// IsScalar return true if the vector means a scalar value.
// e.g.
// 		a + 1, and 1's vector will return true
func (v *Vector) IsScalar() bool {
	return v.IsConst
}

// IsScalarNull return true if the vector means a scalar Null.
// e.g.
// 		a + Null, and the vector of right part will return true
func (v *Vector) IsScalarNull() bool {
	return v.IsConst && v.Nsp != nil && nulls.Contains(v.Nsp, 0)
}

// ConstVectorIsNull checks whether a const vector is null
func (v *Vector) ConstVectorIsNull() bool {
	return v.Nsp != nil && nulls.Contains(v.Nsp, 0)
}

func (v *Vector) Free(m *mheap.Mheap) {
	if !v.Or && v.Data != nil {
		mheap.Free(m, v.Data)
		v.Data = nil
	}
}

func (v *Vector) Realloc(size int, m *mheap.Mheap) error {
	oldLen := len(v.Data)
	data, err := mheap.Grow(m, v.Data, int64(cap(v.Data)+size))
	if err != nil {
		return err
	}
	mheap.Free(m, v.Data)
	v.Data = data[:oldLen]
	switch v.Typ.Oid {
	case types.T_bool:
		v.Col = encoding.DecodeSlice[bool](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_int8:
		v.Col = encoding.DecodeSlice[int8](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_int16:
		v.Col = encoding.DecodeSlice[int16](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_int32:
		v.Col = encoding.DecodeSlice[int32](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_int64:
		v.Col = encoding.DecodeSlice[int64](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_uint8:
		v.Col = encoding.DecodeSlice[uint8](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_uint16:
		v.Col = encoding.DecodeSlice[uint16](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_uint32:
		v.Col = encoding.DecodeSlice[uint32](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_uint64:
		v.Col = encoding.DecodeSlice[uint64](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_date:
		v.Col = encoding.DecodeSlice[types.Date](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_datetime:
		v.Col = encoding.DecodeSlice[types.Datetime](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_timestamp:
		v.Col = encoding.DecodeSlice[types.Timestamp](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_decimal64:
		v.Col = encoding.DecodeSlice[types.Decimal64](v.Data[:len(data)], size)[:oldLen/size]
	case types.T_decimal128:
		v.Col = encoding.DecodeSlice[types.Decimal128](v.Data[:len(data)], size)[:oldLen/size]
	}
	return nil
}

func (v *Vector) Append(w any, m *mheap.Mheap) error {
	switch v.Typ.Oid {
	case types.T_bool:
		wv := w.(bool)
		col := v.Col.([]bool)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(1, m); err != nil {
				return err
			}
			col = v.Col.([]bool)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n + 1)]
	case types.T_int8:
		wv := w.(int8)
		col := v.Col.([]int8)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(1, m); err != nil {
				return err
			}
			col = v.Col.([]int8)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*1]
	case types.T_int16:
		wv := w.(int16)
		col := v.Col.([]int16)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(2, m); err != nil {
				return err
			}
			col = v.Col.([]int16)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*2]
	case types.T_int32:
		wv := w.(int32)
		col := v.Col.([]int32)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(4, m); err != nil {
				return err
			}
			col = v.Col.([]int32)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*4]
	case types.T_int64:
		wv := w.(int64)
		col := v.Col.([]int64)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(8, m); err != nil {
				return err
			}
			col = v.Col.([]int64)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*8]
	case types.T_uint8:
		wv := w.(uint8)
		col := v.Col.([]uint8)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(1, m); err != nil {
				return err
			}
			col = v.Col.([]uint8)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n + 1)]
	case types.T_uint16:
		wv := w.(uint16)
		col := v.Col.([]uint16)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(2, m); err != nil {
				return err
			}
			col = v.Col.([]uint16)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*2]
	case types.T_uint32:
		wv := w.(uint32)
		col := v.Col.([]uint32)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(4, m); err != nil {
				return err
			}
			col = v.Col.([]uint32)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*4]
	case types.T_uint64:
		wv := w.(uint64)
		col := v.Col.([]uint64)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(8, m); err != nil {
				return err
			}
			col = v.Col.([]uint64)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*8]
	case types.T_float32:
		wv := w.(float32)
		col := v.Col.([]float32)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(4, m); err != nil {
				return err
			}
			col = v.Col.([]float32)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*4]
	case types.T_float64:
		wv := w.(float64)
		col := v.Col.([]float64)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(8, m); err != nil {
				return err
			}
			col = v.Col.([]float64)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*8]
	case types.T_date:
		wv := w.(types.Date)
		col := v.Col.([]types.Date)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(4, m); err != nil {
				return err
			}
			col = v.Col.([]types.Date)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*4]
	case types.T_datetime:
		wv := w.(types.Datetime)
		col := v.Col.([]types.Datetime)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(8, m); err != nil {
				return err
			}
			col = v.Col.([]types.Datetime)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*8]
	case types.T_timestamp:
		wv := w.(types.Timestamp)
		col := v.Col.([]types.Timestamp)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(8, m); err != nil {
				return err
			}
			col = v.Col.([]types.Timestamp)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*8]
	case types.T_decimal64:
		wv := w.(types.Decimal64)
		col := v.Col.([]types.Decimal64)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(8, m); err != nil {
				return err
			}
			col = v.Col.([]types.Decimal64)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*8]
	case types.T_decimal128:
		wv := w.(types.Decimal128)
		col := v.Col.([]types.Decimal128)
		n := len(col)
		if n+1 >= cap(col) {
			if err := v.Realloc(16, m); err != nil {
				return err
			}
			col = v.Col.([]types.Decimal128)
		}
		col = append(col, wv)
		v.Col = col
		v.Data = v.Data[:(n+1)*16]
	case types.T_char, types.T_varchar, types.T_blob:
		wv := w.([]byte)
		n := len(v.Data)
		if n+len(wv) >= cap(v.Data) {
			if err := v.Realloc(n+len(wv)-cap(v.Data)+1, m); err != nil {
				return err
			}
		}
		col := v.Col.(*types.Bytes)
		col.Lengths = append(col.Lengths, uint32(len(wv)))
		col.Offsets = append(col.Offsets, uint32(len(v.Data)))
		v.Data = append(v.Data, wv...)
		col.Data = v.Data
		v.Col = col
	}
	return nil
}

func Reset(v *Vector) {
	switch v.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		v.Col.(*types.Bytes).Reset()
	default:
		*(*int)(unsafe.Pointer(uintptr((*(*emptyInterface)(unsafe.Pointer(&v.Col))).word) + uintptr(strconv.IntSize>>3))) = 0
	}
}

func Free(v *Vector, m *mheap.Mheap) {
	v.Ref--
	if !v.Or && v.Data != nil {
		if v.Ref == 0 && v.Link == 0 {
			mheap.Free(m, v.Data)
			v.Data = nil
		}
	}
}

func Clean(v *Vector, m *mheap.Mheap) {
	if !v.Or && v.Data != nil {
		mheap.Free(m, v.Data)
		v.Data = nil
	}
}

func SetCol(v *Vector, col interface{}) {
	v.Col = col
}

func PreAlloc(v, w *Vector, rows int, m *mheap.Mheap) {
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	v.Ref = w.Ref
	switch v.Typ.Oid {
	case types.T_int8:
		data, err := mheap.Alloc(m, int64(rows))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeInt8Slice(v.Data)[:0]
	case types.T_int16:
		data, err := mheap.Alloc(m, int64(rows*2))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeInt16Slice(v.Data)[:0]
	case types.T_int32:
		data, err := mheap.Alloc(m, int64(rows*4))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeInt32Slice(v.Data)[:0]
	case types.T_int64:
		data, err := mheap.Alloc(m, int64(rows*8))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeInt64Slice(v.Data)[:0]
	case types.T_uint8:
		data, err := mheap.Alloc(m, int64(rows))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeUint8Slice(v.Data)[:0]
	case types.T_uint16:
		data, err := mheap.Alloc(m, int64(rows*2))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeUint16Slice(v.Data)[:0]
	case types.T_uint32:
		data, err := mheap.Alloc(m, int64(rows*4))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeUint32Slice(v.Data)[:0]
	case types.T_uint64:
		data, err := mheap.Alloc(m, int64(rows*8))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeUint64Slice(v.Data)[:0]
	case types.T_float32:
		data, err := mheap.Alloc(m, int64(rows*4))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeFloat32Slice(v.Data)[:0]
	case types.T_float64:
		data, err := mheap.Alloc(m, int64(rows*8))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeFloat64Slice(v.Data)[:0]
	case types.T_date:
		data, err := mheap.Alloc(m, int64(rows*4))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeDateSlice(v.Data)[:0]
	case types.T_datetime:
		data, err := mheap.Alloc(m, int64(rows*8))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeDatetimeSlice(v.Data)[:0]
	case types.T_timestamp:
		data, err := mheap.Alloc(m, int64(rows*8))
		if err != nil {
			return
		}
		v.Data = data
		v.Col = encoding.DecodeTimestampSlice(v.Data)[:0]
	case types.T_char, types.T_varchar, types.T_blob:
		vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
		data, err := mheap.Alloc(m, int64(rows*len(ws.Data)/len(ws.Offsets)))
		if err != nil {
			return
		}
		v.Data = data
		vs.Data = data[:0]
		vs.Offsets = make([]uint32, 0, rows)
		vs.Lengths = make([]uint32, 0, rows)
	}
}

func Length(v *Vector) int {
	if v.IsScalar() || v.Typ.Oid == types.T_any {
		return v.Length
	}
	switch v.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		return len(v.Col.(*types.Bytes).Offsets)
	default:
		return reflect.ValueOf(v.Col).Len()
	}
}

func setLengthFixed[T any](v *Vector, n int) {
	vs := v.Col.([]T)
	m := len(vs)
	v.Col = vs[:n]
	nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
}

func SetLength(v *Vector, n int) {
	if v.IsScalar() || v.Typ.Oid == types.T_any {
		SetScalarLength(v, n)
		return
	}
	SetVectorLength(v, n)
}

func SetScalarLength(v *Vector, n int) {
	v.Length = n
}

func SetVectorLength(v *Vector, n int) {
	switch v.Typ.Oid {
	case types.T_bool:
		setLengthFixed[bool](v, n)
	case types.T_int8:
		v.Data = v.Data[:n*1]
		setLengthFixed[int8](v, n)
	case types.T_int16:
		v.Data = v.Data[:n*2]
		setLengthFixed[int16](v, n)
	case types.T_int32:
		v.Data = v.Data[:n*4]
		setLengthFixed[int32](v, n)
	case types.T_int64:
		v.Data = v.Data[:n*8]
		setLengthFixed[int64](v, n)
	case types.T_uint8:
		v.Data = v.Data[:n*1]
		setLengthFixed[uint8](v, n)
	case types.T_uint16:
		v.Data = v.Data[:n*2]
		setLengthFixed[uint16](v, n)
	case types.T_uint32:
		v.Data = v.Data[:n*4]
		setLengthFixed[uint32](v, n)
	case types.T_uint64:
		v.Data = v.Data[:n*8]
		setLengthFixed[uint64](v, n)
	case types.T_float32:
		v.Data = v.Data[:n*4]
		setLengthFixed[float32](v, n)
	case types.T_float64:
		v.Data = v.Data[:n*8]
		setLengthFixed[float64](v, n)
	case types.T_date:
		v.Data = v.Data[:n*4]
		setLengthFixed[types.Date](v, n)
	case types.T_datetime:
		v.Data = v.Data[:n*8]
		setLengthFixed[types.Datetime](v, n)
	case types.T_timestamp:
		v.Data = v.Data[:n*8]
		setLengthFixed[types.Timestamp](v, n)
	case types.T_decimal64:
		v.Data = v.Data[:n*8]
		setLengthFixed[types.Decimal64](v, n)
	case types.T_decimal128:
		v.Data = v.Data[:n*16]
		setLengthFixed[types.Decimal128](v, n)

	case types.T_sel:
		vs := v.Col.([]int64)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_tuple:
		vs := v.Col.([][]interface{})
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs := v.Col.(*types.Bytes)
		m := len(vs.Offsets)
		vs.Data = vs.Data[:vs.Offsets[n-1]+vs.Lengths[n-1]]
		vs.Offsets = vs.Offsets[:n]
		vs.Lengths = vs.Lengths[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.SetLength", v.Typ))
	}
}

func Dup(v *Vector, m *mheap.Mheap) (*Vector, error) {
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	switch v.Typ.Oid {
	case types.T_bool:
		vs := v.Col.([]bool)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeBoolSlice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_int8:
		vs := v.Col.([]int8)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeInt8Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_int16:
		vs := v.Col.([]int16)
		data, err := mheap.Alloc(m, int64(len(vs)*2))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeInt16Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_int32:
		vs := v.Col.([]int32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeInt32Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_int64:
		vs := v.Col.([]int64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeInt64Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_uint8:
		vs := v.Col.([]uint8)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeUint8Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_uint16:
		vs := v.Col.([]uint16)
		data, err := mheap.Alloc(m, int64(len(vs)*2))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeUint16Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_uint32:
		vs := v.Col.([]uint32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeUint32Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_uint64:
		vs := v.Col.([]uint64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeUint64Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_float32:
		vs := v.Col.([]float32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeFloat32Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_float64:
		vs := v.Col.([]float64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeFloat64Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		var err error
		var data []byte

		vs := v.Col.(*types.Bytes)
		ws := &types.Bytes{
			Offsets: make([]uint32, len(vs.Offsets)),
			Lengths: make([]uint32, len(vs.Lengths)),
		}
		if len(vs.Data) > 0 {
			if data, err = mheap.Alloc(m, int64(len(vs.Data))); err != nil {
				return nil, err
			}
			ws.Data = data
			copy(ws.Data, vs.Data)
		} else {
			ws.Data = make([]byte, 0)
		}
		copy(ws.Offsets, vs.Offsets)
		copy(ws.Lengths, vs.Lengths)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_date:
		vs := v.Col.([]types.Date)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeDateSlice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_datetime:
		vs := v.Col.([]types.Datetime)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeDatetimeSlice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_timestamp:
		vs := v.Col.([]types.Timestamp)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeTimestampSlice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_decimal64:
		vs := v.Col.([]types.Decimal64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeDecimal64Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	case types.T_decimal128:
		vs := v.Col.([]types.Decimal128)
		data, err := mheap.Alloc(m, int64(len(vs)*16))
		if err != nil {
			return nil, err
		}
		ws := encoding.DecodeDecimal128Slice(data)
		copy(ws, vs)
		return &Vector{
			Col:  ws,
			Data: data,
			Typ:  v.Typ,
			Nsp:  v.Nsp,
			Ref:  v.Ref,
			Link: v.Link,
		}, nil
	}
	return nil, fmt.Errorf("unsupport type %v", v.Typ)
}

func Window(v *Vector, start, end int, w *Vector) *Vector {
	w.Typ = v.Typ
	switch v.Typ.Oid {
	case types.T_bool:
		w.Col = v.Col.([]bool)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_int8:
		w.Col = v.Col.([]int8)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_int16:
		w.Col = v.Col.([]int16)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_int32:
		w.Col = v.Col.([]int32)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_int64:
		w.Col = v.Col.([]int64)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_uint8:
		w.Col = v.Col.([]uint8)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_uint16:
		w.Col = v.Col.([]uint16)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_uint32:
		w.Col = v.Col.([]uint32)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_uint64:
		w.Col = v.Col.([]uint64)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_float32:
		w.Col = v.Col.([]float32)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_float64:
		w.Col = v.Col.([]float64)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_sel:
		w.Col = v.Col.([]int64)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_tuple:
		w.Col = v.Col.([][]interface{})[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		w.Col = v.Col.(*types.Bytes).Window(start, end)
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_date:
		w.Col = v.Col.([]types.Date)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_datetime:
		w.Col = v.Col.([]types.Datetime)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_timestamp:
		w.Col = v.Col.([]types.Timestamp)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_decimal64:
		w.Col = v.Col.([]types.Decimal64)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	case types.T_decimal128:
		w.Col = v.Col.([]types.Decimal128)[start:end]
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Window", v.Typ))
	}
	return w
}

func Append(v *Vector, arg interface{}) error {
	switch v.Typ.Oid {
	case types.T_bool:
		v.Col = append(v.Col.([]bool), arg.([]bool)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]bool), 1)
	case types.T_int8:
		v.Col = append(v.Col.([]int8), arg.([]int8)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int8), 1)
	case types.T_int16:
		v.Col = append(v.Col.([]int16), arg.([]int16)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int16), 2)
	case types.T_int32:
		v.Col = append(v.Col.([]int32), arg.([]int32)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int32), 4)
	case types.T_int64:
		v.Col = append(v.Col.([]int64), arg.([]int64)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]int64), 8)
	case types.T_uint8:
		v.Col = append(v.Col.([]uint8), arg.([]uint8)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint8), 1)
	case types.T_uint16:
		v.Col = append(v.Col.([]uint16), arg.([]uint16)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint16), 2)
	case types.T_uint32:
		v.Col = append(v.Col.([]uint32), arg.([]uint32)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint32), 4)
	case types.T_uint64:
		v.Col = append(v.Col.([]uint64), arg.([]uint64)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]uint64), 8)
	case types.T_float32:
		v.Col = append(v.Col.([]float32), arg.([]float32)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]float32), 4)
	case types.T_float64:
		v.Col = append(v.Col.([]float64), arg.([]float64)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]float64), 8)
	case types.T_date:
		v.Col = append(v.Col.([]types.Date), arg.([]types.Date)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Date), 4)
	case types.T_datetime:
		v.Col = append(v.Col.([]types.Datetime), arg.([]types.Datetime)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Datetime), 8)
	case types.T_timestamp:
		v.Col = append(v.Col.([]types.Timestamp), arg.([]types.Timestamp)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Timestamp), 8)
	case types.T_sel:
		v.Col = append(v.Col.([]int64), arg.([]int64)...)
	case types.T_tuple:
		v.Col = append(v.Col.([][]interface{}), arg.([][]interface{})...)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		return v.Col.(*types.Bytes).Append(arg.([][]byte))
	case types.T_decimal64:
		v.Col = append(v.Col.([]types.Decimal64), arg.([]types.Decimal64)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal64), 8)
	case types.T_decimal128:
		v.Col = append(v.Col.([]types.Decimal128), arg.([]types.Decimal128)...)
		v.Data = encoding.EncodeFixedSlice(v.Col.([]types.Decimal128), 16)
	default:
		return fmt.Errorf("unexpect type %s for function vector.Append", v.Typ)
	}
	return nil
}

func Shrink(v *Vector, sels []int64) {
	if v.IsScalar() {
		v.Length = len(sels)
		return
	}
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	switch v.Typ.Oid {
	case types.T_bool:
		vs := v.Col.([]bool)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int8:
		vs := v.Col.([]int8)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*1]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int16:
		vs := v.Col.([]int16)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*2]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int32:
		vs := v.Col.([]int32)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*4]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int64:
		vs := v.Col.([]int64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint8:
		vs := v.Col.([]uint8)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*1]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint16:
		vs := v.Col.([]uint16)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*2]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint32:
		vs := v.Col.([]uint32)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*4]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint64:
		vs := v.Col.([]uint64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_float32:
		vs := v.Col.([]float32)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*4]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_float64:
		vs := v.Col.([]float64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_sel:
		vs := v.Col.([]int64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_tuple:
		vs := v.Col.([][]interface{})
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs := v.Col.(*types.Bytes)
		for i, sel := range sels {
			vs.Offsets[i] = vs.Offsets[sel]
			vs.Lengths[i] = vs.Lengths[sel]
		}
		vs.Offsets = vs.Offsets[:len(sels)]
		vs.Lengths = vs.Lengths[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_date:
		vs := v.Col.([]types.Date)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*4]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_datetime:
		vs := v.Col.([]types.Datetime)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_timestamp:
		vs := v.Col.([]types.Timestamp)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_decimal64:
		vs := v.Col.([]types.Decimal64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*8]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_decimal128:
		vs := v.Col.([]types.Decimal128)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Data = v.Data[:len(sels)*16]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	}
}

func Shuffle(v *Vector, sels []int64, m *mheap.Mheap) error {
	if v.IsScalar() {
		v.Length = len(sels)
		return nil
	}
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	switch v.Typ.Oid {
	case types.T_bool:
		vs := v.Col.([]bool)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return err
		}
		ws := encoding.DecodeBoolSlice(data)
		v.Col = shuffle.BoolShuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_int8:
		vs := v.Col.([]int8)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt8Slice(data)
		v.Col = shuffle.Int8Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*1]
		mheap.Free(m, data)
	case types.T_int16:
		vs := v.Col.([]int16)
		data, err := mheap.Alloc(m, int64(len(vs)*2))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt16Slice(data)
		v.Col = shuffle.Int16Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*2]
		mheap.Free(m, data)
	case types.T_int32:
		vs := v.Col.([]int32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt32Slice(data)
		v.Col = shuffle.Int32Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*4]
		mheap.Free(m, data)
	case types.T_int64:
		vs := v.Col.([]int64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		v.Col = shuffle.Int64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_uint8:
		vs := v.Col.([]uint8)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint8Slice(data)
		v.Col = shuffle.Uint8Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*1]
		mheap.Free(m, data)
	case types.T_uint16:
		vs := v.Col.([]uint16)
		data, err := mheap.Alloc(m, int64(len(vs)*2))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint16Slice(data)
		v.Col = shuffle.Uint16Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*2]
		mheap.Free(m, data)
	case types.T_uint32:
		vs := v.Col.([]uint32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint32Slice(data)
		v.Col = shuffle.Uint32Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*4]
		mheap.Free(m, data)
	case types.T_uint64:
		vs := v.Col.([]uint64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint64Slice(data)
		v.Col = shuffle.Uint64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_float32:
		vs := v.Col.([]float32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return err
		}
		ws := encoding.DecodeFloat32Slice(data)
		v.Col = shuffle.Float32Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*4]
		mheap.Free(m, data)
	case types.T_float64:
		vs := v.Col.([]float64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeFloat64Slice(data)
		v.Col = shuffle.Float64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_sel:
		vs := v.Col.([]int64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		v.Col = shuffle.Int64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_tuple:
		vs := v.Col.([][]interface{})
		ws := make([][]interface{}, len(vs))
		v.Col = shuffle.TupleShuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs := v.Col.(*types.Bytes)
		odata, err := mheap.Alloc(m, int64(len(vs.Offsets)*4))
		if err != nil {
			return err
		}
		os := encoding.DecodeUint32Slice(odata)
		ndata, err := mheap.Alloc(m, int64(len(vs.Offsets)*4))
		if err != nil {
			mheap.Free(m, odata)
			return err
		}
		ns := encoding.DecodeUint32Slice(ndata)
		v.Col = shuffle.StrShuffle(vs, os, ns, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, odata)
		mheap.Free(m, ndata)
	case types.T_date:
		vs := v.Col.([]types.Date)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return err
		}
		ws := encoding.DecodeDateSlice(data)
		v.Col = shuffle.DateShuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*4]
		mheap.Free(m, data)
	case types.T_datetime:
		vs := v.Col.([]types.Datetime)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeDatetimeSlice(data)
		v.Col = shuffle.DatetimeShuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_timestamp:
		vs := v.Col.([]types.Timestamp)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeTimestampSlice(data)
		v.Col = shuffle.TimestampShuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_decimal64:
		vs := v.Col.([]types.Decimal64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeDecimal64Slice(data)
		v.Col = shuffle.Decimal64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*8]
		mheap.Free(m, data)
	case types.T_decimal128:
		vs := v.Col.([]types.Decimal128)
		data, err := mheap.Alloc(m, int64(len(vs)*16))
		if err != nil {
			return err
		}
		ws := encoding.DecodeDecimal128Slice(data)
		v.Col = shuffle.Decimal128Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		v.Data = v.Data[:len(sels)*1]
		mheap.Free(m, data)
	default:
		panic(fmt.Sprintf("unexpect type %s for function vector.Shuffle", v.Typ))
	}
	return nil
}

// Copy v[vi] = w[wi]
func Copy(v, w *Vector, vi, wi int64, m *mheap.Mheap) error {
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
	data := ws.Data[ws.Offsets[wi] : ws.Offsets[wi]+ws.Lengths[wi]]
	if vs.Lengths[vi] >= ws.Lengths[wi] {
		vs.Lengths[vi] = ws.Lengths[wi]
		copy(vs.Data[vs.Offsets[vi]:int(vs.Offsets[vi])+len(data)], data)
		return nil
	}
	diff := ws.Lengths[wi] - vs.Lengths[vi]
	buf, err := mheap.Alloc(m, int64(len(vs.Data)+int(diff)))
	if err != nil {
		return err
	}
	copy(buf, vs.Data[:vs.Offsets[vi]])
	copy(buf[vs.Offsets[vi]:], data)
	o := vs.Offsets[vi] + vs.Lengths[vi]
	copy(buf[o+diff:], vs.Data[o:])
	mheap.Free(m, v.Data)
	v.Data = buf
	vs.Data = buf[:len(vs.Data)+int(diff)]
	vs.Lengths[vi] = ws.Lengths[wi]
	for i, j := vi+1, int64(len(vs.Offsets)); i < j; i++ {
		vs.Offsets[i] += diff
	}
	return nil
}

func UnionOne(v, w *Vector, sel int64, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionOne operation cannot be performed for origin vector")
	}
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	switch v.Typ.Oid {
	case types.T_bool:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeBoolSlice(data)
			vs[0] = w.Col.([]bool)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]bool)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeBoolSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]bool)[sel])
			v.Col = vs
		}
	case types.T_int8:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt8Slice(data)
			vs[0] = w.Col.([]int8)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int8)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt8Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]int8)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*1]
		}
	case types.T_int16:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 2*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt16Slice(data)
			vs[0] = w.Col.([]int16)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int16)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*2], int64(n+1)*2)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt16Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]int16)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*2]
		}
	case types.T_int32:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt32Slice(data)
			vs[0] = w.Col.([]int32)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int32)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt32Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]int32)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_int64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt64Slice(data)
			vs[0] = w.Col.([]int64)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]int64)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_uint8:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint8Slice(data)
			vs[0] = w.Col.([]uint8)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint8)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint8Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]uint8)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*1]
		}
	case types.T_uint16:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 2*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint16Slice(data)
			vs[0] = w.Col.([]uint16)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint16)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*2], int64(n+1)*2)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint16Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]uint16)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*2]
		}
	case types.T_uint32:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint32Slice(data)
			vs[0] = w.Col.([]uint32)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint32)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint32Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]uint32)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_uint64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint64Slice(data)
			vs[0] = w.Col.([]uint64)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]uint64)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_float32:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeFloat32Slice(data)
			vs[0] = w.Col.([]float32)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]float32)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat32Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]float32)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_float64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeFloat64Slice(data)
			vs[0] = w.Col.([]float64)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]float64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]float64)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_tuple:
		v.Ref = w.Ref
		vs, ws := v.Col.([][]interface{}), w.Col.([][]interface{})
		vs = append(vs, ws[sel])
		v.Col = vs
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
		from := ws.Get(sel)
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, int64(len(from))*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			v.Data = data
			vs.Data = data[:0]
		} else if n := len(vs.Data); n+len(from) >= cap(vs.Data) {
			data, err := mheap.Grow(m, vs.Data, int64(n+len(from)))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			v.Data = data
			n = len(vs.Offsets)
			vs.Data = data[:vs.Offsets[n-1]+vs.Lengths[n-1]]
		}
		vs.Lengths = append(vs.Lengths, uint32(len(from)))
		{
			n := len(vs.Offsets)
			if n > 0 {
				vs.Offsets = append(vs.Offsets, vs.Offsets[n-1]+vs.Lengths[n-1])
			} else {
				vs.Offsets = append(vs.Offsets, 0)
			}
		}
		vs.Data = append(vs.Data, from...)
		v.Col = vs
	case types.T_date:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDateSlice(data)
			vs[0] = w.Col.([]types.Date)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Date)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDateSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Date)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_datetime:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDatetimeSlice(data)
			vs[0] = w.Col.([]types.Datetime)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Datetime)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDatetimeSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Datetime)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_timestamp:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeTimestampSlice(data)
			vs[0] = w.Col.([]types.Timestamp)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Timestamp)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeTimestampSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Timestamp)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_decimal64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDecimal64Slice(data)
			vs[0] = w.Col.([]types.Decimal64)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Decimal64)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_decimal128:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 16*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDecimal128Slice(data)
			vs[0] = w.Col.([]types.Decimal128)[sel]
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal128)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*16], int64(n+1)*16)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal128Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, w.Col.([]types.Decimal128)[sel])
			v.Col = vs
			v.Data = v.Data[:len(vs)*16]
		}
	}
	if nulls.Any(w.Nsp) && nulls.Contains(w.Nsp, uint64(sel)) {
		nulls.Add(v.Nsp, uint64(Length(v)-1))
	}
	return nil
}

func UnionNull(v, _ *Vector, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionNull operation cannot be performed for origin vector")
	}
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	switch v.Typ.Oid {
	case types.T_bool:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeBoolSlice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]bool)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeBoolSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
		}
	case types.T_int8:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeInt8Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int8)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt8Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*1]
		}
	case types.T_int16:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 2*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeInt16Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int16)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*2], int64(n+1)*2)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt16Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*2]
		}
	case types.T_int32:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeInt32Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int32)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt32Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_int64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeInt64Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]int64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_uint8:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeUint8Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint8)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+1))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint8Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*1]
		}
	case types.T_uint16:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 2*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeUint16Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint16)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*2], int64(n+1)*2)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint16Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*2]
		}
	case types.T_uint32:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeUint32Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint32)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint32Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_uint64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeUint64Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]uint64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_float32:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeFloat32Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]float32)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat32Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_float64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeFloat64Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]float64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs := v.Col.(*types.Bytes)
		vs.Offsets = append(vs.Offsets, 0)
		vs.Lengths = append(vs.Lengths, 0)
		v.Col = vs
	case types.T_date:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 4*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeDateSlice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Date)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+1)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDateSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*4]
		}
	case types.T_datetime:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeDatetimeSlice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Datetime)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDatetimeSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_timestamp:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeTimestampSlice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Timestamp)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeTimestampSlice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_decimal64:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 8*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeDecimal64Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal64)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+1)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal64Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*8]
		}
	case types.T_decimal128:
		if len(v.Data) == 0 {
			data, err := mheap.Alloc(m, 16*8)
			if err != nil {
				return err
			}
			vs := encoding.DecodeDecimal128Slice(data)
			v.Col = vs[:1]
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal128)
			if n := len(vs); n+1 >= cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*16], int64(n+1)*16)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal128Slice(data)
				vs = vs[:n]
				v.Col = vs
				v.Data = data
			}
			vs = append(vs, vs[0])
			v.Col = vs
			v.Data = v.Data[:len(vs)*16]
		}
	}
	nulls.Add(v.Nsp, uint64(Length(v)-1))
	return nil
}

func Union(v, w *Vector, sels []int64, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("Union operation cannot be performed for origin vector")
	}
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()
	oldLen := Length(v)
	switch v.Typ.Oid {
	case types.T_bool:
		cnt := len(sels)
		ws := w.Col.([]bool)
		vs := v.Col.([]bool)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeBoolSlice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_int8:
		cnt := len(sels)
		ws := w.Col.([]int8)
		vs := v.Col.([]int8)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeInt8Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_int16:
		cnt := len(sels)
		ws := w.Col.([]int16)
		vs := v.Col.([]int16)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*2)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeInt16Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_int32:
		cnt := len(sels)
		ws := w.Col.([]int32)
		vs := v.Col.([]int32)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*4)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeInt32Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_int64:
		cnt := len(sels)
		ws := w.Col.([]int64)
		vs := v.Col.([]int64)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*8)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeInt64Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_uint8:
		cnt := len(sels)
		ws := w.Col.([]uint8)
		vs := v.Col.([]uint8)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeUint8Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_uint16:
		cnt := len(sels)
		ws := w.Col.([]uint16)
		vs := v.Col.([]uint16)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*2)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeUint16Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_uint32:
		cnt := len(sels)
		ws := w.Col.([]uint32)
		vs := v.Col.([]uint32)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*4)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeUint32Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_uint64:
		cnt := len(sels)
		ws := w.Col.([]uint64)
		vs := v.Col.([]uint64)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*8)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeUint64Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_float32:
		cnt := len(sels)
		ws := w.Col.([]float32)
		vs := v.Col.([]float32)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*4)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeFloat32Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_float64:
		cnt := len(sels)
		ws := w.Col.([]float64)
		vs := v.Col.([]float64)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*8)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeFloat64Slice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
		incSize := 0
		for _, sel := range sels {
			incSize += int(ws.Lengths[sel])
		}
		if n := len(vs.Data); n+incSize > cap(vs.Data) {
			data, err := mheap.Grow(m, vs.Data, int64(n+incSize))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			v.Data = data
			vs.Data = data[:n]
		}
		for _, sel := range sels {
			from := ws.Get(sel)
			vs.Lengths = append(vs.Lengths, uint32(len(from)))
			vs.Offsets = append(vs.Offsets, uint32(len(vs.Data)))
			vs.Data = append(vs.Data, from...)
		}
		v.Col = vs
	case types.T_date:
		cnt := len(sels)
		ws := w.Col.([]types.Date)
		vs := v.Col.([]types.Date)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*4)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeDateSlice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_datetime:
		cnt := len(sels)
		ws := w.Col.([]types.Datetime)
		vs := v.Col.([]types.Datetime)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*8)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeDatetimeSlice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	case types.T_timestamp:
		cnt := len(sels)
		ws := w.Col.([]types.Timestamp)
		vs := v.Col.([]types.Timestamp)
		n := len(vs)
		if n+cnt >= cap(vs) {
			data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt)*8)
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			vs = encoding.DecodeTimestampSlice(data)
			v.Data = data
		}
		vs = vs[:n+cnt]
		j := n
		for i, sel := range sels {
			vs[i] = ws[sel]
			j++
		}
		v.Col = vs
	}
	if nulls.Any(w.Nsp) {
		j := uint64(oldLen)
		for _, sel := range sels {
			if nulls.Contains(w.Nsp, uint64(sel)) {
				nulls.Add(v.Nsp, j)
				j++
			}
		}
	}
	return nil
}

func UnionBatch(v, w *Vector, offset int64, cnt int, flags []uint8, m *mheap.Mheap) error {
	if v.Or {
		return errors.New("UnionOne operation cannot be performed for origin vector")
	}
	defer func() {
		size := v.Typ.Oid.TypeLen()
		if v.Typ.Oid != types.T_char && v.Typ.Oid != types.T_varchar && v.Typ.Oid != types.T_blob {
			v.Data = v.Data[:reflect.ValueOf(v.Col).Len()*size]
		}
	}()

	oldLen := Length(v)

	switch v.Typ.Oid {
	case types.T_bool:
		col := w.Col.([]bool)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize))
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeBoolSlice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]bool)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeBoolSlice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}
	case types.T_int8:
		col := w.Col.([]int8)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize))
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt8Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]int8)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt8Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_int16:
		col := w.Col.([]int16)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*2)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt16Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]int16)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*2], int64(n+cnt)*2)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt16Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_int32:
		col := w.Col.([]int32)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*4)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt32Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]int32)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+cnt)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt32Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_int64:
		col := w.Col.([]int64)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeInt64Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]int64)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeInt64Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_uint8:
		col := w.Col.([]uint8)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize))
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint8Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]uint8)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n], int64(n+cnt))
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint8Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_uint16:
		col := w.Col.([]uint16)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*2)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint16Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]uint16)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*2], int64(n+cnt)*2)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint16Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_uint32:
		col := w.Col.([]uint32)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*4)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint32Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]uint32)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+cnt)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint32Slice(data)
				v.Col = vs
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_uint64:
		col := w.Col.([]uint64)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeUint64Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]uint64)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeUint64Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_float32:
		col := w.Col.([]float32)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*4)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeFloat32Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]float32)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+cnt)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat32Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_float64:
		col := w.Col.([]float64)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeFloat64Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]float64)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeFloat64Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_tuple:
		v.Ref = w.Ref
		vs, ws := v.Col.([][]interface{}), w.Col.([][]interface{})
		for i, flag := range flags {
			if flag > 0 {
				vs = append(vs, ws[int(offset)+i])
			}
		}
		v.Col = vs

	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		vs, ws := v.Col.(*types.Bytes), w.Col.(*types.Bytes)
		incSize := 0
		for i, flag := range flags {
			if flag > 0 {
				incSize += int(ws.Lengths[int(offset)+i])
			}
		}

		if len(v.Data) == 0 {
			newSize := 8
			for newSize < incSize {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize))
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			v.Data = data
			vs.Data = data[:0]
		} else if n := len(vs.Data); n+incSize > cap(vs.Data) {
			data, err := mheap.Grow(m, vs.Data, int64(n+incSize))
			if err != nil {
				return err
			}
			mheap.Free(m, v.Data)
			v.Data = data
			vs.Data = data[:n]
		}

		for i, flag := range flags {
			if flag > 0 {
				from := ws.Get(offset + int64(i))
				vs.Lengths = append(vs.Lengths, uint32(len(from)))
				vs.Offsets = append(vs.Offsets, uint32(len(vs.Data)))
				vs.Data = append(vs.Data, from...)
			}
		}
		v.Col = vs

	case types.T_date:
		col := w.Col.([]types.Date)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*4)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDateSlice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]types.Date)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*4], int64(n+cnt)*4)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDateSlice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_datetime:
		col := w.Col.([]types.Datetime)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDatetimeSlice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]types.Datetime)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDatetimeSlice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_timestamp:
		col := w.Col.([]types.Timestamp)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeTimestampSlice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]types.Timestamp)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeTimestampSlice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	case types.T_decimal64:
		col := w.Col.([]types.Decimal64)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*8)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDecimal64Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal64)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*8], int64(n+cnt)*8)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal64Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}
	case types.T_decimal128:
		col := w.Col.([]types.Decimal128)
		if len(v.Data) == 0 {
			newSize := 8
			for newSize < cnt {
				newSize <<= 1
			}
			data, err := mheap.Alloc(m, int64(newSize)*16)
			if err != nil {
				return err
			}
			v.Ref = w.Ref
			vs := encoding.DecodeDecimal128Slice(data)[:cnt]
			for i, j := 0, 0; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
			v.Data = data
		} else {
			vs := v.Col.([]types.Decimal128)
			n := len(vs)
			if n+cnt > cap(vs) {
				data, err := mheap.Grow(m, v.Data[:n*16], int64(n+cnt)*16)
				if err != nil {
					return err
				}
				mheap.Free(m, v.Data)
				vs = encoding.DecodeDecimal128Slice(data)
				v.Data = data
			}
			vs = vs[:n+cnt]
			for i, j := 0, n; i < len(flags); i++ {
				if flags[i] > 0 {
					vs[j] = col[int(offset)+i]
					j++
				}
			}
			v.Col = vs
		}

	}

	for i, j := 0, uint64(oldLen); i < len(flags); i++ {
		if flags[i] > 0 {
			if nulls.Contains(w.Nsp, uint64(offset)+uint64(i)) {
				nulls.Add(v.Nsp, j)
			}
			j++
		}
	}
	return nil
}

func (v *Vector) Show() ([]byte, error) {
	var buf bytes.Buffer

	switch v.Typ.Oid {
	case types.T_bool:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeBoolSlice(v.Col.([]bool)))
		return buf.Bytes(), nil
	case types.T_int8:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeInt8Slice(v.Col.([]int8)))
		return buf.Bytes(), nil
	case types.T_int16:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeInt16Slice(v.Col.([]int16)))
		return buf.Bytes(), nil
	case types.T_int32:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeInt32Slice(v.Col.([]int32)))
		return buf.Bytes(), nil
	case types.T_int64:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeInt64Slice(v.Col.([]int64)))
		return buf.Bytes(), nil
	case types.T_uint8:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeUint8Slice(v.Col.([]uint8)))
		return buf.Bytes(), nil
	case types.T_uint16:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeUint16Slice(v.Col.([]uint16)))
		return buf.Bytes(), nil
	case types.T_uint32:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeUint32Slice(v.Col.([]uint32)))
		return buf.Bytes(), nil
	case types.T_uint64:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeUint64Slice(v.Col.([]uint64)))
		return buf.Bytes(), nil
	case types.T_float32:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeFloat32Slice(v.Col.([]float32)))
		return buf.Bytes(), nil
	case types.T_float64:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeFloat64Slice(v.Col.([]float64)))
		return buf.Bytes(), nil
	case types.T_date:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeDateSlice(v.Col.([]types.Date)))
		return buf.Bytes(), nil
	case types.T_datetime:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeDatetimeSlice(v.Col.([]types.Datetime)))
		return buf.Bytes(), nil
	case types.T_timestamp:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeTimestampSlice(v.Col.([]types.Timestamp)))
		return buf.Bytes(), nil
	case types.T_sel:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeInt64Slice(v.Col.([]int64)))
		return buf.Bytes(), nil
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		Col := v.Col.(*types.Bytes)
		cnt := int32(len(Col.Offsets))
		buf.Write(encoding.EncodeInt32(cnt))
		if cnt == 0 {
			return buf.Bytes(), nil
		}
		buf.Write(encoding.EncodeUint32Slice(Col.Lengths))
		buf.Write(Col.Data)
		return buf.Bytes(), nil
	case types.T_tuple:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		data, err := encoding.Encode(v.Col.([][]interface{}))
		if err != nil {
			return nil, err
		}
		buf.Write(data)
		return buf.Bytes(), nil
	case types.T_decimal64:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeDecimal64Slice(v.Col.([]types.Decimal64)))
		return buf.Bytes(), nil
	case types.T_decimal128:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeDecimal128Slice(v.Col.([]types.Decimal128)))
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("unsupport encoding type %s", v.Typ.Oid)
	}
}

func (v *Vector) Read(data []byte) error {
	v.Data = data
	typ := encoding.DecodeType(data[:encoding.TypeSize])
	data = data[encoding.TypeSize:]
	v.Typ = typ
	v.Or = true
	switch typ.Oid {
	case types.T_bool:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeBoolSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeBoolSlice(data[size:])
		}
	case types.T_int8:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeInt8Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeInt8Slice(data[size:])
		}
	case types.T_int16:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeInt16Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeInt16Slice(data[size:])
		}
	case types.T_int32:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeInt32Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeInt32Slice(data[size:])
		}
	case types.T_int64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeInt64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeInt64Slice(data[size:])
		}
	case types.T_uint8:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeUint8Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeUint8Slice(data[size:])
		}
	case types.T_uint16:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeUint16Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeUint16Slice(data[size:])
		}
	case types.T_uint32:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeUint32Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeUint32Slice(data[size:])
		}
	case types.T_uint64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeUint64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeUint64Slice(data[size:])
		}
	case types.T_float32:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeFloat32Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeFloat32Slice(data[size:])
		}
	case types.T_float64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeFloat64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeFloat64Slice(data[size:])
		}
	case types.T_date:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeDateSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeDateSlice(data[size:])
		}
	case types.T_datetime:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeDatetimeSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeDatetimeSlice(data[size:])
		}
	case types.T_timestamp:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeTimestampSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeTimestampSlice(data[size:])
		}
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		Col := v.Col.(*types.Bytes)
		Col.Reset()
		size := encoding.DecodeUint32(data)
		data = data[4:]
		if size > 0 {
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			data = data[size:]
		}
		cnt := encoding.DecodeInt32(data)
		if cnt == 0 {
			break
		}
		data = data[4:]
		Col.Offsets = make([]uint32, cnt)
		Col.Lengths = encoding.DecodeUint32Slice(data[:4*cnt])
		Col.Data = data[4*cnt:]
		{
			o := uint32(0)
			for i, n := range Col.Lengths {
				Col.Offsets[i] = o
				o += n
			}
		}
	case types.T_tuple:
		col := v.Col.([][]interface{})
		size := encoding.DecodeUint32(data)
		data = data[4:]
		if size > 0 {
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			data = data[size:]
		}
		cnt := encoding.DecodeInt32(data)
		if cnt == 0 {
			break
		}
		if err := encoding.Decode(data, &col); err != nil {
			return err
		}
		v.Col = col
	case types.T_decimal64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeDecimal64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeDecimal64Slice(data[size:])
		}
	case types.T_decimal128:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Data = data[4:]
			v.Col = encoding.DecodeDecimal128Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Data = data[size:]
			v.Col = encoding.DecodeDecimal128Slice(data[size:])
		}
	}
	return nil
}

func (v *Vector) String() string {
	switch v.Typ.Oid {
	case types.T_bool:
		col := v.Col.([]bool)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int8:
		col := v.Col.([]int8)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int16:
		col := v.Col.([]int16)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int32:
		col := v.Col.([]int32)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int64:
		col := v.Col.([]int64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint8:
		col := v.Col.([]uint8)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint16:
		col := v.Col.([]uint16)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint32:
		col := v.Col.([]uint32)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint64:
		col := v.Col.([]uint64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_float32:
		col := v.Col.([]float32)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_float64:
		col := v.Col.([]float64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_date:
		col := v.Col.([]types.Date)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_datetime:
		col := v.Col.([]types.Datetime)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_timestamp:
		col := v.Col.([]types.Timestamp)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_sel:
		col := v.Col.([]int64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_tuple:
		col := v.Col.([][]interface{})
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_char, types.T_varchar, types.T_json, types.T_blob:
		col := v.Col.(*types.Bytes)
		if len(col.Offsets) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%s\n", col.Get(0))
			}
		}
	case types.T_decimal64:
		col := v.Col.([]types.Decimal64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_decimal128:
		col := v.Col.([]types.Decimal128)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				return "null"
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	}
	return fmt.Sprintf("%v-%s", v.Col, v.Nsp)
}

// GetColumnData get whole column from a vector
func (v *Vector) GetColumnData(selectIndexs []int64, occurCounts []int64, rs []string) error {
	const nullStr = "null"
	typ := v.Typ
	rows := len(rs)
	allData := !nulls.Any(v.Nsp)
	ifSel := len(selectIndexs) != 0

	switch typ.Oid {
	case types.T_bool:
		vs := v.Col.([]bool)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%v", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%v", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_int8:
		vs := v.Col.([]int8)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_int16:
		vs := v.Col.([]int16)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_int32:
		vs := v.Col.([]int32)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_int64:
		vs := v.Col.([]int64)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_uint8:
		vs := v.Col.([]uint8)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_uint16:
		vs := v.Col.([]uint16)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_uint32:
		vs := v.Col.([]uint32)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_uint64:
		vs := v.Col.([]uint64)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_float32:
		vs := v.Col.([]float32)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%f", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%f", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_float64:
		vs := v.Col.([]float64)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%f", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%f", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_char, types.T_varchar, types.T_blob:
		vs := v.Col.(*types.Bytes)
		var i int64
		for i = 0; i < int64(rows); i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = selectIndexs[i]
			}
			if allData {
				rs[i] = string(vs.Get(index))
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = string(vs.Get(index))
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_date:
		vs := v.Col.([]types.Date)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = vs[index].String()
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = vs[index].String()
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_datetime:
		vs := v.Col.([]types.Datetime)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = vs[index].String()
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = vs[index].String()
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_timestamp:
		vs := v.Col.([]types.Timestamp)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = vs[index].String()
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = vs[index].String()
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_decimal64:
		vs := v.Col.([]types.Decimal64)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	case types.T_decimal128:
		vs := v.Col.([]types.Decimal128)
		for i := 0; i < rows; i++ {
			index := i
			count := occurCounts[i]
			if count <= 0 {
				i--
				continue
			}
			if ifSel {
				index = int(selectIndexs[i])
			}
			if allData {
				rs[i] = fmt.Sprintf("%d", vs[index])
			} else {
				if nulls.Contains(v.Nsp, uint64(index)) {
					rs[i] = nullStr
				} else {
					rs[i] = fmt.Sprintf("%d", vs[index])
				}
			}
			for count > 1 {
				count--
				i++
				rs[i] = rs[i-1]
			}
		}
	default:
		return fmt.Errorf("unexpect type %v for function vector.GetColumnData", typ)
	}
	return nil
}
