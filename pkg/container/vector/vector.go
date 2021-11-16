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
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/shuffle"
	"matrixone/pkg/vm/mheap"
	"reflect"
	"strconv"
	"unsafe"
)

func New(typ types.Type) *Vector {
	switch typ.Oid {
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
	case types.T_decimal:
		return &Vector{
			Typ: typ,
			Col: []types.Decimal{},
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
	case types.T_char, types.T_varchar, types.T_json:
		return &Vector{
			Typ: typ,
			Col: &types.Bytes{},
			Nsp: &nulls.Nulls{},
		}
	}
	return nil
}

func Reset(v *Vector) {
	switch v.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json:
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

func Length(v *Vector) int {
	switch v.Typ.Oid {
	case types.T_char, types.T_varchar, types.T_json:
		return len(v.Col.(*types.Bytes).Offsets)
	default:
		hp := *(*reflect.SliceHeader)((*(*emptyInterface)(unsafe.Pointer(&v.Col))).word)
		return hp.Len
	}
}

func SetLength(v *Vector, n int) {
	switch v.Typ.Oid {
	case types.T_int8:
		vs := v.Col.([]int8)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_int16:
		vs := v.Col.([]int16)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_int32:
		vs := v.Col.([]int32)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_int64:
		vs := v.Col.([]int64)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_uint8:
		vs := v.Col.([]uint8)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_uint16:
		vs := v.Col.([]uint16)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_uint32:
		vs := v.Col.([]uint32)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_uint64:
		vs := v.Col.([]uint64)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_float32:
		vs := v.Col.([]float32)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	case types.T_float64:
		vs := v.Col.([]float64)
		m := len(vs)
		v.Col = vs[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
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
	case types.T_char, types.T_varchar, types.T_json:
		vs := v.Col.(*types.Bytes)
		m := len(vs.Offsets)
		vs.Data = vs.Data[:vs.Offsets[n-1]+vs.Lengths[n-1]]
		vs.Offsets = vs.Offsets[:n]
		vs.Lengths = vs.Lengths[:n]
		nulls.RemoveRange(v.Nsp, uint64(n), uint64(m))
	}
}

func Dup(v *Vector, m *mheap.Mheap) (*Vector, error) {
	switch v.Typ.Oid {
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
	case types.T_char, types.T_varchar, types.T_json:
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
	}
	return nil, fmt.Errorf("unsupport type %v", v.Typ)
}

func Window(v *Vector, start, end int, w *Vector) *Vector {
	w.Typ = v.Typ
	switch v.Typ.Oid {
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
	case types.T_char, types.T_varchar, types.T_json:
		w.Col = v.Col.(*types.Bytes).Window(start, end)
		w.Nsp = nulls.Range(v.Nsp, uint64(start), uint64(end), w.Nsp)
	}
	return w
}

func Append(v *Vector, arg interface{}) error {
	switch v.Typ.Oid {
	case types.T_int8:
		v.Col = append(v.Col.([]int8), arg.([]int8)...)
	case types.T_int16:
		v.Col = append(v.Col.([]int16), arg.([]int16)...)
	case types.T_int32:
		v.Col = append(v.Col.([]int32), arg.([]int32)...)
	case types.T_int64:
		v.Col = append(v.Col.([]int64), arg.([]int64)...)
	case types.T_uint8:
		v.Col = append(v.Col.([]uint8), arg.([]uint8)...)
	case types.T_uint16:
		v.Col = append(v.Col.([]uint16), arg.([]uint16)...)
	case types.T_uint32:
		v.Col = append(v.Col.([]uint32), arg.([]uint32)...)
	case types.T_uint64:
		v.Col = append(v.Col.([]uint64), arg.([]uint64)...)
	case types.T_decimal:
		v.Col = append(v.Col.([]types.Decimal), arg.([]types.Decimal)...)
	case types.T_float32:
		v.Col = append(v.Col.([]float32), arg.([]float32)...)
	case types.T_float64:
		v.Col = append(v.Col.([]float64), arg.([]float64)...)
	case types.T_date:
		v.Col = append(v.Col.([]types.Date), arg.([]types.Date)...)
	case types.T_datetime:
		v.Col = append(v.Col.([]types.Datetime), arg.([]types.Datetime)...)
	case types.T_sel:
		v.Col = append(v.Col.([]int64), arg.([]int64)...)
	case types.T_tuple:
		v.Col = append(v.Col.([][]interface{}), arg.([][]interface{})...)
	case types.T_char, types.T_varchar, types.T_json:
		return v.Col.(*types.Bytes).Append(arg.([][]byte))
	}
	return nil
}

func Shrink(v *Vector, sels []int64) {
	switch v.Typ.Oid {
	case types.T_int8:
		vs := v.Col.([]int8)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int16:
		vs := v.Col.([]int16)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int32:
		vs := v.Col.([]int32)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_int64:
		vs := v.Col.([]int64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint8:
		vs := v.Col.([]uint8)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint16:
		vs := v.Col.([]uint16)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint32:
		vs := v.Col.([]uint32)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_uint64:
		vs := v.Col.([]uint64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_float32:
		vs := v.Col.([]float32)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_float64:
		vs := v.Col.([]float64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_sel:
		vs := v.Col.([]int64)
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_tuple:
		vs := v.Col.([][]interface{})
		for i, sel := range sels {
			vs[i] = vs[sel]
		}
		v.Col = vs[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_char, types.T_varchar, types.T_json:
		vs := v.Col.(*types.Bytes)
		for i, sel := range sels {
			vs.Offsets[i] = vs.Offsets[sel]
			vs.Lengths[i] = vs.Lengths[sel]
		}
		vs.Offsets = vs.Offsets[:len(sels)]
		vs.Lengths = vs.Lengths[:len(sels)]
		v.Nsp = nulls.Filter(v.Nsp, sels)
	}
}

func Shuffle(v *Vector, sels []int64, m *mheap.Mheap) error {
	switch v.Typ.Oid {
	case types.T_int8:
		vs := v.Col.([]int8)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt8Slice(data)
		v.Col = shuffle.I8Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_int16:
		vs := v.Col.([]int16)
		data, err := mheap.Alloc(m, int64(len(vs)*2))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt16Slice(data)
		v.Col = shuffle.I16Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_int32:
		vs := v.Col.([]int32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt32Slice(data)
		v.Col = shuffle.I32Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_int64:
		vs := v.Col.([]int64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		v.Col = shuffle.I64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_uint8:
		vs := v.Col.([]uint8)
		data, err := mheap.Alloc(m, int64(len(vs)))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint8Slice(data)
		v.Col = shuffle.Ui8Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_uint16:
		vs := v.Col.([]uint16)
		data, err := mheap.Alloc(m, int64(len(vs)*2))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint16Slice(data)
		v.Col = shuffle.Ui16Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_uint32:
		vs := v.Col.([]uint32)
		data, err := mheap.Alloc(m, int64(len(vs)*4))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint32Slice(data)
		v.Col = shuffle.Ui32Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_uint64:
		vs := v.Col.([]uint64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeUint64Slice(data)
		v.Col = shuffle.Ui64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
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
		mheap.Free(m, data)
	case types.T_sel:
		vs := v.Col.([]int64)
		data, err := mheap.Alloc(m, int64(len(vs)*8))
		if err != nil {
			return err
		}
		ws := encoding.DecodeInt64Slice(data)
		v.Col = shuffle.I64Shuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, data)
	case types.T_tuple:
		vs := v.Col.([][]interface{})
		ws := make([][]interface{}, len(vs))
		v.Col = shuffle.TupleShuffle(vs, ws, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
	case types.T_char, types.T_varchar, types.T_json:
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
		v.Col = shuffle.SShuffle(vs, os, ns, sels)
		v.Nsp = nulls.Filter(v.Nsp, sels)
		mheap.Free(m, odata)
		mheap.Free(m, ndata)
	}
	return nil
}

// v[vi] = w[wi]
func Copy(v, w *Vector, vi, wi int64, m *mheap.Mheap) error {
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
	switch v.Typ.Oid {
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
		}
	case types.T_tuple:
		v.Ref = w.Ref
		vs, ws := v.Col.([][]interface{}), w.Col.([][]interface{})
		vs = append(vs, ws[sel])
		v.Col = vs
	case types.T_char, types.T_varchar, types.T_json:
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
	}
	if nulls.Any(w.Nsp) && nulls.Contains(w.Nsp, uint64(sel)) {
		nulls.Add(v.Nsp, uint64(Length(v)-1))
	}
	return nil
}

func (v *Vector) Show() ([]byte, error) {
	var buf bytes.Buffer

	switch v.Typ.Oid {
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
	case types.T_decimal:
		buf.Write(encoding.EncodeType(v.Typ))
		nb, err := v.Nsp.Show()
		if err != nil {
			return nil, err
		}
		buf.Write(encoding.EncodeUint32(uint32(len(nb))))
		if len(nb) > 0 {
			buf.Write(nb)
		}
		buf.Write(encoding.EncodeDecimalSlice(v.Col.([]types.Decimal)))
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
	case types.T_char, types.T_varchar, types.T_json:
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
	case types.T_int8:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeInt8Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeInt8Slice(data[size:])
		}
	case types.T_int16:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeInt16Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeInt16Slice(data[size:])
		}
	case types.T_int32:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeInt32Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeInt32Slice(data[size:])
		}
	case types.T_int64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeInt64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeInt64Slice(data[size:])
		}
	case types.T_uint8:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeUint8Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeUint8Slice(data[size:])
		}
	case types.T_uint16:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeUint16Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeUint16Slice(data[size:])
		}
	case types.T_uint32:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeUint32Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeUint32Slice(data[size:])
		}
	case types.T_uint64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeUint64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeUint64Slice(data[size:])
		}
	case types.T_decimal:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeDecimalSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeDecimalSlice(data[size:])
		}
	case types.T_float32:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeFloat32Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeFloat32Slice(data[size:])
		}
	case types.T_float64:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeFloat64Slice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeFloat64Slice(data[size:])
		}
	case types.T_date:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeDateSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeDateSlice(data[size:])
		}
	case types.T_datetime:
		size := encoding.DecodeUint32(data)
		if size == 0 {
			v.Col = encoding.DecodeDatetimeSlice(data[4:])
		} else {
			data = data[4:]
			if err := v.Nsp.Read(data[:size]); err != nil {
				return err
			}
			v.Col = encoding.DecodeDatetimeSlice(data[size:])
		}
	case types.T_char, types.T_varchar, types.T_json:
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
	}
	return nil
}

func (v *Vector) String() string {
	switch v.Typ.Oid {
	case types.T_int8:
		col := v.Col.([]int8)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int16:
		col := v.Col.([]int16)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int32:
		col := v.Col.([]int32)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_int64:
		col := v.Col.([]int64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint8:
		col := v.Col.([]uint8)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint16:
		col := v.Col.([]uint16)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint32:
		col := v.Col.([]uint32)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_uint64:
		col := v.Col.([]uint64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_decimal:
		col := v.Col.([]types.Decimal)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_float32:
		col := v.Col.([]float32)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_float64:
		col := v.Col.([]float64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_date:
		col := v.Col.([]types.Date)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_datetime:
		col := v.Col.([]types.Datetime)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_sel:
		col := v.Col.([]int64)
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_tuple:
		col := v.Col.([][]interface{})
		if len(col) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%v", col[0])
			}
		}
	case types.T_char, types.T_varchar, types.T_json:
		col := v.Col.(*types.Bytes)
		if len(col.Offsets) == 1 {
			if nulls.Contains(v.Nsp, 0) {
				fmt.Print("null")
			} else {
				return fmt.Sprintf("%s", col.Data[:col.Lengths[0]])
			}
		}
	}
	return fmt.Sprintf("%v-%s", v.Col, v.Nsp)
}
