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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// FunctionParameterWrapper is generated from a vector.
// It hides the relevant details of vector (like scalar and contain null or not.)
// and provides a series of methods to get values.
type FunctionParameterWrapper[T types.FixedSizeT] interface {
	// GetType will return the type info of wrapped parameter.
	GetType() types.Type

	// GetSourceVector return the source vector.
	GetSourceVector() *Vector

	// GetValue return the Idx th value and if it's null or not.
	// watch that, if str type, GetValue will return the []types.Varlena directly.
	GetValue(idx uint64) (T, bool)

	// GetStrValue return the Idx th string value and if it's null or not.
	GetStrValue(idx uint64) ([]byte, bool)

	// UnSafeGetAllValue return all the values.
	// please use it carefully because we didn't check the null situation.
	UnSafeGetAllValue() []T
}

var _ FunctionParameterWrapper[int64] = &FunctionParameterNormal[int64]{}
var _ FunctionParameterWrapper[int64] = &FunctionParameterWithoutNull[int64]{}
var _ FunctionParameterWrapper[int64] = &FunctionParameterScalar[int64]{}
var _ FunctionParameterWrapper[int64] = &FunctionParameterScalarNull[int64]{}

func GenerateFunctionFixedTypeParameter[T types.FixedSizeT](v *Vector) FunctionParameterWrapper[T] {
	t := v.GetType()
	if v.IsConstNull() {
		return &FunctionParameterScalarNull[T]{
			typ:          *t,
			sourceVector: v,
		}
	}
	cols := MustFixedCol[T](v)
	if v.IsConst() {
		return &FunctionParameterScalar[T]{
			typ:          *t,
			sourceVector: v,
			scalarValue:  cols[0],
		}
	}
	if !v.nsp.EmptyByFlag() {
		return &FunctionParameterNormal[T]{
			typ:          *t,
			sourceVector: v,
			values:       cols,
			nullMap:      v.GetNulls().GetBitmap(),
		}
	}
	return &FunctionParameterWithoutNull[T]{
		typ:          *t,
		sourceVector: v,
		values:       cols,
	}
}

func GenerateFunctionStrParameter(v *Vector) FunctionParameterWrapper[types.Varlena] {
	t := v.GetType()
	if v.IsConstNull() {
		return &FunctionParameterScalarNull[types.Varlena]{
			typ:          *t,
			sourceVector: v,
		}
	}
	cols := v.col.([]types.Varlena)
	if v.IsConst() {
		return &FunctionParameterScalar[types.Varlena]{
			typ:          *t,
			sourceVector: v,
			scalarValue:  cols[0],
			scalarStr:    cols[0].GetByteSlice(v.area),
		}
	}
	if !v.GetNulls().EmptyByFlag() {
		return &FunctionParameterNormal[types.Varlena]{
			typ:          *t,
			sourceVector: v,
			strValues:    cols,
			area:         v.area,
			nullMap:      v.GetNulls().GetBitmap(),
		}
	}
	return &FunctionParameterWithoutNull[types.Varlena]{
		typ:          *t,
		sourceVector: v,
		strValues:    cols,
		area:         v.area,
	}
}

// FunctionParameterNormal is a wrapper of normal vector which
// may contains null value.
type FunctionParameterNormal[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	values       []T
	strValues    []types.Varlena
	area         []byte
	nullMap      *bitmap.Bitmap
}

func (p *FunctionParameterNormal[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterNormal[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterNormal[T]) GetValue(idx uint64) (value T, isNull bool) {
	if p.nullMap.Contains(idx) {
		return value, true
	}
	return p.values[idx], false
}

func (p *FunctionParameterNormal[T]) GetStrValue(idx uint64) (value []byte, isNull bool) {
	if p.nullMap.Contains(idx) {
		return nil, true
	}
	return p.strValues[idx].GetByteSlice(p.area), false
}

func (p *FunctionParameterNormal[T]) UnSafeGetAllValue() []T {
	return p.values
}

// FunctionParameterWithoutNull is a wrapper of normal vector but
// without null value.
type FunctionParameterWithoutNull[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	values       []T
	strValues    []types.Varlena
	area         []byte
}

func (p *FunctionParameterWithoutNull[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterWithoutNull[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterWithoutNull[T]) GetValue(idx uint64) (T, bool) {
	return p.values[idx], false
}

func (p *FunctionParameterWithoutNull[T]) GetStrValue(idx uint64) ([]byte, bool) {
	return p.strValues[idx].GetByteSlice(p.area), false
}

func (p *FunctionParameterWithoutNull[T]) UnSafeGetAllValue() []T {
	return p.values
}

// FunctionParameterScalar is a wrapper of scalar vector.
type FunctionParameterScalar[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	scalarValue  T
	scalarStr    []byte
}

func (p *FunctionParameterScalar[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterScalar[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterScalar[T]) GetValue(_ uint64) (T, bool) {
	return p.scalarValue, false
}

func (p *FunctionParameterScalar[T]) GetStrValue(_ uint64) ([]byte, bool) {
	return p.scalarStr, false
}

func (p *FunctionParameterScalar[T]) UnSafeGetAllValue() []T {
	return []T{p.scalarValue}
}

// FunctionParameterScalarNull is a wrapper of scalar null vector.
type FunctionParameterScalarNull[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
}

func (p *FunctionParameterScalarNull[T]) GetType() types.Type {
	return p.typ
}

func (p *FunctionParameterScalarNull[T]) GetSourceVector() *Vector {
	return p.sourceVector
}

func (p *FunctionParameterScalarNull[T]) GetValue(_ uint64) (value T, isNull bool) {
	return value, true
}

func (p *FunctionParameterScalarNull[T]) GetStrValue(_ uint64) ([]byte, bool) {
	return nil, true
}

func (p *FunctionParameterScalarNull[T]) UnSafeGetAllValue() []T {
	return nil
}

type FunctionResultWrapper interface {
	GetResultVector() *Vector
	Free()
}

var _ FunctionResultWrapper = &FunctionResult[int64]{}

type FunctionResult[T types.FixedSizeT] struct {
	vec *Vector
	mp  *mpool.MPool
}

func MustFunctionResult[T types.FixedSizeT](wrapper FunctionResultWrapper) *FunctionResult[T] {
	if fr, ok := wrapper.(*FunctionResult[T]); ok {
		return fr
	}
	panic("wrong type for FunctionResultWrapper")
}

func newResultFunc[T types.FixedSizeT](v *Vector, mp *mpool.MPool) *FunctionResult[T] {
	return &FunctionResult[T]{
		vec: v,
		mp:  mp,
	}
}

func (fr *FunctionResult[T]) Append(val T, isnull bool) error {
	if !fr.vec.IsConst() {
		return AppendFixed(fr.vec, val, isnull, fr.mp)
	} else if !isnull {
		return SetConstFixed(fr.vec, val, fr.vec.Length(), fr.mp)
	}
	return nil
}

func (fr *FunctionResult[T]) AppendBytes(val []byte, isnull bool) error {
	if !fr.vec.IsConst() {
		return AppendBytes(fr.vec, val, isnull, fr.mp)
	} else if !isnull {
		return SetConstBytes(fr.vec, val, fr.vec.Length(), fr.mp)
	}
	return nil
}

func (fr *FunctionResult[T]) GetType() types.Type {
	return *fr.vec.GetType()
}

func (fr *FunctionResult[T]) SetFromParameter(fp FunctionParameterWrapper[T]) {
	// clean the old memory
	if fr.vec != fp.GetSourceVector() {
		fr.Free()
	}
	fr.vec = fp.GetSourceVector()
}

func (fr *FunctionResult[T]) GetResultVector() *Vector {
	return fr.vec
}

func (fr *FunctionResult[T]) ConvertToParameter() FunctionParameterWrapper[T] {
	return GenerateFunctionFixedTypeParameter[T](fr.vec)
}

func (fr *FunctionResult[T]) ConvertToStrParameter() FunctionParameterWrapper[types.Varlena] {
	return GenerateFunctionStrParameter(fr.vec)
}

func (fr *FunctionResult[T]) Free() {
	fr.vec.Free(fr.mp)
}

func NewFunctionResultWrapper(typ types.Type, mp *mpool.MPool, isConst bool, length int) FunctionResultWrapper {
	var v *Vector
	if isConst {
		v = NewConstNull(typ, length, mp)
	} else {
		v = NewVec(typ)
		v.PreExtend(length, mp)
	}

	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary:
		// IF STRING type.
		return newResultFunc[types.Varlena](v, mp)
	case types.T_json:
		return newResultFunc[types.Varlena](v, mp)
	}

	// Pre allocate the memory
	// XXX PreAllocType has BUG. It only shrinks the cols.
	//v = PreAllocType(typ, 0, length, mp)
	//SetLength(v, 0)
	switch typ.Oid {
	case types.T_bool:
		return newResultFunc[bool](v, mp)
	case types.T_int8:
		return newResultFunc[int8](v, mp)
	case types.T_int16:
		return newResultFunc[int16](v, mp)
	case types.T_int32:
		return newResultFunc[int32](v, mp)
	case types.T_int64:
		return newResultFunc[int64](v, mp)
	case types.T_uint8:
		return newResultFunc[uint8](v, mp)
	case types.T_uint16:
		return newResultFunc[uint16](v, mp)
	case types.T_uint32:
		return newResultFunc[uint32](v, mp)
	case types.T_uint64:
		return newResultFunc[uint64](v, mp)
	case types.T_float32:
		return newResultFunc[float32](v, mp)
	case types.T_float64:
		return newResultFunc[float64](v, mp)
	case types.T_date:
		return newResultFunc[types.Date](v, mp)
	case types.T_datetime:
		return newResultFunc[types.Datetime](v, mp)
	case types.T_time:
		return newResultFunc[types.Time](v, mp)
	case types.T_timestamp:
		return newResultFunc[types.Timestamp](v, mp)
	case types.T_decimal64:
		return newResultFunc[types.Decimal64](v, mp)
	case types.T_decimal128:
		return newResultFunc[types.Decimal128](v, mp)
	case types.T_TS:
		return newResultFunc[types.TS](v, mp)
	case types.T_Rowid:
		return newResultFunc[types.Rowid](v, mp)
	case types.T_Blockid:
		return newResultFunc[types.Blockid](v, mp)
	case types.T_uuid:
		return newResultFunc[types.Uuid](v, mp)
	}
	panic(fmt.Sprintf("unexpected type %s for function result", typ))
}
