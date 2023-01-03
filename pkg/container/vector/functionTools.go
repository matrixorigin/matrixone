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

type FunctionResult[T types.FixedSizeT] struct {
	vec *Vector
	mp  *mpool.MPool
}

type FunctionParameter[T types.FixedSizeT] struct {
	typ          types.Type
	sourceVector *Vector
	isScalar     bool
	values       []T
	strValues    []types.Varlena
	area         []byte

	// nulls information related.
	containsNull bool
	nullMap      *bitmap.Bitmap
}

type FunctionResultWrapper interface {
	GetResultVector() *Vector
	Free()
}

func NewResultFunc[T types.FixedSizeT](v *Vector, mp *mpool.MPool) *FunctionResult[T] {
	return &FunctionResult[T]{
		vec: v,
		mp:  mp,
	}
}

func (fr *FunctionResult[T]) Append(val T, isnull bool) error {
	return fr.vec.Append(val, isnull, fr.mp)
}

func (fr *FunctionResult[T]) AppendStr(val []byte, isnull bool) error {
	return fr.vec.Append(val, isnull, fr.mp)
}

func (fr *FunctionResult[T]) GetType() types.Type {
	return fr.vec.Typ
}

func (fr *FunctionResult[T]) SetFromParameter(fp *FunctionParameter[T]) {
	// clean the old memory
	if fr.vec != fp.sourceVector {
		fr.Free()
	}
	fr.vec = fp.sourceVector
}

func (fr *FunctionResult[T]) GetResultVector() *Vector {
	return fr.vec
}

func (fr *FunctionResult[T]) ConvertToParameter() FunctionParameter[T] {
	return GenerateFunctionFixedTypeParameter[T](fr.vec)
}

func (fr *FunctionResult[T]) ConvertToStrParameter() FunctionParameter[types.Varlena] {
	return GenerateFunctionStrParameter(fr.vec)
}

func (fr *FunctionResult[T]) Free() {
	fr.vec.Free(fr.mp)
}

func (fp *FunctionParameter[T]) GetSourceVector() *Vector {
	return fp.sourceVector
}

// GetValue return the nth value and if it's null or not.
func (fp *FunctionParameter[T]) GetValue(idx uint64) (value T, isNull bool) {
	if fp.isScalar {
		idx = 0
	}
	if fp.containsNull && fp.nullMap.Contains(idx) {
		return value, true
	}
	return fp.values[idx], false
}

func (fp *FunctionParameter[T]) IsBin() bool {
	return fp.sourceVector.GetIsBin()
}

func (fp *FunctionParameter[T]) UnSafeGetAllValue() []T {
	return fp.values
}

// GetStrValue return nth value of string parameter and if it's null or not.
func (fp *FunctionParameter[T]) GetStrValue(idx uint64) (value []byte, isNull bool) {
	if fp.isScalar {
		idx = 0
	}
	if fp.containsNull && fp.nullMap.Contains(idx) {
		return nil, true
	}
	vrl := fp.strValues[idx]
	return vrl.GetByteSlice(fp.area), false
}

func (fp *FunctionParameter[T]) GetType() types.Type {
	return fp.typ
}

// GenerateFunctionFixedTypeParameter generate a structure which is easy to get value.
func GenerateFunctionFixedTypeParameter[T types.FixedSizeT](v *Vector) FunctionParameter[T] {
	var containsNull = false
	var nullMap *bitmap.Bitmap
	cols := MustTCols[T](v)
	if v.IsScalarNull() && len(cols) == 0 {
		cols = make([]T, 1)
	}
	if v.Nsp != nil && v.Nsp.Np != nil && v.Nsp.Np.Len() > 0 {
		containsNull = true
		nullMap = v.Nsp.Np
	}
	return FunctionParameter[T]{
		typ:          v.GetType(),
		sourceVector: v,
		isScalar:     v.IsScalar(),
		values:       cols,
		containsNull: containsNull,
		nullMap:      nullMap,
	}
}

func GenerateFunctionStrParameter(v *Vector) FunctionParameter[types.Varlena] {
	var containsNull = false
	var nullMap *bitmap.Bitmap
	cols := MustTCols[types.Varlena](v)
	area := v.area
	if v.IsScalarNull() && len(cols) == 0 {
		cols = make([]types.Varlena, 1)
		area = make([]byte, 0)
	}
	if v.Nsp != nil && v.Nsp.Np != nil && v.Nsp.Np.Len() > 0 {
		containsNull = true
		nullMap = v.Nsp.Np
	}
	return FunctionParameter[types.Varlena]{
		typ:          v.GetType(),
		sourceVector: v,
		isScalar:     v.IsScalar(),
		area:         area,
		strValues:    cols,
		containsNull: containsNull,
		nullMap:      nullMap,
	}
}

func NewFunctionResultWrapper(typ types.Type, mp *mpool.MPool, isConst bool, length int) FunctionResultWrapper {
	var v *Vector
	if isConst {
		v = NewConst(typ, length)
	} else {
		v = New(typ)
	}

	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_blob, types.T_text:
		// IF STRING type.
		return NewResultFunc[types.Varlena](v, mp)
	case types.T_json:
		return NewResultFunc[types.Varlena](v, mp)
	}

	// Pre allocate the memory
	// XXX PreAllocType has BUG. It only shrinks the cols.
	//v = PreAllocType(typ, 0, length, mp)
	//SetLength(v, 0)
	switch typ.Oid {
	case types.T_bool:
		return NewResultFunc[bool](v, mp)
	case types.T_int8:
		return NewResultFunc[int8](v, mp)
	case types.T_int16:
		return NewResultFunc[int16](v, mp)
	case types.T_int32:
		return NewResultFunc[int32](v, mp)
	case types.T_int64:
		return NewResultFunc[int64](v, mp)
	case types.T_uint8:
		return NewResultFunc[uint8](v, mp)
	case types.T_uint16:
		return NewResultFunc[uint16](v, mp)
	case types.T_uint32:
		return NewResultFunc[uint32](v, mp)
	case types.T_uint64:
		return NewResultFunc[uint64](v, mp)
	case types.T_float32:
		return NewResultFunc[float32](v, mp)
	case types.T_float64:
		return NewResultFunc[float64](v, mp)
	case types.T_date:
		return NewResultFunc[types.Date](v, mp)
	case types.T_datetime:
		return NewResultFunc[types.Datetime](v, mp)
	case types.T_time:
		return NewResultFunc[types.Time](v, mp)
	case types.T_timestamp:
		return NewResultFunc[types.Timestamp](v, mp)
	case types.T_decimal64:
		return NewResultFunc[types.Decimal64](v, mp)
	case types.T_decimal128:
		return NewResultFunc[types.Decimal128](v, mp)
	case types.T_TS:
		return NewResultFunc[types.TS](v, mp)
	case types.T_Rowid:
		return NewResultFunc[types.Rowid](v, mp)
	case types.T_uuid:
		return NewResultFunc[types.Uuid](v, mp)
	}
	panic(fmt.Sprintf("unexpected type %s for function result", typ))
}
