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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

// assert related method
func MustTVector[T types.Element](v AnyVector) *Vector[T] {
	if vt, ok := any(v).(*Vector[T]); ok {
		return vt
	}
	panic(fmt.Sprintf("unexpected type assert for AnyVector"))
}

func SetCol[T types.Element](v *Vector[T], col []T) {
	v.Col = col
}

// scalar vector related methods.
func (v *Vector[T]) IsScalar() bool {
	return v.IsConst
}

func (v *Vector[T]) IsScalarNull() bool {
	return v.IsConst && nulls.Any(v.Nsp)
}

// ConstVectorIsNull checks whether a const vector is null
func (v *Vector[T]) ConstVectorIsNull() bool {
	return v.Nsp != nil && nulls.Contains(v.Nsp, 0)
}

// generic vector related methods
func NewWithType(typ types.Type) AnyVector {
	switch typ.Oid {
	// number
	case types.T_int8:
		return New[types.Int8](typ)
	case types.T_int16:
		return New[types.Int16](typ)
	case types.T_int32:
		return New[types.Int32](typ)
	case types.T_int64:
		return New[types.Int64](typ)
	case types.T_uint8:
		return New[types.UInt8](typ)
	case types.T_uint16:
		return New[types.UInt16](typ)
	case types.T_uint32:
		return New[types.UInt32](typ)
	case types.T_uint64:
		return New[types.UInt64](typ)
	case types.T_float32:
		return New[types.Float32](typ)
	case types.T_float64:
		return New[types.Float64](typ)

	// string
	case types.T_varchar, types.T_char:
		return New[types.String](typ)

	// decimal
	case types.T_decimal64:
		return New[types.Decimal64](typ)
	case types.T_decimal128:
		return New[types.Decimal128](typ)

	// time
	case types.T_date:
		return New[types.Date](typ)
	case types.T_datetime:
		return New[types.Datetime](typ)
	case types.T_timestamp:
		return New[types.Timestamp](typ)
	}
	panic(fmt.Sprintf("unexpected parameters [%v] for function NewWithType", typ))
}

func NewConst(typ types.Type) AnyVector {
	v := NewWithType(typ)
	v.SetConst()
	return v
}
