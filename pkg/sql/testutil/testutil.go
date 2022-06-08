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

package testutil

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
	"reflect"
	"unsafe"
)

var (
	bt   = types.T_bool.ToType()
	i8   = types.T_int8.ToType()
	i16  = types.T_int16.ToType()
	i32  = types.T_int32.ToType()
	i64  = types.T_int64.ToType()
	u8   = types.T_uint8.ToType()
	u16  = types.T_uint16.ToType()
	u32  = types.T_uint32.ToType()
	u64  = types.T_uint64.ToType()
	f32  = types.T_float32.ToType()
	f64  = types.T_float64.ToType()
	ct   = types.T_char.ToType()
	vc   = types.T_varchar.ToType()
	d64  = types.T_decimal64.ToType()
	d128 = types.T_decimal128.ToType()
	dt   = types.T_date.ToType()
	dti  = types.T_datetime.ToType()
)

type vecType interface {
	constraints.Integer | constraints.Float | bool
}

func makeVector[T vecType](values []T, nsp []uint64, typ types.Type) *vector.Vector {
	vec := vector.New(typ)
	vec.Col = values
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	return vec
}

func makeScalar[T vecType](value T, length int, typ types.Type) *vector.Vector {
	vec := NewProc().AllocScalarVector(typ)
	vec.Length = length
	vec.Col = []T{value}
	return vec
}

func makeStringVector(values []string, nsp []uint64, typ types.Type) *vector.Vector {
	vec := vector.New(typ)
	bs := &types.Bytes{
		Lengths: make([]uint32, len(values)),
		Offsets: make([]uint32, len(values)),
	}
	next := uint32(0)
	if nsp == nil {
		for i, s := range values {
			l := uint32(len(s))
			bs.Data = append(bs.Data, []byte(s)...)
			bs.Lengths[i] = l
			bs.Offsets[i] = next
			next += l
		}
	} else {
		for _, n := range nsp {
			nulls.Add(vec.Nsp, n)
		}
		for i := range values {
			if nulls.Contains(vec.Nsp, uint64(i)) {
				continue
			}
			s := values[i]
			l := uint32(len(s))
			bs.Data = append(bs.Data, []byte(s)...)
			bs.Lengths[i] = l
			bs.Offsets[i] = next
			next += l
		}
	}
	vec.Col = bs
	return vec
}

func NewProc() *process.Process {
	return process.New(mheap.New(guest.New(1<<10, host.New(1<<10))))
}

func MakeScalarNull(length int) *vector.Vector {
	vec := NewProc().AllocScalarNullVector(types.Type{Oid: types.T_any})
	vec.Length = length
	return vec
}

func MakeBoolVector(values []bool) *vector.Vector {
	return makeVector[bool](values, nil, bt)
}

func MakeScalarBool(v bool, length int) *vector.Vector {
	return makeScalar[bool](v, length, bt)
}

func MakeInt64Vector(values []int64, nsp []uint64) *vector.Vector {
	return makeVector[int64](values, nsp, i64)
}

func MakeScalarInt64(v int64, length int) *vector.Vector {
	return makeScalar[int64](v, length, i64)
}

func MakeInt32Vector(values []int32, nsp []uint64) *vector.Vector {
	return makeVector[int32](values, nsp, i32)
}

func MakeScalarInt32(v int32, length int) *vector.Vector {
	return makeScalar[int32](v, length, i32)
}

func MakeInt16Vector(values []int16, nsp []uint64) *vector.Vector {
	return makeVector[int16](values, nsp, i16)
}

func MakeScalarInt16(v int16, length int) *vector.Vector {
	return makeScalar[int16](v, length, i16)
}

func MakeInt8Vector(values []int8, nsp []uint64) *vector.Vector {
	return makeVector[int8](values, nsp, i8)
}

func MakeScalarInt8(v int8, length int) *vector.Vector {
	return makeScalar[int8](v, length, i8)
}

func MakeUint64Vector(values []uint64, nsp []uint64) *vector.Vector {
	return makeVector[uint64](values, nsp, u64)
}

func MakeScalarUint64(v uint64, length int) *vector.Vector {
	return makeScalar[uint64](v, length, u64)
}

func MakeUint32Vector(values []uint32, nsp []uint64) *vector.Vector {
	return makeVector[uint32](values, nsp, u32)
}

func MakeScalarUint32(v uint32, length int) *vector.Vector {
	return makeScalar[uint32](v, length, u32)
}

func MakeUint16Vector(values []uint16, nsp []uint64) *vector.Vector {
	return makeVector[uint16](values, nsp, u16)
}

func MakeScalarUint16(v uint16, length int) *vector.Vector {
	return makeScalar[uint16](v, length, u16)
}

func MakeUint8Vector(values []uint8, nsp []uint64) *vector.Vector {
	return makeVector[uint8](values, nsp, u8)
}

func MakeScalarUint8(v uint8, length int) *vector.Vector {
	return makeScalar[uint8](v, length, u8)
}

func MakeFloat32Vector(values []float32, nsp []uint64) *vector.Vector {
	return makeVector[float32](values, nsp, f32)
}

func MakeScalarFloat32(v float32, length int) *vector.Vector {
	return makeScalar[float32](v, length, f32)
}

func MakeFloat64Vector(values []float64, nsp []uint64) *vector.Vector {
	return makeVector[float64](values, nsp, f64)
}

func MakeScalarFloat64(v float64, length int) *vector.Vector {
	return makeScalar[float64](v, length, f64)
}

func MakeCharVector(values []string, nsp []uint64) *vector.Vector {
	return makeStringVector(values, nsp, ct)
}

func MakeVarcharVector(values []string, nsp []uint64) *vector.Vector {
	return makeStringVector(values, nsp, vc)
}

func MakeDecimal64Vector(values []int64, nsp []uint64) *vector.Vector {
	vec := vector.New(d64)
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	vec.Col = (*types.Decimal64)(unsafe.Pointer(&values))
	return vec
}

func MakeScalarDecimal64(v int64, length int) *vector.Vector {
	vec := NewProc().AllocScalarVector(d64)
	vec.Length = length
	vec.Col = []types.Decimal64{types.Decimal64(v)}
	return vec
}

func MakeDecimal128Vector(values []uint64, nsp []uint64) *vector.Vector {
	vec := vector.New(d128)
	cols := make([]types.Decimal128, len(values))
	if nsp == nil {
		for i, v := range values {
			d := types.InitDecimal128UsingUint(v)
			cols[i] = d
		}
	} else {
		for _, n := range nsp {
			nulls.Add(vec.Nsp, n)
		}
		for i, v := range values {
			if nulls.Contains(vec.Nsp, uint64(i)) {
				continue
			}
			d := types.InitDecimal128UsingUint(v)
			cols[i] = d
		}
	}
	vec.Col = cols
	return vec
}

func MakeScalarDecimal128(v uint64, length int) *vector.Vector {
	vec := NewProc().AllocScalarVector(d128)
	vec.Length = length
	vec.Col = []types.Decimal128{types.InitDecimal128UsingUint(v)}
	return vec
}

func MakeDateVector(values []string, nsp []uint64) *vector.Vector {
	vec := vector.New(dt)
	ds := make([]types.Date, len(values))
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	for i, s := range values {
		if nulls.Contains(vec.Nsp, uint64(i)) {
			continue
		}
		d, err := types.ParseDate(s)
		if err != nil {
			panic(err)
		}
		ds[i] = d
	}
	vec.Col = ds
	return vec
}

func MakeScalarDate(value string, length int) *vector.Vector {
	vec := NewProc().AllocScalarVector(dt)
	vec.Length = length
	d, err := types.ParseDate(value)
	if err != nil {
		panic(err)
	}
	vec.Col = []types.Date{d}
	return vec
}

func MakeDateTimeVector(values []string, nsp []uint64) *vector.Vector {
	vec := vector.New(dti)
	ds := make([]types.Datetime, len(values))
	for _, n := range nsp {
		nulls.Add(vec.Nsp, n)
	}
	for i, s := range values {
		if nulls.Contains(vec.Nsp, uint64(i)) {
			continue
		}
		d, err := types.ParseDatetime(s)
		if err != nil {
			panic(err)
		}
		ds[i] = d
	}
	vec.Col = ds
	return vec
}

func MakeScalarDateTime(value string, length int) *vector.Vector {
	vec := NewProc().AllocScalarVector(dti)
	vec.Length = length
	d, err := types.ParseDatetime(value)
	if err != nil {
		panic(err)
	}
	vec.Col = []types.Datetime{d}
	return vec
}

func CompareVectors(expected *vector.Vector, got *vector.Vector) bool {
	if expected.IsScalar() {
		if !got.IsScalar() {
			return false
		}
		if expected.IsScalarNull() {
			return got.IsScalarNull()
		} else {
			return reflect.DeepEqual(expected.Col, got.Col)
		}
	} else {
		if got.IsScalar() {
			return false
		}
		// expected length and got length
		expectedLength := vector.Length(expected)
		gotLength := vector.Length(got)
		if expectedLength != gotLength {
			return false
		}
		if nulls.Any(expected.Nsp) {
			var k uint64 = 0
			if !nulls.Any(got.Nsp) {
				return false
			}
			for k = 0; k < uint64(expectedLength); k++ {
				c1 := nulls.Contains(expected.Nsp, k)
				c2 := nulls.Contains(got.Nsp, k)
				if c1 != c2 {
					return false
				}
			}
		}
		return reflect.DeepEqual(expected.Col, got.Col)
	}
}
