// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"bytes"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"golang.org/x/exp/constraints"
)

func betweenImpl(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int, selectList *FunctionSelectList) error {
	paramType := parameters[0].GetType()
	rs := vector.MustFunctionResult[bool](result)
	switch paramType.Oid {
	case types.T_bool:
		return opBetweenBool(parameters, rs, proc, length)
	case types.T_bit:
		return opBetweenFixed[uint64](parameters, rs, proc, length)
	case types.T_int8:
		return opBetweenFixed[int8](parameters, rs, proc, length)
	case types.T_int16:
		return opBetweenFixed[int16](parameters, rs, proc, length)
	case types.T_int32:
		return opBetweenFixed[int32](parameters, rs, proc, length)
	case types.T_int64:
		return opBetweenFixed[int64](parameters, rs, proc, length)
	case types.T_uint8:
		return opBetweenFixed[uint8](parameters, rs, proc, length)
	case types.T_uint16:
		return opBetweenFixed[uint16](parameters, rs, proc, length)
	case types.T_uint32:
		return opBetweenFixed[uint32](parameters, rs, proc, length)
	case types.T_uint64:
		return opBetweenFixed[uint64](parameters, rs, proc, length)
	case types.T_float32:
		return opBetweenFixed[float32](parameters, rs, proc, length)
	case types.T_float64:
		return opBetweenFixed[float64](parameters, rs, proc, length)
	case types.T_date:
		return opBetweenFixed[types.Date](parameters, rs, proc, length)
	case types.T_datetime:
		return opBetweenFixed[types.Datetime](parameters, rs, proc, length)
	case types.T_time:
		return opBetweenFixed[types.Time](parameters, rs, proc, length)
	case types.T_timestamp:
		return opBetweenFixed[types.Timestamp](parameters, rs, proc, length)

	case types.T_uuid:
		return opBetweenFixedWithFn(parameters, rs, proc, length, func(lhs, rhs types.Uuid) bool {
			return types.CompareUuid(lhs, rhs) <= 0
		})
	case types.T_decimal64:
		return opBetweenFixedWithFn(parameters, rs, proc, length, func(lhs, rhs types.Decimal64) bool {
			return lhs.Compare(rhs) <= 0
		})
	case types.T_decimal128:
		return opBetweenFixedWithFn(parameters, rs, proc, length, func(lhs, rhs types.Decimal128) bool {
			return lhs.Compare(rhs) <= 0
		})
	case types.T_Rowid:
		return opBetweenFixedWithFn(parameters, rs, proc, length, func(lhs, rhs types.Rowid) bool {
			return lhs.Le(rhs)
		})

	case types.T_char, types.T_varchar, types.T_blob, types.T_text, types.T_binary, types.T_varbinary, types.T_datalink:
		return opBetweenBytes(parameters, rs, proc, length)
	}

	panic("unreached code")
}

func opBetweenBool(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	p0 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[0])
	p1 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[1])
	p2 := vector.GenerateFunctionFixedTypeParameter[bool](parameters[2])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	// The lower and upper bound of BETWEEN must be non-null constants, or it should be collapsed to "a >= b and a <= c"
	lb, _ := p1.GetValue(0)
	ub, _ := p2.GetValue(0)
	alwaysTrue := lb != ub

	if parameters[0].IsConst() {
		v0, null0 := p0.GetValue(0)
		if null0 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			if alwaysTrue {
				for i := 0; i < length; i++ {
					rss[i] = true
				}
			}
			r := lb == v0
			for i := 0; i < length; i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p0.WithAnyNullValue() {
		nulls.Set(rsVec.GetNulls(), parameters[0].GetNulls())
		for i := 0; i < length; i++ {
			v0, null0 := p0.GetValue(uint64(i))
			if null0 {
				continue
			}
			rss[i] = alwaysTrue || lb == v0
		}
		return nil
	}

	if alwaysTrue {
		for i := 0; i < length; i++ {
			rss[i] = true
		}
	} else {
		for i := 0; i < length; i++ {
			v0, _ := p0.GetValue(uint64(i))
			rss[i] = lb == v0
		}
	}
	return nil
}

func opBetweenFixed[T constraints.Integer | constraints.Float](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	p0 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[2])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	// The lower and upper bound of BETWEEN must be non-null constants, or it should be collapsed to "a >= b and a <= c"
	lb, _ := p1.GetValue(0)
	ub, _ := p2.GetValue(0)

	if parameters[0].IsConst() {
		v0, null0 := p0.GetValue(0)
		if null0 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := v0 >= lb && v0 <= ub
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p0.WithAnyNullValue() {
		nulls.Set(rsVec.GetNulls(), parameters[0].GetNulls())
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			v0, null0 := p0.GetValue(i)
			if null0 {
				continue
			}
			rss[i] = v0 >= lb && v0 <= ub
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v0, _ := p0.GetValue(i)
		rss[i] = v0 >= lb && v0 <= ub
	}
	return nil
}

func opBetweenFixedWithFn[T types.FixedSizeTExceptStrType](
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
	lessEqualFn func(v1, v2 T) bool,
) error {
	p0 := vector.GenerateFunctionFixedTypeParameter[T](parameters[0])
	p1 := vector.GenerateFunctionFixedTypeParameter[T](parameters[1])
	p2 := vector.GenerateFunctionFixedTypeParameter[T](parameters[2])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	// The lower and upper bound of BETWEEN must be non-null constants, or it should be collapsed to "a >= b and a <= c"
	lb, _ := p1.GetValue(0)
	ub, _ := p2.GetValue(0)

	if parameters[0].IsConst() {
		v0, null0 := p0.GetValue(0)
		if null0 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := lessEqualFn(lb, v0) && lessEqualFn(v0, ub)
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p0.WithAnyNullValue() {
		nulls.Set(rsVec.GetNulls(), parameters[0].GetNulls())
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			v0, null0 := p0.GetValue(i)
			if null0 {
				continue
			}
			rss[i] = lessEqualFn(lb, v0) && lessEqualFn(v0, ub)
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v0, _ := p0.GetValue(i)
		rss[i] = lessEqualFn(lb, v0) && lessEqualFn(v0, ub)
	}
	return nil
}

func opBetweenBytes(
	parameters []*vector.Vector,
	result vector.FunctionResultWrapper,
	_ *process.Process,
	length int,
) error {
	p0 := vector.GenerateFunctionStrParameter(parameters[0])
	p1 := vector.GenerateFunctionStrParameter(parameters[1])
	p2 := vector.GenerateFunctionStrParameter(parameters[2])
	rs := vector.MustFunctionResult[bool](result)
	rsVec := rs.GetResultVector()
	rss := vector.MustFixedCol[bool](rsVec)

	// The lower and upper bound of BETWEEN must be non-null constants, or it should be collapsed to "a >= b and a <= c"
	lb, _ := p1.GetStrValue(0)
	ub, _ := p2.GetStrValue(0)

	if parameters[0].IsConst() {
		v0, null0 := p0.GetStrValue(0)
		if null0 {
			nulls.AddRange(rsVec.GetNulls(), 0, uint64(length))
		} else {
			r := bytes.Compare(v0, lb) >= 0 && bytes.Compare(v0, ub) <= 0
			rowCount := uint64(length)
			for i := uint64(0); i < rowCount; i++ {
				rss[i] = r
			}
		}
		return nil
	}

	// basic case.
	if p0.WithAnyNullValue() {
		nulls.Set(rsVec.GetNulls(), parameters[0].GetNulls())
		rowCount := uint64(length)
		for i := uint64(0); i < rowCount; i++ {
			v0, null0 := p0.GetStrValue(i)
			if null0 {
				continue
			}
			rss[i] = bytes.Compare(v0, lb) >= 0 && bytes.Compare(v0, ub) <= 0
		}
		return nil
	}

	rowCount := uint64(length)
	for i := uint64(0); i < rowCount; i++ {
		v0, _ := p0.GetStrValue(i)
		rss[i] = bytes.Compare(v0, lb) >= 0 && bytes.Compare(v0, ub) <= 0
	}
	return nil
}
