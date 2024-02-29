// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// sArg is the interface of single column aggregation's argument.
// it is used to get value from input vector.
type sArg interface {
	prepare(*vector.Vector)

	// reset and collect the resources for reuse.
	collect()
}

var (
	_ = sArg(&sFixedArg[int8]{})
	_ = sArg(&sBytesArg{})
)

// sFixedArg and sBytesArg were used to get value from input vector.
type sFixedArg[T types.FixedSizeTExceptStrType] struct {
	w vector.FunctionParameterWrapper[T]
}
type sBytesArg struct {
	w vector.FunctionParameterWrapper[types.Varlena]
}

func (arg *sFixedArg[T]) prepare(v *vector.Vector) {
	arg.w = vector.GenerateFunctionFixedTypeParameter[T](v)
}
func (arg *sFixedArg[T]) collect() {}

func (arg *sBytesArg) prepare(v *vector.Vector) {
	arg.w = vector.GenerateFunctionStrParameter(v)
}
func (arg *sBytesArg) collect() {}

// mArg1 and mArg2 are the interface of multi columns aggregation's argument.
// mArg1 for agg whose return type is a fixed length type except string.
// mArg2 for agg whose return type is a byte type.
type mArg1[ret types.FixedSizeTExceptStrType] interface {
	prepare(v *vector.Vector)

	// fill one row of input vector into the group.
	doRowFill(aggImp multiAggPrivateStructure1[ret], row uint64) error

	// fill is func(multiAggPrivateStructure1[T], value), value is the arg type.
	cacheFill(fill any, fillNull func(multiAggPrivateStructure1[ret]))
}
type mArg2 interface {
	prepare(v *vector.Vector)

	doRowFill(aggImp multiAggPrivateStructure2, row uint64) error

	cacheFill(fill any, fillNull func(multiAggPrivateStructure2))
}

var (
	_ = mArg1[int64](&mArg1Fixed[int64, int64]{})
	_ = mArg1[int64](&mArg1Bytes[int64]{})
	_ = mArg2(&mArg2Fixed[int64]{})
	_ = mArg2(&mArg2Bytes{})
)

func newArgumentOfMultiAgg1[ret types.FixedSizeTExceptStrType](paramType types.Type) mArg1[ret] {
	if paramType.IsVarlen() {
		return &mArg1Bytes[ret]{}
	}

	switch paramType.Oid {
	case types.T_bool:
		return &mArg1Fixed[ret, bool]{}
	case types.T_int8:
		return &mArg1Fixed[ret, int8]{}
	case types.T_int16:
		return &mArg1Fixed[ret, int16]{}
	case types.T_int32:
		return &mArg1Fixed[ret, int32]{}
	case types.T_int64:
		return &mArg1Fixed[ret, int64]{}
	case types.T_uint8:
		return &mArg1Fixed[ret, uint8]{}
	case types.T_uint16:
		return &mArg1Fixed[ret, uint16]{}
	case types.T_uint32:
		return &mArg1Fixed[ret, uint32]{}
	case types.T_uint64:
		return &mArg1Fixed[ret, uint64]{}
	case types.T_float32:
		return &mArg1Fixed[ret, float32]{}
	case types.T_float64:
		return &mArg1Fixed[ret, float64]{}
	case types.T_decimal64:
		return &mArg1Fixed[ret, types.Decimal64]{}
	case types.T_decimal128:
		return &mArg1Fixed[ret, types.Decimal128]{}
	case types.T_date:
		return &mArg1Fixed[ret, types.Date]{}
	case types.T_datetime:
		return &mArg1Fixed[ret, types.Datetime]{}
	case types.T_time:
		return &mArg1Fixed[ret, types.Time]{}
	case types.T_timestamp:
		return &mArg1Fixed[ret, types.Timestamp]{}
	}
	panic("unsupported parameter type for multiAggFuncExec1")
}

func newArgumentOfMultiAgg2(paramType types.Type) mArg2 {
	if paramType.IsVarlen() {
		return &mArg2Bytes{}
	}

	switch paramType.Oid {
	case types.T_bool:
		return &mArg2Fixed[bool]{}
	case types.T_int8:
		return &mArg2Fixed[int8]{}
	case types.T_int16:
		return &mArg2Fixed[int16]{}
	case types.T_int32:
		return &mArg2Fixed[int32]{}
	case types.T_int64:
		return &mArg2Fixed[int64]{}
	case types.T_uint8:
		return &mArg2Fixed[uint8]{}
	case types.T_uint16:
		return &mArg2Fixed[uint16]{}
	case types.T_uint32:
		return &mArg2Fixed[uint32]{}
	case types.T_uint64:
		return &mArg2Fixed[uint64]{}
	case types.T_float32:
		return &mArg2Fixed[float32]{}
	case types.T_float64:
		return &mArg2Fixed[float64]{}
	case types.T_decimal64:
		return &mArg2Fixed[types.Decimal64]{}
	case types.T_decimal128:
		return &mArg2Fixed[types.Decimal128]{}
	case types.T_date:
		return &mArg2Fixed[types.Date]{}
	case types.T_datetime:
		return &mArg2Fixed[types.Datetime]{}
	case types.T_time:
		return &mArg2Fixed[types.Time]{}
	case types.T_timestamp:
		return &mArg2Fixed[types.Timestamp]{}
	}
	panic("unsupported parameter type for multiAggFuncExec2")
}

type mArg1Fixed[ret types.FixedSizeTExceptStrType, arg types.FixedSizeTExceptStrType] struct {
	w vector.FunctionParameterWrapper[arg]

	fill     func(multiAggPrivateStructure1[ret], arg)
	fillNull func(multiAggPrivateStructure1[ret])
}

func (a *mArg1Fixed[ret, arg]) prepare(v *vector.Vector) {
	a.w = vector.GenerateFunctionFixedTypeParameter[arg](v)
}

func (a *mArg1Fixed[ret, arg]) doRowFill(aggImp multiAggPrivateStructure1[ret], row uint64) error {
	v, null := a.w.GetValue(row)
	if null {
		a.fillNull(aggImp)
	} else {
		a.fill(aggImp, v)
	}
	return nil
}

func (a *mArg1Fixed[ret, arg]) cacheFill(fill any, fillNull func(multiAggPrivateStructure1[ret])) {
	a.fill = fill.(func(multiAggPrivateStructure1[ret], arg))
	a.fillNull = fillNull
}

type mArg1Bytes[ret types.FixedSizeTExceptStrType] struct {
	w vector.FunctionParameterWrapper[types.Varlena]

	fill     func(multiAggPrivateStructure1[ret], []byte)
	fillNull func(multiAggPrivateStructure1[ret])
}

func (a *mArg1Bytes[ret]) prepare(v *vector.Vector) {
	a.w = vector.GenerateFunctionStrParameter(v)
}

func (a *mArg1Bytes[ret]) doRowFill(aggImp multiAggPrivateStructure1[ret], row uint64) error {
	v, null := a.w.GetStrValue(row)
	if null {
		a.fillNull(aggImp)
	} else {
		a.fill(aggImp, v)
	}
	return nil
}

func (a *mArg1Bytes[ret]) cacheFill(fill any, fillNull func(multiAggPrivateStructure1[ret])) {
	a.fill = fill.(func(multiAggPrivateStructure1[ret], []byte))
	a.fillNull = fillNull
}

type mArg2Fixed[arg types.FixedSizeTExceptStrType] struct {
	w vector.FunctionParameterWrapper[arg]

	fill     func(multiAggPrivateStructure2, arg)
	fillNull func(multiAggPrivateStructure2)
}

func (a *mArg2Fixed[arg]) prepare(v *vector.Vector) {
	a.w = vector.GenerateFunctionFixedTypeParameter[arg](v)
}

func (a *mArg2Fixed[arg]) doRowFill(aggImp multiAggPrivateStructure2, row uint64) error {
	v, null := a.w.GetValue(row)
	if null {
		a.fillNull(aggImp)
	} else {
		a.fill(aggImp, v)
	}
	return nil
}

func (a *mArg2Fixed[arg]) cacheFill(fill any, fillNull func(multiAggPrivateStructure2)) {
	a.fill = fill.(func(multiAggPrivateStructure2, arg))
	a.fillNull = fillNull
}

type mArg2Bytes struct {
	w vector.FunctionParameterWrapper[types.Varlena]

	fill     func(multiAggPrivateStructure2, []byte)
	fillNull func(multiAggPrivateStructure2)
}

func (a *mArg2Bytes) prepare(v *vector.Vector) {
	a.w = vector.GenerateFunctionStrParameter(v)
}

func (a *mArg2Bytes) doRowFill(aggImp multiAggPrivateStructure2, row uint64) error {
	v, null := a.w.GetStrValue(row)
	if null {
		a.fillNull(aggImp)
	} else {
		a.fill(aggImp, v)
	}
	return nil
}

func (a *mArg2Bytes) cacheFill(fill any, fillNull func(multiAggPrivateStructure2)) {
	a.fill = fill.(func(multiAggPrivateStructure2, []byte))
	a.fillNull = fillNull
}
