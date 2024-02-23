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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// all the codes in this file were to new the single column aggregation executors.

func newSingleAggFuncExec1(
	proc *process.Process, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		return newSingleAggFuncExec1WithKnownResultType[bool](proc, info, opt, f)
	case types.T_int8:
		return newSingleAggFuncExec1WithKnownResultType[int8](proc, info, opt, f)
	case types.T_int16:
		return newSingleAggFuncExec1WithKnownResultType[int16](proc, info, opt, f)
	case types.T_int32:
		return newSingleAggFuncExec1WithKnownResultType[int32](proc, info, opt, f)
	case types.T_int64:
		return newSingleAggFuncExec1WithKnownResultType[int64](proc, info, opt, f)
	case types.T_uint8:
		return newSingleAggFuncExec1WithKnownResultType[uint8](proc, info, opt, f)
	case types.T_uint16:
		return newSingleAggFuncExec1WithKnownResultType[uint16](proc, info, opt, f)
	case types.T_uint32:
		return newSingleAggFuncExec1WithKnownResultType[uint32](proc, info, opt, f)
	case types.T_uint64:
		return newSingleAggFuncExec1WithKnownResultType[uint64](proc, info, opt, f)
	case types.T_float32:
		return newSingleAggFuncExec1WithKnownResultType[float32](proc, info, opt, f)
	case types.T_float64:
		return newSingleAggFuncExec1WithKnownResultType[float64](proc, info, opt, f)
	case types.T_decimal64:
		return newSingleAggFuncExec1WithKnownResultType[types.Decimal64](proc, info, opt, f)
	case types.T_decimal128:
		return newSingleAggFuncExec1WithKnownResultType[types.Decimal128](proc, info, opt, f)
	case types.T_date:
		return newSingleAggFuncExec1WithKnownResultType[types.Date](proc, info, opt, f)
	case types.T_datetime:
		return newSingleAggFuncExec1WithKnownResultType[types.Datetime](proc, info, opt, f)
	case types.T_time:
		return newSingleAggFuncExec1WithKnownResultType[types.Time](proc, info, opt, f)
	case types.T_timestamp:
		return newSingleAggFuncExec1WithKnownResultType[types.Timestamp](proc, info, opt, f)
	}
	panic("unsupported result type for singleAggFuncExec1")
}

func newSingleAggFuncExec1WithKnownResultType[to types.FixedSizeTExceptStrType](
	proc *process.Process, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	switch info.argType.Oid {
	case types.T_bool:
		if g, ok := f.(func() singleAggPrivateStructure1[bool, to]); ok {
			e := &singleAggFuncExec1[bool, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_int8:
		if g, ok := f.(func() singleAggPrivateStructure1[int8, to]); ok {
			e := &singleAggFuncExec1[int8, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_int16:
		if g, ok := f.(func() singleAggPrivateStructure1[int16, to]); ok {
			e := &singleAggFuncExec1[int16, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_int32:
		if g, ok := f.(func() singleAggPrivateStructure1[int32, to]); ok {
			e := &singleAggFuncExec1[int32, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_int64:
		if g, ok := f.(func() singleAggPrivateStructure1[int64, to]); ok {
			e := &singleAggFuncExec1[int64, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_uint8:
		if g, ok := f.(func() singleAggPrivateStructure1[uint8, to]); ok {
			e := &singleAggFuncExec1[uint8, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_uint16:
		if g, ok := f.(func() singleAggPrivateStructure1[uint16, to]); ok {
			e := &singleAggFuncExec1[uint16, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_uint32:
		if g, ok := f.(func() singleAggPrivateStructure1[uint32, to]); ok {
			e := &singleAggFuncExec1[uint32, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_uint64:
		if g, ok := f.(func() singleAggPrivateStructure1[uint64, to]); ok {
			e := &singleAggFuncExec1[uint64, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_float32:
		if g, ok := f.(func() singleAggPrivateStructure1[float32, to]); ok {
			e := &singleAggFuncExec1[float32, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_float64:
		if g, ok := f.(func() singleAggPrivateStructure1[float64, to]); ok {
			e := &singleAggFuncExec1[float64, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_decimal64:
		if g, ok := f.(func() singleAggPrivateStructure1[types.Decimal64, to]); ok {
			e := &singleAggFuncExec1[types.Decimal64, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_decimal128:
		if g, ok := f.(func() singleAggPrivateStructure1[types.Decimal128, to]); ok {
			e := &singleAggFuncExec1[types.Decimal128, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_date:
		if g, ok := f.(func() singleAggPrivateStructure1[types.Date, to]); ok {
			e := &singleAggFuncExec1[types.Date, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_datetime:
		if g, ok := f.(func() singleAggPrivateStructure1[types.Datetime, to]); ok {
			e := &singleAggFuncExec1[types.Datetime, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_time:
		if g, ok := f.(func() singleAggPrivateStructure1[types.Time, to]); ok {
			e := &singleAggFuncExec1[types.Time, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	case types.T_timestamp:
		if g, ok := f.(func() singleAggPrivateStructure1[types.Timestamp, to]); ok {
			e := &singleAggFuncExec1[types.Timestamp, to]{}
			e.init(proc, info, opt, g)
			return e
		}
	default:
		panic(fmt.Sprintf("unsupported parameter type for singleAggFuncExec1, aggInfo: %s", info))
	}
	panic(fmt.Sprintf("unexpected parameter to init a singleAggFuncExec1, aggInfo: %s", info))
}

func newSingleAggFuncExec2(
	proc *process.Process, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	return nil
}

func newSingleAggFuncExec3(
	proc *process.Process, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	return nil
}

func newSingleAggFuncExec4(
	proc *process.Process, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	return nil
}
