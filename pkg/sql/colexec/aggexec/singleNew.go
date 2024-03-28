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
)

// all the codes in this file were to new the single column aggregation executors.

func newSingleAggFuncExec1(
	mg AggMemoryManager, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		return newSingleAggFuncExec1WithKnownResultType[bool](mg, info, opt, f)
	case types.T_int8:
		return newSingleAggFuncExec1WithKnownResultType[int8](mg, info, opt, f)
	case types.T_int16:
		return newSingleAggFuncExec1WithKnownResultType[int16](mg, info, opt, f)
	case types.T_int32:
		return newSingleAggFuncExec1WithKnownResultType[int32](mg, info, opt, f)
	case types.T_int64:
		return newSingleAggFuncExec1WithKnownResultType[int64](mg, info, opt, f)
	case types.T_uint8:
		return newSingleAggFuncExec1WithKnownResultType[uint8](mg, info, opt, f)
	case types.T_uint16:
		return newSingleAggFuncExec1WithKnownResultType[uint16](mg, info, opt, f)
	case types.T_uint32:
		return newSingleAggFuncExec1WithKnownResultType[uint32](mg, info, opt, f)
	case types.T_uint64:
		return newSingleAggFuncExec1WithKnownResultType[uint64](mg, info, opt, f)
	case types.T_float32:
		return newSingleAggFuncExec1WithKnownResultType[float32](mg, info, opt, f)
	case types.T_float64:
		return newSingleAggFuncExec1WithKnownResultType[float64](mg, info, opt, f)
	case types.T_decimal64:
		return newSingleAggFuncExec1WithKnownResultType[types.Decimal64](mg, info, opt, f)
	case types.T_decimal128:
		return newSingleAggFuncExec1WithKnownResultType[types.Decimal128](mg, info, opt, f)
	case types.T_date:
		return newSingleAggFuncExec1WithKnownResultType[types.Date](mg, info, opt, f)
	case types.T_datetime:
		return newSingleAggFuncExec1WithKnownResultType[types.Datetime](mg, info, opt, f)
	case types.T_time:
		return newSingleAggFuncExec1WithKnownResultType[types.Time](mg, info, opt, f)
	case types.T_timestamp:
		return newSingleAggFuncExec1WithKnownResultType[types.Timestamp](mg, info, opt, f)
	case types.T_bit:
		return newSingleAggFuncExec1WithKnownResultType[uint64](mg, info, opt, f)
	case types.T_TS:
		return newSingleAggFuncExec1WithKnownResultType[types.TS](mg, info, opt, f)
	case types.T_Rowid:
		return newSingleAggFuncExec1WithKnownResultType[types.Rowid](mg, info, opt, f)
	case types.T_Blockid:
		return newSingleAggFuncExec1WithKnownResultType[types.Blockid](mg, info, opt, f)
	case types.T_uuid:
		return newSingleAggFuncExec1WithKnownResultType[types.Uuid](mg, info, opt, f)
	}
	panic(fmt.Sprintf("unsupported result type %s for singleAggFuncExec1", info.retType))
}

func newSingleAggFuncExec1WithKnownResultType[to types.FixedSizeTExceptStrType](
	mg AggMemoryManager, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	switch info.argType.Oid {
	case types.T_bool:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[bool, to]); ok {
			e := &singleAggFuncExec1[bool, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_bit:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[uint64, to]); ok {
			e := &singleAggFuncExec1[uint64, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int8:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[int8, to]); ok {
			e := &singleAggFuncExec1[int8, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int16:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[int16, to]); ok {
			e := &singleAggFuncExec1[int16, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int32:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[int32, to]); ok {
			e := &singleAggFuncExec1[int32, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int64:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[int64, to]); ok {
			e := &singleAggFuncExec1[int64, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint8:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[uint8, to]); ok {
			e := &singleAggFuncExec1[uint8, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint16:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[uint16, to]); ok {
			e := &singleAggFuncExec1[uint16, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint32:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[uint32, to]); ok {
			e := &singleAggFuncExec1[uint32, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint64:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[uint64, to]); ok {
			e := &singleAggFuncExec1[uint64, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_float32:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[float32, to]); ok {
			e := &singleAggFuncExec1[float32, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_float64:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[float64, to]); ok {
			e := &singleAggFuncExec1[float64, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_decimal64:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Decimal64, to]); ok {
			e := &singleAggFuncExec1[types.Decimal64, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_decimal128:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Decimal128, to]); ok {
			e := &singleAggFuncExec1[types.Decimal128, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_date:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Date, to]); ok {
			e := &singleAggFuncExec1[types.Date, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_datetime:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Datetime, to]); ok {
			e := &singleAggFuncExec1[types.Datetime, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_time:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Time, to]); ok {
			e := &singleAggFuncExec1[types.Time, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_timestamp:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Timestamp, to]); ok {
			e := &singleAggFuncExec1[types.Timestamp, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_TS:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.TS, to]); ok {
			e := &singleAggFuncExec1[types.TS, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_Rowid:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Rowid, to]); ok {
			e := &singleAggFuncExec1[types.Rowid, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_Blockid:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Rowid, to]); ok {
			e := &singleAggFuncExec1[types.Rowid, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uuid:
		if g, ok := f.(func() SingleAggFromFixedRetFixed[types.Uuid, to]); ok {
			e := &singleAggFuncExec1[types.Uuid, to]{}
			e.init(mg, info, opt, g)
			return e
		}
	default:
		panic(fmt.Sprintf("unsupported parameter type for singleAggFuncExec1, aggInfo: %s", info))
	}
	panic(fmt.Sprintf("unexpected parameter to Init a singleAggFuncExec1, aggInfo: %s", info))
}

func newSingleAggFuncExec2(
	mg AggMemoryManager, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	switch info.argType.Oid {
	case types.T_bool:
		if g, ok := f.(func() SingleAggFromFixedRetVar[bool]); ok {
			e := &singleAggFuncExec2[bool]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_bit:
		if g, ok := f.(func() SingleAggFromFixedRetVar[uint64]); ok {
			e := &singleAggFuncExec2[uint64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int8:
		if g, ok := f.(func() SingleAggFromFixedRetVar[int8]); ok {
			e := &singleAggFuncExec2[int8]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int16:
		if g, ok := f.(func() SingleAggFromFixedRetVar[int16]); ok {
			e := &singleAggFuncExec2[int16]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int32:
		if g, ok := f.(func() SingleAggFromFixedRetVar[int32]); ok {
			e := &singleAggFuncExec2[int32]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int64:
		if g, ok := f.(func() SingleAggFromFixedRetVar[int64]); ok {
			e := &singleAggFuncExec2[int64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint8:
		if g, ok := f.(func() SingleAggFromFixedRetVar[uint8]); ok {
			e := &singleAggFuncExec2[uint8]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint16:
		if g, ok := f.(func() SingleAggFromFixedRetVar[uint16]); ok {
			e := &singleAggFuncExec2[uint16]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint32:
		if g, ok := f.(func() SingleAggFromFixedRetVar[uint32]); ok {
			e := &singleAggFuncExec2[uint32]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint64:
		if g, ok := f.(func() SingleAggFromFixedRetVar[uint64]); ok {
			e := &singleAggFuncExec2[uint64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_float32:
		if g, ok := f.(func() SingleAggFromFixedRetVar[float32]); ok {
			e := &singleAggFuncExec2[float32]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_float64:
		if g, ok := f.(func() SingleAggFromFixedRetVar[float64]); ok {
			e := &singleAggFuncExec2[float64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_decimal64:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Decimal64]); ok {
			e := &singleAggFuncExec2[types.Decimal64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_decimal128:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Decimal128]); ok {
			e := &singleAggFuncExec2[types.Decimal128]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_date:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Date]); ok {
			e := &singleAggFuncExec2[types.Date]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_datetime:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Datetime]); ok {
			e := &singleAggFuncExec2[types.Datetime]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_time:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Time]); ok {
			e := &singleAggFuncExec2[types.Time]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_timestamp:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Timestamp]); ok {
			e := &singleAggFuncExec2[types.Timestamp]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_TS:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.TS]); ok {
			e := &singleAggFuncExec2[types.TS]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_Rowid:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Rowid]); ok {
			e := &singleAggFuncExec2[types.Rowid]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_Blockid:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Blockid]); ok {
			e := &singleAggFuncExec2[types.Blockid]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uuid:
		if g, ok := f.(func() SingleAggFromFixedRetVar[types.Uuid]); ok {
			e := &singleAggFuncExec2[types.Uuid]{}
			e.init(mg, info, opt, g)
			return e
		}
	}
	panic(fmt.Sprintf("unsupported parameter type %s for singleAggFuncExec2", info.argType))
}

func newSingleAggFuncExec3(
	mg AggMemoryManager, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	switch info.retType.Oid {
	case types.T_bool:
		if g, ok := f.(func() SingleAggFromVarRetFixed[bool]); ok {
			e := &singleAggFuncExec3[bool]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_bit:
		if g, ok := f.(func() SingleAggFromVarRetFixed[uint64]); ok {
			e := &singleAggFuncExec3[uint64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int8:
		if g, ok := f.(func() SingleAggFromVarRetFixed[int8]); ok {
			e := &singleAggFuncExec3[int8]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int16:
		if g, ok := f.(func() SingleAggFromVarRetFixed[int16]); ok {
			e := &singleAggFuncExec3[int16]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int32:
		if g, ok := f.(func() SingleAggFromVarRetFixed[int32]); ok {
			e := &singleAggFuncExec3[int32]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_int64:
		if g, ok := f.(func() SingleAggFromVarRetFixed[int64]); ok {
			e := &singleAggFuncExec3[int64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint8:
		if g, ok := f.(func() SingleAggFromVarRetFixed[uint8]); ok {
			e := &singleAggFuncExec3[uint8]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint16:
		if g, ok := f.(func() SingleAggFromVarRetFixed[uint16]); ok {
			e := &singleAggFuncExec3[uint16]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint32:
		if g, ok := f.(func() SingleAggFromVarRetFixed[uint32]); ok {
			e := &singleAggFuncExec3[uint32]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uint64:
		if g, ok := f.(func() SingleAggFromVarRetFixed[uint64]); ok {
			e := &singleAggFuncExec3[uint64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_float32:
		if g, ok := f.(func() SingleAggFromVarRetFixed[float32]); ok {
			e := &singleAggFuncExec3[float32]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_float64:
		if g, ok := f.(func() SingleAggFromVarRetFixed[float64]); ok {
			e := &singleAggFuncExec3[float64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_decimal64:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Decimal64]); ok {
			e := &singleAggFuncExec3[types.Decimal64]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_decimal128:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Decimal128]); ok {
			e := &singleAggFuncExec3[types.Decimal128]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_date:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Date]); ok {
			e := &singleAggFuncExec3[types.Date]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_datetime:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Datetime]); ok {
			e := &singleAggFuncExec3[types.Datetime]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_time:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Time]); ok {
			e := &singleAggFuncExec3[types.Time]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_timestamp:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Timestamp]); ok {
			e := &singleAggFuncExec3[types.Timestamp]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_TS:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.TS]); ok {
			e := &singleAggFuncExec3[types.TS]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_Rowid:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Rowid]); ok {
			e := &singleAggFuncExec3[types.Rowid]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_Blockid:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Blockid]); ok {
			e := &singleAggFuncExec3[types.Blockid]{}
			e.init(mg, info, opt, g)
			return e
		}
	case types.T_uuid:
		if g, ok := f.(func() SingleAggFromVarRetFixed[types.Uuid]); ok {
			e := &singleAggFuncExec3[types.Uuid]{}
			e.init(mg, info, opt, g)
			return e
		}
	}
	panic(fmt.Sprintf("unsupported result type %s for singleAggFuncExec3", info.retType))
}

func newSingleAggFuncExec4(
	mg AggMemoryManager, info singleAggInfo, opt singleAggOptimizedInfo, f any) AggFuncExec {
	g, ok := f.(func() SingleAggFromVarRetVar)
	if !ok {
		panic(fmt.Sprintf("unexpected parameter to Init a singleAggFuncExec1, aggInfo: %s", info))
	}

	e := &singleAggFuncExec4{}
	e.init(mg, info, opt, g)
	return e
}
