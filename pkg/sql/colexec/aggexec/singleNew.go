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

import "github.com/matrixorigin/matrixone/pkg/container/types"

// all the codes in this file were to new the single column aggregation executors.

func newSingleAggFuncExec1(from, to types.Type) AggFuncExec {
	switch to.Oid {
	case types.T_bool:
		return newSingleAggFuncExec1WithKnownResultType[bool](from)
	case types.T_int8:
		return newSingleAggFuncExec1WithKnownResultType[int8](from)
	case types.T_int16:
		return newSingleAggFuncExec1WithKnownResultType[int16](from)
	case types.T_int32:
		return newSingleAggFuncExec1WithKnownResultType[int32](from)
	case types.T_int64:
		return newSingleAggFuncExec1WithKnownResultType[int64](from)
	case types.T_uint8:
		return newSingleAggFuncExec1WithKnownResultType[uint8](from)
	case types.T_uint16:
		return newSingleAggFuncExec1WithKnownResultType[uint16](from)
	case types.T_uint32:
		return newSingleAggFuncExec1WithKnownResultType[uint32](from)
	case types.T_uint64:
		return newSingleAggFuncExec1WithKnownResultType[uint64](from)
	case types.T_float32:
		return newSingleAggFuncExec1WithKnownResultType[float32](from)
	case types.T_float64:
		return newSingleAggFuncExec1WithKnownResultType[float64](from)
	case types.T_decimal64:
		return newSingleAggFuncExec1WithKnownResultType[types.Decimal64](from)
	case types.T_decimal128:
		return newSingleAggFuncExec1WithKnownResultType[types.Decimal128](from)
	case types.T_date:
		return newSingleAggFuncExec1WithKnownResultType[types.Date](from)
	case types.T_datetime:
		return newSingleAggFuncExec1WithKnownResultType[types.Datetime](from)
	case types.T_time:
		return newSingleAggFuncExec1WithKnownResultType[types.Time](from)
	case types.T_timestamp:
		return newSingleAggFuncExec1WithKnownResultType[types.Timestamp](from)
	}
	panic("unsupported result type for singleAggFuncExec1")
}

func newSingleAggFuncExec1WithKnownResultType[to types.FixedSizeTExceptStrType](from types.Type) AggFuncExec {
	switch from.Oid {
	case types.T_bool:
		return &singleAggFuncExec1[bool, to]{}
	case types.T_int8:
		return &singleAggFuncExec1[int8, to]{}
	case types.T_int16:
		return &singleAggFuncExec1[int16, to]{}
	case types.T_int32:
		return &singleAggFuncExec1[int32, to]{}
	case types.T_int64:
		return &singleAggFuncExec1[int64, to]{}
	case types.T_uint8:
		return &singleAggFuncExec1[uint8, to]{}
	case types.T_uint16:
		return &singleAggFuncExec1[uint16, to]{}
	case types.T_uint32:
		return &singleAggFuncExec1[uint32, to]{}
	case types.T_uint64:
		return &singleAggFuncExec1[uint64, to]{}
	case types.T_float32:
		return &singleAggFuncExec1[float32, to]{}
	case types.T_float64:
		return &singleAggFuncExec1[float64, to]{}
	case types.T_decimal64:
		return &singleAggFuncExec1[types.Decimal64, to]{}
	case types.T_decimal128:
		return &singleAggFuncExec1[types.Decimal128, to]{}
	case types.T_date:
		return &singleAggFuncExec1[types.Date, to]{}
	case types.T_datetime:
		return &singleAggFuncExec1[types.Datetime, to]{}
	case types.T_time:
		return &singleAggFuncExec1[types.Time, to]{}
	case types.T_timestamp:
		return &singleAggFuncExec1[types.Timestamp, to]{}
	}
	panic("unsupported parameter type for singleAggFuncExec1")
}
